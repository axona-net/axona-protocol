// registry/shadowRegistry.js — the SHADOW dispatcher (refactor Phase 1, REF-1.1;
// hardened per Aster S1 disposition items 1/2/5/6).
//
// A ShadowRegistry holds contract rows and can WRAP a boundary handler so that,
// when enabled, each frame is validated against its row and a bounded, sanitized
// trace is emitted — BESIDE the real handler, never in front of it. Ship-safety
// invariants:
//
//   1. Flag OFF → verbatim pass-through: the handler is called with EVERY
//      argument unchanged and the same `this`; its return/throw is returned
//      verbatim. A fault reading the flag also falls through to the handler.
//   2. Flag ON → validation + tracing are READ-ONLY and DEFENSIVE. The row's
//      schema/correlation/idempotency callbacks are invoked ONLY against an
//      IMMUTABLE SNAPSHOT, never the live payload/meta the handler will see, so
//      a buggy or hostile callback cannot mutate dispatch input. The handler's
//      return value, `this`, throw, and async resolution/rejection are never
//      altered; an async promise is returned untouched and observed passively.
//      Every fault in the shadow path (flag, thenable probe, snapshot, callback,
//      sink) is contained.
//   3. Telemetry is ALLOWLISTED, hashed/bounded, and sampleable: no payload
//      bodies, signatures, secrets, or unbounded values ever enter a trace.
//
// Report mode is the only mode Phase 1 ships. Enforcement (a row rejecting a
// frame) is a later-phase, per-family migration and exists nowhere here.

import { isRow } from './types.js';

let _override = null; // null = defer to env; true/false = forced
function envFlag() {
  try {
    const v = (typeof process !== 'undefined' && process.env && process.env.AXONA_REGISTRY_SHADOW) || '';
    return v === '1' || v === 'true' || v === 'on';
  } catch { return false; }
}
export function shadowEnabled() { return _override === null ? envFlag() : _override; }
export function setShadowEnabled(on) { _override = (on === null || on === undefined) ? null : !!on; }

const MAX_STR = 80;                 // hard cap on any string that reaches a trace
const clamp = (s) => { try { s = String(s); return s.length > MAX_STR ? s.slice(0, MAX_STR) : s; } catch { return ''; } };

// FNV-1a 32-bit → base36. Non-crypto, dependency-free; used only to turn a
// correlation/idempotency value into a stable, bounded, non-reversible tag so
// the raw value (which may be a msgId, author id, nonce, etc.) never ships.
function shortHash(input) {
  let str; try { str = typeof input === 'string' ? input : JSON.stringify(input); } catch { str = ''; }
  if (str == null) return 'h:0';
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return 'h:' + (h >>> 0).toString(36);
}

// Deep, throwaway snapshot so observation callbacks cannot touch live dispatch
// input. structuredClone deep-copies plain frames; on any exotic value we fall
// back to a shallow frozen copy, and if even that fails we hand back an empty
// object (observation degrades to structural facts only — never the handler).
function safeSnapshot(x) {
  try { if (typeof structuredClone === 'function') return structuredClone(x); } catch { /* fall through */ }
  try { return Object.freeze(Array.isArray(x) ? x.slice() : { ...x }); } catch { return Object.freeze({}); }
}

function isThenableSafe(x) {
  try { return x != null && (typeof x === 'object' || typeof x === 'function') && typeof x.then === 'function'; }
  catch { return false; }  // a throwing `then` getter must not escape
}

export class ShadowRegistry {
  // opts.sink(record)   — where sanitized trace records go (default no-op).
  // opts.enabled()      — flag fn (default module shadowEnabled()).
  // opts.now()          — clock for durationMs (default performance.now, best-effort).
  // opts.sampleEvery    — emit 1 of every N traces (default 1 = all). Deterministic.
  constructor(opts = {}) {
    this.boundary = opts.boundary || 'unknown';
    this._rows = new Map();
    this._sink = typeof opts.sink === 'function' ? opts.sink : () => {};
    this._enabled = typeof opts.enabled === 'function' ? opts.enabled : shadowEnabled;
    this._now = typeof opts.now === 'function' ? opts.now : defaultNow;
    const s = Number(opts.sampleEvery);
    this._sampleEvery = Number.isInteger(s) && s >= 1 ? s : 1;
    this._seen = 0;
  }

  static _key(type, variant) { return variant ? `${type}#${variant}` : type; }

  // Register a BRANDED, frozen row (from defineRow). Rejects raw/mutable rows
  // and DUPLICATE type/variant keys — a silent overwrite is a contract error
  // (Aster item 2).
  register(row) {
    if (!isRow(row)) throw new TypeError('ShadowRegistry.register: a defineRow()-branded row required');
    const key = ShadowRegistry._key(row.type, row.variant);
    if (this._rows.has(key)) throw new TypeError(`ShadowRegistry.register: duplicate row key ${key}`);
    this._rows.set(key, row);
    return this;
  }

  row(type, variant = null) { return this._rows.get(ShadowRegistry._key(type, variant)) || null; }
  size() { return this._rows.size; }

  // Wrap a boundary handler for `type`. The handler is invoked EXACTLY as
  // un-wrapped: all arguments forwarded, same `this`. `opts.select(...args) ->
  // variant|null` disambiguates multi-variant rows; an unknown variant or a
  // selector fault is REPORTED and never silently resolves to a different
  // contract (Aster item 2).
  wrap(type, handler, opts = {}) {
    if (typeof handler !== 'function') throw new TypeError(`ShadowRegistry.wrap(${type}): handler function required`);
    const self = this;
    const select = typeof opts.select === 'function' ? opts.select : null;

    return function shadowWrapped(...args) {
      // (1) flag read is itself defensive: a throwing flag → pass-through.
      let on; try { on = self._enabled(); } catch { on = false; }
      if (!on) return handler.apply(this, args);

      // (2) report mode. Resolve the row/variant defensively.
      const t0 = self._safeNow();
      let variant = null, selectorFault = null, variantFault = null;
      if (select) { try { variant = select(...args) || null; } catch (e) { selectorFault = errMsg(e); } }
      let row = null;
      if (selectorFault) {
        row = null;                              // selection failed → resolve NO contract (report only)
      } else if (variant != null) {
        row = self.row(type, variant);
        if (!row) variantFault = `unknown variant ${clamp(variant)}`;   // NO fallback to a different contract
      } else {
        row = self.row(type, null);
      }

      // Observation runs against an immutable snapshot — never the live args.
      const pre = self._observe(row, args, { selectorFault, variantFault });

      let result, threw = null;
      try { result = handler.apply(this, args); } catch (e) { threw = e; }

      if (threw !== null) {
        self._emit(row, type, variant, pre, { verdict: 'threw', error: errMsg(threw) }, self._safeNow() - t0);
        throw threw;
      }
      if (isThenableSafe(result)) {
        try {
          result.then(
            (v) => self._emit(row, type, variant, pre, { verdict: verdictOf(v) }, self._safeNow() - t0),
            (e) => self._emit(row, type, variant, pre, { verdict: 'rejected', error: errMsg(e) }, self._safeNow() - t0),
          );
        } catch { /* observing must never affect the result */ }
        return result;
      }
      self._emit(row, type, variant, pre, { verdict: verdictOf(result) }, self._safeNow() - t0);
      return result;
    };
  }

  // Read-only observation against a SNAPSHOT. Never throws, never mutates live
  // input. A missing row is a real signal (catalogue gap) in shadow mode.
  _observe(row, args, faults) {
    const out = { registered: !!row, selectorFault: faults.selectorFault || null, variantFault: faults.variantFault || null };
    if (!row) return out;
    out.kind = row.kind; out.owningService = row.owningService; out.evidence = row.evidence; out.proves = row.proves;
    let snapP, snapM;
    try { snapP = safeSnapshot(args[0]); snapM = safeSnapshot(args[1]); }
    catch { return out; } // snapshot failed → structural facts only
    try { const s = row.schema(snapP, snapM); out.schemaOk = !!(s && s.ok); if (!out.schemaOk) out.schemaReason = clamp(s && s.reason); }
    catch (e) { out.schemaOk = null; out.schemaFault = errMsg(e); }
    if (row.correlation) {
      try { out.correlation = sanitizeCorrelation(row.correlation(snapP, snapM), row); }
      catch (e) { out.correlationFault = errMsg(e); }
    }
    if (row.idempotencyKey) {
      try { const k = row.idempotencyKey(snapP, snapM); out.idempotencyTag = k == null ? null : shortHash(k); }
      catch (e) { out.idempotencyFault = errMsg(e); }
    }
    return out;
  }

  _emit(row, type, variant, pre, post, durationMs) {
    // Sampling (deterministic; no RNG). Faults always emit so problems are never sampled away.
    const isFault = post.verdict === 'threw' || post.verdict === 'rejected'
      || pre.schemaOk === false || pre.schemaFault || pre.correlationFault || pre.idempotencyFault
      || pre.selectorFault || pre.variantFault || pre.registered === false;
    this._seen++;
    if (!isFault && this._sampleEvery > 1 && (this._seen % this._sampleEvery) !== 0) return;

    let rec;
    try {
      rec = {
        boundary: this.boundary,
        type: clamp(type),
        variant: variant || (row && row.variant) || null,
        registered: pre.registered,
        kind: pre.kind ?? null,
        owningService: pre.owningService ?? null,
        evidence: pre.evidence ?? null,   // an orthogonal fact label, never a score
        proves: pre.proves ?? null,
        schemaOk: pre.schemaOk ?? null,
        schemaReason: pre.schemaReason ?? (pre.schemaFault ? clamp(pre.schemaFault) : null),
        correlation: pre.correlation ?? null,        // {kind, digest} — never raw fields
        idempotencyTag: pre.idempotencyTag ?? null,  // hashed, never raw
        correlationFault: pre.correlationFault ? clamp(pre.correlationFault) : null,
        selectorFault: pre.selectorFault ? clamp(pre.selectorFault) : null,
        variantFault: pre.variantFault ? clamp(pre.variantFault) : null,
        verdict: post.verdict,
        error: post.error ? clamp(post.error) : null,
        durationMs: Number.isFinite(durationMs) ? Math.round(durationMs * 1000) / 1000 : null,
      };
    } catch (e) { rec = { boundary: this.boundary, type: clamp(type), traceFault: errMsg(e) }; }
    try { this._sink(rec); } catch { /* a broken sink must never break dispatch */ }
  }

  _safeNow() { try { const n = this._now(); return Number.isFinite(n) ? n : 0; } catch { return 0; } }
}

// Sanitize a correlation subject into an allowlisted, non-reversible record:
// the tagged `kind`, plus a single digest over ONLY the row's declared
// correlationFields. Raw ids/nonces/signatures never ship (Aster item 5).
function sanitizeCorrelation(subject, row) {
  if (subject == null || typeof subject !== 'object') return null;
  const allow = Array.isArray(row.correlationFields) ? row.correlationFields : [];
  const projected = {};
  for (const f of allow) { if (Object.prototype.hasOwnProperty.call(subject, f)) projected[f] = subject[f]; }
  return { kind: clamp(subject.kind || row.subjectShape || 'unknown'), digest: shortHash(projected) };
}

function defaultNow() {
  try { if (typeof performance !== 'undefined' && typeof performance.now === 'function') return performance.now(); } catch { /* */ }
  return 0; // Date.now() avoided; duration is best-effort telemetry, never behavior
}
function errMsg(e) { try { return clamp((e && e.message) ? e.message : String(e)); } catch { return 'unknown'; } }

// Coarse verdict for the trace, mirroring the pub/sub dispatch convention
// without importing it (keeps the registry boundary-agnostic).
function verdictOf(r) {
  if (r === 'consumed') return 'consumed';
  if (r && typeof r === 'object' && r.consumed === true) return 'consumed';
  if (r === undefined || r === null || r === false) return 'passed';
  return 'other';
}

export default ShadowRegistry;
