// registry/shadowRegistry.js — the SHADOW dispatcher (refactor Phase 1, REF-1.1;
// re-cut for S1c per Aster's S1b disposition, all seven findings).
//
// Ship-safety invariants (each an adversarial-tested case in the gate):
//
//   1. OBSERVATION ISOLATION — a row's schema/correlation/idempotency callbacks
//      receive ONLY a bounded, reference-disjoint, deeply-frozen PROJECTION of
//      the frame's declared fields. They can neither see the whole payload nor
//      mutate live dispatch input, on every path; if a bounded projection can't
//      be produced, observation is skipped and a snapshot fault recorded. No
//      shared graph (Aster #1); bounded work before allocation (Aster #7).
//   2. Handler return inspection is FULLY defensive — no arbitrary accessor or
//      proxy trap (e.g. a throwing `consumed` getter) can escape after the
//      handler returns (Aster #2).
//   3. Selector output is validated inside the guard to `null | bounded string`;
//      an invalid value resolves NO contract, records a fault, and dispatches
//      unchanged — never reaching a string-template coercion (Aster #3).
//   4. Only defineRow-branded rows register (module-private WeakSet); duplicate
//      keys rejected (Aster #4, in types.js).
//   5. Telemetry emits ONLY bounded, registry-DECLARED labels: variant clamped,
//      correlation kind fixed to the row's declared subjectShape (never the
//      callback's), no payload/signature/hash values (Aster #5).
//   * Flag OFF is a verbatim pass-through; a throwing flag read falls through to
//     the handler. Report mode only; no enforcement anywhere.

import { isRow } from './types.js';

let _override = null;
function envFlag() {
  try { const v = (typeof process !== 'undefined' && process.env && process.env.AXONA_REGISTRY_SHADOW) || ''; return v === '1' || v === 'true' || v === 'on'; }
  catch { return false; }
}
export function shadowEnabled() { return _override === null ? envFlag() : _override; }
export function setShadowEnabled(on) { _override = (on === null || on === undefined) ? null : !!on; }

const MAX_STR = 120;         // hard cap on any string reaching a trace or projection
const MAX_FIELDS = 24;       // hard cap on projection fields observed per side
const clamp = (s) => { try { s = String(s); return s.length > MAX_STR ? s.slice(0, MAX_STR) : s; } catch { return ''; } };

// A scalar, bounded copy — the ONLY kind of value that enters a projection.
// Non-scalars are dropped (represented as absent) so no object graph is shared
// and no unbounded value is copied.
function boundScalar(v) {
  const t = typeof v;
  if (t === 'string') return v.length > MAX_STR ? v.slice(0, MAX_STR) : v;
  if (t === 'number') return Number.isFinite(v) ? v : null;
  if (t === 'boolean') return v;
  if (t === 'bigint') { try { return clamp(v.toString()); } catch { return null; } }
  return undefined; // objects/functions/symbols/null → omitted
}

// Build a reference-disjoint, deeply-frozen projection of ONLY the declared
// fields. Property reads are individually trap-contained. Returns a frozen
// { payload, meta } or null (→ snapshot fault). Never clones the whole frame.
function buildProjection(row, args) {
  try {
    const out = { payload: {}, meta: {} };
    const src = [['payload', args[0]], ['meta', args[1]]];
    for (const [side, obj] of src) {
      const fields = row.projection[side];
      if (!obj || typeof obj !== 'object' || !fields.length) { Object.freeze(out[side]); continue; }
      const n = Math.min(fields.length, MAX_FIELDS);
      for (let i = 0; i < n; i++) {
        const f = fields[i];
        let v; try { v = obj[f]; } catch { continue; }   // getter/proxy trap contained
        const s = boundScalar(v);
        if (s !== undefined) out[side][f] = s;
      }
      Object.freeze(out[side]);
    }
    return Object.freeze(out);
  } catch { return null; }
}

export class ShadowRegistry {
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

  static _key(type, variant) { return variant ? `${type}#${variant}` : type; } // variant is always a validated string here

  register(row) {
    if (!isRow(row)) throw new TypeError('ShadowRegistry.register: a defineRow()-branded row required');
    const key = ShadowRegistry._key(row.type, row.variant);
    if (this._rows.has(key)) throw new TypeError(`ShadowRegistry.register: duplicate row key ${key}`);
    this._rows.set(key, row);
    return this;
  }

  row(type, variant = null) { return this._rows.get(ShadowRegistry._key(type, variant)) || null; }
  size() { return this._rows.size; }

  wrap(type, handler, opts = {}) {
    if (typeof handler !== 'function') throw new TypeError(`ShadowRegistry.wrap(${type}): handler function required`);
    const self = this;
    const select = typeof opts.select === 'function' ? opts.select : null;

    return function shadowWrapped(...args) {
      let on; try { on = self._enabled(); } catch { on = false; }
      if (!on) return handler.apply(this, args);

      const t0 = self._safeNow();

      // (3) selector output validated to null | bounded string — no coercion.
      let variant = null, selectorFault = null, variantFault = null;
      if (select) {
        try {
          const v = select(...args);
          if (v == null) variant = null;
          else if (typeof v === 'string') { if (v.length <= MAX_STR) variant = v; else selectorFault = 'selector output too long'; }
          else selectorFault = `selector output not a string (${typeof v})`;
        } catch (e) { selectorFault = errMsg(e); }
      }
      let row = null;
      if (selectorFault) row = null;
      else if (variant != null) { row = self.row(type, variant); if (!row) variantFault = `unknown variant ${clamp(variant)}`; }
      else row = self.row(type, null);

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
      // (2) verdict extraction is fully contained; compute BEFORE returning but
      // it can never throw.
      self._emit(row, type, variant, pre, { verdict: verdictOf(result) }, self._safeNow() - t0);
      return result;
    };
  }

  _observe(row, args, faults) {
    const out = { registered: !!row, selectorFault: faults.selectorFault || null, variantFault: faults.variantFault || null };
    if (!row) return out;
    out.kind = row.kind; out.owningService = row.owningService; out.evidence = row.evidence; out.proves = row.proves;
    out.subjectShape = row.subjectShape;
    const proj = buildProjection(row, args);
    if (proj === null) { out.snapshotFault = true; return out; }
    try { const s = row.schema(proj.payload, proj.meta); out.schemaOk = !!(s && s.ok); if (!out.schemaOk) out.schemaReason = clamp(s && s.reason); }
    catch (e) { out.schemaOk = null; out.schemaFault = errMsg(e); }
    if (row.correlation) {
      // Presence only — the emitted kind is the row's DECLARED subjectShape,
      // never a callback-controlled value.
      try { out.correlationPresent = row.correlation(proj.payload, proj.meta) != null; }
      catch (e) { out.correlationFault = errMsg(e); }
    }
    if (row.idempotencyKey) {
      try { out.idempotencyPresent = row.idempotencyKey(proj.payload, proj.meta) != null; }
      catch (e) { out.idempotencyFault = errMsg(e); }
    }
    return out;
  }

  _emit(row, type, variant, pre, post, durationMs) {
    const isFault = post.verdict === 'threw' || post.verdict === 'rejected'
      || pre.schemaOk === false || pre.schemaFault || pre.correlationFault || pre.idempotencyFault
      || pre.selectorFault || pre.variantFault || pre.snapshotFault || pre.registered === false;
    this._seen++;
    if (!isFault && this._sampleEvery > 1 && (this._seen % this._sampleEvery) !== 0) return;

    let rec;
    try {
      rec = {
        boundary: this.boundary,
        type: clamp(type),
        variant: variant == null ? null : clamp(variant),
        registered: pre.registered,
        kind: pre.kind ?? null,
        owningService: pre.owningService ?? null,
        evidence: pre.evidence ?? null,              // orthogonal fact label, never a score
        proves: pre.proves ?? null,
        schemaOk: pre.schemaOk ?? null,
        schemaReason: pre.schemaReason ?? (pre.schemaFault ? clamp(pre.schemaFault) : null),
        correlationPresent: pre.correlationPresent ?? null,
        correlationKind: pre.subjectShape ?? null,   // DECLARED shape, not callback output
        idempotencyPresent: pre.idempotencyPresent ?? null,
        correlationFault: pre.correlationFault ? clamp(pre.correlationFault) : null,
        idempotencyFault: pre.idempotencyFault ? clamp(pre.idempotencyFault) : null,
        selectorFault: pre.selectorFault ? clamp(pre.selectorFault) : null,
        variantFault: pre.variantFault ? clamp(pre.variantFault) : null,
        snapshotFault: pre.snapshotFault ? true : null,
        verdict: post.verdict,
        error: post.error ? clamp(post.error) : null,
        durationMs: Number.isFinite(durationMs) ? Math.round(durationMs * 1000) / 1000 : null,
      };
    } catch (e) { rec = { boundary: this.boundary, type: clamp(type), traceFault: errMsg(e) }; }
    try { this._sink(rec); } catch { /* a broken sink must never break dispatch */ }
  }

  _safeNow() { try { const n = this._now(); return Number.isFinite(n) ? n : 0; } catch { return 0; } }
}

function isThenableSafe(x) {
  try { return x != null && (typeof x === 'object' || typeof x === 'function') && typeof x.then === 'function'; }
  catch { return false; }
}
function defaultNow() {
  try { if (typeof performance !== 'undefined' && typeof performance.now === 'function') return performance.now(); } catch { /* */ }
  return 0;
}
function errMsg(e) { try { return clamp((e && e.message) ? e.message : String(e)); } catch { return 'unknown'; } }

// Coarse verdict — fully contained; a throwing `consumed` getter cannot escape.
function verdictOf(r) {
  try {
    if (r === 'consumed') return 'consumed';
    if (r === undefined || r === null || r === false) return 'passed';
    if (typeof r === 'object') { let c; try { c = r.consumed; } catch { return 'other'; } if (c === true) return 'consumed'; }
    return 'other';
  } catch { return 'other'; }
}

export default ShadowRegistry;
