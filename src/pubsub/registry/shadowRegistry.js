// registry/shadowRegistry.js — the SHADOW dispatcher (refactor Phase 1, REF-1.1).
//
// A ShadowRegistry holds contract rows by frame type and can WRAP a boundary's
// handler so that, when enabled, each frame is validated against its row and a
// trace record is emitted — beside the real handler, never in front of it. The
// invariants that make this safe to ship into a running kernel:
//
//   1. Flag OFF  → the wrap is a pure pass-through: the original handler is
//      called with the same `this`/args and its return value (or throw) is
//      returned verbatim. Byte-identical, zero added work beyond one boolean.
//   2. Flag ON   → validation and tracing are READ-ONLY and DEFENSIVE. They
//      never mutate payload/meta, never change the handler's return value, and
//      never convert a handler success into a failure (or vice-versa). Any
//      error inside validation/tracing is swallowed and recorded as an
//      internal trace fault — it must not perturb acceptance behavior.
//   3. The handler's own throw is preserved exactly (re-thrown), and an async
//      handler's promise is returned UNCHANGED — tracing observes it passively
//      via a detached .then and never consumes its rejection.
//
// "Report mode" is the only mode Phase 1 ships. Enforcement (a row rejecting a
// frame) is a later-phase, per-family migration and lives nowhere in here.

// Flag resolution. Default OFF. In Node it reads AXONA_REGISTRY_SHADOW=1|true|on;
// in any environment a programmatic override wins (for tests and for the peer to
// flip it at runtime without env access). Never throws.
let _override = null; // null = defer to env; true/false = forced
function envFlag() {
  try {
    const v = (typeof process !== 'undefined' && process.env && process.env.AXONA_REGISTRY_SHADOW) || '';
    return v === '1' || v === 'true' || v === 'on';
  } catch { return false; }
}
export function shadowEnabled() {
  return _override === null ? envFlag() : _override;
}
export function setShadowEnabled(on) {
  _override = (on === null || on === undefined) ? null : !!on;
}

const isThenable = (x) => x != null && (typeof x === 'object' || typeof x === 'function') && typeof x.then === 'function';

export class ShadowRegistry {
  // opts.sink(traceRecord)  — where trace records go (default: no-op).
  // opts.enabled()          — flag function (default: module shadowEnabled()).
  // opts.now()              — clock for durationMs (default: a monotonic-ish stamp; falls back to 0 when unavailable, never throws).
  constructor(opts = {}) {
    this.boundary = opts.boundary || 'unknown';
    this._rows = new Map();               // key -> row  (key = type, or `${type}#${variant}`)
    this._sink = typeof opts.sink === 'function' ? opts.sink : () => {};
    this._enabled = typeof opts.enabled === 'function' ? opts.enabled : shadowEnabled;
    this._now = typeof opts.now === 'function' ? opts.now : defaultNow;
  }

  static _key(type, variant) { return variant ? `${type}#${variant}` : type; }

  // Register a (frozen) row. A type may carry multiple variant rows (e.g. the
  // signed vs legacy-unsigned INGESTACK); a `select` at wrap time picks one.
  register(row) {
    if (!row || typeof row.type !== 'string') throw new TypeError('ShadowRegistry.register: a defined row required');
    this._rows.set(ShadowRegistry._key(row.type, row.variant), row);
    return this;
  }

  row(type, variant = null) { return this._rows.get(ShadowRegistry._key(type, variant)) || null; }
  size() { return this._rows.size; }

  // Wrap a boundary handler for `type`. `handler` is called EXACTLY as it would
  // be un-wrapped. `opts.select(payload, meta) -> variant|null` disambiguates
  // multi-variant rows (returns the variant string, or null for the base row).
  wrap(type, handler, opts = {}) {
    if (typeof handler !== 'function') throw new TypeError(`ShadowRegistry.wrap(${type}): handler function required`);
    const self = this;
    const select = typeof opts.select === 'function' ? opts.select : null;
    return function shadowWrapped(payload, meta) {
      // (1) fast path: flag off → verbatim pass-through.
      if (!self._enabled()) return handler.call(this, payload, meta);

      // (2) report mode. Everything here is defensive; a fault in the shadow
      // path is traced, never propagated.
      const t0 = self._safeNow();
      let variant = null;
      if (select) { try { variant = select(payload, meta) || null; } catch { variant = null; } }
      const row = self.row(type, variant) || self.row(type, null);
      // Read-only validation BEFORE the handler (captures schema/correlation
      // facts even if the handler later throws).
      const pre = self._observe(row, payload, meta);

      let result, threw = null;
      try {
        result = handler.call(this, payload, meta);
      } catch (e) {
        threw = e;
      }

      if (threw !== null) {
        self._emit(row, type, variant, pre, { verdict: 'threw', error: errMsg(threw) }, self._safeNow() - t0);
        throw threw;                       // preserve original throw exactly
      }

      if (isThenable(result)) {
        // Observe passively; return the ORIGINAL promise untouched so the real
        // caller still owns its resolution/rejection.
        try {
          result.then(
            (v) => self._emit(row, type, variant, pre, { verdict: verdictOf(v), value: false }, self._safeNow() - t0),
            (e) => self._emit(row, type, variant, pre, { verdict: 'rejected', error: errMsg(e) }, self._safeNow() - t0),
          );
        } catch { /* attaching an observer must never affect the result */ }
        return result;
      }

      self._emit(row, type, variant, pre, { verdict: verdictOf(result), value: false }, self._safeNow() - t0);
      return result;
    };
  }

  // Read-only observation of a frame against its row. Never throws, never
  // mutates. Missing row → recorded as an unregistered frame (a real signal in
  // shadow mode: it means the catalogue is incomplete for this boundary).
  _observe(row, payload, meta) {
    if (!row) return { registered: false };
    const out = { registered: true, kind: row.kind, owningService: row.owningService, evidence: row.evidence, proves: row.proves };
    try { const s = row.schema(payload, meta); out.schemaOk = !!(s && s.ok); if (!out.schemaOk) out.schemaReason = s && s.reason; }
    catch (e) { out.schemaOk = null; out.schemaFault = errMsg(e); }
    if (row.correlation) {
      try { out.correlation = row.correlation(payload, meta) || null; }
      catch (e) { out.correlationFault = errMsg(e); }
    }
    if (row.idempotencyKey) {
      try { out.idempotencyKey = row.idempotencyKey(payload, meta) || null; }
      catch (e) { out.idempotencyFault = errMsg(e); }
    }
    return out;
  }

  _emit(row, type, variant, pre, post, durationMs) {
    let rec;
    try {
      rec = {
        boundary: this.boundary,
        type,
        variant: variant || (row && row.variant) || null,
        registered: pre.registered,
        kind: pre.kind ?? null,
        owningService: pre.owningService ?? null,
        evidence: pre.evidence ?? null,
        proves: pre.proves ?? null,
        schemaOk: pre.schemaOk ?? null,
        schemaReason: pre.schemaReason ?? pre.schemaFault ?? null,
        correlation: pre.correlation ?? null,
        correlationFault: pre.correlationFault ?? null,
        idempotencyKey: pre.idempotencyKey ?? null,
        verdict: post.verdict,
        error: post.error ?? null,
        durationMs: Number.isFinite(durationMs) ? durationMs : null,
      };
    } catch (e) { rec = { boundary: this.boundary, type, traceFault: errMsg(e) }; }
    try { this._sink(rec); } catch { /* a broken sink must never break dispatch */ }
  }

  _safeNow() { try { const n = this._now(); return Number.isFinite(n) ? n : 0; } catch { return 0; } }
}

function defaultNow() {
  try { if (typeof performance !== 'undefined' && typeof performance.now === 'function') return performance.now(); } catch { /* */ }
  return 0; // Date.now() deliberately avoided; duration is best-effort telemetry, never behavior
}

function errMsg(e) { try { return (e && e.message) ? String(e.message) : String(e); } catch { return 'unknown'; } }

// Normalize a handler return into a coarse verdict for the trace. Mirrors the
// pub/sub dispatch convention ('consumed' / {consumed:true} → consumed) without
// importing it, so the registry stays boundary-agnostic.
function verdictOf(r) {
  if (r === 'consumed') return 'consumed';
  if (r && typeof r === 'object' && r.consumed === true) return 'consumed';
  if (r === undefined || r === null || r === false) return 'passed';   // handler declined / forwarded
  return 'other';
}

export default ShadowRegistry;
