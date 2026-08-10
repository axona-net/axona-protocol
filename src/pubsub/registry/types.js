// registry/types.js — the per-boundary frame CONTRACT ROW (refactor Phase 1,
// REF-1.1; re-cut for S1c per Aster's S1b disposition items 4/6). One row
// describes one frame family on one trust boundary. Rows are DECLARATIVE data —
// no handler business logic (§4.2). Phase 1 runs rows in SHADOW MODE only (§4.3):
// validate + trace beside the handler, change no acceptance behavior.
//
// Two hardening properties live here:
//   - Rows are BRANDED via a module-private WeakSet (isRow), so a hand-rolled
//     object carrying a public marker cannot be registered (Aster S1b #4).
//   - defineRow STRICTLY validates every field — integer version bounds and
//     ordering, string guard/profile/cursor/ordering types, unique correlation
//     fields, string arrays, positive bounded budgets — and rejects at
//     construction, so no under-specified or malformed row reaches a registry
//     (Aster S1b #6).
//
// Observation reads ONLY a bounded, declared PROJECTION of the frame (the
// `projection` field lists the payload/meta keys a row's callbacks may see);
// the shadow dispatcher builds that projection reference-disjoint and frozen, so
// callbacks can neither see the whole payload nor mutate live dispatch input.

export const FrameKind = Object.freeze({
  REQUEST_RESPONSE:  'REQUEST_RESPONSE',
  ONE_WAY:           'ONE_WAY',
  MULTICAST:         'MULTICAST',
  UNSOLICITED_EVENT: 'UNSOLICITED_EVENT',
});
const FRAME_KINDS = new Set(Object.values(FrameKind));

// Evidence levels are ORTHOGONAL FACT LABELS, not an ordinal scale (Aster #4):
// no comparison operator exists here; a policy relating two facts must use an
// explicit, reviewed implication, never `level >= other`.
export const EvidenceLevel = Object.freeze({
  ROUTED: 'ROUTED', INGESTED: 'INGESTED', RETAINED: 'RETAINED', COMMITTED: 'COMMITTED', OBSERVED: 'OBSERVED',
});
const EVIDENCE_LEVELS = new Set(Object.values(EvidenceLevel));

export const CorrelationSubjectKind = Object.freeze({
  LegacyAuthorityRef: 'LegacyAuthorityRef', IngressRef: 'IngressRef', HolderRef: 'HolderRef', AuthorLaneRef: 'AuthorLaneRef',
});
const CORRELATION_KINDS = new Set(Object.values(CorrelationSubjectKind));

export const Proves = Object.freeze({ ROUTING: 'routing', INGESTION: 'ingestion', RETENTION: 'retention', OBSERVATION: 'observation' });
const PROVES = new Set(Object.values(Proves));

// Non-forgeable brand: a module-private WeakSet of rows minted by defineRow.
const _minted = new WeakSet();
export const isRow = (x) => { try { return _minted.has(x); } catch { return false; } };

const MAX_FIELD = 64;
const isFn = (x) => typeof x === 'function';
const isStr = (x) => typeof x === 'string' && x.length > 0 && x.length <= 256;
const isBoundedStr = (x) => typeof x === 'string' && x.length > 0 && x.length <= MAX_FIELD;
const isPosInt = (x) => Number.isInteger(x) && x > 0;
const fail = (type, msg) => { throw new TypeError(`defineRow(${type ?? '?'}): ${msg}`); };

function validStrArray(type, name, arr, { unique = false } = {}) {
  if (!Array.isArray(arr)) fail(type, `${name} must be an array`);
  for (const s of arr) if (!isBoundedStr(s)) fail(type, `${name} entries must be non-empty strings <= ${MAX_FIELD} chars`);
  if (unique && new Set(arr).size !== arr.length) fail(type, `${name} entries must be unique`);
  return Object.freeze([...arr]);
}

export function defineRow(row) {
  if (!row || typeof row !== 'object') throw new TypeError('defineRow: row object required');
  const { type, kind, owningService } = row;
  if (!isStr(type)) fail(type, 'type (frame type string) required');
  if (!FRAME_KINDS.has(kind)) fail(type, `invalid frame kind ${String(kind)}`);
  if (!isStr(owningService)) fail(type, 'owningService (§4.9) required');
  if (row.variant != null && !isBoundedStr(row.variant)) fail(type, 'variant must be a bounded non-empty string or null');

  // version range — REQUIRED, integer, ordered (Aster #6).
  const vr = row.versionRange;
  if (!vr || !Number.isInteger(vr.min) || !Number.isInteger(vr.max) || vr.min < 1 || vr.max < vr.min) {
    fail(type, 'versionRange { min, max } required with integer min>=1 and max>=min');
  }

  // guards + profile/addressing metadata — string-typed when present.
  for (const g of ['authGuard', 'admissionGuard', 'placementGuard']) {
    if (row[g] != null && !isStr(row[g])) fail(type, `${g} must be a string`);
  }
  for (const p of ['topicProfile', 'eventIdScheme', 'replayCursorType', 'orderingModel', 'producedPolicy', 'requiredPolicy', 'outcome', 'terminalOutcome']) {
    if (row[p] != null && !isStr(row[p])) fail(type, `${p} must be a string or null`);
  }

  if (row.schema != null && !isFn(row.schema)) fail(type, 'schema must be a function or null');
  if (row.correlation != null && !isFn(row.correlation)) fail(type, 'correlation must be a function or null');
  if (row.idempotencyKey != null && !isFn(row.idempotencyKey)) fail(type, 'idempotencyKey must be a function or null');
  if (row.evidence != null && !EVIDENCE_LEVELS.has(row.evidence)) fail(type, `invalid evidence ${String(row.evidence)}`);
  if (row.proves != null && !PROVES.has(row.proves)) fail(type, `invalid proves ${String(row.proves)}`);
  if (row.subjectShape != null && !CORRELATION_KINDS.has(row.subjectShape)) fail(type, `invalid subjectShape ${String(row.subjectShape)}`);

  // projection allowlist — the ONLY frame keys observation may read.
  const proj = row.projection ?? {};
  if (typeof proj !== 'object') fail(type, 'projection must be an object');
  const projection = Object.freeze({
    payload: validStrArray(type, 'projection.payload', proj.payload ?? [], { unique: true }),
    meta:    validStrArray(type, 'projection.meta', proj.meta ?? [], { unique: true }),
  });

  const errorContract = validStrArray(type, 'errorContract', row.errorContract ?? []);
  const traceFields = validStrArray(type, 'traceFields', row.traceFields ?? []);

  // budgets — positive integers or null (Aster #6).
  const b = row.budget ?? {};
  if (b.maxBytes != null && !isPosInt(b.maxBytes)) fail(type, 'budget.maxBytes must be a positive integer or null');
  if (b.maxWork != null && !isPosInt(b.maxWork)) fail(type, 'budget.maxWork must be a positive integer or null');

  // capability range — values must be string|number|null.
  const cap = row.capabilityRange ?? {};
  if (typeof cap !== 'object') fail(type, 'capabilityRange must be an object');
  for (const k of Object.keys(cap)) { const v = cap[k]; if (v != null && typeof v !== 'string' && typeof v !== 'number') fail(type, `capabilityRange.${k} must be string|number|null`); }

  // cross-field completeness (Aster S1 #3 / S1b #6).
  const hasCorr = row.correlation != null;
  if (kind === FrameKind.REQUEST_RESPONSE && !hasCorr) fail(type, 'REQUEST_RESPONSE requires a correlation contract');
  let correlationFields = Object.freeze([]);
  if (hasCorr) {
    correlationFields = validStrArray(type, 'correlationFields', row.correlationFields ?? [], { unique: true });
    if (correlationFields.length === 0) fail(type, 'a correlated row must declare non-empty correlationFields');
    if (!CORRELATION_KINDS.has(row.subjectShape)) fail(type, 'a correlated row must declare subjectShape');
  } else if (row.correlationFields != null) {
    correlationFields = validStrArray(type, 'correlationFields', row.correlationFields, { unique: true });
  }
  if (row.evidence === EvidenceLevel.COMMITTED && !isStr(row.producedPolicy)) {
    fail(type, 'COMMITTED evidence requires a producedPolicy (a commitment carries a named policy, never a raw count)');
  }

  const norm = {
    type,
    variant:        row.variant ?? null,
    versionRange:   Object.freeze({ min: vr.min, max: vr.max }),
    kind,
    owningService,
    authGuard:      row.authGuard ?? 'none',
    admissionGuard: row.admissionGuard ?? 'none',
    placementGuard: row.placementGuard ?? 'none',
    topicProfile:    row.topicProfile ?? null,
    eventIdScheme:   row.eventIdScheme ?? null,
    replayCursorType:row.replayCursorType ?? null,
    orderingModel:   row.orderingModel ?? null,
    projection,
    schema:         row.schema ?? (() => ({ ok: true })),
    correlation:    row.correlation ?? null,
    idempotencyKey: row.idempotencyKey ?? null,
    correlationFields,
    subjectShape:   row.subjectShape ?? null,
    evidence:       row.evidence ?? null,
    producedPolicy: row.producedPolicy ?? null,
    requiredPolicy: row.requiredPolicy ?? null,
    proves:         row.proves ?? null,
    outcome:        row.outcome ?? null,
    terminalOutcome:row.terminalOutcome ?? null,
    errorContract,
    traceFields,
    budget:         Object.freeze({ maxBytes: b.maxBytes ?? null, maxWork: b.maxWork ?? null }),
    capabilityRange:Object.freeze({ ...cap }),
    note:           isStr(row.note) ? row.note : '',
  };
  Object.freeze(norm);
  _minted.add(norm);
  return norm;
}

export default defineRow;
