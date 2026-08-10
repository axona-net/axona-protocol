// registry/types.js — the per-boundary frame CONTRACT ROW (refactor Phase 1,
// REF-1.1; S1d re-cut around a DECLARATIVE, side-effect-free observation
// boundary per Aster's S1c disposition). A row is declarative data — no handler
// business logic (§4.2). Phase 1 runs rows in SHADOW MODE only (§4.3).
//
// The observation boundary is declarative by construction (S1d):
//   - `projection` names dotted LEAF PATHS the shadow layer may read from a
//     frame. The dispatcher walks them reading ONLY own DATA-property
//     descriptors (accessors are never invoked); scalars come back EXACT within
//     the declared byte budget (oversized → a budget fault, never a truncated
//     value treated as canonical), and arrays / byte sequences / objects come
//     back as bounded STRUCTURAL FACTS. Nothing dynamic is executed on live
//     dispatch input.
//   - `variantBy` is a DECLARATIVE discriminator over the projection (a path +
//     presence/case map), never a function on live args.
//   - Rows are branded via a module-private WeakSet (isRow); register accepts
//     nothing else.
//
// defineRow REJECTS (never silently normalizes) malformed rows: over-limit
// projection lists, non-object budget/capability/projection, oversized or
// non-finite capability values, non-string notes, evidence/proof
// contradictions, and correlationFields not answerable from the projection.

export const FrameKind = Object.freeze({
  REQUEST_RESPONSE: 'REQUEST_RESPONSE', ONE_WAY: 'ONE_WAY', MULTICAST: 'MULTICAST', UNSOLICITED_EVENT: 'UNSOLICITED_EVENT',
});
const FRAME_KINDS = new Set(Object.values(FrameKind));

// Orthogonal fact labels — NOT an ordinal scale (Aster). No comparison exists.
export const EvidenceLevel = Object.freeze({
  ROUTED: 'ROUTED', INGESTED: 'INGESTED', RETAINED: 'RETAINED', COMMITTED: 'COMMITTED', OBSERVED: 'OBSERVED',
});
const EVIDENCE_LEVELS = new Set(Object.values(EvidenceLevel));

export const Proves = Object.freeze({ ROUTING: 'routing', INGESTION: 'ingestion', RETENTION: 'retention', OBSERVATION: 'observation' });
const PROVES = new Set(Object.values(Proves));

// The ONLY evidence↔proof pairings that are not a contradiction. A frame may
// declare one, the other, or a consistent pair; an inconsistent pair is rejected
// (e.g. OBSERVED paired with proves:routing).
const EVIDENCE_FOR_PROOF = Object.freeze({
  routing: new Set(['ROUTED']),
  ingestion: new Set(['INGESTED']),
  retention: new Set(['RETAINED', 'COMMITTED']),
  observation: new Set(['OBSERVED']),
});

export const CorrelationSubjectKind = Object.freeze({
  LegacyAuthorityRef: 'LegacyAuthorityRef', IngressRef: 'IngressRef', HolderRef: 'HolderRef', AuthorLaneRef: 'AuthorLaneRef',
});
const CORRELATION_KINDS = new Set(Object.values(CorrelationSubjectKind));

const _minted = new WeakSet();
export const isRow = (x) => { try { return _minted.has(x); } catch { return false; } };

export const MAX_PROJECTION_FIELDS = 24;
export const MAX_PATH = 96;
export const MAX_NOTE = 500;
export const MAX_CAP_STR = 256;
const isFn = (x) => typeof x === 'function';
const isStr = (x) => typeof x === 'string' && x.length > 0 && x.length <= 256;
const isBoundedStr = (x, m) => typeof x === 'string' && x.length > 0 && x.length <= m;
const isPosInt = (x) => Number.isInteger(x) && x > 0;
const isPlainObject = (x) => x != null && typeof x === 'object' && !Array.isArray(x) &&
  (Object.getPrototypeOf(x) === Object.prototype || Object.getPrototypeOf(x) === null);
const fail = (type, msg) => { throw new TypeError(`defineRow(${type ?? '?'}): ${msg}`); };

function validPaths(type, name, arr) {
  if (!Array.isArray(arr)) fail(type, `${name} must be an array`);
  for (const s of arr) if (!isBoundedStr(s, MAX_PATH)) fail(type, `${name} entries must be non-empty strings <= ${MAX_PATH} chars`);
  if (new Set(arr).size !== arr.length) fail(type, `${name} entries must be unique`);
  return Object.freeze([...arr]);
}

export function defineRow(row) {
  if (!isPlainObject(row)) throw new TypeError('defineRow: a plain row object required');
  const { type, kind, owningService } = row;
  if (!isStr(type)) fail(type, 'type required');
  if (!FRAME_KINDS.has(kind)) fail(type, `invalid frame kind ${String(kind)}`);
  if (!isStr(owningService)) fail(type, 'owningService (§4.9) required');
  if (row.variant != null && !isBoundedStr(row.variant, 64)) fail(type, 'variant must be a bounded string or null');

  const vr = row.versionRange;
  if (!isPlainObject(vr) || !Number.isInteger(vr.min) || !Number.isInteger(vr.max) || vr.min < 1 || vr.max < vr.min) {
    fail(type, 'versionRange { min, max } required with integer min>=1 and max>=min');
  }
  for (const g of ['authGuard', 'admissionGuard', 'placementGuard']) if (row[g] != null && !isStr(row[g])) fail(type, `${g} must be a string`);
  for (const p of ['topicProfile', 'eventIdScheme', 'replayCursorType', 'orderingModel', 'producedPolicy', 'requiredPolicy', 'outcome', 'terminalOutcome']) {
    if (row[p] != null && !isStr(row[p])) fail(type, `${p} must be a string or null`);
  }
  if (row.schema != null && !isFn(row.schema)) fail(type, 'schema must be a function or null');
  if (row.correlation != null && !isFn(row.correlation)) fail(type, 'correlation must be a function or null');
  if (row.idempotencyKey != null && !isFn(row.idempotencyKey)) fail(type, 'idempotencyKey must be a function or null');
  if (row.evidence != null && !EVIDENCE_LEVELS.has(row.evidence)) fail(type, `invalid evidence ${String(row.evidence)}`);
  if (row.proves != null && !PROVES.has(row.proves)) fail(type, `invalid proves ${String(row.proves)}`);
  if (row.subjectShape != null && !CORRELATION_KINDS.has(row.subjectShape)) fail(type, `invalid subjectShape ${String(row.subjectShape)}`);

  // evidence/proof consistency (Aster S1c #5).
  if (row.evidence != null && row.proves != null && !EVIDENCE_FOR_PROOF[row.proves].has(row.evidence)) {
    fail(type, `evidence ${row.evidence} contradicts proves ${row.proves}`);
  }

  // projection — plain object, path arrays, HARD cap (reject, never truncate).
  const proj = row.projection ?? {};
  if (!isPlainObject(proj)) fail(type, 'projection must be a plain object');
  const pPay = validPaths(type, 'projection.payload', proj.payload ?? []);
  const pMeta = validPaths(type, 'projection.meta', proj.meta ?? []);
  if (pPay.length + pMeta.length > MAX_PROJECTION_FIELDS) fail(type, `projection exceeds ${MAX_PROJECTION_FIELDS} fields (declare fewer; the runtime rejects, it does not truncate)`);
  const projection = Object.freeze({ payload: pPay, meta: pMeta });
  const projSet = new Set([...pPay, ...pMeta]);

  const errorContract = validPaths(type, 'errorContract', row.errorContract ?? []);
  const traceFields = validPaths(type, 'traceFields', row.traceFields ?? []);

  // budget — plain object, positive ints; maxBytes is the per-scalar byte cap.
  const b = row.budget ?? {};
  if (!isPlainObject(b)) fail(type, 'budget must be a plain object');
  if (b.maxBytes != null && !isPosInt(b.maxBytes)) fail(type, 'budget.maxBytes must be a positive integer or null');
  if (b.maxWork != null && !isPosInt(b.maxWork)) fail(type, 'budget.maxWork must be a positive integer or null');

  // capability range — plain object, finite numbers / bounded strings only.
  const cap = row.capabilityRange ?? {};
  if (!isPlainObject(cap)) fail(type, 'capabilityRange must be a plain object');
  for (const k of Object.keys(cap)) {
    const v = cap[k];
    if (v == null) continue;
    if (typeof v === 'number') { if (!Number.isFinite(v)) fail(type, `capabilityRange.${k} must be finite`); }
    else if (typeof v === 'string') { if (v.length > MAX_CAP_STR) fail(type, `capabilityRange.${k} exceeds ${MAX_CAP_STR} chars`); }
    else fail(type, `capabilityRange.${k} must be a finite number, bounded string, or null`);
  }

  if (row.note != null && !isBoundedStr(row.note, MAX_NOTE)) fail(type, `note must be a string <= ${MAX_NOTE} chars`);

  const hasCorr = row.correlation != null;
  if (kind === FrameKind.REQUEST_RESPONSE && !hasCorr) fail(type, 'REQUEST_RESPONSE requires a correlation contract');
  let correlationFields = Object.freeze([]);
  if (hasCorr) {
    correlationFields = validPaths(type, 'correlationFields', row.correlationFields ?? []);
    if (correlationFields.length === 0) fail(type, 'a correlated row must declare non-empty correlationFields');
    if (!CORRELATION_KINDS.has(row.subjectShape)) fail(type, 'a correlated row must declare subjectShape');
    // correlationFields must be answerable from the projection (Aster S1c #5).
    for (const f of correlationFields) if (!projSet.has(f)) fail(type, `correlationField ${f} is not in the declared projection`);
  } else if (row.correlationFields != null) {
    correlationFields = validPaths(type, 'correlationFields', row.correlationFields);
  }
  if (row.evidence === EvidenceLevel.COMMITTED && !isStr(row.producedPolicy)) {
    fail(type, 'COMMITTED evidence requires a producedPolicy');
  }

  const norm = {
    type, variant: row.variant ?? null,
    versionRange: Object.freeze({ min: vr.min, max: vr.max }),
    kind, owningService,
    authGuard: row.authGuard ?? 'none', admissionGuard: row.admissionGuard ?? 'none', placementGuard: row.placementGuard ?? 'none',
    topicProfile: row.topicProfile ?? null, eventIdScheme: row.eventIdScheme ?? null,
    replayCursorType: row.replayCursorType ?? null, orderingModel: row.orderingModel ?? null,
    projection,
    schema: row.schema ?? (() => ({ ok: true })),
    correlation: row.correlation ?? null,
    idempotencyKey: row.idempotencyKey ?? null,
    correlationFields, subjectShape: row.subjectShape ?? null,
    evidence: row.evidence ?? null, producedPolicy: row.producedPolicy ?? null, requiredPolicy: row.requiredPolicy ?? null,
    proves: row.proves ?? null, outcome: row.outcome ?? null, terminalOutcome: row.terminalOutcome ?? null,
    errorContract, traceFields,
    budget: Object.freeze({ maxBytes: b.maxBytes ?? null, maxWork: b.maxWork ?? null }),
    capabilityRange: Object.freeze({ ...cap }),
    note: row.note ?? '',
  };
  Object.freeze(norm);
  _minted.add(norm);
  return norm;
}

export default defineRow;
