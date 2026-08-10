// registry/types.js — the per-boundary frame CONTRACT ROW (refactor Phase 1,
// REF-1.1; hardened per Aster S1 disposition items 2/3/4). One row describes one
// frame family on one trust boundary: what it is, what it proves, who owns it,
// how it correlates, and how to trace it. Rows are DECLARATIVE data — no handler
// business logic (§4.2 keeps that in the owning service). In Phase 1 rows run in
// SHADOW MODE only (§4.3): they validate and trace beside the existing handler
// and change no acceptance behavior. A row governs dispatch only after its
// family's own migration proof, in a later phase.
//
// Nothing here reads the wire or mutates a payload. schema/correlation/
// idempotencyKey are pure, READ-ONLY functions; the shadow dispatcher only ever
// calls them against an immutable snapshot (see shadowRegistry.js).

// Frame kind — the shape of the exchange. A REQUEST_RESPONSE implies a
// correlation contract; a proof-bearing ONE_WAY (D1 signed INGESTACK) also
// declares one; a bare MULTICAST/UNSOLICITED_EVENT (ROOTBEACON) may omit it.
export const FrameKind = Object.freeze({
  REQUEST_RESPONSE:  'REQUEST_RESPONSE',
  ONE_WAY:           'ONE_WAY',
  MULTICAST:         'MULTICAST',
  UNSOLICITED_EVENT: 'UNSOLICITED_EVENT',
});
const FRAME_KINDS = new Set(Object.values(FrameKind));

// The evidence hierarchy names (§4.3). CRITICAL (Aster item 4): these are
// ORTHOGONAL FACTS, not an ordinal scale. OBSERVED does not imply COMMITTED;
// ROUTED does not imply INGESTED. This module deliberately exposes NO ordinal
// comparison — any policy that relates two facts must use an explicit,
// reviewed implication, never `level >= other`.
export const EvidenceLevel = Object.freeze({
  ROUTED:    'ROUTED',     // a transport/routing hop accepted or forwarded it
  INGESTED:  'INGESTED',   // a profile-valid ingest endpoint accepted the exact event
  RETAINED:  'RETAINED',   // an authenticated named holder confirms it stores the entry
  COMMITTED: 'COMMITTED',  // holder receipts satisfy a named retention policy
  OBSERVED:  'OBSERVED',   // an app subscriber delivered the event exactly once
});
const EVIDENCE_LEVELS = new Set(Object.values(EvidenceLevel));

// Correlation subject — a tagged union (§4.3) so not every write profile is
// forced through rootId+rootEpoch. A row declares the subject SHAPE it produces.
export const CorrelationSubjectKind = Object.freeze({
  LegacyAuthorityRef: 'LegacyAuthorityRef', // singleton-root { nodeId, incarnation }
  IngressRef:         'IngressRef',
  HolderRef:          'HolderRef',
  AuthorLaneRef:      'AuthorLaneRef',
});
const CORRELATION_KINDS = new Set(Object.values(CorrelationSubjectKind));

export const Proves = Object.freeze({
  ROUTING: 'routing', INGESTION: 'ingestion', RETENTION: 'retention', OBSERVATION: 'observation',
});
const PROVES = new Set(Object.values(Proves));

// Brand: only rows minted by defineRow() may enter a registry (Aster item 2).
export const ROW_BRAND = Symbol.for('axona.registry.row.v1');
export const isRow = (x) => !!x && typeof x === 'object' && x[ROW_BRAND] === true;

const isFn = (x) => typeof x === 'function';
const isStr = (x) => typeof x === 'string' && x.length > 0;
const PASS_SCHEMA = () => ({ ok: true });
const fail = (type, msg) => { throw new TypeError(`defineRow(${type ?? '?'}): ${msg}`); };

// defineRow — validate a declarative row and return a FROZEN, BRANDED copy.
// Throws at construction on any malformed or incomplete row: a registry that
// accepts a half-declared contract is worse than none. Required: type, kind,
// owningService. Cross-field rules enforce contract completeness so S2 rows
// cannot be under-specified.
export function defineRow(row) {
  if (!row || typeof row !== 'object') throw new TypeError('defineRow: row object required');
  const { type, kind, owningService } = row;
  if (!isStr(type)) fail(type, 'type (frame type string) required');
  if (!FRAME_KINDS.has(kind)) fail(type, `invalid frame kind ${String(kind)}`);
  if (!isStr(owningService)) fail(type, 'owningService (§4.9) required');
  if (row.variant != null && !isStr(row.variant)) fail(type, 'variant must be a non-empty string or null');

  if (row.schema != null && !isFn(row.schema)) fail(type, 'schema must be a function or null');
  if (row.correlation != null && !isFn(row.correlation)) fail(type, 'correlation must be a function or null');
  if (row.idempotencyKey != null && !isFn(row.idempotencyKey)) fail(type, 'idempotencyKey must be a function or null');
  if (row.evidence != null && !EVIDENCE_LEVELS.has(row.evidence)) fail(type, `invalid evidence ${String(row.evidence)}`);
  if (row.proves != null && !PROVES.has(row.proves)) fail(type, `invalid proves ${String(row.proves)}`);
  if (row.subjectShape != null && !CORRELATION_KINDS.has(row.subjectShape)) fail(type, `invalid subjectShape ${String(row.subjectShape)}`);

  // Contract-completeness cross-rules (Aster item 3).
  const hasCorr = row.correlation != null;
  if (kind === FrameKind.REQUEST_RESPONSE && !hasCorr) fail(type, 'REQUEST_RESPONSE requires a correlation contract');
  if (hasCorr) {
    if (!Array.isArray(row.correlationFields) || row.correlationFields.length === 0) {
      fail(type, 'a correlated row must declare non-empty correlationFields');
    }
    if (!CORRELATION_KINDS.has(row.subjectShape)) fail(type, 'a correlated row must declare subjectShape');
  }
  if (row.evidence === EvidenceLevel.COMMITTED && !isStr(row.producedPolicy)) {
    fail(type, 'COMMITTED evidence requires a producedPolicy (a commitment carries a named policy, never a raw count)');
  }

  const norm = {
    [ROW_BRAND]: true,
    type,
    variant:        row.variant ?? null,
    versionRange:   Object.freeze({ min: (row.versionRange?.min ?? 4), max: (row.versionRange?.max ?? 4) }),
    kind,
    owningService,

    // guards — every row declares its stance explicitly (default 'none', never undefined).
    authGuard:      row.authGuard ?? 'none',
    admissionGuard: row.admissionGuard ?? 'none',
    placementGuard: row.placementGuard ?? 'none',

    // profile / addressing / ordering metadata.
    topicProfile:    row.topicProfile ?? null,     // e.g. 'LEGACY_ROOT_V4'
    eventIdScheme:   row.eventIdScheme ?? null,     // e.g. 'msgId' (legacy adapter) | 'eventId'
    replayCursorType:row.replayCursorType ?? null,  // e.g. 'stamp' | 'seq' | null
    orderingModel:   row.orderingModel ?? null,     // e.g. 'per-topic-stamp' | 'none'

    // pure READ-ONLY callbacks (invoked only against an immutable snapshot).
    schema:         row.schema ?? PASS_SCHEMA,      // (snap) -> {ok, reason?}
    correlation:    row.correlation ?? null,        // (snap) -> subject | null
    idempotencyKey: row.idempotencyKey ?? null,     // (snap) -> string | null

    // correlation contract detail.
    correlationFields: Object.freeze(row.correlationFields ? [...row.correlationFields] : []),
    subjectShape:      row.subjectShape ?? null,    // CorrelationSubjectKind | null

    // evidence / policy / proof — orthogonal facts (no ordinal semantics).
    evidence:       row.evidence ?? null,
    producedPolicy: row.producedPolicy ?? null,
    requiredPolicy: row.requiredPolicy ?? null,
    proves:         row.proves ?? null,
    outcome:        row.outcome ?? null,
    terminalOutcome:row.terminalOutcome ?? null,

    errorContract:  Object.freeze(row.errorContract ? [...row.errorContract] : []),
    traceFields:    Object.freeze(row.traceFields ? [...row.traceFields] : []),
    budget:         Object.freeze({ maxBytes: row.budget?.maxBytes ?? null, maxWork: row.budget?.maxWork ?? null }),
    capabilityRange:Object.freeze(row.capabilityRange ? { ...row.capabilityRange } : {}),
    note:           row.note ?? '',
  };
  return Object.freeze(norm);
}

export default defineRow;
