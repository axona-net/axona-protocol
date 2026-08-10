// registry/types.js — the per-boundary frame CONTRACT ROW (refactor Phase 1,
// REF-1.1). One row describes one frame family on one trust boundary: what it
// is, what it proves, who owns it, and how to trace it. Rows are DECLARATIVE
// data — they carry no handler business logic (§4.2 keeps that in the owning
// service). In Phase 1 rows run in SHADOW MODE only: they validate and trace
// beside the existing handler and change no acceptance behavior (§4.3). A row
// governs dispatch only after its family's own migration proof, in a later
// phase — "catalogued is not enforcement-ready."
//
// Nothing here reads the wire or mutates a payload. `schema`/`correlation` are
// pure, read-only functions supplied by the boundary's row table.

// Frame kind — the shape of the exchange. A correlation contract is required
// ONLY where the kind implies one; a ROOTBEACON (UNSOLICITED_EVENT) has no
// opposite and registers without one (§4.3).
export const FrameKind = Object.freeze({
  REQUEST_RESPONSE: 'REQUEST_RESPONSE',
  ONE_WAY:          'ONE_WAY',
  MULTICAST:        'MULTICAST',
  UNSOLICITED_EVENT:'UNSOLICITED_EVENT',
});
const FRAME_KINDS = new Set(Object.values(FrameKind));

// The evidence hierarchy (§4.3). These are NOT interchangeable — the #28
// incident proved a routing verdict is not retention and self-delivery is not
// durability. A row names the STRONGEST fact its frame can establish, or null
// for frames that carry no evidence (e.g. a bare ROOTBEACON announcement).
export const EvidenceLevel = Object.freeze({
  ROUTED:    'ROUTED',     // a transport/routing hop accepted or forwarded it
  INGESTED:  'INGESTED',   // a profile-valid ingest endpoint accepted the exact event
  RETAINED:  'RETAINED',   // an authenticated named holder confirms it stores the entry
  COMMITTED: 'COMMITTED',  // holder receipts satisfy a named retention policy
  OBSERVED:  'OBSERVED',   // an app subscriber delivered the event exactly once
});
const EVIDENCE_LEVELS = new Set(Object.values(EvidenceLevel));

// The correlation subject is a tagged union (§4.3) so that not every write
// profile is forced through rootId + rootEpoch. A row's `correlation` fn, when
// present, returns one of these tags (or null when the frame instance carries
// no subject).
export const CorrelationSubjectKind = Object.freeze({
  LegacyAuthorityRef: 'LegacyAuthorityRef', // singleton-root { nodeId, incarnation }
  IngressRef:         'IngressRef',
  HolderRef:          'HolderRef',
  AuthorLaneRef:      'AuthorLaneRef',
});
const CORRELATION_KINDS = new Set(Object.values(CorrelationSubjectKind));

// What a frame PROVES, orthogonal to its evidence level's strength — used to
// keep "this frame is routing evidence" distinct from "this frame is an ingest
// proof" (the D1 lesson: the last hop is only routing).
export const Proves = Object.freeze({
  ROUTING:     'routing',
  INGESTION:   'ingestion',
  RETENTION:   'retention',
  OBSERVATION: 'observation',
});
const PROVES = new Set(Object.values(Proves));

const isFn = (x) => typeof x === 'function';
const PASS_SCHEMA = () => ({ ok: true });

// defineRow — validate a declarative row and return a FROZEN, normalized copy.
// Throws at construction on a malformed row: a registry that silently accepts a
// half-declared contract is worse than none (same principle as the test
// manifest guard). Required: type, kind, owningService. Everything else has a
// safe default so a minimal row is legal and a rich row is fully described.
export function defineRow(row) {
  if (!row || typeof row !== 'object') throw new TypeError('defineRow: row object required');
  const { type, kind, owningService } = row;
  if (typeof type !== 'string' || !type) throw new TypeError('defineRow: `type` (frame type string) required');
  if (!FRAME_KINDS.has(kind)) throw new TypeError(`defineRow(${type}): invalid frame kind ${String(kind)}`);
  if (typeof owningService !== 'string' || !owningService) throw new TypeError(`defineRow(${type}): owningService (§4.9) required`);

  // A REQUEST_RESPONSE or a proof-bearing ONE_WAY implies a correlation
  // contract; MULTICAST / UNSOLICITED_EVENT may omit it. We don't force one
  // (a ONE_WAY announcement legitimately has none), but a supplied correlation
  // must be callable.
  if (row.correlation != null && !isFn(row.correlation)) {
    throw new TypeError(`defineRow(${type}): correlation must be a function or null`);
  }
  if (row.schema != null && !isFn(row.schema)) {
    throw new TypeError(`defineRow(${type}): schema must be a function or null`);
  }
  if (row.evidence != null && !EVIDENCE_LEVELS.has(row.evidence)) {
    throw new TypeError(`defineRow(${type}): invalid evidence level ${String(row.evidence)}`);
  }
  if (row.proves != null && !PROVES.has(row.proves)) {
    throw new TypeError(`defineRow(${type}): invalid proves ${String(row.proves)}`);
  }

  const norm = {
    type,
    variant:        row.variant ?? null,            // e.g. 'signed' | 'legacy-unsigned' for INGESTACK
    versionRange:   Object.freeze(row.versionRange ?? { min: 4, max: 4 }),
    kind,
    owningService,                                  // §4.9 owner
    schema:         row.schema ?? PASS_SCHEMA,      // (payload, meta) -> {ok, reason?}  READ-ONLY
    correlation:    row.correlation ?? null,        // (payload, meta) -> {kind, ...} | null  READ-ONLY
    evidence:       row.evidence ?? null,           // EvidenceLevel | null
    producedPolicy: row.producedPolicy ?? null,     // policy this frame can produce (COMMITTED etc.)
    requiredPolicy: row.requiredPolicy ?? null,     // policy this frame requires to be accepted
    outcome:        row.outcome ?? null,            // normalized outcome type name
    proves:         row.proves ?? null,             // Proves.* | null
    errorContract:  Object.freeze(row.errorContract ? [...row.errorContract] : []),
    traceFields:    Object.freeze(row.traceFields ? [...row.traceFields] : []),
    idempotencyKey: row.idempotencyKey ?? null,     // (payload, meta) -> string | null  READ-ONLY
    budget:         Object.freeze(row.budget ?? {}),// { maxBytes?, maxWork? }
    terminalOutcome:row.terminalOutcome ?? null,    // terminal negative outcome name
    capabilityRange:Object.freeze(row.capabilityRange ?? {}),
    note:           row.note ?? '',
  };
  return Object.freeze(norm);
}

export default defineRow;
