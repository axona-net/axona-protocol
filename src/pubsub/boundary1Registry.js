// boundary1Registry.js — REF-1.1 S2 (recut-2): the Boundary-1 (pub/sub + DHT
// control) frame-contract registry TABLE, plus the wiring map that shadow-wraps
// the 19 routed handlers registered in wireHandlers.js `_registerHandlers`.
//
// SHADOW MODE ONLY. This module changes NO acceptance behavior. The registry is
// built ONLY when AxonaManager is constructed with `frameRegistry:true`; the wrap
// runs the handler verbatim whenever the runtime shadow flag is off (the default),
// so flag-off is byte-identical. Flag-on OBSERVES a decoder-certified snapshot
// beside each handler and emits a trace — never mutating, suppressing, or
// reordering a handler or its arguments. Dispatch is NOT migrated (§4.3).
//
// MODELING (grounded in axona-docs code-refactor-plan §4.3 + Refactor-Phase0-
// Inventory §1 + Refactor-Phase0-OwnershipMap §2; catalog cross-checked against
// the live handler bodies). Recut-2 addresses Aster's S2/S3 review (dde2562):
//   * F2 — the Phase-1 row contract is completed: authentication / admission /
//     placement guards, error contract, and trace fields are NAMED per frame
//     (§4.3), not left at `none`. PUB/KILL name envelope/kill authentication,
//     freshness, topic-binding, and write-policy; signed INGESTACK names
//     ackProof.js + the flight/proof binding under LEGACY_ROOT_V4; legacy
//     INGESTACK names adjacent-sender + incarnation; placement/admission frames
//     name their region/role/admission guards.
//   * F3 — correlation separates a CONVERSATION (a request/response pairing keyed
//     by a conversation id — corrId, parentId, from) from the AUTHORITY subject
//     union. Read/catch-up pairs (PULL↔PULLRESP, PULLUP↔REPLAYUP, HANDOFF↔
//     HANDOFFACK) declare `conversation`, NOT an authority subject. Writes are an
//     IngressRef (the ingress attempt — topicId alone is NOT a LegacyAuthorityRef);
//     the LegacyAuthorityRef is claimed only by INGESTACK, whose signed variant
//     binds op/attemptId/ackTo/flightNonce/rootPub, not just topicId/msgId. An
//     unsigned `from`/`parent` is a routing hint, so REPLICATE (a cohort spray)
//     claims no authenticated holder subject.
//   * F4 — INGESTACK variant selection mirrors the handler exactly: `signed` only
//     when `typeof payload.sig === 'string'` (variantBy.valueType:'string'); a
//     present-but-non-string sig selects `legacy`, as the handler does.
//   * F5 — PUB idempotency is UNDECLARED: production dedups on the envelope msgId,
//     which hashes author+message (excludes ts/seq/topic/sig) and lives INSIDE the
//     signed `json`, so it is not an accurately-observable top-level frame key at
//     Boundary-1. KILL keeps `kill.msgId` (the projected tombstone key).
//   * CAP_ATTEST is NOT here (transport/auth boundary). UNPUB is retired.
//   * No new wire fields. Rows describe the EXISTING frames.

import { T } from './constants.js';
import {
  defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind,
  ShadowRegistry,
} from '../registry/index.js';

const V = { min: 4, max: 4 };                 // Kernel-4 wire version
const LAR = CorrelationSubjectKind.LegacyAuthorityRef;
const INGRESS = CorrelationSubjectKind.IngressRef;

// The Boundary-1 row DEFINITIONS. `wire` (a routed-message T.* string) is carried
// on the def so the wiring map derives from these, not from the minted rows
// (defineRow drops unknown keys). Single source of truth for both registration
// and handler wrapping.
function rowDefs() {
  return [
    // ── pub/sub control: subscription lease (TopicDeliveryPlane) ──
    ({
      type: 'pubsub:sub', wire: T.SUB, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      placementGuard: 'regionOk+admitRole+meshBareSelfRootGuard',
      projection: { payload: ['topicId', 'subscriberId', 'since', 'latest', 'hw', 'lw'] },
      schema: { require: ['topicId', 'subscriberId'], types: { topicId: 'string', subscriberId: 'string' } },
      idempotency: { from: ['topicId', 'subscriberId'] },   // a renewal is idempotent
      errorContract: ['refuse-region', 'refuse-role-budget'],
      traceFields: ['topicId', 'subscriberId'],
      note: 'establishes/renews a delivery lease routed toward topicId; no authority subject',
    }),
    ({
      type: 'pubsub:unsub', wire: T.UNSUB, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'subscriberId'] },
      schema: { require: ['topicId', 'subscriberId'], types: { topicId: 'string', subscriberId: 'string' } },
      idempotency: { from: ['topicId', 'subscriberId'] },
      traceFields: ['topicId', 'subscriberId'],
    }),

    // ── write ingress: PUB / KILL are an ingress attempt (IngressRef), NOT an
    //    authority reference — the authority is proven by the returned INGESTACK.
    ({
      type: 'pubsub:pub', wire: T.PUB, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      authGuard: 'verifyEnvelope',                                   // B-4: signature + msgId
      admissionGuard: 'checkFreshness+writePolicy+topicBinding',     // C-2 freshness, owner-write, derive===role.topicId
      placementGuard: 'regionOk+admitRole',
      projection: { payload: ['topicId', 'json', 'via', 'ackTo', 'attemptId', 'flightNonce'] },
      schema: { require: ['topicId', 'json'], types: { topicId: 'string', json: 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId'] },
      // F5: NO idempotency key — production dedups on the envelope msgId, which is
      // inside the signed `json`, not an observable top-level frame field here.
      errorContract: ['drop-unparseable', 'drop-bad-envelope', 'drop-stale', 'drop-topic-mismatch', 'drop-write-policy'],
      traceFields: ['topicId'],
      note: 'signed envelope routed to the root; ingest proof returns as a separate INGESTACK',
    }),
    ({
      type: 'pubsub:kill', wire: T.KILL, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      authGuard: 'verifyKill',                                       // signed kill object
      admissionGuard: 'authorship+topicBinding',                     // signer===author, kill.topicId===role.topicId
      projection: { payload: ['topicId', 'kill.msgId', 'kill.signerPubkey', 'via', 'ackTo', 'attemptId', 'flightNonce'] },
      schema: { require: ['topicId', 'kill.msgId'], types: { topicId: 'string', 'kill.msgId': 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId'] },
      idempotency: { from: ['kill.msgId'] },                          // tombstone keyed by the target msgId
      errorContract: ['drop-bad-kill-sig', 'drop-authorship-mismatch'],
      traceFields: ['topicId', 'kill.msgId'],
    }),

    // ── ingest proof: D1 signed + legacy unsigned variants ──
    ({
      type: 'pubsub:ingestack', variant: 'signed', wire: T.INGESTACK, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      authGuard: 'verifyAckProof',
      projection: { payload: ['topicId', 'msgId', 'op', 'epoch', 'attemptId', 'ackTo', 'flightNonce', 'sig', 'rootPub', 'purpose'] },
      schema: { require: ['topicId', 'msgId', 'sig', 'rootPub'], types: { topicId: 'string', msgId: 'string', sig: 'string', rootPub: 'string' } },
      // F3: the FULL D1 flight + proof-signer binding, not a topicId/msgId presence test.
      correlation: { kind: LAR, requires: ['topicId', 'msgId', 'op', 'attemptId', 'ackTo', 'flightNonce', 'rootPub'] },
      idempotency: { from: ['topicId', 'msgId', 'op'] },
      capabilityRange: { proofModule: 'ackProof.js', profile: 'LEGACY_ROOT_V4' },
      errorContract: ['drop-bad-proof', 'drop-flight-mismatch', 'drop-wrong-signer'],
      traceFields: ['topicId', 'msgId', 'op'],
      note: 'D1 signed proof; byte construction/verification owned by ackProof.js; bound under LEGACY_ROOT_V4',
    }),
    ({
      type: 'pubsub:ingestack', variant: 'legacy', wire: T.INGESTACK, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      authGuard: 'adjacentSenderAuth',                               // authenticated adjacent sender + intended incarnation
      projection: { payload: ['topicId', 'msgId', 'op', 'epoch', 'sig'] },   // `sig` projected (absent at runtime) for the type-gated discriminator
      schema: { require: ['topicId', 'msgId'], types: { topicId: 'string', msgId: 'string' } },
      correlation: { kind: LAR, requires: ['topicId', 'msgId', 'op', 'epoch'] },   // adjacent-sender + incarnation(epoch)
      idempotency: { from: ['topicId', 'msgId', 'op'] },
      capabilityRange: { profile: 'LEGACY_ROOT_V4' },
      errorContract: ['drop-wrong-sender', 'drop-wrong-incarnation'],
      traceFields: ['topicId', 'msgId', 'op'],
      note: 'legacy unsigned one-hop; completion binds the authenticated adjacent sender + intended incarnation',
    }),

    // ── write-flight receipt probe / nack (writeFlight) ──
    ({
      type: 'pubsub:receiptprobe', wire: T.RECEIPTPROBE, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      projection: { payload: ['topicId', 'msgId', 'op'] },
      schema: { require: ['topicId', 'msgId', 'op'], types: { topicId: 'string', msgId: 'string', op: 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId', 'msgId'] },
      traceFields: ['topicId', 'msgId', 'op'],
      note: 'pure read of a suspect root’s held state; answered by INGESTACK (held) or RECEIPTNACK (not); no evidence produced',
    }),
    ({
      type: 'pubsub:receiptnack', wire: T.RECEIPTNACK, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      projection: { payload: ['topicId', 'msgId', 'op', 'reason'] },
      schema: { require: ['topicId', 'msgId'], types: { topicId: 'string', msgId: 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId', 'msgId'] },
      errorContract: ['reason'],
      traceFields: ['topicId', 'msgId', 'reason'],
      note: 'explicit non-retention; earns exactly one direct retry; asserts absence, not a positive evidence level',
    }),

    // ── delivery + adoption (TopicDeliveryPlane) ──
    ({
      type: 'pubsub:deliver', wire: T.DELIVER, kind: FrameKind.MULTICAST, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.OBSERVED, proves: Proves.OBSERVATION,
      projection: { payload: ['topicId', 'from', 'msgs'] },
      schema: { require: ['topicId', 'msgs'], types: { topicId: 'string', msgs: 'arr' } },
      traceFields: ['topicId'],
      note: 'fan-out down the tree; each entry keyed by its own msgId (per-entry idempotency, not a frame key)',
    }),
    ({
      type: 'pubsub:adopt', wire: T.ADOPT, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      placementGuard: 'regionOk',
      projection: { payload: ['topicId', 'parent', 'subs'] },
      schema: { require: ['topicId', 'parent'], types: { topicId: 'string', parent: 'string', subs: 'arr' } },
      idempotency: { from: ['topicId', 'parent'] },
      traceFields: ['topicId', 'parent'],
      note: 'delegation command: adopt these subscribers under this parent',
    }),

    // ── read path: PULL / PULLRESP are a CONVERSATION keyed by corrId (F3) ──
    ({
      type: 'pubsub:pull', wire: T.PULL, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      projection: { payload: ['topicId', 'postHash', 'corrId', 'requesterId'] },
      schema: { require: ['topicId', 'corrId', 'requesterId'], types: { topicId: 'string', corrId: 'string', requesterId: 'string' } },
      conversation: { key: ['corrId', 'requesterId'] },
      traceFields: ['topicId', 'corrId'],
      note: 'read request; corrId is the conversation id (not an authority subject); pure read, no idempotency',
    }),
    ({
      type: 'pubsub:pullresp', wire: T.PULLRESP, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      projection: { payload: ['corrId', 'json', 'publishTs', 'requesterId'] },
      schema: { require: ['corrId', 'requesterId'], types: { corrId: 'string', requesterId: 'string' } },
      conversation: { key: ['corrId', 'requesterId'] },
      traceFields: ['corrId'],
      note: 'read response matched to _pending by corrId; json:null is a genuine no-hit',
    }),

    // ── sync engine: catch-up + cohort + handoff (SyncEngine / TopicRoleLifecycle) ──
    ({
      type: 'pubsub:pullup', wire: T.PULLUP, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'sinceHw', 'parentId'] },
      schema: { require: ['topicId', 'parentId'], types: { topicId: 'string', parentId: 'string' } },
      conversation: { key: ['topicId', 'parentId'] },   // catch-up conversation (unsigned parent → not an authenticated holder)
      traceFields: ['topicId', 'parentId'],
      note: 'catch-up trigger to the parent holder; answered by REPLAYUP',
    }),
    ({
      type: 'pubsub:replayup', wire: T.REPLAYUP, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION,
      authGuard: 'verifyEnvelope',                       // per-entry re-verify at ingest
      projection: { payload: ['topicId', 'msgs', 'dels'] },
      schema: { require: ['topicId'], types: { topicId: 'string', msgs: 'arr', dels: 'arr' } },
      conversation: { key: ['topicId'] },                // response leg; matched by topicId to the requesting parent
      traceFields: ['topicId'],
      note: 'replay-up response routed to the requesting parent; per-entry msgId idempotency at ingest',
    }),
    ({
      type: 'pubsub:handoff', wire: T.HANDOFF, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION,
      admissionGuard: 'admitPushedRole',
      projection: { payload: ['topicId', 'from', 'msgs', 'dels'] },
      schema: { require: ['topicId', 'from'], types: { topicId: 'string', from: 'string', msgs: 'arr', dels: 'arr' } },
      conversation: { key: ['topicId', 'from'] },        // transfer conversation (unsigned from → routing hint, not an authenticated holder)
      traceFields: ['topicId', 'from'],
      note: 'standing-state transfer to the heir; answered by HANDOFFACK',
    }),
    ({
      type: 'pubsub:handoffack', wire: T.HANDOFFACK, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'held', 'sent'] },
      schema: { require: ['topicId'], types: { topicId: 'string', held: 'number', sent: 'number' } },
      conversation: { key: ['topicId'] },                // handoff completion
      traceFields: ['topicId'],
      note: 'handoff completion ack; a short ack (held<sent) is deliberately not recorded',
    }),
    ({
      type: 'pubsub:replicate', wire: T.REPLICATE, kind: FrameKind.MULTICAST, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION,
      projection: { payload: ['topicId', 'from', 'msgs', 'dels'] },
      schema: { require: ['topicId'], types: { topicId: 'string', from: 'string', msgs: 'arr', dels: 'arr' } },
      // No correlation: a cohort spray keyed by an unsigned `from` is not an
      // authenticated holder subject, and has no request/response pairing (F3).
      traceFields: ['topicId'],
      note: 'cohort spray to K-closest; empty msgs+dels = liveness keepalive; union ingest is order-independent',
    }),

    // ── locator + lifecycle: beacon / metrics demand / deprecated touch ──
    ({
      type: 'pubsub:rootbeacon', wire: T.ROOTBEACON, kind: FrameKind.UNSOLICITED_EVENT, owningService: 'TopicLocator', versionRange: V,
      placementGuard: 'verifyClosenessGate',             // verify-don't-trust closeness + epoch/tombstone yield rules
      projection: { payload: ['root', 'topics', 'epochs', 'beaconId', 'layer'] },
      schema: { require: ['root', 'beaconId'], types: { root: 'string', beaconId: 'string', topics: 'arr', epochs: 'arr' } },
      idempotency: { from: ['beaconId'] },               // flood dedup
      traceFields: ['root', 'beaconId'],
      note: 'soft-state root advertisement; no opposite → registers with no correlation (§4.3)',
    }),
    ({
      type: 'pubsub:metricson', wire: T.METRICSON, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      placementGuard: 'regionOk+admitRole',
      projection: { payload: ['topicId'] },
      schema: { require: ['topicId'], types: { topicId: 'string' } },
      idempotency: { from: ['topicId'] },                // lease arm/renew is idempotent
      traceFields: ['topicId'],
      note: 'demand signal arming a renewable metrics-publish lease at the root; no durability',
    }),
    ({
      type: 'pubsub:touch', wire: T.TOUCH, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId'] },
      schema: { require: ['topicId'], types: { topicId: 'string' } },
      traceFields: ['topicId'],
      note: 'registered wire-compat no-op (peer.touch deprecated v4.3.0) — governed deprecation exception',
    }),
  ];
}

// Mint the Boundary-1 rows (defineRow-branded, ready to register). Throws if any
// def fails validation — a table defect fails loud at build, never silently.
function boundary1Rows() { return rowDefs().map(defineRow); }

// The wiring map: routed-message T.* string -> { type, variantBy? }. Built from
// the raw defs' `wire` tag (the minted rows drop it). INGESTACK carries a
// TYPE-GATED variant discriminator (F4): `signed` only when `typeof sig ===
// 'string'` — a present-but-non-string sig selects `legacy`, mirroring the live
// handler. Every other frame maps to a single row type.
function frameWiring(defs) {
  const byWire = new Map();
  for (const d of defs) {
    const cur = byWire.get(d.wire);
    if (!cur) byWire.set(d.wire, { type: d.type, variants: d.variant != null });
    else cur.variants = true;   // more than one variant registered for this wire
  }
  const out = new Map();
  for (const [wire, info] of byWire) {
    if (info.variants) {
      out.set(wire, { type: info.type, variantBy: { path: 'sig', valueType: 'string', whenPresent: 'signed', whenAbsent: 'legacy' } });
    } else {
      out.set(wire, { type: info.type });
    }
  }
  return out;
}

// Build a per-node Boundary-1 ShadowRegistry with every row registered, plus the
// wire->row wiring the handler-registration site uses. `enabled` gates observation
// (default-off); `sink` receives trace records; `now` is the clock. Construction
// throws if any row fails defineRow validation.
export function buildBoundary1Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'pubsub+dht', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

export { boundary1Rows, rowDefs, frameWiring };
export default buildBoundary1Registry;
