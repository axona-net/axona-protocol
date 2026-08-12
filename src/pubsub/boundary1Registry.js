// boundary1Registry.js — REF-1.1 S2: the Boundary-1 (pub/sub + DHT control)
// frame-contract registry TABLE, plus the wiring map that shadow-wraps the 19
// routed handlers registered in wireHandlers.js `_registerHandlers`.
//
// SHADOW MODE ONLY. This module changes NO acceptance behavior. The registry is
// built ONLY when AxonaManager is constructed with `frameRegistry:true`; the
// wrap runs the handler verbatim whenever the shadow flag is off (the default),
// so flag-off is byte-identical to legacy. Flag-on OBSERVES a decoder-certified
// snapshot beside each handler and emits a trace via the sink — it never mutates,
// suppresses, or reorders a handler or its arguments (the ShadowRegistry.wrap
// contract, gated in smoke_registry_core.mjs). Dispatch is NOT migrated here;
// the registry is source material observed alongside the live handlers (§4.3).
//
// MODELING (grounded in axona-docs code-refactor-plan §4.3 + Refactor-Phase0-
// Inventory §1 + Refactor-Phase0-OwnershipMap §2; catalog cross-checked against
// the live handler bodies in wireHandlers.js / syncEngine.js / writeFlight.js /
// rootElection.js):
//   * frame kind carries a correlation contract ONLY where the kind implies one
//     (§4.3). No Boundary-1 frame is REQUEST_RESPONSE: the correlation-subject
//     union (LegacyAuthorityRef | IngressRef | HolderRef | AuthorLaneRef) is
//     write-authority-centric, while the request/response PAIRS (PULL↔PULLRESP,
//     PULLUP↔REPLAYUP) correlate by a conversation id (corrId / parentId) that is
//     not a union subject. So reads and delivery register as ONE_WAY/MULTICAST
//     with NO correlation; correlation attaches to the write/durability/handoff
//     frames as LegacyAuthorityRef (writes) or HolderRef (cohort/transfer). Under
//     Kernel 4 every authority subject is a LegacyAuthorityRef (§4.3).
//   * evidence uses the five-level hierarchy (ROUTED/INGESTED/RETAINED/COMMITTED/
//     OBSERVED). A probe / nack / read / demand / soft-state advert produces no
//     positive evidence → evidence:null. COMMITTED is never asserted here (it
//     needs a producedPolicy + receipt digest — out of Boundary-1 shadow scope).
//   * idempotency key = the projected path(s) that make a repeat a duplicate.
//     `msgId` is the universal durable key but is not always a top-level field:
//     for PUB it lives inside the signed `json` envelope, so the projected key is
//     `json` (identical envelope ⇒ identical msgId ⇒ duplicate); for KILL it is
//     `kill.msgId` (a projected nested path).
//   * INGESTACK is ONE row with two variants — `signed` (D1 proof, carries `sig`
//     + `rootPub`) and `legacy` (unsigned one-hop) — discriminated by the
//     presence of `sig`. Both are ONE_WAY proofs (§4.3: signed INGESTACK is a
//     routed ONE_WAY proof, not a last-hop-authenticated response).
//   * CAP_ATTEST is NOT here (it is a transport/auth boundary frame, §4.3).
//     UNPUB is retired (unregistered, Inventory §1) and has no row.
//   * No new wire fields are introduced. Rows describe the EXISTING frames.

import { T } from './constants.js';
import {
  defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind,
  ShadowRegistry,
} from '../registry/index.js';

const V = { min: 4, max: 4 };                 // Kernel-4 wire version
const LAR = CorrelationSubjectKind.LegacyAuthorityRef;
const HOLDER = CorrelationSubjectKind.HolderRef;

// The Boundary-1 row DEFINITIONS. `type` is the stable registry key (also the
// trace label); `wire` is the routed-message T.* string the handler is registered
// under. `wire` is carried on the def (defineRow ignores unknown keys and mints a
// normalized row WITHOUT it), so the wiring map is derived from these defs, not
// from the minted rows. This keeps the table the single source of truth for both
// registration and the handler wrapping.
function rowDefs() {
  return [
    // ── pub/sub control: subscription lease (TopicDeliveryPlane) ──
    ({
      type: 'pubsub:sub', wire: T.SUB, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'subscriberId', 'since', 'latest', 'hw', 'lw'] },
      schema: { require: ['topicId', 'subscriberId'], types: { topicId: 'string', subscriberId: 'string' } },
      idempotency: { from: ['topicId', 'subscriberId'] },   // a renewal is idempotent
      note: 'establishes/renews a delivery lease routed toward topicId; no authority subject',
    }),
    ({
      type: 'pubsub:unsub', wire: T.UNSUB, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'subscriberId'] },
      schema: { require: ['topicId', 'subscriberId'], types: { topicId: 'string', subscriberId: 'string' } },
      idempotency: { from: ['topicId', 'subscriberId'] },
    }),

    // ── write ingress: PUB / KILL open an authority flight (WriteIngress) ──
    ({
      type: 'pubsub:pub', wire: T.PUB, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      projection: { payload: ['topicId', 'json', 'via', 'ackTo', 'attemptId', 'flightNonce'] },
      schema: { require: ['topicId', 'json'], types: { topicId: 'string', json: 'string' } },
      correlation: { kind: LAR, requires: ['topicId'] },
      idempotency: { from: ['json'] },   // msgId = hash(publisher+message) is deterministic from json
      note: 'signed envelope routed to the root; ingest proof returns as a separate INGESTACK ONE_WAY',
    }),
    ({
      type: 'pubsub:kill', wire: T.KILL, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      projection: { payload: ['topicId', 'kill.msgId', 'kill.signerPubkey', 'via', 'ackTo', 'attemptId', 'flightNonce'] },
      schema: { require: ['topicId', 'kill.msgId'], types: { topicId: 'string', 'kill.msgId': 'string' } },
      correlation: { kind: LAR, requires: ['topicId'] },
      idempotency: { from: ['kill.msgId'] },   // tombstone keyed by the target msgId
    }),

    // ── ingest proof: D1 signed + legacy unsigned variants (WriteIngress / writeFlight) ──
    ({
      type: 'pubsub:ingestack', variant: 'signed', wire: T.INGESTACK, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      projection: { payload: ['topicId', 'msgId', 'op', 'epoch', 'attemptId', 'ackTo', 'flightNonce', 'sig', 'rootPub', 'purpose'] },
      schema: { require: ['topicId', 'msgId', 'sig', 'rootPub'], types: { topicId: 'string', msgId: 'string', sig: 'string', rootPub: 'string' } },
      correlation: { kind: LAR, requires: ['topicId', 'msgId'] },   // flight (topicId,msgId,op,attemptId,ackTo,flightNonce)
      idempotency: { from: ['topicId', 'msgId', 'op'] },
      note: 'D1 signed proof; byte construction/verification owned by ackProof.js',
    }),
    ({
      type: 'pubsub:ingestack', variant: 'legacy', wire: T.INGESTACK, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
      projection: { payload: ['topicId', 'msgId', 'op', 'epoch', 'sig'] },   // `sig` projected (absent) so the variant discriminator resolves
      schema: { require: ['topicId', 'msgId'], types: { topicId: 'string', msgId: 'string' } },
      correlation: { kind: LAR, requires: ['topicId', 'msgId'] },
      idempotency: { from: ['topicId', 'msgId', 'op'] },
      note: 'legacy unsigned one-hop; completion still binds the authenticated adjacent sender + incarnation',
    }),

    // ── write-flight receipt probe / nack (writeFlight) ──
    ({
      type: 'pubsub:receiptprobe', wire: T.RECEIPTPROBE, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      projection: { payload: ['topicId', 'msgId', 'op'] },
      schema: { require: ['topicId', 'msgId', 'op'], types: { topicId: 'string', msgId: 'string', op: 'string' } },
      correlation: { kind: LAR, requires: ['topicId', 'msgId'] },
      note: 'pure read of a suspect root’s held state; answered by INGESTACK (held) or RECEIPTNACK (not); no evidence produced',
    }),
    ({
      type: 'pubsub:receiptnack', wire: T.RECEIPTNACK, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      projection: { payload: ['topicId', 'msgId', 'op', 'reason'] },
      schema: { require: ['topicId', 'msgId'], types: { topicId: 'string', msgId: 'string' } },
      correlation: { kind: LAR, requires: ['topicId', 'msgId'] },
      note: 'explicit non-retention; earns exactly one direct retry; asserts absence, not a positive evidence level',
    }),

    // ── delivery + adoption (TopicDeliveryPlane) ──
    ({
      type: 'pubsub:deliver', wire: T.DELIVER, kind: FrameKind.MULTICAST, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.OBSERVED, proves: Proves.OBSERVATION,
      projection: { payload: ['topicId', 'from', 'msgs'] },
      schema: { require: ['topicId', 'msgs'], types: { topicId: 'string', msgs: 'arr' } },
      note: 'fan-out down the tree; each entry keyed by its own msgId (per-entry idempotency, not a frame key)',
    }),
    ({
      type: 'pubsub:adopt', wire: T.ADOPT, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'parent', 'subs'] },
      schema: { require: ['topicId', 'parent'], types: { topicId: 'string', parent: 'string', subs: 'arr' } },
      idempotency: { from: ['topicId', 'parent'] },
      note: 'delegation command: adopt these subscribers under this parent',
    }),

    // ── read path: PULL / PULLRESP correlate by corrId (no authority subject) ──
    ({
      type: 'pubsub:pull', wire: T.PULL, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      projection: { payload: ['topicId', 'postHash', 'corrId', 'requesterId'] },
      schema: { require: ['topicId', 'corrId', 'requesterId'], types: { topicId: 'string', corrId: 'string', requesterId: 'string' } },
      note: 'read request; corrId is the conversation id (not a correlation-union subject); pure read, no idempotency',
    }),
    ({
      type: 'pubsub:pullresp', wire: T.PULLRESP, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      projection: { payload: ['corrId', 'json', 'publishTs', 'requesterId'] },
      schema: { require: ['corrId', 'requesterId'], types: { corrId: 'string', requesterId: 'string' } },
      note: 'read response matched to _pending by corrId; json:null is a genuine no-hit',
    }),

    // ── sync engine: catch-up + cohort + handoff (SyncEngine / TopicRoleLifecycle) ──
    ({
      type: 'pubsub:pullup', wire: T.PULLUP, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'sinceHw', 'parentId'] },
      schema: { require: ['topicId', 'parentId'], types: { topicId: 'string', parentId: 'string' } },
      correlation: { kind: HOLDER, requires: ['topicId', 'parentId'] },
      note: 'catch-up trigger to the parent holder; answered by REPLAYUP',
    }),
    ({
      type: 'pubsub:replayup', wire: T.REPLAYUP, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION,
      projection: { payload: ['topicId', 'msgs', 'dels'] },
      schema: { require: ['topicId'], types: { topicId: 'string', msgs: 'arr', dels: 'arr' } },
      correlation: { kind: HOLDER, requires: ['topicId'] },
      note: 'replay-up response routed to the requesting parent; per-entry msgId idempotency at ingest',
    }),
    ({
      type: 'pubsub:handoff', wire: T.HANDOFF, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION,
      projection: { payload: ['topicId', 'from', 'msgs', 'dels'] },
      schema: { require: ['topicId', 'from'], types: { topicId: 'string', from: 'string', msgs: 'arr', dels: 'arr' } },
      correlation: { kind: HOLDER, requires: ['topicId', 'from'] },
      note: 'standing-state transfer to the heir; answered by HANDOFFACK',
    }),
    ({
      type: 'pubsub:handoffack', wire: T.HANDOFFACK, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId', 'held', 'sent'] },
      schema: { require: ['topicId'], types: { topicId: 'string', held: 'number', sent: 'number' } },
      correlation: { kind: HOLDER, requires: ['topicId'] },
      note: 'handoff completion ack; a short ack (held<sent) is deliberately not recorded',
    }),
    ({
      type: 'pubsub:replicate', wire: T.REPLICATE, kind: FrameKind.MULTICAST, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION,
      projection: { payload: ['topicId', 'from', 'msgs', 'dels'] },
      schema: { require: ['topicId'], types: { topicId: 'string', from: 'string', msgs: 'arr', dels: 'arr' } },
      correlation: { kind: HOLDER, requires: ['topicId'] },
      note: 'cohort spray to K-closest; empty msgs+dels = liveness keepalive; union ingest is order-independent',
    }),

    // ── locator + lifecycle: beacon / metrics demand / deprecated touch ──
    ({
      type: 'pubsub:rootbeacon', wire: T.ROOTBEACON, kind: FrameKind.UNSOLICITED_EVENT, owningService: 'TopicLocator', versionRange: V,
      projection: { payload: ['root', 'topics', 'epochs', 'beaconId', 'layer'] },
      schema: { require: ['root', 'beaconId'], types: { root: 'string', beaconId: 'string', topics: 'arr', epochs: 'arr' } },
      idempotency: { from: ['beaconId'] },   // flood dedup
      note: 'soft-state root advertisement; no opposite → registers with no correlation (§4.3)',
    }),
    ({
      type: 'pubsub:metricson', wire: T.METRICSON, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      projection: { payload: ['topicId'] },
      schema: { require: ['topicId'], types: { topicId: 'string' } },
      idempotency: { from: ['topicId'] },   // lease arm/renew is idempotent
      note: 'demand signal arming a renewable metrics-publish lease at the root; no durability',
    }),
    ({
      type: 'pubsub:touch', wire: T.TOUCH, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING,
      projection: { payload: ['topicId'] },
      schema: { require: ['topicId'], types: { topicId: 'string' } },
      note: 'registered wire-compat no-op (peer.touch deprecated v4.3.0) — governed deprecation exception',
    }),
  ];
}

// Mint the Boundary-1 rows (defineRow-branded, ready to register). Throws if any
// def fails validation — a table defect fails loud at build, never silently.
function boundary1Rows() { return rowDefs().map(defineRow); }

// The wiring map: routed-message T.* string -> { type, variantBy? }. Built from
// the raw defs' `wire` tag (the minted rows drop it), so the table is the single
// source of truth for both registration and handler wrapping. INGESTACK carries a
// variant discriminator (presence of `sig` -> signed, else legacy); every other
// frame maps to a single row type.
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
      // Only INGESTACK is variant-bearing in Boundary-1; discriminate on `sig`.
      out.set(wire, { type: info.type, variantBy: { path: 'sig', whenPresent: 'signed', whenAbsent: 'legacy' } });
    } else {
      out.set(wire, { type: info.type });
    }
  }
  return out;
}

// Build a per-node Boundary-1 ShadowRegistry with every row registered, plus the
// wire->row wiring the handler-registration site uses. `enabled` gates observation
// (default-off); `sink` receives trace records; `now` is the clock. Construction
// throws if any row fails defineRow validation (a table defect must fail loud at
// build, never silently mis-observe).
export function buildBoundary1Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'pubsub+dht', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

export { boundary1Rows, rowDefs, frameWiring };
export default buildBoundary1Registry;
