// fence_topic_independent_durability.mjs — GH #26, council-unanimous.
//
// THE INVARIANT (David): a node's topics are TOTALLY independent. A message is
// (topicId, msgId, body); msgId = sha256({publisher, message}) EXCLUDES the
// topic, so identical content published to two different topics carries the
// SAME msgId. CivilDefense/alert-bot reuse content across topics, so this is
// their ordinary case, not a corner one.
//
// THE DEFECT this fences: the durability ledger was node-global and keyed by
// msgId alone. When one node rooted two such topics, the second topic's
// open() no-op'd on the shared msgId (no obligation), and its cohort verdict
// discharged the FIRST topic's obligation — cross-topic pollution, and a lost
// durability guarantee under churn.
//
// THE FIX (per-Role durability): obligations live ON the Role (role.durability),
// so msgId is only ever a key WITHIN one topic. Two topics cannot collide
// because they were never in one structure. This file proves it by execution.
//
// Run: node test/fence_topic_independent_durability.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};
const TS = 1_700_000_000_000;
const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });

function mgr(rootReplicas = 0) {
  const selfId = (0x89n << 256n) | 0x5eedn;
  const clock = { t: TS };
  const dht = {
    verdictsSupported: true, getSelfId: () => selfId, onRoutedMessage: () => {},
    routeMessage: async () => ({}), findKClosest: async () => [], neighbors: () => [], bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas });
  am.nodeId = selfId; am.setLogSink(() => {});
  return am;
}

async function main() {
  console.log('Topic-independent durability (GH #26) — a shared msgId must NOT merge two topics\n');

  // ── 1. the collision is REAL ────────────────────────────────────────────
  const author = await createAuthorIdentity();
  const descA = DESC('civil-A'), descB = DESC('civil-B');
  const topicA = await deriveTopicIdBig(descA);
  const topicB = await deriveTopicIdBig(descB);
  const MSG = { alert: 'identical-bytes-to-two-topics' };
  const envA = await buildEnvelope({ topic: descA, message: MSG, seq: 1, identity: author, ts: TS });
  const envB = await buildEnvelope({ topic: descB, message: MSG, seq: 1, identity: author, ts: TS });
  ok('1a. two DIFFERENT topics', topicA !== topicB);
  ok('1b. identical {publisher,message} → the SAME msgId (topic excluded from the hash)',
    envA.msgId === envB.msgId, `${envA.msgId} vs ${envB.msgId}`);

  // ── 2. per-Role open: each topic keeps its OWN obligation (the fix) ──────
  {
    const am = mgr(2);
    am.pubsubSubscribe(topicA); am.pubsubSubscribe(topicB);
    const roleA = am._becomeRoot(topicA), roleB = am._becomeRoot(topicB);
    const M = envA.msgId;
    am._durability.open(roleA, M);
    am._durability.open(roleB, M);   // pre-fix: a node-global msgId table → this no-ops
    ok('2a. topic A holds its obligation', roleA.durability?.get(M)?.state === 'pending');
    ok('2b. topic B ALSO holds its own — NOT swallowed by the shared-msgId collision',
      roleB.durability?.get(M)?.state === 'pending');
    ok('2c. the node counts BOTH (pre-fix it counted one)', am.durabilityPending() === 2, String(am.durabilityPending()));
    ok('2d. the obligations live in SEPARATE per-Role stores', roleA.durability !== roleB.durability);
  }

  // ── 3. no cross-topic verdict pollution ─────────────────────────────────
  {
    const am = mgr(2);
    am.pubsubSubscribe(topicA); am.pubsubSubscribe(topicB);
    const roleA = am._becomeRoot(topicA), roleB = am._becomeRoot(topicB);
    const M = envA.msgId;
    am._durability.open(roleA, M);
    am._durability.open(roleB, M);
    am._durability.cancel(roleB, M);                          // a KILL on topic B
    ok('3a. B is cancelled', roleB.durability?.get(M)?.state === 'cancelled');
    ok("3b. …and A is UNTOUCHED — B's kill did not discharge A's obligation",
      roleA.durability?.get(M)?.state === 'pending');
    am._durability.record(roleA, M, { verified: 1, attempted: 1 });   // A's OWN cohort verdict
    ok('3c. A verifies on its own verdict while B stays cancelled — no leakage either way',
      roleA.durability?.get(M)?.state === 'verified' && roleB.durability?.get(M)?.state === 'cancelled');
  }

  // ── 4. the REAL ingress path opens per-Role too — AND delivers both ─────
  // One node roots BOTH topics (the shared-root condition) and subscribes to
  // both. Two publishes carrying the SAME msgId must each open their own
  // obligation AND each be delivered to the app — the colliding msgId must not
  // let one topic's delivery/obligation swallow the other's.
  {
    const am = mgr(0);   // no cohort → each obligation goes terminal, but is PRESENT on its own role
    am.pubsubSubscribe(topicA); am.pubsubSubscribe(topicB);
    const roleA = am._becomeRoot(topicA), roleB = am._becomeRoot(topicB);
    const delivered = [];
    am.onPubsubDelivery((t, _j, msgId) => delivered.push({ topic: String(t), msgId }));
    await am._ingestPublish(roleA, JSON.stringify(envA));
    await am._ingestPublish(roleB, JSON.stringify(envB));   // same msgId, other topic
    ok('4a. ingress opened an obligation on topic A', roleA.durability?.get(envA.msgId) != null);
    ok('4b. ingress opened a SEPARATE obligation on topic B for the same msgId', roleB.durability?.get(envB.msgId) != null);
    ok('4c. …and they are distinct records', roleA.durability?.get(envA.msgId) !== roleB.durability?.get(envB.msgId));
    // DELIVERY, not just durability: the shared root delivered the body to BOTH
    // topics' subscribers — the colliding msgId did not dedup the second away.
    const forA = delivered.filter((d) => d.topic === String(topicA) && d.msgId === envA.msgId).length;
    const forB = delivered.filter((d) => d.topic === String(topicB) && d.msgId === envB.msgId).length;
    ok('4d. topic A delivered its body to the app', forA === 1, `forA=${forA}`);
    ok('4e. topic B ALSO delivered the same-msgId body — delivery is topic-scoped, not collision-swallowed', forB === 1, `forB=${forB} all=${JSON.stringify(delivered.map(d=>d.topic.slice(-4)+':'+d.msgId.slice(0,4)))}`);
  }

  console.log(`\n${fail === 0 ? '✓ all ' + n + ' checks passed' : '✗ ' + fail + ' FAILED'}`);
  process.exit(fail === 0 ? 0 : 1);
}
main().catch((e) => { console.error('threw:', e?.stack || e); process.exit(2); });
