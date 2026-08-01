// fence_q2_end_to_end.mjs — Q2 blocker 2. The fail-closed EFFECTS, not the counter.
//
// WHAT WAS MISSING. fence_dispatch_contract proves role.replicas stays empty
// without a verdict, and that _replicateRole reports the counters the confirm
// gate reads. It does NOT prove the gate acts on them. Aster, council seq 106,
// restated as the blocker in seq 117:
//
//   "declared-false and declared-true/void must each record the right diagnostic
//    class, leave publish and kill pending, and leave non-root _handoffAcked
//    empty; a consumed control must prove each corresponding success path."
//
// A counter nothing acts on is exactly the shape of defect this whole version
// exists to remove — evidence that is collected and then not used. So this file
// drives the REAL ingress with REAL SIGNED ENVELOPES and asserts what a caller
// would actually observe:
//
//   PUBLISH  _pendingPub still holds the msgId  → the publisher keeps retrying
//   KILL     _pendingKill still holds the msgId → the killer keeps retrying
//   HANDOFF  _handoffAcked stays empty          → no permanent retry exemption
//
// NOTHING IS STUBBED ON THE PATH UNDER TEST. buildEnvelope signs with a real
// Ed25519 author identity; _ingestPublish runs verifyEnvelope + checkFreshness +
// deriveTopicIdBig + the write-policy check + the stamp + the confirm gate. The
// topic id is DERIVED from the descriptor, not invented, so the id the root holds
// is the id the envelope resolves to. Only the transport's answer varies.
//
// THREE TRANSPORT BEHAVIOURS, one matrix:
//   consumed          — a verdict of success   → every success path must fire
//   declared-false    — honest non-reporting   → no evidence, nothing discharges
//   declared-true/void— contract violation     → no evidence, nothing discharges
//
// The last two are the ones that must NOT confirm. They are distinct classes and
// are asserted separately, because collapsing them is how 'unreported' got
// credited in v4.57.0.
//
// Run: node test/fence_q2_end_to_end.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const idHex = (b) => b.toString(16).padStart(66, '0');
const TICK = 6_000;

const CONSUMED = () => ({ consumed: true,  hops: 2 });
const VOID     = () => undefined;

// `report` is the transport's answer; `declares` is what the adapter CLAIMS.
// Everything between them is production code.
function mk(report, declares, selfId) {
  const clock = { t: 1_700_000_000_000 };          // real-ish epoch: checkFreshness is live
  const dht = {
    verdictsSupported: declares,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async () => report(),
    // A real two-member cohort, so attempted > 0 and the gate is actually armed.
    findKClosest: async () => [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)],
    neighbors: () => [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)],
    bridgeId: () => null,
    // The leave-handoff resolves heirs through lookup().path — without it every
    // job has heir=null, `sendable` is empty, and the handoff section would pass
    // for the wrong reason (nothing to ack rather than nothing acked).
    lookup: async () => ({ path: [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)] }),
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = selfId;
  am.setLogSink(() => {});
  return { am, clock };
}

// A root for a DERIVED topic id. The node id is placed in the topic's own region
// so _regionOk admits it — the region prefix is the first byte of the id.
async function rootFor(desc, report, declares) {
  const topicId = await deriveTopicIdBig(desc);
  const region = topicId >> 256n;                       // top byte = region prefix
  const selfId = (region << 256n) | 0x5eedn;            // same region, arbitrary suffix
  const { am, clock } = mk(report, declares, selfId);
  am.pubsubSubscribe(topicId);
  const role = am._becomeRoot(topicId);
  return { am, clock, topicId, role };
}

const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });

console.log('Q2 end-to-end — the confirm gate must ACT on the evidence, not just count it\n');

// ── 0. THE HARNESS IS REAL ─────────────────────────────────────────────────
// If the envelope did not verify, every "stays pending" assertion below would
// pass for the wrong reason — the publish would be DROPPED, not withheld. This
// section exists because a fence that cannot tell "refused" from "never arrived"
// certifies nothing.
{
  const author = await createAuthorIdentity();
  const desc = DESC('q2-harness');
  const { am, clock, topicId, role } = await rootFor(desc, CONSUMED, true);
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  ok('0a. the topic id is DERIVED from the descriptor, and the root holds it',
    role.topicId === topicId);
  ok('0b. the envelope is genuinely signed (signature + signerPubkey present)',
    !!env.signature && !!env.signerPubkey, JSON.stringify(Object.keys(env)));
  await am._ingestPublish(role, JSON.stringify(env));
  ok('0c. the real ingress ACCEPTED it — verifyEnvelope, freshness, descriptor ' +
     'match and write policy all passed, so a later "not confirmed" means ' +
     'WITHHELD, never dropped',
    role.cacheIds.has(env.msgId), `cache=${role.cache.length}`);
}

// ── 1. PUBLISH ─────────────────────────────────────────────────────────────
async function publishCase(label, report, declares, expect) {
  const author = await createAuthorIdentity();
  const desc = DESC(`q2-pub-${label}`);
  const { am, clock, topicId, role } = await rootFor(desc, report, declares);
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  const json = JSON.stringify(env);
  am.pubsubPublish(topicId, json);                       // production writer → _pendingPub
  ok(`1${expect.tag}a. [${label}] precondition — the publish is PENDING before ingest`,
    am._pendingPub.has(env.msgId), JSON.stringify([...am._pendingPub.keys()]));
  await am._ingestPublish(role, json);
  ok(`1${expect.tag}b. [${label}] ${expect.pub}`,
    am._pendingPub.has(env.msgId) === expect.stillPending,
    `stillPending=${am._pendingPub.has(env.msgId)}, want ${expect.stillPending}`);
  return { am, role };
}
{
  const { role } = await publishCase('consumed', CONSUMED, true,
    { tag: 'a', stillPending: false, pub: 'CONTROL — a verified dispatch CONFIRMS the publish' });
  ok('1ac. CONTROL — …and the cohort is credited in replicas',
    role.replicas.size > 0, `replicas=${role.replicas.size}`);
}
{
  const { role } = await publishCase('declared-false', VOID, false,
    { tag: 'b', stillPending: true,
      pub: 'an honest non-reporting adapter leaves the publish PENDING — the publisher keeps retrying' });
  ok('1bc. …and the failure is recorded in the right diagnostic class: unsupported',
    [...role.attempted.values()].every(v => v.via === 'unsupported') && role.attempted.size > 0,
    JSON.stringify([...role.attempted.values()]));
  ok('1bd. …with nothing credited as a replica',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}
{
  const { role } = await publishCase('declared-true/void', VOID, true,
    { tag: 'c', stillPending: true,
      pub: 'a CONTRACT VIOLATION leaves the publish PENDING too' });
  ok('1cc. …and is recorded as violation, NOT collapsed into unsupported — ' +
     'the two are different facts about the adapter',
    [...role.attempted.values()].every(v => v.via === 'violation') && role.attempted.size > 0,
    JSON.stringify([...role.attempted.values()]));
  ok('1cd. …with nothing credited as a replica',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}

// ── 2. KILL ────────────────────────────────────────────────────────────────
// A kill is a publish plus a side effect, so it must gate identically. The kill
// confirm is fire-and-forget off a .then(), so the assertion waits a turn.
async function killCase(label, report, declares, stillPending) {
  const author = await createAuthorIdentity();
  const desc = DESC(`q2-kill-${label}`);
  const { am, clock, topicId, role } = await rootFor(desc, report, declares);
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  await am._ingestPublish(role, JSON.stringify(env));    // something to kill
  role.replicas.clear(); role.attempted?.clear();        // isolate the kill's own evidence

  // A REAL signed kill: _onKill runs verifyKill, so a hand-built object is
  // dropped and every "stays pending" below would pass for the wrong reason.
  const kill = await buildKill({ topicId: idHex(topicId), msgId: env.msgId, ts: clock.t, seq: 2, identity: author });
  if (!am._pendingKill) am._pendingKill = new Map();
  am._pendingKill.set(env.msgId, { topicBig: topicId, kill, at: clock.t, tries: 0 });
  // isTerminal is REQUIRED: _topicDecision returns 'forward' without it and
  // _onKill returns before reaching the gate — which is how 2d/2v were passing
  // vacuously against code that never ran.
  await am._onKill({ topicId: idHex(topicId), kill }, { targetId: am.nodeId, isTerminal: true });
  await new Promise(r => setImmediate(r));               // the confirm rides a .then()
  await new Promise(r => setImmediate(r));
  ok(`2${label[0]}. [${label}] the kill ${stillPending ? 'stays PENDING' : 'CONFIRMS'}`,
    am._pendingKill.has(env.msgId) === stillPending,
    `stillPending=${am._pendingKill.has(env.msgId)}, want ${stillPending}`);
}
await killCase('consumed', CONSUMED, true, false);              // CONTROL
await killCase('declared-false', VOID, false, true);
await killCase('void-violation', VOID, true, true);

// ── 3. LEAVE-HANDOFF EXEMPTION — FAIL-CLOSED HALF ONLY ────────────────────
// HONEST LIMIT, MEASURED. _handoffAcked is populated by _onHandoffAck: a wire
// ACK sent back BY THE HEIR. A single-node harness has no heir process, so the
// consumed control cannot fire here no matter what the transport says — I
// instrumented it and watched three pubsub:handoff messages dispatch and resolve
// {consumed:true} while the set stayed empty, because the ack never comes from
// anywhere. The dispatched()/REPLICATE path this section was written against is
// only the LAST-GASP fallback, not the primary handoff.
//
// So the two fail-closed checks below are real but WEAKER than they look: they
// prove the set stays empty, which it would also do if nothing ran. Proving the
// exemption is EARNED requires a two-node harness where a real heir acks. Until
// that exists this section is not evidence, and the file stays quarantined.
//// ── 3. LEAVE-HANDOFF EXEMPTION ─────────────────────────────────────────────
// A departing holder earns a PERMANENT retry exemption by being added to
// _handoffAcked. There is no REPLICATE ack on this path, so dispatch is the only
// evidence — which is exactly why it must be a VERIFIED dispatch. Until v4.58.0
// the test was `dispatchVerdict(r) !== 'failed'`, a negative that let 'unknown'
// into a success path (Aster named this line).
async function handoffCase(label, report, declares, expectAcked) {
  const author = await createAuthorIdentity();
  const desc = DESC(`q2-handoff-${label}`);
  const { am, clock, topicId, role } = await rootFor(desc, report, declares);
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  await am._ingestPublish(role, JSON.stringify(env));    // history worth handing off
  await am.pubsubLeaveHandoff();
  ok(`3${label[0]}. [${label}] _handoffAcked ${expectAcked ? 'records the exemption' : 'stays EMPTY'}` +
     (expectAcked ? '' : ' — no verified dispatch, no permanent exemption'),
    (am._handoffAcked.size > 0) === expectAcked,
    `acked=${am._handoffAcked.size}, want ${expectAcked ? '>0' : '0'}`);
}
await handoffCase('consumed', CONSUMED, true, true);            // CONTROL
await handoffCase('declared-false', VOID, false, false);
await handoffCase('void-violation', VOID, true, false);

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
