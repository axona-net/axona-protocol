// fence_durability_evidence.mjs — only a DISPATCHED FULL SNAPSHOT is evidence
// about a message.
//
// WHAT ASTER FOUND IN v4.58.1 (council 2026-08-01, CHANGES-REQUIRED 533116a).
// My fix for the previous blocker wired the periodic replication result into the
// durability ledger — and wired in two results that are not evidence:
//
//   1. A SCHEDULER DEFERRAL COUNTED AS AN UNDURABLE ATTEMPT. _replicateRole
//      returns nil('deferred-no-budget') when the tick's full-push budget is
//      spent: nothing sent, attempted:0. _replicateRoots passed every result to
//      recordTopic unconditionally, and record() reads attempted===0 as "no
//      cohort exists" → EXPIRED. So under load, a message whose snapshot never
//      reached the wire became permanently undurable because the tick was busy.
//      That is fail-closed run backwards: absence of evidence, promoted to
//      evidence of failure.
//
//   2. AN EMPTY KEEPALIVE COULD VERIFY A BODY IT NEVER CARRIED. _replicateRole
//      deliberately sends full:false when the signature is unchanged and the
//      replicas are already credited — an empty payload. It still resolves
//      consumed, so it still returned verified>0, and recordTopic then marked
//      EVERY pending message on that topic VERIFIED. Aster's reachability
//      argument, which I verified: a new message whose eager full push fails
//      still writes role.sync.sig, so the next periodic call sees the old
//      credited replica, chooses full:false, and one consumed keepalive declares
//      the new body durable although the receiver never saw it.
//
// MY PART IN IT. The v4.58.1 commit justified topic-level granularity with
// "_syncPush sends the role's whole snapshot, so one verified cohort push covers
// every message this root holds." That is true of a FULL push and I wrote it
// without the qualifier. Third time this week I have written something true of
// the happy path as though it were unconditional — which is the same defect as
// the durability lifecycle that documented behaviour it did not run.
//
// THE FIX, and why it lives in the ledger rather than the caller: _replicateRole
// now reports `dispatched` and `snapshot`, and DurabilityLedger.recordTopic is
// FAIL-CLOSED on both. Putting the rule at the single call site would make it a
// rule that lasts until the second call site.
//
// Run: node test/fence_durability_evidence.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { DurabilityLedger } from '../src/pubsub/durability.js';
import { REPLICATE_FULL_BUDGET, ROOT_REPLICATE_FULL_MS } from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const idHex = (b) => b.toString(16).padStart(66, '0');
const lc = (s) => String(s).toLowerCase();
const TICK = 6_000;
const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });

// `verdict` is a function so a case can change the transport's answer mid-run.
// `pushes` records every COHORT_REPLICATE with the `full` flag actually sent, so
// a claim about keepalive-vs-snapshot is measured, never assumed.
async function rootNode(name, verdict) {
  const desc = DESC(name);
  const topicId = await deriveTopicIdBig(desc);
  const region = topicId >> 256n;
  const selfId = (region << 256n) | 0x5eedn;
  const clock = { t: 1_700_000_000_000 };
  const pushes = [];
  const peer = idHex((region << 256n) | 0x00b1n);
  const dht = {
    verdictsSupported: true,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async (_t, type, payload) => {
      if (/replicate/i.test(type)) {
        pushes.push({ topicId: String(payload?.topicId || ''),
                      msgs: (payload?.msgs?.length ?? 0), dels: (payload?.dels?.length ?? 0) });
      }
      return verdict();
    },
    findKClosest: async () => [peer],
    neighbors: () => [peer],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 1 });
  am.nodeId = selfId; am.setLogSink(() => {});
  am.pubsubSubscribe(topicId);
  const role = am._becomeRoot(topicId);
  const author = await createAuthorIdentity();
  const add = async (k) => {
    const env = await buildEnvelope({ topic: desc, message: { k }, seq: k, identity: author, ts: clock.t });
    await am._ingestPublish(role, JSON.stringify(env));
    return env;
  };
  return { am, clock, topicId, role, add, pushes, desc };
}
const tick = async (am, clock, times = 1) => {
  for (let i = 0; i < times; i++) {
    clock.t += TICK;
    await am.refreshTick();
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));
  }
};
const PASS = () => ({ consumed: true, hops: 1 });
const FAIL = () => ({ consumed: false, exhausted: true });

console.log('durability evidence — a deferral is not a failure, a keepalive is not a snapshot\n');

// ── 1. THE LEDGER REFUSES NON-EVIDENCE AT ITS OWN BOUNDARY ────────────────
// Unit-level first, because this is where the rule has to live: the two callers
// today are the periodic sweep and the eager ingress, and a rule enforced by
// whoever happens to call is a rule with a half-life.
{
  const L = new DurabilityLedger({ now: () => 1 });
  L.open('m1', 7n);
  const deferred = L.recordTopic(7n, { verified: 0, attempted: 0, dispatched: false, snapshot: false });
  ok('1a. a DEFERRAL (nothing sent) leaves the message PENDING — no dispatch is ' +
     'not evidence in either direction',
    L.state('m1') === 'pending', String(L.state('m1')));
  ok('1b. …and says so, rather than silently doing nothing',
    deferred.ignored === 'no-dispatch' && deferred.pending === 1, JSON.stringify(deferred));
  ok('1c. …and does NOT burn an attempt against the budget',
    (L.get('m1')?.attempts ?? -1) === 0, String(L.get('m1')?.attempts));

  const keepalive = L.recordTopic(7n, { verified: 1, attempted: 1, dispatched: true, snapshot: false });
  ok('1d. a consumed EMPTY KEEPALIVE does NOT verify — it proves a peer is ' +
     'reachable, not that a body it never carried arrived',
    L.state('m1') === 'pending', String(L.state('m1')));
  ok('1e. …and is reported as ignored-for-lack-of-snapshot',
    keepalive.ignored === 'no-snapshot' && keepalive.verified === 0, JSON.stringify(keepalive));

  ok('1f. CONTROL — a dispatched FULL snapshot with a consumed verdict DOES ' +
     'verify. Without this the rule could be "never verify" and 1a-1e would ' +
     'all still pass.',
    L.recordTopic(7n, { verified: 1, attempted: 1, dispatched: true, snapshot: true }).verified === 1 &&
    L.state('m1') === 'verified', String(L.state('m1')));
}

// ── 2. THE DEFERRAL IS REAL, THROUGH THE PRODUCTION SWEEP ─────────────────
// Aster's first required control: exhaust the full-push budget with a pending
// message and prove the deferral changes neither its state nor its attempt count.
// The budget is REPLICATE_FULL_BUDGET roles per tick, so the topic under test is
// pushed past that many other roots needing a full push.
{
  const { am, clock, topicId, add, pushes } = await rootNode('de-defer', FAIL);
  const env = await add(1);
  ok('2a. precondition — the message is PENDING after a failed eager replicate',
    am._durability.state(env.msgId) === 'pending', String(am._durability.state(env.msgId)));
  const before = am._durability.get(env.msgId)?.attempts ?? -1;

  // Crowd the sweep: enough OTHER roots each needing a full push to spend the
  // whole budget before the cursor reaches ours.
  const author = await createAuthorIdentity();
  for (let i = 0; i < REPLICATE_FULL_BUDGET + 4; i++) {
    const d = DESC(`de-defer-filler-${i}`);
    const t = await deriveTopicIdBig(d);
    const r = am._becomeRoot(t);
    const e = await buildEnvelope({ topic: d, message: { i }, seq: 1, identity: author, ts: clock.t });
    r.cache.push({ msgId: e.msgId, publishTs: clock.t, json: JSON.stringify(e), seq: 1, bytes: 200 });
    r.cacheIds.add(e.msgId);
  }
  // Start the sweep AT THE FILLERS. My first draft left the cursor at 0, which
  // is the topic under test — it took the budget first, pushed, failed, and
  // burned an attempt. 2d caught that; 2c had been passing vacuously, since
  // "still pending" is also what a failed-but-not-deferred push produces.
  am._replicateCursor = 1;
  ok('2b. precondition — more roles need a full push than the tick budget allows',
    am.axonRoles.size > REPLICATE_FULL_BUDGET,
    `roles=${am.axonRoles.size} budget=${REPLICATE_FULL_BUDGET}`);

  pushes.length = 0;
  await tick(am, clock, 1);
  const mine = lc(idHex(topicId));
  const forMe = pushes.filter(p => p.topicId.toLowerCase() === mine);
  ok('2c. THE DEFERRAL IS REAL — the budget was spent on other roles and OUR ' +
     'topic sent nothing this tick. Without this the section proves nothing.',
    pushes.length > 0 && forMe.length === 0,
    `total=${pushes.length} mine=${forMe.length}`);

  const e2 = am._durability.get(env.msgId);
  ok('2d. our message is STILL PENDING — a message deferred by the scheduler ' +
     'must not go terminal-undurable because the tick was busy',
    am._durability.state(env.msgId) === 'pending',
    `state=${am._durability.state(env.msgId)} reason=${e2?.reason}`);
  ok('2e. …and the deferral burned NO attempt against its budget',
    (e2?.attempts ?? -1) === before, `attempts=${e2?.attempts} before=${before}`);
  ok('2f. …and it is not counted as undurable',
    am.durabilityUndurable() === 0 && am.durabilityPending() >= 1,
    `undurable=${am.durabilityUndurable()} pending=${am.durabilityPending()}`);
}

// ── 3. THE KEEPALIVE IS REAL, THROUGH THE PRODUCTION SWEEP ────────────────
// Aster's second required control, staged exactly as he specified: preload a
// VERIFIED replica; add a new message whose eager full push FAILS; then let a
// periodic empty keepalive be consumed. The new message must remain PENDING, and
// only a later successful FULL snapshot may advance it.
{
  let answer = PASS;
  const { am, clock, add, pushes } = await rootNode('de-keepalive', () => answer());
  const first = await add(1);                    // credited replica, full push consumed
  ok('3a. precondition — the first message verified via a real full push',
    am._durability.state(first.msgId) === 'verified', String(am._durability.state(first.msgId)));
  ok('3b. precondition — a cohort replica is credited, which is what makes the ' +
     'next push a keepalive rather than a snapshot',
    [...am.axonRoles.values()].some(r => (r.replicas?.size ?? 0) > 0));

  answer = FAIL;                                 // the new message's eager push fails
  const second = await add(2);
  ok('3c. precondition — the new message is PENDING after its failed eager push',
    am._durability.state(second.msgId) === 'pending', String(am._durability.state(second.msgId)));

  answer = PASS;                                 // the cohort answers again…
  pushes.length = 0;
  await tick(am, clock, 1);
  const empty = pushes.filter(p => p.msgs === 0);
  ok('3d. precondition — the periodic push really was an EMPTY keepalive, ' +
     'carrying zero messages. Measured, not assumed.',
    pushes.length > 0 && empty.length === pushes.length,
    JSON.stringify(pushes));
  ok('3e. the consumed keepalive does NOT verify the new message — it never ' +
     'carried it. This is the check that fails on 533116a.',
    am._durability.state(second.msgId) === 'pending',
    String(am._durability.state(second.msgId)));

  // …and only a real full snapshot may advance it. ROOT_REPLICATE_FULL_MS is the
  // backstop that re-arms one, so advancing the clock past it produces a genuine
  // full push rather than another keepalive.
  clock.t += ROOT_REPLICATE_FULL_MS + TICK;
  pushes.length = 0;
  await tick(am, clock, 1);
  ok('3f. …and after the full-push backstop re-arms, the push DOES carry the ' +
     'message',
    pushes.some(p => p.msgs > 0), JSON.stringify(pushes));
  ok('3g. CONTROL — that real snapshot, consumed, verifies it. The rule is ' +
     '"only a full snapshot counts", not "nothing counts".',
    am._durability.state(second.msgId) === 'verified',
    String(am._durability.state(second.msgId)));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
