// fence_kill_late_subscriber.mjs — a killed body must never reach a subscriber
// who arrives after the kill, and the retry machinery must not resurrect it.
//
// ASTER'S THIRD REQUIRED CONTROL (council 2026-08-01): "retain an actual
// late-subscriber/retry regression for the killed unverified body, not only
// cache/map assertions." fence_q2_end_to_end §5f–5i assert on _pendingPub,
// role.tombstones and role.cacheIds. Those are the right invariants and they are
// all one layer above the thing that actually matters — whether a human on
// another node sees a retracted message. Every one of them can hold while the
// body still arrives by some path the assertions do not name. So this file asks
// the question in the only form that cannot be satisfied vacuously: a REAL
// second node subscribes, and we look at what its app callback received.
//
// THE REGRESSION IT PINS. My first attempt at the self-delivery confirm defect
// made _deliverToApp withhold the publish confirm. The publish then stayed in
// _pendingPub and refreshTick's persistent PUB retry (repairPlane 1c) re-sent it
// every tick, and smoke_pubsub_kill went red on a re-delivered killed body. I
// reverted the fix; the defect is still open and documented in wireHandlers.js,
// and this fence is what any future fix has to survive.
//
// Section 4 states plainly what I measured while building this, because it is not
// what I first wrote: re-arming that retry is NOT by itself enough to resurrect
// the body here. The tombstone gates re-ingest, and it is the load-bearing
// defence. Both layers are asserted separately, so a failure names which one went.
//
// THE CONTROL IS THE POINT. Section 1 runs the identical flow WITHOUT the kill
// and requires the late subscriber to RECEIVE the body. Without it, "the late
// subscriber got nothing" is also what a broken fabric, a failed subscribe, or a
// silent routing error produces — the confident-false-negative this whole
// release is about, reproduced in the test meant to catch it. (I made exactly
// that mistake in fence_handoff_nonroot and only caught it by measuring.)
//
// Run: node test/fence_kill_late_subscriber.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const idHex = (b) => b.toString(16).padStart(66, '0');
const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });
const TICK = 6_000;

// ── the fabric ─────────────────────────────────────────────────────────────
// Two managers, honest routed delivery: routeMessage hands the payload to the
// real handler on the real destination and reports the real verdict. Nothing
// here fakes a dispatch outcome, and nothing fakes a delivery.
class Net {
  constructor() { this.nodes = new Map(); this.clock = { t: 1_700_000_000_000 }; }
  add(idBig, { rootReplicas = 0 } = {}) {
    const self = this;
    const handlers = new Map();
    const other = () => [...self.nodes.keys()].find(x => x !== idBig) ?? null;
    const dht = {
      verdictsSupported: true,
      getSelfId: () => idBig,
      onRoutedMessage: (type, fn) => handlers.set(type, fn),
      routeMessage: async (target, type, payload) => {
        let t; try { t = typeof target === 'bigint' ? target : BigInt('0x' + String(target)); } catch { t = null; }
        const dest = self.nodes.has(t) ? t : other();
        if (dest === null) return { consumed: false, exhausted: true };
        const h = self.nodes.get(dest).handlers.get(type);
        if (!h) return { consumed: false, terminal: true };
        const r = await h(payload, { targetId: dest, isTerminal: true, fromId: idHex(idBig) });
        return r === 'consumed' ? { consumed: true, atNode: idHex(dest), hops: 1 }
                                : { consumed: false, terminal: true };
      },
      // In a two-node net the other node IS the K-closest set. Stated plainly
      // rather than left to XOR luck, so the subscriber's SUB deterministically
      // reaches the root and a failure here means the ROOT misbehaved.
      findKClosest: async () => { const o = other(); return o === null ? [] : [idHex(o)]; },
      neighbors: () => { const o = other(); return o === null ? [] : [idHex(o)]; },
      bridgeId: () => null,
      lookup: async () => { const o = other(); return { path: o === null ? [] : [idHex(o)] }; },
      isReachableId: () => true,
    };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => self.clock.t, rootReplicas });
    am.nodeId = idBig; am.setLogSink(() => {});
    this.nodes.set(idBig, { am, handlers, id: idBig });
    return am;
  }
}

// A publishing ROOT plus a second node that has not subscribed yet. The body is
// UNVERIFIED by construction: rootReplicas 0 means no cohort exists to consume a
// REPLICATE, so durability can never reach `verified` — which is precisely the
// state Aster named ("the killed UNVERIFIED body").
async function scene(name) {
  const desc = DESC(name);
  const topicId = await deriveTopicIdBig(desc);
  const net = new Net();
  // The root sits AT the topic address, so it is unambiguously topic-closest and
  // stays the root. My first draft placed the two nodes at 0x0001/0x0002 and let
  // XOR decide: the "late subscriber" turned out to be closer, self-rooted on the
  // first beacon, and served itself. Leaving that to luck would make this fence
  // measure a different topology on a different topic name.
  const root = net.add(topicId);
  const late = net.add(topicId ^ 0xffffn);
  const got = [];
  late.onPubsubDelivery((_t, json, msgId) => got.push({ msgId, json }));

  root.pubsubSubscribe(topicId);
  const role = root._becomeRoot(topicId);
  const author = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: desc, message: { secret: 'retracted' }, seq: 1, identity: author, ts: net.clock.t });
  const json = JSON.stringify(env);
  root.pubsubPublish(topicId, json);          // opens the DELIVERY obligation (_pendingPub)
  await root._ingestPublish(role, json);      // stamps it at the root
  return { net, root, late, topicId, role, env, author, desc, got };
}

const tick = async (am, net, times = 1) => {
  for (let i = 0; i < times; i++) {
    net.clock.t += TICK;
    await am.refreshTick();
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));
  }
};
// `replayLatest` is how "serve me the newest history you hold" is expressed —
// there is no since:'all' option on pubsubSubscribe. It matters here for a second
// reason: replayLatest serves the newest cache entry REGARDLESS OF AGE, so a
// killed body still sitting in the root's cache WOULD be handed over. A plain
// subscribe takes since = now and would withhold the body for reasons that have
// nothing to do with the kill — a pass that proves nothing.
const subscribeLate = async (late, net, topicId) => {
  late.pubsubSubscribe(topicId, { replayLatest: true });
  await new Promise(r => setImmediate(r));
  await tick(late, net, 1);
  await new Promise(r => setImmediate(r));
};
const bodies = (got, env) => got.filter(g => g.msgId === env.msgId);

console.log('killed body vs a REAL late subscriber — asked at the app callback\n');

// ── 1. CONTROL — WITHOUT the kill, the late subscriber DOES receive it ────
// If this ever fails, every "received nothing" below is worthless.
{
  const { net, root, late, topicId, env, got } = await scene('kls-control');
  await tick(root, net, 1);
  await subscribeLate(late, net, topicId);
  ok('1a. CONTROL — a live body reaches a subscriber who arrives AFTER the ' +
     'publish. The path this fence measures is real.',
    bodies(got, env).length === 1, `delivered=${JSON.stringify(got.map(g => g.msgId.slice(0, 8)))}`);
}

// ── 2. THE KILLED BODY NEVER ARRIVES ──────────────────────────────────────
{
  const { net, root, late, topicId, role, env, author, got } = await scene('kls-killed');
  ok('2a. precondition — the body is UNVERIFIED: no cohort exists, so durability ' +
     'never reached `verified`',
    root._durability?.state(env.msgId) !== 'verified', String(root._durability?.state(env.msgId)));

  const kill = await buildKill({ topicId: idHex(topicId), msgId: env.msgId, ts: net.clock.t + 1, seq: 2, identity: author });
  await root._onKill({ topicId: idHex(topicId), kill }, { targetId: root.nodeId, isTerminal: true });
  await new Promise(r => setImmediate(r));
  ok('2b. precondition — the retraction actually took at the root',
    role.tombstones.has(env.msgId) && !role.cacheIds.has(env.msgId),
    JSON.stringify({ tomb: role.tombstones.has(env.msgId), cached: role.cacheIds.has(env.msgId) }));

  await subscribeLate(late, net, topicId);
  ok('2c. the late subscriber receives NO body — the replay serves the retraction, ' +
     'not the message',
    bodies(got, env).length === 0, JSON.stringify(got.map(g => g.json?.slice(0, 60))));
}

// ── 3. THE RETRY PUMP MUST NOT RESURRECT IT ───────────────────────────────
// The kill lands, THEN the root ticks — driving repairPlane 1c's persistent PUB
// retry — and only THEN does the subscriber arrive. The ordering is the point: a
// pump that put the body back in the root's cache would do it in time for this
// subscriber's replay, and that is invisible to a _pendingPub assertion taken
// before the ticks.
{
  const { net, root, late, topicId, role, env, author, got } = await scene('kls-retry');
  const kill = await buildKill({ topicId: idHex(topicId), msgId: env.msgId, ts: net.clock.t + 1, seq: 2, identity: author });
  await root._onKill({ topicId: idHex(topicId), kill }, { targetId: root.nodeId, isTerminal: true });
  await new Promise(r => setImmediate(r));

  await tick(root, net, 5);                    // five full retry rounds
  ok('3a. after five retry rounds the body is STILL out of the root\'s cache — ' +
     'nothing re-ingested it',
    !role.cacheIds.has(env.msgId), `cache=${role.cache.length}`);

  await subscribeLate(late, net, topicId);
  ok('3b. …and a subscriber arriving after those rounds receives NO body. This is ' +
     'the assertion the cache/map checks stand in for, asked directly.',
    bodies(got, env).length === 0, JSON.stringify(got.map(g => g.json?.slice(0, 60))));
  ok('3c. …with the retraction still standing — suppression must not come from ' +
     'the tombstone having been dropped',
    role.tombstones.has(env.msgId));
}

// ── 4. DEFENCE IN DEPTH — the cancellation is not the only thing holding ───
// WHAT I MEASURED, and a correction to what I first wrote here. I had section 3
// claiming that a publish surviving the kill would be re-ingested by the pump. It
// is not. I injected the exact regression — removed the `_pendingPub.delete` from
// _confirmPending, which is precisely what my reverted self-delivery fix amounted
// to — and section 3 stayed green. The probe says why: the pump DID fire (six
// `pubsub:pub` re-sends across three ticks, _pendingPub still holding the killed
// msgId) and the ingest refused every one of them, because the tombstone gates
// re-ingest. Cancellation is the first line; the tombstone is the load-bearing one.
//
// A fence that only passes when BOTH hold cannot tell you which failed. So this
// section removes the first line deliberately — re-arming the delivery retry after
// the kill, from the outside, no source edit — and requires the second to hold on
// its own. That is the property a future fix to the self-delivery confirm actually
// needs, and section 3 alone would not have caught its absence.
{
  const { net, root, late, topicId, role, env, author, got } = await scene('kls-depth');
  const json = JSON.stringify(env);
  const kill = await buildKill({ topicId: idHex(topicId), msgId: env.msgId, ts: net.clock.t + 1, seq: 2, identity: author });
  await root._onKill({ topicId: idHex(topicId), kill }, { targetId: root.nodeId, isTerminal: true });
  await new Promise(r => setImmediate(r));

  // Undo the cancellation: the delivery leg behaves as though the kill never
  // reached it. This is the failure mode, staged rather than assumed.
  root._pendingPub.set(env.msgId, { topicBig: topicId, json, at: net.clock.t, tries: 0 });
  ok('4a. precondition — the delivery retry is armed again for a KILLED message',
    root._pendingPub.has(env.msgId));

  await tick(root, net, 3);
  ok('4b. the pump re-sent, and the ingest REFUSED it every time — the tombstone ' +
     'holds without help from the cancellation',
    !role.cacheIds.has(env.msgId) && role.tombstones.has(env.msgId),
    JSON.stringify({ cache: role.cache.length, tomb: role.tombstones.has(env.msgId) }));

  await subscribeLate(late, net, topicId);
  ok('4c. …so the late subscriber STILL receives no body, with the first line of ' +
     'defence deliberately disabled',
    bodies(got, env).length === 0, JSON.stringify(got.map(g => g.json?.slice(0, 60))));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
