// =====================================================================
// smoke_kill_resurrection.js — regression guard for the user-reported
// "kill, then reload, and the message comes back" bug, and its fix.
//
// Root cause (replica divergence): a topic is replicated across a cohort of
// root/backup axons, but the tombstone from a kill converges through the
// anti-entropy plane (cohort REPLICATE → union-ingest). If even ONE holder
// misses that convergence — a dropped REPLICATE, churn between publish- and
// kill-time K-closest, or a holder that joined the set late — that holder keeps
// the message AND has no tombstone. On a fresh reload the subscriber's in-memory
// tombstone set is empty, so the stale holder's replay-on-subscribe resurrects it.
//
// Fix (cohort anti-entropy): a holder that applied the kill re-pushes its full
// state — cache PLUS active tombstones (`dels`) — to the K-closest cohort every
// refreshTick (and eagerly the instant a kill lands) via _replicateRole. A holder
// that missed the original kill ingests the tombstone (UNION_AT_ROOT → _applyDels
// → _applyKill), which removes the body from its cache and tombstones it. The
// receive-side tombstone gate (_tombstoned, checked in _ingestStamped / _onDeliver)
// is the standing backstop: a killed body can never re-enter a tombstoned holder's
// cache, whatever path re-offers it.
//
// This test reproduces the divergence, proves the reloaded subscriber resurrects
// the message off the stale holder, runs the anti-entropy path, and proves the
// stale holder is healed and a later reload no longer resurrects the message.
//
// Run: node test/smoke_kill_resurrection.js
//   SKIP_RECONCILE=1 node test/smoke_kill_resurrection.js  → red check: 4 + 5 fail
// =====================================================================

import { AxonaManager }        from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }       from '../src/pubsub/envelope.js';
import { buildKill }           from '../src/pubsub/kill.js';
import { deriveTopicIdBig }    from '../src/pubsub/post.js';
import { idHex }               from '../src/pubsub/ids.js';
import { sealTestDht }         from './lib/testCapability.mjs';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const T            = 1_700_000_000_000;               // fixed test clock
const SKIP_RECONCILE = process.env.SKIP_RECONCILE === '1';
const DESC         = { region: 'useast', owner: null, name: 'cats', write: 'open' };
const REPLICATE    = 'pubsub:replicate';              // T.REPLICATE — the anti-entropy carrier we drop
const DELIVER      = 'pubsub:deliver';

// ── In-memory mesh of real AxonaManager instances, wired by routeMessage /
//    findKClosest over XOR distance (mirrors smoke_pubsub_kill.mjs's Fabric).
//    Routed frames are queued and drained by settle() so async eager-replicate
//    and ingest cascades never recurse. `dropReplicateTo`: a holder that never
//    receives the tombstone-bearing REPLICATE — the modelled missed convergence. ─
class MockNet {
  constructor(rootReplicas) {
    this.mgrs = new Map();               // nodeIdBig -> AxonaManager
    this.routed = new Map();             // nodeIdBig -> Map(type -> handler)
    this.queue = [];
    this.errors = [];
    this.dropReplicateTo = null;         // nodeIdBig whose REPLICATE deliveries are dropped
    this.rootReplicas = rootReplicas;
  }

  kclosest(targetBig, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => { const da = a ^ targetBig, db = b ^ targetBig; return da < db ? -1 : da > db ? 1 : 0; })
      .slice(0, K);
  }

  makeDht(selfId) {
    const net = this;
    const routed = new Map();
    net.routed.set(selfId, routed);
    return {
      getSelfId:       () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onEvent:         () => () => {},
      neighbors:       () => [],
      verdictsSupported: false,          // audited: returns consumed/undefined, never a routing verdict
      findKClosest:    async (targetBig, K) => net.kclosest(targetBig, K),
      // Route toward the node closest to `targetBig`. Every frame we drive here
      // targets a concrete node id (DELIVER→subscriber, REPLICATE→cohort member),
      // so the closest node IS that node. Enqueue + report consumed; settle()
      // delivers. A REPLICATE aimed at the divergent holder is silently dropped.
      routeMessage:    async (targetBig, type, payload) => {
        const dest = net.kclosest(targetBig, 1)[0];
        if (dest == null) return { consumed: false };
        if (type === REPLICATE && net.dropReplicateTo != null && dest === net.dropReplicateTo) {
          return { consumed: false };    // missed convergence
        }
        if (!net.routed.get(dest)?.has(type)) return { consumed: false };
        net.queue.push({ dest, type, payload, meta: { targetId: dest, isTerminal: true, hopCount: 1, fromId: idHex(selfId) } });
        return { consumed: true };
      },
    };
  }

  spawn(selfId, tag) {
    const dht = this.makeDht(selfId);
    const mgr = new AxonaManager({ dht: sealTestDht(dht), now: () => T, rootReplicas: this.rootReplicas });
    this.mgrs.set(selfId, mgr);
    return mgr;
  }

  async settle(cap = 500000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle cap');
      const j = this.queue.shift();
      const h = this.routed.get(j.dest)?.get(j.type);
      if (h) { try { await h(j.payload, j.meta); } catch (e) { this.errors.push(e); } }
    }
  }

  // Drain the queue AND every async cascade it spawns (eager replicate, the
  // time-sliced ingest pump), flushing microtasks between passes.
  async drain(rounds = 8) {
    for (let i = 0; i < rounds; i++) {
      await new Promise(r => setImmediate(r));
      await this.settle();
      for (const m of this.mgrs.values()) { try { await m._ingestIdle?.(); } catch { /* */ } }
    }
  }
}

const role     = (r, t) => r.axonRoles.get(t);
const hasMsg   = (r, t, id) => (role(r, t)?.cache || []).some(c => c.msgId === id);
const tombed   = (r, t, id) => !!role(r, t)?.tombstones?.has(id);

async function main() {
  console.log('Axona kill-resurrection regression (replica divergence + anti-entropy convergence)');

  const alice     = await createAuthorIdentity();
  const TOPIC_BIG = await deriveTopicIdBig(DESC);      // the REAL derived topic id (msgId excludes topic)
  const TOPIC_HEX = idHex(TOPIC_BIG);
  const env       = await buildEnvelope({ topic: DESC, message: 'hi', identity: alice, ts: T, seq: 1 });
  const json      = JSON.stringify(env);

  // Five roots XOR-closest to the topic form the durability cohort; A is far, so
  // it is never a root — it is only the source of the (authorized) kill.
  const net     = new MockNet(/* rootReplicas */ 4);   // cohort of the 4 nearest holders
  const rootIds = [1n, 3n, 5n, 9n, 17n].map(x => TOPIC_BIG ^ x);
  const roots   = rootIds.map((id, i) => net.spawn(id, `root${i}`));
  net.spawn(TOPIC_BIG ^ (1n << 200n), 'A');            // killer, far ⇒ never a root

  // ── Seed a fully-replicated publish: every root holds the message in cache. ──
  for (const r of roots) {
    const role = r._becomeRoot(TOPIC_BIG, 'seed');     // real root role (all fields present)
    role.lastTs = T; role.seq = 1;
    r._cachePush(role, { msgId: env.msgId, publishTs: T, json, seq: 1 });
  }
  check('1. all 5 roots initially hold the message', roots.every(r => hasMsg(r, TOPIC_BIG, env.msgId)));

  // A fresh subscriber reload: a brand-new node with an EMPTY tombstone set that
  // re-subscribes and is served each root's replay-on-subscribe (_replayTo). It
  // is a pure app sink (no role → no local suppression), exactly the "empty
  // in-memory tombstone set" a reload starts from. Returns the delivered bodies.
  async function reloadServe(servers, label) {
    const B = net.spawn(TOPIC_BIG ^ (1n << 201n), label);
    B.mySubscriptions.set(TOPIC_BIG, { since: 0, lastRenewSent: 0, interval: 5000 });
    const got = [];
    B.onPubsubDelivery((_t, j) => { try { const o = JSON.parse(j); if (!o?.deleted) got.push(o); } catch { /* */ } });
    for (const r of servers) r._replayTo(role(r, TOPIC_BIG), idHex(B.nodeId), 0, true, false);
    await net.drain();
    net.mgrs.delete(B.nodeId); net.routed.delete(B.nodeId);
    return got.filter(o => o.msgId === env.msgId);
  }

  // ── Kill from alice reaches the cohort EXCEPT one holder. We deliver the kill
  //    to each healthy root (its receive path: _onKill → verify → authorship →
  //    _applyKill), while dropping the tombstone-bearing REPLICATE to the stale
  //    root so its eager cohort re-push can't heal it — the modelled divergence. ─
  const STALE = 2;
  net.dropReplicateTo = rootIds[STALE];
  const kill = await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: 1, identity: alice });
  for (let i = 0; i < roots.length; i++) {
    if (i === STALE) continue;
    await roots[i]._onKill({ topicId: TOPIC_HEX, via: [], kill }, { targetId: rootIds[i], isTerminal: true, fromId: idHex(TOPIC_BIG ^ (1n << 200n)) });
  }
  await net.drain();

  const others    = roots.filter((_, i) => i !== STALE);
  const staleRoot = roots[STALE];
  check('2a. roots that got the kill removed + tombstoned the message',
    others.every(r => !hasMsg(r, TOPIC_BIG, env.msgId) && tombed(r, TOPIC_BIG, env.msgId)));
  check('2b. the divergent root is STALE: still holds it, no tombstone',
    hasMsg(staleRoot, TOPIC_BIG, env.msgId) && tombed(staleRoot, TOPIC_BIG, env.msgId) === false);

  // ── Without convergence, a reloaded subscriber resurrects the message (the bug). ──
  const beforeAll     = await reloadServe(roots, 'B-before-all');
  const beforeHealthy = await reloadServe(others, 'B-before-healthy');
  check('3. divergence is real: a reloaded subscriber resurrects the message',
    beforeAll.length === 1);
  check('3b. the STALE root is the source: replaying the killed cohort alone yields nothing',
    beforeHealthy.length === 0);

  // ── FIX: a holder that applied the kill re-pushes cache+tombstones to the
  //    cohort (the anti-entropy plane, _replicateRole). The stale holder ingests
  //    the tombstone (UNION_AT_ROOT → _applyDels), dropping the body + tombstoning. ─
  net.dropReplicateTo = null;                          // the transient drop has cleared
  if (!SKIP_RECONCILE) {
    for (const r of others) await r._replicateRole(TOPIC_BIG, role(r, TOPIC_BIG), null, T);
    await net.drain();
  }
  check('4. anti-entropy healed the stale root (message removed + tombstoned)',
    !hasMsg(staleRoot, TOPIC_BIG, env.msgId) && tombed(staleRoot, TOPIC_BIG, env.msgId) === true);

  // ── Now a fresh reload sees NO resurrection. ──
  const after = await reloadServe(roots, 'B-after');
  check('5. FIX VERIFIED: a reloaded subscriber no longer resurrects the message',
    after.length === 0);

  // ── Receive-side backstop: a killed body re-offered to a tombstoned holder
  //    (a stale replica pushing the body back through the verified ingest path)
  //    is suppressed by the tombstone gate — it never re-enters the cache. This
  //    is what keeps the heal durable under continued anti-entropy churn. ──
  const guinea = roots[0];                             // healed: tombstoned, body dropped
  const tally  = await guinea._ingestStampedBatch(role(guinea, TOPIC_BIG),
    [{ msgId: env.msgId, publishTs: T, json, seq: 1 }]);
  check('6. receive-side backstop: a tombstoned holder refuses to re-cache the killed body',
    tally.held === 1 && !hasMsg(guinea, TOPIC_BIG, env.msgId) && tombed(guinea, TOPIC_BIG, env.msgId));

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
