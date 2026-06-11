// =====================================================================
// smoke_kill_resurrection.js — DIAGNOSTIC repro for the user-reported
// "kill, then reload, and the message comes back" bug.
//
// Hypothesis (replica divergence): a topic is replicated across R root
// axons, but a kill is best-effort-pushed to the K-closest set. If even
// ONE root misses the kill (routed-fallback single target, a sendDirect
// failure, or churn between publish-time and kill-time K-closest), that
// root keeps the message AND has no tombstone. On a fresh reload the
// subscriber's in-memory tombstone set is empty, so when the stale root
// replays, nothing suppresses it → the message resurrects.
//
// This test reproduces it deterministically: 5 roots all hold the message,
// the kill is DROPPED to exactly one root, then a freshly-reloaded
// subscriber re-subscribes and receives the killed message again. It also
// verifies the new replay-batch instrumentation names the culprit root.
//
// NOTE: this asserts the bug is PRESENT (resurrection observed). When the
// fix lands (send-side tombstone filter + full-fanout kill + replica
// reconciliation), check #3 flips to "no resurrection" — update then.
//
// Run: node test/smoke_kill_resurrection.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { deriveIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { buildKill }      from '../src/pubsub/kill.js';
import { toHex }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON    = { lat: 51.5074, lng: -0.1278 };
const TOPIC_HEX = '89' + 'ab'.repeat(32);          // 66-char hex topic id
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const T         = 1_700_000_000_000;               // fixed test clock
const tick      = () => new Promise(r => setTimeout(r, 0));

// ── In-memory mesh of real AxonaManager instances, wired by sendDirect /
//    routeMessage / findKClosest over XOR distance (mirrors smoke_metrics_
//    kfanout). Adds `dropKillTo`: a root that never receives `pubsub:kill-k`. ──
class MockNet {
  constructor() { this.mgrs = new Map(); this.dropKillTo = null; this.logs = []; }

  kclosest(topicId, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => { const da = a ^ topicId, db = b ^ topicId; return da < db ? -1 : da > db ? 1 : 0; })
      .slice(0, K);
  }

  makeDht(selfId) {
    const net = this;
    const routed = new Map(), direct = new Map();
    const dht = {
      getSelfId:       () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onDirectMessage: (t, h) => direct.set(t, h),
      onEvent:         () => () => {},
      findKClosest:    async (topicId, K) => net.kclosest(topicId, K),
      routeMessage:    async (topicId, type, payload) => {
        const target = net.kclosest(topicId, 1)[0];
        const h = net.mgrs.get(target)?._dht._routed.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
      },
      sendDirect:      async (target, type, payload) => {
        if (type === 'pubsub:kill-k' && net.dropKillTo != null && target === net.dropKillTo) return false; // simulate missed delivery
        const m = net.mgrs.get(target);
        if (!m) return false;
        const h = m._dht._direct.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _routed: routed, _direct: direct,
    };
    return dht;
  }

  spawn(selfId, tag) {
    const dht = this.makeDht(selfId);
    const net = this;
    const mgr = new AxonaManager({
      dht, now: () => T,
      emitLog: (level, code, data) => net.logs.push({ node: tag, code, ...data }),
    });
    mgr._dht = dht;
    this.mgrs.set(selfId, mgr);
    return mgr;
  }
}

async function main() {
  console.log('Axona kill-resurrection diagnostic (replica divergence)');

  const alice = await deriveIdentity(LONDON);
  const env   = await buildEnvelope({ topic: 'cats', message: 'hi', identity: alice, ts: T, seq: T });
  const json  = JSON.stringify(env);

  const net = new MockNet();
  // 5 roots: nodeIds close to the topic (small XOR) so they are the K-closest.
  const rootIds = [1n, 3n, 5n, 9n, 17n].map(x => TOPIC_BIG ^ x);
  const roots   = rootIds.map((id, i) => net.spawn(id, `root${i}`));
  // A (killer) and B (subscriber): far from the topic → never roots themselves.
  const A = net.spawn(TOPIC_BIG ^ (1n << 200n), 'A');

  // ── Seed: a fully-replicated publish — every root holds the message. ──
  for (const r of roots) {
    r.axonRoles.set(TOPIC_BIG, {
      isRoot: true, isInRootSet: true, children: new Map(),
      replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null }],
    });
  }
  check('1. all 5 roots initially hold the message',
    roots.every(r => r.axonRoles.get(TOPIC_BIG).replayCache.length === 1));

  // ── Kill from A, but DROP the kill to root index 2. ──
  const STALE = 2;
  net.dropKillTo = rootIds[STALE];
  const kill = await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice });
  await A._asyncKill(TOPIC_BIG, kill);
  await tick(); await tick();                       // flush the fire-and-forget kill-k sends

  const killedRoots  = roots.filter((_, i) => i !== STALE);
  const staleRoot    = roots[STALE];
  check('2a. roots that received the kill removed the message',
    killedRoots.every(r => r.axonRoles.get(TOPIC_BIG).replayCache.length === 0));
  check('2b. roots that received the kill tombstoned it',
    killedRoots.every(r => r._isTombstoned(env.msgId) === true));
  check('2c. the dropped root STILL holds the message (no tombstone)',
    staleRoot.axonRoles.get(TOPIC_BIG).replayCache.length === 1 &&
    staleRoot._isTombstoned(env.msgId) === false);

  // ── B "reloads": a brand-new subscriber with an empty tombstone set. It
  //    re-subscribes to the K-closest roots; each replays what it holds. ──
  const B = net.spawn(TOPIC_BIG ^ (1n << 201n), 'B');
  const bDeliveries = [];
  B.onPubsubDelivery((_t, j) => { try { bDeliveries.push(JSON.parse(j)); } catch {} });
  net.logs.length = 0;                              // capture only the reload's replay logs
  for (const r of roots) {
    const role = r.axonRoles.get(TOPIC_BIG);
    await r._maybeSendReplay(TOPIC_BIG, role, B.nodeId, 0);   // lastSeenTs=0 → full backfill
  }
  await tick();

  const resurrected = bDeliveries.filter(d => !d.deleted && d.msgId === env.msgId);
  check('3. BUG REPRODUCED: reloaded B receives the killed message again',
    resurrected.length === 1);

  // ── The instrumentation must name the stale root as the source. ──
  const serveLog = net.logs.find(l => l.node === 'B' && l.code === 'replay-serve' && l.msgId === env.msgId);
  check('4a. instrumentation logged replay-serve for the resurrected msg',
    !!serveLog);
  check('4b. ...and `from` is exactly the stale root (culprit identified)',
    !!serveLog && serveLog.from === toHex(rootIds[STALE]));

  // ── Control: a subscriber that did NOT reload (holds the tombstone) is
  //    protected — proves the reload (empty tombstone set) is the trigger. ──
  const noReload = roots[0];                        // a root that saw the kill ⇒ has the tombstone
  net.logs.length = 0;
  noReload._onReplayBatch(
    { topicId: TOPIC_HEX, responderId: toHex(rootIds[STALE]),
      messages: [{ json, publishId: 'p9', publishTs: T, postHash: env.msgId, publisher: null }] },
    { fromId: toHex(rootIds[STALE]) });
  const skipped = net.logs.some(l => l.code === 'replay-skip-tombstoned' && l.msgId === env.msgId);
  check('5. a node holding the tombstone suppresses the same stale replay',
    skipped === true);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
