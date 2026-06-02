// =====================================================================
// smoke_metrics_kfanout.js — peer.metrics must sweep the WHOLE K-closest
// root set, not just the single node a routed walk lands on.
//
// Regression: a publish replicates to all K roots (pubsub:publish-k), but
// the old requestMetrics did one routeMessage(topicId), reaching only the
// globally-closest root + its subscriber-children.  In an asymmetric mesh
// that root's replayCache can be empty/diverged while a sibling root (and
// the replay-on-subscribe path) holds the queue — so current_count read 0
// even though new subscribers received a full replay.  Also: the response's
// `subscribers` field was dropped on receive and never aggregated.
//
// This builds a tiny in-memory mesh of real AxonaManager instances where
// the routed walk lands on an EMPTY root and the queue lives on a SIBLING
// root, and asserts the fan-out finds it.
//
// Run: node test/smoke_metrics_kfanout.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { toHex }        from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

// ── In-memory mesh: nodeId(bigint) → AxonaManager, wired by sendDirect /
//    routeMessage / findKClosest over XOR distance to the topicId. ──────
class MockNet {
  constructor() { this.mgrs = new Map(); }

  kclosest(topicId, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => {
        const da = a ^ topicId, db = b ^ topicId;
        return da < db ? -1 : da > db ? 1 : 0;
      })
      .slice(0, K);
  }

  makeDht(selfId) {
    const net = this;
    const routed = new Map(), direct = new Map();
    const dht = {
      getSelfId: () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onDirectMessage: (t, h) => direct.set(t, h),
      onEvent: () => () => {},
      findKClosest: async (topicId, K) => net.kclosest(topicId, K),
      routeMessage: async (topicId, type, payload) => {
        const target = net.kclosest(topicId, 1)[0];
        const m = net.mgrs.get(target);
        const h = m?._dht._routed.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
      },
      sendDirect: async (target, type, payload) => {
        const m = net.mgrs.get(target);              // fake children → no-op
        if (!m) return false;
        const h = m._dht._direct.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _routed: routed, _direct: direct,
    };
    return dht;
  }

  spawn(selfId) {
    const dht = this.makeDht(selfId);
    const mgr = new AxonaManager({ dht, now: () => 1_700_000_000_000 });
    mgr._dht = dht;                                  // tests reach handlers via this
    this.mgrs.set(selfId, mgr);
    return mgr;
  }
}

// Give a manager a hosted role: `n` live cache entries (unowned anchor →
// readable by anyone) and `subs` direct subscriber-children.
function hostRole(mgr, topicId, n, subs) {
  const replayCache = [];
  for (let i = 0; i < n; i++) replayCache.push({ json: '{}', postHash: `h${i}`, publisher: undefined });
  const children = new Map();
  for (let i = 0; i < subs; i++) children.set(BigInt(1000 + i), {});
  mgr.axonRoles.set(topicId, { isRoot: true, replayCache, children });
}

// Mirror AxonaPeer.metrics() aggregation over the accumulated responses.
function aggregate(responses) {
  let current_count = 0, subscribers = 0;
  for (const r of responses) {
    if (typeof r?.current_count === 'number') current_count = Math.max(current_count, r.current_count);
    if (typeof r?.subscribers   === 'number') subscribers   = Math.max(subscribers,   r.subscribers);
  }
  return { current_count, subscribers, relayCount: responses.length };
}

async function testFanOutFindsSiblingRoot() {
  console.log('\n── metrics sweeps the whole K-root set (queue on a sibling, not the routed root) ──');
  const net = new MockNet();
  // topicId == A's id, so the routed walk lands on A (closest).  We leave A
  // EMPTY and put the live queue on sibling root C.
  const A = net.spawn(0x01n);   // closest to topicId → routeMessage target
  const B = net.spawn(0x02n);
  const C = net.spawn(0x04n);
  const R = net.spawn(0xA0n);   // requester (not a root holder)
  const topicId = 0x01n;

  hostRole(A, topicId, 0, 0);          // routed root: EMPTY cache, no subs
  hostRole(B, topicId, 0, 0);
  hostRole(C, topicId, 3, 2);          // sibling root: 3 live events, 2 subs
  // R hosts no role for this topic.

  const responses = await R.requestMetrics(topicId, null, { timeoutMs: 50 });
  await new Promise(r => setTimeout(r, 60));   // let timeout fire / responses settle
  const m = aggregate(responses);

  check('routed-only would miss it (A is the routed target and is empty)',
    A.axonRoles.get(topicId).replayCache.length === 0);
  check('current_count = 3 (found on sibling root C via fan-out)', m.current_count === 3);
  check('subscribers = 2 (carried through the round trip)',        m.subscribers === 2);
  check('heard from multiple roots',                               m.relayCount >= 2);
}

async function testRequesterIsARoot() {
  console.log('\n── requester is itself a root: own cache folded in without a self-send ──');
  const net = new MockNet();
  const A = net.spawn(0x01n);
  const R = net.spawn(0x02n);   // requester AND a root holding the queue
  const topicId = 0x01n;        // A is closest; R is a sibling root

  hostRole(A, topicId, 0, 0);
  hostRole(R, topicId, 5, 4);   // requester's own cache

  const responses = await R.requestMetrics(topicId, null, { timeoutMs: 50 });
  await new Promise(r => setTimeout(r, 60));
  const m = aggregate(responses);

  check('current_count = 5 (self-root counted locally)', m.current_count === 5);
  check('subscribers = 4 (self-root)',                   m.subscribers === 4);
  check('no duplicate self-response',
    responses.filter(r => r.responderId === toHex(0x02n)).length === 1);
}

async function main() {
  console.log('Axona metrics K-closest fan-out smoke');
  await testFanOutFindsSiblingRoot();
  await testRequesterIsARoot();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
