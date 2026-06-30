// =====================================================================
// smoke_backlog_reannounce.mjs — cache-bearing-root re-announce (v4.9.x).
//
// History-recovery durability: a node that won root for a topic and holds cached
// history — but is NOT a deliberate host() and has NO app subscription (a publisher
// that published with no subscribers and incidentally rooted on its transient id) —
// must keep advertising that history toward the live root so a late since:'all'
// joiner can recover it. Each refreshTick it re-announces via _sendSubscribe,
// carrying its high-water; a strictly-closer root then PULLUPs the cache (migration).
// This generalises the hosted re-announce to ANY cache-bearing root, turning the
// one-shot beacon-demote into a continuous chase (closes the ZERO history-recovery
// failures seen in the soak).
//
//   1. cache-bearing, non-hosted, non-subscribed ROOT re-announces with hw>0
//   2. an EMPTY root (no cache) stays silent (nothing to preserve)
//   3. the re-announce carries the cache high-water (so a closer root PULLUPs)
//
// Run: node test/smoke_backlog_reannounce.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };

const SELF  = 0x87n << 248n | 0x11n;
const TOPIC = 0x87n << 248n | 0xabcn;
const idHex = (b) => b.toString(16).padStart(66, '0');

function mkManager() {
  const sends = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => { sends.push({ target, type, payload }); },
    neighbors: () => [],
    bridgeId: () => null,
    async findKClosest() { return []; },          // we are the only/closest node → SUB self-terminates
  };
  const am = new AxonaManager({ dht, now: () => Date.now() });
  am.nodeId = SELF;
  am._reannounceCacheRoots = true;   // feature is flag-gated (A/B); enable to test the mechanism
  return { am, sends };
}
const subsFor = (sends, topicBig) =>
  sends.filter(s => s.type === 'pubsub:sub' && s.payload?.topicId === idHex(topicBig));

// ── 1+3. cache-bearing non-hosted non-subscribed root → re-announces with hw ──
{
  const { am, sends } = mkManager();
  const role = am._becomeRoot(TOPIC);                              // isRoot, no subscribers
  am._cachePush(role, { msgId: 'm1', publishTs: 100, json: '{}', seq: 1 });
  am._cachePush(role, { msgId: 'm2', publishTs: 250, json: '{}', seq: 2 });
  sends.length = 0;                                                // ignore _becomeRoot's beacon
  await am.refreshTick();
  const subs = subsFor(sends, TOPIC);
  ok('cache-bearing root re-announces (emits a SUB for the topic)', subs.length >= 1);
  ok('re-announce carries the cache high-water (PULLUP trigger)', subs.some(s => s.payload.hw === 250));
}

// ── 2. empty root (no cache) stays silent — nothing to preserve ──
{
  const { am, sends } = mkManager();
  am._becomeRoot(TOPIC);                                           // isRoot, EMPTY cache
  sends.length = 0;
  await am.refreshTick();
  ok('empty root does NOT re-announce (no history to chase)', subsFor(sends, TOPIC).length === 0);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_backlog_reannounce: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
