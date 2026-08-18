// smoke_read_routing.mjs — v4.10.1 read/host path routing (cohort-aware).
//
// pull used a bare greedy via:[] and so stranded on a local minimum, reaching a
// non-cohort node (false "no message"). The WRITE/READ path (pull/kill/publish)
// routes via the warm lookup-assist hint so it reaches a node that serves the
// topic's cohort. The SUBSCRIBE path does NOT (v4.64.0): it routes greedy and lets
// the neuromorphic layer pick each hop, so a stale root hint can't force a
// resubscribe down a path the mesh has since restructured around.
//
//   1. requestPull with a warm root hint routes the PULL to that root (not greedy)
//   2. requestPull with no hint falls back to greedy toward the topic (cold path OK)
//   3. pubsubHost routes its announce via _sendSubscribe, which goes GREEDY toward
//      the topic id even with a hint present, and registers the hosted topic
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };
const REG = 0x87n << 248n, idHex = (b) => b.toString(16).padStart(66, '0'), lc = (s) => s.toLowerCase();
const SELF = REG | 0x11n, ROOT = REG | 0xab0n, T1 = REG | 0xabcn, T2 = REG | 0xdefn;

function mk() {
  const sends = [];
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: false,   // audited: returns a push-count / undefined, never a verdict
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => [],
    bridgeId: () => null,
    async findKClosest() { return []; },
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t }); am.nodeId = SELF;
  return { am, sends, clock };
}
// pre-warm the root hint the cheap, deterministic way: a live root beacon.
const warmHint = (am, topicBig, rootBig) =>
  am._rootBeacons.set(topicBig, { root: lc(idHex(rootBig)), exp: am._now() + 3_600_000, seq: 1 });

// ── 1. pull WITH a warm hint → PULL routed to the root ──
{
  const { am, sends } = mk();
  warmHint(am, T1, ROOT);
  am.requestPull(T1, null, { timeoutMs: 50 });
  const pull = sends.find(s => s.type === 'pubsub:pull');
  ok('requestPull emits a PULL', !!pull);
  ok('PULL is routed to the hinted root (not greedy)', pull?.target === ROOT && pull?.payload?.via?.[0] === lc(idHex(ROOT)));
}

// ── 2. pull with NO hint → greedy toward the topic (cold fallback) ──
{
  const { am, sends } = mk();
  am.requestPull(T1, null, { timeoutMs: 50 });
  const pull = sends.find(s => s.type === 'pubsub:pull');
  ok('cold pull falls back to greedy toward the topic', !!pull && pull.target === T1 && (pull.payload.via || []).length === 0);
}

// ── 3. host routes via _sendSubscribe, which as of v4.64.0 goes GREEDY toward the
//      bare topic id even with a warm hint present — the neuromorphic layer routes
//      each hop to the current-best terminal, so the SUB is NOT via-pinned to the
//      cached root (a stale pin would fight mesh restructuring on resubscribe).
//      Distinct from PULL above, which still uses the write-path hint.
{
  const { am, sends } = mk();
  warmHint(am, T2, ROOT);   // hint present — and deliberately ignored by the SUB
  am.pubsubHost(T2);
  const sub = sends.find(s => s.type === 'pubsub:sub');
  ok('pubsubHost emits a SUB (via _sendSubscribe, not a bare host-send)', !!sub);
  ok('host SUB routes greedy — no via pin despite the warm hint (v4.64.0)',
     sub?.target === T2 && (sub.payload.via || []).length === 0);
  ok('hosted topic is registered', am._hostedTopics.has(T2));
}

console.log(`\n${fail ? '✗' : '✓'} smoke_read_routing: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
