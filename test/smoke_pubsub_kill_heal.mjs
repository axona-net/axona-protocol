// =====================================================================
// smoke_pubsub_kill_heal.mjs — kill routes like publish (hint + strand-heal).
//
// Regression for the ~30% "kill not received" flake: pubsubKill sent
// `via: []` (a bare greedy walk, NO root hint, NO retry) while pubsubPublish
// seeds the warm root hint AND retains the publish in _pendingPub so a stranded
// send is re-routed toward the true root once the background findKClosest
// resolves. A kill is a one-shot routed message too — without the hint it
// strands on the greedy walk just like a cold publish, and never re-routes on
// its own, so a stranded kill = a tombstone that never reaches subscribers.
//
// This pins: (1) a kill issued with a WARM hint carries via=[root];
//            (2) a kill issued COLD is retained and RE-SENT toward the root the
//                moment the background lookup resolves, then cleared.
//
// Run: node test/smoke_pubsub_kill_heal.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const idHex = (b) => b.toString(16).padStart(66, '0');

function mkManager({ closest }) {
  const selfId = 0x89n << 248n | 0x11n;
  const sends = [];
  const dht = {
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => { sends.push({ target, type, payload }); },
    neighbors: () => [],
    async findKClosest() { return closest != null ? [closest] : []; },
  };
  const am = new AxonaManager({ dht, now: () => Date.now(), renewMs: 60_000, dropMs: 180_000 });
  am.nodeId = selfId;
  return { am, sends };
}
const kills = (sends) => sends.filter(s => String(s.type).includes('kill'));

// ── 1. WARM hint → the kill carries via=[root] ───────────────────────
{
  const root = 0x89n << 248n | 0xabcdn;
  const { am, sends } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0xbeefn;
  await am.warmRootHint(topic, 1000);                 // seed the hint
  am.pubsubKill(topic, { msgId: 'm1' });
  const k = kills(sends);
  ok('warm kill emitted one KILL', k.length === 1);
  ok('warm kill carries via=[root] (not bare greedy)',
     k[0] && Array.isArray(k[0].payload.via) && k[0].payload.via[0] === idHex(root));
}

// ── 2. COLD → retained in _pendingKill, RE-SENT toward root on resolve ─
{
  const root = 0x89n << 248n | 0x1234n;
  const { am, sends } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0x5678n;
  am.pubsubKill(topic, { msgId: 'm2' });              // cold: hint null, kicks bg resolve
  const first = kills(sends);
  ok('cold kill sent immediately (greedy, via empty)',
     first.length === 1 && (first[0].payload.via || []).length === 0);
  ok('cold kill retained in _pendingKill', am._pendingKill && am._pendingKill.has(topic));
  await sleep(60);                                     // let the bg findKClosest resolve
  const after = kills(sends);
  ok('kill RE-SENT toward the resolved root', after.length === 2 &&
     Array.isArray(after[1].payload.via) && after[1].payload.via[0] === idHex(root));
  ok('_pendingKill cleared after the heal (no infinite re-send)', !am._pendingKill.has(topic));
}

console.log(`\n${fail ? '✗' : '✓'} smoke_pubsub_kill_heal: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
