// =====================================================================
// fence_pending_per_topic.mjs — a publisher's pending publish/kill retry is
// per (topic, msgId), not per msgId (4.156.0).
//
// A msgId is sha256(publisher, message). It carries no topic. One body sent
// to several topics (Civil Defense sends each alert to all 15 containing
// cells) therefore has ONE msgId on every topic. Before 4.156.0 the pending
// maps were keyed by msgId alone:
//   - each publish overwrote the one entry; the last topic won;
//   - every topic's early re-send pump read json from that shared entry and
//     sent the LAST topic's envelope to its own topic, which the root drops
//     as drop-topic-mismatch (Lyra night 11-18: 10 drops per sibling cell);
//   - the other topics had no retry of their own envelope at all;
//   - seeing the message on any one topic confirmed all of them.
// Each check below fails on 4.154.0 and passes on 4.156.0.
//
// Run: node test/fence_pending_per_topic.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { pendingKey } from '../src/pubsub/ids.js';

let n = 0, fail = 0;
const ok = (m, c, d = '') => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m} ${d}`); fail++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function mkManager() {
  const selfId = 0x80n << 248n | 0x11n;
  const sends = [];
  const dht = {
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    verdictsSupported: false,
    routeMessage: (target, type, payload) => { sends.push({ target, type, payload }); },
    neighbors: () => [],                       // no neighbours → a COLD publisher (the burst plan)
    async findKClosest() { return []; },
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => Date.now(), renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
  am.nodeId = selfId;
  return { am, sends };
}
const pubs = (sends) => sends.filter((s) => String(s.type).includes('pub'));
const kills = (sends) => sends.filter((s) => String(s.type).includes('kill'));

// Three cell topics, one body, one msgId; each topic's envelope names its own cell.
const topics = [0x80n << 248n | 0xa1n, 0x80n << 248n | 0xa2n, 0x80n << 248n | 0xa3n];
const MSG = 'same-body-msgid';
const envFor = (t) => JSON.stringify({ msgId: MSG, topic: idHex(t), message: 'fire' });

// ── 1. three publishes of one msgId keep three entries ─────────────────
{
  const { am } = mkManager();
  for (const t of topics) am.pubsubPublish(t, envFor(t));
  ok('1a. one pending entry per topic for a shared msgId', am._pendingPub.size === 3, `size=${am._pendingPub.size}`);
  ok('1b. each entry holds its own topic\'s envelope',
    topics.every((t) => am._pendingPub.get(pendingKey(t, MSG))?.json === envFor(t)));
  for (const t of topics) am._confirmPending(t, MSG);
}

// ── 2. the cold burst re-sends each topic's OWN envelope ───────────────
{
  const { am, sends } = mkManager();
  for (const t of topics) am.pubsubPublish(t, envFor(t));
  await sleep(3400);                            // 5 × 200 ms + 5 × 400 ms burst, with margin
  const p = pubs(sends);
  const wrong = p.filter((s) => JSON.parse(s.payload.json).topic !== s.payload.topicId);
  const perTopic = topics.map((t) => p.filter((s) => s.payload.topicId === idHex(t)).length);
  ok('2a. no re-send carries another topic\'s envelope', wrong.length === 0, `wrong=${wrong.length} of ${p.length}`);
  ok('2b. every topic got its burst (more than the first send)', perTopic.every((c) => c > 1), `perTopic=${perTopic}`);
  for (const t of topics) am._confirmPending(t, MSG);
}

// ── 3. confirm is per topic ────────────────────────────────────────────
{
  const { am } = mkManager();
  for (const t of topics) am.pubsubPublish(t, envFor(t));
  am._confirmPending(topics[0], MSG);           // seen on the first cell only
  ok('3a. confirming one topic removes only that topic\'s entry',
    !am._pendingPub.has(pendingKey(topics[0], MSG)) &&
    am._pendingPub.has(pendingKey(topics[1], MSG)) && am._pendingPub.has(pendingKey(topics[2], MSG)));
  am._confirmPending(null, MSG);                // no topic named → msgId-wide fallback
  ok('3b. a confirm without a topic clears every entry for the msgId', am._pendingPub.size === 0, `size=${am._pendingPub.size}`);
}

// ── 4. the tick retry re-sends each surviving topic its own envelope ───
{
  const { am, sends } = mkManager();
  for (const t of topics) am.pubsubPublish(t, envFor(t));
  am._confirmPending(topics[0], MSG);
  const before = pubs(sends).length;
  await am.refreshTick();
  const after = pubs(sends).slice(before);
  const got = new Set(after.map((s) => s.payload.topicId));
  ok('4a. the tick retries both unconfirmed topics', got.has(idHex(topics[1])) && got.has(idHex(topics[2])), `topics=${[...got].map((x) => x.slice(-2))}`);
  ok('4b. the tick does not retry the confirmed topic', !got.has(idHex(topics[0])));
  ok('4c. every tick re-send carries its own topic\'s envelope',
    after.every((s) => JSON.parse(s.payload.json).topic === s.payload.topicId));
  am._confirmPending(null, MSG);
}

// ── 5. kills: one kill msgId on two topics keeps two entries ───────────
{
  const { am, sends } = mkManager();
  am.pubsubKill(topics[0], { msgId: 'k-shared', cell: idHex(topics[0]) });
  am.pubsubKill(topics[1], { msgId: 'k-shared', cell: idHex(topics[1]) });
  ok('5a. one pending kill per topic for a shared kill msgId', am._pendingKill.size === 2, `size=${am._pendingKill.size}`);
  const before = kills(sends).length;
  await am.refreshTick();
  const after = kills(sends).slice(before);
  ok('5b. each kill re-send carries its own topic\'s kill',
    after.length >= 2 && after.every((s) => s.payload.kill.cell === s.payload.topicId), `n=${after.length}`);
  am._confirmPending(topics[0], 'k-shared');
  ok('5c. confirming one topic\'s kill leaves the other pending',
    !am._pendingKill.has(pendingKey(topics[0], 'k-shared')) && am._pendingKill.has(pendingKey(topics[1], 'k-shared')));
}

console.log(`\n${fail ? '✗' : '✓'} fence_pending_per_topic: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
