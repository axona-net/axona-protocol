// =====================================================================
// smoke_bounded_queue.js — bounded replay queue + per-publisher quota
//                          (Phase A #4).
//
//   1. Deterministic eviction by signed (seq, ts, msgId) — NOT insertion
//      order — so replicas converge; maxMessages=1 = retained slot.
//   2. Per-publisher quota on OPEN topics: one signer can't flush the queue.
//   3. Owned topics: no quota (single owner may fill their own queue).
//   4. _openTopicQuota only fires for public-derived topic ids.
//
// Run: node test/smoke_bounded_queue.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { deriveTopicId }  from '../src/pubsub/post.js';
import { deriveIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

function stubDht() {
  return {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, sendDirect: async () => true, routeMessage: async () => {},
  };
}
const am = new AxonaManager({ dht: stubDht(), now: () => 1_700_000_000_000 });

function entry(seq, signer, ts = 0) {
  return { json: '{}', publishId: `p${seq}-${signer}`, postHash: `${signer}:${seq}`, seq, ts, signerPubkey: signer };
}
const seqs = (cache) => cache.map(e => e.seq).sort((a, b) => a - b);

function testOrderedEviction() {
  console.log('\n── eviction is by seq, not insertion order ──');
  const role = { replayCache: [], maxMessages: 3 };
  for (const s of [5, 1, 3, 2]) am._addToReplayCache(role, entry(s, 'A'));
  // FIFO would have dropped seq 5 (first in); ordered eviction drops seq 1 (lowest).
  check('bounded to maxMessages', role.replayCache.length === 3);
  check('lowest seq (1) evicted', !role.replayCache.some(e => e.seq === 1));
  check('survivors are the 3 highest seqs', JSON.stringify(seqs(role.replayCache)) === '[2,3,5]');
}

function testRetainedSlot() {
  console.log('\n── maxMessages = 1 is a retained / latest-value slot ──');
  const role = { replayCache: [], maxMessages: 1 };
  am._addToReplayCache(role, entry(10, 'A'));
  am._addToReplayCache(role, entry(20, 'A'));
  am._addToReplayCache(role, entry(15, 'A'));   // older seq than 20 → ignored on eviction
  check('exactly one slot', role.replayCache.length === 1);
  check('retains the highest seq (20)', role.replayCache[0].seq === 20);
}

function testPerPublisherQuotaOpen() {
  console.log('\n── open-topic per-publisher quota self-limits a flooder ──');
  const role = { replayCache: [], maxMessages: 8 };
  const quotaPerPublisher = 2;                  // ceil(8/4)
  for (const s of [1, 2, 3, 4, 5]) am._addToReplayCache(role, entry(s, 'FLOOD'), { quotaPerPublisher });
  for (const s of [6, 7])          am._addToReplayCache(role, entry(s, 'BOB'),   { quotaPerPublisher });

  const flood = role.replayCache.filter(e => e.signerPubkey === 'FLOOD');
  const bob   = role.replayCache.filter(e => e.signerPubkey === 'BOB');
  check('flooder capped at quota (2)', flood.length === 2);
  check('flooder keeps its highest seqs (4,5)', JSON.stringify(seqs(flood)) === '[4,5]');
  check('other publisher unaffected', bob.length === 2);
  check('legit publisher not evicted by the flood', bob.every(e => [6, 7].includes(e.seq)));
}

function testOwnedTopicNoQuota() {
  console.log('\n── owned topic (no quota): owner may fill their own queue ──');
  const role = { replayCache: [], maxMessages: 8 };
  for (const s of [1, 2, 3, 4, 5]) am._addToReplayCache(role, entry(s, 'OWNER'));  // no quota opt
  check('all 5 owner messages kept (under global max)', role.replayCache.length === 5);
}

async function testOpenTopicDetection() {
  console.log('\n── _openTopicQuota fires only for public-derived topic ids ──');
  const alice = await deriveIdentity({ lat: 51.5, lng: -0.1 });
  const role = { replayCache: [] };

  // Public topic: id === deriveTopicId(null, name).
  const pubHex = await deriveTopicId(null, 'room');
  const pubEnv = await buildEnvelope({ topic: 'room', message: 'x', sign: false, ts: 1, seq: 1 });
  const qOpen  = await am._openTopicQuota(role, JSON.stringify(pubEnv), BigInt('0x' + pubHex));
  check('public topic → quota number', typeof qOpen === 'number' && qOpen === Math.ceil(am.replayCacheSize / 4));

  // Owned topic: id derived from alice, not public.
  const ownHex = await deriveTopicId(alice.id, 'room');
  const ownEnv = await buildEnvelope({ topic: 'room', message: 'x', identity: alice, ts: 1, seq: 1 });
  const qOwned = await am._openTopicQuota(role, JSON.stringify(ownEnv), BigInt('0x' + ownHex));
  check('owned topic → no quota (null)', qOwned === null);
}

async function main() {
  console.log('Axona bounded queue + per-publisher quota (Phase A #4) smoke');
  testOrderedEviction();
  testRetainedSlot();
  testPerPublisherQuotaOpen();
  testOwnedTopicNoQuota();
  await testOpenTopicDetection();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
