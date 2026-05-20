// =====================================================================
// smoke_public_topics.mjs — verify public-topic mode (issue #47).
//
// Application choice between two topic-naming schemes:
//
//   · Publisher-keyed (default).  Topic ID = [publisher S2][SHA-256(publisher:topicName)].
//     Two publishers with the same topicName produce DIFFERENT topic IDs.
//     Verifiable provenance via signed envelopes; routing locality follows
//     the publisher's geographic prefix.
//
//   · Public.  Topic ID = '00' || SHA-256(topicName).
//     Anyone-can-publish, anyone-can-subscribe.  S2 prefix is 0x00 (global
//     bucket, no geographic anchor).  Signed envelopes still carry
//     signerPubkey — subscribers can still verify per-message provenance,
//     they just can't tie the TOPIC to a single publisher.
//
// Public mode is opt-in: pass `{ publisher: null }` to peer.pub/sub/pull/metrics.
//
// Run:  node test/smoke_public_topics.mjs
// =====================================================================

import {
  deriveTopicId,
  sha256Hex,
} from '../src/index.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const ALICE = '2e' + '0a'.repeat(32);   // 66-char hex, S2 prefix 2e
const BOB   = '7f' + 'cd'.repeat(32);   // 66-char hex, S2 prefix 7f
const TOPIC = 'news';

async function testPublisherKeyedSeparate() {
  console.log('\n── publisher-keyed: same topicName, different publishers → different IDs ──');
  const aliceTopic = await deriveTopicId(ALICE, TOPIC);
  const bobTopic   = await deriveTopicId(BOB,   TOPIC);

  check('alice topic is 66 hex chars',   aliceTopic.length === 66);
  check('bob topic is 66 hex chars',     bobTopic.length === 66);
  check('alice topic carries her S2 prefix (2e)', aliceTopic.slice(0, 2) === '2e');
  check('bob topic carries his S2 prefix (7f)',   bobTopic.slice(0, 2) === '7f');
  check('different publishers → different topic IDs',
    aliceTopic !== bobTopic);
}

async function testPublicMode() {
  console.log('\n── public mode: publisher=null → simple sha256(topicName) ──');
  const aliceAsPublic = await deriveTopicId(null, TOPIC);
  const bobAsPublic   = await deriveTopicId(null, TOPIC);

  check('public topic is 66 hex chars',          aliceAsPublic.length === 66);
  check('public topic S2 prefix is 00',          aliceAsPublic.slice(0, 2) === '00');
  check('two callers compute the SAME public ID', aliceAsPublic === bobAsPublic);

  // Cross-check the hash: '00' || sha256(topicName)
  const expected = '00' + (await sha256Hex(TOPIC));
  check('public ID is "00" + sha256(topicName)', aliceAsPublic === expected);
}

async function testPublicVsKeyed() {
  console.log('\n── public ≠ keyed (so they cannot collide accidentally) ──');
  const aliceKeyed = await deriveTopicId(ALICE, TOPIC);
  const publik     = await deriveTopicId(null,  TOPIC);
  check('public topic ID differs from keyed topic ID',
    aliceKeyed !== publik);
}

async function testEdgeCases() {
  console.log('\n── input validation ──');
  let threw = false;
  try { await deriveTopicId(null, ''); } catch { threw = true; }
  check('empty topicName throws (with public mode)', threw);

  threw = false;
  try { await deriveTopicId(null, undefined); } catch { threw = true; }
  check('undefined topicName throws (with public mode)', threw);

  threw = false;
  try { await deriveTopicId('not-66-chars', TOPIC); } catch { threw = true; }
  check('publisher not 66 chars throws',           threw);

  // null and undefined and '' all collapse to public mode
  const a = await deriveTopicId(null,      TOPIC);
  const b = await deriveTopicId(undefined, TOPIC);
  const c = await deriveTopicId('',        TOPIC);
  check('null/undefined/empty publisher all produce the same public ID',
    a === b && b === c);
}

async function main() {
  console.log('Public-topic mode (#47): deriveTopicId with optional publisher\n');

  await testPublisherKeyedSeparate();
  await testPublicMode();
  await testPublicVsKeyed();
  await testEdgeCases();

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
