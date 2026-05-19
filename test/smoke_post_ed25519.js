// =====================================================================
// smoke_post_ed25519.js — verify post.js (Web Crypto) + ed25519.js
//                         round-trip in Node 20+ and modern browsers.
// Run: node test/smoke_post_ed25519.js
// =====================================================================

import {
  makePost,
  deriveTopicId,
  verifyPostHash,
  verifyTopicOwnership,
  verifySignature,
} from '../src/pubsub/post.js';

import {
  generateKeyPair,
  exportPublicKey,
  importPublicKey,
  sign,
  verify,
  makeSigner,
  makeVerifier,
} from '../src/pubsub/ed25519.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// 264-bit hex nodeIds for test publishers (66 chars: [8-bit S2 prefix] || [256-bit hash]).
const ALICE = 'aa' + 'a1'.repeat(32);   // S2 prefix 0xaa
const BOB   = 'bb' + 'b2'.repeat(32);   // S2 prefix 0xbb

async function testDeriveTopicId() {
  console.log('\n── deriveTopicId (async) ──');
  const id = await deriveTopicId(ALICE, 'cat-pics');
  check('returns 66-char hex',
    typeof id === 'string' && id.length === 66 && /^[0-9a-f]+$/.test(id));
  check("topic_id top 8 bits = publisher's S2 prefix",
    id.slice(0, 2) === ALICE.slice(0, 2));
  const id2 = await deriveTopicId(ALICE, 'cat-pics');
  check('deterministic (same input → same id)', id === id2);
  const id3 = await deriveTopicId(ALICE, 'dog-pics');
  check('different topic → different id',          id !== id3);
  const id4 = await deriveTopicId(BOB, 'cat-pics');
  check('different publisher → different id',       id !== id4);
  check("BOB's topic carries BOB's S2 prefix",
    id4.slice(0, 2) === BOB.slice(0, 2));
}

async function testMakePostUnsigned() {
  console.log('\n── makePost (unsigned, stub: signature) ──');
  const post = await makePost({
    publisher: ALICE,
    topicName: 'hello',
    content:   { msg: 'hi mesh' },
  });
  check('returns a post object',           post && typeof post === 'object');
  check('has post_hash 64-char hex',       /^[0-9a-f]{64}$/.test(post.post_hash));
  check('has stub signature',              post.signature === 'stub:' + ALICE);
  check('topic_id matches deriveTopicId',
    post.topic_id === await deriveTopicId(ALICE, 'hello'));
}

async function testVerifyPostHash() {
  console.log('\n── verifyPostHash ──');
  const post = await makePost({
    publisher: ALICE,
    topicName: 'hello',
    content:   { msg: 'hi mesh' },
  });
  check('verify intact post → true', await verifyPostHash(post));
  // Tamper content.
  const tampered = { ...post, content: { msg: 'tampered' } };
  check('verify tampered post → false', !(await verifyPostHash(tampered)));
}

async function testVerifyTopicOwnership() {
  console.log('\n── verifyTopicOwnership ──');
  const post = await makePost({
    publisher: ALICE,
    topicName: 'hello',
    content:   {},
  });
  check('intact topic_id → true', await verifyTopicOwnership(post));
  const spoofed = { ...post, topic_id: '0'.repeat(66) };
  check('forged topic_id → false', !(await verifyTopicOwnership(spoofed)));
}

async function testEd25519RoundTrip() {
  console.log('\n── Ed25519 round-trip ──');
  let pair;
  try { pair = await generateKeyPair(); }
  catch (err) {
    console.log(`  ⚠ Ed25519 not supported on this runtime (${err.message})`);
    console.log('  Skipping Ed25519 tests.  Browsers/Node 20+ should have it.');
    return;
  }

  const msg = new TextEncoder().encode('test message');
  const sig = await sign(pair.privateKey, msg);
  check('sign returns 64-byte signature', sig instanceof Uint8Array && sig.length === 64);
  check('verify(intact)  → true',         await verify(pair.publicKey, msg, sig));
  const tamperedMsg = new TextEncoder().encode('not the message');
  check('verify(tampered) → false',        !(await verify(pair.publicKey, tamperedMsg, sig)));
}

async function testMakePostSigned() {
  console.log('\n── makePost (signed) + verifySignature ──');
  let pair;
  try { pair = await generateKeyPair(); }
  catch { console.log('  ⚠ Ed25519 not supported; skipping signed-post tests.'); return; }

  const post = await makePost({
    publisher: ALICE,
    topicName: 'signed-topic',
    content:   { msg: 'authenticated' },
    signer:    makeSigner(pair.privateKey),
  });
  check('signature starts with ed25519:',  post.signature.startsWith('ed25519:'));
  check('signature is 128 hex chars',       post.signature.length === 'ed25519:'.length + 128);

  // Verify with the verifier function.
  const verifier = makeVerifier();
  const pubBytes = await exportPublicKey(pair.publicKey);
  check('verifySignature(intact) → true',
    await verifySignature(post, verifier, pubBytes));

  // Tamper and re-check.
  const tampered = { ...post, content: { msg: 'forged' } };
  check('verifySignature(tampered) → false',
    !(await verifySignature(tampered, verifier, pubBytes)));
}

async function testStubSignatureCompat() {
  console.log('\n── Backward-compat: stub: signatures pass verifySignature ──');
  const post = await makePost({
    publisher: ALICE,
    topicName: 'test',
    content:   {},
  });
  // No verifier needed — stub: short-circuits to true (simulator mode).
  const result = await verifySignature(post, null, null);
  check('stub: signature returns true (sim mode)', result === true);
}

async function main() {
  console.log('post.js (Web Crypto) + ed25519.js smoke');
  await testDeriveTopicId();
  await testMakePostUnsigned();
  await testVerifyPostHash();
  await testVerifyTopicOwnership();
  await testEd25519RoundTrip();
  await testMakePostSigned();
  await testStubSignatureCompat();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
