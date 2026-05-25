// =====================================================================
// smoke_envelope.js — signed-publish envelope: build, verify, tamper.
//                      Plus end-to-end through AxonaPeer.pub() / sub()
//                      against a mock AxonManager.
// Run: node test/smoke_envelope.js
// =====================================================================

import { AxonaPeer }     from '../src/dht/AxonaPeer.js';
import { deriveIdentity } from '../src/identity/index.js';
import {
  buildEnvelope,
  verifyEnvelope,
  computeMsgId,
}                       from '../src/pubsub/envelope.js';
import { canonical, sha256Hex } from '../src/pubsub/post.js';
import { PublishError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

// ── Mock AxonManager (same shape as A1 smoke) ────────────────────────

class MockAxonManager {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.published = [];
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
  }
  pubsubPublish(topicId, json, meta) {
    const publishId = `${this.nodeId}:${++this._publishCounter}`;
    this.published.push({ topicId, json, meta, publishId });
    return publishId;
  }
  pubsubSubscribe()   {}
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
  triggerDelivery(topicId, json, publishId = 'm-1', ts = Date.now()) {
    if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, ts);
  }
}

// ── envelope.js direct tests ─────────────────────────────────────────

async function testBuildSigned() {
  console.log('\n── buildEnvelope: signed ──');
  const id = await deriveIdentity(LONDON);
  const env = await buildEnvelope({
    topic: 'cats',
    message: { meow: 1 },
    identity: id,
    ts: 1700000000000,
  });
  check('has msgId',            typeof env.msgId === 'string' && env.msgId.length === 64);
  check('has ts',               env.ts === 1700000000000);
  check('has topic',            env.topic === 'cats');
  check('has message',          env.message.meow === 1);
  check('signature ed25519:',   env.signature.startsWith('ed25519:'));
  check('signature is 128 hex', env.signature.length === 'ed25519:'.length + 128);
  check('signerPubkey is 64 hex', env.signerPubkey.length === 64);
  check('signerPubkey matches identity.pubkeyHex',
    env.signerPubkey === id.pubkeyHex);
}

async function testBuildUnsigned() {
  console.log('\n── buildEnvelope: unsigned (opt-out) ──');
  const env = await buildEnvelope({
    topic: 'public',
    message: 'broadcast',
    sign: false,
    ts: 1700000000000,
  });
  check('has msgId',            typeof env.msgId === 'string' && env.msgId.length === 64);
  check('no signature field',   env.signature === undefined);
  check('no signerPubkey field', env.signerPubkey === undefined);
}

async function testSignWithoutIdentity() {
  console.log('\n── buildEnvelope: sign:true without identity throws ──');
  let threw = false;
  try {
    await buildEnvelope({ topic: 'x', message: null, sign: true });
  } catch { threw = true; }
  check('throws TypeError when sign:true and no identity', threw);
}

async function testVerifyHappy() {
  console.log('\n── verifyEnvelope: signed envelope verifies ──');
  const id = await deriveIdentity(LONDON);
  const env = await buildEnvelope({
    topic: 'cats', message: { meow: 1 }, identity: id,
  });
  const r = await verifyEnvelope(env);
  check('verify(intact, signed) → ok',  r.ok === true);
  check('verify reports signed: true',  r.signed === true);
}

async function testVerifyUnsignedHappy() {
  console.log('\n── verifyEnvelope: unsigned envelope verifies ──');
  const env = await buildEnvelope({
    topic: 'announce', message: 'hi', sign: false,
  });
  const r = await verifyEnvelope(env);
  check('verify(intact, unsigned) → ok', r.ok === true);
  check('verify reports signed: false',  r.signed === false);
}

async function testTamperMessage() {
  console.log('\n── verifyEnvelope: tampered message ──');
  const id = await deriveIdentity(LONDON);
  const env = await buildEnvelope({ topic: 'cats', message: 'real', identity: id });
  const tampered = { ...env, message: 'forged' };
  const r = await verifyEnvelope(tampered);
  check('tampered message → ok:false', r.ok === false);
  check('reason is bad_signature or bad_msgid',
    r.reason === 'bad_signature' || r.reason === 'bad_msgid');
}

async function testTamperMsgId() {
  console.log('\n── verifyEnvelope: tampered msgId ──');
  const id = await deriveIdentity(LONDON);
  const env = await buildEnvelope({ topic: 'cats', message: 'real', identity: id });
  const tampered = { ...env, msgId: '0'.repeat(64) };
  const r = await verifyEnvelope(tampered);
  check('tampered msgId → ok:false',  r.ok === false);
  check('reason is bad_msgid',        r.reason === 'bad_msgid');
}

async function testTamperSignature() {
  console.log('\n── verifyEnvelope: tampered signature ──');
  const id = await deriveIdentity(LONDON);
  const env = await buildEnvelope({ topic: 'cats', message: 'real', identity: id });
  // Flip one hex char in the signature.
  const sig = env.signature;
  const flippedHex = sig.slice(0, 10) + (sig[10] === 'a' ? 'b' : 'a') + sig.slice(11);
  const tampered = { ...env, signature: flippedHex };
  const r = await verifyEnvelope(tampered);
  check('tampered signature → ok:false', r.ok === false);
  // Either bad_signature (sig doesn't verify) or bad_msgid (msgId
  // includes the signature) — both are valid rejection reasons.
  check('reason is bad_signature or bad_msgid',
    r.reason === 'bad_signature' || r.reason === 'bad_msgid');
}

async function testRejectMissingFields() {
  console.log('\n── verifyEnvelope: structural rejections ──');
  check('null → not_an_object',
    (await verifyEnvelope(null)).reason === 'not_an_object');
  check('no msgId',
    (await verifyEnvelope({ ts: 1, topic: 'x', message: 1 })).reason === 'missing_msgId');
  check('no ts',
    (await verifyEnvelope({ msgId: 'x', topic: 'x', message: 1 })).reason === 'missing_ts');
  check('no topic',
    (await verifyEnvelope({ msgId: 'x', ts: 1, message: 1 })).reason === 'missing_topic');
  check('no message',
    (await verifyEnvelope({ msgId: 'x', ts: 1, topic: 'x' })).reason === 'missing_message');
}

async function testMsgIdDeterminism() {
  console.log('\n── computeMsgId determinism ──');
  const a = await computeMsgId({ ts: 100, topic: 't', message: { a: 1, b: 2 } });
  const b = await computeMsgId({ ts: 100, topic: 't', message: { b: 2, a: 1 } });
  check('msgId independent of object key order', a === b);

  const c = await computeMsgId({ ts: 101, topic: 't', message: { a: 1, b: 2 } });
  check('different ts → different msgId', a !== c);
}

// ── End-to-end through AxonaPeer ─────────────────────────────────────

async function testPeerSignedRoundTrip() {
  console.log('\n── peer.pub signed → peer.sub envelope (e2e) ──');
  const id = await deriveIdentity(LONDON);
  const node = { id: id.id, alive: true };
  const am = new MockAxonManager(id.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, axonManager: am, identity: id,
  });

  const received = [];
  const sub = await peer.sub('cats', e => received.push(e));

  const msgId = await peer.pub('cats', { meow: 1 });
  check('pub returns content-derived msgId (64-char hex)',
    typeof msgId === 'string' && msgId.length === 64 && /^[0-9a-f]+$/.test(msgId));
  check('AxonManager.pubsubPublish called',
    am.published.length === 1);

  // Trigger delivery with the same JSON the publisher wrote.
  am.triggerDelivery(sub._topicId, am.published[0].json, 'internal-1', 1700000000000);

  check('handler received envelope', received.length === 1);
  check('envelope.msgId matches pub return value',
    received[0].msgId === msgId);
  check('envelope.signature present (signed by default)',
    received[0].signature?.startsWith('ed25519:'));
  check('envelope.signerPubkey matches identity',
    received[0].signerPubkey === id.pubkeyHex);

  // Verify the delivered envelope.
  const r = await verifyEnvelope(received[0]);
  check('delivered envelope verifies signed',
    r.ok === true && r.signed === true);

  await sub.stop();
}

async function testPeerUnsignedRoundTrip() {
  console.log('\n── peer.pub({ sign: false }) → unsigned envelope ──');
  const id = await deriveIdentity(LONDON);
  const node = { id: id.id, alive: true };
  const am = new MockAxonManager(id.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, axonManager: am, identity: id,
  });

  const received = [];
  const sub = await peer.sub('announce', e => received.push(e));

  const msgId = await peer.pub('announce', 'public broadcast', { sign: false });
  am.triggerDelivery(sub._topicId, am.published[0].json, 'internal-1', 1700000000000);

  check('unsigned envelope delivered',
    received.length === 1 && received[0].msgId === msgId);
  check('no signature on unsigned envelope',
    received[0].signature === undefined);
  check('no signerPubkey on unsigned envelope',
    received[0].signerPubkey === undefined);

  const r = await verifyEnvelope(received[0]);
  check('unsigned envelope still verifies (msgId check)',
    r.ok === true && r.signed === false);

  await sub.stop();
}

async function testPeerSignWithoutIdentity() {
  console.log('\n── peer.pub(sign:true) without identity throws ──');
  const node = { id: 'aa' + 'a1'.repeat(32), alive: true };
  const am = new MockAxonManager(node.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, axonManager: am, /* no identity */
  });

  let err = null;
  try { await peer.pub('cats', null); }
  catch (e) { err = e; }
  check('throws PublishError when identity missing',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_SIGN_FAILED);

  // Unsigned still works without identity.
  const msgId = await peer.pub('cats', 'anon', { sign: false });
  check('unsigned publish works without identity',
    typeof msgId === 'string' && msgId.length === 64);
}

async function testPeerCrossSignerVerification() {
  console.log('\n── cross-peer verification: bob receives, verifies alice ──');
  const alice = await deriveIdentity(LONDON);
  const bob   = await deriveIdentity({ lat: 35.6762, lng: 139.6503 });

  const aliceNode = { id: alice.id, alive: true };
  const aliceAm   = new MockAxonManager(alice.id);
  const alicePeer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: aliceNode, axonManager: aliceAm, identity: alice,
  });

  await alicePeer.pub('news', { headline: 'mesh launch' });
  const json = aliceAm.published[0].json;

  // bob's side: parse the JSON and verify.
  const env = JSON.parse(json);
  // bob doesn't trust the publisher to have set msgId correctly; he
  // recomputes via verifyEnvelope.
  const r = await verifyEnvelope(env);
  check('bob verifies alice signed envelope',
    r.ok === true && r.signed === true);
  check('signerPubkey is alice, not bob',
    env.signerPubkey === alice.pubkeyHex &&
    env.signerPubkey !== bob.pubkeyHex);
}

async function main() {
  console.log('Axona signed-envelope (A2) smoke');
  await testBuildSigned();
  await testBuildUnsigned();
  await testSignWithoutIdentity();
  await testVerifyHappy();
  await testVerifyUnsignedHappy();
  await testTamperMessage();
  await testTamperMsgId();
  await testTamperSignature();
  await testRejectMissingFields();
  await testMsgIdDeterminism();
  await testPeerSignedRoundTrip();
  await testPeerUnsignedRoundTrip();
  await testPeerSignWithoutIdentity();
  await testPeerCrossSignerVerification();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
