// =====================================================================
// smoke_unpub.js — owner-only topic-queue removal (Phase A #3).
//
//   1. unpub.js: buildUnpub / verifyUnpub (+ tamper).
//   2. AxonaManager._handleUnpub: owner-authorized unpub clears the queue
//      (and tombstones the msgIds); {destroy} removes the role; a NON-owner
//      unpub is rejected.
//   3. peer.unpub validation (public topic, missing identity).
//
// Run: node test/smoke_unpub.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }      from '../src/dht/AxonaPeer.js';
import { deriveIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { deriveTopicId }  from '../src/pubsub/post.js';
import { buildUnpub, verifyUnpub } from '../src/pubsub/unpub.js';
import { UnpubError, ErrorCodes }  from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const TOKYO  = { lat: 35.6762, lng: 139.6503 };
const T = 1_700_000_000_000;

function stubDht() {
  return {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, sendDirect: async () => true, routeMessage: async () => {},
  };
}
function mkManager() { return new AxonaManager({ dht: stubDht(), now: () => T }); }

// Seed a root role for `owner`'s topic `name` with one cached message.
async function seed(am, owner, name) {
  const topicHex = await deriveTopicId(owner.id, name);
  const topicBig = BigInt('0x' + topicHex);
  const env = await buildEnvelope({ topic: name, message: 'hi', identity: owner, ts: T, seq: T });
  am.axonRoles.set(topicBig, {
    isRoot: true, children: new Map(),
    replayCache: [{ json: JSON.stringify(env), publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null }],
  });
  return { topicHex, topicBig, env };
}

async function testUnpubObject() {
  console.log('\n── unpub.js build / verify / tamper ──');
  const alice = await deriveIdentity(LONDON);
  const u = await buildUnpub({ topicId: '89' + 'ab'.repeat(32), topicName: 'cats', ownerNodeId: alice.id, destroy: false, ts: T, seq: T, identity: alice });
  check('carries domain kind', u.kind === 'axona:pubsub-unpub:v1');
  check('destroy flag present', u.destroy === false);
  check('verifyUnpub(intact) → ok', (await verifyUnpub(u)).ok === true);
  check('tampered topicName → ok:false', (await verifyUnpub({ ...u, topicName: 'dogs' })).ok === false);
  check('flipped destroy → ok:false', (await verifyUnpub({ ...u, destroy: true })).ok === false);
}

async function testOwnerUnpubClears() {
  console.log('\n── owner unpub clears the queue + tombstones ──');
  const alice = await deriveIdentity(LONDON);
  const am = mkManager();
  const { topicHex, topicBig, env } = await seed(am, alice, 'cats');

  const u = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: alice.id, destroy: false, ts: T, seq: T, identity: alice });
  const verdict = await am._handleUnpub(topicBig, u);

  check('unpub consumed', verdict === 'consumed');
  check('queue cleared', am.axonRoles.get(topicBig).replayCache.length === 0);
  check('role kept (non-destroy)', am.axonRoles.has(topicBig) === true);
  check('msgId tombstoned (no resurrection)', am._isTombstoned(env.msgId) === true);
}

async function testDestroyRemovesRole() {
  console.log('\n── unpub { destroy } removes the role entirely ──');
  const alice = await deriveIdentity(LONDON);
  const am = mkManager();
  const { topicHex, topicBig } = await seed(am, alice, 'cats');

  const u = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: alice.id, destroy: true, ts: T, seq: T, identity: alice });
  await am._handleUnpub(topicBig, u);
  check('role deleted', am.axonRoles.has(topicBig) === false);
}

async function testNonOwnerRejected() {
  console.log('\n── a non-owner cannot unpub someone else’s topic ──');
  const alice = await deriveIdentity(LONDON);
  const bob   = await deriveIdentity(TOKYO);
  const am = mkManager();
  const { topicHex, topicBig } = await seed(am, alice, 'cats');   // alice owns it

  // (a) bob claims alice's nodeId but signs with his key → pubkey↔nodeId bind fails.
  const forged = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: alice.id, destroy: false, ts: T, seq: T, identity: bob });
  await am._handleUnpub(topicBig, forged);
  check('forged owner (bob signs as alice) rejected — queue intact',
    am.axonRoles.get(topicBig).replayCache.length === 1);

  // (b) bob uses his OWN nodeId → deriveTopicId(bob, cats) ≠ alice's topicId.
  const bobOwn = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: bob.id, destroy: false, ts: T, seq: T, identity: bob });
  await am._handleUnpub(topicBig, bobOwn);
  check('bob-as-self rejected — queue still intact',
    am.axonRoles.get(topicBig).replayCache.length === 1);
}

async function testPeerValidation() {
  console.log('\n── peer.unpub input validation ──');
  const id = await deriveIdentity(LONDON);
  const am = mkManager();
  const peer = new AxonaPeer({ engine: { onEvent: () => () => {} }, node: { id: id.id, alive: true }, axonaManager: am, identity: id });

  let e1 = null; try { await peer.unpub('cats', { publisher: null }); } catch (e) { e1 = e; }
  check('public topic → UnpubError(UNPUB_PUBLIC_TOPIC)',
    e1 instanceof UnpubError && e1.code === ErrorCodes.UNPUB_PUBLIC_TOPIC);

  const noId = new AxonaPeer({ engine: { onEvent: () => () => {} }, node: { id: id.id, alive: true }, axonaManager: am });
  let e2 = null; try { await noId.unpub('cats'); } catch (e) { e2 = e; }
  check('no identity → UnpubError(UNPUB_SIGN_FAILED)',
    e2 instanceof UnpubError && e2.code === ErrorCodes.UNPUB_SIGN_FAILED);
}

async function main() {
  console.log('Axona unpub / owner queue removal (Phase A #3) smoke');
  await testUnpubObject();
  await testOwnerUnpubClears();
  await testDestroyRemovesRole();
  await testNonOwnerRejected();
  await testPeerValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
