// =====================================================================
// smoke_unsub.js — peer.unsub(topic) (Phase A #1).
//
// Topic-keyed counterpart to peer.sub: stops all local subscriptions for
// a topic and sends the network unsubscribe once the last one goes.
// Idempotent; topic-id derivation must match sub() (publisher mode).
//
// Run: node test/smoke_unsub.js
// =====================================================================

import { AxonaPeer }       from '../src/dht/AxonaPeer.js';
import { deriveIdentity }  from '../src/identity/index.js';
import { SubscribeError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

class MockAxonaManager {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.subscribed   = [];     // topicId BigInts
    this.unsubscribed = [];     // topicId BigInts
    this._deliveryCallback = null;
  }
  pubsubSubscribe(topicId)   { this.subscribed.push(topicId); }
  pubsubUnsubscribe(topicId) { this.unsubscribed.push(topicId); }
  pubsubPublish() { return 'm-1'; }
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
}

async function mkPeer() {
  const id = await deriveIdentity(LONDON);
  const am = new MockAxonaManager(id.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id.id, alive: true }, axonaManager: am, identity: id,
  });
  return { peer, am, id };
}

async function testBasicUnsub() {
  console.log('\n── unsub stops all local subs for a topic + network leave ──');
  const { peer, am } = await mkPeer();
  const a = await peer.sub('cats', () => {});
  const b = await peer.sub('cats', () => {});   // 2nd handler, same topic
  check('two subscriptions registered', am.subscribed.length === 2);

  const r = await peer.unsub('cats');
  check('unsub reports removed: 2',  r.removed === 2 && r.ok === true);
  check('both subscriptions stopped', a.stopped === true && b.stopped === true);
  check('network unsubscribe sent ONCE (set emptied)', am.unsubscribed.length === 1);
}

async function testIdempotent() {
  console.log('\n── unsub is idempotent ──');
  const { peer, am } = await mkPeer();
  await peer.sub('news', () => {});
  await peer.unsub('news');
  const again = await peer.unsub('news');
  check('second unsub → removed: 0', again.removed === 0 && again.ok === true);
  check('no extra network unsubscribe', am.unsubscribed.length === 1);
  const never = await peer.unsub('never-subscribed');
  check('unsub of unknown topic → removed: 0', never.removed === 0);
}

async function testTopicIsolation() {
  console.log('\n── unsub only affects the named topic ──');
  const { peer, am } = await mkPeer();
  const a = await peer.sub('alpha', () => {});
  const b = await peer.sub('beta',  () => {});
  await peer.unsub('alpha');
  check('alpha stopped', a.stopped === true);
  check('beta still active', b.stopped === false);
  check('exactly one topic left the network', am.unsubscribed.length === 1);
}

async function testPublisherModeParity() {
  console.log('\n── unsub topic-id derivation matches sub (publisher mode) ──');
  const { peer, am } = await mkPeer();
  // Subscribe to a PUBLIC topic (publisher: null).
  const pub = await peer.sub('room', () => {}, { publisher: null });
  // unsub with the DEFAULT publisher derives a different topicId → no match.
  const wrong = await peer.unsub('room');
  check('default-publisher unsub does NOT match the public sub', wrong.removed === 0);
  check('public sub still active', pub.stopped === false);
  // unsub with the SAME public mode matches.
  const right = await peer.unsub('room', { publisher: null });
  check('public-mode unsub matches', right.removed === 1 && pub.stopped === true);
}

async function testValidation() {
  console.log('\n── unsub input validation ──');
  const { peer } = await mkPeer();
  let err = null;
  try { await peer.unsub(''); } catch (e) { err = e; }
  check('empty topic → SubscribeError(INVALID_TOPIC)',
    err instanceof SubscribeError && err.code === ErrorCodes.SUBSCRIBE_INVALID_TOPIC);
}

async function main() {
  console.log('Axona peer.unsub (Phase A #1) smoke');
  await testBasicUnsub();
  await testIdempotent();
  await testTopicIsolation();
  await testPublisherModeParity();
  await testValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
