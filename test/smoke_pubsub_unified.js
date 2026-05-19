// =====================================================================
// smoke_pubsub_unified.js — verify AxonaPeer.pub() / sub() / stop()
//                            against a mock AxonManager.
// Run: node test/smoke_pubsub_unified.js
//
// Covers the unified-API contract (A1 surface):
//   - string topic input
//   - hex topic-id derivation via deriveTopicId
//   - msgId returned from pub()
//   - Subscription handle with .stop()
//   - envelope shape on delivery: { msgId, ts, topic, message, publisher }
//   - `since` modes seed lastSeenTs correctly
//   - input validation
//
// Full pub/sub end-to-end against the production AxonManager + Engine
// is exercised by axona-peer/src/smoke_pubsub*.js and (post-T-I2) the
// dht-sim regression suite — those validate the wiring layer.
// =====================================================================

import { AxonaPeer }       from '../src/dht/AxonaPeer.js';
import { Subscription }    from '../src/dht/Subscription.js';
import { deriveTopicId }   from '../src/pubsub/post.js';
import { isHexId }         from '../src/utils/hexid.js';
import { PublishError, SubscribeError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const NODE_ID = 'aa' + 'a1'.repeat(32);   // 66-char hex

// ── MockAxonManager: implements just the surface AxonaPeer needs ─────

class MockAxonManager {
  constructor() {
    this.nodeId = NODE_ID;
    this.published = [];            // [{ topicId, json, meta }]
    this.subscribed = [];           // topicIds
    this.unsubscribed = [];         // topicIds
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
  }
  pubsubPublish(topicId, json, meta) {
    const publishId = `${NODE_ID}:${++this._publishCounter}`;
    this.published.push({ topicId, json, meta, publishId });
    return publishId;
  }
  pubsubSubscribe(topicId) { this.subscribed.push(topicId); }
  pubsubUnsubscribe(topicId) { this.unsubscribed.push(topicId); }
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }

  // Test helper: simulate a delivery.
  triggerDelivery(topicId, json, publishId = `${NODE_ID}:1`, publishTs = Date.now()) {
    if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, publishTs);
  }
}

// ── MockNode + mockEngine just enough for AxonaPeer constructor ──────

function makePeer({ withAxonManager = true } = {}) {
  const node    = { id: NODE_ID, alive: true };
  const engine  = {
    onEvent: (_cb) => () => {},
    simEpoch: 0,
  };
  const am = withAxonManager ? new MockAxonManager() : null;
  const peer = new AxonaPeer({ engine, node, axonManager: am });
  return { peer, am };
}

// ── Tests ────────────────────────────────────────────────────────────

async function testPubBasics() {
  console.log('\n── peer.pub() basics ──');
  const { peer, am } = makePeer();
  // Anonymous publish keeps this smoke test independent of identity
  // wiring (the signed-publish flow is exercised by smoke_envelope.js).
  const msgId = await peer.pub('cats', { meow: 1 }, { sign: false });

  check('pub resolves with msgId string',
    typeof msgId === 'string' && msgId.length === 64);
  check('AxonManager.pubsubPublish called once',
    am.published.length === 1);
  check('topicId is 66-char hex',
    isHexId(am.published[0].topicId));

  const expectedTopicId = await deriveTopicId(NODE_ID, 'cats');
  check('topicId = deriveTopicId(nodeId, topic)',
    am.published[0].topicId === expectedTopicId);

  // Envelope is JSON-serialized: { msgId, ts, topic, message }
  const env = JSON.parse(am.published[0].json);
  check('json envelope has msgId',     env.msgId === msgId);
  check('json envelope has topic',     env.topic === 'cats');
  check('json envelope has message',   env.message.meow === 1);
  check('meta carries publisher',      am.published[0].meta.publisher === NODE_ID);
}

async function testPubValidation() {
  console.log('\n── peer.pub() validation ──');
  const { peer } = makePeer();

  let err = null;
  try { await peer.pub('', { x: 1 }); }
  catch (e) { err = e; }
  check('empty topic → PublishError',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_INVALID_TOPIC);

  err = null;
  try { await peer.pub(null, { x: 1 }); }
  catch (e) { err = e; }
  check('null topic → PublishError', err instanceof PublishError);

  err = null;
  const cyclic = {};
  cyclic.self = cyclic;
  try { await peer.pub('cats', cyclic, { sign: false }); }
  catch (e) { err = e; }
  // Cyclic payloads explode inside canonical() during envelope build
  // (msgId hashing).  The pub() entry point catches that as
  // PUBLISH_SIGN_FAILED — sign-or-not, the envelope step always runs.
  check('un-stringifiable payload → PublishError',
    err instanceof PublishError &&
    (err.code === ErrorCodes.PUBLISH_PAYLOAD_TOO_LARGE ||
     err.code === ErrorCodes.PUBLISH_SIGN_FAILED));
}

async function testPubNoManager() {
  console.log('\n── peer.pub() without AxonManager ──');
  const { peer } = makePeer({ withAxonManager: false });
  let err = null;
  try { await peer.pub('cats', null, { sign: false }); }
  catch (e) { err = e; }
  check('no AxonManager → PublishError', err instanceof PublishError);
}

async function testSubReceives() {
  console.log('\n── peer.sub() receives delivery as envelope ──');
  const { peer, am } = makePeer();
  const received = [];
  const sub = await peer.sub('cats', env => received.push(env));

  check('sub returns Subscription', sub instanceof Subscription);
  check('sub.topicName preserved',  sub.topicName === 'cats');
  check('sub.topicId is hex',       isHexId(sub.topicId));
  check('AxonManager.pubsubSubscribe called',
    am.subscribed.length === 1 && am.subscribed[0] === sub.topicId);

  // Synthesize a JSON envelope (matches what peer.pub would emit).
  // Use sign:false here so this smoke stays independent of identity
  // wiring — the signed flow is exercised by smoke_envelope.js.
  const env = {
    msgId: '0'.repeat(64),
    ts: 1234567,
    topic: 'cats',
    message: { hi: 1 },
  };
  am.triggerDelivery(sub.topicId, JSON.stringify(env), 'internal-7', 999);

  check('handler invoked once', received.length === 1);
  check('envelope.msgId is content-derived, not publishId',
    received[0].msgId === env.msgId);
  check('envelope.ts (from envelope, not delivery ts)',
    received[0].ts === 1234567);
  check('envelope.topic',       received[0].topic === 'cats');
  check('envelope.message',     received[0].message.hi === 1);
}

async function testMultipleSubsSameTopic() {
  console.log('\n── multiple subs to same topic both receive ──');
  const { peer, am } = makePeer();
  const a = [], b = [];
  const subA = await peer.sub('cats', e => a.push(e));
  const subB = await peer.sub('cats', e => b.push(e));

  check('AxonManager.pubsubSubscribe called twice',
    am.subscribed.length === 2);

  const payload = JSON.stringify({
    msgId: '1'.repeat(64), ts: 1, topic: 'cats', message: 'meow',
  });
  am.triggerDelivery(subA.topicId, payload, 'm-1', 1);

  check('handler A received', a.length === 1);
  check('handler B received', b.length === 1);
}

async function testSubStopUnsubscribes() {
  console.log('\n── sub.stop() unsubscribes only when last handler gone ──');
  const { peer, am } = makePeer();
  const a = [], b = [];
  const subA = await peer.sub('cats', e => a.push(e));
  const subB = await peer.sub('cats', e => b.push(e));

  await subA.stop();
  check('unsubscribe not called yet (subB still active)',
    am.unsubscribed.length === 0);
  check('subA.stopped', subA.stopped);

  // Stopped sub no longer receives.
  const payload = JSON.stringify({
    msgId: '2'.repeat(64), ts: 2, topic: 'cats', message: 'x',
  });
  am.triggerDelivery(subA.topicId, payload);
  check('subA handler no longer fires after stop', a.length === 0);
  check('subB handler still fires',                b.length === 1);

  // stop is idempotent.
  await subA.stop();
  check('repeat stop is no-op', am.unsubscribed.length === 0);

  await subB.stop();
  check('after last handler stops: unsubscribe called',
    am.unsubscribed.length === 1 && am.unsubscribed[0] === subB.topicId);
}

async function testSubValidation() {
  console.log('\n── peer.sub() validation ──');
  const { peer } = makePeer();

  let err = null;
  try { await peer.sub('', () => {}); }
  catch (e) { err = e; }
  check('empty topic → SubscribeError',
    err instanceof SubscribeError && err.code === ErrorCodes.SUBSCRIBE_INVALID_TOPIC);

  err = null;
  try { await peer.sub('cats', 'not-a-fn'); }
  catch (e) { err = e; }
  check('non-fn handler → SubscribeError',
    err instanceof SubscribeError && err.code === ErrorCodes.SUBSCRIBE_HANDLER_MISSING);
}

async function testSinceModes() {
  console.log('\n── peer.sub({since}) modes ──');
  const { peer, am } = makePeer();

  const sub1 = await peer.sub('a', () => {});
  const ts1 = am._lastSeenTsByTopic.get(sub1.topicId);
  check('default since: lastSeenTs ≈ now (live tail)',
    typeof ts1 === 'number' && ts1 > 0 && ts1 <= Date.now());

  const sub2 = await peer.sub('b', () => {}, { since: 'all' });
  check("since:'all' → lastSeenTs = 0",
    am._lastSeenTsByTopic.get(sub2.topicId) === 0);

  const sub3 = await peer.sub('c', () => {}, { since: 'latest' });
  const ts3 = am._lastSeenTsByTopic.get(sub3.topicId);
  check("since:'latest' → lastSeenTs ≈ now-1s",
    typeof ts3 === 'number' && Math.abs(ts3 - (Date.now() - 1000)) < 100);

  const sub4 = await peer.sub('d', () => {}, { since: 42 });
  check('since:<number> → lastSeenTs set exactly',
    am._lastSeenTsByTopic.get(sub4.topicId) === 42);

  let err = null;
  try { await peer.sub('e', () => {}, { since: 'invalid' }); }
  catch (e) { err = e; }
  check('since: invalid string → SubscribeError', err instanceof SubscribeError);
}

async function testEngineFallback() {
  console.log('\n── AxonManager fallback via engine.axonManagerFor() ──');
  const node = { id: NODE_ID, alive: true };
  const am   = new MockAxonManager();
  const engine = {
    onEvent: () => () => {},
    axonManagerFor: (n) => (n === node ? am : null),
  };
  const peer = new AxonaPeer({ engine, node });

  const msgId = await peer.pub('topic', { hi: 1 }, { sign: false });
  check('engine.axonManagerFor wired through',
    typeof msgId === 'string' && am.published.length === 1);
}

async function testEngineFallbackMapShape() {
  console.log('\n── AxonManager fallback via engine._axonManagers Map ──');
  const node = { id: NODE_ID, alive: true };
  const am   = new MockAxonManager();
  const engine = {
    onEvent: () => () => {},
    _axonManagers: new Map([[NODE_ID, am]]),
  };
  const peer = new AxonaPeer({ engine, node });

  await peer.pub('topic', { hi: 1 }, { sign: false });
  check('engine._axonManagers Map wired through',
    am.published.length === 1);
}

async function main() {
  console.log('Axona AxonaPeer.pub() / sub() smoke');
  await testPubBasics();
  await testPubValidation();
  await testPubNoManager();
  await testSubReceives();
  await testMultipleSubsSameTopic();
  await testSubStopUnsubscribes();
  await testSubValidation();
  await testSinceModes();
  await testEngineFallback();
  await testEngineFallbackMapShape();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
