// =====================================================================
// smoke_pull_metrics.js — peer.pull(msgId, {topic, publisher}) and
//                          peer.metrics(topic, {publisher}) against a
//                          mock AxonManager that implements
//                          requestPull / requestMetrics.
// Run: node test/smoke_pull_metrics.js
// =====================================================================

import { AxonaPeer }       from '../src/dht/AxonaPeer.js';
import { deriveIdentity }  from '../src/identity/index.js';
import { buildEnvelope }   from '../src/pubsub/envelope.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { fromHex }         from '../src/utils/hexid.js';
import { PullError, MetricsError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

// ── MockAxonManager with replay cache + counter store ────────────────

class MockAxonManager {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
    // Replay cache: topicId → [{ json, postHash, publisher }]
    this._replay   = new Map();
    // Counters: topicId → [{ post_hash, delivery_count, pull_count, reshare_count }]
    this._counters = new Map();
  }
  pubsubPublish(topicId, json, meta) {
    const publishId = `${this.nodeId}:${++this._publishCounter}`;
    if (!this._replay.has(topicId)) this._replay.set(topicId, []);
    this._replay.get(topicId).push({
      json, postHash: meta?.postHash, publisher: meta?.publisher,
    });
    // Seed counters: bump publishes.
    if (!this._counters.has(topicId)) this._counters.set(topicId, new Map());
    const byHash = this._counters.get(topicId);
    if (meta?.postHash && !byHash.has(meta.postHash)) {
      byHash.set(meta.postHash, {
        post_hash: meta.postHash, delivery_count: 0, pull_count: 0, reshare_count: 0,
      });
    }
    return publishId;
  }
  pubsubSubscribe()   {}
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }

  async requestPull(topicId, postHash, { timeoutMs = 1000 } = {}) {
    const cache = this._replay.get(topicId);
    if (!cache) return null;
    for (let i = cache.length - 1; i >= 0; i--) {
      if (cache[i].postHash === postHash) {
        // Bump pull_count.
        const ctr = this._counters.get(topicId)?.get(postHash);
        if (ctr) ctr.pull_count++;
        try { return JSON.parse(cache[i].json); }
        catch { return null; }
      }
    }
    return null;
  }
  async requestMetrics(topicId, _postHashes, { timeoutMs = 500 } = {}) {
    const byHash = this._counters.get(topicId);
    if (!byHash) return [];
    const entries = [...byHash.values()].map(c => ({ ...c }));
    return [{
      responderId: this.nodeId,
      entries,
      timestamp: Date.now(),
      subscribers: (this._replay.get(topicId)?.length ?? 0),  // proxy
    }];
  }

  // Test helper: bump delivery_count for a post (simulates a relay
  // forwarding the publish to a subscriber).
  _bumpDelivery(topicId, postHash, by = 1) {
    const c = this._counters.get(topicId)?.get(postHash);
    if (c) c.delivery_count += by;
  }
}

// ── Setup helper ─────────────────────────────────────────────────────

async function setupPeer() {
  const identity = await deriveIdentity(LONDON);
  const node     = { id: identity.id, alive: true };
  const am       = new MockAxonManager(identity.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, axonManager: am, identity,
  });
  return { peer, am, identity };
}

// ── Tests ────────────────────────────────────────────────────────────

async function testPullHappy() {
  console.log('\n── peer.pull() happy path ──');
  const { peer, am, identity } = await setupPeer();

  const msgId = await peer.pub('cats', { meow: 1 });
  check('pub succeeded', typeof msgId === 'string');
  // Kernel passes BigInt topicId to AxonManager now.
  check('replay cache populated with postHash',
    am._replay.get(await deriveTopicIdBig(fromHex(identity.id), 'cats'))?.[0]?.postHash === msgId);

  const pulled = await peer.pull(msgId, { topic: 'cats', publisher: identity.id });
  check('pull returned envelope',
    pulled !== null && pulled.msgId === msgId);
  check('pulled envelope.message matches',
    pulled.message.meow === 1);
  check('pulled envelope is signed',
    pulled.signature?.startsWith('ed25519:'));
}

async function testPullMiss() {
  console.log('\n── peer.pull() miss returns null ──');
  const { peer, identity } = await setupPeer();

  // Unknown msgId — not in cache.
  const result = await peer.pull('0'.repeat(64), {
    topic: 'cats', publisher: identity.id,
  });
  check('miss returns null', result === null);
}

async function testPullValidation() {
  console.log('\n── peer.pull() validation ──');
  const { peer, identity } = await setupPeer();

  let err = null;
  try { await peer.pull('short', { topic: 'cats', publisher: identity.id }); }
  catch (e) { err = e; }
  check('short msgId → PullError',
    err instanceof PullError && err.code === ErrorCodes.PULL_INVALID_MSGID);

  err = null;
  try { await peer.pull('0'.repeat(64), { publisher: identity.id }); }
  catch (e) { err = e; }
  check('missing topic → PullError', err instanceof PullError);

  err = null;
  try { await peer.pull('0'.repeat(64), { topic: 'cats', publisher: 'not-hex' }); }
  catch (e) { err = e; }
  check('non-hex publisher → PullError', err instanceof PullError);
}

async function testPullBumpsCounter() {
  console.log('\n── peer.pull() bumps pull_count ──');
  const { peer, am, identity } = await setupPeer();

  const msgId = await peer.pub('cats', 'hi');
  await peer.pull(msgId, { topic: 'cats', publisher: identity.id });

  // BigInt topicId is the kernel-internal key.
  const topicIdBig = await deriveTopicIdBig(fromHex(identity.id), 'cats');
  const ctr = am._counters.get(topicIdBig).get(msgId);
  check('pull_count incremented', ctr.pull_count === 1);

  // Pull again.
  await peer.pull(msgId, { topic: 'cats', publisher: identity.id });
  check('pull_count = 2 after second pull', ctr.pull_count === 2);
}

async function testMetricsAggregation() {
  console.log('\n── peer.metrics() aggregates relay counters ──');
  const { peer, am, identity } = await setupPeer();

  // Publish three messages on the same topic.
  const m1 = await peer.pub('cats', 1);
  const m2 = await peer.pub('cats', 2);
  const m3 = await peer.pub('cats', 3);

  // Simulate deliveries (each post delivered to 5 subscribers).
  const topicId = await deriveTopicIdBig(fromHex(identity.id), 'cats');
  am._bumpDelivery(topicId, m1, 5);
  am._bumpDelivery(topicId, m2, 5);
  am._bumpDelivery(topicId, m3, 5);

  // Simulate two pulls of m1.
  await peer.pull(m1, { topic: 'cats', publisher: identity.id });
  await peer.pull(m1, { topic: 'cats', publisher: identity.id });

  const m = await peer.metrics('cats', { publisher: identity.id });
  check('publishes count = 3',     m.publishes === 3);
  check('deliveries = 15',         m.deliveries === 15);
  check('pulls = 2',               m.pulls === 2);
  check('reshares = 0',            m.reshares === 0);
  check('relayCount = 1',          m.relayCount === 1);
  check('subscribers reported',    typeof m.subscribers === 'number');
}

async function testMetricsEmpty() {
  console.log('\n── peer.metrics() on unknown topic ──');
  const { peer, identity } = await setupPeer();

  const m = await peer.metrics('never-published', { publisher: identity.id });
  check('publishes = 0',  m.publishes === 0);
  check('deliveries = 0', m.deliveries === 0);
  check('pulls = 0',      m.pulls === 0);
  check('relayCount = 0 (no relays responded)', m.relayCount === 0);
}

async function testMetricsValidation() {
  console.log('\n── peer.metrics() validation ──');
  const { peer, identity } = await setupPeer();

  let err = null;
  try { await peer.metrics('', { publisher: identity.id }); }
  catch (e) { err = e; }
  check('empty topic → MetricsError', err instanceof MetricsError);

  err = null;
  try { await peer.metrics('cats', { publisher: 'not-hex' }); }
  catch (e) { err = e; }
  check('non-hex publisher → MetricsError', err instanceof MetricsError);
}

async function testCrossPublisherIsolation() {
  console.log('\n── alice can pull from her own topic ──');
  const alice = await setupPeer();
  const bob   = await setupPeer();

  // Alice publishes; pull from her own AxonManager (which is what
  // the production routing eventually resolves to via K-closest).
  const m = await alice.peer.pub('news', { headline: 'launch' });
  const pulled = await alice.peer.pull(m, {
    topic: 'news',
    publisher: alice.identity.id,
  });
  check('alice pulls her own message',
    pulled !== null && pulled.message.headline === 'launch');

  // Same msgId addressed to BOB's topicId space → null (different
  // topicId derivation = different replay cache entry).
  const bobView = await alice.peer.pull(m, {
    topic: 'news',
    publisher: bob.identity.id,    // wrong publisher
  });
  check('pull with wrong publisher returns null',
    bobView === null);
}

async function main() {
  console.log('Axona pull/metrics (A3) smoke');
  await testPullHappy();
  await testPullMiss();
  await testPullValidation();
  await testPullBumpsCounter();
  await testMetricsAggregation();
  await testMetricsEmpty();
  await testMetricsValidation();
  await testCrossPublisherIsolation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
