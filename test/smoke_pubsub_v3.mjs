// smoke_pubsub_v3.mjs — AxonaPeer v0.3 data-plane: structured topics, signWith /
// ANONYMOUS, owner-only pre-check, region-required, descriptor in the envelope.
// Run: node test/smoke_pubsub_v3.mjs
import assert from 'node:assert';
import { AxonaPeer, ANONYMOUS } from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { resolveTopic } from '../src/pubsub/post.js';
import { PublishError, ErrorCodes } from '../src/errors.js';

let n = 0; const ok = (m) => console.log(`  ok ${++n} - ${m}`);

class MockAM {
  constructor(nodeId) { this.nodeId = nodeId; this.published = []; this.subscribed = []; this._lastSeenTsByTopic = new Map(); this._cb = null; this._c = 0; }
  pubsubPublish(topicId, json, meta) { this.published.push({ topicId, json, meta }); return `pub_${++this._c}`; }
  pubsubSubscribe(topicId) { this.subscribed.push(topicId); }
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._cb = cb; }
  deliver(topicId, json) { this._cb?.(topicId, json, `pub_${this._c}`, Date.now()); }
}

async function mkPeer() {
  const node = await createNodeIdentity({ lat: 38, lng: -78 });
  const engine = { onEvent: () => () => {}, simEpoch: 0 };
  const am = new MockAM(node.id);
  const peer = new AxonaPeer({ engine, node: { id: BigInt('0x' + node.id) }, axonaManager: am, nodeIdentity: node });
  return { peer, am };
}

const me    = await createAuthorIdentity();
const alice = await createAuthorIdentity();

// ── open topic, signed by an author ──
{
  const { peer, am } = await mkPeer();
  const msgId = await peer.pub({ region: 'useast', name: 'lobby' }, { hi: 1 }, { signWith: me });
  assert.equal(typeof msgId, 'string'); assert.equal(am.published.length, 1);
  const want = await resolveTopic({ region: 'useast', name: 'lobby', write: 'open' });
  assert.equal(am.published[0].topicId, BigInt('0x' + want.topicId), 'routed to the resolved topic id');
  const env = JSON.parse(am.published[0].json);
  assert.deepEqual(env.topic, { region: want.region, owner: null, name: 'lobby', write: 'open' }, 'envelope carries the descriptor');
  assert.equal(env.signerPubkey, me.authorId, 'signed by the author');
  assert.equal(am.published[0].meta.publishId, undefined, 'no app publishId');
  ok('open topic: signed, descriptor in envelope, no publishId');
}

// ── omitting a signer is an ERROR (no default author, no node-key fallback) ──
{
  const { peer } = await mkPeer();
  await assert.rejects(() => peer.pub({ region: 'useast', name: 'lobby' }, { x: 1 }),
    (e) => e instanceof PublishError && e.code === ErrorCodes.PUBLISH_NO_PUBLISH_IDENTITY);
  ok('no signer → PUBLISH_NO_PUBLISH_IDENTITY');
}

// ── explicit anonymous ──
{
  const { peer, am } = await mkPeer();
  await peer.pub({ region: 'useast', name: 'lobby' }, { x: 1 }, { signWith: ANONYMOUS });
  const env = JSON.parse(am.published[0].json);
  assert.equal(env.signerPubkey, undefined, 'anonymous → unsigned');
  ok('signWith: ANONYMOUS → unsigned');
}

// ── open topic with NO region → error (no global region) ──
{
  const { peer } = await mkPeer();
  await assert.rejects(() => peer.pub({ name: 'lobby' }, { x: 1 }, { signWith: me }),
    (e) => e instanceof PublishError && e.code === ErrorCodes.TOPIC_REGION_REQUIRED);
  ok('open topic without region → TOPIC_REGION_REQUIRED');
}

// ── owner-only topic: non-owner signer rejected; owner accepted ──
{
  const { peer } = await mkPeer();
  await assert.rejects(() => peer.pub({ owner: me.authorId, name: 'feed', write: 'owner' }, { x: 1 }, { signWith: alice }),
    (e) => e instanceof PublishError && e.code === ErrorCodes.WRITE_POLICY_VIOLATION);
  ok('owner-only + wrong signer → WRITE_POLICY_VIOLATION (publisher-side)');
}
{
  const { peer, am } = await mkPeer();
  await peer.pub({ owner: me.authorId, name: 'feed', write: 'owner' }, { x: 1 }, { signWith: me });   // region key-derived
  const env = JSON.parse(am.published[0].json);
  assert.equal(env.topic.write, 'owner'); assert.equal(env.topic.owner, me.authorId);
  assert.equal(env.signerPubkey, me.authorId);
  ok('owner-only feed by owner: key-derived placement, owner descriptor');
}

// ── sub resolves + subscribes to the SAME topic id a publisher computes ──
{
  const { peer, am } = await mkPeer();
  await peer.sub({ region: 'useast', name: 'lobby' }, () => {}, { since: 'all' });
  assert.equal(am.subscribed.length, 1, 'subscribed once');
  const want = await resolveTopic({ region: 'useast', name: 'lobby', write: 'open' });
  assert.equal(am.subscribed[0], BigInt('0x' + want.topicId), 'sub targets the resolved topic id (matches a publisher)');
  // owner feed: subscriber finds it from the Author ID alone (key-derived region)
  await peer.sub({ owner: me.authorId, name: 'feed', write: 'owner' }, () => {});
  const feed = await resolveTopic({ owner: me.authorId, name: 'feed', write: 'owner' });
  assert.equal(am.subscribed[1], BigInt('0x' + feed.topicId), 'owner-feed discoverable from Author ID (key-derived)');
  ok('sub({region,name}) and sub({owner,name}) resolve to publisher-matching topic ids');
}

console.log(`\nsmoke_pubsub_v3: ${n} checks passed`);
