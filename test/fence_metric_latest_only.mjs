// fence_metric_latest_only.mjs — 4.119.0: a METRIC topic (axona:metric:<id>) keeps
// ONLY its newest snapshot at every holder; an ordinary topic keeps its history.
//
// Why: a root publishes one snapshot per METRICS_PUB_MS (20 s) while any reader
// holds the metric subscription, and the 24 h cache kept all of them (~4,320 per
// open topic per day, 2026-10-22 census) though readers use only the newest.
//
// Scenes (each asserts its precondition so a deleted fix turns it red):
//   [A] metric topic, in-order stamped ingest → one entry, the newest
//   [B] metric topic, newest FIRST then older → the older is never cached
//   [C] ordinary topic, same flow → full history kept (no collateral)
//   [D] forged metric name on an ordinary topic → rejected, history intact,
//       the role is NOT marked latest-only
//   [E] history held before the kind was known → trimmed by the first bound body
//   [F] a pinned entry (dual-root 4(d)) survives the trim
//   [G] relay DELIVER path (unverified) marks the role only from a body whose
//       descriptor derives to the topic id; a metric fan-out keeps one entry
//
// Run: node test/fence_metric_latest_only.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { metricTopic } from '../src/pubsub/metrics.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

const nowRef = { t: 1_700_000_000_000 };
function mkHolder(topicBig) {
  const dht = {
    verdictsSupported: false,
    getSelfId: () => topicBig ^ (1n << 12n),
    onRoutedMessage: () => {},
    routeMessage: async () => ({ consumed: false }),
    neighbors: () => [],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => nowRef.t });
  am.nodeId = dht.getSelfId();
  am.setLogSink(() => {});
  const role = makeRole(topicBig, true, nowRef.t);
  am.axonRoles.set(topicBig, role);
  return { am, role };
}

const author = await createAuthorIdentity();
const dataId = await deriveTopicId({ region: 'useast', name: 'fence-metric-data' });
const mDesc = { ...metricTopic(dataId), owner: null, write: 'open' };
const M = await deriveTopicIdBig(mDesc);
const oDesc = { region: 'useast', name: 'fence-metric-ordinary', write: 'open' };
const O = await deriveTopicIdBig(oDesc);

const BASE = nowRef.t - 60_000;
async function bodies(desc, n, tag) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const env = await buildEnvelope({ topic: desc, message: JSON.stringify({ v: 1, tag, i, ts: BASE + i * 20 }), identity: author });
    out.push({ msgId: env.msgId, publishTs: BASE + i * 20, seq: i + 1, json: JSON.stringify(env) });
  }
  return out;
}
const ingest = (h, list) => (async () => { const r = []; for (const m of list) r.push(await h.am._ingestStamped(h.role, { ...m })); return r; })();

const mBodies = await bodies(mDesc, 5, 'metric');
const oBodies = await bodies(oDesc, 5, 'ordinary');

console.log('[A] metric topic, in-order stamped ingest');
{
  const h = mkHolder(M);
  check('precondition: role starts undecided', h.role.latestOnly === undefined);
  const r = await ingest(h, mBodies);
  check('every ingest reports held', r.every(x => x === 'held'), JSON.stringify(r));
  check('role marked latest-only', h.role.latestOnly === true);
  check('one entry held', h.role.cache.length === 1 && h.role.cacheIds.size === 1, `len=${h.role.cache.length}`);
  check('it is the newest', h.role.cache[0]?.msgId === mBodies[4].msgId);
  check('cacheBytes matches the one entry', h.role.cacheBytes === h.role.cache[0]?.bytes, `${h.role.cacheBytes}`);
}

console.log('[B] metric topic, newest first then older');
{
  const h = mkHolder(M);
  await ingest(h, [mBodies[4], mBodies[1], mBodies[3], mBodies[0]]);
  check('only the newest held', h.role.cache.length === 1 && h.role.cache[0].msgId === mBodies[4].msgId, `len=${h.role.cache.length}`);
  check('older ids not in cacheIds', !h.role.cacheIds.has(mBodies[1].msgId) && !h.role.cacheIds.has(mBodies[0].msgId));
}

console.log('[C] ordinary topic keeps its history');
{
  const h = mkHolder(O);
  await ingest(h, oBodies);
  check('role marked NOT latest-only', h.role.latestOnly === false);
  check('all five held', h.role.cache.length === 5, `len=${h.role.cache.length}`);
}

console.log('[D] forged metric name on an ordinary topic');
{
  const h = mkHolder(O);
  await ingest(h, oBodies.slice(0, 3));
  check('precondition: three ordinary bodies held', h.role.cache.length === 3);
  const forged = { ...mBodies[4], publishTs: BASE + 999 };     // a metric-named body sent to topic O
  const r = await h.am._ingestStamped(h.role, forged);
  check('forged body rejected', r === 'rejected', r);
  check('history intact', h.role.cache.length === 3);
  check('role still ordinary', h.role.latestOnly === false);
}

console.log('[E] history held before the kind was known is trimmed');
{
  const h = mkHolder(M);
  for (const m of mBodies.slice(0, 3)) h.am._cachePush(h.role, { ...m });   // undecided: cached as ordinary
  check('precondition: three held while undecided', h.role.cache.length === 3 && h.role.latestOnly === undefined);
  await ingest(h, [mBodies[4]]);
  check('trimmed to the newest', h.role.cache.length === 1 && h.role.cache[0].msgId === mBodies[4].msgId, `len=${h.role.cache.length}`);
}

console.log('[F] a pinned entry survives the trim');
{
  const h = mkHolder(M);
  await ingest(h, [mBodies[0]]);
  h.role.merge = { pins: new Set([mBodies[0].msgId]) };
  await ingest(h, [mBodies[2]]);
  check('pinned older entry kept', h.role.cacheIds.has(mBodies[0].msgId));
  check('newest kept', h.role.cacheIds.has(mBodies[2].msgId));
  check('nothing else', h.role.cache.length === 2, `len=${h.role.cache.length}`);
  h.role.merge = null;
}

console.log('[G] relay DELIVER path');
{
  const h = mkHolder(M);
  h.role.isRoot = false;
  const from = (M ^ (1n << 20n)).toString(16).padStart(66, '0');
  const topicId = M.toString(16).padStart(66, '0');
  for (const m of mBodies) {
    await h.am._onDeliver({ topicId, from, msgs: [{ ...m }] }, { targetId: h.am.nodeId });
  }
  check('role marked latest-only from a bound body', h.role.latestOnly === true);
  check('one entry held', h.role.cache.length === 1 && h.role.cache[0].msgId === mBodies[4].msgId, `len=${h.role.cache.length}`);

  const h2 = mkHolder(O);
  h2.role.isRoot = false;
  const topicId2 = O.toString(16).padStart(66, '0');
  await h2.am._onDeliver({ topicId: topicId2, from, msgs: [{ ...mBodies[4] }] }, { targetId: h2.am.nodeId });
  check('an unbound metric-named body does not mark an ordinary role', h2.role.latestOnly !== true);
  for (const m of oBodies) await h2.am._onDeliver({ topicId: topicId2, from, msgs: [{ ...m }] }, { targetId: h2.am.nodeId });
  check('ordinary relay keeps its history', h2.role.cache.filter(e => oBodies.some(o => o.msgId === e.msgId)).length === 5, `len=${h2.role.cache.length}`);
}

console.log('[H] root publish path (_ingestPublish)');
{
  nowRef.t = Date.now();                       // live ingress checks freshness against the clock
  const h = mkHolder(M);
  h.am._rootReplicas = 0;                      // hermetic: no cohort replication on the wire
  const ids = [];
  for (let i = 0; i < 4; i++) {
    const env = await buildEnvelope({ topic: mDesc, message: JSON.stringify({ v: 1, tag: 'live', i }), identity: author });
    const r = await h.am._ingestPublish(h.role, JSON.stringify(env));
    ids.push(env.msgId);
    if (!r?.ok) { check(`publish ${i} accepted`, false, JSON.stringify(r)); break; }
    nowRef.t += 20_000;
  }
  check('role marked latest-only by a live publish', h.role.latestOnly === true);
  check('root holds one entry, the newest', h.role.cache.length === 1 && h.role.cache[0].msgId === ids[ids.length - 1], `len=${h.role.cache.length}`);
}

console.log(`\nfence_metric_latest_only: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
