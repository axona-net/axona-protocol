// =====================================================================
// smoke_rooted_topics.mjs — AxonaManager.rootedTopics() / AxonaPeer.rootedTopics()
// the read side of the derived metric-topic convention.
//
// A root must be able to enumerate the topics it serves, each with its signed
// topic descriptor + a locally-computed metric snapshot (no network), so an
// infrastructure node can republish those snapshots to metricTopic(T) on a
// timer. This checks the MECHANISM the kernel supplies; the POLICY (skip metric
// topics, skip owned topics, cadence, signing) lives in the relay loop.
//
//   1. a rooted open topic appears with its descriptor + current_count + subscribers
//   2. the descriptor is the SIGNED { region, owner, name, write } (recursion
//      guard + open/owned policy can be applied to it)
//   3. bytes reflects the cached envelope size; empty/expired roles report 0
//   4. a rooted METRIC topic is flagged by the core isMetricTopic() guard
//   5. an owned topic is recognisable as non-open (write:'owner') for the
//      privacy filter
//   6. a role with no cached envelope yields descriptor:null (caller skips it)
//
//   node test/smoke_rooted_topics.mjs
// =====================================================================

import { AxonaManager }        from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }       from '../src/pubsub/envelope.js';
import { toHex }               from '../src/utils/hexid.js';
import { deriveTopicId, metricTopic, isMetricTopic } from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

const T = 1_700_000_000_000;
const emptyRole = () => ({ isRoot: true, isInRootSet: true, children: new Map(), replayCache: [], peerRoots: new Set(), roleCreatedAt: T, emptiedAt: 0 });

function spawnMgr(selfId) {
  const routed = new Map(), direct = new Map();
  const dht = {
    getSelfId: () => selfId,
    onRoutedMessage: (t, h) => routed.set(t, h),
    onDirectMessage: (t, h) => direct.set(t, h),
    onEvent: () => () => {},
    findKClosest: async () => [],
    routeMessage: async () => {},
    sendDirect: async () => false,
  };
  const mgr = new AxonaManager({ dht, now: () => T });
  mgr.nodeId = selfId;
  return mgr;
}

async function rootTopic(mgr, descriptor, { message = 'hello', subscribers = 0 } = {}) {
  const author = await createAuthorIdentity();
  const env    = await buildEnvelope({ topic: descriptor, message, identity: author, ts: T, seq: T });
  const id     = await deriveTopicId(descriptor);
  const big    = BigInt('0x' + id);
  const role   = emptyRole();
  for (let i = 0; i < subscribers; i++) role.children.set('sub' + i, { createdAt: T, lastRenewed: T });
  mgr.axonRoles.set(big, role);
  mgr._addToReplayCache(role, { json: JSON.stringify(env), postHash: env.msgId, publishTs: T, publisher: null });
  return { id, big, env };
}

async function main() {
  console.log('Axona rootedTopics() — derived-metric read side');
  const mgr = spawnMgr(0x1234n);

  // ── 1–3. a rooted OPEN topic with one cached post + 2 subscribers ──
  const lobby = { region: 'useast', name: 'lobby' };
  const { id: lobbyId } = await rootTopic(mgr, lobby, { message: 'first', subscribers: 2 });

  // ── 4. a rooted METRIC topic (its descriptor name is in the reserved ns) ──
  const metricDesc = metricTopic(lobbyId);
  const { id: metricId } = await rootTopic(mgr, metricDesc, { message: JSON.stringify({ current_count: 1 }) });

  // ── 5. a rooted OWNED topic (write:'owner') ──
  const owner = await createAuthorIdentity();
  const ownedDesc = { region: 'useast', owner: owner.authorId, name: 'feed', write: 'owner' };
  const { id: ownedId } = await rootTopic(mgr, ownedDesc, { message: 'mine' });

  // ── 6. a role with NO cached envelope (subscribers only) ──
  const emptyId = await deriveTopicId({ region: 'useast', name: 'ghost' });
  const emptyRoleObj = emptyRole();
  emptyRoleObj.children.set('s', { createdAt: T, lastRenewed: T });
  mgr.axonRoles.set(BigInt('0x' + emptyId), emptyRoleObj);

  const rooted = mgr.rootedTopics();
  const byId = (id) => rooted.find(r => r.topicId === id);

  // 1
  const L = byId(lobbyId);
  check('1. rooted open topic is enumerated', !!L);
  check('1b. current_count = 1 cached post', L?.current_count === 1);
  check('1c. subscribers = 2', L?.subscribers === 2);

  // 2 — descriptor is the signed one, usable by the policy filters
  check('2a. descriptor carries the topic name', L?.descriptor?.name === 'lobby');
  check('2b. open topic descriptor: write !== "owner"', L?.descriptor?.write !== 'owner');
  check('2c. open topic is NOT flagged as a metric topic', isMetricTopic(L?.descriptor) === false);

  // 3 — bytes
  check('3. bytes reflects the cached envelope size', typeof L?.bytes === 'number' && L.bytes > 0);

  // 4 — metric topic flagged by the core guard (relay would SKIP it)
  const M = byId(metricId);
  check('4a. metric topic is enumerated', !!M);
  check('4b. isMetricTopic(descriptor) is true → recursion guard skips it',
    isMetricTopic(M?.descriptor) === true);

  // 5 — owned topic recognisable as non-open (privacy filter)
  const O = byId(ownedId);
  const isOpen = (d) => d?.write === 'open' || !d?.owner;
  check('5a. owned topic is enumerated', !!O);
  check('5b. owned topic descriptor: write === "owner" + owner set', O?.descriptor?.write === 'owner' && !!O?.descriptor?.owner);
  check('5c. open-policy predicate rejects the owned topic', isOpen(O?.descriptor) === false);
  check('5d. open-policy predicate accepts the lobby topic', isOpen(L?.descriptor) === true);

  // 6 — empty role → descriptor null (caller skips: nothing to report)
  const E = byId(emptyId);
  check('6a. empty role is enumerated', !!E);
  check('6b. empty role has descriptor:null (no envelope to recover)', E?.descriptor === null);
  check('6c. empty role current_count = 0', E?.current_count === 0);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
