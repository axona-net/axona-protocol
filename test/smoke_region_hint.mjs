// smoke_region_hint.mjs — region is a PLACEMENT HINT, never an eligibility gate
// (v4.75.0, replacing smoke_region_lock).
//
// The region byte (S2 high byte) is folded into every id for routing locality.
// No code compares regions to gate admission, rooting, seating, storage, repair,
// or delivery. The closest reachable node roots a topic whatever its region — an
// empty region is rooted by a neighbour, not refused. This smoke fails if any of
// the removed region walls is reintroduced (the configureRegionLock flag, the
// _topicDecision 'reject', the _promoteChild / _onAdopt / pubsubHost refusals).
//
// Run: node test/smoke_region_hint.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const idHex = (b) => b.toString(16).padStart(66, '0');
const mk = (region, tag) => (BigInt(region) << 256n) | BigInt(tag);   // region byte = top byte

const SELF    = mk(0x89, 0x11);             // this node is in region 0x89
const HOME    = mk(0x89, 0xabc);            // a topic in OUR region
const FOREIGN = mk(0x12, 0xabc);            // a topic in a region with NO node here

function newAM() {
  const sent = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: false,
    routeMessage: (_t, type, payload) => sent.push({ type, payload }),
    neighbors: () => [], bridgeId: () => null, findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => Date.now() });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, sent };
}
const termIn = { isTerminal: true };
const bare = (t) => ({ topicId: idHex(t), via: [] });

// 1. _topicDecision: the terminus handles ANY region — no 'reject' exists.
{
  const { am } = newAM();
  ok('in-region bare terminus → handle',        am._topicDecision(bare(HOME),    termIn) === 'handle');
  ok('foreign-region bare terminus → handle (no reject gate)', am._topicDecision(bare(FOREIGN), termIn) === 'handle');
}

// 2. _onSub at a foreign-region terminus forms the root (empty region is served).
{
  const { am } = newAM();
  am._onSub({ ...bare(FOREIGN), subscriberId: idHex(mk(0x89, 0x77)), since: 0 }, termIn);
  const role = am.axonRoles.get(FOREIGN);
  ok('foreign-region subscribe forms the root here', !!role && role.isRoot);
}

// 3. _promoteChild promotes any leaf, foreign region included.
{
  const { am } = newAM();
  am._onSub({ ...bare(HOME), subscriberId: idHex(mk(0x89, 0x77)), since: 0 }, termIn);
  const role = am.axonRoles.get(HOME);
  const foreignLeaf = idHex(mk(0x12, 0xBB));
  role.subscribers.set(foreignLeaf, { since: 0, lastRenewed: am._now() });
  role.subscribers.set(idHex(mk(0x12, 0xCC)), { since: 0, lastRenewed: am._now() });
  am._promoteChild(role);
  ok('a foreign-region leaf is eligible for promotion to a child relay', role.children.size >= 1);
}

// 4. _onAdopt relays a foreign-region topic (no region refusal).
{
  const { am } = newAM();
  am._onAdopt({ topicId: idHex(FOREIGN), parent: idHex(mk(0x12, 0x1)), subs: [] }, { targetId: SELF });
  ok('_onAdopt relays a foreign-region topic', am.axonRoles.has(FOREIGN));
}

// 5. pubsubHost accepts a foreign-region topic.
{
  const { am } = newAM();
  am.pubsubHost(FOREIGN);
  ok('pubsubHost accepts a foreign-region topic', am._hostedTopics.has(FOREIGN));
  am.pubsubHost(HOME);
  ok('pubsubHost accepts an in-region topic', am._hostedTopics.has(HOME));
}

console.log(`\n${fail ? '✗' : '✓'} smoke_region_hint: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
