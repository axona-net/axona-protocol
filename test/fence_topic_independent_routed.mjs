// fence_topic_independent_routed.mjs — GH #26, the ROUTED multi-node case.
//
// fence_topic_independent_durability.mjs proves per-Role isolation at a single
// node that roots both topics. This proves the same thing END TO END across a
// routed fabric: one node is the shared root for TWO topics; two SEPARATE
// subscriber nodes each subscribe to one; a publisher sends the SAME body (hence
// the SAME msgId, topic excluded from the hash) to both topics. Each subscriber
// must receive ITS topic's copy — the colliding msgId must not let the shared
// root fan one delivery out and swallow the other.
//
// Honest routed delivery: routeMessage hands the payload to the real handler on
// the XOR-closest live node and reports the real verdict. Nothing fakes a
// dispatch or a delivery.
//
// Run: node test/fence_topic_independent_routed.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};
const idHex = (b) => b.toString(16).padStart(66, '0');
const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });
const settle = async () => { for (let i = 0; i < 6; i++) await new Promise(r => setImmediate(r)); };

class Net {
  constructor() { this.nodes = new Map(); this.clock = { t: 1_700_000_000_000 }; }
  _closest(target) { let best = null, bd = null; for (const id of this.nodes.keys()) { const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
  add(idBig, { rootReplicas = 0 } = {}) {
    const self = this;
    const handlers = new Map();
    const dht = {
      verdictsSupported: true,
      getSelfId: () => idBig,
      onRoutedMessage: (type, fn) => handlers.set(type, fn),
      routeMessage: async (target, type, payload) => {
        let t; try { t = typeof target === 'bigint' ? target : BigInt('0x' + String(target)); } catch { t = null; }
        const dest = t === null ? null : self._closest(t);
        if (dest === null) return { consumed: false, exhausted: true };
        const h = self.nodes.get(dest).handlers.get(type);
        if (!h) return { consumed: false, terminal: true };
        const r = await h(payload, { targetId: dest, isTerminal: true, fromId: idHex(idBig) });
        return r === 'consumed' ? { consumed: true, atNode: idHex(dest), hops: 1 } : { consumed: false, terminal: true };
      },
      findKClosest: async (target, k = 3) => [...self.nodes.keys()].filter(x => x !== idBig)
        .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k).map(idHex),
      neighbors: () => [...self.nodes.keys()].filter(x => x !== idBig).map(idHex),
      bridgeId: () => null,
      lookup: async () => ({ path: [] }),
      isReachableId: () => true,
    };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => self.clock.t, rootReplicas });
    am.nodeId = idBig; am.setLogSink(() => {});
    this.nodes.set(idBig, { am, handlers, id: idBig });
    return am;
  }
}

async function main() {
  console.log('Topic-independent durability — ROUTED multi-node: one shared root, two subscribers, one msgId\n');

  const author = await createAuthorIdentity();
  const descA = DESC('routed-A'), descB = DESC('routed-B');
  const topicA = await deriveTopicIdBig(descA);
  const topicB = await deriveTopicIdBig(descB);
  const MSG = { alert: 'identical-bytes-two-topics-routed' };

  const net = new Net();
  // R sits AT topicA and, because the subscriber/publisher nodes are placed in a
  // flipped region (high byte XOR 0xFF), stays XOR-closest to BOTH useast topics —
  // so it is the shared root for topicA and topicB. Stated, not left to luck.
  const FAR = (id, salt) => (id ^ (0xffn << 256n)) ^ salt;
  const R  = net.add(topicA, { rootReplicas: 0 });
  const SA = net.add(FAR(topicA, 0x1n));
  const SB = net.add(FAR(topicB, 0x2n));
  const P  = net.add(FAR(topicA, 0x3n));

  ok('0a. R is XOR-closest to topicA', net._closest(topicA) === topicA);
  ok('0b. R is XOR-closest to topicB too (shared root)', net._closest(topicB) === topicA);

  // R roots BOTH topics.
  R.pubsubSubscribe(topicA); R._becomeRoot(topicA);
  R.pubsubSubscribe(topicB); R._becomeRoot(topicB);
  await settle();

  // The two subscriber nodes, one per topic. Their SUB routes to the closest
  // node = R.
  const gotA = [], gotB = [];
  SA.onPubsubDelivery((t, _j, msgId) => gotA.push({ topic: String(t), msgId }));
  SB.onPubsubDelivery((t, _j, msgId) => gotB.push({ topic: String(t), msgId }));
  SA.pubsubSubscribe(topicA); await settle();
  SB.pubsubSubscribe(topicB); await settle();
  const roleA = R.axonRoles.get(topicA), roleB = R.axonRoles.get(topicB);
  ok('1a. R registered SA as a subscriber of topic A', roleA?.subscribers?.size >= 1, `subsA=${roleA?.subscribers?.size}`);
  ok('1b. R registered SB as a subscriber of topic B', roleB?.subscribers?.size >= 1, `subsB=${roleB?.subscribers?.size}`);

  // The publisher sends the SAME body to both topics → the SAME msgId.
  const envA = await buildEnvelope({ topic: descA, message: MSG, seq: 1, identity: author, ts: net.clock.t });
  const envB = await buildEnvelope({ topic: descB, message: MSG, seq: 1, identity: author, ts: net.clock.t });
  ok('2a. the two publishes carry the SAME msgId', envA.msgId === envB.msgId, `${envA.msgId} vs ${envB.msgId}`);
  P.pubsubPublish(topicA, JSON.stringify(envA)); await settle();
  P.pubsubPublish(topicB, JSON.stringify(envB)); await settle();

  // Each subscriber must have received ITS topic's copy of the shared msgId.
  const aGot = gotA.filter((d) => d.topic === String(topicA) && d.msgId === envA.msgId).length;
  const bGot = gotB.filter((d) => d.topic === String(topicB) && d.msgId === envB.msgId).length;
  ok('3a. subscriber A received topic A\'s body', aGot === 1, `aGot=${aGot} gotA=${JSON.stringify(gotA.map(d=>d.topic.slice(-4)+':'+d.msgId.slice(0,4)))}`);
  ok('3b. subscriber B received topic B\'s body — the shared msgId did NOT swallow the second delivery', bGot === 1, `bGot=${bGot} gotB=${JSON.stringify(gotB.map(d=>d.topic.slice(-4)+':'+d.msgId.slice(0,4)))}`);
  ok('3c. SA did NOT receive topic B\'s traffic and vice-versa (no cross-topic leak)',
    gotA.every((d) => d.topic === String(topicA)) && gotB.every((d) => d.topic === String(topicB)));

  // The shared root holds a SEPARATE durability obligation per topic.
  ok('4a. shared root holds topic A\'s obligation', roleA.durability?.get(envA.msgId) != null);
  ok('4b. shared root holds topic B\'s SEPARATE obligation for the same msgId',
    roleB.durability?.get(envB.msgId) != null && roleA.durability !== roleB.durability);

  console.log(`\n${fail === 0 ? '✓ all ' + n + ' checks passed' : '✗ ' + fail + ' FAILED'}`);
  process.exit(fail === 0 ? 0 : 1);
}
main().catch((e) => { console.error('threw:', e?.stack || e); process.exit(2); });
