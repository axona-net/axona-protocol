// =====================================================================
// smoke_pubsub_beacon.mjs — root-beacon last-mile correction (Pubsub-Root-Beacon-v0.1).
//
// The idealized Fabric (other pubsub smokes) routes to the GLOBAL closest, so
// publisher and subscribers always agree on the root and divergence can't occur.
// This smoke uses a GAPPY-ROUTING fabric: routeMessage greedy-walks over each
// node's explicit neighbor set and STOPS at a local minimum (no neighbor closer
// to the target). We engineer a topology where the publisher dead-ends at Y —
// the second-closest node to the topic — while subscribers home on the true root
// R. Without the beacon, Y becomes a spurious root and delivery is 0. With the
// beacon (R announces "root for T = R" into its 2-hop neighborhood, reaching Y),
// Y corrects the publish to R and every subscriber receives it.
//
// Run: node test/smoke_pubsub_beacon.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

// Gappy mesh: greedy routeMessage over explicit neighbor sets; terminus = local
// minimum in XOR distance to the target (exactly DHT greedy routing on a sparse
// graph). neighbors() exposes the node's adjacency to the beacon.
class GappyFabric {
  constructor() { this.nodes = new Map(); this.adj = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig) {
    const handlers = new Map(); const self = this; const me = idBig;
    const dht = {
      getSelfId: () => me,
      neighbors: () => [...(self.adj.get(me) || [])],
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload, meta = {}) => {
        const dest = self._greedyTerminus(me, target);
        if (dest === null) return;
        const isTerminal = (dest === self._globalClosest(target)) || (self._greedyTerminus(dest, target) === dest);
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: meta.fromId ?? idHex(me) } });
      },
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, dropMs: 180_000 });
    const rec = { id: me, am, handlers, alive: true, got: [] };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(me, rec);
    return rec;
  }
  link(a, b) {
    (this.adj.get(a) ?? this.adj.set(a, new Set()).get(a)).add(b);
    (this.adj.get(b) ?? this.adj.set(b, new Set()).get(b)).add(a);
  }
  _globalClosest(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  // Greedy walk from `start` toward `target` over live neighbors; stop at a local min.
  _greedyTerminus(start, target) {
    let cur = start, guard = 0;
    while (guard++ < 64) {
      let next = cur, bestD = cur ^ target;
      for (const nb of (this.adj.get(cur) || [])) {
        const n = this.nodes.get(nb); if (!n || !n.alive) continue;
        const d = nb ^ target; if (d < bestD) { bestD = d; next = nb; }
      }
      if (next === cur) return cur;
      cur = next;
    }
    return cur;
  }
  async settle(cap = 200000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle: did not converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(j.type);
      if (!h) continue;
      await h(j.payload, j.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}

async function main() {
  console.log('Axona pub/sub — root beacon last-mile correction (gappy mesh)');
  const author = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'beacon-demo', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // Generate a pool of node identities and rank by XOR distance to the topic.
  const fab = new GappyFabric();
  const pool = [];
  for (let i = 0; i < 24; i++) {
    const id = await createNodeIdentity({ lat: (i * 13) % 80 - 40, lng: (i * 29) % 300 - 150 });
    pool.push(BigInt('0x' + id.id));
  }
  pool.sort((a, b) => { const da = a ^ topicId, db = b ^ topicId; return da < db ? -1 : da > db ? 1 : 0; });
  const R = pool[0];        // true root (closest to topic)
  // Find Y (local-minimum dead-end toward the TOPIC) + Z (bridge that is closer
  // to R than Y, so a forward Y→Z→R actually progresses). Both constraints must
  // hold simultaneously — distance-to-topic and distance-to-root are independent.
  let Y = null, Z = null;
  for (const y of pool.slice(1)) {
    for (const z of pool) {
      if (z === R || z === y) continue;
      if ((z ^ topicId) > (y ^ topicId) && (z ^ R) < (y ^ R)) { Y = y; Z = z; break; }
    }
    if (Y) break;
  }
  if (!Y || !Z) { console.error('topology search failed (regenerate pool)'); process.exit(2); }
  const used = new Set([R, Y, Z]);
  const rest = pool.filter(id => !used.has(id));
  // P must be farther from the TOPIC than Y (so it funnels INTO Y and stops) AND
  // farther from R than Z (so Y's greedy step toward R is Z, not P).
  const P = rest.find(id => (id ^ topicId) > (Y ^ topicId) && (id ^ R) > (Z ^ R)) ?? rest[rest.length - 1];
  const subs = rest.filter(id => id !== P).slice(0, 4);
  for (const id of [R, Y, Z, P, ...subs]) fab.addNode(id);

  // Topology engineered for divergence:
  //  Y's ONLY neighbor is Z, and d(Z,T) > d(Y,T) → Y is a local minimum (greedy
  //  arriving at Y stops). Z bridges to R. The publisher flows into Y and stops;
  //  subscribers attach directly to R. R reaches Y only via the 2-hop beacon R→Z→Y.
  fab.link(Z, R);
  fab.link(Z, Y);
  fab.link(P, Y);
  for (const s of subs) fab.link(s, R);

  const rec = (id) => fab.nodes.get(id);
  const xstr = (id) => idHex(id).slice(0, 8);
  console.log(`  topic ${xstr(topicId)}…  root R=${xstr(R)} deadend Y=${xstr(Y)} bridge Z=${xstr(Z)} pub P=${xstr(P)}`);

  // sanity: greedy from P really dead-ends at Y (not R)
  check('publisher greedy-routes to the dead-end Y, not the true root R', fab._greedyTerminus(P, topicId) === Y);

  // subscribers attach (their SUB greedy-reaches R, which becomes root)
  for (const s of subs) rec(s).am.pubsubSubscribe(topicId);
  await fab.settle();
  const rootRole = rec(R).am.axonRoles.get(topicId);
  check('R became the single root and adopted the subscribers',
    !!rootRole && rootRole.isRoot && rootRole.subscribers.size === subs.length,
    `(subs=${rootRole?.subscribers?.size})`);

  // ── CONTROL: publish BEFORE any beacon → dead-ends at Y, subscribers get nothing
  {
    const e = await buildEnvelope({ topic: desc, message: { n: 'pre-beacon' }, seq: 1, identity: author, ts: fab.clock });
    rec(P).am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle();
    const got = subs.filter(s => rec(s).got.includes(e.msgId)).length;
    check('WITHOUT beacon: publish strands at Y → 0/N subscribers (the bug)', got === 0, `(${got}/${subs.length})`);
    check('Y wrongly became a spurious root for the stranded publish', !!rec(Y).am.axonRoles.get(topicId)?.isRoot);
  }

  // ── emit beacons: R announces itself; flood reaches Y via R→Z→Y (2 layers)
  fab.clock += 21_000;                 // pass BEACON_MS throttle
  await fab.tickAll();                 // refreshTick → _emitRootBeacons on R → settle
  const yBeacon = rec(Y).am._rootBeacons.get(topicId);
  check('beacon propagated 2 hops to the dead-end Y (Y cached root=R)',
    !!yBeacon && yBeacon.root === idHex(R), `(${yBeacon ? xstr(BigInt('0x' + yBeacon.root)) : 'none'})`);
  check('Y did NOT accept a beacon farther than its own best-known (verify-don\'t-trust holds)', !!yBeacon);

  // ── WITH beacon: publish again → Y corrects to R → all subscribers receive
  {
    const e = await buildEnvelope({ topic: desc, message: { n: 'post-beacon' }, seq: 2, identity: author, ts: fab.clock });
    rec(P).am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle();
    const got = subs.filter(s => rec(s).got.includes(e.msgId)).length;
    check('WITH beacon: Y corrects the publish to R → ALL subscribers receive', got === subs.length, `(${got}/${subs.length})`);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
