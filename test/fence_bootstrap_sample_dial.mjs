#!/usr/bin/env node
// fence_bootstrap_sample_dial — Bootstrap Sample Dial (4.135.0).
//
// A node WITHOUT the fill (no synaptomeMaintain, attemptGuard, admissionGate:
// what an application passes when it passes nothing) dials the ids a bridge
// offers in a peer-list's `graduated` field. Before this the row-12 handler
// returned at once on such a node, and a newcomer to a bridge whose clients
// had left their sockets held ONE connection, the bridge.
//
//   A. NO PATH, NO DIAL. With no bound peer the offer is kept and nothing is
//      dialed. Handles (`c17`, `c-self-9`) are never read as ids.
//   B. A BIND RUNS THE PASS. When a peer binds, each kept id is dialed once
//      through connectViaRelay. The node's own id, a table member and a
//      bound peer are never dialed.
//   C. BOUNDED. A failed dial is retried at the next bind and dropped after
//      2 tries. A second bind with nothing left dials nothing. At most 16
//      ids are kept from one list. An offer older than 60 s is dropped.
//   D. NEWEST OFFER REPLACES. A list with no ids leaves the offer alone.
//   E. OFF. `bootstrapSample: false` dials nothing.
//   F. THE FILL IS UNTOUCHED. An armed node nominates and never takes this path.
//
// Mutants (each run by hand; the section named fails): drop the anyBound
// break → A; drop the onPeerBound trigger → B; drop the tries bound → C;
// merge instead of replace → D; ignore the option → E; call the offer before
// the _fillArmed test → F.
import { AxonaPeer }            from '../src/dht/AxonaPeer.js';
import { AxonaDomain }          from '../src/dht/AxonaDomain.js';
import { NeuronNode }           from '../src/dht/NeuronNode.js';
import { Transport }            from '../src/contracts/Transport.js';
import { depositDispatchCapability } from '../src/registry/index.js';
import { createNodeIdentity }   from '../src/identity/index.js';
import { fromHex, toHex }       from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise((r) => setTimeout(r, 5));
const ARMED = { synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 4, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2 } };

class StubTransport extends Transport {
  constructor() { super(); this.listH = []; this.boundH = []; this.bound = new Set(); this.dials = []; this.relayAnswer = () => 'inc-1'; depositDispatchCapability(this, { request: () => {}, notification: () => {} }); }
  onPeerList(h) { this.listH.push(h); return () => {}; }
  emitPeerList(list) { for (const h of this.listH) h(list.slice()); }
  onPeerBound(h) { this.boundH.push(h); return () => {}; }
  bind(big) { this.bound.add(big); for (const h of this.boundH) h(big, 'm', undefined); }
  connectViaRelay(hex) { this.dials.push(hex); return this.relayAnswer(hex); }
  async start() {} async stop() {} getLocalNodeId() { return 0n; }
  async openConnection() { return false; } async closeConnection() {}
  isConnected(id) { return this.bound.has(id); }
  async send() { throw new Error('stub'); } async notify() {}
  onPeerDied() { return () => {}; } getLatency() { return 10; }
  boundPeers() { return [...this.bound]; }
}

async function make(opts = {}) {
  const id = await createNodeIdentity({ lat: 38, lng: -77 });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 38, lng: -77 });
  const t = new StubTransport(); node.transport = t;
  const peer = new AxonaPeer({ domain: new AxonaDomain(), node, nodeIdentity: id, transport: t, ...opts });
  peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  const self = fromHex(id.id);
  const hex = (i) => (self ^ (1n << BigInt(40 + i))).toString(16).padStart(66, '0');
  return { id, node, t, peer, self, hex };
}

(async () => {
  console.log('fence_bootstrap_sample_dial: a node without the fill dials the bridge\'s offered ids, once a peer is bound, within bounds');

  // ── A + B + C ──────────────────────────────────────────────────────────
  {
    const { id, t, peer, hex } = await make();
    const g1 = hex(1), g2 = hex(2), boundHex = hex(3), bridgeHex = hex(9);
    check('precondition: the fill is not armed', peer._fillArmed() === false);
    t.bound.add(fromHex(boundHex)); t.bound.delete(fromHex(boundHex));          // nothing bound
    t.emitPeerList(['c17', 'c-self-9', g1, id.id, g2, g1]);
    await tick();
    check('A1 with no bound peer nothing is dialed', t.dials.length === 0, `dials ${t.dials.length}`);
    check('A2 the offer is kept: two ids, handles and own id ignored', peer._bootSample.size === 2 && peer._bootSample.has(fromHex(g1)) && peer._bootSample.has(fromHex(g2)));
    t.bind(fromHex(bridgeHex));
    await tick();
    check('B1 a bind runs the pass: both ids dialed through the relay, once each', t.dials.length === 2 && t.dials.includes(g1) && t.dials.includes(g2), JSON.stringify(t.dials.map((d) => d.slice(-6))));
    check('B2 the offer is spent', peer._bootSample.size === 0 && peer._bootSampleStats.dialed === 2);
    t.bind(fromHex(hex(10)));
    await tick();
    check('C1 a later bind with nothing left dials nothing', t.dials.length === 2);
    await peer.stop?.();
  }
  {
    const { t, peer, hex } = await make();
    const bridge = fromHex(hex(9)), member = hex(4), boundHex = hex(3), g = hex(1);
    t.bind(bridge);
    t.bound.add(fromHex(boundHex));
    peer._node.synaptome.set?.(fromHex(member), {}) ?? peer._node.synaptome.add?.(fromHex(member));
    t.emitPeerList([member, boundHex, g]);
    await tick();
    check('B3 a table member and a bound peer are never dialed', t.dials.length === 1 && t.dials[0] === g, JSON.stringify(t.dials.map((d) => d.slice(-6))));
    await peer.stop?.();
  }
  {
    const { t, peer, hex } = await make();
    t.relayAnswer = () => false;                                                 // the relay cannot issue
    t.bind(fromHex(hex(9)));
    t.emitPeerList([hex(1)]);
    await tick();
    check('C2 a failed dial keeps the id for the next bind', t.dials.length === 1 && peer._bootSample.size === 1);
    t.bind(fromHex(hex(10))); await tick();
    check('C3 the second failure drops it', t.dials.length === 2 && peer._bootSample.size === 0 && peer._bootSampleStats.failed === 1);
    t.bind(fromHex(hex(11))); await tick();
    check('C4 no third dial', t.dials.length === 2);
    await peer.stop?.();
  }
  {
    const { t, peer, hex } = await make();
    t.emitPeerList(Array.from({ length: 40 }, (_, i) => hex(i + 20)));
    check('C5 at most 16 ids are kept from one list', peer._bootSample.size === 16, `kept ${peer._bootSample.size}`);
    for (const r of peer._bootSample.values()) r.at = Date.now() - 61_000;
    t.bind(fromHex(hex(9))); await tick();
    check('C6 an offer older than 60 s is dropped undialed', t.dials.length === 0 && peer._bootSample.size === 0 && peer._bootSampleStats.expired === 16);
    await peer.stop?.();
  }

  // ── D ──────────────────────────────────────────────────────────────────
  {
    const { t, peer, hex } = await make();
    t.emitPeerList([hex(1), hex(2)]);
    t.emitPeerList(['c17']);
    check('D1 a list with no ids leaves the offer alone', peer._bootSample.size === 2);
    t.emitPeerList([hex(5)]);
    check('D2 the newest offer replaces the old one', peer._bootSample.size === 1 && peer._bootSample.has(fromHex(hex(5))));
    await peer.stop?.();
  }

  // ── E ──────────────────────────────────────────────────────────────────
  {
    const { t, peer, hex } = await make({ bootstrapSample: false });
    t.bind(fromHex(hex(9)));
    t.emitPeerList([hex(1)]);
    await tick();
    check('E1 bootstrapSample:false keeps nothing and dials nothing', peer._bootSample.size === 0 && t.dials.length === 0);
    await peer.stop?.();
  }

  // ── F ──────────────────────────────────────────────────────────────────
  {
    const { t, peer, hex } = await make(ARMED);
    t.bind(fromHex(hex(9)));
    t.emitPeerList([hex(1)]);
    await tick();
    check('F1 an armed node nominates into the fill cache and takes no bootstrap pass', peer._fillCache.has(fromHex(hex(1))) && peer._bootSample.size === 0 && peer._bootSampleStats.offers === 0 && t.dials.length === 0);
    await peer.stop?.();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error('fence crashed:', e); process.exit(2); });
