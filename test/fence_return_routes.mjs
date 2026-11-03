#!/usr/bin/env node
// =====================================================================
// fence_return_routes.mjs — Closer-Born Relay Inherits the Root v0.6, the
// return routes for pub/sub (David 2026-11-03 "go ahead with the return
// routes"): a SUB, MERGE_OFFER or PULLUP leaves a breadcrumb at the node it
// reaches, and the frames that node sends back return by the reverse of the
// request's hops, with the own lookup as fallback.
//
// The island, as in fence_signal_return_route: real AxonaPeers on the sim
// transport, bound-only, chain O -> H1..Hn -> T with FORWARD synapses only;
// after the request crosses, every backward entry it taught is removed while
// channels stay open, so T's own routing cannot reach O. Live cases: 01:38Z
// (a merge pull across the island timed out) and 03:47Z (a fresh reader's
// SUB reached a holder of 92 bodies; the replay never came back).
//   P0 control (manager returnRoute off): T's REPLAYUP to O is not delivered.
//   P1 on, PULLUP: T's REPLAYUP returns over the five reverse hops.
//   P2 on, SUB re-sent by H1 naming O as subscriber: the breadcrumb keyed O
//      ends at O, and T's reply reaches O through H1.
//   P3 on, MERGE_OFFER: the breadcrumb keyed by the loser is recorded and
//      T's REPLAYACK reaches it.
//   P4 a 12-hop PULLUP with domain RETURN_ROUTE on keeps all twelve hops and
//      the reply returns; with it off the path is cut to PATH_MEMORY and the
//      reply does not cross.
//   P5 a broken reverse hop: the return route falls back once, nothing
//      throws. P6 an expired breadcrumb is not used. P7 defaults are OFF.
// Orion ab295b9b (RR-1..3):
//   P8 RR-1: a direct PULLUP records once; a SUB from H2 merely NAMING O does
//      not replace O's fresh anchored breadcrumb.
//   P9 RR-2: relays that only forward a SUB keep no breadcrumb.
//   P10 RR-3: a breadcrumb that fell back is dropped.
//   P11 RR-2: a REPLAYUP arriving refreshes the receiver's way back to its sender.
// Run: node test/fence_return_routes.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const N_CHECKS = 14;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const J = (v) => JSON.stringify(v, (k, x) => typeof x === 'bigint' ? x.toString(16).slice(0, 12) : x);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
process.on('exit', (code) => { if (code === 0 && passed + failed < N_CHECKS) { console.log(`  ✗ exited after ${passed + failed} of ${N_CHECKS} checks`); process.exitCode = 1; } });

function boundOnly(t) {
  t.openConnection = async (id) => t.isConnected(id);
  const send = t.send.bind(t);
  t.send = async (id, type, body) => { if (!t.isConnected(id)) throw new Error(`Transport.send: peer ${String(id)} not connected`); return send(id, type, body); };
  return t;
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
async function build({ rr, n = 6, domainRR = false }) {
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK_DEADLINE_MS: 800, RETURN_ROUTE: domainRR });
  const raw = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    const am = peer._requireAxonaManager('fence');
    if (rr !== null) am._returnRoute = rr;
    raw.push({ peer, transport, node, am, big: fromHex(id.id), hex: id.id });
  }
  const T = raw[0];
  const chain = raw.slice(1).sort((x, y) => ((x.big ^ T.big) > (y.big ^ T.big) ? -1 : 1));
  const nodes = [...chain, T];
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  await sleep(20);
  return { O: nodes[0], T, nodes, all: raw };
}
function island(w) {
  for (let i = 1; i < w.nodes.length; i++) w.nodes[i].node.synaptome.delete(w.nodes[i - 1].big);
  for (const p of w.all) p.node.incomingSynapses?.clear?.();
}
async function stopAll(w) { for (const p of w.all) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
const TOPIC = 'ab'.repeat(33);
const pullup = (w) => w.O.am._route(w.T.big, 'pubsub:pullup', { topicId: TOPIC, sinceHw: 0, parentId: w.O.hex });
const replayUpTo = (w, to) => w.T.am._route(to.big, 'pubsub:replayup', { topicId: TOPIC, msgs: [] });
const key = (p) => p.hex.toLowerCase();

(async () => {
  console.log('fence_return_routes: a pub/sub reply returns by the reverse of its request\'s hops');
  {
    const w = await build({ rr: false });
    const q = await pullup(w);
    island(w);
    const r = await replayUpTo(w, w.O);
    check('P0 control (off): the PULLUP crosses, and T\'s REPLAYUP to O is not delivered', q?.consumed === true && r?.consumed !== true, J({ q, r }));
    await stopAll(w);
  }
  {
    const w = await build({ rr: true });
    const q = await pullup(w);
    const bc = w.T.am._retRoutes.get(key(w.O))?.rpath || [];
    island(w);
    const r = await replayUpTo(w, w.O);
    const RS = w.T.am._returnRouteStats;
    check('P1 on, PULLUP: T\'s REPLAYUP reaches O by the return route over the five reverse hops', q?.consumed === true && bc.length === 5 && r?.consumed === true && r?.hops === 5 && RS.delivered === 1 && RS.fellBack === 0, J({ bc: bc.length, r, RS }));
    await stopAll(w);
  }
  {
    const w = await build({ rr: true });
    const H1 = w.nodes[1];
    await H1.am._route(w.T.big, 'pubsub:sub', { topicId: TOPIC, subscriberId: w.O.hex, since: 'all' });
    const bc = w.T.am._retRoutes.get(key(w.O))?.rpath || [];
    island(w);
    const r = await replayUpTo(w, w.O);
    check('P2 on, a SUB re-sent by H1 naming O: the breadcrumb for O runs H4..H1 then O', bc.length === 5 && bc[3]?.toLowerCase() === key(H1) && bc[4]?.toLowerCase() === key(w.O), J(bc.map((h) => h.slice(0, 8))));
    check('P2 … and T\'s reply reaches O through H1', r?.consumed === true && r?.hops === 5 && w.T.am._returnRouteStats.delivered >= 1 && w.T.am._returnRouteStats.fellBack === 0, J({ r, RS: w.T.am._returnRouteStats }));
    const relays = w.nodes.slice(2, -1).map((p) => p.am._retRoutes.size);
    check('P9 RR-2: the relays that only forwarded the SUB (H2..H4) keep no breadcrumb', relays.every((n) => n === 0), J(relays));
    await stopAll(w);
  }
  {
    const w = await build({ rr: true });
    await w.O.am._route(w.T.big, 'pubsub:mergeoffer', { topicId: TOPIC, corr: 'c0ffee'.repeat(4), loser: w.O.hex, loserEpoch: 1, hw: null, lw: null, count: 0, timing: null });
    const bc = w.T.am._retRoutes.get(key(w.O))?.rpath || [];
    island(w);
    const r = await w.T.am._route(w.O.big, 'pubsub:replayack', { topicId: TOPIC, corr: 'c0ffee'.repeat(4), winner: w.T.hex, winnerEpoch: 0, loser: w.O.hex, loserEpoch: 1, disposition: 'not-root' });
    check('P3 on, MERGE_OFFER: the loser\'s breadcrumb is recorded and T\'s REPLAYACK reaches it', bc.length === 5 && r?.consumed === true, J({ bc: bc.length, r }));
    await stopAll(w);
  }
  for (const domainRR of [true, false]) {
    const w = await build({ rr: true, n: 13, domainRR });
    const q = await pullup(w);
    const bc = w.T.am._retRoutes.get(key(w.O))?.rpath || [];
    island(w);
    const r = await replayUpTo(w, w.O);
    if (domainRR) check('P4 domain RETURN_ROUTE on: a 12-hop PULLUP keeps twelve hops and the reply returns over them', q?.consumed === true && bc.length === 12 && r?.consumed === true && r?.hops === 12, J({ q: q?.hops, bc: bc.length, r }));
    else check('P4 … domain RETURN_ROUTE off (control): the path is cut to PATH_MEMORY and the reply does not cross', bc.length < 12 && r?.consumed !== true, J({ bc: bc.length, r }));
    await stopAll(w);
  }
  {
    const w = await build({ rr: true });
    await pullup(w);
    island(w);
    const h2 = w.nodes[2], h1 = w.nodes[1];
    h2.transport.isConnected = ((orig) => (id) => (id === h1.big || String(id).toLowerCase() === key(h1)) ? false : orig(id))(h2.transport.isConnected.bind(h2.transport));
    let threw = false, r = null; try { r = await replayUpTo(w, w.O); } catch { threw = true; }
    const RS = w.T.am._returnRouteStats;
    check('P5 a gone reverse hop: no throw, the return route fell back once, the own lookup decided', !threw && RS.used === 1 && RS.fellBack === 1 && r?.consumed !== true, J({ threw, RS, r }));
    check('P10 RR-3: the breadcrumb that fell back is dropped', !w.T.am._retRoutes.has(key(w.O)), J([...w.T.am._retRoutes.keys()].map((k) => k.slice(0, 8))));
    await stopAll(w);
  }
  {
    const w = await build({ rr: true });
    await pullup(w);
    for (const v of w.T.am._retRoutes.values()) v.exp = 0;
    island(w);
    const r = await replayUpTo(w, w.O);
    check('P6 an expired breadcrumb is not used (behaves as the control)', w.T.am._returnRouteStats.used === 0 && r?.consumed !== true, J({ RS: w.T.am._returnRouteStats, r }));
    await stopAll(w);
  }
  {
    const w = await build({ rr: true });
    await pullup(w);
    const first = w.T.am._retRoutes.get(key(w.O));
    const rec1 = w.T.am._returnRouteStats.recorded;
    await w.nodes[2].am._route(w.T.big, 'pubsub:sub', { topicId: TOPIC, subscriberId: w.O.hex, since: 'all' });
    const after = w.T.am._retRoutes.get(key(w.O));
    check('P8 RR-1: a direct PULLUP records O once, and a SUB from H2 naming O leaves O\'s anchored breadcrumb as it was', rec1 === 1 && first?.anchored === true && after === first, J({ rec1, anchored: first?.anchored, same: after === first, rp: after?.rpath?.map((h) => h.slice(0, 6)) }));
    await w.O.am._route(w.T.big, 'pubsub:replayup', { topicId: TOPIC, msgs: [] });
    const fresh = w.T.am._retRoutes.get(key(w.O));
    check('P11 RR-2: a REPLAYUP from O refreshes T\'s anchored way back to O', fresh && fresh !== first && fresh.anchored === true && fresh.rpath.length === 5, J({ fresh: !!fresh, same: fresh === first }));
    await stopAll(w);
  }
  {
    const prev = process.env.RETURN_ROUTE; delete process.env.RETURN_ROUTE;
    const w = await build({ rr: null, n: 2 });
    check('P7 defaults OFF: domain RETURN_ROUTE false and a peer\'s manager returnRoute false', new AxonaDomain().RETURN_ROUTE === false && w.T.am._returnRoute === false && w.O.am._returnRoute === false);
    await stopAll(w);
    if (prev !== undefined) process.env.RETURN_ROUTE = prev;
  }
  console.log(`\nfence_return_routes: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
