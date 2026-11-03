#!/usr/bin/env node
// =====================================================================
// fence_signal_return_route.mjs — Closer-Born Relay Inherits the Root v0.4,
// build order step 1: the reply route of axona-bot finding 80fe76eb
// (David 2026-11-03 "go ahead with the reply route").
//
// The island case, from the 2026-11-02 cross-island read: offers crossed
// (delivered by the bridge node as last hop) while the answerer's own lookup
// toward the offerer failed (reach lookup-no, not-sent), so no answer left.
//
// Scene: real AxonaPeers over the sim transport, bound-only. Chain
// O -> H1 -> H2 -> H3 -> H4 -> T with channels open both ways but synapses
// FORWARD ONLY; once the offer has crossed, every backward entry the offer
// taught is removed (the island holds no entry pointing back) while channels
// stay open, so T's own lookup of O fails, as on the Mac island.
//   R0 control (SIGNAL_RETURN_ROUTE off): T's answer is not sent; O hears
//      nothing; the origin row reads reach lookup-no.
//   R1 on: T's answer returns along the reverse of the offer's hops and O
//      receives it; the origin row reads reach return-route, delivered, 5 hops.
//   R2 a reverse hop whose channel is gone: the reply falls back (fellBack 1),
//      nothing throws, O hears nothing, the own lookup still ran.
//   R3 an expired breadcrumb is not used: behaves as the control.
//   R4 the default is OFF.
// Orion 34db5e6d review fixes (SRR-1..3), each with its own scene:
//   R5 SRR-3: a forged offer naming O as sender but originated elsewhere,
//      and O's own later ice-candidate, leave T's breadcrumb for O unchanged.
//   R6 SRR-2: a 12-hop offer (past PATH_MEMORY 8) still returns by the
//      reverse of all twelve hops; the breadcrumb ends at O.
//   R7 SRR-1: with LOOKUP_RESELECT on, a reverse hop whose send throws an
//      edge loss is reselected, and the frame to the reselected hop carries
//      no rpath.
// Mutants (driven 2026-11-03, all red at R1): drop `path` from the
// terminal's _deliverRouted meta; drop the hop's rpath honour; remove the
// reverse(); the originator ignores opts.rpath. SRR mutants: drop the
// originId match (R5 red); record on any kind (R5 red); signal frames keep
// PATH_MEMORY only (R6 red); leave rpathRest after the send-threw reselect
// (R7 red).
// Run: node test/fence_signal_return_route.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const J = (v) => JSON.stringify(v, (k, x) => typeof x === 'bigint' ? x.toString(16).slice(0, 12) : x);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
process.on('exit', (code) => { if (code === 0 && passed + failed < 12) { console.log(`  ✗ exited after ${passed + failed} of 12 checks`); process.exitCode = 1; } });

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
async function build({ on, n = 6, dom = {} }) {
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK_DEADLINE_MS: 800, SIGNAL_RETURN_ROUTE: on, ...dom });
  const raw = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    const rec = { peer, transport, node, big: fromHex(id.id), hex: id.id, heard: [], logs: [] };
    transport.deliverMeshSignal = async (from, sig) => { rec.heard.push({ from, kind: sig?.kind }); };
    peer._emitLog = (lvl, ev, ctx) => rec.logs.push({ ev, ctx });
    raw.push(rec);
  }
  const T = raw[0];
  const chain = raw.slice(1).sort((x, y) => ((x.big ^ T.big) > (y.big ^ T.big) ? -1 : 1));
  const nodes = [...chain, T];
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }   // FORWARD ONLY
  await sleep(20);
  return { O: nodes[0], T, nodes, all: raw };
}
// THE ISLAND, applied after the offer has crossed: on 2026-11-02 the
// answerer's side held no routing entry pointing back toward the offerer.
// The offer's passage teaches every hop a BACKWARD synapse toward its
// predecessor (measured: each node's synaptome gains i-1); the island is the
// absence of those entries. They are removed, with the incoming synapses the
// two-hop check would otherwise walk; forward synapses and every open channel
// stay, so a reverse hop is still REACHABLE, just not ROUTABLE by greedy.
function island(w) {
  for (let i = 1; i < w.nodes.length; i++) w.nodes[i].node.synaptome.delete(w.nodes[i - 1].big);
  for (const p of w.all) p.node.incomingSynapses?.clear?.();
}
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
const offer  = (w) => w.O.peer.routeMessage(w.T.big, 'mesh:signal', { from: w.O.hex, signal: { kind: 'sdp-offer', attempt: 'f-1' } });
const answer = (w) => (island(w), w.T.peer._relayMeshSignal(w.O.big, w.O.hex, { kind: 'sdp-answer', attempt: 'f-1' }));
const originRow = (w) => w.T.logs.filter((l) => l.ev === 'relay-signal-origin').pop()?.ctx;

(async () => {
  console.log('fence_signal_return_route: the answer returns along the reverse of the offer\'s hops');
  {
    const w = await build({ on: false });
    const r = await offer(w);
    check('R0 scene: the offer walks O to T and is consumed at T', r?.consumed === true && w.T.heard.some((h) => h.kind === 'sdp-offer'), J(r));
    await answer(w); await sleep(50);
    const o = originRow(w);
    check('R0 control (off): T\'s answer is not sent and O hears nothing (reach lookup-no)', !w.O.heard.some((h) => h.kind === 'sdp-answer') && o?.reach === 'lookup-no', J(o));
    await stopAll(w);
  }
  {
    const w = await build({ on: true });
    await offer(w);
    await answer(w); await sleep(50);
    const o = originRow(w);
    check('R1 on: O receives T\'s answer', w.O.heard.some((h) => h.kind === 'sdp-answer'), J(w.O.heard));
    check('R1 … by the return route, delivered, over the five reverse hops', o?.reach === 'return-route' && o?.result === 'delivered' && o?.hops === 5, J(o));
    await stopAll(w);
  }
  {
    const w = await build({ on: true });
    await offer(w);
    const h2 = w.nodes[2], h1 = w.nodes[1];
    h2.transport.isConnected = ((orig) => (id) => (id === h1.big || String(id) === h1.hex) ? false : orig(id))(h2.transport.isConnected.bind(h2.transport));
    let threw = false; try { await answer(w); } catch { threw = true; }
    await sleep(50);
    const RS = w.T.peer._returnRouteStats || {};
    check('R2 a gone reverse hop: no throw, the return route fell back once, O hears nothing', !threw && RS.fellBack === 1 && !w.O.heard.some((h) => h.kind === 'sdp-answer'), J({ threw, RS }));
    check('R2 … and the own lookup still decided (origin row reach lookup-no)', originRow(w)?.reach === 'lookup-no', J(originRow(w)));
    await stopAll(w);
  }
  {
    const w = await build({ on: true });
    await offer(w);
    for (const v of w.T.peer._sigReturn.values()) v.exp = Date.now() - 1;
    await answer(w); await sleep(50);
    check('R3 an expired breadcrumb is not used (behaves as the control)', !w.O.heard.some((h) => h.kind === 'sdp-answer') && originRow(w)?.reach === 'lookup-no', J(originRow(w)));
    await stopAll(w);
  }
  check('R4 SIGNAL_RETURN_ROUTE defaults OFF', new AxonaDomain().SIGNAL_RETURN_ROUTE === false);
  {
    const w = await build({ on: true });
    await offer(w);
    const key = w.O.hex.toLowerCase();
    const before = w.T.peer._sigReturn.get(key);
    const H2 = w.nodes[2];
    const forged = await H2.peer.routeMessage(w.T.big, 'mesh:signal', { from: w.O.hex, signal: { kind: 'sdp-offer', attempt: 'forged' } });
    check('R5 SRR-3: an offer naming O but originated by H2 leaves O\'s breadcrumb as it was', forged?.consumed === true && w.T.peer._sigReturn.get(key) === before, J({ forged, rp: w.T.peer._sigReturn.get(key)?.rpath?.[0] }));
    await w.O.peer.routeMessage(w.T.big, 'mesh:signal', { from: w.O.hex, signal: { kind: 'ice-candidate', attempt: 'f-1' } });
    check('R5 … and O\'s own later ice-candidate does not overwrite it', w.T.peer._sigReturn.get(key) === before, J(w.T.peer._sigReturn.get(key)));
    await stopAll(w);
  }
  {
    const w = await build({ on: true, n: 13 });
    const r = await offer(w);
    await answer(w); await sleep(80);
    const o = originRow(w);
    const bc = w.T.peer._sigReturn.get(w.O.hex.toLowerCase())?.rpath || [];
    check('R6 SRR-2: a 12-hop offer returns by all twelve reverse hops and the breadcrumb ends at O', r?.consumed === true && w.O.heard.some((h) => h.kind === 'sdp-answer') && o?.reach === 'return-route' && o?.hops === 12 && bc.length === 12 && bc[11]?.toLowerCase() === w.O.hex.toLowerCase(), J({ hops: r?.hops, o, bc: bc.length }));
    await stopAll(w);
  }
  {
    const w = await build({ on: true, dom: { LOOKUP_RESELECT: true } });
    await offer(w);
    island(w);
    const H2 = w.nodes[2], H1 = w.nodes[1], O = w.O;
    await Object.getPrototypeOf(H2.transport).openConnection.call(H2.transport, O.hex);
    synapse(H2, O);
    await sleep(20);
    const sent = [];
    let thrown = 0;
    const send0 = H2.transport.send.bind(H2.transport);
    H2.transport.send = async (id, type, body) => {
      const toH1 = id === H1.big || String(id).toLowerCase() === H1.hex.toLowerCase();
      if (type === 'route_msg' && toH1 && thrown === 0) { thrown++; throw new Error('Transport.send: peer not connected'); }
      if (type === 'route_msg') sent.push({ to: typeof id === 'bigint' ? id : fromHex(String(id)), body });
      return send0(id, type, body);
    };
    await w.T.peer._relayMeshSignal(O.big, O.hex, { kind: 'sdp-answer', attempt: 'f-1' });
    await sleep(50);
    const toO = sent.find((x) => x.to === O.big);
    check('R7 SRR-1: after a send-threw reselect at H2 the frame to the reselected hop carries no rpath, and O hears the answer', thrown === 1 && toO && !('rpath' in toO.body) && O.heard.some((h) => h.kind === 'sdp-answer'), J({ thrown, toO: toO ? Object.keys(toO.body) : null, rpath: toO?.body?.rpath }));
    await stopAll(w);
  }
  console.log(`\nfence_signal_return_route: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
