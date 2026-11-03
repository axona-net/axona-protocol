#!/usr/bin/env node
// =====================================================================
// fence_cbr_merge_across_island.mjs — Closer-Born Relay Inherits the Root
// v0.6: a whole merge round crosses a ONE-WAY island, end to end. The shape
// of the live #council split: the farther root L can reach the closer root
// W, and W's own routing cannot reach L.
//
// Scene: real AxonaPeers on the sim transport, bound-only, chain
// L -> H1 -> H2 -> H3 -> W ordered by XOR distance to a real topic (W
// closest). Synapses point FORWARD only, and every node refuses to learn a
// backward one, for the whole run: channels stay open, greedy routing from W
// toward L has no entry. L and W are both root; L holds three signed bodies.
// L's verify names W (the walk's result is given: on this island the real
// walk stops short of W, a separate finding) and L offers; the MERGE_OFFER
// crosses forward. Root replication is off so W's bodies come only by merge. W's
// PULLUP and REPLAYACK go BACK to L and can cross only by a return route.
//   X1 (returnRoute + cutover on): the round completes. W is the one root and
//      holds L's three bodies; L is no longer root.
//   X2 (returnRoute off, cutover on, control): nothing crosses back. W
//      lacks the bodies, and L is still root (cutover: silence never demotes).
// Run: node test/fence_cbr_merge_across_island.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { realTopic, author }        from './lib/mergeFixture.mjs';
import { buildEnvelope }            from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS }     from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const hex = (b) => b.toString(16).padStart(66, '0');
process.on('exit', (code) => { if (code === 0 && passed + failed < 2) { console.log(`  ✗ exited after ${passed + failed} of 2 checks`); process.exitCode = 1; } });

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

async function run(name, rr) {
  const Lt = await realTopic(name);
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK_DEADLINE_MS: 800, RETURN_ROUTE: rr });
  const raw = [];
  for (let i = 0; i < 5; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, mergeCapable: true, rootReplicas: 0 });
    await peer.start();
    const am = peer._requireAxonaManager('fence');
    am._returnRoute = rr; am._cutover = true;
    const rec = { peer, transport, node, am, big: fromHex(id.id), hex: id.id, ev: [] };
    am._logSink = (l, ev) => { if (/merge-/.test(ev)) rec.ev.push(ev.replace('pubsub:', '')); };
    raw.push(rec);
  }
  // W is the node closest to the topic. The rest are ordered by distance to
  // W, farthest first, so every forward hop toward W is strictly closer to it.
  const W = raw.reduce((a, b) => ((a.big ^ Lt.topicId) < (b.big ^ Lt.topicId) ? a : b));
  const nodes = [...raw.filter((p) => p !== W).sort((x, y) => ((x.big ^ W.big) > (y.big ^ W.big) ? -1 : 1)), W];
  const L = nodes[0];
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  // THE ISLAND, held for the whole run: no node keeps a routing entry that
  // points back toward L's side.
  for (let i = 1; i < nodes.length; i++) {
    const behind = new Set(nodes.slice(0, i).map((p) => p.big));
    const syn = nodes[i].node.synaptome;
    for (const b of behind) syn.delete(b);
    const set = syn.set.bind(syn);
    syn.set = (k, v) => (behind.has(k) ? syn : set(k, v));
    const inc = nodes[i].node.incomingSynapses;
    if (inc && typeof inc.set === 'function') { for (const b of behind) inc.delete?.(b); const iset = inc.set.bind(inc); inc.set = (k, v) => (behind.has(k) ? inc : iset(k, v)); }
  }
  await sleep(20);
  const wRole = W.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  const lRole = L.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  const ids = [];
  for (let k = 1; k <= 3; k++) {
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: Date.now() + k });
    await L.am._onPub({ topicId: hex(Lt.topicId), json: JSON.stringify(env) }, { targetId: L.big, isTerminal: true, hopCount: 2, fromId: hex(Lt.publisher) });
    ids.push(env.msgId);
  }
  await L.am._ingestIdle?.();
  // L's verify walk names W. On this island the real walk stops short of W
  // (recorded separately); the subject here is the round, not the walk, so
  // the walk's result is given, as mergeFixture's splitScene gives it.
  L.am.dht.lookup = async () => ({ path: [L.hex, W.hex], hops: 1, time: 1, found: false });
  lRole.formedAt = Date.now() - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
  L.am._verifyRoots(Date.now());
  const done = () => W.am.axonRoles.get(Lt.topicId)?.isRoot && ids.every((id) => W.am.axonRoles.get(Lt.topicId)?.cacheIds?.has(id)) && !L.am.axonRoles.get(Lt.topicId)?.isRoot;
  for (let t = 0; t < 60 && !done(); t++) await sleep(100);
  const out = {
    lCache: lRole.cache.length, lRoot: !!L.am.axonRoles.get(Lt.topicId)?.isRoot,
    wRoot: !!W.am.axonRoles.get(Lt.topicId)?.isRoot, wHas: ids.filter((id) => W.am.axonRoles.get(Lt.topicId)?.cacheIds?.has(id)).length,
    RS: { W: { ...W.am._returnRouteStats }, L: { ...L.am._returnRouteStats } },
    offerReachedW: W.ev.includes('merge-pull-start'), lEv: L.ev, wEv: W.ev,
  };
  for (const p of raw) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } }
  return out;
}

console.log('fence_cbr_merge_across_island: a merge round crosses a one-way island end to end');
const on = await run('cbr-island-on', true);
check('X1 return routes on: W is the one root with L\'s three bodies, L demoted', on.wRoot && on.wHas === 3 && !on.lRoot && on.RS.W.delivered >= 1, JSON.stringify(on));
const off = await run('cbr-island-off', false);
check('X2 off (control): the offer reaches W, nothing crosses back — W lacks the bodies and L is still root', off.offerReachedW && off.wHas === 0 && off.lRoot === true, JSON.stringify(off));
console.log(`\nfence_cbr_merge_across_island: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
