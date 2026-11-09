#!/usr/bin/env node
// =====================================================================
// fence_cbr_verify_walk.mjs — Closer-Born Relay Inherits the Root v0.6, the
// farther-reaching verify (David 2026-11-03 "go ahead with the
// farther-reaching verify"), manager option verifyWalk / env VERIFY_WALK.
//
// The root self-verify on a standalone peer calls dht.lookup, which is
// findKClosest: it asks only peers this node holds a channel to, so one pass
// reaches two channel-hops (fence_findk_chain_reach). Armed, the verify also
// runs the DHT walk (dht.walk = peer.lookup, each hop forwarding over its own
// channels) and nominates the closest node either one visited.
// Scene: real AxonaPeers, bound-only, chain L -> H1 -> H2 -> H3 -> W sorted by
// distance to a real topic; L is root and holds three signed bodies; W holds
// nothing.
//   V1 on: the verify nominates W (merge-candidate names W, a verify-walk row).
//   V2 off (control): it nominates H2, the farthest node findKClosest sees.
//   V3 on, with cutover and returnRoute: W ends the one root holding L's
//      three bodies and L is demoted (3b staging + the return routes).
// Run: node test/fence_cbr_verify_walk.mjs
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
process.on('exit', (code) => { if (code === 0 && passed + failed < 3) { console.log(`  ✗ exited after ${passed + failed} of 3 checks`); process.exitCode = 1; } });

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
async function run(name, { verifyWalk, cutover = false, returnRoute = false }) {
  const Lt = await realTopic(name);
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK_DEADLINE_MS: 800, RETURN_ROUTE: returnRoute });
  // PRECONDITION (2026-11-09): the chain is sorted by distance to the TOPIC, but the
  // merge offer is routed toward W's NODE ID, hop by hop over bound-only channels
  // (no new connections). With random ids, a chain node's neighbour closest to W is
  // sometimes the node behind it; the offer then dead-ends and V3 waits 15 s for
  // merge-offer-timeout. Measured: 7 of 40 random draws were blocked and all 7 stuck;
  // all 33 greedy-routable draws merged in ~100 ms. Draw ids until every interior hop
  // makes greedy progress toward W, so V3 tests the merge and not the draw.
  let idents = null;
  for (let tries = 0; tries < 50 && !idents; tries++) {
    const cand = [];
    for (let i = 0; i < 5; i++) cand.push(await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 }));
    const order = cand.map((id) => ({ id, big: fromHex(id.id) })).sort((x, y) => ((x.big ^ Lt.topicId) > (y.big ^ Lt.topicId) ? -1 : 1));
    const Wb = order[4].big;
    if ([1, 2, 3].every((i) => (order[i + 1].big ^ Wb) < (order[i - 1].big ^ Wb))) idents = order.map((o) => o.id);
  }
  if (!idents) { console.log('  ✗ precondition: no greedy-routable 5-node chain in 50 draws'); process.exit(1); }
  const raw = [];
  for (let i = 0; i < 5; i++) {
    const id = idents[i];
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, mergeCapable: true, rootReplicas: 0 });
    await peer.start();
    const am = peer._requireAxonaManager('fence');
    am._verifyWalk = verifyWalk; am._cutover = cutover; am._returnRoute = returnRoute;
    const rec = { peer, transport, node, am, big: fromHex(id.id), hex: id.id, logs: [] };
    am._logSink = (l, ev, ctx) => { if (/merge-candidate|verify-walk|merge-stage-promoted/.test(ev)) rec.logs.push({ ev: ev.replace('pubsub:', ''), ctx }); };
    raw.push(rec);
  }
  const nodes = raw.sort((x, y) => ((x.big ^ Lt.topicId) > (y.big ^ Lt.topicId) ? -1 : 1));
  const [L, , H2, , W] = nodes;
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  await sleep(20);
  const lRole = L.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  const ids = [];
  for (let k = 1; k <= 3; k++) {
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: Date.now() + k });
    await L.am._onPub({ topicId: hex(Lt.topicId), json: JSON.stringify(env) }, { targetId: L.big, isTerminal: true, hopCount: 2, fromId: hex(Lt.publisher) });
    ids.push(env.msgId);
  }
  await L.am._ingestIdle?.();
  lRole.formedAt = Date.now() - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
  L.am._verifyRoots(Date.now());
  for (let t = 0; t < 60; t++) {
    await sleep(100);
    const w = W.am.axonRoles.get(Lt.topicId);
    if (!cutover && L.logs.some((l) => l.ev === 'merge-candidate')) break;
    if (cutover && w?.isRoot && ids.every((id) => w.cacheIds.has(id)) && !L.am.axonRoles.get(Lt.topicId)?.isRoot) break;
  }
  const cand = L.logs.find((l) => l.ev === 'merge-candidate')?.ctx?.id ?? null;
  const w = W.am.axonRoles.get(Lt.topicId);
  const out = { cand, W: W.hex.slice(0, 10), H2: H2.hex.slice(0, 10), walkRow: L.logs.some((l) => l.ev === 'verify-walk'),
    wRoot: !!w?.isRoot, wHas: ids.filter((id) => w?.cacheIds?.has(id)).length, lRoot: !!L.am.axonRoles.get(Lt.topicId)?.isRoot };
  for (const p of raw) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } }
  return out;
}

console.log('fence_cbr_verify_walk: the root self-verify reaches past two channel-hops');
const on = await run('cbr-vwalk-on', { verifyWalk: true });
check('V1 on: the verify nominates W, past two channel-hops', on.cand === on.W && on.walkRow, JSON.stringify(on));
const off = await run('cbr-vwalk-off', { verifyWalk: false });
check('V2 off (control): it nominates H2, the farthest node findKClosest sees', off.cand === off.H2 && !off.walkRow, JSON.stringify(off));
const e2e = await run('cbr-vwalk-e2e', { verifyWalk: true, cutover: true, returnRoute: true });
check('V3 on + cutover + returnRoute: W ends the one root with L\'s three bodies, L demoted', e2e.wRoot && e2e.wHas === 3 && !e2e.lRoot, JSON.stringify(e2e));
console.log(`\nfence_cbr_verify_walk: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
