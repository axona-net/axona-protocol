#!/usr/bin/env node
// =====================================================================
// characterize_findk_chain_reach.mjs — why the verify "walk" stops two hops
// in (axona-bot 5c51da7a, re Orion 0cba73ba).
//
// On standalone peers the dht adapter's lookup() is findKClosest(target, 4)
// (AxonaPeer.js _buildDefaultAxonaManager), not the lookup_walk: an iterative
// K-closest search that PROBES a candidate with find_closest_set over
// transport.send. A candidate the asker has no open channel to cannot be
// asked. On a chain L -> H1 -> H2 -> H3 -> W where L holds a channel only to
// H1, L asks H1, learns H2's id from the answer, cannot ask H2, and the result
// is { L, H1, H2 }: found true, hops 2, W never named. That is the stop seen
// in fence_cbr_merge_across_island, with or without the one-way island.
//   C1 the chain sorted by distance to the topic, bound-only transport: the
//      lookup returns exactly L, H1, H2 (terminus H2) and never names W.
//   C2 give L a channel to H2 as well: the lookup now names H3; reach grows
//      by one per channel, not by the chain's routing.
// This pins today's behaviour; it changes nothing.
// Run: node test/characterize_findk_chain_reach.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { realTopic }                from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const short = (b) => (typeof b === 'bigint' ? b.toString(16).padStart(66, '0') : String(b)).slice(0, 8);

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
async function build(name) {
  const Lt = await realTopic(name);
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK_DEADLINE_MS: 800 });
  const raw = [];
  for (let i = 0; i < 5; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    const am = peer._requireAxonaManager('fence');
    raw.push({ peer, transport, node, am, big: fromHex(id.id), hex: id.id });
  }
  const nodes = raw.sort((x, y) => ((x.big ^ Lt.topicId) > (y.big ^ Lt.topicId) ? -1 : 1));
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  await sleep(20);
  return { Lt, nodes, raw, openRaw };
}
const stop = async (w) => { for (const p of w.raw) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } };
const ids = (r) => (r?.path || []).map((x) => (typeof x === 'bigint' ? x : BigInt('0x' + x)));

console.log('characterize_findk_chain_reach: the standalone lookup reaches only what the asker can ask');
{
  const w = await build('findk-chain-1');
  const [L, H1, H2, , W] = w.nodes;
  const r = await L.am.dht.lookup(w.Lt.topicId);
  const p = ids(r);
  check('C1 bound-only chain: the lookup returns exactly L, H1, H2 (terminus H2) and never names W',
    r?.found === true && p.length === 3 && p[2] === H2.big && p.includes(H1.big) && p.includes(L.big) && !p.includes(W.big),
    JSON.stringify({ path: p.map(short), chain: w.nodes.map((n) => short(n.big)) }));
  await stop(w);
}
{
  const w = await build('findk-chain-2');
  const [L, , H2, H3] = w.nodes;
  await w.openRaw(L, H2); await sleep(20);
  const r = await L.am.dht.lookup(w.Lt.topicId);
  const p = ids(r);
  check('C2 with a channel L to H2 as well, the lookup names H3 — reach grows per open channel',
    p.includes(H3.big), JSON.stringify({ path: p.map(short), chain: w.nodes.map((n) => short(n.big)) }));
  await stop(w);
}
console.log(`\ncharacterize_findk_chain_reach: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
