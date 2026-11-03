#!/usr/bin/env node
// =====================================================================
// fence_cbr_3d_hint_refresh.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 3d, the hint refresh (David 2026-11-03 "go ahead with 3d's hint
// refresh"), manager option hintRefresh / env HINT_REFRESH, default off.
//
// _rootHint_ asked the network only when the local closest was null or self,
// so a subscriber whose neighbour looks closer kept that guess however long
// it stayed unattached. Armed, past ROOT_CLAIM_MS unattached it asks the
// network (the DHT walk where the adapter has one) and takes the closest
// visited node when that beats both the local guess and itself.
// Scene: real AxonaPeers, bound-only, chain S -> H1 -> H2 -> H3 -> W sorted by
// distance to a real topic; W holds the root; S is not pinned.
//   K1 on, unattached past the claim window: S's hint becomes W (hint-refresh row).
//   K2 off (control): the hint stays a nearer node (findKClosest's H2).
//   K3 on, unattached only just now: no refresh; the hint is not W.
// Run: node test/fence_cbr_3d_hint_refresh.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { realTopic }                from './lib/mergeFixture.mjs';
import { ROOT_CLAIM_MS }            from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
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
async function run(name, { hintRefresh, unattachedFor }) {
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
    am._hintRefresh = hintRefresh;
    const rec = { peer, transport, node, am, big: fromHex(id.id), hex: id.id, logs: [] };
    am._logSink = (l, ev, ctx) => { if (/hint-refresh/.test(ev)) rec.logs.push(ctx); };
    raw.push(rec);
  }
  const nodes = raw.sort((x, y) => ((x.big ^ Lt.topicId) > (y.big ^ Lt.topicId) ? -1 : 1));
  const S = nodes[0], W = nodes[nodes.length - 1];
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  await sleep(20);
  W.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  S.am._upstream.delete(Lt.topicId);
  S.am._rootHint.delete(Lt.topicId);
  S.am._unattachedSince.set(Lt.topicId, Date.now() - unattachedFor);
  S.am._rootHint_(Lt.topicId);
  for (let t = 0; t < 30 && !S.am._rootHint.get(Lt.topicId); t++) await sleep(100);
  await sleep(100);
  const via = S.am._rootHint.get(Lt.topicId)?.via ?? null;
  const out = { via: via ? via.slice(0, 10) : null, W: W.hex.slice(0, 10), refreshRows: S.logs.length };
  for (const p of raw) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } }
  return out;
}

console.log('fence_cbr_3d_hint_refresh: an unattached subscriber asks the network past the claim window');
const on = await run('cbr-hint-on', { hintRefresh: true, unattachedFor: ROOT_CLAIM_MS + 1000 });
check('K1 on, unattached past the claim window: the hint becomes W', on.via === on.W && on.refreshRows === 1, JSON.stringify(on));
const off = await run('cbr-hint-off', { hintRefresh: false, unattachedFor: ROOT_CLAIM_MS + 1000 });
check('K2 off (control): the hint stays a nearer node, not W', off.via !== off.W && off.refreshRows === 0, JSON.stringify(off));
const fresh = await run('cbr-hint-fresh', { hintRefresh: true, unattachedFor: 0 });
check('K3 on, only just unattached: no refresh, the hint is not W', fresh.via !== fresh.W && fresh.refreshRows === 0, JSON.stringify(fresh));
console.log(`\nfence_cbr_3d_hint_refresh: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
