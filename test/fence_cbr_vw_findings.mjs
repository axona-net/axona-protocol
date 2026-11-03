#!/usr/bin/env node
// =====================================================================
// fence_cbr_vw_findings.mjs — Orion 7908492c on c8ebe2e (verifyWalk), which
// also bear on the 3d hint refresh (2252915). Real AxonaPeers, bound-only,
// chain S -> H1 -> H2 -> H3 -> W sorted by distance to a real topic.
//   VW-1  the adapter's bridgeNodeId reads the transport's bridge id, and
//         the closer-born paths never nominate it:
//     A1 hint refresh with W as the bridge: the hint is H3, not W.
//     A2 verify under cutover with W as the bridge: the nominee is H3, not W.
//   VW-2  a walk and a verify do not outlive the node:
//     B1 a walk whose lookup_done is lost settles at peer.stop() with reason
//        'stopped', well inside its deadline.
//     B2 a verify whose inflight entry was cleared (as stop/leave do) nominates
//        nobody; B3 the control with the entry kept nominates a closer node.
//   VW-3  a walk toward a topic id never dials the topic id:
//     C1 dht.walk (noCache): no node calls openConnection(topic).
//     C2 control: a plain peer.lookup(topic) does, at least once.
// Run: node test/fence_cbr_vw_findings.mjs
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

const EXPECT = 7;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

function boundOnly(t, dials) {
  t.openConnection = async (id) => { dials.push(id); return t.isConnected(id); };
  const send = t.send.bind(t);
  t.send = async (id, type, body) => { if (!t.isConnected(id)) throw new Error(`Transport.send: peer ${String(id)} not connected`); return send(id, type, body); };
  return t;
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
async function scene(name, { deadlineMs = 800 } = {}) {
  const Lt = await realTopic(name);
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK_DEADLINE_MS: deadlineMs });
  const raw = [], dials = [];
  for (let i = 0; i < 5; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }), dials);
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    const am = peer._requireAxonaManager('fence');
    const rec = { peer, transport, node, am, big: fromHex(id.id), hex: id.id, logs: [] };
    am._logSink = (l, ev, ctx) => { rec.logs.push({ ev, ctx }); };
    raw.push(rec);
  }
  const nodes = raw.sort((x, y) => ((x.big ^ Lt.topicId) > (y.big ^ Lt.topicId) ? -1 : 1));
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  await sleep(20);
  dials.length = 0;
  const stop = async () => { for (const p of raw) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } };
  return { Lt, nodes, S: nodes[0], H3: nodes[3], W: nodes[4], dials, stop };
}
const rows = (r, ev) => r.logs.filter((l) => l.ev === ev || String(l.ev).endsWith(':' + ev)).map((l) => l.ctx);
const s10 = (h) => (h ? String(h).toLowerCase().slice(0, 10) : null);
const isTopic = (Lt) => (id) => { try { return (typeof id === 'bigint' ? id : fromHex(String(id))) === Lt.topicId; } catch { return false; } };

console.log('fence_cbr_vw_findings: Orion 7908492c VW-1..3 on the closer-born walk paths');

// A1 — VW-1 on the hint refresh.
{
  const sc = await scene('cbr-vw-a1');
  const { Lt, S, H3, W } = sc;
  S.transport.bridgeNodeIdBig = W.big;
  S.am._hintRefresh = true;
  W.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  S.am._upstream.delete(Lt.topicId); S.am._rootHint.delete(Lt.topicId);
  S.am._unattachedSince.set(Lt.topicId, Date.now() - ROOT_CLAIM_MS - 1000);
  const adapterBridge = S.am.dht.bridgeNodeId?.();
  S.am._rootHint_(Lt.topicId);
  for (let t = 0; t < 30 && !S.am._rootHint.get(Lt.topicId); t++) await sleep(100);
  await sleep(100);
  const via = s10(S.am._rootHint.get(Lt.topicId)?.via);
  const ok = s10(typeof adapterBridge === 'bigint' ? adapterBridge.toString(16).padStart(66, '0') : adapterBridge) === s10(W.hex) && via === s10(H3.hex);
  check('A1 VW-1 hint refresh, W is the bridge: adapter reports W, the hint is H3', ok, JSON.stringify({ via, H3: s10(H3.hex), W: s10(W.hex), adapterBridge: String(adapterBridge).slice(0, 12) }));
  await sc.stop();
}
// A2 — VW-1 on the verify under cutover. S holds the root; W is the bridge.
{
  const sc = await scene('cbr-vw-a2');
  const { Lt, S, H3, W } = sc;
  S.transport.bridgeNodeIdBig = W.big;
  S.am._verifyWalk = true; S.am._cutover = true;
  S.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  const role = S.am.axonRoles.get(Lt.topicId); role.formedAt = 0; role.lastVerify = 0;
  S.am._verifyRoots(Date.now());
  for (let t = 0; t < 30 && S.am._verifyInflight.has(Lt.topicId); t++) await sleep(100);
  await sleep(100);
  const to = rows(S, 'root-verify-demote').map((c) => s10(c.to));
  check('A2 VW-1 verify under cutover, W is the bridge: the nominee is H3, never W', to.length >= 1 && to.every((h) => h === s10(H3.hex)), JSON.stringify({ to, H3: s10(H3.hex), W: s10(W.hex) }));
  await sc.stop();
}
// B1 — VW-2: a walk whose lookup_done is lost settles at stop.
{
  const sc = await scene('cbr-vw-b1', { deadlineMs: 6000 });
  const { Lt, nodes, S } = sc;
  const H1 = nodes[1];
  const notify = H1.transport.notify.bind(H1.transport);
  H1.transport.notify = async (id, type, body) => (type === 'lookup_done' ? undefined : notify(id, type, body));
  const t0 = Date.now();
  const p = S.peer.lookup(Lt.topicId, { noCache: true });
  await sleep(300);
  await S.peer.stop();
  const r = await Promise.race([p, sleep(3000).then(() => 'hung')]);
  const ms = Date.now() - t0;
  check('B1 VW-2 a walk with a lost lookup_done settles at peer.stop(): reason stopped, < 2 s', r && r !== 'hung' && r.reason === 'stopped' && ms < 2000, JSON.stringify({ reason: r?.reason ?? r, ms }));
  await sc.stop();
}
// B2/B3 — VW-2: a verify whose inflight entry was cleared does nothing.
for (const cleared of [true, false]) {
  const sc = await scene(cleared ? 'cbr-vw-b2' : 'cbr-vw-b3');
  const { Lt, S } = sc;
  S.am._verifyWalk = true;
  S.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  const role = S.am.axonRoles.get(Lt.topicId); role.formedAt = 0; role.lastVerify = 0;
  S.am._verifyRoots(Date.now());
  if (cleared) S.am._verifyInflight.clear();
  await sleep(1500);
  const n = rows(S, 'root-verify-demote').length;
  if (cleared) check('B2 VW-2 verify settles after its inflight entry was cleared: no nominee', n === 0, JSON.stringify({ demotes: n }));
  else check('B3 control, entry kept: the verify nominates a closer node', n >= 1, JSON.stringify({ demotes: n }));
  await sc.stop();
}
// C1/C2 — VW-3: a topic walk never dials the topic id.
for (const viaAdapter of [true, false]) {
  const sc = await scene(viaAdapter ? 'cbr-vw-c1' : 'cbr-vw-c2');
  const { Lt, S, dials } = sc;
  if (viaAdapter) await S.am.dht.walk(Lt.topicId); else await S.peer.lookup(Lt.topicId);
  await sleep(100);
  const n = dials.filter(isTopic(Lt)).length;
  if (viaAdapter) check('C1 VW-3 dht.walk toward a topic: no node dials the topic id', n === 0, JSON.stringify({ topicDials: n, dials: dials.length }));
  else check('C2 control, plain peer.lookup(topic): the topic id is dialled', n >= 1, JSON.stringify({ topicDials: n, dials: dials.length }));
  await sc.stop();
}
console.log(`\nfence_cbr_vw_findings: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
