#!/usr/bin/env node
// =====================================================================
// fence_cbr_hr_ri_findings.mjs — Orion 867c3048 on the 3d hint refresh
// (2252915) and 3c replicas inherit (21ef1ca).
//   HR-1  the local guess may itself be the bridge:
//     H1 S holds a channel to W, W is the bridge and closest: the hint is not W.
//   HR-2  the heal SUB after a refresh names the hint (a noCache walk warmed nothing):
//     H2 refreshed: the heal SUB carries via [hint]; H3 control, refresh off: via [].
//   RI-1  the verify's visited set is ranked before it is bounded:
//     R1 a close node visited after 20 farther hops is kept, first, once.
//   RI-2  a bridge never holds a backup, and a refusing candidate frees its slot:
//     R2 a neverRoot manager does not consume REPLICATE and takes no backup role.
//     R3 a seated candidate whose push failed is not re-picked for RENEW_MS, then is.
// Run: node test/fence_cbr_hr_ri_findings.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { realTopic }                from './lib/mergeFixture.mjs';
import { ROOT_CLAIM_MS, RENEW_MS }  from '../src/pubsub/constants.js';
import { AxonaManager }             from '../src/pubsub/AxonaManager.js';
import { sealTestDht }              from './lib/testCapability.mjs';

const EXPECT = 6;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

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
async function scene(name, { directSW = false } = {}) {
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
    const rec = { peer, transport, node, am, big: fromHex(id.id), hex: id.id, logs: [] };
    am._logSink = (l, ev, ctx) => { rec.logs.push({ ev, ctx }); };
    raw.push(rec);
  }
  const nodes = raw.sort((x, y) => ((x.big ^ Lt.topicId) > (y.big ^ Lt.topicId) ? -1 : 1));
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); }
  if (directSW) { await openRaw(nodes[0], nodes[4]); synapse(nodes[0], nodes[4]); }
  await sleep(20);
  const stop = async () => { for (const p of raw) { try { p.am.stop?.(); } catch { /* */ } try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } };
  return { Lt, nodes, S: nodes[0], H3: nodes[3], W: nodes[4], stop };
}
const s10 = (h) => (h ? String(h).toLowerCase().slice(0, 10) : null);
async function resolveHint(sc, { hintRefresh = true, subscribed = false } = {}) {
  const { Lt, S, W } = sc;
  S.am._hintRefresh = hintRefresh;
  W.am._rootClaim.become(Lt.topicId, 'sub-terminal');
  S.am._upstream.delete(Lt.topicId); S.am._rootHint.delete(Lt.topicId);
  S.am._unattachedSince.set(Lt.topicId, Date.now() - ROOT_CLAIM_MS - 1000);
  const emits = [];
  if (subscribed) { S.am.mySubscriptions.set(Lt.topicId, { since: 0, lastRenewSent: 0 }); S.am._emitSubscribe = (t, via) => { emits.push(via); }; }
  S.am._rootHint_(Lt.topicId);
  for (let t = 0; t < 30 && !S.am._rootHint.get(Lt.topicId); t++) await sleep(100);
  await sleep(100);
  return { via: s10(S.am._rootHint.get(Lt.topicId)?.via), emits };
}

console.log('fence_cbr_hr_ri_findings: Orion 867c3048 HR-1/2, RI-1/2');

{ const sc = await scene('cbr-hr1', { directSW: true });
  sc.S.transport.bridgeNodeIdBig = sc.W.big;
  const r = await resolveHint(sc);
  const row = sc.S.logs.find((l) => /hint-refresh/.test(l.ev));
  check('H1 HR-1 S holds a channel to the bridge W, closest: the hint is not W; the row says dropped bridge', r.via !== s10(sc.W.hex) && row?.ctx?.dropped === 'bridge', JSON.stringify({ via: r.via, W: s10(sc.W.hex), row: row?.ctx ?? null }));
  await sc.stop(); }
for (const on of [true, false]) {
  const sc = await scene(on ? 'cbr-hr2-on' : 'cbr-hr2-off');
  const r = await resolveHint(sc, { hintRefresh: on, subscribed: true });
  if (on) check('H2 HR-2 refreshed: the heal SUB names the hint (via [W])', r.emits.length >= 1 && r.emits.some((v) => Array.isArray(v) && v.length === 1 && s10(v[0]) === s10(sc.W.hex)), JSON.stringify({ emits: r.emits, W: s10(sc.W.hex) }));
  else check('H3 control, refresh off: the heal SUB stays greedy (via [])', r.emits.length >= 1 && r.emits.every((v) => Array.isArray(v) && v.length === 0), JSON.stringify(r.emits));
  await sc.stop();
}

// ── manager harness (RI-1, RI-2) ─────────────────────────────────────
const REG = 0x87n << 248n;
const hex = (b) => b.toString(16).padStart(66, '0');
const H = (x) => x << 224n;
{ // R1: path = self, 20 far nodes, then N (closer than them, farther than self), then a duplicate.
  const T = REG | 0x1000n, SELF = T ^ H(0x1n);
  const far = Array.from({ length: 20 }, (_, i) => hex(T ^ H(0x80n + BigInt(i))));
  const N = hex(T ^ H(0x20n));
  const clock = { t: 1_000_000 };
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [], bridgeId: () => null,
                lookup: async () => ({ path: [hex(SELF), ...far, N, far[0]] }) };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2, replicaInherit: true });
  am.nodeId = SELF; am.setLogSink(() => {});
  am.pubsubSubscribe(T); am._becomeRoot(T);
  const role = am.axonRoles.get(T); role.formedAt = 0; role.lastVerify = 0;
  am._verifyRoots(clock.t);
  for (let i = 0; i < 20 && am._verifyInflight?.has(T); i++) await sleep(10);
  const ids = role.verifySeen?.ids ?? [];
  check('R1 RI-1 a close node visited after 20 farther hops is kept, first, once; bound 16', ids[0] === N && ids.length === 16 && new Set(ids).size === ids.length, JSON.stringify({ first: ids[0]?.slice(0, 10), n: ids.length })); }
{ // R2: a bridge manager receives a REPLICATE.
  const T = REG | 0x2000n, SELF = T ^ H(0x3n), FROM = hex(T ^ H(0x1n));
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [], bridgeId: () => null };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => 1_000_000, neverRoot: true });
  am.nodeId = SELF; am.setLogSink(() => {});
  const r = await am._onReplicate({ topicId: hex(T), from: FROM, msgs: [], dels: [] }, { targetId: SELF, fromId: FROM });
  const role = am.axonRoles.get(T);
  check('R2 RI-2 a neverRoot manager does not consume REPLICATE and takes no backup role', r !== 'consumed' && !(role && role.backupOf), JSON.stringify({ r, backupOf: role?.backupOf ?? null })); }
{ // R3: seated N closer than D2 refuses every push; D1/D2 accept.
  const T = REG | 0x3000n, SELF = T ^ H(0x1n);
  const D1 = hex(T ^ H(0x10n)), N = hex(T ^ H(0x20n)), D2 = hex(T ^ H(0x40n));
  const clock = { t: 1_000_000 };
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [D1, D2], bridgeId: () => null };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2, replicaInherit: true });
  am.nodeId = SELF; am.setLogSink(() => {});
  const pushes = [];
  am._syncPush = (target) => { const h = hex(target); pushes.push({ at: clock.t, h }); return { consumed: h !== N }; };
  am.pubsubSubscribe(T); am._becomeRoot(T);
  const role = am.axonRoles.get(T);
  role.cache.push({ msgId: 'm1', publishTs: clock.t, json: '{}' });
  role.subscribers.set(N, { since: clock.t, lastRenewed: clock.t });
  // Verdicts land asynchronously (a tick or two after the push), so the
  // window is measured from the moment the refusal is RECORDED.
  const t0 = clock.t; let refusedAt = null, recordedAt = null; let d2During = false;
  for (let i = 0; i < 24; i++) {
    clock.t += 5_000; role.subscribers.get(N).lastRenewed = clock.t; await am.refreshTick();
    if (refusedAt == null && role.inheritRefused?.has(N)) { refusedAt = role.inheritRefused.get(N); recordedAt = clock.t; }
    if (refusedAt != null && clock.t < refusedAt + RENEW_MS && role.replicas.has(D2)) d2During = true;
  }
  const toN = pushes.filter((p) => p.h === N).map((p) => p.at);
  const inWindow = refusedAt == null ? [] : toN.filter((a) => a > recordedAt && a < refusedAt + RENEW_MS);
  const after = refusedAt == null ? [] : toN.filter((a) => a >= refusedAt + RENEW_MS);
  check('R3 RI-2 a refusing seated candidate is not re-picked for RENEW_MS after its refusal is recorded, then is; D2 holds the slot meanwhile',
    refusedAt != null && inWindow.length === 0 && after.length >= 1 && d2During, JSON.stringify({ refusedAt: refusedAt == null ? null : refusedAt - t0, recordedAt: recordedAt == null ? null : recordedAt - t0, toN: toN.map((a) => a - t0), d2During })); }

console.log(`\nfence_cbr_hr_ri_findings: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
