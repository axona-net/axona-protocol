#!/usr/bin/env node
// fence_flight_address_guard.mjs — 4.152.0, manager option flightAddressGuard /
// env FLIGHT_ADDRESS_GUARD, default OFF.
//
// routeMessage runs a routed frame's handler at the SENDER'S OWN hop 0 before
// anything is sent. INGESTACK, RECEIPTPROBE and RECEIPTNACK answered 'consumed'
// there, so on a real mesh none of the three ever left its sender:
//   · a probe was answered by the node that asked;
//   · a root's ack was consumed by the root;
//   · a flight never completed, and its root was evicted for silence.
// The manager-seam tests (smoke_ack_routing and friends) use a dht double with
// no hop 0, so they could not see it. This scene uses REAL AxonaPeers on the
// in-memory network: an owner O and a root R, one channel between them.
//
// Run: node test/fence_flight_address_guard.mjs
import assert from 'node:assert/strict';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { realTopic }                from './lib/mergeFixture.mjs';
import { AxonaManager }             from '../src/pubsub/AxonaManager.js';
import { T }                        from '../src/pubsub/constants.js';
import { idHex }                    from '../src/pubsub/ids.js';

let n = 0;
const check = (name, ok, extra = '') => { assert(ok, `${name} ${extra}`); n++; console.log('PASS ' + name); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const MSG = 'a'.repeat(64);

async function scene(guard, { rootHolds = true } = {}) {
  const Lt = await realTopic('flight-guard-' + (guard ? 'on' : 'off') + (rootHolds ? '' : '-nh'));
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0 });
  const mk = async (i) => {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 });
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    const am = peer._requireAxonaManager('fence');
    am._flightAddressGuard = guard;
    const rec = { peer, transport, node, am, big: fromHex(id.id), hex: id.id, logs: [] };
    am._logSink = (l, ev, ctx) => rec.logs.push({ ev, ctx });
    return rec;
  };
  const O = await mk(0), R = await mk(1);
  await O.transport.openConnection(R.hex);
  for (const [a, b] of [[O, R], [R, O]]) {
    const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
    syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
    a.node.synaptome.set(b.big, syn);
  }
  await sleep(20);
  const role = R.am._rootClaim.become(Lt.topicId, 'sub-terminal') || R.am.axonRoles.get(Lt.topicId);
  if (rootHolds) role.cacheIds.add(MSG);
  const topicHex = idHex(Lt.topicId);
  // O holds an open write flight toward R for MSG, as _forwardToRoot would open.
  const tuple = O.am._flightOpen(Lt.topicId, R.hex, T.PUB, { topicId: topicHex, json: JSON.stringify({ msgId: MSG }) });
  const flights = () => (O.am._writeFlights ? O.am._writeFlights.size : 0);
  const ev = (rec, name) => rec.logs.filter((l) => l.ev === name || l.ev === 'pubsub:' + name).length;
  return { O, R, role, topicHex, tuple, flights, ev, Lt, stop: async () => { try { await O.peer.stop?.(); await R.peer.stop?.(); } catch { /* */ } } };
}

check('off: no frame is ever "not mine"', AxonaManager.prototype._notAddressedToMe.call({ _flightAddressGuard: false, nodeId: 1n }, { targetId: 2n }) === false);
check('on: a frame for another node is not mine; a frame for me is; no targetId is treated as mine',
  AxonaManager.prototype._notAddressedToMe.call({ _flightAddressGuard: true, nodeId: 1n }, { targetId: 2n }) === true
  && AxonaManager.prototype._notAddressedToMe.call({ _flightAddressGuard: true, nodeId: 1n }, { targetId: 1n }) === false
  && AxonaManager.prototype._notAddressedToMe.call({ _flightAddressGuard: true, nodeId: 1n }, {}) === false
  && AxonaManager.prototype._notAddressedToMe.call({ _flightAddressGuard: true, nodeId: 1n }, undefined) === false);

// ── RECEIPTPROBE ─────────────────────────────────────────────────────────────
{
  const s = await scene(false);
  const r = await s.O.am._route(s.R.big, T.RECEIPTPROBE, { topicId: s.topicHex, msgId: MSG, op: 'pub' });
  await sleep(60);
  check('off (the defect): a probe addressed to the root is consumed at its sender, hop 0', r && r.consumed === true && r.hops === 0 && r.atNode === s.O.big, JSON.stringify(r, (k, v) => typeof v === 'bigint' ? v.toString(16).slice(0, 8) : v));
  check('off (the defect): the flight stays open; the root was never asked', s.flights() === 1);
  await s.stop();
}
{
  const s = await scene(true);
  const r = await s.O.am._route(s.R.big, T.RECEIPTPROBE, { topicId: s.topicHex, msgId: MSG, op: 'pub' });
  await sleep(80);
  check('on: the probe is consumed at the root, one hop away', r && r.consumed === true && r.hops === 1 && r.atNode === s.R.big, JSON.stringify(r, (k, v) => typeof v === 'bigint' ? v.toString(16).slice(0, 8) : v));
  check('on: the root holds the message, its ack returns, and the flight completes', s.flights() === 0);
  check('on: no ack was mis-attributed', s.ev(s.O, 'write-flight-ack-unbound') === 0);
  await s.stop();
}
// ── a backup that holds the message asks the root (the 16:49Z rows) ──────────
{
  const s = await scene(false);
  // O also holds the message, as a backup does.
  const own = s.O.am._rootClaim.become(s.Lt.topicId, 'sub-terminal') || s.O.am.axonRoles.get(s.Lt.topicId);
  own.cacheIds.add(MSG);
  await s.O.am._route(s.R.big, T.RECEIPTPROBE, { topicId: s.topicHex, msgId: MSG, op: 'pub' });
  await sleep(60);
  const row = s.O.logs.find((l) => /write-flight-ack-unbound$/.test(l.ev));
  check('off (the defect): a holder that probes the root acknowledges ITSELF: ack-unbound from its own id', !!row && row.ctx.from === s.O.hex.slice(0, 12) && row.ctx.want === s.R.hex.slice(0, 12), JSON.stringify(row));
  check('off (the defect): and its flight stays open, on the way to evicting a live root', s.flights() === 1);
  await s.stop();
}
{
  const s = await scene(true);
  const own = s.O.am._rootClaim.become(s.Lt.topicId, 'sub-terminal') || s.O.am.axonRoles.get(s.Lt.topicId);
  own.cacheIds.add(MSG);
  await s.O.am._route(s.R.big, T.RECEIPTPROBE, { topicId: s.topicHex, msgId: MSG, op: 'pub' });
  await sleep(80);
  check('on: the same holder is answered by the root and its flight completes', s.flights() === 0 && s.ev(s.O, 'write-flight-ack-unbound') === 0);
  await s.stop();
}
// ── INGESTACK ────────────────────────────────────────────────────────────────
{
  const s = await scene(false);
  s.R.am._sendUnsignedIngestAck(s.O.hex, s.role, MSG, 'pub');
  await sleep(60);
  check('off (the defect): the root\'s ack is consumed at the root; the owner\'s flight stays open', s.flights() === 1);
  await s.stop();
}
{
  const s = await scene(true);
  s.R.am._sendUnsignedIngestAck(s.O.hex, s.role, MSG, 'pub');
  await sleep(80);
  check('on: the root\'s unsigned ack reaches the owner and completes the flight', s.flights() === 0);
  await s.stop();
}
{
  const s = await scene(true);
  await s.R.am._sendSignedIngestAck(s.role, MSG, 'pub', s.tuple);
  await sleep(120);
  check('on: the root\'s SIGNED ack proof reaches the owner and completes the flight', s.flights() === 0, JSON.stringify(s.O.logs.slice(-3)));
  await s.stop();
}
{
  const s = await scene(false);
  await s.R.am._sendSignedIngestAck(s.role, MSG, 'pub', s.tuple);
  await sleep(120);
  check('off (the defect): the signed proof never leaves the root either', s.flights() === 1);
  await s.stop();
}
// ── RECEIPTNACK ──────────────────────────────────────────────────────────────
{
  const s = await scene(true, { rootHolds: false });
  for (const f of s.O.am._writeFlights.values()) f.stage = 'probing';
  await s.O.am._route(s.R.big, T.RECEIPTPROBE, { topicId: s.topicHex, msgId: MSG, op: 'pub' });
  await sleep(80);
  check('on: a root that does not hold the message says so, and the owner retries once', s.ev(s.O, 'receipt-nack-retry') === 1 && [...s.O.am._writeFlights.values()][0].stage === 'retrying');
  check('on: the root itself did not act on its own nack', s.ev(s.R, 'receipt-nack-retry') === 0);
  await s.stop();
}
// ── an intermediate node does not answer for the root ────────────────────────
{
  const me = { _flightAddressGuard: true, nodeId: 5n, axonRoles: new Map(), _notAddressedToMe: AxonaManager.prototype._notAddressedToMe, _log: () => {}, _now: () => 1, _flightComplete: () => {}, _route: () => { throw new Error('answered'); } };
  check('on: a probe passing through a third node returns nothing and sends nothing', AxonaManager.prototype._onReceiptProbe.call(me, { topicId: 'ab', msgId: MSG }, { targetId: 9n, fromId: 'ab' }) === undefined);
  check('on: an ack passing through a third node returns nothing', (await AxonaManager.prototype._onIngestAck.call(me, { topicId: 'ab', msgId: MSG, op: 'pub' }, { targetId: 9n })) === undefined);
  check('on: a nack passing through a third node returns nothing', AxonaManager.prototype._onReceiptNack.call(me, { topicId: 'ab' }, { targetId: 9n }) === undefined);
  const off = { ...me, _flightAddressGuard: false, _route: () => {} };
  check('off: the same three calls return consumed, as before', AxonaManager.prototype._onReceiptNack.call(off, { topicId: 'ab' }, { targetId: 9n }) === 'consumed'
    && (await AxonaManager.prototype._onIngestAck.call(off, { topicId: 'ab', msgId: MSG, op: 'x' }, { targetId: 9n })) === 'consumed'
    && AxonaManager.prototype._onReceiptProbe.call(off, { topicId: 'zz', msgId: MSG }, { targetId: 9n, fromId: 'ab' }) === 'consumed');
}
console.log(`\n${n} checks passed`);
process.exit(0);
