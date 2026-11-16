#!/usr/bin/env node
// fence_flight_three_node_walk.mjs — the write flight across TWO hops.
// Manager option flightAddressGuard / env FLIGHT_ADDRESS_GUARD, default OFF
// (4.152.0 the address guard; 4.153.0 the probe's reply).
//
// The scene is Orion's (council 23b60d5d, 29017d0d; 2026-11-16): three REAL
// AxonaPeers on the in-memory network in a line, O <-> M <-> R, with no O–R
// channel. O owns a flight, R is the root, M is a relay that holds a backup
// role for the topic. It showed what a two-node scene cannot: past one hop,
// meta.fromId at the root is the RELAY, so a probe reply sent to it stops one
// hop short, and an unsigned ack that does reach the owner binds to the relay
// and is refused.
//
// Run: node test/fence_flight_three_node_walk.mjs
import assert from 'node:assert/strict';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { realTopic, author }        from './lib/mergeFixture.mjs';
import { buildEnvelope }            from '../src/pubsub/envelope.js';
import { T }                        from '../src/pubsub/constants.js';
import { idHex }                    from '../src/pubsub/ids.js';

let n = 0;
const check = (name, ok, extra = '') => { assert(ok, `${name} ${extra}`); n++; console.log('PASS ' + name); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function pickId(pred) {
  for (;;) {
    const id = await createNodeIdentity({ lat: 40.7128, lng: -74.0060 });
    if (pred(parseInt(id.id.slice(2, 4), 16))) return id;
  }
}

// ── Orion's scene ────────────────────────────────────────────────────────────
async function threeNodeLineScene(guard, tag = '') {
  const Lt = await realTopic('line3-' + (guard ? 'on' : 'off') + tag);
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0 });
  // Bits [6:7] of byte 1, relative to the TOPIC's (tb): R = tb, M = tb^01, O = tb^11.
  // So (M^R) < (O^R) and (M^O) < (R^O): the line routes O -> M -> R; and R is
  // the closest of the three to the topic, so no beacon from M's backup role
  // can demote it. (Orion's scene fixed O=00, M=01, R=11, which is this with
  // tb = 11; it held for his two topic names and not for others.)
  const tb = parseInt(idHex(Lt.topicId).slice(2, 4), 16) & 0xc0;
  const [idO, idM, idR] = [
    await pickId((b) => (b & 0xc0) === (tb ^ 0xc0)),
    await pickId((b) => (b & 0xc0) === (tb ^ 0x40)),
    await pickId((b) => (b & 0xc0) === tb),
  ];
  const mk = async (id) => {
    const transport = simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 });
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40.7128, lng: -74.0060 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    const am = peer._requireAxonaManager('line3');
    am._flightAddressGuard = guard;
    const logs = [];
    am._logSink = (l, ev, ctx) => logs.push({ ev, ctx });
    return { peer, transport, node, am, big: fromHex(id.id), hex: id.id, logs };
  };
  const [O, M, R] = [await mk(idO), await mk(idM), await mk(idR)];
  await O.transport.openConnection(M.hex);
  await M.transport.openConnection(R.hex);
  for (const [a, b] of [[O, M], [M, O], [M, R], [R, M]]) {
    const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
    syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'line3';
    a.node.synaptome.set(b.big, syn);
  }
  await sleep(20);
  const mRole = M.am._rootClaim.become(Lt.topicId, 'sub-terminal') || M.am.axonRoles.get(Lt.topicId);
  mRole.isRoot = false;
  const role = R.am._rootClaim.become(Lt.topicId, 'sub-terminal') || R.am.axonRoles.get(Lt.topicId);
  const flights = () => (O.am._writeFlights ? O.am._writeFlights.size : 0);
  const ev = (rec, name) => rec.logs.filter((l) => l.ev === name || l.ev === 'pubsub:' + name).length;
  return {
    Lt, O, M, R, role, mRole, flights, ev,
    stop: async () => { for (const x of [O, M, R]) { try { await x.peer.stop?.(); } catch { /* */ } } },
  };
}
const envelopeFor = (s, text) => buildEnvelope({ topic: s.Lt.desc, message: { v: 1, text, authorClass: 'agent' }, seq: 1, identity: author, ts: Date.now() });
// Put O's flight past its ack deadline and run the sweep: the next step is a probe.
const sweepNow = (s) => { for (const f of s.O.am._writeFlights.values()) f.deadline = 0; s.O.am._flightSweep(); };

// ── 1. Orion's walk: a forwarded write and the root's signed proof ───────────
for (const guard of [false, true]) {
  const s = await threeNodeLineScene(guard, '-fwd');
  const env = await envelopeFor(s, 'hello-' + guard);
  s.mRole.cacheIds.add(env.msgId);
  s.O.am._forwardToRoot(s.Lt.topicId, T.PUB, { topicId: idHex(s.Lt.topicId), json: JSON.stringify(env) }, s.R.hex);
  await sleep(150);
  check(`${guard ? 'on' : 'off'}: the root two hops away ingests the forwarded write`, s.R.am.axonRoles.get(s.Lt.topicId)?.cacheIds.has(env.msgId) === true, JSON.stringify({ R: s.R.logs.slice(-5), M: s.M.logs.slice(-3), O: s.O.logs.slice(-3) }));
  if (guard) check('on: its signed proof crosses the relay and the owner\'s flight completes', s.flights() === 0);
  else       check('off (the defect): its signed proof never leaves the root; the flight stays open', s.flights() === 1);
  await s.stop();
}

// ── 2. the probe, root HOLDS the message, two hops (4.153.0) ─────────────────
{
  const s = await threeNodeLineScene(true, '-held');
  const env = await envelopeFor(s, 'held');
  s.role.cacheIds.add(env.msgId);                 // the root has it; the first ack was lost
  s.mRole.cacheIds.add(env.msgId);                // so does the relay, as a backup
  const tuple = s.O.am._flightOpen(s.Lt.topicId, s.R.hex, T.PUB, { topicId: idHex(s.Lt.topicId), json: JSON.stringify(env) });
  check('a flight is open at the owner', !!tuple && s.flights() === 1);
  sweepNow(s);
  await sleep(200);
  check('on: the owner probed once', s.ev(s.O, 'receipt-probe') === 1);
  check('on: the relay that holds the message did not answer for the root', s.ev(s.O, 'write-flight-ack-unbound') === 0);
  check('on: the root answered with a signed proof and the flight completed', s.flights() === 0, JSON.stringify(s.O.logs.slice(-4)));
  check('on: the relay recorded no ack of its own', !(s.M.am._ingestAcks && s.M.am._ingestAcks.size));
  await s.stop();
}
// ── 3. the probe, root does NOT hold the message, two hops (4.153.0) ─────────
{
  const s = await threeNodeLineScene(true, '-nack');
  const env = await envelopeFor(s, 'never-arrived');
  s.O.am._flightOpen(s.Lt.topicId, s.R.hex, T.PUB, { topicId: idHex(s.Lt.topicId), json: JSON.stringify(env) });
  sweepNow(s);
  await sleep(250);
  check('on: the root says not-held to the OWNER, which retries once', s.ev(s.O, 'receipt-nack-retry') === 1, JSON.stringify(s.O.logs.slice(-4)));
  check('on: the relay did not act on the nack', s.ev(s.M, 'receipt-nack-retry') === 0);
  check('on: the retry reaches the root, which ingests the write', s.R.am.axonRoles.get(s.Lt.topicId)?.cacheIds.has(env.msgId) === true);
  check('on: the root\'s signed proof for the retry completes the flight; no root is evicted', s.flights() === 0 && s.ev(s.O, 'root-evicted') === 0);
  await s.stop();
}
// ── 4. control: the same probe with the switch off ───────────────────────────
{
  const s = await threeNodeLineScene(false, '-ctl');
  const env = await envelopeFor(s, 'control');
  s.role.cacheIds.add(env.msgId);
  s.O.am._flightOpen(s.Lt.topicId, s.R.hex, T.PUB, { topicId: idHex(s.Lt.topicId), json: JSON.stringify(env) });
  sweepNow(s);
  await sleep(200);
  check('off (the defect): the probe is answered at its sender and the flight stays open', s.flights() === 1 && s.ev(s.O, 'receipt-probe') === 1);
  await s.stop();
}
// ── 5. an asker that sends no ack routing (an older kernel) ──────────────────
{
  const s = await threeNodeLineScene(true, '-old');
  const env = await envelopeFor(s, 'old-asker');
  s.role.cacheIds.add(env.msgId);
  s.O.am._flightOpen(s.Lt.topicId, s.R.hex, T.PUB, { topicId: idHex(s.Lt.topicId), json: JSON.stringify(env) });
  const r = await s.O.am._route(s.R.big, T.RECEIPTPROBE, { topicId: idHex(s.Lt.topicId), msgId: env.msgId, op: 'pub' });
  await sleep(200);
  check('on: a probe with no ack routing still reaches the root', r && r.consumed === true && r.hops === 2);
  check('on: its unsigned reply reaches the owner and is REFUSED there, as an unsigned ack past one hop always is', s.flights() === 1 && s.ev(s.O, 'write-flight-ack-unbound') === 1);
  await s.stop();
}
console.log(`\n${n} checks passed`);
process.exit(0);
