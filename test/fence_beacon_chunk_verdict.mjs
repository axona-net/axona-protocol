// fence_beacon_chunk_verdict.mjs — 4.151.0, two switches, both default OFF.
//   BEACON_CHUNK    a beacon lists at most BEACON_TOPICS_MAX (256) topics; a
//                   longer list goes out as several beacons.
//   BEACON_VERDICT  the beacon handler reports 'consumed' at the node the
//                   beacon was addressed to, and nowhere else.
import assert from 'node:assert/strict';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { BEACON_TOPICS_MAX } from '../src/pubsub/constants.js';
let n = 0; const check = (name, ok, extra = '') => { assert(ok, name + ' ' + extra); n++; console.log('PASS ' + name); };
const P = AxonaManager.prototype;
const T = (i) => (0x80n << 256n) | BigInt(i);
const hex = (b) => b.toString(16).padStart(66, '0');
const ME = (0x80n << 256n) | 0xabcdefn, PEER = (0x80n << 256n) | 0x123456n;
function sender({ chunk, topics }) {
  const sent = [];
  const o = { nodeId: ME, _beaconChunk: chunk, _announceNewOnly: false, _beaconFanout: 6, _beaconLayers: 2, _beaconSeq: 0, _beaconSeen: new Map(), axonRoles: new Map(), _now: () => 1_000_000,
    dht: { neighbors: () => Array.from({ length: 8 }, (_, i) => hex((0x80n << 256n) | BigInt(1000 + i))) }, _cmpXor: P._cmpXor, _route: (nb, type, payload) => sent.push({ nb, payload }), sent };
  for (let i = 1; i <= topics; i++) o.axonRoles.set(T(i), { isRoot: true, epoch: i });
  return o;
}
const uniq = (o) => { const m = new Map(); for (const s of o.sent) m.set(s.payload.beaconId, s.payload); return [...m.values()]; };
function receiver({ verdict = false, id = PEER } = {}) {
  return { nodeId: id, _beaconVerdict: verdict, _beaconSeen: new Map(), _rootBeacons: new Map(), axonRoles: new Map(), _now: () => 5, _bestKnownClosest: () => null,
    _rootTombstoned: () => false, _roleRegionOk: () => true, _mergeCandidate: () => {}, mySubscriptions: new Map(), _subTerminalVerify: false, dht: {}, _beaconFanout: 6, _cmpXor: P._cmpXor };
}
check('the limit is 256', BEACON_TOPICS_MAX === 256);

// ── CHUNK ────────────────────────────────────────────────────────────────
const off = sender({ chunk: false, topics: 600 }); P._emitRootBeacons.call(off);
check('off: 600 topics go out as ONE beacon of 600', uniq(off).length === 1 && uniq(off)[0].topics.length === 600);
const rxOff = receiver(); for (const b of uniq(off)) P._onRootBeacon.call(rxOff, { ...b, layer: 1 }, {});
check('off: the receiver hears 256 of the 600 (the defect)', rxOff._rootBeacons.size === 256);

const on = sender({ chunk: true, topics: 600 }); P._emitRootBeacons.call(on);
const parts = uniq(on);
check('on: 600 topics go out as three beacons of 256, 256, 88', parts.map((p) => p.topics.length).join() === '256,256,88');
check('on: each beacon has its own id and is marked seen by the sender', new Set(parts.map((p) => p.beaconId)).size === 3 && parts.every((p) => on._beaconSeen.has(p.beaconId)));
check('on: each goes to the fan-out of 6', on.sent.length === 18);
check('on: epochs stay index-aligned inside every part', parts.every((p) => p.epochs.length === p.topics.length) && parts[2].epochs[0] === 513 && parts[2].topics[0] === hex(T(513)));
check('on: no topic is lost and none is repeated', new Set(parts.flatMap((p) => p.topics)).size === 600);
const rxOn = receiver(); for (const b of parts) P._onRootBeacon.call(rxOn, { ...b, layer: 1 }, {});
check('on: the receiver hears all 600', rxOn._rootBeacons.size === 600 && rxOn._rootBeacons.get(T(600)).epoch === 600);

for (const count of [1, 255, 256]) { const s = sender({ chunk: true, topics: count }); P._emitRootBeacons.call(s); check(`on: ${count} topic(s) is still one beacon`, uniq(s).length === 1 && uniq(s)[0].topics.length === count); }
const edge = sender({ chunk: true, topics: 257 }); P._emitRootBeacons.call(edge);
check('on: 257 topics is 256 + 1', uniq(edge).map((p) => p.topics.length).join() === '256,1');
const none = sender({ chunk: true, topics: 0 }); P._emitRootBeacons.call(none);
check('on: a node rooting nothing sends nothing', none.sent.length === 0);
const one = sender({ chunk: true, topics: 600 }); P._emitRootBeacons.call(one, [T(599)]);
check('on: a one-topic announcement is one beacon of one', uniq(one).length === 1 && uniq(one)[0].topics[0] === hex(T(599)));
const sameOff = sender({ chunk: false, topics: 40 }), sameOn = sender({ chunk: true, topics: 40 }); P._emitRootBeacons.call(sameOff); P._emitRootBeacons.call(sameOn);
check('within the limit, on and off send the same beacon', JSON.stringify(uniq(sameOff)) === JSON.stringify(uniq(sameOn)));

// ── VERDICT ──────────────────────────────────────────────────────────────
const beacon = (id) => ({ root: hex(ME), topics: [hex(T(1))], epochs: [1], beaconId: id, layer: 1 });
const v0 = receiver({ verdict: false });
check('off: the handler returns nothing, as before', P._onRootBeacon.call(v0, beacon('a'), { targetId: PEER }) === undefined && v0._rootBeacons.size === 1);
const v1 = receiver({ verdict: true });
check('on: the addressed node reports consumed', P._onRootBeacon.call(v1, beacon('b'), { targetId: PEER }) === 'consumed' && v1._rootBeacons.size === 1);
check('on: a duplicate at the addressed node is consumed too, and not re-read', P._onRootBeacon.call(v1, beacon('b'), { targetId: PEER }) === 'consumed');
const hop = receiver({ verdict: true });
check('on: a hop the beacon is NOT addressed to does not consume it', P._onRootBeacon.call(hop, beacon('c'), { targetId: (0x80n << 256n) | 0x777n }) === undefined && hop._rootBeacons.size === 1);
const origin = receiver({ verdict: true, id: ME }); origin._beaconSeen.set('d', 1);
check('on: the sender, running its own handler at hop 0, does not consume it', P._onRootBeacon.call(origin, beacon('d'), { targetId: PEER }) === undefined);
const bad = receiver({ verdict: true });
check('on: a malformed beacon is not consumed', P._onRootBeacon.call(bad, { root: 5 }, { targetId: PEER }) === undefined && P._onRootBeacon.call(bad, null, { targetId: PEER }) === undefined);
check('on: a beacon with a bad root id is not consumed', P._onRootBeacon.call(bad, { ...beacon('e'), root: 'zz' }, { targetId: PEER }) === undefined);
check('on: hostile meta does not throw and does not consume', P._onRootBeacon.call(receiver({ verdict: true }), beacon('f'), undefined) === undefined && P._onRootBeacon.call(receiver({ verdict: true }), beacon('g'), { targetId: hex(PEER) }) === undefined && P._onRootBeacon.call(receiver({ verdict: true }), beacon('h'), { targetId: null }) === undefined);
// The forward still happens before the verdict is returned.
const fw = receiver({ verdict: true }); const fwSent = []; fw.dht = { neighbors: () => [hex((0x80n << 256n) | 0x2000n), hex((0x80n << 256n) | 0x2001n)] }; fw._route = (nb, t, p) => fwSent.push(p);
check('on: a layer-2 beacon is still forwarded, one layer down, and consumed', P._onRootBeacon.call(fw, { ...beacon('i'), layer: 2 }, { targetId: PEER }) === 'consumed' && fwSent.length === 2 && fwSent[0].layer === 1);
console.log(`fence_beacon_chunk_verdict: ${n} passed, 0 failed`);
