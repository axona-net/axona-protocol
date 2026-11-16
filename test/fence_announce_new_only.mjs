// fence_announce_new_only.mjs — 4.150.0 ANNOUNCE_NEW_ONLY (default OFF).
// A root claim announces the ONE topic it is about; the BEACON_MS timer still
// sends the whole list. Off, every claim sends the whole list, as 4.149.0.
import assert from 'node:assert/strict';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
let n = 0; const check = (name, ok, extra = '') => { assert(ok, name + ' ' + extra); n++; console.log('PASS ' + name); };
const P = AxonaManager.prototype;
const T = (i) => (0x80n << 256n) | BigInt(i);
const hex = (b) => b.toString(16).padStart(66, '0');
function mk({ on, topics = 5, neighbours = 8 } = {}) {
  const sent = []; let clock = 1_000_000;
  const o = {
    nodeId: (0x80n << 256n) | 0xabcdefn, _announceNewOnly: on, _beaconFanout: 6, _beaconLayers: 2, _beaconSeq: 0, _beaconSeen: new Map(),
    axonRoles: new Map(), _now: () => clock, tick: (ms) => { clock += ms; },
    dht: { neighbors: () => Array.from({ length: neighbours }, (_, i) => hex((0x80n << 256n) | BigInt(1000 + i))) },
    _cmpXor: P._cmpXor, _route: (nb, type, payload) => { sent.push({ nb, type, payload }); }, sent,
  };
  o._emitRootBeacons = P._emitRootBeacons.bind(o);
  for (let i = 1; i <= topics; i++) o.axonRoles.set(T(i), { isRoot: true, epoch: i });
  return o;
}
const beacons = (o) => { const m = new Map(); for (const s of o.sent) m.set(s.payload.beaconId, s.payload); return [...m.values()]; };

// OFF: a claim sends the whole list (the behaviour being replaced).
const off = mk({ on: false, topics: 5 });
P._announceRoot.call(off, T(3));
check('off: one beacon to the fan-out of 6', off.sent.length === 6 && beacons(off).length === 1);
check('off: it lists every rooted topic', beacons(off)[0].topics.length === 5);

// ON: a claim sends the one topic.
const on = mk({ on: true, topics: 5 });
P._announceRoot.call(on, T(3));
const b = beacons(on)[0];
check('on: one beacon to the fan-out of 6', on.sent.length === 6 && beacons(on).length === 1);
check('on: it lists only the claimed topic', b.topics.length === 1 && b.topics[0] === hex(T(3)));
check('on: the epoch is that topic\'s, index-aligned', b.epochs.length === 1 && b.epochs[0] === 3);
check('on: root, beaconId and layer are as before', b.root === hex(on.nodeId) && typeof b.beaconId === 'string' && b.layer === 2 && on._beaconSeen.has(b.beaconId));

// The timer path is untouched: full list, switch on or off.
const timer = mk({ on: true, topics: 7 }); timer._emitRootBeacons();
check('on: the timer still sends the whole list', beacons(timer)[0].topics.length === 7);

// The per-topic rate limit holds; a different topic is not limited by it.
P._announceRoot.call(on, T(3));
check('on: the same topic inside the window is not re-announced', beacons(on).length === 1);
P._announceRoot.call(on, T(4));
check('on: another topic is announced', beacons(on).length === 2 && beacons(on)[1].topics[0] === hex(T(4)));
on.tick(10_001); P._announceRoot.call(on, T(3));
check('on: the same topic after the window is announced again', beacons(on).length === 3);

// A topic this node does not root, or is dismantling, is never announced.
const neg = mk({ on: true, topics: 2 });
neg.axonRoles.set(T(50), { isRoot: false, epoch: 1 }); neg.axonRoles.set(T(51), { isRoot: true, dismantling: true, epoch: 1 });
P._announceRoot.call(neg, T(50)); P._announceRoot.call(neg, T(51)); P._announceRoot.call(neg, T(99));
check('on: not-root, dismantling and unknown topics send nothing', neg.sent.length === 0);
const negOff = mk({ on: false, topics: 2 }); negOff.axonRoles.set(T(51), { isRoot: true, dismantling: true, epoch: 1 }); negOff._emitRootBeacons();
check('off: a dismantling root is left out of the full list', beacons(negOff)[0].topics.length === 2);

// The point of the change: n claims cost n entries, not n squared.
function claimAll(flag, count) { const o = mk({ on: flag, topics: 0 }); for (let i = 1; i <= count; i++) { o.axonRoles.set(T(i), { isRoot: true, epoch: 1 }); P._announceRoot.call(o, T(i)); } return beacons(o).reduce((a, x) => a + x.topics.length, 0); }
const offCost = claimAll(false, 300), onCost = claimAll(true, 300);
check('300 claims: off sends 45,150 topic entries, on sends 300', offCost === 45150 && onCost === 300, `${offCost} ${onCost}`);

// A receiver reads the short beacon exactly as it reads a long one.
const rx = { _beaconSeen: new Map(), _rootBeacons: new Map(), axonRoles: new Map(), nodeId: (0x80n << 256n) | 0x999999999n, _now: () => 5, _bestKnownClosest: () => null,
  _rootTombstoned: () => false, _roleRegionOk: () => true, _mergeCandidate: () => {}, mySubscriptions: new Map(), _subTerminalVerify: false, dht: {}, _beaconFanout: 6, _cmpXor: P._cmpXor };
P._onRootBeacon.call(rx, { ...b, layer: 1 }, {});
check('receiver records the pointer and epoch from a one-topic beacon', rx._rootBeacons.get(T(3))?.root === hex(on.nodeId) && rx._rootBeacons.get(T(3)).epoch === 3 && rx._rootBeacons.size === 1);
console.log(`fence_announce_new_only: ${n} passed, 0 failed`);
