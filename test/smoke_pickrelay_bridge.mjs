// =====================================================================
// smoke_pickrelay_bridge.mjs — the bridge is never recruited as a relay axon.
//
// Sub-axon recruitment (_pickRelayPeer) hands a subtree of subscribers to a
// peer XOR-closest to the new subscriber. The bridge is in every peer's
// synaptome (universal connector) and is often XOR-closest, but it is signaling
// infra — handing it a subtree black-holes that branch (it doesn't serve as a
// relay axon). It must be skipped here, like findKClosest / route_msg /
// _greedyNextHopToward already do.  (_pickRelayPeer reads only `this._node`, so
// we drive it with a minimal stub.)
//
// Run: node test/smoke_pickrelay_bridge.mjs
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };

const REG = 0x80n << 248n;
const SELF   = REG | 0x1000n;
const SUB    = REG | 0x2000n;          // the new subscriber
const BRIDGE = REG | 0x2001n;          // XOR-closest to SUB → would be picked, but it's the bridge
const RELAY  = REG | 0x2010n;          // a real relay, slightly farther from SUB

function mkSelf(bridgeId) {
  const synaptome = new Map([
    [BRIDGE, { peerId: BRIDGE }],
    [RELAY,  { peerId: RELAY  }],
  ]);
  return { _node: {
    alive: true, id: SELF, synaptome, _deadPeers: new Set(),
    transport: { bridgeNodeIdBig: bridgeId },
  } };
}
const role = { children: new Map() };
const pick = (self) => AxonaPeer.prototype._pickRelayPeer.call(self, role, SUB, 0n);

// ── 1. with the bridge known, it is skipped → the real relay is chosen ──
ok('bridge excluded → picks the real relay (not the closer bridge)', pick(mkSelf(BRIDGE)) === RELAY);

// ── 2. control: if that id were NOT the bridge, it WOULD be picked (proves it's closest) ──
ok('control: same id wins when it is not flagged as the bridge', pick(mkSelf(null)) === BRIDGE);

console.log(`\n${fail ? '✗' : '✓'} smoke_pickrelay_bridge: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
