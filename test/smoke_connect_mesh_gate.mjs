// =====================================================================
// smoke_connect_mesh_gate.mjs — connect() fails hard when it forms no
// WebRTC mesh, with an easy bridge-only override (GH #46, David 2026-08-06).
//
// Howard (#46): if a node fails to connect via WebRTC to ANY peer, connect()
// still succeeds and nothing is logged — the only tell is status.peers on some
// surfaces. David's call: fail hard, because a silent bridge-only success tells
// an app it will work when for its user it won't. But bridge-only is genuinely
// the only option for some users (WebRTC blocked, carrier NAT), so there must be
// an easy override.
//
// The contract this pins:
//   A  0 mesh peers + default        → throws MeshUnreachableError, and tears
//                                        down (no leaked transport socket)
//   B  0 mesh peers + allowBridgeOnly → resolves; status.bridgeOnly === true
//   C  ready:false (opted out)        → no gate (nothing measured), no throw
//   D  a populated synaptome          → the gated signal (ready().peers) is > 0,
//                                        so the gate does NOT fire
//
// A minimal stub transport stands in for the web stack: it completes start()
// and never forms a mesh peer, so the synaptome stays empty — exactly the
// "reached the bridge and nothing else" state.
//
// Run: node test/smoke_connect_mesh_gate.mjs
// =====================================================================

import { connect }               from '../src/connect.js';
import { MeshUnreachableError }  from '../src/errors.js';

let passed = 0, failed = 0;
const check = (label, cond) => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
};

// A complete-enough Transport stub: satisfies AxonaPeer.start()'s handler
// registrations and never forms a mesh peer (synaptome stays empty).
function stubTransport() {
  const t = {
    stopped: 0,
    async start() {},
    async stop() { t.stopped++; },
    openConnection: async () => false,
    closeConnection: async () => {},
    isConnected: () => false,
    boundPeers: () => [],
    getLatency: () => 0,
    nodeIdFor: () => null,
    async send() { return null; },
    async notify() {},
    onRequest() {}, onNotification() {}, onPeerBound() {}, onPeerDied() {},
    deliverMeshSignal() {},
  };
  return t;
}

const LOC = { lat: 40.71, lng: -74.0 };
// A ready() window that resolves fast on an empty synaptome (timeout → peers:0).
const FAST_READY = { minPeers: 1, timeoutMs: 300, stableMs: 100, pollMs: 50 };

async function main() {
  console.log('connect() — mesh-reachability gate (fail hard, bridge-only override)\n');

  // ── A. default: 0 mesh peers → throws + tears down ──────────────────
  {
    const tr = stubTransport();
    let threw = null;
    try {
      await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    check('A: threw when no WebRTC peers formed', !!threw);
    check('A: error is MeshUnreachableError', threw instanceof MeshUnreachableError);
    check('A: code is MESH_UNREACHABLE', threw?.code === 'MESH_UNREACHABLE');
    check('A: context reports peers:0', threw?.context?.peers === 0);
    check('A: tore down the transport (no leaked socket)', tr.stopped >= 1);
  }

  // ── B. allowBridgeOnly: 0 mesh peers → resolves, flagged bridge-only ─
  {
    const tr = stubTransport();
    let res = null, threw = null;
    try {
      res = await connect({
        location: LOC, author: false, transport: tr,
        ready: FAST_READY, allowBridgeOnly: true,
      });
    } catch (e) { threw = e; }
    check('B: did NOT throw under allowBridgeOnly', !threw);
    check('B: status.bridgeOnly === true', res?.status?.bridgeOnly === true);
    check('B: status.peers === 0 (honest about the mesh)', res?.status?.peers === 0);
    check('B: did NOT tear down (peer is usable)', tr.stopped === 0);
    await res?.disconnect?.();
  }

  // ── C. ready:false → no measurement, no gate, no throw ──────────────
  {
    const tr = stubTransport();
    let res = null, threw = null;
    try {
      res = await connect({ location: LOC, author: false, transport: tr, ready: false });
    } catch (e) { threw = e; }
    check('C: ready:false does not throw (gate skipped)', !threw);
    check('C: status is null (nothing measured)', res?.status === null);
    await res?.disconnect?.();
  }

  // ── D. a populated synaptome → the gated signal is > 0 ───────────────
  // Bring a peer up without the gate (ready:false), seed one synapse, then run
  // the very check connect() gates on: ready() must report peers > 0, so the
  // gate's positive branch (no throw, bridgeOnly:false) is the one taken.
  {
    const tr = stubTransport();
    const res = await connect({ location: LOC, author: false, transport: tr, ready: false });
    res.peer._node.synaptome.set(123n, { peerId: 123n, stratum: 0 });
    const st = await res.peer.ready({ minPeers: 1, timeoutMs: 500, stableMs: 100, pollMs: 50 });
    check('D: ready() reports peers > 0 when the synaptome is populated', st.peers >= 1);
    check('D: so the gate condition (peers === 0) is false', st.peers !== 0);
    await res.disconnect?.();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
