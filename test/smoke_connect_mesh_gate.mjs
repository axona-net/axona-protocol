// =====================================================================
// smoke_connect_mesh_gate.mjs — connect() fails hard when it forms no LIVE
// WebRTC mesh, with an easy bridge-only override (GH #46, David 2026-08-06).
//
// Howard (#46): if a node forms no WebRTC connection to any peer, connect()
// still succeeds and nothing is logged — the only tell was status.peers on some
// surfaces. A silent bridge-only success tells an app it will work when for its
// user it won't.
//
// David's call: fail hard, with an easy override — bridge-only is genuinely the
// only path for some users (WebRTC blocked, carrier NAT).
//
// Council (Aster + Orion, 2026-08-06): gate on the count of LIVE authenticated
// WebRTC channels (transport.meshBoundCount()), NOT node.synaptome.size, which
// can hold un-evicted stale entries during churn and mask a dead mesh.
//
// The contract this pins:
//   A  0 live peers + default        → throws MeshUnreachableError, and tears
//                                        down (no leaked transport socket)
//   B  0 live peers + allowBridgeOnly → resolves; status.initialBridgeOnly true
//   C  ready:false (opted out)        → no gate (nothing measured), no throw
//   D  live channels present, EMPTY synaptome → no throw. This is the direct
//      proof the gate reads LIVE channels and NOT node.synaptome.size: the pass
//      decision is taken with an empty routing table, so a stale/lingering
//      synapse could never mask a dead mesh (the churn case Aster + Orion raised).
//      (Post-connect liveness — a mesh that dies AFTER a healthy start — is the
//      runtime monitor's job, GH #438, not connect()'s.)
//
// A stub transport stands in for the web stack: meshBoundCount() is settable so
// the "live channel" count is controlled independently of the routing table.
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
// registrations. `meshBound` controls the LIVE authenticated-channel count that
// connect()'s gate reads via meshBoundCount() — independent of the synaptome.
function stubTransport({ meshBound = 0 } = {}) {
  const t = {
    stopped: 0,
    meshBound,
    meshBoundCount() { return t.meshBound; },
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
const FAST_READY = { minPeers: 1, timeoutMs: 300, stableMs: 100, pollMs: 50 };

async function main() {
  console.log('connect() — mesh-reachability gate (live channels, fail hard, bridge-only override)\n');

  // ── A. default: 0 live peers → throws + tears down ──────────────────
  {
    const tr = stubTransport({ meshBound: 0 });
    let threw = null;
    try {
      await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    check('A: threw when no live WebRTC peers formed', !!threw);
    check('A: error is MeshUnreachableError', threw instanceof MeshUnreachableError);
    check('A: code is MESH_UNREACHABLE', threw?.code === 'MESH_UNREACHABLE');
    check('A: context reports peers:0 (live)', threw?.context?.peers === 0);
    check('A: tore down the transport (no leaked socket)', tr.stopped >= 1);
  }

  // ── B. allowBridgeOnly: 0 live peers → resolves, flagged bridge-only ─
  {
    const tr = stubTransport({ meshBound: 0 });
    let res = null, threw = null;
    try {
      res = await connect({
        location: LOC, author: false, transport: tr,
        ready: FAST_READY, allowBridgeOnly: true,
      });
    } catch (e) { threw = e; }
    check('B: did NOT throw under allowBridgeOnly', !threw);
    check('B: status.initialBridgeOnly === true', res?.status?.initialBridgeOnly === true);
    check('B: did NOT tear down (peer is usable)', tr.stopped === 0);
    await res?.disconnect?.();
  }

  // ── C. ready:false → no measurement, no gate, no throw ──────────────
  {
    const tr = stubTransport({ meshBound: 0 });
    let res = null, threw = null;
    try {
      res = await connect({ location: LOC, author: false, transport: tr, ready: false });
    } catch (e) { threw = e; }
    check('C: ready:false does not throw (gate skipped)', !threw);
    check('C: status is null (nothing measured)', res?.status === null);
    await res?.disconnect?.();
  }

  // ── D. live channel present + EMPTY synaptome → no throw ────────────
  // The gate reads LIVE channels (meshBoundCount), so the pass decision is taken
  // even though the routing table is empty. That is exactly why a stale/lingering
  // synapse can never mask a dead mesh: the gate never consults synaptome.size.
  {
    const tr = stubTransport({ meshBound: 1 });
    let res = null, threw = null;
    try {
      res = await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    check('D: did NOT throw with a live channel present', !threw);
    check('D: decision was taken with an EMPTY synaptome (gate reads live, not routing table)',
      res?.peer?._node?.synaptome?.size === 0);
    check('D: status.initialBridgeOnly === false', res?.status?.initialBridgeOnly === false);
    await res?.disconnect?.();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
