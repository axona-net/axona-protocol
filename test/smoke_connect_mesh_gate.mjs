// =====================================================================
// smoke_connect_mesh_gate.mjs — connect() fails hard when it forms no LIVE
// WebRTC mesh, judged against ONE shared deadline (GH #46 + GH #48).
//
// Howard (#46): if a node forms no WebRTC connection to any peer, connect()
// still succeeds and nothing is logged. David's call: fail hard, with an easy
// override (allowBridgeOnly) — bridge-only is genuinely the only path for some
// users (WebRTC blocked, carrier NAT).
//
// GH #48 (prod outage, 2026-08-07): the 4.61.0 gate judged live channels at
// the instant ready() resolved — but ready({minPeers}) resolves off the
// SYNAPTOME, which bridge announcements seed instantly, so every minPeers
// caller threw MESH_UNREACHABLE at 0ms while its channels were still binding.
// Council (Aster + Orion, 2026-08-07): the contract is atomic — when mesh is
// required, connect() resolves only after the live-mesh predicate holds
// within the caller's own single monotonic deadline (ready.timeoutMs). No
// grace period; no silent extension; allowBridgeOnly is the only bypass.
//
// The contract this pins (council regression set, replacing old case E):
//   A  0 live peers, none bind by deadline → throws MeshUnreachableError at
//      ~deadline (not at 0ms), and tears down (no leaked socket)
//   B  0 live peers + allowBridgeOnly → resolves immediately (bypass — no
//      admission wait); status.initialBridgeOnly true
//   C  ready:false (opted out) → no gate (nothing measured), no throw
//   D  already bound at gate time, EMPTY synaptome → immediate pass. Proof the
//      gate reads LIVE channels, never node.synaptome.size.
//   E  synaptome seeded instantly (the outage shape), channel binds INSIDE the
//      deadline → PASSES, resolving on the bind event (not the full deadline)
//   F  synaptome seeded, NO bind by deadline → throws; context carries
//      synapses>0 / peers===0 (the stale-table distinction)
//   G  no leak: after both resolve and throw paths, the stub holds ZERO
//      registered bind listeners, and a bind that fires AFTER the deadline
//      finds no listener and does nothing (no crash, no late resolve)
//
// A stub transport stands in for the web stack: meshBound is settable, and
// bindAfterMs schedules meshBound=1 plus a fire of every registered
// onPeerBound listener — modelling the real DTLS/ICE completion.
//
// Run: node test/smoke_connect_mesh_gate.mjs
// =====================================================================

import { connect }               from '../src/connect.js';
import { MeshUnreachableError }  from '../src/errors.js';
import { sealByOwnMethods }      from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, cond) => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
};

// Transport stub. `seedPeerAfterMs` fires listeners with a synthetic BigInt id
// WITHOUT touching meshBound (routing-table entry, no live channel — the churn
// /stale shape). `bindAfterMs` sets meshBound=1 and fires listeners (a real
// channel bind). `listeners` is inspectable for the leak check (case G).
function stubTransport({ meshBound = 0, seedPeerAfterMs = null, bindAfterMs = null, seedAtRegistration = false } = {}) {
  const t = {
    stopped: 0,
    meshBound,
    seedAtRegistration,
    _seeded: false,
    listeners: new Set(),
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
    onRequest() {}, onNotification() {}, onPeerDied() {},
    onPeerBound(cb) {
      t.listeners.add(cb);
      // seedAtRegistration: fire synchronously on the FIRST registration
      // (AxonaPeer's, during start(), before ready() begins) so the synapse
      // exists before any wait starts. A setTimeout seed raced the ready
      // window under full-suite parallel load and made case F flake — the
      // snapshot in status.peers was taken before the starved timer fired.
      if (t.seedAtRegistration && !t._seeded) {
        t._seeded = true;
        try { cb(0x9999n); } catch { /* smoke */ }
      }
      return () => t.listeners.delete(cb);
    },
    fireBound(id = 0x9999n) {
      for (const cb of [...t.listeners]) { try { cb(id); } catch { /* smoke */ } }
    },
    deliverMeshSignal() {},
  };
  if (seedPeerAfterMs != null) setTimeout(() => t.fireBound(), seedPeerAfterMs);
  if (bindAfterMs != null) setTimeout(() => { t.meshBound = 1; t.fireBound(0x8888n); }, bindAfterMs);
  // E3b.4 (SEAL): the transport mock deposits its dispatch capability (no fallback).
  return sealByOwnMethods(t);
}

const LOC = { lat: 40.71, lng: -74.0 };
const FAST_READY = { minPeers: 1, timeoutMs: 400, stableMs: 100, pollMs: 25 };

async function main() {
  console.log('connect() — mesh gate: one deadline, live-channel admission, bridge-only override\n');

  // ── A. default: nothing ever binds → throws at ~deadline, tears down ─
  {
    const tr = stubTransport({ meshBound: 0 });
    const t0 = Date.now();
    let threw = null;
    try {
      await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    const elapsed = Date.now() - t0;
    check('A: threw when no live WebRTC peers formed', !!threw);
    check('A: error is MeshUnreachableError', threw instanceof MeshUnreachableError);
    check('A: code is MESH_UNREACHABLE', threw?.code === 'MESH_UNREACHABLE');
    check('A: context reports peers:0 (live)', threw?.context?.peers === 0);
    check('A: waited the deadline out, not 0ms (GH #48 regression)',
      threw?.context?.ms >= FAST_READY.timeoutMs - 50 && elapsed >= FAST_READY.timeoutMs - 50);
    check('A: tore down the transport (no leaked socket)', tr.stopped >= 1);
  }

  // ── B. allowBridgeOnly: bypass — resolves without the admission wait ─
  {
    const tr = stubTransport({ meshBound: 0 });
    const t0 = Date.now();
    let res = null, threw = null;
    try {
      res = await connect({
        location: LOC, author: false, transport: tr,
        ready: FAST_READY, allowBridgeOnly: true,
      });
    } catch (e) { threw = e; }
    check('B: did NOT throw under allowBridgeOnly', !threw);
    check('B: status.initialBridgeOnly === true', res?.status?.initialBridgeOnly === true);
    check('B: bypass did not sit out the admission deadline',
      Date.now() - t0 < FAST_READY.timeoutMs + 400);
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

  // ── D. already bound + EMPTY synaptome → immediate pass ─────────────
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

  // ── E. THE OUTAGE SHAPE: synaptome seeded instantly, bind lands inside
  //       the deadline → PASSES, resolving on the bind event ────────────
  {
    const tr = stubTransport({ meshBound: 0, seedPeerAfterMs: 1, bindAfterMs: 120 });
    const t0 = Date.now();
    let res = null, threw = null;
    try {
      res = await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    const elapsed = Date.now() - t0;
    check('E: PASSES when the channel binds inside the deadline (4.61.0 threw here)', !threw);
    check('E: resolved on the bind event, well before the deadline expired',
      elapsed < FAST_READY.timeoutMs + 200);
    check('E: status.initialBridgeOnly === false', res?.status?.initialBridgeOnly === false);
    await res?.disconnect?.();
  }

  // ── F. stale-table shape: synaptome seeded, NO bind by deadline → throws
  {
    const tr = stubTransport({ meshBound: 0, seedAtRegistration: true });
    let threw = null;
    try {
      await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    check('F: throws when live channels stay 0 despite a non-empty synaptome',
      threw instanceof MeshUnreachableError);
    check('F: context proves the distinction — synapses>0 but live peers===0',
      threw?.context?.synapses >= 1 && threw?.context?.peers === 0);
    check('F: and it tore down (no leaked socket)', tr.stopped >= 1);
  }

  // ── G. no leak: listeners cleaned on BOTH paths; a late bind is inert ─
  {
    // Throw path: after connect() threw (case-F shape), every gate/peer
    // listener must be gone, and a bind arriving late must do nothing.
    const tr = stubTransport({ meshBound: 0 });
    let threw = null;
    try {
      await connect({ location: LOC, author: false, transport: tr, ready: FAST_READY });
    } catch (e) { threw = e; }
    check('G: throw path released every bind listener', threw && tr.listeners.size === 0);
    let lateCrash = false;
    try { tr.meshBound = 1; tr.fireBound(0x7777n); } catch { lateCrash = true; }
    check('G: a bind AFTER the deadline finds no listener and does nothing', !lateCrash);

    // Resolve path: after a clean connect + disconnect, listeners are gone too.
    const tr2 = stubTransport({ meshBound: 1 });
    const res2 = await connect({ location: LOC, author: false, transport: tr2, ready: FAST_READY });
    await res2.disconnect();
    check('G: resolve+disconnect path released every bind listener', tr2.listeners.size === 0);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
