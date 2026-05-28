// =====================================================================
// smoke_mesh_eviction.js — MeshManager heartbeat-timeout eviction.
//
// Regression guard for the Safari-after-sleep failure: a data channel
// whose pongs stop (or whose send() throws) while readyState still lies
// 'open' must be evicted — _teardown + onPeerLost fire — so the mesh
// heals and routes around it.  Before this, the stale-checker only
// oscillated open↔stale and nothing ever tore a silent-but-'open'
// channel down, so it stuck at yellow/red forever.
//
// Drives the extracted _staleCheckTick / _pingTick directly so the
// time-based logic is testable without waiting on real intervals.
//
// Run: node test/smoke_mesh_eviction.js
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PEER = 'bb' + 'b2'.repeat(32);

function newMesh() {
  return new MeshManager({ sendSignal: () => {}, log: () => {} });
}

// A peer-state shaped like the ones MeshManager builds for an open dc.
function fakeState(over = {}) {
  return {
    peerId:   PEER,
    state:    'open',
    role:     'offerer',
    openedAt: Date.now() - 60_000,   // was genuinely open (so onPeerLost fires)
    lastPongAt: Date.now(),
    pings: 10, pongs: 10,
    rttBuffer: [],
    sendFailures: 0,
    dc: { readyState: 'open', send() {}, close() {} },
    pc: { close() {} },
    ...over,
  };
}

function main() {
  console.log('MeshManager heartbeat-timeout eviction\n');

  // ── pong-timeout → eviction ───────────────────────────────────────
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState({ lastPongAt: Date.now() - 11_000 });  // > DEAD_PONG_MS (10s)
    mesh._peers.set(PEER, st);
    const r = mesh._staleCheckTick(st);
    check('pong-timeout returns evicted',        r === 'evicted');
    check('peer removed from _peers',            !mesh._peers.has(PEER));
    check('onPeerLost fired with peerId',        lost.length === 1 && lost[0] === PEER);
  }

  // ── stale (3s<gap<10s) → marked stale, NOT evicted ────────────────
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState({ lastPongAt: Date.now() - 4_000 });
    mesh._peers.set(PEER, st);
    const r = mesh._staleCheckTick(st);
    check('stale gap returns stale',             r === 'stale');
    check('state flipped to stale',              st.state === 'stale');
    check('peer NOT evicted while merely stale',  mesh._peers.has(PEER) && lost.length === 0);
  }

  // ── recovery: a fresh pong while stale → back to open ─────────────
  {
    const mesh = newMesh();
    const st = fakeState({ state: 'stale', lastPongAt: Date.now() });
    mesh._peers.set(PEER, st);
    const r = mesh._staleCheckTick(st);
    check('fresh pong returns recovered',        r === 'recovered');
    check('state flipped back to open',          st.state === 'open');
  }

  // ── healthy ping send → sent, failure streak cleared ──────────────
  {
    const mesh = newMesh();
    const st = fakeState({ sendFailures: 2 });
    mesh._peers.set(PEER, st);
    const r = mesh._pingTick(st);
    check('healthy ping returns sent',           r === 'sent');
    check('sendFailures reset to 0 on success',  st.sendFailures === 0);
  }

  // ── throwing send (dc lies 'open') → evict after SEND_FAIL_LIMIT ──
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState({
      dc: { readyState: 'open', send() { throw new Error('InvalidStateError'); }, close() {} },
    });
    mesh._peers.set(PEER, st);
    check('1st throw → fail (not yet evicted)',  mesh._pingTick(st) === 'fail');
    check('2nd throw → fail (not yet evicted)',  mesh._pingTick(st) === 'fail');
    const r3 = mesh._pingTick(st);
    check('3rd throw → evicted',                 r3 === 'evicted');
    check('peer removed after send-fail limit',  !mesh._peers.has(PEER));
    check('onPeerLost fired on send-fail evict',  lost.length === 1 && lost[0] === PEER);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
