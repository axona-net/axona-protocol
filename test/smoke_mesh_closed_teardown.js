// =====================================================================
// smoke_mesh_closed_teardown.js — a PC that reaches connectionState
//   'closed' out from under us must FREE its map slot.
//
// Regression guard for the bridgeless-reconnect wedge: before this, the
// 'closed' branch of onconnectionstatechange only set state.state='closed'
// and left the entry in _peers. That kept hasPeer(peer) === true forever,
// so connectViaRelay()'s idempotency guard (ownsPeer || isConnected ||
// hasPeer) no-op'd permanently — a peer whose relayed channel dropped to
// 'closed' could never re-establish a direct connection without the bridge.
//
// Contract (see MeshManager._onConnState):
//   · a LIVE entry reaching 'closed' is torn down → hasPeer → false (so a
//     fresh connectViaRelay/discovery can re-drive), and onPeerLost fires
//     IFF the channel had ever opened (Transport death semantics).
//   · 'failed' is NOT terminal here — it schedules a retry, entry stays.
//   · idempotent: a stale closure firing 'closed' after the entry was
//     replaced by a fresh negotiation must NOT tear down the new entry.
//
// ALSO (4.76.3): the close REASON computed at every _retire call site must
//   reach the onPeerLost listener and, through the transport, the peer-died
//   event — so eviction churn is attributable (WHY a peer died), not a
//   nameless death in the logs. Before 4.76.3 _retire fired cb(peerId) with
//   the reason dropped; grepping prod logs by nodeId found only
//   peer-died-evicted, never a pong-timeout/send-failed/pc-closed cause.
//
// Run: node test/smoke_mesh_closed_teardown.js
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';
import { WebRTCTransport } from '../src/transport/web/webrtc.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PEER = 'cc' + 'c3'.repeat(32);

function newMesh() { return new MeshManager({ sendSignal: () => {}, log: () => {} }); }
function fakeState(over = {}) {
  return {
    peerId: PEER, state: 'open', role: 'offerer',
    openedAt: Date.now() - 30_000,            // was genuinely open
    lastPongAt: Date.now(), pings: 5, pongs: 5, rttBuffer: [], sendFailures: 0,
    dc: { readyState: 'open', send() {}, close() {} },
    pc: { close() {} },
    ...over,
  };
}

function main() {
  console.log('MeshManager closed-PC teardown (bridgeless re-drive guard)\n');

  // ── live 'closed' entry that WAS open → torn down + onPeerLost ──────
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
    const st = fakeState();
    mesh._peers.set(PEER, st);
    check('precondition: hasPeer true while live', mesh.hasPeer(PEER));
    mesh._onConnState(st, 'closed');
    check('state marked closed',              st.state === 'closed');
    check('entry removed from _peers',        !mesh._peers.has(PEER));
    check('hasPeer → false (connectViaRelay guard clears)', !mesh.hasPeer(PEER));
    check('onPeerLost fired (channel had opened)', lost.length === 1 && lost[0].id === PEER);
  }

  // ── live 'closed' entry that NEVER opened → slot freed, no peer-death
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
    const st = fakeState({ openedAt: 0 });    // negotiation died before opening
    mesh._peers.set(PEER, st);
    mesh._onConnState(st, 'closed');
    check('never-opened entry removed from _peers', !mesh._peers.has(PEER));
    check('hasPeer → false after never-opened close', !mesh.hasPeer(PEER));
    check('onPeerLost NOT fired (no one was using it)', lost.length === 0);
  }

  // ── idempotency: stale closure must not tear down a REPLACED entry ──
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
    const stale = fakeState();
    const fresh = fakeState();                // a new negotiation for the same peer
    mesh._peers.set(PEER, fresh);             // fresh is the live entry now
    mesh._onConnState(stale, 'closed');       // stale PC fires late
    check('fresh entry NOT torn down by stale closure', mesh._peers.get(PEER) === fresh);
    check('hasPeer stays true (fresh negotiation intact)', mesh.hasPeer(PEER));
    check('no spurious onPeerLost from stale closure', lost.length === 0);
  }

  // ── 'failed' is not terminal: entry stays (retry path) ─────────────
  {
    const mesh = newMesh();
    const st = fakeState();
    mesh._peers.set(PEER, st);
    mesh._onConnState(st, 'failed');
    check('failed marks state',               st.state === 'failed');
    check('failed keeps entry for retry',     mesh._peers.has(PEER));
  }

  // ── 4.76.3: the close REASON reaches the onPeerLost listener ────────
  // One case per _retire path so a future edit that drops the reason at any
  // one site is caught. Each uses a fresh mesh + a genuinely-open state so
  // onPeerLost fires (wasOpen).
  {
    // pc-closed — remote/abrupt PC close via _onConnState
    {
      const mesh = newMesh();
      const lost = [];
      mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
      const st = fakeState();
      mesh._peers.set(PEER, st);
      mesh._onConnState(st, 'closed');
      check("reason 'pc-closed' propagates", lost.length === 1 && lost[0].reason === 'pc-closed');
    }
    // pong-timeout — heartbeat death via the reaper
    {
      const mesh = newMesh();
      const lost = [];
      mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
      const st = fakeState({ lastPongAt: Date.now() - 60_000 });   // pongs long stopped
      mesh._peers.set(PEER, st);
      const verdict = mesh._reapTick(st);
      check("reaper pong-timeout verdict",   verdict === 'reaped-pong');
      check("reason 'pong-timeout' propagates", lost.length === 1 && lost[0].reason === 'pong-timeout');
    }
    // send-failed — a throwing send crossed the streak limit
    {
      const mesh = newMesh();
      const lost = [];
      mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
      const st = fakeState({ sendFailures: 3 });                   // >= SEND_FAIL_LIMIT
      mesh._peers.set(PEER, st);
      const verdict = mesh._reapTick(st);
      check("reaper send-failed verdict",    verdict === 'reaped-send');
      check("reason 'send-failed' propagates", lost.length === 1 && lost[0].reason === 'send-failed');
    }
    // peer-left — the bridge told us the peer departed. onPeerLeft KEEPS a
    // still-open channel (#374), so a real peer-left eviction is a channel
    // that WAS open and has since gone stale; wasOpen stays true → fires.
    {
      const mesh = newMesh();
      const lost = [];
      mesh.onPeerLost((id, reason) => lost.push({ id, reason }));
      const st = fakeState({ state: 'stale' });   // was open (openedAt in past), now stale
      mesh._peers.set(PEER, st);
      mesh.onPeerLeft(PEER);
      check("reason 'peer-left' propagates", lost.length === 1 && lost[0].reason === 'peer-left');
    }
  }

  // ── 4.76.3: the reason survives the transport hop to peer-died ──────
  // WebRTCTransport._onPeerLost(meshId, reason) → peerDied handler
  // h(nodeId, reason). This is the hop AxonaPeer consumes to log
  // peer-died-evicted { peer, reason }.
  {
    let lostCb = null;
    const stubMesh = {
      onMessage: () => () => {},
      onPeerLost: (cb) => { lostCb = cb; return () => {}; },
      isConnected: () => true,
    };
    const NODE = 0x89n << 248n | 0x1234n;
    const MESH_ID = 'm42';
    const transport = new WebRTCTransport({ mesh: stubMesh, localNodeId: 0x89n << 248n });
    transport.start();
    transport.bindPeer(NODE, MESH_ID);
    const died = [];
    transport.onPeerDied((nodeId, reason) => died.push({ nodeId, reason }));
    check('transport subscribed to mesh onPeerLost', typeof lostCb === 'function');
    lostCb(MESH_ID, 'pong-timeout');          // mesh reports the death with a cause
    check('peer-died fired for the bound node', died.length === 1 && died[0].nodeId === NODE);
    check('reason survives the transport hop', died.length === 1 && died[0].reason === 'pong-timeout');
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
