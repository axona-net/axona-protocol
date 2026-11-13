// =====================================================================
// smoke_transport_web_reconnect.js — webTransport() connection
// management: welcome capture, ping/pong RTT, bridge-state events,
// auto-reconnect with re-handshake, and 4426 upgrade-required.
//
// axona/4 — the bridge handshake is authenticated: welcome seeds a
// per-connection serverNonce, and the bridge proves its nodeId with an
// Ed25519 signature over the CBV.  This smoke uses a real bridge
// identity and signs each (re)handshake.
//
// Run: node test/smoke_transport_web_reconnect.js
// =====================================================================

import { webTransport } from '../src/transport/web/index.js';
import { fromHex }      from '../src/utils/hexid.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { buildAuthHello, cbvFromNonces } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Fixed per-connection bridge channel-binding inputs for the test.
const SERVER_NONCE = 'beefcafe'.repeat(4);
const CONN_ID      = 'zz';

// ── Fake WebSocket with instance tracking + close codes ─────────────
let liveSockets = [];
class FakeWS {
  constructor(url) {
    this.url = url;
    this.sent = [];
    this._listeners = new Map();
    this.readyState = 0;
    liveSockets.push(this);
    // failNextUpgrades emulates Node `ws` on a failed HTTP upgrade (a proxy
    // answering 502 while the backend boots): 'error' then 'close', never
    // 'open'. _fire replicates EventEmitter semantics — an 'error' with NO
    // listener THROWS; that throw is exactly what escaped webTransport as an
    // uncaughtException, killed the reconnect chain before 'close' fired, and
    // wedged all nine prod relays on 2026-07-09.
    if (FakeWS.failNextUpgrades > 0) {
      FakeWS.failNextUpgrades--;
      queueMicrotask(() => {
        this.readyState = 3;
        this._fire('error', { message: 'Unexpected server response: 502',
                              error: new Error('Unexpected server response: 502') });
        this._fire('close', { code: 1006 });
      });
    } else {
      queueMicrotask(() => { this.readyState = 1; this._fire('open'); });
    }
  }
  addEventListener(type, h) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(h);
  }
  send(data) { if (this.readyState !== 1) throw new Error('socket not open'); this.sent.push(data); }
  close(code, reason) {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this._fire('close', { code, reason });
  }
  _fire(type, ev = {}) {
    const set = this._listeners.get(type);
    if (type === 'error' && (!set || set.size === 0)) {
      throw ev.error || new Error(ev.message || 'unhandled socket error');
    }
    if (set) for (const h of set) try { h(ev); } catch {}
  }
  deliver(obj) { this._fire('message', { data: JSON.stringify(obj) }); }
}
FakeWS.failNextUpgrades = 0;

let bridgeIdent;   // the bridge's real identity (set in main)

// welcome seeds the serverNonce + connId (the CBV) BEFORE the hello.
function feedWelcome(sock) {
  sock.deliver({ type: 'welcome', connId: CONN_ID, serverNonce: SERVER_NONCE,
                 version: '2.2.0', kernelVersion: '2.1.0', turn: null });
}

// Authenticated bridge hello, signed over the per-connection CBV.
async function feedBridgeHello(sock) {
  const cbv   = cbvFromNonces(SERVER_NONCE, CONN_ID, 'bridge');
  const hello = await buildAuthHello({ identity: bridgeIdent, cbv });
  sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: hello } });
}

async function main() {
  console.log('webTransport — reconnect + welcome/RTT/state (axona/4 auth)\n');

  const alice = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  bridgeIdent = await createNodeIdentity({ lat: 51.5,  lng: -0.12 });
  const BRIDGE_BIG = fromHex(bridgeIdent.id);

  liveSockets = [];
  const states = [];
  const welcomes = [];
  const t = webTransport({
    bridgeUrl: 'wss://test.example',
    identity:  alice,
    WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000,
    reconnectInitialMs: 60,
    reconnectMaxMs:     60,
  });
  t.onBridgeState((s) => states.push(s));
  t.onWelcome((w) => welcomes.push(w));

  // ── First connection ──────────────────────────────────────────────
  const startP = t.start();
  await sleep(5);
  const sock1 = t.socket;
  check('first socket created', !!sock1 && liveSockets.length === 1);
  const firstFrame = sock1.sent[0] ? JSON.parse(sock1.sent[0]) : null;
  check('client-hello sent first on open', firstFrame && firstFrame.type === 'client-hello');
  check('state went connecting', states.includes('connecting'));

  feedWelcome(sock1);
  check('bridgeInfo captured version',
    t.bridgeInfo && t.bridgeInfo.version === '2.2.0' && t.bridgeInfo.kernelVersion === '2.1.0');
  check('bridgeInfo connId', t.bridgeInfo.connId === CONN_ID);
  check('onWelcome fired', welcomes.length >= 1 && welcomes.at(-1).version === '2.2.0');

  await feedBridgeHello(sock1);
  await startP;
  check('start() resolves after authenticated hello', true);
  check('state went open', states.includes('open'));
  check('bridge bound to proven id', t.bridge.ownsPeer(BRIDGE_BIG) === true);

  // ── RTT from pong ─────────────────────────────────────────────────
  sock1.deliver({ type: 'pong', t: Date.now() - 40 });
  check('bridgeRtt recorded (>=40ms)', typeof t.bridgeRtt === 'number' && t.bridgeRtt >= 40);
  check('bridgeRttAvg set', typeof t.bridgeRttAvg === 'number' && t.bridgeRttAvg >= 40);

  // ── Reconnect on socket drop ──────────────────────────────────────
  const socketsBefore = liveSockets.length;
  sock1.close();                        // no code → reconnect path
  check('state went disconnected on close', states.includes('disconnected'));
  await sleep(120);
  check('a NEW socket was opened by reconnect', liveSockets.length === socketsBefore + 1);
  const sock2 = t.socket;
  check('current socket is the fresh instance', sock2 && sock2 !== sock1);
  const reFrame = sock2.sent[0] ? JSON.parse(sock2.sent[0]) : null;
  check('reconnect re-sent client-hello', reFrame && reFrame.type === 'client-hello');

  // Re-handshake on the new socket (welcome reseeds the CBV first).
  feedWelcome(sock2);
  await feedBridgeHello(sock2);
  await sleep(5);
  check('state returned to open after re-handshake', t.bridgeState === 'open');
  check('bridge re-bound after reconnect', t.bridge.ownsPeer(BRIDGE_BIG) === true);

  await t.stop();
  check('state disconnected after stop', t.bridgeState === 'disconnected');

  // ── 4426 upgrade-required: no reconnect ───────────────────────────
  liveSockets = [];
  const t2 = webTransport({
    bridgeUrl: 'wss://test.example',
    identity:  alice,
    WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000,
    reconnectInitialMs: 30,
    reconnectMaxMs:     30,
  });
  const startP2 = t2.start();
  await sleep(5);
  const us = t2.socket;
  feedWelcome(us);
  await feedBridgeHello(us);
  await startP2;
  const countAtOpen = liveSockets.length;
  us.close(4426, 'client out of date');
  check('4426 → upgrade-required state', t2.bridgeState === 'upgrade-required');
  check('4426 → upgradeReason surfaced',
    typeof t2.upgradeReason === 'string' && t2.upgradeReason.length > 0);
  await sleep(120);
  check('4426 → NO reconnect (no new socket)', liveSockets.length === countAtOpen);
  await t2.stop();

  // ── Failed HTTP upgrade mid-reconnect (the 2026-07-09 prod wedge) ──
  // Bridge restarts; the relay's reconnect attempts hit a 502 from the proxy
  // while the backend boots. The transport must survive the 'error' event
  // (pre-fix: unlistened → THROWS → chain dead) and keep backing off until
  // an attempt succeeds.
  liveSockets = [];
  const t3 = webTransport({
    bridgeUrl: 'wss://test.example',
    identity:  alice,
    WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000,
    reconnectInitialMs: 30,
    reconnectMaxMs:     30,
  });
  const startP3 = t3.start();
  await sleep(5);
  const s3 = t3.socket;
  feedWelcome(s3);
  await feedBridgeHello(s3);
  await startP3;
  check('t3 connected (baseline)', t3.bridgeState === 'open');

  FakeWS.failNextUpgrades = 3;          // next 3 attempts 502 like a booting proxy
  const socketsAtDrop = liveSockets.length;
  s3.close();                           // bridge restart drops the socket
  await sleep(400);                     // 3 failed attempts × ~30ms backoff + slack
  check('survived 3 failed upgrades (no crash) and kept retrying',
    liveSockets.length >= socketsAtDrop + 4);   // 3 failures + the succeeding attempt
  const s4 = t3.socket;
  check('a live socket exists after the failures', !!s4 && s4.readyState === 1);
  feedWelcome(s4);
  await feedBridgeHello(s4);
  await sleep(5);
  check('re-handshake completed after the 502 storm', t3.bridgeState === 'open');
  await t3.stop();

  // ── 4200 graduation: meshed → NO reconnect; thinned → re-dial ─────
  // Bootstrap-nursery: the bridge releases an established peer with code 4200.
  // A meshed client keeps its mesh and stops reconnecting; a watchdog re-dials
  // only if its bound-peer count later drops below the floor.
  liveSockets = [];
  const t4 = webTransport({
    bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 30, reconnectMaxMs: 30,
    graduationMeshFloor: 3, graduationRecheckMs: 40,
  });
  const startP4 = t4.start();
  await sleep(5);
  const g1 = t4.socket;
  feedWelcome(g1); await feedBridgeHello(g1); await startP4;
  t4.webrtc.boundPeers = () => [1n, 2n, 3n];   // pretend: meshed with 3 peers
  const nAtGrad = liveSockets.length;
  g1.close(4200, 'graduated');
  check('4200 (meshed) → graduated state', t4.bridgeState === 'graduated');
  await sleep(130);
  check('4200 (meshed) → NO reconnect while meshed', liveSockets.length === nAtGrad);
  t4.webrtc.boundPeers = () => [1n];           // mesh thins below the floor
  await sleep(130);
  check('4200 → watchdog re-dials when mesh thins', liveSockets.length >= nAtGrad + 1);
  await t4.stop();

  // Graduated while NOT actually meshed → immediate reconnect (self-correct).
  liveSockets = [];
  const t5 = webTransport({
    bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 30, reconnectMaxMs: 30,
    graduationMeshFloor: 3, graduationRecheckMs: 40,
  });
  const startP5 = t5.start();
  await sleep(5);
  const h1 = t5.socket;
  feedWelcome(h1); await feedBridgeHello(h1); await startP5;
  t5.webrtc.boundPeers = () => [];             // not actually meshed
  const nAtGrad5 = liveSockets.length;
  h1.close(4200, 'graduated');
  await sleep(120);
  check('4200 (not meshed) → reconnects (self-correct)', liveSockets.length >= nAtGrad5 + 1);
  check('4200 (not meshed) → not stuck graduated', t5.bridgeState !== 'graduated');
  await t5.stop();

  // ── Close a Retired Socket (4.136.0, David 2026-11-12) ───────────────
  // A bridge closes the socket (4200) as soon as the WebRTC channel to it has
  // replaced it, whatever the mesh size. The client takes that as a release
  // when its channel to THAT bridge is bound, and re-dials only when that
  // channel is gone AND the mesh is thin.
  const mk = () => webTransport({
    bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 30, reconnectMaxMs: 30,
    graduationMeshFloor: 3, graduationRecheckMs: 40,
  });
  const up = async (t) => { const st = t.start(); await sleep(5); const s = t.socket; feedWelcome(s); await feedBridgeHello(s); await st; return s; };

  // R1/R2: a THIN client (mesh = the bridge alone) is released, stays released
  // while the channel lives, and re-dials when the channel dies.
  liveSockets = [];
  const t6 = mk(); const r1 = await up(t6);
  t6.webrtc.boundPeers = () => [BRIDGE_BIG];
  const n6 = liveSockets.length;
  r1.close(4200, 'bootstrap complete');
  check('R1 4200 with the bridge channel bound and mesh 1 → released (graduated)', t6.bridgeState === 'graduated');
  await sleep(130);
  check('R1 no reconnect while the bridge channel lives', liveSockets.length === n6, `sockets ${liveSockets.length} vs ${n6}`);
  t6.webrtc.boundPeers = () => [];
  await sleep(130);
  check('R2 the bridge channel dies with a thin mesh → re-dial', liveSockets.length >= n6 + 1);
  await t6.stop();

  // R3: released, then the bridge channel dies while two OTHER peers remain
  // (mesh 2, under the floor) → re-dial.
  liveSockets = [];
  const t7 = mk(); const r3 = await up(t7);
  t7.webrtc.boundPeers = () => [BRIDGE_BIG, 1n];
  const n7 = liveSockets.length;
  r3.close(4200, 'bootstrap complete');
  await sleep(100);
  check('R3 released with mesh 2 including the bridge → no reconnect', t7.bridgeState === 'graduated' && liveSockets.length === n7);
  t7.webrtc.boundPeers = () => [1n, 2n];
  await sleep(130);
  check('R3 bridge channel gone, mesh 2 → re-dial', liveSockets.length >= n7 + 1);
  await t7.stop();

  // R4: 4200 with a thin mesh that does NOT include the bridge → reconnects at
  // once, as before (the old self-correcting branch is untouched).
  liveSockets = [];
  const t8 = mk(); const r4 = await up(t8);
  t8.webrtc.boundPeers = () => [1n];
  const n8 = liveSockets.length;
  r4.close(4200, 'graduated');
  await sleep(120);
  check('R4 4200, mesh 1 without the bridge → reconnects, not released', liveSockets.length >= n8 + 1 && t8.bridgeState !== 'graduated');
  await t8.stop();

  // R5: released, the bridge channel dies, three other peers remain → no
  // re-dial (the mesh is at the floor; this is the plain graduation rule).
  liveSockets = [];
  const t9 = mk(); const r5 = await up(t9);
  t9.webrtc.boundPeers = () => [BRIDGE_BIG, 1n, 2n, 3n];
  const n9 = liveSockets.length;
  r5.close(4200, 'bootstrap complete');
  t9.webrtc.boundPeers = () => [1n, 2n, 3n];
  await sleep(130);
  check('R5 bridge channel gone, mesh 3 → no re-dial', liveSockets.length === n9 && t9.bridgeState === 'graduated');
  await t9.stop();

  // R6: a non-4200 close with the bridge channel bound still reconnects.
  liveSockets = [];
  const t10 = mk(); const r6 = await up(t10);
  t10.webrtc.boundPeers = () => [BRIDGE_BIG];
  const n10 = liveSockets.length;
  r6.close(1006, 'abnormal');
  await sleep(120);
  check('R6 abnormal close with the bridge channel bound → reconnects', liveSockets.length >= n10 + 1 && t10.bridgeState !== 'graduated');
  await t10.stop();

  // ── Two-sided release (4.137.0; Orion acbffcc6 point 1) ──────────────
  // The heartbeat says whether THIS node's channel to the bridge is bound, so
  // a bridge that still believes a dead channel up does not close the socket.
  liveSockets = [];
  const t11 = webTransport({
    bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 30, reconnectMaxMs: 30,
    graduationMeshFloor: 3, graduationRecheckMs: 40, pingIntervalMs: 15,
  });
  const r7 = await up(t11);
  const lastPing = () => { for (let i = r7.sent.length - 1; i >= 0; i--) { try { const f = JSON.parse(r7.sent[i]); if (f.type === 'ping') return f; } catch { /* not json */ } } return null; };
  t11.webrtc.boundPeers = () => [1n];
  await sleep(60);
  { const p = lastPing(); check('R7 no channel to the bridge → ping says bridgeBound false, with the mesh count', !!p && p.bridgeBound === false && p.meshBound === 1, JSON.stringify(p)); }
  t11.webrtc.boundPeers = () => [BRIDGE_BIG, 1n];
  await sleep(60);
  { const p = lastPing(); check('R8 channel to the bridge bound → ping says bridgeBound true', !!p && p.bridgeBound === true && p.meshBound === 2, JSON.stringify(p)); }
  t11.webrtc.boundPeers = () => [2n, 3n, 4n];
  await sleep(60);
  { const p = lastPing(); check('R9 a full mesh WITHOUT the bridge still says false (the count is not the answer)', !!p && p.bridgeBound === false && p.meshBound === 3, JSON.stringify(p)); }
  t11.webrtc.boundPeers = () => { throw new Error('boom'); };
  await sleep(60);
  { const p = lastPing(); check('R10 a throwing bound list reads false and 0, never a missing field', !!p && p.bridgeBound === false && p.meshBound === 0, JSON.stringify(p)); }
  await t11.stop();

  // ── The first peer-list reaches a handler that registers late (4.140.0) ──
  // connect() starts the transport, then the peer; the bridge's one peer-list
  // arrives in between. Mutants: no replay → R11, R12; replay with no age
  // bound → R13; replay to a handler that was present → R14; call a removed
  // handler → R15; replay only the handles, without the graduated ids → R11.
  const ID_A = '89' + 'a'.repeat(64), ID_B = '89' + 'b'.repeat(64);
  const mkPL = (extra = {}) => webTransport({
    bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 30, reconnectMaxMs: 30, ...extra,
  });
  liveSockets = [];
  { const t = mkPL(); const st = t.start(); await sleep(5); const s = t.socket; feedWelcome(s);
    const early = []; const offEarly = t.onPeerList((l) => early.push(l));
    s.deliver({ type: 'peer-list', peers: [], graduated: [ID_A, ID_B], serverT: Date.now() });   // no handles: this harness has no WebRTC for the mesh to dial
    await feedBridgeHello(s); await st;
    const late = []; t.onPeerList((l) => late.push(l));
    check('R11 nothing is delivered synchronously at registration', late.length === 0);
    await sleep(5);
    check('R11 a handler registered AFTER the list gets that list once, graduated ids included', late.length === 1 && late[0].length === 2 && late[0].includes(ID_A) && late[0].includes(ID_B), JSON.stringify(late));
    check('R14 a handler that was present when the list arrived is not called again', early.length === 1);
    const gone = []; const off = t.onPeerList((l) => gone.push(l)); off();
    await sleep(5);
    check('R15 a handler removed before the microtask is never called', gone.length === 0);
    late[0].push('mutated'); const late2 = []; t.onPeerList((l) => late2.push(l)); await sleep(5);
    check('R16 each handler gets its own copy', late2.length === 1 && !late2[0].includes('mutated'));
    s.deliver({ type: 'peer-list', peers: [], graduated: [ID_B], serverT: Date.now() });
    const late3 = []; t.onPeerList((l) => late3.push(l)); await sleep(5);
    check('R17 the NEWEST list is the one kept', late3.length === 1 && late3[0].length === 1 && late3[0][0] === ID_B, JSON.stringify(late3));
    offEarly(); await t.stop(); }
  { const t = mkPL(); const st = t.start(); await sleep(5); const s = t.socket; feedWelcome(s); await feedBridgeHello(s); await st;
    const none = []; t.onPeerList((l) => none.push(l)); await sleep(5);
    check('R12 with no list received, a late handler is not called', none.length === 0); await t.stop(); }
  { const t = mkPL({ peerListReplayMs: 20 }); const st = t.start(); await sleep(5); const s = t.socket; feedWelcome(s);
    s.deliver({ type: 'peer-list', peers: [], graduated: [ID_A], serverT: Date.now() });
    await feedBridgeHello(s); await st; await sleep(40);
    const old = []; t.onPeerList((l) => old.push(l)); await sleep(5);
    check('R13 a list older than the replay window is not handed to a late handler', old.length === 0); await t.stop(); }
  { const t = mkPL({ peerListReplayMs: 0 }); const st = t.start(); await sleep(5); const s = t.socket; feedWelcome(s);
    s.deliver({ type: 'peer-list', peers: [], graduated: [ID_A], serverT: Date.now() });
    await feedBridgeHello(s); await st;
    const off0 = []; t.onPeerList((l) => off0.push(l)); await sleep(5);
    check('R18 a window of 0 turns the replay off', off0.length === 0); await t.stop(); }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
