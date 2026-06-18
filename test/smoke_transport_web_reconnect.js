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
    queueMicrotask(() => { this.readyState = 1; this._fire('open'); });
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
    if (set) for (const h of set) try { h(ev); } catch {}
  }
  deliver(obj) { this._fire('message', { data: JSON.stringify(obj) }); }
}

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

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
