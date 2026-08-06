// =====================================================================
// smoke_turn_cred_refresh.mjs — a node re-dials the bridge before its
// TURN credential's TTL lapses, so a long-lived or graduated node never
// strands itself with an expired credential.
//
// Prod 2026-08-06: TURN creds are minted with a fixed TTL (2h) and handed
// over ONLY in the welcome frame; the sole refresh path was a (re)connect,
// and the only reconnect trigger was meshed-peer-count. A node that stayed
// meshed past the TTL held an EXPIRED credential — new relay allocations were
// refused by coturn ("Cannot find credentials"), and roots logged
// pubsub:replicate-all-failed. The fix drives a refresh from the credential's
// own expiry, parsed from the REST username's "<expiry-unix-seconds>:" prefix.
//
// Four scenarios:
//   A  graduated node (no socket) re-dials before expiry, mesh untouched
//   B  held-open node closes+reconnects before expiry
//   C  a welcome with NO turn cred arms no timer (credless deploys unchanged)
//   D  a far-future expiry does NOT fire soon — refresh is expiry-driven, not
//      a blind interval
//
// Run: node test/smoke_turn_cred_refresh.mjs
// =====================================================================

import { webTransport }        from '../src/transport/web/index.js';
import { createNodeIdentity }  from '../src/identity/index.js';
import { buildAuthHello, cbvFromNonces } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const SERVER_NONCE = 'beefcafe'.repeat(4);
const CONN_ID      = 'zz';

let liveSockets = [];
class FakeWS {
  constructor(url) {
    this.url = url; this.sent = []; this._listeners = new Map(); this.readyState = 0;
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
    if (type === 'error' && (!set || set.size === 0)) throw ev.error || new Error('socket error');
    if (set) for (const h of set) try { h(ev); } catch {}
  }
  deliver(obj) { this._fire('message', { data: JSON.stringify(obj) }); }
}

let bridgeIdent;

// A bridge-shaped TURN credential expiring `ttlSec` from now. username is
// "<expiry-unix-seconds>:<token>" exactly as makeTurnCredential mints it.
function turnCred(ttlSec) {
  const expiry = Math.floor(Date.now() / 1000) + ttlSec;
  return { urls: ['turn:turn.example:3478'], username: `${expiry}:tok`, credential: 'x', ttlSeconds: ttlSec };
}
function feedWelcome(sock, turn) {
  sock.deliver({ type: 'welcome', connId: CONN_ID, serverNonce: SERVER_NONCE,
                 version: '4.59.2', kernelVersion: '4.59.2', turn });
}
async function feedBridgeHello(sock) {
  const cbv   = cbvFromNonces(SERVER_NONCE, CONN_ID, 'bridge');
  const hello = await buildAuthHello({ identity: bridgeIdent, cbv });
  sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: hello } });
}
async function connectOpen(opts, turn) {
  const t = webTransport({
    bridgeUrl: 'wss://test.example', identity: opts.identity, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 20, reconnectMaxMs: 20,
    graduationMeshFloor: 3,
    // Park the graduation watchdog far out so any re-dial we observe within a
    // second is unambiguously the TURN-refresh timer, not the mesh-thin watch.
    graduationRecheckMs: 100000,
    ...opts.extra,
  });
  const startP = t.start();
  await sleep(5);
  const sock = t.socket;
  feedWelcome(sock, turn);
  await feedBridgeHello(sock);
  await startP;
  return { t, sock };
}

async function main() {
  console.log('webTransport — TURN credential refresh before TTL lapse\n');
  const alice = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  bridgeIdent = await createNodeIdentity({ lat: 51.5, lng: -0.12 });

  // Refresh safety margin is 5 min; any expiry within that window collapses to
  // the 1s floor, so a cred "expiring in 5s" fires the refresh at ~1s. That is
  // what makes these scenarios fast without touching the timer constant.

  // ── A. Graduated node re-dials before expiry, mesh untouched ────────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen({ identity: alice }, turnCred(5));
    check('A: open on first connect', t.bridgeState === 'open');
    t.webrtc.boundPeers = () => [1n, 2n, 3n];       // meshed with 3 peers
    const nAtGrad = liveSockets.length;
    sock.close(4200, 'graduated');
    check('A: graduated (no socket) after 4200', t.bridgeState === 'graduated');
    // Mesh stays at 3 (never thins) and graduationRecheckMs is 100s, so the ONLY
    // thing that can re-dial within ~1.3s is the credential-refresh timer.
    await sleep(1300);
    check('A: re-dialed before expiry despite a healthy mesh',
      liveSockets.length === nAtGrad + 1);
    const s2 = t.socket;
    const reFrame = s2 && s2.sent[0] ? JSON.parse(s2.sent[0]) : null;
    check('A: the re-dial re-sent client-hello (fresh handshake)',
      reFrame && reFrame.type === 'client-hello');
    feedWelcome(s2, turnCred(5)); await feedBridgeHello(s2); await sleep(5);
    check('A: back to open with a fresh credential', t.bridgeState === 'open');
    await t.stop();
  }

  // ── B. Held-open node closes+reconnects before expiry ───────────────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen({ identity: alice }, turnCred(5));
    check('B: open on first connect', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    // No graduation — socket stays open. The refresh timer must close it so the
    // reconnect path re-welcomes with a fresh credential.
    await sleep(1300);
    check('B: held-open socket was refreshed (new socket opened)',
      liveSockets.length === nBefore + 1);
    check('B: the original socket was closed for the refresh', sock.readyState === 3);
    const s2 = t.socket;
    feedWelcome(s2, turnCred(5)); await feedBridgeHello(s2); await sleep(5);
    check('B: back to open after refresh', t.bridgeState === 'open');
    await t.stop();
  }

  // ── C. No TURN cred in welcome → no refresh timer armed ─────────────
  liveSockets = [];
  {
    const { t } = await connectOpen({ identity: alice }, null);
    check('C: open on a credless welcome', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    await sleep(1300);
    check('C: credless deploy is unchanged — no re-dial', liveSockets.length === nBefore);
    await t.stop();
  }

  // ── D. Far-future expiry does NOT fire soon (expiry-driven, not blind) ─
  liveSockets = [];
  {
    const { t } = await connectOpen({ identity: alice }, turnCred(3600));  // 1h out
    check('D: open with a 1h credential', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    await sleep(1300);
    check('D: no premature re-dial for a far-future expiry',
      liveSockets.length === nBefore);
    await t.stop();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
