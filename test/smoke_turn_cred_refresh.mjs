// =====================================================================
// smoke_turn_cred_refresh.mjs — a node refreshes its TURN credential
// before the 2h TTL lapses, so a long-lived or graduated node never
// strands itself with an expired credential (prod 2026-08-06: coturn's
// log was ~100% "Cannot find credentials"; roots logged
// replicate-all-failed).
//
// Design after council review (Orion, Aster) of the first cut:
//   - HELD-OPEN node: refresh IN-BAND over the live socket (send
//     `turn-refresh`, apply the bridge's `turn` reply). If the bridge does
//     not answer, RETRY in-band up to a cap, then defer gracefully. The
//     socket is NEVER closed for a refresh — a close drops in-flight bridge
//     RPCs (Aster's catch). A send error RE-ARMS with backoff, never a
//     silent give-up.
//   - GRADUATED / disconnected node: re-dial (no live socket, no in-flight
//     work to lose) — heals the backbone relays behind the flood.
//
// Scenarios:
//   A  graduated node re-dials before expiry, mesh untouched
//   B  held-open node refreshes IN-BAND: sends turn-refresh, applies the
//      `turn` reply, socket NOT closed, no new socket, re-arms
//   C  held-open, bridge never replies → RETRIES to the cap, then defers:
//      socket stays OPEN, no close, no new socket (the fixed fallback)
//   D  credless welcome arms no timer; far-future expiry does not fire early
//   E  strict expiry parse rejects a partial-numeric prefix
//   F  send error re-arms (retries) instead of giving up silently
//
// Timings are shrunk via constructor opts so the retry cap is exercised in
// ~2s instead of minutes.
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
    this.failNextSends = 0;      // >0 → the next N send() calls throw (wedged-socket sim)
    liveSockets.push(this);
    queueMicrotask(() => { this.readyState = 1; this._fire('open'); });
  }
  addEventListener(type, h) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(h);
  }
  send(data) {
    if (this.readyState !== 1) throw new Error('socket not open');
    if (this.failNextSends > 0) { this.failNextSends--; throw new Error('simulated send failure'); }
    this.sent.push(data);
  }
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
  sentOfType(t) { return this.sent.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(m => m && m.type === t); }
}

let bridgeIdent;

function turnCred(ttlSec) {
  const expiry = Math.floor(Date.now() / 1000) + ttlSec;
  return { urls: ['turn:turn.example:3478'], username: `${expiry}:tok`, credential: 'x', ttlSeconds: ttlSec };
}
function feedWelcome(sock, turn) {
  sock.deliver({ type: 'welcome', connId: CONN_ID, serverNonce: SERVER_NONCE,
                 version: '4.60.1', kernelVersion: '4.60.1', turn });
}
async function feedBridgeHello(sock) {
  const cbv   = cbvFromNonces(SERVER_NONCE, CONN_ID, 'bridge');
  const hello = await buildAuthHello({ identity: bridgeIdent, cbv });
  sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: hello } });
}
async function connectOpen(identity, turn, extra = {}) {
  const t = webTransport({
    bridgeUrl: 'wss://test.example', identity, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 20, reconnectMaxMs: 20,
    graduationMeshFloor: 3, graduationRecheckMs: 100000,
    ...extra,
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
  console.log('webTransport — TURN credential refresh before TTL lapse (in-band, retry-not-close)\n');
  const alice = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  bridgeIdent = await createNodeIdentity({ lat: 51.5, lng: -0.12 });

  // A cred "expiring in 5s" collapses fireIn to the 1s floor (safety default 5m),
  // so the first refresh fires ~1s after welcome in every scenario.

  // ── A. Graduated node re-dials before expiry, mesh untouched ────────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5));
    check('A: open on first connect', t.bridgeState === 'open');
    t.webrtc.boundPeers = () => [1n, 2n, 3n];
    const nAtGrad = liveSockets.length;
    sock.close(4200, 'graduated');
    check('A: graduated (no socket) after 4200', t.bridgeState === 'graduated');
    await sleep(1300);
    check('A: re-dialed before expiry despite a healthy mesh', liveSockets.length === nAtGrad + 1);
    const s2 = t.socket;
    const reFrame = s2 && s2.sent[0] ? JSON.parse(s2.sent[0]) : null;
    check('A: the re-dial re-sent client-hello', reFrame && reFrame.type === 'client-hello');
    feedWelcome(s2, turnCred(5)); await feedBridgeHello(s2); await sleep(5);
    check('A: back to open with a fresh credential', t.bridgeState === 'open');
    await t.stop();
  }

  // ── B. Held-open node refreshes IN-BAND (no close, no new socket) ───
  // Long reply window so no retry fires before we deliver the reply.
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5), { turnRefreshReplyMs: 10000 });
    check('B: open on first connect', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    await sleep(1300);
    check('B: sent exactly one turn-refresh in-band', sock.sentOfType('turn-refresh').length === 1);
    check('B: did NOT close the healthy socket', sock.readyState === 1);
    check('B: did NOT open a new socket', liveSockets.length === nBefore);
    sock.deliver({ type: 'turn', turn: turnCred(5), serverT: Date.now() });
    await sleep(1300);
    check('B: re-armed — a second turn-refresh fired after applying the reply',
      sock.sentOfType('turn-refresh').length === 2 && sock.readyState === 1);
    await t.stop();
  }

  // ── C. Held-open, no reply → RETRIES to the cap, never closes ───────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5),
      { turnRefreshReplyMs: 150, turnRefreshMaxTries: 3 });
    check('C: open on first connect', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    // fire ~1s, then retries at +150ms each up to 3 total, then defer.
    await sleep(1900);
    check('C: retried in-band up to the cap (3 sends)', sock.sentOfType('turn-refresh').length === 3);
    check('C: socket STAYS OPEN — never closed for a refresh', sock.readyState === 1);
    check('C: no new socket (no re-dial, no teardown)', liveSockets.length === nBefore);
    await t.stop();
  }

  // ── D. Credless welcome / far-future expiry: no premature action ────
  liveSockets = [];
  {
    const { t } = await connectOpen(alice, null);
    check('D: open on a credless welcome', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    const { t: t2, sock: s2 } = await connectOpen(alice, turnCred(3600));  // 1h out
    check('D: open with a 1h credential', t2.bridgeState === 'open');
    await sleep(1300);
    check('D: credless deploy unchanged — no re-dial', liveSockets.length === nBefore + 1);
    check('D: no premature refresh for a far-future expiry', s2.sentOfType('turn-refresh').length === 0);
    await t.stop(); await t2.stop();
  }

  // ── E. Strict expiry parse rejects a partial-numeric prefix ─────────
  liveSockets = [];
  {
    const bad = { urls: ['turn:x:3478'], username: '12ab:tok', credential: 'x' };
    const { t, sock } = await connectOpen(alice, bad);
    check('E: open with a malformed-expiry credential', t.bridgeState === 'open');
    await sleep(1300);
    check('E: strict parse rejected it — no refresh armed', sock.sentOfType('turn-refresh').length === 0);
    check('E: and no re-dial', liveSockets.length === 1);
    await t.stop();
  }

  // ── F. Send error RE-ARMS (retries) instead of silent give-up ───────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5),
      { turnRefreshReplyMs: 10000, turnRefreshSendErrBackoffMs: 150 });
    check('F: open on first connect', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    sock.failNextSends = 1;   // the first turn-refresh send throws
    // fire ~1s (send throws, nothing recorded) → re-arm at +150ms → send succeeds.
    await sleep(1500);
    check('F: re-armed after send error — a turn-refresh eventually landed',
      sock.sentOfType('turn-refresh').length === 1);
    check('F: send error did not close or re-dial', sock.readyState === 1 && liveSockets.length === nBefore);
    await t.stop();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
