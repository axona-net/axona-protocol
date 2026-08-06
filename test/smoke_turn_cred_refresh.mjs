// =====================================================================
// smoke_turn_cred_refresh.mjs — a node refreshes its TURN credential
// before the 2h TTL lapses, so a long-lived or graduated node never
// strands itself with an expired credential (prod 2026-08-06: coturn's
// log was ~100% "Cannot find credentials"; roots logged
// replicate-all-failed).
//
// Two paths, per council review (Orion, Aster) of the first cut:
//   - HELD-OPEN node: refresh IN-BAND over the live socket (send
//     `turn-refresh`, apply the bridge's `turn` reply). Never closes a
//     healthy socket — a bare close rejects in-flight bridge requests and
//     drops bootstrap connectivity before a new credential is secured.
//   - GRADUATED / disconnected node: re-dial (no live socket to preserve,
//     no in-flight bridge work to lose). This heals the backbone relays.
//
// Scenarios:
//   A  graduated node re-dials before expiry, mesh untouched
//   B  held-open node refreshes IN-BAND: sends turn-refresh, applies the
//      `turn` reply, socket NOT closed, no new socket
//   C  held-open with NO bridge reply falls back to a re-dial (rollout
//      safety for a bridge predating the RPC)
//   D  credless welcome arms no timer; far-future expiry does not fire early
//   E  strict expiry parse (unit): a partial-numeric prefix is rejected
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
  // Frames of a given type this socket has SENT (client → bridge).
  sentOfType(t) { return this.sent.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(m => m && m.type === t); }
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
                 version: '4.60.0', kernelVersion: '4.60.0', turn });
}
async function feedBridgeHello(sock) {
  const cbv   = cbvFromNonces(SERVER_NONCE, CONN_ID, 'bridge');
  const hello = await buildAuthHello({ identity: bridgeIdent, cbv });
  sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: hello } });
}
async function connectOpen(identity, turn) {
  const t = webTransport({
    bridgeUrl: 'wss://test.example', identity, WebSocketImpl: FakeWS,
    handshakeTimeoutMs: 2000, reconnectInitialMs: 20, reconnectMaxMs: 20,
    graduationMeshFloor: 3,
    // Park the graduation watchdog far out so any re-dial we observe within a
    // second is unambiguously the TURN-refresh path, not the mesh-thin watch.
    graduationRecheckMs: 100000,
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
  console.log('webTransport — TURN credential refresh before TTL lapse (in-band)\n');
  const alice = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  bridgeIdent = await createNodeIdentity({ lat: 51.5, lng: -0.12 });

  // Refresh safety margin is 5 min; any expiry within that window collapses to
  // the 1s floor, so a cred "expiring in 5s" fires the refresh at ~1s.

  // ── A. Graduated node re-dials before expiry, mesh untouched ────────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5));
    check('A: open on first connect', t.bridgeState === 'open');
    t.webrtc.boundPeers = () => [1n, 2n, 3n];        // meshed with 3 peers
    const nAtGrad = liveSockets.length;
    sock.close(4200, 'graduated');
    check('A: graduated (no socket) after 4200', t.bridgeState === 'graduated');
    await sleep(1300);
    check('A: re-dialed before expiry despite a healthy mesh',
      liveSockets.length === nAtGrad + 1);
    const s2 = t.socket;
    const reFrame = s2 && s2.sent[0] ? JSON.parse(s2.sent[0]) : null;
    check('A: the re-dial re-sent client-hello', reFrame && reFrame.type === 'client-hello');
    feedWelcome(s2, turnCred(5)); await feedBridgeHello(s2); await sleep(5);
    check('A: back to open with a fresh credential', t.bridgeState === 'open');
    await t.stop();
  }

  // ── B. Held-open node refreshes IN-BAND (no close, no new socket) ───
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5));
    check('B: open on first connect', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    await sleep(1300);
    check('B: sent a turn-refresh request in-band', sock.sentOfType('turn-refresh').length === 1);
    check('B: did NOT close the healthy socket', sock.readyState === 1);
    check('B: did NOT open a new socket', liveSockets.length === nBefore);
    // Bridge replies with a fresh credential over the same socket.
    sock.deliver({ type: 'turn', turn: turnCred(5), serverT: Date.now() });
    await sleep(5);
    check('B: still open, socket intact after applying the reply', t.bridgeState === 'open' && sock.readyState === 1);
    // The applied credential re-armed the timer: a second refresh fires later.
    await sleep(1300);
    check('B: re-armed — a second turn-refresh fired after the reply',
      sock.sentOfType('turn-refresh').length === 2);
    await t.stop();
  }

  // ── C. Held-open, bridge never replies → fall back to a re-dial ─────
  liveSockets = [];
  {
    const { t, sock } = await connectOpen(alice, turnCred(5));
    check('C: open on first connect', t.bridgeState === 'open');
    const nBefore = liveSockets.length;
    await sleep(1300);
    check('C: sent turn-refresh', sock.sentOfType('turn-refresh').length === 1);
    check('C: socket still open while awaiting the reply', sock.readyState === 1);
    // Never deliver a `turn` reply. The fallback window (TURN_REFRESH_REPLY_MS,
    // 60s in prod) is too long to wait for in a smoke, so assert the interim
    // state is correct: still open, awaiting, no premature close. The fallback
    // re-dial path itself reuses the graduated re-dial exercised in A.
    check('C: no new socket yet (no premature fallback)', liveSockets.length === nBefore);
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

  // ── E. Strict expiry parse (via observable behaviour) ───────────────
  // A partial-numeric username prefix ("12ab:...") must be REJECTED, not
  // truncated to 12 — so it arms no timer and triggers no refresh, exactly
  // like a credential-less welcome. (turnExpiryMs is module-private; we assert
  // it through the scheduler's behaviour.)
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

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
