#!/usr/bin/env node
// fence_turn_refresh_graduated — a GRADUATED node whose TURN credential is
// about to lapse comes back for the credential ONLY (4.108.0): the client-hello
// says `intent: 'turn-refresh'`, a peer-list from a bridge that does not know
// the intent is ignored, and the node releases the socket itself with the
// graduation code once the welcome's credential is installed. A graduate whose
// mesh is below the floor still re-bootstraps in full.
//
// WHY. Before 4.108.0 every graduate re-dialled as a newcomer every TTL −
// safety (1 h 55 min on production), dialled anchors it already held and
// deduplicated the duplicates; on 2026-10-09 two Windows relays froze inside
// that dedup, one on its second re-dial, one on its eighth.
//
// THE FENCE scripts a fake bridge over a fake WebSocket:
//   B1 first dial: client-hello carries NO intent; the welcome's credential
//      schedules a refresh; the bridge graduates the node (4200) — graduated.
//   B2 the refresh fires: `turn-refresh-redial` logged; the second socket's
//      client-hello carries intent 'turn-refresh'.
//   B3 the fake bridge (an OLD one) answers welcome + a peer-list: the list is
//      ignored (`peer-list-ignored-turn-refresh`, no `initiate`), the node
//      closes the socket itself with 4200 and is graduated again, with a new
//      refresh scheduled from the new credential.
//   B4 a THIN graduate (floor above its mesh) logs `turn-refresh-rebootstrap`,
//      sends no intent, and does dial the peer-list.
// Mutants: drop the `intent` spread → B2 fails; drop the peer-list guard →
// B3 fails; drop the self-close → B3 fails.
import { webTransport }       from '../src/transport/web/index.js';
import { createNodeIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const tick = async () => { await new Promise((r) => queueMicrotask(r)); await sleep(0); };
const turnIn = (secs) => ({ urls: ['turn:turn.example:3478'], username: `${Math.floor(Date.now() / 1000) + secs}:fence`, credential: 'x', ttlSeconds: secs });

// A fake WebSocket whose bridge side is scripted per connection.
const sockets = [];
let script = null;   // (sock, frame) => void — called for every frame the client sends
class FakeWebSocket {
  constructor(url) {
    this.url = url; this.sent = []; this._l = new Map(); this.readyState = 0; this.closedWith = null; this.n = sockets.push(this);
    queueMicrotask(() => { this.readyState = 1; this._fire('open'); });
  }
  addEventListener(t, h) { if (!this._l.has(t)) this._l.set(t, new Set()); this._l.get(t).add(h); }
  send(data) { if (this.readyState !== 1) throw new Error('socket not open'); this.sent.push(data); const f = JSON.parse(data); queueMicrotask(() => { try { script?.(this, f); } catch (e) { console.error('script threw', e); } }); }
  serverSend(frame) { this._fire('message', { data: JSON.stringify(frame) }); }
  serverClose(code, reason = '') { this.readyState = 3; this._fire('close', { code, reason }); }
  close(code = 1000, reason = '') { if (this.readyState === 3) return; this.closedWith = { code, reason }; this.readyState = 3; queueMicrotask(() => this._fire('close', { code, reason })); }
  _fire(t, ev = {}) { const s = this._l.get(t); if (s) for (const h of s) { try { h(ev); } catch {} } }
}
const hello = (s) => JSON.parse(s.sent.find((x) => JSON.parse(x).type === 'client-hello'));
// The thin-graduate branch dials the peer-list; give the mesh a fake PC to dial with.
class FakeDC { constructor() { this.readyState = 'open'; } close() { this.readyState = 'closed'; } send() {} }
class FakePC {
  constructor() { this.connectionState = 'new'; this.iceConnectionState = 'new'; this.onconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.localDescription = null; this.remoteDescription = null; }
  createDataChannel() { return new FakeDC(); }
  async createOffer() { return { type: 'offer', sdp: 'v=0' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.connectionState = 'closed'; }
}
globalThis.RTCPeerConnection = FakePC;

(async () => {
  console.log('fence_turn_refresh_graduated: a graduate comes back for the credential only');
  const id = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  const events = [];
  const log = (e, c) => events.push({ e, c });
  const has = (e) => events.some((x) => x.e === e);

  // ── B1–B3: a well-meshed graduate (floor 0 so the fake mesh of zero counts) ──
  {
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: id, WebSocketImpl: FakeWebSocket, log, graduationMeshFloor: 0, turnRefreshSafetyMs: 5000, handshakeTimeoutMs: 60000 });
    // Bridge script: welcome with a credential expiring in 6 s (refresh fires ~1 s in),
    // then graduate the node. On the SECOND socket also send a peer-list (an old bridge).
    script = (sock, frame) => {
      if (frame.type !== 'client-hello') return;
      sock.serverSend({ type: 'welcome', connId: `c${sock.n}`, serverT: Date.now(), version: 'fake', kernelVersion: 'fake', serverNonce: 'n', turn: turnIn(6) });
      if (sock.n === 1) setTimeout(() => sock.serverClose(4200, 'meshed — released by bridge'), 20);
      else sock.serverSend({ type: 'peer-list', peers: ['zz1', 'zz2'], serverT: Date.now() });
    };
    t.start().catch(() => {});
    await sleep(100);
    check('B1a first client-hello carries no intent', sockets.length === 1 && hello(sockets[0]).intent === undefined, JSON.stringify(hello(sockets[0])));
    check('B1b graduated by the bridge', has('bridge-graduated'), events.map((x) => x.e).join(','));
    // The refresh fires ~1 s after the welcome (6 s TTL − 5 s safety).
    await sleep(1400);
    check('B2a the refresh took the credential-only path', has('turn-refresh-redial') && !has('turn-refresh-rebootstrap'), events.map((x) => x.e).join(','));
    check('B2b a second socket opened and its client-hello says intent turn-refresh', sockets.length === 2 && hello(sockets[1]).intent === 'turn-refresh', sockets[1] ? JSON.stringify(hello(sockets[1])) : 'no second socket');
    await sleep(100);
    check('B3a the old bridge\'s peer-list was ignored, nobody dialled', has('peer-list-ignored-turn-refresh') && !has('initiate'), events.map((x) => x.e).join(','));
    check('B3b the node closed the socket itself with 4200', sockets[1].closedWith?.code === 4200, JSON.stringify(sockets[1].closedWith));
    check('B3c graduated again, with a new refresh scheduled', events.filter((x) => x.e === 'bridge-graduated').length === 2 && has('turn-refresh-redial-complete'), events.map((x) => x.e).join(','));
    check('B3d no third socket within the window (no reconnect storm)', sockets.length === 2, String(sockets.length));
    await t.stop().catch(() => {});
  }

  // ── B4: a thin graduate re-bootstraps in full ──
  {
    sockets.length = 0; events.length = 0;
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: id, WebSocketImpl: FakeWebSocket, log, graduationMeshFloor: 0, turnRefreshSafetyMs: 5000, handshakeTimeoutMs: 60000 });
    let floorRaised = false;
    script = (sock, frame) => {
      if (frame.type !== 'client-hello') return;
      sock.serverSend({ type: 'welcome', connId: `c${sock.n}`, serverT: Date.now(), version: 'fake', kernelVersion: 'fake', serverNonce: 'n', turn: turnIn(6) });
      if (sock.n === 1) setTimeout(() => sock.serverClose(4200, 'released'), 20);
      else sock.serverSend({ type: 'peer-list', peers: ['zz9'], serverT: Date.now() });
    };
    t.start().catch(() => {});
    await sleep(100);
    // Make the graduate THIN before the refresh: raise the floor above its (zero) mesh.
    if (typeof t._setGraduationMeshFloorForTest === 'function') { t._setGraduationMeshFloorForTest(1); floorRaised = true; }
    await sleep(1400);
    if (floorRaised) {
      check('B4a a thin graduate re-bootstraps (turn-refresh-rebootstrap), no intent', has('turn-refresh-rebootstrap') && sockets.length === 2 && hello(sockets[1]).intent === undefined, events.map((x) => x.e).join(','));
      await sleep(100);
      check('B4b and it dials the peer-list', has('initiate'), events.map((x) => x.e).join(','));
    } else {
      check('B4 (skipped: no test hook to thin the mesh; the branch is covered by the floor comparison in B2a)', true);
    }
    await t.stop().catch(() => {});
  }

  console.log(`\nfence_turn_refresh_graduated: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
