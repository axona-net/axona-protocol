// =====================================================================
// fence_close_at_rest.mjs — 4.155.0, option closeAtRest / CLOSE_AT_REST=1.
//
// WHY. A relay freezes when a connection is closed in the instant its ICE
// connects over a TURN relay: libjuice's poll thread holds its registry mutex
// and wants the DTLS transport's SSL mutex (DtlsTransport::start), while the
// close holds the SSL mutex and wants the registry mutex (SSL_shutdown ->
// juice_send on a relayed pair). Read from a Windows dump of 2026-11-17 and
// the libraries' source. JS cannot reorder those locks. It can decline to
// issue the native close while the connection is still negotiating.
//
// What this fences, with fake connections (the real deadlock needs the native
// library, a TURN relay and a race; this test fences the DECISION):
//   1. OFF (default): a retire of a connecting connection closes it on the
//      next macrotask, exactly as before.
//   2. ON: the same retire does all its bookkeeping at once (peer gone from
//      the mesh) and issues NO native close while the state is 'connecting'.
//   3. ON: the close goes out when the connection reaches a rest state, for
//      each of connected, failed, disconnected.
//   4. ON: a connection already at rest closes on the next macrotask (no park).
//   5. ON: a parked connection that connects does NOT come back to life: no
//      retry is scheduled on 'failed', and a data channel that opens late
//      binds nothing.
//   6. ON: the ledger's escalation does not close a parked connection.
//   7. ON: the backstop issues the close when no rest state ever arrives.
//   8. ON: dispose() issues every parked close at once.
//   9. ON: 'closed' on a parked connection still confirms to the ledger.
//  10. The env switch: CLOSE_AT_REST=1 turns it on; an explicit false wins.
//
// With the park removed (the old `deferMacrotask(closeNative)` restored for
// every case) checks 2, 3, 5, 6 and 7 fail.
//
// Run: node test/fence_close_at_rest.mjs
// =====================================================================
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise((r) => setTimeout(r, 0));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; this.closeCalls = 0; } send() {} close() { this.closeCalls++; this.readyState = 'closed'; } open() { this.readyState = 'open'; this.onopen?.(); } }
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.closeCalls = 0; }
  createDataChannel() { return new FakeDC(); }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  to(cs) { this.connectionState = cs; try { this.onconnectionstatechange?.(); } catch {} }
  close() { this.closeCalls++; this.connectionState = 'closed'; queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
}
FakePC.instances = [];
globalThis.RTCPeerConnection = FakePC;

(async () => {
  console.log('fence_close_at_rest: the native close of a negotiating connection waits for rest');
  const logs = [];
  const mk = (opts = {}) => new MeshManager({ sendSignal: () => {}, log: (ev, d) => logs.push([ev, d]), ...opts });
  const open1 = async (mesh, id, cs = 'connecting') => { await mesh._initiateTo(id); await tick(); const st = mesh._peers.get(id); st.pc.connectionState = cs; return st; };

  // 1 OFF: unchanged
  {
    const mesh = mk();
    const st = await open1(mesh, 'a1'); const pc = st.pc;
    mesh._retire('a1', 'peer-left'); await tick(); await tick();
    check('1 off (default): a connecting connection is closed on the next macrotask', pc.closeCalls === 1 && !mesh.hasPeer('a1'));
    check('1 off: snapshot says off and nothing held', mesh.closeAtRestSnapshot().on === false && mesh.closeAtRestSnapshot().held === 0);
    mesh.dispose();
  }

  // 2 + 3: ON parks, then releases on each rest state
  for (const rest of ['connected', 'failed', 'disconnected']) {
    const mesh = mk({ closeAtRest: true });
    const st = await open1(mesh, 'b1'); const pc = st.pc; const dc = st.dc;
    mesh._retire('b1', 'peer-left'); await tick(); await tick();
    check(`2 on: retire of a connecting connection removes the peer and issues no native close (${rest} case)`, !mesh.hasPeer('b1') && pc.closeCalls === 0 && dc.closeCalls === 0 && mesh.closeAtRestSnapshot().held === 1);
    pc.to(rest); await tick(); await tick();
    check(`3 on: the close goes out when the connection is ${rest}`, pc.closeCalls === 1 && dc.closeCalls === 1 && mesh.closeAtRestSnapshot().held === 0 && mesh.closeAtRestSnapshot().atRest === 1);
    mesh.dispose();
  }

  // 4: already at rest → no park
  {
    const mesh = mk({ closeAtRest: true });
    const st = await open1(mesh, 'c1', 'connected'); const pc = st.pc;
    mesh._retire('c1', 'pong-timeout'); await tick(); await tick();
    check('4 on: a connected connection closes on the next macrotask, never parked', pc.closeCalls === 1 && mesh.closeAtRestSnapshot().parked === 0);
    mesh.dispose();
  }

  // 5: a parked connection does not come back to life
  {
    const mesh = mk({ closeAtRest: true });
    const st = await open1(mesh, 'd1'); const pc = st.pc; const dc = st.dc;
    mesh._retire('d1', 'peer-left'); await tick();
    dc.open(); await tick();
    check('5 on: a data channel that opens while parked binds nothing (peer stays gone)', !mesh.hasPeer('d1') && mesh._peers.size === 0);
    const before = FakePC.instances.length;
    pc.to('failed'); await sleep(30);
    check('5 on: failed on a parked connection schedules no retry and makes no new connection', FakePC.instances.length === before && st.retryTimer == null && !mesh.hasPeer('d1'));
    check('5 on: and its close went out', pc.closeCalls === 1);
    mesh.dispose();
  }

  // 6: escalation leaves a parked connection alone
  {
    const mesh = mk({ closeAtRest: true, ledger: { closeEscalateMs: 20 } });
    const st = await open1(mesh, 'e1'); const pc = st.pc;
    mesh._retire('e1', 'peer-left'); await sleep(80);
    const esc = logs.filter(([ev, d]) => ev === 'close-escalate' && d.inc === st.inc);
    check('6 on: the ledger escalated and did not close the parked connection', esc.length >= 1 && esc.every(([, d]) => d.parked === true) && pc.closeCalls === 0, `esc=${esc.length} closes=${pc.closeCalls}`);
    pc.to('failed'); await tick(); await tick();
    check('6 on: it still closes at rest afterwards', pc.closeCalls === 1);
    mesh.dispose();
  }

  // 7: the backstop (timer driven by hand: the real one is 90 s)
  {
    const mesh = mk({ closeAtRest: true });
    const st = await open1(mesh, 'f1'); const pc = st.pc;
    const realSetTimeout = globalThis.setTimeout; let backstop = null;
    globalThis.setTimeout = (fn, ms, ...a) => { if (ms === 90_000) { backstop = fn; return { unref() {} }; } return realSetTimeout(fn, ms, ...a); };
    mesh._retire('f1', 'negotiation-timeout');
    globalThis.setTimeout = realSetTimeout;
    await tick(); await tick();
    check('7 on: parked with a 90 s backstop armed, no close yet', typeof backstop === 'function' && pc.closeCalls === 0);
    backstop(); await tick(); await tick();
    check('7 on: the backstop issues the close and is counted', pc.closeCalls === 1 && mesh.closeAtRestSnapshot().backstop === 1 && mesh.closeAtRestSnapshot().held === 0);
    pc.to('failed'); await tick();
    check('7 on: a rest state after the backstop closes nothing twice', pc.closeCalls === 1);
    mesh.dispose();
  }

  // 8: dispose issues parked closes
  {
    const mesh = mk({ closeAtRest: true });
    const s1 = await open1(mesh, 'g1'); const s2 = await open1(mesh, 'g2');
    mesh._retire('g1', 'peer-left'); mesh._retire('g2', 'peer-left'); await tick();
    check('8 on: two closes parked', mesh.closeAtRestSnapshot().held === 2 && s1.pc.closeCalls === 0 && s2.pc.closeCalls === 0);
    mesh.dispose();
    check('8 on: dispose issues both at once', s1.pc.closeCalls === 1 && s2.pc.closeCalls === 1 && mesh.closeAtRestSnapshot().held === 0);
  }

  // 9: 'closed' on a parked connection confirms to the ledger
  {
    const mesh = mk({ closeAtRest: true });
    const st = await open1(mesh, 'h1'); const pc = st.pc;
    mesh._retire('h1', 'peer-left'); await tick();
    const closingBefore = mesh._closingPcs.size;
    pc.to('closed'); await tick(); await tick();
    check('9 on: closed while parked releases the park and the ledger record', closingBefore === 1 && mesh._closingPcs.size === 0 && mesh.closeAtRestSnapshot().held === 0);
    mesh.dispose();
  }

  // 10: env switch
  {
    const had = Object.prototype.hasOwnProperty.call(process.env, 'CLOSE_AT_REST'); const old = process.env.CLOSE_AT_REST;
    process.env.CLOSE_AT_REST = '1';
    const on = mk(); const off = mk({ closeAtRest: false });
    check('10 CLOSE_AT_REST=1 turns it on; an explicit false wins', on.closeAtRestSnapshot().on === true && off.closeAtRestSnapshot().on === false);
    delete process.env.CLOSE_AT_REST;
    const dflt = mk();
    check('10 unset: off', dflt.closeAtRestSnapshot().on === false);
    if (had) process.env.CLOSE_AT_REST = old;
    on.dispose(); off.dispose(); dflt.dispose();
  }

  console.log(`\n${failed === 0 ? '✓' : '✗'} fence_close_at_rest: ${passed}/${passed + failed} checks passed`);
  process.exit(failed === 0 ? 0 : 1);
})();
