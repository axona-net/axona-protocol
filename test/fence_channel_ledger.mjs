// =====================================================================
// fence_channel_ledger.mjs — Hold-and-Fill v0.5/v0.7 (axona-docs 4334504,
// 95c2ff4), repair row 3: the channel and peer records, reduced (no STAGED),
// as bookkeeping and a predicate.
//
// Part A drives the ledger directly (design cases 9, 10, 40, 42 and the
// escalation row). Part B drives the REAL MeshManager with a fake
// RTCPeerConnection and checks that every lifecycle event lands in the
// ledger at the right moment: ALLOCATED at PC construction, NEGOTIATING at
// the first frame, OPEN at dc-open, CLOSING at _retire, GONE at the
// transport's 'closed' or by escalation; that the peer record follows
// bindPeer/unbindPeer; that with enforce off (the default) nothing is
// refused and the would-refuse counters move; and that with enforce on an
// outbound dial and an inbound offer are refused with no PC built.
//
// With the ledger removed from mesh.js (hooks deleted), part B fails; part
// A is the module alone. Neither part is a live WebRTC test.
//
// Run: node test/fence_channel_ledger.mjs
// =====================================================================
import { ChannelLedger, CHAN, LEDGER_DEFAULTS } from '../src/transport/web/channel_ledger.js';
import { MeshManager } from '../src/transport/web/mesh.js';
import { WebRTCTransport } from '../src/transport/web/webrtc.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise(r => setTimeout(r, 0));

// ── Part A: the ledger alone ──────────────────────────────────────────
console.log('fence_channel_ledger: row 3 — channel and peer records');
{
  let now = 1000;
  const timers = [];
  const L = new ChannelLedger({ cPhys: 3, cInbound: 1, pPending: 2, closeEscalateMs: 50, now: () => now,
    setTimeout: (fn, ms) => { timers.push({ fn, at: now + ms }); return timers.length; }, clearTimeout: (id) => { if (timers[id - 1]) timers[id - 1].fn = null; } });
  const fire = () => { for (const t of timers) if (t.fn && now >= t.at) { const f = t.fn; t.fn = null; f(); } };

  check('A defaults are the design\'s first values', LEDGER_DEFAULTS.cPhys === 66 && LEDGER_DEFAULTS.cInbound === 4 && LEDGER_DEFAULTS.pPending === 8 && LEDGER_DEFAULTS.enforce === false);

  // allocate → negotiating → open → closing → gone (prompted)
  L.allocate('t1', 'c1', 'out');
  check('A allocate: ALLOCATED, all=1, outboundPending=1', L.record('t1').state === CHAN.ALLOCATED && L.chanAll() === 1 && L.chanOutboundPending() === 1);
  L.negotiating('t1');
  check('A negotiating', L.record('t1').state === CHAN.NEGOTIATING && L.chanOutboundPending() === 1);
  L.open('t1');
  check('A open: outboundPending back to 0, all still 1', L.record('t1').state === CHAN.OPEN && L.chanOutboundPending() === 0 && L.chanAll() === 1);
  L.bind('c1', 'aa'.repeat(33));
  check('A bind: peer record points at t1', L.peer('aa'.repeat(33))?.t === 't1' && L.record('t1').nodeId === 'aa'.repeat(33));
  L.closing('t1', 'test');
  check('A closing: state CLOSING, pointer cleared in the same step, capacity NOT released', L.record('t1').state === CHAN.CLOSING && L.peer('aa'.repeat(33))?.t === null && L.chanAll() === 1);
  const g = L.gone('t1');
  check('A gone (prompted): released, record dropped, prompted=true', g?.prompted === true && L.chanAll() === 0 && L.record('t1') === null);

  // case 40: zero headroom refuses with enforce on; the next gone lets one through
  const E = new ChannelLedger({ cPhys: 2, cInbound: 5, pPending: 5, enforce: true, closeEscalateMs: 0 });
  E.allocate('e1', 'c1', 'out'); E.allocate('e2', 'c2', 'out');
  check('A case 40: at chan(all) = C_phys the predicate refuses', E.mayAllocate('out').ok === false && E.mayAllocate('out').why === 'phys');
  E.closing('e1');
  check('A case 40: CLOSING still counts (nothing released at close)', E.mayAllocate('out').ok === false);
  E.gone('e1');
  check('A case 40: after one gone the next allocation proceeds', E.mayAllocate('out').ok === true);
  check('A refusals counted (three refusing calls above)', E.stats().refusedOut === 3, String(E.stats().refusedOut));

  // case 9 shape: inbound bound and pending bound
  const I = new ChannelLedger({ cPhys: 10, cInbound: 1, pPending: 1, enforce: true, closeEscalateMs: 0 });
  I.allocate('i1', 'c1', 'in');
  check('A C_inbound: one unbound inbound negotiating refuses the next inbound', I.mayAllocate('in').ok === false && I.mayAllocate('in').why === 'inbound');
  I.bind('c1', 'bb'.repeat(33));
  check('A C_inbound: once bound it no longer counts as inbound-unbound', I.mayAllocate('in').ok === true);
  I.allocate('o1', 'c2', 'out');
  check('A P_pending: one outbound pending refuses the next outbound', I.mayAllocate('out').ok === false && I.mayAllocate('out').why === 'pending');
  check('A P_pending does not refuse inbound', I.mayAllocate('in').ok === true);

  // enforce off: counted, never refused
  const W = new ChannelLedger({ cPhys: 1, enforce: false, closeEscalateMs: 0 });
  W.allocate('w1', 'c1', 'out');
  const r = W.mayAllocate('out');
  check('A enforce off: ok=true with why reported and wouldRefuseOut=1', r.ok === true && r.why === 'phys' && W.stats().wouldRefuseOut === 1 && W.stats().refusedOut === 0);

  // case 42: unprompted close of an OPEN channel → GONE in one step, prompted=false
  L.allocate('t2', 'c2', 'in'); L.negotiating('t2'); L.open('t2'); L.bind('c2', 'cc'.repeat(33));
  const u = L.gone('t2');
  check('A case 42: unprompted gone releases and clears the pointer', u?.prompted === false && L.chanAll() === 0 && L.peer('cc'.repeat(33))?.t === null);

  // escalation: CLOSING with no 'closed' from the transport
  L.allocate('t3', 'c3', 'out'); L.negotiating('t3'); L.open('t3');
  L.closing('t3', 'silent');
  now += 49; fire();
  check('A escalate: before closeEscalateMs the record is still CLOSING', L.record('t3')?.state === CHAN.CLOSING);
  now += 2; fire();
  check('A escalate: after closeEscalateMs the record is GONE and counted', L.record('t3') === null && L.stats().closeEscalated === 1);

  // stale events
  const before = L.stats().staleEvent;
  L.gone('t3'); L.open('nope');
  check('A stale events are counted, not applied', L.stats().staleEvent === before + 2);

  // case 4 shape: an old channel CLOSING beside a new one to the same identity
  L.allocate('t4', 'c4', 'out'); L.negotiating('t4'); L.open('t4'); L.bind('c4', 'dd'.repeat(33));
  L.closing('t4', 'swap-out');
  L.allocate('t5', 'c4', 'out'); L.negotiating('t5'); L.open('t5'); L.bind('c4', 'dd'.repeat(33));
  check('A case 4: both channels counted; the peer points at the new one', L.chanAll() === 2 && L.peer('dd'.repeat(33))?.t === 't5');
  L.gone('t4');
  check('A case 4: gone(t_old) releases only t_old; the pointer is untouched', L.chanAll() === 1 && L.peer('dd'.repeat(33))?.t === 't5');
  L.dispose();
}

// ── Part B: the real MeshManager with a fake RTCPeerConnection ────────
class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } }
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this._dc = null; }
  createDataChannel() { this._dc = new FakeDC(); return this._dc; }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.connectionState = 'closed'; if (FakePC.fireClosedOnClose) queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
}
FakePC.instances = []; FakePC.fireClosedOnClose = true;
globalThis.RTCPeerConnection = FakePC;

(async () => {
  console.log('\n  Part B: MeshManager hooks');
  const mkMesh = (ledger) => new MeshManager({ sendSignal: () => {}, log: () => {}, ledger });

  // B1: outbound lifecycle, prompted close
  {
    const mesh = mkMesh({ closeEscalateMs: 0 });
    FakePC.instances.length = 0;
    await mesh._initiateTo('c1'); await tick();
    const st = mesh._peers.get('c1');
    const s1 = mesh.ledgerStats();
    check('B1 initiate: one record, NEGOTIATING, outbound pending 1, token = state.inc', s1.all === 1 && s1.byState.NEGOTIATING === 1 && s1.outboundPending === 1 && mesh._ledger.record(st.inc) != null, JSON.stringify(s1));
    st.dc.onopen();
    const s2 = mesh.ledgerStats();
    check('B1 dc-open: OPEN, outbound pending 0, unboundOpen 1', s2.byState.OPEN === 1 && s2.outboundPending === 0 && s2.unboundOpen === 1);
    mesh.ledgerBind('c1', 'ab'.repeat(33));
    check('B1 bind through the mesh: boundPeers 1, unboundOpen 0', mesh.ledgerStats().boundPeers === 1 && mesh.ledgerStats().unboundOpen === 0);
    mesh._retire('c1', 'test-close');
    const s3 = mesh.ledgerStats();
    check('B1 _retire: CLOSING, capacity still held', s3.byState.CLOSING === 1 && s3.all === 1);
    await tick(); await tick();
    const s4 = mesh.ledgerStats();
    check('B1 transport closed: GONE, released, goneTotal 1', s4.all === 0 && s4.goneTotal === 1, JSON.stringify(s4));
    mesh.dispose();
  }

  // B2: inbound lifecycle via onSignal, unprompted close
  {
    const mesh = mkMesh({ closeEscalateMs: 0 });
    await mesh.onSignal('c2', { kind: 'sdp-offer', sdp: 'v=0 offer' }); await tick();
    const st = mesh._peers.get('c2');
    const s1 = mesh.ledgerStats();
    check('B2 inbound offer: record NEGOTIATING, inboundUnbound 1', s1.byState.NEGOTIATING === 1 && s1.inboundUnbound === 1, JSON.stringify(s1));
    st.pc.ondatachannel({ channel: new FakeDC() }); st.dc.onopen();
    check('B2 dc-open: OPEN, inboundUnbound 0', mesh.ledgerStats().byState.OPEN === 1 && mesh.ledgerStats().inboundUnbound === 0);
    st.pc.connectionState = 'closed'; st.pc.onconnectionstatechange();
    await tick();
    check('B2 unprompted closed: record released', mesh.ledgerStats().all === 0 && !mesh._peers.has('c2'));
    mesh.dispose();
  }

  // B3: escalation when the PC never reports 'closed'
  {
    FakePC.fireClosedOnClose = false;
    const mesh = mkMesh({ closeEscalateMs: 30 });
    await mesh._initiateTo('c3'); await tick();
    mesh._peers.get('c3').dc.onopen();
    mesh._retire('c3', 'silent');
    await new Promise(r => setTimeout(r, 10));
    check('B3 before escalation: CLOSING held', mesh.ledgerStats().byState.CLOSING === 1);
    await new Promise(r => setTimeout(r, 40));
    check('B3 escalation released the record and counted it', mesh.ledgerStats().all === 0 && mesh.ledgerStats().closeEscalated === 1);
    FakePC.fireClosedOnClose = true;
    mesh.dispose();
  }

  // B4: enforce off (default): nothing refused, would-refuse counted, PCs built
  {
    const mesh = mkMesh({ cPhys: 1 });
    FakePC.instances.length = 0;
    await mesh._initiateTo('d1'); await mesh._initiateTo('d2'); await tick();
    const s = mesh.ledgerStats();
    check('B4 enforce off: both dials built PCs', FakePC.instances.length === 2 && mesh._peers.size === 2 && s.all === 2);
    check('B4 enforce off: wouldRefuseOut counted, refusedOut 0', s.wouldRefuseOut === 1 && s.refusedOut === 0);
    await mesh.onSignal('d3', { kind: 'sdp-offer', sdp: 'v=0' }); await tick();
    check('B4 enforce off: inbound offer built a PC too, wouldRefuseIn 1', mesh._peers.size === 3 && mesh.ledgerStats().wouldRefuseIn === 1);
    mesh.dispose();
  }

  // B5: enforce on: refused with nothing built
  {
    const mesh = mkMesh({ cPhys: 1, enforce: true });
    FakePC.instances.length = 0;
    await mesh._initiateTo('e1'); await mesh._initiateTo('e2'); await tick();
    check('B5 enforce on: second dial refused, one PC, one state', FakePC.instances.length === 1 && mesh._peers.size === 1 && mesh.ledgerStats().refusedOut === 1);
    await mesh.onSignal('e3', { kind: 'sdp-offer', sdp: 'v=0' }); await tick();
    check('B5 enforce on: inbound offer refused, no state, refusedIn 1', mesh._peers.size === 1 && mesh.ledgerStats().refusedIn === 1);
    mesh.dispose();
  }

  // B6: ledger off
  {
    const mesh = mkMesh(false);
    check('B6 ledger:false → ledgerStats null, mesh still works', mesh.ledgerStats() === null);
    await mesh._initiateTo('f1'); await tick();
    check('B6 ledger:false → dial still builds', mesh._peers.size === 1);
    mesh.dispose();
  }

  // B7: WebRTCTransport bindPeer/unbindPeer reach the ledger
  {
    const mesh = mkMesh({ closeEscalateMs: 0 });
    const t = new WebRTCTransport({ mesh, log: () => {} });
    await mesh._initiateTo('g1'); await tick();
    mesh._peers.get('g1').dc.onopen();
    const nid = BigInt('0x' + 'ef'.repeat(33));
    t.bindPeer(nid, 'g1');
    check('B7 bindPeer → ledger peer record', mesh._ledger.peer('ef'.repeat(33))?.t === mesh._peers.get('g1').inc);
    t.unbindPeer('g1');
    check('B7 unbindPeer → peer record dropped (no other channel)', mesh._ledger.peer('ef'.repeat(33)) === null);
    mesh.dispose();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
