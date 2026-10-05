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
// transport's 'closed' and ONLY there (escalation forces a second close and
// releases nothing, Aster 09f59626 R3-A); that the peer record follows
// bindPeer/unbindPeer including the duplicate-resolution branch (R3-B) and
// full retire→unbind→closed churn (R3-C); that with enforce off (the
// default) nothing is refused and the would-refuse counters move; and that
// with enforce on an outbound dial and an inbound offer are refused with no
// PC built.
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
  check('A closing: state CLOSING, the identity leaves the bound set in the same step (no other binding), capacity NOT released', L.record('t1').state === CHAN.CLOSING && L.peer('aa'.repeat(33)) === null && L.chanAll() === 1);
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
  check('A case 42: unprompted gone releases and drops the identity from the bound set', u?.prompted === false && L.chanAll() === 0 && L.peer('cc'.repeat(33)) === null);

  // escalation (R3-A): CLOSING with no 'closed' from the transport. The timer
  // forces a second close through onEscalate and RELEASES NOTHING; capacity
  // waits for the confirmation.
  const escalated = [];
  const LE = new ChannelLedger({ cPhys: 1, enforce: true, closeEscalateMs: 50, now: () => now, onEscalate: (t, m) => escalated.push([t, m]),
    setTimeout: (fn, ms) => { timers.push({ fn, at: now + ms }); return timers.length; }, clearTimeout: (id) => { if (timers[id - 1]) timers[id - 1].fn = null; } });
  LE.allocate('t3', 'c3', 'out'); LE.negotiating('t3'); LE.open('t3');
  LE.closing('t3', 'silent');
  now += 49; fire();
  check('A escalate: before closeEscalateMs the record is CLOSING, nothing escalated', LE.record('t3')?.state === CHAN.CLOSING && escalated.length === 0);
  now += 2; fire();
  check('A escalate: onEscalate called once with (t, meshId)', escalated.length === 1 && escalated[0][0] === 't3' && escalated[0][1] === 'c3');
  check('A escalate: the record STAYS CLOSING and charged; closeEscalated 1', LE.record('t3')?.state === CHAN.CLOSING && LE.chanAll() === 1 && LE.stats().closeEscalated === 1);
  check('A escalate: an unconfirmed close still refuses at cPhys (nothing released on a timer)', LE.mayAllocate('out').ok === false && LE.mayAllocate('out').why === 'phys');
  check('A escalate: oldestClosingMs reports the wait', LE.stats().oldestClosingMs >= 51);
  LE.gone('t3');
  check('A escalate then confirmed close: released now', LE.chanAll() === 0 && LE.mayAllocate('out').ok === true);
  // a throwing escalation callback is logged, not propagated
  const LT = new ChannelLedger({ closeEscalateMs: 10, now: () => now, onEscalate: () => { throw new Error('boom'); },
    setTimeout: (fn, ms) => { timers.push({ fn, at: now + ms }); return timers.length; }, clearTimeout: () => {} });
  LT.allocate('t9', 'c9', 'out'); LT.closing('t9');
  let threw = false; now += 11; try { fire(); } catch { threw = true; }
  check('A escalate: a throwing onEscalate does not propagate; record still CLOSING', !threw && LT.record('t9')?.state === CHAN.CLOSING && LT.stats().closeEscalated === 1);
  // zero semantics: closeEscalateMs:0 is OFF, not the default
  const LZ = new ChannelLedger({ closeEscalateMs: 0 });
  LZ.allocate('z1', 'cz', 'out'); LZ.closing('z1');
  check('A closeEscalateMs:0 means off: stored as 0, no timer armed', LZ.closeEscalateMs === 0 && LZ.record('z1')?.escalateTimer === null);
  check('A numeric options: finite values taken as given, others default', new ChannelLedger({ cPhys: 'x', cInbound: 0 }).cPhys === LEDGER_DEFAULTS.cPhys && new ChannelLedger({ cInbound: 0 }).cInbound === 1 && new ChannelLedger({ cPhys: 7 }).cPhys === 7);

  // stale events
  const before = L.stats().staleEvent;
  L.gone('t3'); L.open('nope');
  check('A stale events are counted, not applied', L.stats().staleEvent === before + 2);

  // churn (R3-C): retire → unbind → closed leaves no peer record behind
  L.allocate('t6', 'c6', 'out'); L.negotiating('t6'); L.open('t6'); L.bind('c6', 'ee'.repeat(33));
  check('A churn: bound', L.stats().boundPeers === 1 && L.stats().peersPointing === 1);
  L.closing('t6', 'retire');          // token leaves the current map here
  L.unbind('c6');                     // onPeerLost → unbindPeer arrives AFTER closing
  check('A churn: unbind after closing still finds the channel; bound set empty', L.stats().boundPeers === 0 && L.record('t6')?.nodeId === null);
  L.gone('t6');
  check('A churn: all 0, boundPeers 0', L.chanAll() === 0 && L.stats().boundPeers === 0);
  // gone without an unbind (transport never called unbindPeer) also drops the record
  L.allocate('t7', 'c7', 'out'); L.open('t7'); L.bind('c7', 'ff'.repeat(33));
  L.gone('t7');
  check('A churn: gone of a bound channel with no other binding drops the record', L.stats().boundPeers === 0);
  // two channels binding one identity: unbind of one keeps the record on the other
  L.allocate('t8', 'c8', 'out'); L.open('t8'); L.bind('c8', 'ab'.repeat(33));
  L.allocate('t8b', 'c8b', 'in'); L.open('t8b'); L.bind('c8b', 'ab'.repeat(33));
  check('A two bindings: pointer at the newest', L.peer('ab'.repeat(33))?.t === 't8b');
  L.closing('t8b'); L.unbind('c8b'); L.gone('t8b');
  check('A two bindings: after the newest goes, the record re-points to the survivor', L.peer('ab'.repeat(33))?.t === 't8' && L.stats().boundPeers === 1);
  L.closing('t8'); L.unbind('c8'); L.gone('t8');
  check('A two bindings: after both go, no record', L.stats().boundPeers === 0 && L.chanAll() === 0);

  // POINTER ELIGIBILITY vs PHYSICAL RETENTION (Aster 38ea5f3e): a CLOSING
  // channel is charged but never pointed at. Escalation off here.
  const LP = new ChannelLedger({ closeEscalateMs: 0 });
  const ID = '77'.repeat(33);
  // Aster's schedule: bind old; closing old (unconfirmed); bind new; closing new; unbind new.
  LP.allocate('o', 'mo', 'out'); LP.open('o'); LP.bind('mo', ID);
  LP.closing('o', 'retire');
  check('P1 closing old: pointer leaves old at once; no other binding → record dropped; old still charged', LP.peer(ID) === null && LP.chanAll() === 1 && LP.record('o').state === CHAN.CLOSING);
  LP.allocate('n', 'mn', 'out'); LP.open('n'); LP.bind('mn', ID);
  check('P1 bind new: record points at new (OPEN), never at closing old', LP.peer(ID)?.t === 'n');
  LP.closing('n', 'retire');
  check('P1 closing new: both CLOSING and charged, NO pointer, boundPeers 0', LP.chanAll() === 2 && LP.peer(ID) === null && LP.stats().boundPeers === 0 && LP.stats().peersPointing === 0);
  LP.unbind('mn');
  check('P1 unbind new: still no pointer', LP.peer(ID) === null);
  LP.gone('n');
  check('P1 gone new: pointer never resurrected onto closing old', LP.peer(ID) === null && LP.chanAll() === 1);
  LP.gone('o');
  check('P1 gone old: all 0', LP.chanAll() === 0);
  // Variant: gone without unbind, old closed first then new gone
  LP.allocate('o2', 'mo2', 'out'); LP.open('o2'); LP.bind('mo2', ID);
  LP.allocate('n2', 'mn2', 'out'); LP.open('n2'); LP.bind('mn2', ID);
  LP.closing('o2');
  check('P2 old closing while new OPEN: pointer stays on new', LP.peer(ID)?.t === 'n2');
  LP.gone('n2');   // no unbind delivered
  check('P2 new gone without unbind: no pointer onto closing old; record dropped', LP.peer(ID) === null && LP.record('o2').state === CHAN.CLOSING);
  LP.gone('o2');
  // Variant: other order — new closes first, old is OPEN: pointer settles on old (positive control)
  LP.allocate('o3', 'mo3', 'out'); LP.open('o3'); LP.bind('mo3', ID);
  LP.allocate('n3', 'mn3', 'in');  LP.open('n3'); LP.bind('mn3', ID);
  check('P3 setup: pointer at newest', LP.peer(ID)?.t === 'n3');
  LP.closing('n3');
  check('P3 new closing, old OPEN: pointer settles on the OPEN alternate (positive control)', LP.peer(ID)?.t === 'o3' && LP.stats().peersPointing === 1);
  LP.unbind('mn3'); LP.gone('n3');
  check('P3 after new is gone: pointer still on old', LP.peer(ID)?.t === 'o3');
  LP.closing('o3'); LP.unbind('mo3'); LP.gone('o3');
  check('P3 all gone: no record', LP.peer(ID) === null && LP.chanAll() === 0);
  // Variant: a NEGOTIATING alternate is pointable; a CLOSING one is not
  LP.allocate('o4', 'mo4', 'out'); LP.open('o4'); LP.bind('mo4', ID);
  LP.allocate('n4', 'mn4', 'out'); LP.negotiating('n4'); LP.bind('mn4', ID);
  check('P4 setup: pointer at the newest (NEGOTIATING) binding', LP.peer(ID)?.t === 'n4');
  LP.closing('o4');
  check('P4 old closing: pointer stays on the negotiating alternate', LP.peer(ID)?.t === 'n4');
  LP.closing('n4');
  check('P4 both closing: no pointer', LP.peer(ID) === null && LP.chanAll() === 2);
  LP.gone('o4'); LP.gone('n4');
  // A bind that arrives for a channel already CLOSING is stale and points nowhere
  LP.allocate('o5', 'mo5', 'out'); LP.open('o5'); LP.closing('o5');
  const st0 = LP.stats().staleEvent; LP.bind('mo5', ID);
  check('P5 bind on a CLOSING channel is stale; no pointer', LP.peer(ID) === null && LP.stats().staleEvent === st0 + 1);
  LP.gone('o5'); LP.dispose();

  // case 4 shape: an old channel CLOSING beside a new one to the same identity
  L.allocate('t4', 'c4', 'out'); L.negotiating('t4'); L.open('t4'); L.bind('c4', 'dd'.repeat(33));
  L.closing('t4', 'swap-out');
  L.allocate('t5', 'c4', 'out'); L.negotiating('t5'); L.open('t5'); L.bind('c4', 'dd'.repeat(33));
  check('A case 4: both channels counted; the peer points at the new one', L.chanAll() === 2 && L.peer('dd'.repeat(33))?.t === 't5');
  L.gone('t4');
  check('A case 4: gone(t_old) releases only t_old; the pointer is untouched', L.chanAll() === 1 && L.peer('dd'.repeat(33))?.t === 't5');
  L.dispose(); LE.dispose(); LT.dispose(); LZ.dispose();
}

// ── Part B: the real MeshManager with a fake RTCPeerConnection ────────
class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } }
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this._dc = null; this.closeCalls = 0; this.throwOnSecondClose = false; }
  createDataChannel() { this._dc = new FakeDC(); return this._dc; }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.closeCalls++; if (this.throwOnSecondClose && this.closeCalls >= 2) throw new Error('close threw'); this.connectionState = 'closed'; if (FakePC.fireClosedOnClose) queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
  fireClosed() { this.connectionState = 'closed'; this.onconnectionstatechange?.(); }
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

  // B3: escalation when the PC never reports 'closed' (R3-A): a second
  // pc.close() is forced, the record stays CLOSING and charged, and only the
  // transport's 'closed' releases it.
  {
    FakePC.fireClosedOnClose = false;
    const mesh = mkMesh({ closeEscalateMs: 30 });
    await mesh._initiateTo('c3'); await tick();
    const st = mesh._peers.get('c3'); const pc = st.pc;
    st.dc.onopen();
    mesh._retire('c3', 'silent');
    await new Promise(r => setTimeout(r, 10));
    check('B3 before escalation: CLOSING held, one close() so far', mesh.ledgerStats().byState.CLOSING === 1 && pc.closeCalls === 1);
    await new Promise(r => setTimeout(r, 40));
    const s = mesh.ledgerStats();
    check('B3 escalation forced a second pc.close()', pc.closeCalls === 2);
    check('B3 escalation released NOTHING: still CLOSING, all 1, counted', s.byState.CLOSING === 1 && s.all === 1 && s.closeEscalated === 1);
    pc.fireClosed();
    check('B3 the transport\'s closed releases it', mesh.ledgerStats().all === 0 && mesh.ledgerStats().goneTotal === 1);
    mesh.dispose();
  }
  // B3b: the forced close throws — logged, record still CLOSING; confirmed close later releases
  {
    FakePC.fireClosedOnClose = false;
    const mesh = mkMesh({ closeEscalateMs: 20 });
    await mesh._initiateTo('c3b'); await tick();
    const st = mesh._peers.get('c3b'); const pc = st.pc; pc.throwOnSecondClose = true;
    st.dc.onopen();
    mesh._retire('c3b', 'silent');
    await new Promise(r => setTimeout(r, 40));
    check('B3b throwing forced close: counted, still CLOSING, nothing released', pc.closeCalls === 2 && mesh.ledgerStats().byState.CLOSING === 1 && mesh.ledgerStats().closeEscalated === 1);
    pc.fireClosed();
    check('B3b confirmed close releases', mesh.ledgerStats().all === 0);
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

  // B8: duplicate resolution through the REAL transport (R3-B): the ledger's
  // pointer moves to the winner in the dedup transaction, before the loser's
  // teardown, in both winner directions and in the no-key case.
  const dedupCase = async (label, keyOld, keyNew, expectWinner) => {
    const mesh = mkMesh({ closeEscalateMs: 0 });
    const t = new WebRTCTransport({ mesh, log: () => {} });
    await mesh._initiateTo('h1'); await mesh._initiateTo('h2'); await tick();
    mesh._peers.get('h1').dc.onopen(); mesh._peers.get('h2').dc.onopen();
    const incOld = mesh._peers.get('h1').inc, incNew = mesh._peers.get('h2').inc;
    const hex = 'cd'.repeat(33); const nid = BigInt('0x' + hex);
    t.bindPeer(nid, 'h1', keyOld);
    t.bindPeer(nid, 'h2', keyNew);          // dedup runs here; loser is disconnected (closing)
    const winner = expectWinner === 'new' ? 'h2' : 'h1';
    const winInc = winner === 'h2' ? incNew : incOld;
    check(`${label}: transport winner is ${winner}`, t.meshIdFor(nid) === winner);
    check(`${label}: ledger pointer at the winner before the loser's close`, mesh._ledger.peer(hex)?.t === winInc, String(mesh._ledger.peer(hex)?.t));
    await tick(); await tick();              // the loser's fake PC reports 'closed'
    const s = mesh.ledgerStats();
    check(`${label}: after the loser's close: one OPEN bound channel, unboundOpen 0, peersPointing 1, boundPeers 1`, s.all === 1 && s.byState.OPEN === 1 && s.unboundOpen === 0 && s.peersPointing === 1 && s.boundPeers === 1, JSON.stringify(s));
    check(`${label}: winner record carries the identity`, mesh._ledger.record(winInc)?.nodeId === hex);
    mesh.dispose();
  };
  await dedupCase('B8a new wins (smaller key)', 'zz', 'aa', 'new');
  await dedupCase('B8b old wins (smaller key)', 'aa', 'zz', 'old');
  await dedupCase('B8c no keys: keep existing', null, null, 'old');

  // B9: full churn through the real transport (R3-C): retire → onPeerLost →
  // unbindPeer → transport 'closed' leaves no peer record and no channel.
  {
    const mesh = mkMesh({ closeEscalateMs: 0 });
    const t = new WebRTCTransport({ mesh, log: () => {} });
    for (let i = 0; i < 3; i++) {
      const m = `k${i}`;
      await mesh._initiateTo(m); await tick();
      mesh._peers.get(m).dc.onopen();
      const hex = (i + 1).toString(16).padStart(2, '0').repeat(33);
      t.bindPeer(BigInt('0x' + hex), m);
      mesh._retire(m, 'churn');            // closing first (token leaves the current map) ...
      if (!t._unsubPeerLost) t._onPeerLost(m, 'churn');   // ... then onPeerLost → unbindPeer, in production order; delivered by hand if the transport was not started
      await tick(); await tick();          // 'closed' → gone
    }
    const s = mesh.ledgerStats();
    check('B9 churn: three retire→unbind→closed cycles leave all 0 and boundPeers 0', s.all === 0 && s.boundPeers === 0 && s.goneTotal === 3, JSON.stringify(s));
    check('B9 churn: transport bound set agrees', t.boundPeers().length === 0);
    mesh.dispose();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
