#!/usr/bin/env node
// =====================================================================
// fence_pool_reservation.mjs — G1 of the two-pool packet (axona-docs
// ca52384; Aster v1.1 bc4fe433 §1–§2, v1.2 b395fe8f G1): THE PHYSICAL
// BOUND AND THE REFUSAL-CLOSE PATH.
//
// Part A drives the ledger alone: a RESERVATION precedes every allocation
// and carries its class; charged + reserved is bounded per class and in
// total; allocate() CONSUMES the token; release() frees an unconsumed one;
// a token not consumed within reserveTtlMs expires; CLOSING keeps its class
// charge until gone() for the SAME incarnation; an unarmed ledger counts
// exactly as 4.113.0 (direction-only bounds, no class on any record).
//
// Part B drives the REAL MeshManager with a fake RTCPeerConnection, armed
// M50/I0 with enforce on: forty unsolicited offers build ZERO PCs and each
// is answered with a reject frame (v1.2 G1 oracle: "M50, I0 plus 40
// unsolicited offers never creates 90 unclassified PCs"); an outbound dial
// is charged M and released only when the transport confirms the close.
//
// Mutants: make allocate() charge without consuming a token → A3 fails;
// release the charge in closing() instead of gone() → A6 fails; drop the
// class bound in reserve() → B1 fails.
// Run: node test/fence_pool_reservation.mjs
// =====================================================================
import { ChannelLedger, CHAN, CLS, LEDGER_DEFAULTS } from '../src/transport/web/channel_ledger.js';
import { makePoolProfile } from '../src/transport/web/pool_profile.js';
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise((r) => setTimeout(r, 0));
const J = (v) => JSON.stringify(v);

console.log('fence_pool_reservation: G1 — reserve before anything physical; one release, at gone');

// ── Part A: the ledger alone ──────────────────────────────────────────
{
  let now = 1000;
  const P = makePoolProfile({ M: 2, I: 1, H: 3, pPending: 2, cInbound: 1 });
  const L = new ChannelLedger({ pools: P, enforce: true, closeEscalateMs: 0, now: () => now, reserveTtlMs: 100 });
  check('A1 an armed ledger reads its bounds from the profile (pPending, cInbound, H as cPhys)', L.armed && L.pPending === 2 && L.cInbound === 1 && L.cPhys === 3 && L.stats().pools?.M === 2);

  const r1 = L.reserve({ cls: 'M', dir: 'out', attempt: 'a1' });
  const r2 = L.reserve({ cls: 'M', dir: 'out', attempt: 'a2' });
  const r3 = L.reserve({ cls: 'M', dir: 'out', attempt: 'a3' });
  check('A2 M is bounded by charged + reserved: the third M reservation is refused with pool-M, nothing allocated', r1.ok && r2.ok && !r3.ok && r3.why === 'pool-M' && L.reservedOf(CLS.M) === 2 && L.chanAll() === 0, J(r3));

  const rec = L.allocate('t1', 'c1', 'out', r1.token);
  check('A3 allocate CONSUMES the token and the channel takes its class', rec.cls === 'M' && L.reservedOf(CLS.M) === 1 && L.chargedOf(CLS.M) === 1 && L.reservation(r1.token) === null && L.stats().reservationsConsumed === 1);
  const again = L.reserve({ cls: 'M', dir: 'out' });
  check('A3b still full: 1 charged + 1 reserved = M', !again.ok && again.why === 'pool-M');

  check('A4 release frees an unconsumed token; a second release is a stale event', L.release(r2.token) === true && L.reservedOf(CLS.M) === 0 && L.release(r2.token) === false && L.stats().reservationsReleased === 1);

  const ri = L.reserve({ cls: 'I', dir: 'in', attempt: 'b1' });
  const ri2 = L.reserve({ cls: 'I', dir: 'in', attempt: 'b2' });
  check('A5 I is bounded on its own: one I reservation, the second refused pool-I; M untouched', ri.ok && !ri2.ok && ri2.why === 'pool-I' && L.chargedOf(CLS.M) === 1);
  L.allocate('t2', 'c2', 'in', ri.token);
  const rh1 = L.reserve({ cls: 'M', dir: 'out' });
  const rh2 = L.reserve({ cls: 'M', dir: 'out' });
  check('A5b H bounds the total: with M 1 + I 1 charged and one M reserved, the next M reservation is refused with phys though M itself has room', rh1.ok && !rh2.ok && rh2.why === 'phys', J(rh2));
  L.allocate('t5', 'c5', 'out', rh1.token);   // M now 2 of 2, H 3 of 3

  L.closing('t1', 'test');
  const rClosing = L.reserve({ cls: 'M', dir: 'out' });
  check('A6 CLOSING keeps the M charge: the pool reads full until the transport confirms', L.record('t1').state === CHAN.CLOSING && L.chargedOf(CLS.M) === 2 && !rClosing.ok, J(rClosing));
  const gStale = L.gone('t-older');
  check('A6b gone for another incarnation releases nothing', gStale === null && L.chargedOf(CLS.M) === 2 && L.stats().staleEvent >= 1);
  L.gone('t1');
  const rAfter = L.reserve({ cls: 'M', dir: 'out' });
  check('A6c gone for the matching incarnation is the ONE release: M has room again', L.chargedOf(CLS.M) === 1 && rAfter.ok, J(rAfter));
  L.release(rAfter.token);

  // expiry
  const rx = L.reserve({ cls: 'M', dir: 'out' });
  now += 101;
  const rafter = L.reserve({ cls: 'M', dir: 'out' });
  check('A7 a reservation older than reserveTtlMs expires at the next reserve (never a charge, never held past the negotiation window)', rx.ok && L.reservation(rx.token) === null && L.stats().reservationsExpired === 1 && rafter.ok, J(L.stats()));
  L.release(rafter.token);

  // reclass (t2 opens and binds first: an unbound pre-open inbound still counts on the inbound row)
  L.open('t2'); L.bind('c2', 'cd'.repeat(33));
  const t2 = L.record('t2');
  const rc = L.reclass('t2', 'M');
  check('A8 reclass I→M moves the live channel\'s class without a second allocation', t2.cls === 'M' && rc.ok && rc.prev === 'I' && L.chargedOf(CLS.I) === 0 && L.chargedOf(CLS.M) === 2);
  const ri4 = L.reserve({ cls: 'I', dir: 'in' });
  L.allocate('t4', 'c4', 'in', ri4.token);
  const rc2 = L.reclass('t4', 'M');
  check('A8b reclass into a FULL class is refused with enforce on, the channel keeps its class', ri4.ok && !rc2.ok && rc2.why === 'pool-M' && L.record('t4').cls === 'I' && L.stats().reclassRefused === 1, J(rc2));

  // enforce off: counted, never refused, tokens still issued
  const C = new ChannelLedger({ pools: makePoolProfile({ M: 1, I: 0, H: 2 }), enforce: false, closeEscalateMs: 0 });
  const c1 = C.reserve({ cls: 'M', dir: 'out' }); const c2 = C.reserve({ cls: 'M', dir: 'out' });
  check('A9 enforce off (the default): a would-be refusal is COUNTED by class and a token is still issued', c1.ok && c2.ok && c2.why === 'pool-M' && C.stats().wouldRefuseM === 1 && C.stats().refusedM === 0);

  // unarmed: byte-identical to the direction-only predicate
  const U = new ChannelLedger({ cPhys: 2, cInbound: 1, pPending: 1, enforce: true, closeEscalateMs: 0 });
  const u1 = U.reserve({ cls: 'M', dir: 'out' });
  const u2 = U.reserve({ cls: 'M', dir: 'out' });
  const recU = U.allocate('u1', 'c1', 'out', u1.token);
  check('A10 unarmed: no class on any record, no pool bound; the direction-only rows (pending here) refuse as 4.113.0 did', !U.armed && recU.cls === null && u1.ok && !u2.ok && u2.why === 'pending' && U.stats().pools === null && U.stats().byClass.none.charged === 1);
  check('A11 the defaults are unarmed', LEDGER_DEFAULTS.pools === null && new ChannelLedger().armed === false);
}

// ── Part B: the real MeshManager ──────────────────────────────────────
class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } }
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this._dc = null; this.closeCalls = 0; }
  createDataChannel() { this._dc = new FakeDC(); return this._dc; }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.closeCalls++; this.connectionState = 'closed'; queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
}
FakePC.instances = [];
globalThis.RTCPeerConnection = FakePC;

(async () => {
  console.log('\n  Part B: the mesh under an armed, enforcing ledger');
  const mk = (pools, extra = {}) => {
    const sent = [];
    const mesh = new MeshManager({ sendSignal: (to, payload) => sent.push({ to, payload }), log: () => {}, ledger: { pools, enforce: true, closeEscalateMs: 0, ...extra } });
    return { mesh, sent };
  };
  // B1: M50 / I0 — forty unsolicited offers build nothing
  {
    const { mesh, sent } = mk(makePoolProfile({ M: 50, I: 0, H: 50, cInbound: 0 }));
    FakePC.instances.length = 0;
    for (let i = 0; i < 40; i++) await mesh.onSignal(`u${i}`, { kind: 'sdp-offer', sdp: 'v=0 x', attempt: `att-${i}` });
    await tick();
    const rejects = sent.filter((s) => s.payload.kind === 'sdp-reject');
    check('B1 M50/I0 + 40 unsolicited offers: ZERO PCs built, zero rows kept, ledger charged 0', FakePC.instances.length === 0 && mesh._peers.size === 0 && mesh.ledgerStats().all === 0, J({ pcs: FakePC.instances.length, rows: mesh._peers.size }));
    check('B1b every offer is answered with a reject frame naming the attempt and the reason pool-I', rejects.length === 40 && rejects.every((r, i) => r.payload.attempt === `att-${i}` && r.payload.reason === 'pool-I' && r.payload.v === 1), J(rejects[0]?.payload));
    check('B1c the ledger counted 40 refusals in class I, none in M', mesh.ledgerStats().refusedI === 40 && mesh.ledgerStats().refusedM === 0);
    mesh.dispose();
  }
  // B2: an outbound dial is charged M; the charge survives _retire and leaves at the transport's closed
  {
    const { mesh } = mk(makePoolProfile({ M: 1, I: 1, H: 2 }));
    FakePC.instances.length = 0;
    await mesh._initiateTo('c1'); await tick();
    const st = mesh._peers.get('c1');
    const s1 = mesh.ledgerStats();
    check('B2 the dial reserved M and the PC consumed it: byClass.M.charged 1, reserved 0, state.cls M', s1.byClass.M.charged === 1 && s1.byClass.M.reserved === 0 && s1.reservations === 0 && st.cls === 'M' && mesh.classOf('c1') === 'M', J(s1.byClass));
    await mesh._initiateTo('c2'); await tick();
    check('B2b a second M dial at M = 1 is refused: no state, no PC, allocRefusedFor answers once', !mesh._peers.has('c2') && FakePC.instances.length === 1 && mesh.allocRefusedFor('c2') === true && mesh.allocRefusedFor('c2') === false);
    mesh._retire('c1', 'test');
    check('B2c after _retire the M charge is still held (CLOSING)', mesh.ledgerStats().byClass.M.charged === 1);
    await tick(); await tick();
    check('B2d the transport\'s closed releases it: charged 0', mesh.ledgerStats().byClass.M.charged === 0 && mesh.ledgerStats().goneTotal === 1);
    await mesh._initiateTo('c3'); await tick();
    check('B2e and the next dial is admitted', mesh._peers.has('c3') && mesh.classOf('c3') === 'M');
    mesh.dispose();
  }
  // B3: an inbound offer under room is charged I; the caller's class rides the dial
  {
    const { mesh, sent } = mk(makePoolProfile({ M: 2, I: 1, H: 3 }));
    await mesh.onSignal('p1', { kind: 'sdp-offer', sdp: 'v=0 x', attempt: 'a' }); await tick();
    check('B3 an inbound offer reserves I before the PC and the channel is class I', mesh.classOf('p1') === 'I' && mesh.ledgerStats().byClass.I.charged === 1 && !sent.some((s) => s.payload.kind === 'sdp-reject'));
    await mesh._initiateTo('d1', { cls: 'I' }); await tick();
    check('B3b a dial that names class I is bounded by I (full): refused', !mesh._peers.has('d1'));
    await mesh._initiateTo('d2'); await tick();
    check('B3c a dial with no class named is M', mesh.classOf('d2') === 'M');
    mesh.dispose();
  }
  // B4: unarmed mesh is byte-identical: no class, no reject frame, the 4.113.0 refusal rows
  {
    const sent = [];
    const mesh = new MeshManager({ sendSignal: (to, payload) => sent.push({ to, payload }), log: () => {}, ledger: { cInbound: 1, enforce: true, closeEscalateMs: 0 } });
    await mesh.onSignal('p1', { kind: 'sdp-offer', sdp: 'v=0 x', attempt: 'a' }); await tick();
    await mesh.onSignal('p2', { kind: 'sdp-offer', sdp: 'v=0 x', attempt: 'b' }); await tick();
    check('B4 unarmed: the second unbound inbound is refused by the inbound row (as 4.113.0), no class on the first', mesh._peers.has('p1') && !mesh._peers.has('p2') && mesh.classOf('p1') === null && mesh.poolProfile() === null && mesh.ledgerStats().refusedIn === 1);
    check('B4b unarmed refusal still answers the offer with a reject frame when the offer carried an attempt id (G5 is independent of the pools)', sent.some((s) => s.to === 'p2' && s.payload.kind === 'sdp-reject' && s.payload.reason === 'inbound'));
    mesh.dispose();
  }
  console.log(`\nfence_pool_reservation: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
