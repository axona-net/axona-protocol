#!/usr/bin/env node
// =====================================================================
// fence_pool_attempt_fence.mjs — G2 of the two-pool packet (axona-docs
// ca52384; Aster v1.1 bc4fe433 §3, v1.2 b395fe8f G2): CLASSIFICATION AND
// THE ATTEMPT FENCE.
//
//   A. RESERVE BEFORE RETIRE. A new attempt for a key holding an UNBOUND
//      negotiation first reserves the replacement; refused at cap, the OLD
//      attempt stays and the offer is rejected on the wire — the key never
//      ends with neither (the a1bc5e0b ordering). Under room the old attempt
//      is retired after the reservation and, once the transport confirms
//      its close, exactly ONE charge remains.
//   B. A DELAYED CALLBACK from the retired attempt neither charges nor
//      reclassifies: its answer is dropped as stale.
//   C. AN OPEN authenticated channel is never replaced by a new attempt.
//   D. BOUNDED REPLACEMENT: past ATTEMPT_REPLACE_MAX replacements within
//      the negotiation window the key SHEDS further attempts (reject 'shed').
//   E. THE RETRY takes a fresh reservation in the same class; while the old
//      PC is CLOSING two incarnations are charged (v1.1 §2) and after its
//      close one remains; a refused retry ends the attempt as
//      'retry-refused' through onNegotiationFailed.
//   F. THE DEDUP LOSER: when two channels bind one identity, the ledger's
//      pointer and class move to the winner and the loser's charge leaves
//      at its close.
//
// Mutants: restore the retire-then-reserve order in the offer branch → A2
// fails; drop the shed bound → D fails.
// Run: node test/fence_pool_attempt_fence.mjs
// =====================================================================
import { MeshManager } from '../src/transport/web/mesh.js';
import { WebRTCTransport } from '../src/transport/web/webrtc.js';
import { makePoolProfile } from '../src/transport/web/pool_profile.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise((r) => setTimeout(r, 0));
const J = (v) => JSON.stringify(v);

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
  close() { this.closeCalls++; this.connectionState = 'closed'; if (!FakePC.holdClosed) queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
  fireClosed() { this.connectionState = 'closed'; this.onconnectionstatechange?.(); }
}
FakePC.instances = []; FakePC.holdClosed = false;
globalThis.RTCPeerConnection = FakePC;
const openDc = (st) => { if (!st.dc) st.pc.ondatachannel({ channel: new FakeDC() }); st.dc.readyState = 'open'; st.dc.onopen(); };

const mk = (pools, extra = {}) => {
  const sent = [];
  const mesh = new MeshManager({ sendSignal: (to, payload) => sent.push({ to, payload }), log: () => {}, ledger: { pools, enforce: true, closeEscalateMs: 0, ...extra } });
  return { mesh, sent };
};

(async () => {
  console.log('fence_pool_attempt_fence: G2 — reserve, then retire; the key never ends with neither');

  // ── A. reserve before retire ─────────────────────────────────────────
  {
    const { mesh, sent } = mk(makePoolProfile({ M: 1, I: 1, H: 2 }));
    FakePC.instances.length = 0;
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-1' }); await tick();
    const st1 = mesh._peers.get('k');
    check('A1 first offer: one I channel, attempt att-1', st1?.attempt === 'att-1' && mesh.classOf('k') === 'I' && mesh.ledgerStats().byClass.I.charged === 1);
    // at cap (I = 1, and the old attempt is still charged): the replacement cannot reserve
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 b', attempt: 'att-2' }); await tick();
    const rej = sent.find((s) => s.to === 'k' && s.payload.kind === 'sdp-reject');
    check('A2 replacement at cap: the reservation is refused BEFORE any retire — the old attempt stays (same state, same PC), the new one is rejected on the wire', mesh._peers.get('k') === st1 && st1.attempt === 'att-1' && st1.pc === FakePC.instances[0] && rej?.payload.attempt === 'att-2' && rej.payload.reason === 'pool-I' && mesh.attemptStats.replaced === 0, J({ att: mesh._peers.get('k')?.attempt, rej: rej?.payload }));
    check('A2b the key never ended with neither: rows 1, PCs 1, I charged 1', mesh._peers.size === 1 && FakePC.instances.length === 1 && mesh.ledgerStats().byClass.I.charged === 1);
    mesh.dispose();
  }
  {
    const { mesh, sent } = mk(makePoolProfile({ M: 1, I: 2, H: 3 }));
    FakePC.instances.length = 0;
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-1' }); await tick();
    const old = mesh._peers.get('k');
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 b', attempt: 'att-2' }); await tick();
    const nu = mesh._peers.get('k');
    check('A3 replacement under room: the reservation holds, the old attempt is retired, the new one is the key\'s current attempt', nu !== old && nu.attempt === 'att-2' && mesh.attemptStats.replaced === 1 && !sent.some((s) => s.payload.kind === 'sdp-reject'));
    await tick(); await tick();
    check('A3b after the old PC\'s close is confirmed exactly ONE I charge remains', mesh.ledgerStats().byClass.I.charged === 1 && mesh.ledgerStats().goneTotal === 1 && FakePC.instances.length === 2, J(mesh.ledgerStats().byClass));
    // B. a delayed callback from the retired attempt
    await mesh.onSignal('k', { kind: 'ice', candidate: { c: 1 }, attempt: 'att-1' });
    check('B a delayed frame from the retired attempt is dropped as stale: no charge, no class change', mesh.attemptStats.stale === 1 && nu.pendingCandidates.length === 0 && mesh.ledgerStats().byClass.I.charged === 1 && mesh.classOf('k') === 'I');
    mesh.dispose();
  }
  // ── C. an OPEN channel is never replaced ─────────────────────────────
  {
    const { mesh, sent } = mk(makePoolProfile({ M: 1, I: 2, H: 3 }));
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-1' }); await tick();
    const st = mesh._peers.get('k'); openDc(st);
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 b', attempt: 'att-9' }); await tick();
    check('C a new attempt for a key holding an OPEN channel is ignored: no retire, no reservation, no reject', mesh._peers.get('k') === st && mesh.attemptStats.ignoredOnOpen === 1 && mesh.ledgerStats().reservations === 0 && !sent.some((s) => s.payload.kind === 'sdp-reject'));
    mesh.dispose();
  }
  // ── D. bounded replacement ───────────────────────────────────────────
  {
    const { mesh, sent } = mk(makePoolProfile({ M: 1, I: 8, H: 9 }));
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-0' }); await tick();
    for (let i = 1; i <= 3; i++) { await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 b', attempt: `att-${i}` }); await tick(); }
    check('D three replacements within the window are taken', mesh.attemptStats.replaced === 3 && mesh._peers.get('k')?.attempt === 'att-3');
    await mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 b', attempt: 'att-4' }); await tick();
    const shed = sent.find((s) => s.payload.kind === 'sdp-reject' && s.payload.reason === 'shed');
    check('D the fourth is SHED: rejected with reason shed, the key keeps att-3, nothing reserved', shed?.payload.attempt === 'att-4' && mesh.attemptStats.shed === 1 && mesh._peers.get('k')?.attempt === 'att-3' && mesh.ledgerStats().reservations === 0);
    mesh.dispose();
  }
  // ── E. the retry ─────────────────────────────────────────────────────
  {
    const { mesh } = mk(makePoolProfile({ M: 2, I: 1, H: 3 }));
    FakePC.instances.length = 0;
    await mesh._initiateTo('c1'); await tick();
    const st = mesh._peers.get('c1');
    FakePC.holdClosed = true;                   // the old PC's 'closed' is withheld
    st.pc.connectionState = 'failed'; mesh._onConnState(st, 'failed');
    check('E1 pc failed: one retry scheduled', !!st.retryTimer && st.retryUsed === true);
    // fire the retry now instead of waiting RETRY_AFTER_MS
    clearTimeout(st.retryTimer); st.retryTimer = null;
    mesh._retire('c1', 'retry', { keepDeadline: true, notifyLost: false });
    await mesh._initiateTo('c1', { cls: st.cls }); await tick();
    const st2 = mesh._peers.get('c1');
    check('E2 the retry took a FRESH M reservation: two incarnations charged while the old PC is CLOSING (v1.1 §2)', st2 && st2 !== st && st2.cls === 'M' && mesh.ledgerStats().byClass.M.charged === 2 && mesh.ledgerStats().byState.CLOSING === 1, J(mesh.ledgerStats().byClass));
    FakePC.holdClosed = false;
    st.pc.fireClosed(); await tick();
    check('E3 the old incarnation\'s close releases one: M charged 1, the retry\'s channel is the one left', mesh.ledgerStats().byClass.M.charged === 1 && mesh._peers.get('c1') === st2);
    // a refused retry
    await mesh._initiateTo('c2'); await tick();                 // M now 2 of 2
    const stX = mesh._peers.get('c2');
    const failures = [];
    mesh.onNegotiationFailed((peerId, reason, inc) => failures.push({ peerId, reason, inc }));
    FakePC.holdClosed = true;
    stX.pc.connectionState = 'failed'; mesh._onConnState(stX, 'failed');
    clearTimeout(stX.retryTimer); stX.retryTimer = null;
    // replay what the retry timer does, with M full (c1 + closing c2 both charged)
    mesh._retire('c2', 'retry', { keepDeadline: true, notifyLost: false });
    await mesh._initiateTo('c2', { cls: stX.cls }); await tick();
    check('E4 a retry whose reservation is refused starts nothing (the old charge is still held)', !mesh._peers.has('c2') && mesh.ledgerStats().byClass.M.charged === 2);
    FakePC.holdClosed = false; stX.pc.fireClosed(); await tick();
    check('E4b and its old incarnation releases at the transport\'s close', mesh.ledgerStats().byClass.M.charged === 1);
    mesh.dispose();
  }
  // ── E'. the retry timer itself (the production path) ends a refused retry as 'retry-refused'
  {
    const { mesh } = mk(makePoolProfile({ M: 1, I: 1, H: 2 }));
    const failures = [];
    mesh.onNegotiationFailed((peerId, reason) => failures.push({ peerId, reason }));
    await mesh._initiateTo('c1'); await tick();
    const st = mesh._peers.get('c1');
    FakePC.holdClosed = true;
    st.pc.connectionState = 'failed'; mesh._onConnState(st, 'failed');
    // run the timer's body now
    const timerFn = st.retryTimer && st.retryTimer._onTimeout ? st.retryTimer._onTimeout : null;
    if (timerFn) { clearTimeout(st.retryTimer); st.retryTimer = null; timerFn(); await tick(); }
    check('E5 the retry timer, refused at M = 1 while the old PC is CLOSING, reports retry-refused through onNegotiationFailed and leaves no row', !!timerFn && failures.some((f) => f.peerId === 'c1' && f.reason === 'retry-refused') && !mesh._peers.has('c1'), J(failures));
    FakePC.holdClosed = false; st.pc.fireClosed(); await tick();
    mesh.dispose();
  }
  // ── F. the dedup loser ───────────────────────────────────────────────
  {
    const { mesh } = mk(makePoolProfile({ M: 2, I: 2, H: 4 }));
    const w = new WebRTCTransport({ mesh, log: () => {} });
    await mesh._initiateTo('m1'); await tick();
    await mesh.onSignal('m2', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'a' }); await tick();
    openDc(mesh._peers.get('m1')); openDc(mesh._peers.get('m2'));
    const nodeId = BigInt('0x' + 'ab'.repeat(33));
    w.bindPeer(nodeId, 'm1', 'key-b');
    w.bindPeer(nodeId, 'm2', 'key-a');                  // smaller key wins: m2 (the I channel) survives
    const hex = 'ab'.repeat(33);
    check('F the dedup moves the pointer to the winner in the same step: the identity\'s class is the winner\'s (I), the loser is retiring', w.meshIdFor(nodeId) === 'm2' && mesh._ledger.classOfNode(hex) === 'I' && !mesh._peers.has('m1') && mesh.ledgerStats().byState.CLOSING === 1);
    await tick(); await tick();
    check('F2 the loser\'s charge leaves at its close: M charged 0, I charged 1', mesh.ledgerStats().byClass.M.charged === 0 && mesh.ledgerStats().byClass.I.charged === 1);
    mesh.dispose();
  }

  console.log(`\nfence_pool_attempt_fence: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
