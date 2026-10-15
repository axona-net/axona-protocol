#!/usr/bin/env node
// =====================================================================
// fence_pool_reject_frame.mjs — G5 of the two-pool packet (axona-docs
// ca52384; Aster v1.1 bc4fe433 §6, v1.2 b395fe8f G5): THE EXPLICIT
// REFUSAL FRAME.
//
// A refused offer is ANSWERED (`sdp-reject` {v, attempt, generation,
// reason}), not dropped. At the offerer the frame ends only the current,
// never-opened attempt it names, as a failed negotiation with reason
// 'rejected' (row 13: onNegotiationFailed fires, the guard token ends);
// a reject carrying another attempt, for an open channel, or for an
// unknown key is dropped and counted. A legacy responder sends no frame;
// its silence still ends at the negotiation deadline. Generic ACK, socket
// send and timeout are not confirmation.
//
// Oracle (v1.2 G5): eight saturated responders cannot hold a dialer's
// pending work past the bounded deadline — with the frame, pending drops
// to zero as soon as the rejects arrive.
//
// Mutants: apply a reject regardless of attempt → B fails; drop the
// responder's _sendReject → A fails.
// Run: node test/fence_pool_reject_frame.mjs
// =====================================================================
import { MeshManager } from '../src/transport/web/mesh.js';
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
  close() { this.closeCalls++; this.connectionState = 'closed'; queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
}
FakePC.instances = [];
globalThis.RTCPeerConnection = FakePC;
const openDc = (st) => { if (!st.dc) st.pc.ondatachannel({ channel: new FakeDC() }); st.dc.readyState = 'open'; st.dc.onopen(); };

const mk = (ledger) => { const sent = []; const mesh = new MeshManager({ sendSignal: (to, payload) => sent.push({ to, payload }), log: () => {}, ledger }); return { mesh, sent }; };

(async () => {
  console.log('fence_pool_reject_frame: G5 — a refused offer is answered, and the answer binds the attempt');

  // ── A. responder: the frame ──────────────────────────────────────────
  {
    const { mesh, sent } = mk({ pools: makePoolProfile({ M: 1, I: 0, H: 1, cInbound: 0 }), enforce: true, closeEscalateMs: 0 });
    await mesh.onSignal('o1', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-1' }); await tick();
    const f = sent.find((s) => s.to === 'o1' && s.payload.kind === 'sdp-reject');
    check('A the refused offer is answered with sdp-reject {v:1, attempt, generation, reason}', !!f && f.payload.v === 1 && f.payload.attempt === 'att-1' && f.payload.generation === mesh._generation && f.payload.reason === 'pool-I' && mesh.attemptStats.rejectSent === 1, J(f?.payload));
    await mesh.onSignal('o2', { kind: 'sdp-offer', sdp: 'v=0 a' }); await tick();
    check('A2 a LEGACY offer (no attempt id) refused gets no frame: silence and the bounded timeout, as before', !sent.some((s) => s.to === 'o2') && mesh.attemptStats.rejectSent === 1);
    mesh.dispose();
  }
  // ── B. offerer: binding ──────────────────────────────────────────────
  {
    const { mesh } = mk({ closeEscalateMs: 0 });
    const failures = [];
    mesh.onNegotiationFailed((peerId, reason, inc) => failures.push({ peerId, reason, inc }));
    await mesh._initiateTo('r1'); await tick();
    const st = mesh._peers.get('r1');
    const pendingBefore = mesh.pendingNegotiations();
    await mesh.onSignal('r1', { kind: 'sdp-reject', v: 1, attempt: 'not-this', generation: 'g', reason: 'pool-I' });
    check('B1 a reject carrying ANOTHER attempt is dropped and counted; the attempt stands', mesh._peers.get('r1') === st && mesh.attemptStats.rejectStale === 1 && failures.length === 0);
    await mesh.onSignal('r9', { kind: 'sdp-reject', v: 1, attempt: 'x', generation: 'g', reason: 'pool-I' });
    check('B2 a reject for an unknown key is dropped and counted', mesh.attemptStats.rejectStale === 2 && failures.length === 0);
    await mesh.onSignal('r1', { kind: 'sdp-reject', v: 1, attempt: st.attempt, generation: 'g-far', reason: 'pool-I' });
    check('B3 the reject naming the CURRENT attempt ends it: row gone, onNegotiationFailed(\'rejected\') fired with the incarnation, pending fell by one', !mesh._peers.has('r1') && failures.length === 1 && failures[0].reason === 'rejected' && failures[0].inc === st.inc && mesh.pendingNegotiations() === pendingBefore - 1 && mesh.attemptStats.rejectRecv === 1, J(failures));
    check('B3b the reason is readable once by the synchronous caller (lastRejectFor), then gone', mesh.lastRejectFor('r1')?.reason === 'pool-I' && mesh.lastRejectFor('r1') === null);
    await tick(); await tick();
    check('B3c its charge left at the transport\'s close, as every other release does', mesh.ledgerStats().all === 0);
    // an open channel
    await mesh._initiateTo('r2'); await tick();
    const st2 = mesh._peers.get('r2'); openDc(st2);
    await mesh.onSignal('r2', { kind: 'sdp-reject', v: 1, attempt: st2.attempt, generation: 'g', reason: 'pool-I' });
    check('B4 a reject for an OPEN channel is dropped: no frame closes an open channel', mesh._peers.get('r2') === st2 && st2.openedAt > 0 && mesh.attemptStats.rejectStale === 3);
    // a responder's row
    await mesh.onSignal('r3', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-r3' }); await tick();
    await mesh.onSignal('r3', { kind: 'sdp-reject', v: 1, attempt: 'att-r3', generation: 'g', reason: 'x' });
    check('B5 a reject addressed to a RESPONDER row is dropped (only the offerer\'s attempt can be rejected)', mesh._peers.has('r3') && mesh.attemptStats.rejectStale === 4);
    mesh.dispose();
  }
  // ── C. the oracle: eight saturated responders ────────────────────────
  {
    const dialer = mk({ closeEscalateMs: 0 });
    const responders = [];
    for (let i = 0; i < 8; i++) responders.push(mk({ pools: makePoolProfile({ M: 1, I: 0, H: 1, cInbound: 0 }), enforce: true, closeEscalateMs: 0 }));
    for (let i = 0; i < 8; i++) await dialer.mesh._initiateTo(`s${i}`);
    await tick();
    check('C1 eight offers out, eight negotiations pending at the dialer', dialer.mesh.pendingNegotiations() === 8 && dialer.sent.filter((s) => s.payload.kind === 'sdp-offer').length === 8);
    // deliver each offer to its saturated responder, and each reply back
    for (let i = 0; i < 8; i++) {
      const offer = dialer.sent.find((s) => s.to === `s${i}` && s.payload.kind === 'sdp-offer');
      await responders[i].mesh.onSignal('dialer', offer.payload); await tick();
      for (const r of responders[i].sent.splice(0)) await dialer.mesh.onSignal(`s${i}`, r.payload);
    }
    await tick();
    check('C2 every responder answered with a reject and the dialer\'s pending count is ZERO — nothing waits for the deadline', dialer.mesh.pendingNegotiations() === 0 && dialer.mesh.attemptStats.rejectRecv === 8 && responders.every((r) => r.mesh.attemptStats.rejectSent === 1 && r.mesh._peers.size === 0), J({ pending: dialer.mesh.pendingNegotiations(), recv: dialer.mesh.attemptStats.rejectRecv }));
    dialer.mesh.dispose(); for (const r of responders) r.mesh.dispose();
  }
  // ── D. a legacy responder: silence, then the deadline ────────────────
  {
    const { mesh } = mk({ closeEscalateMs: 0 });
    await mesh._initiateTo('legacy'); await tick();
    const st = mesh._peers.get('legacy');
    const realNow = Date.now;
    Date.now = () => realNow() + 31_000;
    const verdict = mesh._reapTick(st);
    Date.now = realNow;
    check('D with no frame the attempt ends at NEGOTIATION_DEADLINE_MS, as 4.113.0: reaped-negotiation', verdict === 'reaped-negotiation' && !mesh._peers.has('legacy'));
    mesh.dispose();
  }

  console.log(`\nfence_pool_reject_frame: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
