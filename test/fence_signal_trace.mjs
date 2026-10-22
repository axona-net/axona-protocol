#!/usr/bin/env node
// fence_signal_trace — 4.120.0 (David 2026-10-22, "Add the logging and the
// polyfill fix, then roll it"; triage axona.bot 0662d789).
//
//   P. EARLY CANDIDATES ARE QUEUED ON BOTH RUNTIMES. node-datachannel's polyfill
//      returns a truthy, EMPTY RTCSessionDescription before any SDP; a browser
//      returns null. A candidate that arrives before the remote SDP is queued
//      and applied after it, on either shape (mesh.js hasRemoteDescription).
//   L. THE SIGNAL TRACE. The answering side logs every offer that reaches it,
//      named by its sender, before any drop; the dialing side logs the answer
//      and the first remote candidate of each attempt with their age; the
//      transport logs which path each offer and answer took.
//
// Mutants: revert the ICE branch to `peer.pc && peer.pc.remoteDescription` →
// P1 fails (the polyfill shape adds the candidate before the answer); drop the
// offer-recv line → L1; drop answer-recv → L2; log the first candidate on every
// candidate → L3b.
import { readFileSync } from 'node:fs';
import { MeshManager, hasRemoteDescription } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise((r) => setTimeout(r, 0));

class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } }
// Browser shape: remoteDescription is null until set.
class BrowserPC {
  constructor() { this.connectionState = 'new'; this.iceConnectionState = 'new'; this._rd = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.added = []; this.addedBeforeSdp = 0; }
  get remoteDescription() { return this._rd; }
  createDataChannel() { return new FakeDC(); }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this._rd = { type: d.type, sdp: d.sdp }; }
  async addIceCandidate(c) { if (!this._rd) { this.addedBeforeSdp++; throw new Error('Got a remote candidate without remote description'); } this.added.push(c); }
  async getStats() { return new Map(); }
  close() { this.connectionState = 'closed'; }
}
// node-datachannel 0.33.4 shape: remoteDescription is ALWAYS a new object; type undefined until set.
class PolyfillPC extends BrowserPC {
  get remoteDescription() { return { type: this._rd ? this._rd.type : undefined, sdp: this._rd ? this._rd.sdp : '' }; }
}

const mkMesh = (PC) => {
  globalThis.RTCPeerConnection = PC;
  const sent = []; const logs = [];
  const mesh = new MeshManager({ sendSignal: (to, payload) => sent.push({ to, payload }), log: (ev, ctx) => logs.push({ ev, ctx }), ledger: false });
  return { mesh, sent, logs };
};
const cand = (n) => ({ candidate: `candidate:${n} 1 UDP 1 192.0.2.${n} 5000${n} typ host`, sdpMid: '0' });

(async () => {
  console.log('fence_signal_trace: early candidates queued on both runtimes; offer/answer/candidate trace');

  // ── P. early candidates ─────────────────────────────────────────────
  check('P0 hasRemoteDescription: null, {}, a typeless object → false; a typed one → true',
    hasRemoteDescription(null) === false && hasRemoteDescription({ remoteDescription: null }) === false
    && hasRemoteDescription({ remoteDescription: {} }) === false && hasRemoteDescription({ remoteDescription: { type: undefined, sdp: '' } }) === false
    && hasRemoteDescription({ remoteDescription: { type: 'answer', sdp: 'x' } }) === true);

  for (const [name, PC] of [['polyfill', PolyfillPC], ['browser', BrowserPC]]) {
    const O = mkMesh(PC);
    await O.mesh._initiateTo('c1'); await tick();
    const st = O.mesh._peers.get('c1');
    check(`P1 ${name}: precondition — before the answer the PC has no typed remote SDP`, !hasRemoteDescription(st.pc), JSON.stringify(st.pc.remoteDescription));
    await O.mesh.onSignal('c1', { kind: 'ice', candidate: cand(1), attempt: st.attempt }); await tick();
    check(`P1 ${name}: a candidate that beats the answer is QUEUED, not handed to the PC`, st.pc.added.length === 0 && st.pc.addedBeforeSdp === 0 && st.pendingCandidates.length === 1,
      `added=${st.pc.added.length} beforeSdp=${st.pc.addedBeforeSdp} queued=${st.pendingCandidates.length}`);
    await O.mesh.onSignal('c1', { kind: 'sdp-answer', sdp: 'v=0 answer', attempt: st.attempt }); await tick();
    check(`P2 ${name}: the answer flushes the queue into the PC`, st.pc.added.length === 1 && st.pendingCandidates.length === 0);
    await O.mesh.onSignal('c1', { kind: 'ice', candidate: cand(2), attempt: st.attempt }); await tick();
    check(`P3 ${name}: after the answer a candidate goes straight to the PC`, st.pc.added.length === 2);
    const fails = O.logs.filter((l) => l.ev === 'signal-handler-failed');
    check(`P4 ${name}: no signal-handler-failed logged`, fails.length === 0, JSON.stringify(fails));
  }

  // ── L. trace lines ──────────────────────────────────────────────────
  {
    const O = mkMesh(PolyfillPC); const R = mkMesh(PolyfillPC);
    await O.mesh._initiateTo('c9'); await tick();
    const stO = O.mesh._peers.get('c9');
    const offer = O.sent.find((s) => s.payload.kind === 'sdp-offer');
    await R.mesh.onSignal('dialerX', offer.payload); await tick();
    const rec = R.logs.find((l) => l.ev === 'offer-recv');
    check('L1 the answering side logs offer-recv naming the sender and the attempt', rec?.ctx.from === 'dialerX' && rec?.ctx.attempt === offer.payload.attempt, JSON.stringify(rec));
    await O.mesh.onSignal('c9', { kind: 'ice', candidate: cand(3), attempt: stO.attempt }); await tick();
    await O.mesh.onSignal('c9', { kind: 'ice', candidate: cand(4), attempt: stO.attempt }); await tick();
    const firsts = O.logs.filter((l) => l.ev === 'first-remote-candidate');
    check('L3a the dialing side logs first-remote-candidate with its age, before the SDP', firsts.length === 1 && firsts[0].ctx.beforeSdp === true && Number.isFinite(firsts[0].ctx.ms) && firsts[0].ctx.ms >= 0, JSON.stringify(firsts));
    check('L3b … once per attempt, not once per candidate', firsts.length === 1);
    const answer = R.sent.find((s) => s.payload.kind === 'sdp-answer');
    await O.mesh.onSignal('c9', answer.payload); await tick();
    const ans = O.logs.find((l) => l.ev === 'answer-recv');
    check('L2 the dialing side logs answer-recv with the attempt and its age', ans?.ctx.from === 'c9' && ans?.ctx.attempt === stO.attempt && Number.isFinite(ans?.ctx.ms) && ans.ctx.ms >= 0, JSON.stringify(ans));
    const applied = O.logs.find((l) => l.ev === 'answer-applied');
    check('L2b answer-applied follows answer-recv, after setRemoteDescription, with the queued count', applied && O.logs.indexOf(applied) > O.logs.indexOf(ans) && applied.ctx.attempt === stO.attempt && applied.ctx.queued === 2 && stO.pc.added.length === 2, JSON.stringify(applied));
    const sentLog = O.logs.find((l) => l.ev === 'offer-sent');
    check('L4 offer-sent binds the dialer\'s PC incarnation to the wire attempt id (the join key with offer-recv)', sentLog?.ctx.inc === stO.inc && sentLog?.ctx.attempt === offer.payload.attempt && rec?.ctx.attempt === sentLog?.ctx.attempt, JSON.stringify(sentLog));
    check('L5 no trace line carries an SDP body', !O.logs.concat(R.logs).some((l) => ['offer-recv', 'offer-sent', 'answer-recv', 'answer-applied', 'first-remote-candidate'].includes(l.ev) && JSON.stringify(l.ctx).includes('v=0')));
    // A drop must still be traced: an offer refused for a missing attempt id logs offer-recv first.
    const P = mkMesh(PolyfillPC);
    P.mesh._attemptPolicy = { requireFor: () => true };
    await P.mesh.onSignal('legacyY', { kind: 'sdp-offer', sdp: 'v=0 offer' }); await tick();
    check('L1b an offer that is then DROPPED is still traced as offer-recv', P.logs.some((l) => l.ev === 'offer-recv' && l.ctx.from === 'legacyY') && P.logs.some((l) => l.ev === 'signal-missing-attempt'));
  }

  // ── S. statics: the transport traces every outbound path of an offer/answer ──
  {
    const src = readFileSync(new URL('../src/transport/web/index.js', import.meta.url), 'utf8');
    const paths = ['mesh-relay', 'door', 'dropped', 'bridge', 'send-failed'].filter((p) => src.includes(`trace('${p}')`));
    check('S1 sendSignal traces each path: mesh-relay, door, dropped, bridge, send-failed', paths.length === 5, JSON.stringify(paths));
    check('S2 only offers and answers are traced (no per-candidate line)', /payload\.kind === 'sdp-offer' \|\| payload\.kind === 'sdp-answer'/.test(src));
  }

  console.log(`\nfence_signal_trace: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
