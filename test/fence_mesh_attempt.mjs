#!/usr/bin/env node
// fence_mesh_attempt — Socket-is-bootstrap v0.5 (axona-docs 7a27d24):
// ATTEMPT IDS END TO END, PROVISIONAL CHANNELS, MAKE-ROOM AT BIND.
//
// The MeshManager keys a negotiation by the signalling id alone and, before
// this change, a later offer, answer or candidate under one key acted on
// whatever the key held (mesh.js:881–897; Vega 07c5ff8e). Now:
//
//   A. ATTEMPT IDS. The offerer mints an attempt id into its offer and every
//      candidate; the responder stores it with the key and echoes it in its
//      answer and candidates; a frame whose attempt differs from the key's
//      current one is DROPPED and counted at either end; a continuation that
//      resumes after its attempt ended applies nothing.
//   B. REPLACEMENT. A new attempt for a key holding an UNBOUND negotiation
//      replaces it; a new attempt for a key holding an OPEN channel is
//      IGNORED (no unauthenticated frame closes an authenticated channel).
//   C. POLICY. Where the attempt policy requires an id, a frame without one
//      is dropped (offer, answer, candidate); elsewhere a frame without one
//      is legacy and accepted (byte-identical to 4.106.0).
//   D. PROVISIONAL. Under a degree policy, open channels the policy marks
//      provisional are neither counted nor candidates in _enforceDegree; the
//      newest provisional above maxProvisional is retired; a provisional
//      channel still unbound after bindDeadlineMs is retired; a bound one is
//      not (Aster 156d2e1d 1–2).
//   E. MAKE ROOM. retireForNewcomer retires ONE non-provisional incumbent
//      with the newcomer excluded and protections honoured; dryRun selects
//      without retiring; no eligible incumbent → null and nothing retired.
//   F. ONOPEN GUARD. A channel the degree pass retires inside dc.onopen
//      starts no heartbeat or path poll afterwards (Vega 4cd16bde).
//   G. STATICS.
//
// Mutants: drop the stale check in _attemptOk → A; drop the open-ignore → B;
// drop the policy check → C; count provisional in _enforceDegree → D; let
// retireForNewcomer include the newcomer → E; drop the onopen guard → F.
import { readFileSync } from 'node:fs';
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise((r) => setTimeout(r, 0));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } }
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this._dc = null; this.added = []; this.closeCalls = 0; }
  createDataChannel() { this._dc = new FakeDC(); return this._dc; }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate(c) { this.added.push(c); }
  async getStats() { return new Map(); }
  close() { this.closeCalls++; this.connectionState = 'closed'; queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
}
FakePC.instances = [];
globalThis.RTCPeerConnection = FakePC;

const mkMesh = (opts = {}) => {
  const sent = [];
  const mesh = new MeshManager({ sendSignal: (to, payload) => sent.push({ to, payload }), log: () => {}, ledger: false, ...opts });
  return { mesh, sent };
};
const openDc = (st) => { if (!st.dc) st.pc.ondatachannel({ channel: new FakeDC() }); st.dc.readyState = 'open'; st.dc.onopen(); };

(async () => {
  console.log('fence_mesh_attempt: socket-is-bootstrap v0.5 — attempt ids, provisional channels, make-room at bind');

  // ── A. attempt ids ───────────────────────────────────────────────────
  {
    const O = mkMesh(); const R = mkMesh();
    await O.mesh._initiateTo('c1'); await tick();
    const offer = O.sent.find((s) => s.payload.kind === 'sdp-offer');
    const stO = O.mesh._peers.get('c1');
    check('A1 the offerer mints an attempt id and carries it in the offer', typeof offer?.payload.attempt === 'string' && offer.payload.attempt === stO.attempt);
    stO.pc.onicecandidate({ candidate: { type: 'host', protocol: 'udp', address: '1', port: 1, toJSON() { return { c: 1 }; } } });
    const ice = O.sent.find((s) => s.payload.kind === 'ice');
    check('A2 every candidate the offerer sends carries the same attempt id', ice?.payload.attempt === stO.attempt);
    await R.mesh.onSignal('cX', offer.payload); await tick();
    const stR = R.mesh._peers.get('cX');
    const answer = R.sent.find((s) => s.payload.kind === 'sdp-answer');
    check('A3 the responder stores the attempt with the key and echoes it in its answer', stR.attempt === offer.payload.attempt && answer?.payload.attempt === offer.payload.attempt);
    stR.pc.onicecandidate({ candidate: { type: 'host', protocol: 'udp', address: '2', port: 2, toJSON() { return { c: 2 }; } } });
    check('A4 the responder\'s candidates carry it too', R.sent.find((s) => s.payload.kind === 'ice')?.payload.attempt === offer.payload.attempt);
    // stale answer / candidate at the offerer
    await O.mesh.onSignal('c1', { kind: 'sdp-answer', sdp: 'v=0 x', attempt: 'not-this' });
    check('A5 an answer carrying another attempt is dropped at the offerer and counted', stO.pc.remoteDescription === null && O.mesh.attemptStats.stale === 1);
    await O.mesh.onSignal('c1', { kind: 'ice', candidate: { c: 9 }, attempt: 'not-this' });
    check('A6 a candidate carrying another attempt is dropped', stO.pendingCandidates.length === 0 && stO.pc.added.length === 0 && O.mesh.attemptStats.stale === 2);
    await O.mesh.onSignal('c1', answer.payload);
    check('A7 the answer with the right attempt is applied', stO.pc.remoteDescription?.type === 'answer');
    // stale candidate at the responder
    await R.mesh.onSignal('cX', { kind: 'ice', candidate: { c: 7 }, attempt: 'other' });
    check('A8 a stale candidate is dropped at the responder', stR.pc.added.length === 0 && R.mesh.attemptStats.stale === 1);
    // continuation: an offer whose state ended while createOffer awaited sends nothing
    {
      const C = mkMesh();
      const p = C.mesh._initiateTo('c5');          // awaits createOffer
      C.mesh._retire('c5', 'disconnect');          // the attempt ends during the await
      await p; await tick();
      check('A9 an offer continuation whose attempt ended applies nothing (no offer sent)', !C.sent.some((s) => s.payload.kind === 'sdp-offer'));
      C.mesh.dispose();
    }
    // continuation at the responder: the offer handler's answer after replacement
    {
      const D = mkMesh();
      const p = D.mesh.onSignal('cZ', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'att-a' });
      D.mesh._retire('cZ', 'disconnect');
      await p; await tick();
      check('A10 an answer continuation whose attempt ended sends nothing', !D.sent.some((s) => s.payload.kind === 'sdp-answer'));
      D.mesh.dispose();
    }
    O.mesh.dispose(); R.mesh.dispose();
  }

  // ── B. replacement ───────────────────────────────────────────────────
  {
    const R = mkMesh();
    await R.mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'A' }); await tick();
    const first = R.mesh._peers.get('k');
    await R.mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 b', attempt: 'B' }); await tick();
    const second = R.mesh._peers.get('k');
    check('B1 a new attempt for a key holding an UNBOUND negotiation replaces it', second !== first && second.attempt === 'B' && R.mesh.attemptStats.replaced === 1 && first.pc.closeCalls === 1);
    await R.mesh.onSignal('k', { kind: 'ice', candidate: { c: 1 }, attempt: 'A' });
    check('B2 the old attempt\'s frames are stale from that instant', R.mesh.attemptStats.stale === 1 && second.pendingCandidates.length === 0);
    openDc(second);
    await R.mesh.onSignal('k', { kind: 'sdp-offer', sdp: 'v=0 c', attempt: 'C' }); await tick();
    check('B3 a new attempt for a key holding an OPEN channel is IGNORED: the channel stays', R.mesh._peers.get('k') === second && second.state === 'open' && R.mesh.attemptStats.ignoredOnOpen === 1 && second.pc.closeCalls === 0);
    R.mesh.dispose();
  }

  // ── C. policy ────────────────────────────────────────────────────────
  {
    const R = mkMesh();
    R.mesh.setAttemptPolicy({ requireFor: (id) => id.startsWith('d') });
    await R.mesh.onSignal('d1:c3', { kind: 'sdp-offer', sdp: 'v=0 a' }); await tick();
    check('C1 an offer without an attempt id on a key the policy names is dropped', !R.mesh._peers.has('d1:c3') && R.mesh.attemptStats.missing === 1);
    await R.mesh.onSignal('d1:c3', { kind: 'sdp-offer', sdp: 'v=0 a', attempt: 'A' }); await tick();
    const st = R.mesh._peers.get('d1:c3');
    await R.mesh.onSignal('d1:c3', { kind: 'ice', candidate: { c: 1 } });
    check('C2 a candidate without an attempt id on that key is dropped', st.pc.added.length === 0 && st.pendingCandidates.length === 0 && R.mesh.attemptStats.missing === 2);
    await R.mesh.onSignal('c9', { kind: 'sdp-offer', sdp: 'v=0 a' }); await tick();
    const legacy = R.mesh._peers.get('c9');
    await R.mesh.onSignal('c9', { kind: 'ice', candidate: { c: 2 } });
    check('C3 a key the policy does not name accepts legacy frames (no id) as today', legacy && legacy.attempt === null && (legacy.pc.added.length + legacy.pendingCandidates.length) === 1 && R.mesh.attemptStats.missing === 2);
    const L = mkMesh();   // no policy at all
    await L.mesh.onSignal('c1', { kind: 'sdp-offer', sdp: 'v=0 a' }); await tick();
    const lst = L.mesh._peers.get('c1');
    await L.mesh.onSignal('c1', { kind: 'sdp-offer', sdp: 'v=0 b' }); await tick();
    check('C4 with no policy a second legacy offer takes today\'s path (same state, re-offered)', L.mesh._peers.get('c1') === lst && L.mesh.attemptStats.replaced === 0);
    const answers = L.sent.filter((s) => s.payload.kind === 'sdp-answer');
    check('C5 legacy answers carry no attempt field', answers.length >= 1 && answers.every((a) => !('attempt' in a.payload)));
    R.mesh.dispose(); L.mesh.dispose();
  }

  // ── D. provisional ───────────────────────────────────────────────────
  {
    const bound = new Set();
    const { mesh } = mkMesh({ degree: { maxPeers: 2, slack: 0, intervalMs: 0, minUptimeMs: 0, regionOf: (id) => id.startsWith('d') ? null : 'r1', isProtected: () => false } });
    mesh.setDegreePolicy({ isProvisional: (id) => id.startsWith('d') && !bound.has(id), maxProvisional: 2, bindDeadlineMs: 40 });
    // two incumbents (non-provisional, same region so one is retirable)
    for (const id of ['c1', 'c2']) { await mesh._initiateTo(id); await tick(); openDc(mesh._peers.get(id)); }
    const retiredBefore = mesh._degreeRetired;
    // three provisional door channels
    for (const id of ['d1', 'd2', 'd3']) { await mesh.onSignal(id, { kind: 'sdp-offer', sdp: 'v=0', attempt: id }); await tick(); openDc(mesh._peers.get(id)); }
    check('D1 provisional channels are not counted by the degree pass (incumbents untouched at cap 2 + 3 provisional)', mesh._degreeRetired === retiredBefore && mesh._peers.has('c1') && mesh._peers.has('c2'));
    check('D2 the newest provisional above maxProvisional is retired', !mesh._peers.has('d3') && mesh._peers.has('d1') && mesh._peers.has('d2') && mesh.degreeStats().provisionalRefused === 1);
    bound.add('d1'); mesh.clearBindDeadline('d1');
    await sleep(70);
    check('D3 a provisional channel still unbound after bindDeadlineMs is retired; the bound one is kept', !mesh._peers.has('d2') && mesh._peers.has('d1') && mesh.degreeStats().bindTimeouts === 1);
    // now d1 is bound: it counts; a third non-provisional over cap triggers the ordinary pass
    await mesh._initiateTo('c3'); await tick(); openDc(mesh._peers.get('c3'));
    check('D4 once bound, a former provisional counts for the pass (3 counted > cap 2 → one retire)', mesh._degreeRetired === retiredBefore + 1);
    mesh.dispose();
  }

  // ── E. make room ─────────────────────────────────────────────────────
  {
    const protectedIds = new Set();
    const { mesh } = mkMesh({ degree: { maxPeers: 50, minUptimeMs: 0, regionOf: (id) => id === 'n' ? 'rn' : (id === 'c1' ? 'ra' : 'rb'), isProtected: (id) => protectedIds.has(id) } });
    mesh.setDegreePolicy({ isProvisional: (id) => id.startsWith('p') });
    for (const id of ['c1', 'c2', 'c3', 'n', 'p1']) { await mesh._initiateTo(id); await tick(); openDc(mesh._peers.get(id)); }
    const dry = mesh.retireForNewcomer('n', { dryRun: true });
    check('E1 dryRun selects without retiring (most over-represented region, newcomer and provisional excluded)', (dry === 'c2' || dry === 'c3') && mesh._peers.size === 5 && mesh.degreeStats().makeRoomRetired === 0);
    const v = mesh.retireForNewcomer('n');
    check('E2 retireForNewcomer retires exactly one incumbent, never the newcomer or a provisional', v === dry && !mesh._peers.has(v) && mesh._peers.has('n') && mesh._peers.has('p1') && mesh.degreeStats().makeRoomRetired === 1);
    check('E3 the victim is in cooldown', mesh._inRetireCooldown(v) === true);
    protectedIds.add('c1'); protectedIds.add(v === 'c2' ? 'c3' : 'c2');
    const none = mesh.retireForNewcomer('n');
    check('E4 with every incumbent protected (or a region\'s last) nothing is retired and null is returned', none === null && mesh._peers.size === 4);
    mesh.dispose();
  }

  // ── F. onopen guard ──────────────────────────────────────────────────
  {
    const { mesh } = mkMesh({ degree: { maxPeers: 1, slack: 0, intervalMs: 0, minUptimeMs: 0, regionOf: () => 'r', isProtected: () => false } });
    await mesh._initiateTo('a'); await tick(); const a = mesh._peers.get('a'); openDc(a);
    await mesh._initiateTo('b'); await tick(); const b = mesh._peers.get('b');
    // make the pass pick the newest: give 'a' protection so 'b' (just opened) is the only eligible one
    mesh._degreeProtected = (id) => id === 'a';
    openDc(b);
    check('F1 the degree pass retired the just-opened channel', !mesh._peers.has('b'));
    // The reaper was armed at initiation and cleared by _retire (the field keeps
    // the cleared handle); what the guard prevents is a heartbeat or path poll
    // STARTED after the retire.
    check('F2 the retired state started no heartbeat and no path poll afterwards', b.pingTimer == null && b.pathPollTimer == null);
    mesh.dispose();
  }

  // ── G. statics ───────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/transport/web/mesh.js', import.meta.url), 'utf8');
    const onopen = src.slice(src.indexOf('dc.onopen = () => {'), src.indexOf('dc.onclose = () => {'));
    check('G1 dc.onopen re-validates its state right after the degree pass', /_enforceDegree\(\);[\s\S]{0,400}if \(this\._peers\.get\(state\.peerId\) !== state\) return;/.test(onopen));
    const ed = src.slice(src.indexOf('_enforceDegree() {'), src.indexOf('ledgerStats() {'));
    check('G2 _enforceDegree excludes provisional channels from the count', /!this\._isProvisional\(st\.peerId\)/.test(ed));
    const rn = src.slice(src.indexOf('retireForNewcomer(newcomerId'), src.indexOf('openNonProvisionalCount() {'));
    check('G3 retireForNewcomer excludes the newcomer and provisional channels and is outside the interval return', /st\.peerId === newcomerId\) continue/.test(rn) && /_isProvisional\(st\.peerId\)\) continue/.test(rn) && !/_degreeInterval/.test(rn));
    const sig = src.slice(src.indexOf('async onSignal(from, payload) {'), src.indexOf('_newPeerState(peerId, role) {'));
    check('G4 onSignal checks the attempt on answer and candidate before applying either', /_attemptOk\(from, peer, payload, 'sdp-answer'\)/.test(sig) && /_attemptOk\(from, peer, payload, 'ice'\)/.test(sig));
  }

  console.log(`\nfence_mesh_attempt: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
