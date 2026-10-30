// =====================================================================
// fence_no_path_defer.mjs — P3 of Mesh Partition Self-Heal v0.2 (axona-docs
// 87a608a; David 2026-10-30 "Build P3 and P2, default off"): an offer the mesh
// COULD NOT DELIVER is a deferral, not a failure of the identity. Flag
// NO_PATH_DEFER (default OFF).
//
// The record it answers (Air slot 1, 2026-10-29/30): 48 offers to 12 Windows
// ids, every one `reach: not-sent` — the offer never left — and every one
// dangled to the 30 s negotiation deadline, counted a failure on the attempt
// guard (four per id, then `expired` for the life of the process; the only
// refill is a presence record no host arms) and wrote a loss mark. Then
// silence. The targets were alive.
//
// Real AxonaPeer over the sim transport, the web transport's dial surfaces
// proxied in (connectViaRelay → a held token; abortNegotiation → a spy;
// onNegotiationFailed → the captured callback). The relay path is driven
// directly (_relayMeshSignal) with the reachability verdict and the route
// stubbed, so the verdict is chosen, not timed.
//
//   D  defaults OFF; env arms; option wins; =0 control. The guard's expiryRefill
//      follows the flag.
//   X  OFF control (the 4.125.0 record): not-sent leaves the token in flight and
//      the candidate gone; the deadline then counts a failure and writes a mark.
//   N  ON, confirmed non-delivery — not-sent, terminal-not-target, no-live-hop,
//      no-capable-terminal: the attempt is aborted (that attempt only), the
//      token is RELEASED (no count), NO mark is written, the candidate is back
//      in the cache with its src and a PATH cooldown, and the line is logged.
//   B  ON, NOT confirmed — bare exhausted (a caught send throw), threw: nothing
//      is aborted or released; bare exhausted is counted; the deadline still
//      counts and marks as today. An undeliverable ANSWER is left alone.
//   C  the cooldown lives on the cache entry: the tick skips a cooling
//      candidate (fair-retry), and dials it once the cooldown ends. The mark
//      table never sees it.
//   G  the guard: with expiryRefill an exhausted identity is allowed ONE attempt
//      per refill window; the window advances at begin; without it, never
//      (today). A fresh presence record still refills fully.
//   M  the real MeshManager: abortNegotiation retires a never-opened attempt
//      with reason no-path, fires NO negotiation-failed listener, clears the
//      deadline; a stale attempt id, an opened channel, an unknown key refuse.
//      The real web composite exposes it.
//
// Mutants: abort by identity instead of attempt → M2; release → end → N
// (attempts counted); requeue without the cooldown → C1; cooldown in DeadPeers
// → C3; treat bare exhausted as confirmed → B1; expiryRefill ignoring the
// window → G2; default ON → D.
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { AttemptGuard }             from '../src/dht/attemptGuard.js';
import { MeshManager }              from '../src/transport/web/mesh.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 8) : x));
const EXPECTED = 38;
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECTED) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECTED} checks`); process.exitCode = 1; } });

const ARMED = { synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 2, directoryMs: 600000, noPathMs: 100 }, attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2, closeGraceMs: 60000 } };

/** A real AxonaPeer on the sim with the web transport's dial surfaces proxied in. */
async function makePeer(net, domain, lat, lng, opts = ARMED) {
  const id = await createNodeIdentity({ lat, lng });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const ctl = { negCb: null, relay: [], aborts: [], abortReturn: true, inc: 'inc-1' };
  const transport = new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'onNegotiationFailed') return (h) => { ctl.negCb = h; return () => { ctl.negCb = null; }; };
      if (prop === 'connectViaRelay') return (hex) => { ctl.relay.push(hex); return ctl.inc; };
      if (prop === 'abortNegotiation') return (hex, attempt, reason) => { ctl.aborts.push({ hex, attempt, reason }); return ctl.abortReturn; };
      if (prop === 'openConnection') return async () => false;   // bound-only: a stranger never binds here
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  if (opts.admissionGate) peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  node._maxSynaptome = 20;
  peer.findKClosest = async () => [];
  peer._bandDiscoveryStep = async () => ({ band: null, probed: false, nominated: 0 });
  const logs = []; peer.onLog('info', (ev, ctx) => logs.push({ ev, ctx }));
  return { peer, id, transport, sim, node, big: fromHex(id.id), hex: id.id, ctl, logs };
}
const stranger = (self, seed) => self ^ (1n << BigInt(seed));
const tick = async (rec) => { rec.peer._deficitBackoff?.reset(); return rec.peer._maintainSynaptome(); };
const noPathLogs = (p) => p.logs.filter((l) => l.ev === 'relay-signal-no-path').map((l) => l.ctx);
/** Nominate S as `src` and run one tick: the dial goes out as a HELD relay attempt. */
async function dialOut(rec, S, src = 'band') {
  rec.peer._nominateCandidate(S, src);
  await tick(rec);
  return rec.ctl.relay.length;
}
/** Drive the relay path for an offer to S with a chosen verdict. */
async function offer(rec, S, { reach, route, kind = 'sdp-offer', attempt = 'a1' }) {
  rec.peer._relayReachable = async () => reach;
  rec.peer._relayReach = new Map();
  if (route) rec.peer.routeMessage = route;
  await rec.peer._relayMeshSignal(S, toHex(S), { kind, sdp: 'v=0', attempt });
}
const guardState = (rec, S) => ({ inflight: rec.peer._attemptGuard.inflightOf(S), attempts: rec.peer._attemptGuard.attemptsOf(S), expired: rec.peer._attemptGuard.expiredOf(S) });
const stopAll = async (...xs) => { for (const x of xs) { try { await x.peer.stop(); } catch { /* */ } try { await x.sim.stop(); } catch { /* */ } } };

(async () => {
  console.log('fence_no_path_defer: P3 — an undeliverable offer is a deferral, not a failure of the identity');

  // ── D. defaults ──────────────────────────────────────────────────────
  {
    const had = Object.prototype.hasOwnProperty.call(process.env, 'NO_PATH_DEFER'), prev = process.env.NO_PATH_DEFER;
    delete process.env.NO_PATH_DEFER;
    const d0 = new AxonaDomain().NO_PATH_DEFER;
    process.env.NO_PATH_DEFER = '1';
    const d1 = new AxonaDomain().NO_PATH_DEFER;
    const d2 = new AxonaDomain({ NO_PATH_DEFER: false }).NO_PATH_DEFER;
    process.env.NO_PATH_DEFER = '0';
    const d3 = new AxonaDomain().NO_PATH_DEFER;
    if (had) process.env.NO_PATH_DEFER = prev; else delete process.env.NO_PATH_DEFER;
    check('D the default is OFF', d0 === false, String(d0));
    check('D env NO_PATH_DEFER=1 arms it; an explicit option wins; NO_PATH_DEFER=0 is a control arm', d1 === true && d2 === false && d3 === false, `${d1} ${d2} ${d3}`);
    const net = new SimNetwork();
    const on = await makePeer(net, new AxonaDomain({ NO_PATH_DEFER: true }), 0, 1);
    const off = await makePeer(net, new AxonaDomain({ NO_PATH_DEFER: false }), 0, 2);
    check('D the guard\'s expiryRefill follows the flag (on → true, off → false)', on.peer._attemptGuard.expiryRefill === true && off.peer._attemptGuard.expiryRefill === false);
    await stopAll(on, off);
  }

  const ON = new AxonaDomain({ NO_PATH_DEFER: true });
  const OFF = new AxonaDomain({ NO_PATH_DEFER: false });

  // ── X. OFF control: the 4.125.0 record ───────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, OFF, 1, 1);
    const S = stranger(A.big, 100);
    const n = await dialOut(A, S);
    check('X0 precondition: the dial went out as a held relay attempt; the candidate left the cache; the token is in flight', n === 1 && !A.peer._fillCache.has(S) && guardState(A, S).inflight === true, J({ n, g: guardState(A, S) }));
    await offer(A, S, { reach: false });
    check('X1 OFF: not-sent aborts nothing, releases nothing, requeues nothing, logs nothing (byte-identical to 4.125.0)',
      A.ctl.aborts.length === 0 && guardState(A, S).inflight === true && !A.peer._fillCache.has(S) && noPathLogs(A).length === 0 && A.peer._heldSrc.size === 0, J({ aborts: A.ctl.aborts, g: guardState(A, S), held: A.peer._heldSrc.size }));
    A.ctl.negCb(S, 'negotiation-timeout', 'inc-1');
    check('X2 OFF: the 30 s deadline then counts a failure on the guard and writes a loss mark — the record this fence exists for',
      guardState(A, S).inflight === false && guardState(A, S).attempts === 1 && A.node._deadPeers.has(S) && A.node._deadPeers.get(S)?.kind === 'loss', J({ g: guardState(A, S), mark: A.node._deadPeers.get(S) }));
    await stopAll(A);
  }

  // ── N. ON, confirmed non-delivery ────────────────────────────────────
  const cases = [
    ['not-sent',             { reach: false }],
    ['terminal-not-target',  { reach: true, route: async () => ({ consumed: false, terminal: true, atNode: 0xabcdn, hops: 2 }) }],
    ['no-live-hop',          { reach: true, route: async () => ({ consumed: false, exhausted: true, stopped: 'no-live-hop', atNode: 1n, hops: 0 }) }],
    ['no-capable-terminal',  { reach: true, route: async () => ({ consumed: false, exhausted: true, stopped: 'no-capable-terminal', atNode: 1n, hops: 1 }) }],
  ];
  for (const [why, verdict] of cases) {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 2, 1);
    const S = stranger(A.big, 101);
    await dialOut(A, S, 'band');
    const heldSrc = A.peer._heldSrc.get(S);
    await offer(A, S, verdict);
    const rec = A.peer._fillCache.get(S);
    const l = noPathLogs(A)[0];
    check(`N ${why}: the attempt is aborted by (peer, attempt), the token RELEASED without a count, NO mark written, the candidate back as band under a cooldown, logged`,
      heldSrc === 'band' && A.ctl.aborts.length === 1 && A.ctl.aborts[0].hex === toHex(S) && A.ctl.aborts[0].attempt === 'a1' && A.ctl.aborts[0].reason === 'no-path'
      && guardState(A, S).inflight === false && guardState(A, S).attempts === 0 && !A.node._deadPeers.has(S)
      && rec && rec.src === 'band' && rec.noPathUntil > Date.now() && rec.noPathUntil <= Date.now() + 160 && A.peer._heldSrc.size === 0
      && l && l.why === why && l.aborted === true && l.released === true && l.requeued === true && l.src === 'band' && l.attempt === 'a1'
      && A.peer._noPathStats.deferred === 1 && A.peer._noPathStats.aborted === 1 && A.peer._noPathStats.requeued === 1,
      J({ heldSrc, aborts: A.ctl.aborts, g: guardState(A, S), marked: A.node._deadPeers.has(S), rec, l, st: A.peer._noPathStats }));
    await stopAll(A);
  }
  {
    // N5 the deadline for an attempt already aborted ends nothing: the token is gone, so a late 'negotiation-timeout' is ignored by the guard and marks nothing.
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 2, 2);
    const S = stranger(A.big, 102);
    await dialOut(A, S);
    await offer(A, S, { reach: false });
    const ignoredBefore = A.peer._attemptGuard.ignoredEnds;
    A.ctl.negCb(S, 'negotiation-timeout', 'inc-1');
    check('N5 a late deadline for the aborted attempt is ignored by the guard (no token) and, with no live attempt, falls to the mark path — which is why the mesh never sends one: abort retires with a non-failure reason',
      A.peer._attemptGuard.ignoredEnds === ignoredBefore + 1 && guardState(A, S).attempts === 0, J({ ignored: A.peer._attemptGuard.ignoredEnds - ignoredBefore, g: guardState(A, S), marked: A.node._deadPeers.has(S) }));
    // N6 the listener receiving reason 'no-path' (defensive): no mark, no guard change.
    A.node._deadPeers.delete(S);
    A.peer._fillCache.delete(S);   // drop the cooling entry so the dial goes out again
    await dialOut(A, S);
    A.ctl.negCb(S, 'no-path', 'inc-1');
    check('N6 a negotiation-failed event with reason no-path writes no mark and ends no token (the kernel kept its own books)', !A.node._deadPeers.has(S) && guardState(A, S).inflight === true, J({ marked: A.node._deadPeers.has(S), g: guardState(A, S) }));
    // N7 requeue refused when the cache is full: counted, the identity is simply not cached (discovery brings it back).
    await offer(A, S, { reach: false });
    const B = await makePeer(net, ON, 2, 3);
    const T = stranger(B.big, 103);
    await dialOut(B, T);
    for (let i = 0; i < 8; i++) B.peer._nominateCandidate(stranger(B.big, 110 + i), 'near');
    await offer(B, T, { reach: false });
    check('N7 with the cache at K_cache the requeue is refused and counted; the abort and the release still happen', B.ctl.aborts.length === 1 && guardState(B, T).inflight === false && !B.peer._fillCache.has(T) && B.peer._noPathStats.requeueRefused === 1, J({ st: B.peer._noPathStats }));
    // N8 a mesh that has nothing to abort (already retired): aborted false, the rest unchanged.
    const C = await makePeer(net, ON, 2, 4);
    const U = stranger(C.big, 104);
    await dialOut(C, U); C.ctl.abortReturn = false;
    await offer(C, U, { reach: false });
    check('N8 when the mesh has nothing to retire (aborted false) the token is still released and the candidate requeued', C.ctl.aborts.length === 1 && noPathLogs(C)[0]?.aborted === false && guardState(C, U).inflight === false && C.peer._fillCache.has(U), J({ l: noPathLogs(C)[0], g: guardState(C, U) }));
    await stopAll(A, B, C);
  }

  // ── B. ON, NOT confirmed ─────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 3, 1);
    const S = stranger(A.big, 105);
    await dialOut(A, S);
    await offer(A, S, { reach: true, route: async () => ({ consumed: false, exhausted: true, atNode: 1n, hops: 0 }) });
    check('B1 a BARE exhausted (a caught send throw, AxonaPeer.js:1374/:6473) is NOT confirmed non-delivery: no abort, no release, no requeue; counted bareExhausted',
      A.ctl.aborts.length === 0 && guardState(A, S).inflight === true && !A.peer._fillCache.has(S) && A.peer._noPathStats.bareExhausted === 1 && noPathLogs(A).length === 0, J({ g: guardState(A, S), st: A.peer._noPathStats }));
    A.ctl.negCb(S, 'negotiation-timeout', 'inc-1');
    check('B1b … and the deadline then counts and marks as today', guardState(A, S).attempts === 1 && A.node._deadPeers.has(S), J({ g: guardState(A, S), marked: A.node._deadPeers.has(S) }));
    const B = await makePeer(net, ON, 3, 2);
    const T = stranger(B.big, 106);
    await dialOut(B, T);
    await offer(B, T, { reach: true, route: async () => { throw new Error('boom'); } });
    check('B2 a route that throws is not confirmed either: nothing aborted or released', B.ctl.aborts.length === 0 && guardState(B, T).inflight === true && B.peer._noPathStats.deferred === 0, J({ g: guardState(B, T) }));
    await offer(B, T, { reach: true, route: async () => ({ consumed: false, atNode: 1n, hops: 1 }) });
    check('B3 not-consumed without a terminal verdict: not confirmed', B.ctl.aborts.length === 0 && guardState(B, T).inflight === true, J({ g: guardState(B, T) }));
    await offer(B, T, { reach: false, kind: 'sdp-answer' });
    check('B4 an undeliverable ANSWER is left to the offerer\'s deadline: no abort, counted answersSkipped', B.ctl.aborts.length === 0 && B.peer._noPathStats.answersSkipped === 1, J(B.peer._noPathStats));
    await offer(B, T, { reach: true, route: async () => ({ consumed: true, hops: 2 }) });
    check('B5 a delivered offer changes nothing here', B.ctl.aborts.length === 0 && guardState(B, T).inflight === true && B.peer._noPathStats.deferred === 0);
    await stopAll(A, B);
  }

  // ── C. the cooldown lives on the cache entry ─────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 4, 1);
    const S = stranger(A.big, 107);
    await dialOut(A, S);
    await offer(A, S, { reach: false });
    const dialsBefore = A.ctl.relay.length;
    await tick(A);
    const rep = A.peer._fillLast;
    check('C1 the next tick SKIPS the cooling candidate: no dial, cooling counted, the candidate still cached, state fair-retry',
      A.ctl.relay.length === dialsBefore && rep.cooling === 1 && A.peer._fillCache.has(S) && rep.state === 'fill-stalled:fair-retry' && A.peer._noPathStats.cooling === 1, J({ rep: { cooling: rep.cooling, state: rep.state, dialed: rep.dialed }, dials: A.ctl.relay.length - dialsBefore }));
    check('C4 nominate refuses a duplicate while cooling (the entry is kept, not replaced)', A.peer._nominateCandidate(S, 'near') === false && A.peer._fillCache.get(S)?.noPathUntil > Date.now());
    await wait(170);
    await tick(A);
    check('C2 once the cooldown ends the tick dials it again (a new held attempt)', A.ctl.relay.length === dialsBefore + 1 && guardState(A, S).inflight === true && !A.peer._fillCache.has(S), J({ dials: A.ctl.relay.length - dialsBefore, g: guardState(A, S) }));
    check('C3 the mark table never saw the identity', !A.node._deadPeers.has(S) && A.node._deadPeers.size === 0);
    // C5 the cooldown draws in [T/2, 3T/2] of noPathMs (100 → 50..150).
    const B = await makePeer(net, ON, 4, 2);
    const T = stranger(B.big, 108);
    const draws = [];
    for (let i = 0; i < 12; i++) { B.ctl.relay.length = 0; B.peer._fillCache.delete(T); await dialOut(B, T); await offer(B, T, { reach: false }); draws.push(noPathLogs(B).at(-1).cooldownMs); B.peer._fillCache.delete(T); }
    check('C5 the cooldown is drawn in [T/2, 3T/2] of noPathMs and is not constant', draws.every((d) => d >= 50 && d <= 150) && new Set(draws).size > 1, J(draws));
    await stopAll(A, B);
  }

  // ── G. the guard's refill ────────────────────────────────────────────
  {
    const mk = (refill) => { const g = new AttemptGuard({ maxAttempts: 2, baseMs: 1, refillWindowMs: 40 }); g.expiryRefill = refill; return g; };
    const id = 'ab'.repeat(33);
    const exhaust = (g, t) => { let k = g.begin(id, t); g.end(id, false, t + 1, k); k = g.begin(id, t + 5); g.end(id, false, t + 6, k); };
    const off = mk(false); exhaust(off, 1000);
    check('G1 OFF (today): after exhaustion the identity is never allowed again, however long it waits', off.expiredOf(id) === true && off.allow(id, 1100) === false && off.allow(id, 1_000_000) === false);
    const on = mk(true); exhaust(on, 1000);
    check('G2 ON: exhausted → not allowed inside the refill window; allowed once the window has passed', on.expiredOf(id) === true && on.allow(id, 1040) === false && on.allow(id, 1047) === true, J({ exp: on.expiredOf(id), a40: on.allow(id, 1040), a47: on.allow(id, 1047) }));
    const k = on.begin(id, 1050);
    check('G3 ON: begin advances the window at ISSUE and counts the refill; in flight it is not allowed', on.expiryRefills === 1 && on.inflightOf(id) === true && on.allow(id, 1051) === false);
    on.end(id, false, 1052, k);
    check('G4 ON: a failure after the refill keeps it expired, with one more attempt a window later — one per window, never a burst', on.expiredOf(id) === true && on.allow(id, 1060) === false && on.allow(id, 1093) === true, J({ a60: on.allow(id, 1060), a93: on.allow(id, 1093) }));
    const k2 = on.begin(id, 1095); on.end(id, true, 1096, k2);
    check('G5 ON: a bind clears the entry as before', on.expiredOf(id) === false && on.attemptsOf(id) === 0 && on.allow(id, 1097) === true);
    const p = mk(true); exhaust(p, 2000); p.onFreshRecord(id, 2010);
    check('G6 a fresh presence record still refills the whole budget at once', p.expiredOf(id) === false && p.allow(id, 2011) === true);
  }

  // ── M. the real mesh and composite ───────────────────────────────────
  {
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
    const events = []; const failed = [];
    const mesh = new MeshManager({ sendSignal: () => {}, log: (e, c) => events.push({ e, c }) });
    mesh.onNegotiationFailed((p, r, inc) => failed.push({ p, r, inc }));
    await mesh._initiateTo('p1'); await wait(0);
    const st = mesh._peers.get('p1');
    check('M0 precondition: a never-opened offerer attempt exists with an attempt id and a deadline', !!st && st.openedAt === 0 && typeof st.attempt === 'string' && mesh._negotiationDeadline.has('p1'));
    check('M1 unknown key → false; stale attempt id → false (the current attempt is untouched)', mesh.abortNegotiation('nobody') === false && mesh.abortNegotiation('p1', 'stale-attempt') === false && mesh._peers.get('p1') === st);
    const ok = mesh.abortNegotiation('p1', st.attempt);
    const td = events.find((x) => x.e === 'teardown' && x.c.peerId === 'p1');
    const ab = events.find((x) => x.e === 'negotiation-aborted');
    check('M2 the named attempt is retired with reason no-path: teardown logged, negotiation-aborted logged with inc and attempt, the key gone, the deadline cleared',
      ok === true && !mesh._peers.has('p1') && td?.c.reason === 'no-path' && ab?.c.attempt === st.attempt && ab?.c.inc === st.inc && !mesh._negotiationDeadline.has('p1'), J({ ok, td: td?.c, ab: ab?.c }));
    check('M3 NO negotiation-failed listener fired (no-path is not a failure of the identity)', failed.length === 0, J(failed));
    await mesh._initiateTo('p2'); await wait(0);
    const st2 = mesh._peers.get('p2'); st2.dc.onopen();
    check('M4 an OPENED channel is refused (nothing to abort; it is a live peer)', mesh.abortNegotiation('p2', st2.attempt) === false && mesh._peers.has('p2'));
    await mesh._initiateTo('p3'); await wait(0);
    const st3 = mesh._peers.get('p3');
    check('M5 attempt null aborts whatever never-opened attempt the key holds', mesh.abortNegotiation('p3', null) === true && !mesh._peers.has('p3') && st3.openedAt === 0);
    const { webTransport } = await import('../src/transport/web/index.js');
    class FakeWebSocket { constructor() { this.readyState = 0; this._l = new Map(); } addEventListener(t, h) { if (!this._l.has(t)) this._l.set(t, new Set()); this._l.get(t).add(h); } send() {} close() {} }
    const alice = await createNodeIdentity({ lat: 5, lng: 5 });
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWebSocket, handshakeTimeoutMs: 1000 });
    check('M6 the real web composite exposes abortNegotiation, answering false for a key it does not hold', typeof t.abortNegotiation === 'function' && t.abortNegotiation('ff'.repeat(33), 'x', 'no-path') === false);
    mesh.dispose?.();
  }

  if (passed + failed !== EXPECTED) { console.log(`  ✗ ran ${passed + failed} of ${EXPECTED} checks`); failed++; }
  console.log(`\nfence_no_path_defer: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
