// =====================================================================
// fence_mark_automaton.mjs — Hold-and-Fill v0.7 (axona-docs 95c2ff4,
// "Marks"), repair rows 10 and 13: the mark automaton on row 1's marks, and
// class A's signal.
//
// Part A, the module (design cases 6, 37, 38, 43, 44-outbound, 45, 48, 49,
// 50): ELIGIBLE / FAIL / CONSUME / BIND with an injected monotonic clock;
// attempts count failures; the A_max-th failure takes the R_refill schedule;
// the lazy refill; ISSUE advances the window so one window issues at most
// once; a reservation refusal consumes nothing; a wall-clock step moves no
// schedule; MARKS-FULL with eviction of exhausted-and-waiting marks only and
// hysteresis; POLICY-FULL; add() is membership with attempts 0; the row-1
// composition rules still hold.
//
// Part B, the kernel: _localCandidate filters by ELIGIBILITY, not by mark
// existence; _considerCandidate refuses an ineligible identity and CONSUMEs
// at issue; a bind on our own channel deletes the mark (BIND).
//
// Part C, row 13: a never-opened negotiation fires mesh.onNegotiationFailed
// for the failing reasons and not for retry/dispose; WebRTCTransport resolves
// a hex meshId to the identity and names nothing for a bridge handle; the
// kernel writes a mark ONLY when no OPEN channel to the identity exists.
//
// With the automaton reverted to row 1's plain marks, part A's schedule
// checks and part B's eligibility checks fail; with row 13's hooks removed,
// part C fails.
//
// Run: node test/fence_mark_automaton.mjs
// =====================================================================
import { DeadPeers, MARK_DEFAULTS } from '../src/dht/DeadPeers.js';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { MeshManager }              from '../src/transport/web/mesh.js';
import { WebRTCTransport }          from '../src/transport/web/webrtc.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const tick = () => new Promise(r => setTimeout(r, 0));
const H = (b) => b.toString(16).padStart(2, '0').repeat(33);

console.log('fence_mark_automaton: rows 10 + 13');

// ── Part A ────────────────────────────────────────────────────────────
{
  let now = 1000, wall = 5_000_000;
  const mk = (o = {}) => new DeadPeers({ B: 100, factor: 2, A_max: 3, R_refill: 1000, M_marks: 4, M_hyst: 1, M_policy: 2, now: () => now, wall: () => wall, ...o });
  check('A defaults are the design\'s first values', MARK_DEFAULTS.B === 30_000 && MARK_DEFAULTS.A_max === 4 && MARK_DEFAULTS.R_refill === 60_000 && MARK_DEFAULTS.M_marks === 256 && MARK_DEFAULTS.M_hyst === 32 && MARK_DEFAULTS.M_policy === 1024);

  const D = mk();
  const X = 1n;
  check('A unmarked is eligible', D.eligible(X) === true);
  D.fail(X, 'pong-timeout');
  let m = D.get(X);
  check('A first FAIL: attempts 1, dueAt now+B, cause, wall at, no token', m.attempts === 1 && m.dueAt === 1100 && m.cause === 'pong-timeout' && m.at === wall && m.token === 0);
  check('A before due: ineligible', D.eligible(X) === false);
  now = 1100; check('A at due: eligible', D.eligible(X) === true);
  D.consume(X); check('A CONSUME below A_max changes nothing (charged at FAIL)', D.get(X).dueAt === 1100 && D.get(X).attempts === 1);
  D.fail(X, 'negotiation-timeout');
  check('A second FAIL: attempts 2, dueAt now + B*2', D.get(X).attempts === 2 && D.get(X).dueAt === 1300 && D.get(X).cause === 'negotiation-timeout');
  now = 1300; D.fail(X, 'x');
  check('A case 50: the A_max-th FAIL enters exhaustion on the R_refill schedule, not the doubling one', D.get(X).attempts === 3 && D.get(X).dueAt === 2300 && D.get(X).token === 0);
  check('A exhausted, before refill: ineligible', D.eligible(X) === false);
  now = 2299; check('A one ms early: ineligible', D.eligible(X) === false);
  now = 2300;
  check('A case 43: at refill the predicate refills the token and is true', D.eligible(X) === true && D.get(X).token === 1 && D.stats().refilled === 1);
  check('A REFILL never moves dueAt', D.get(X).dueAt === 2300);
  D.consume(X);
  check('A case 48: ISSUE advances the window; token cleared', D.get(X).token === 0 && D.get(X).dueAt === 3300);
  check('A case 48: a second evaluation in the same window is false', D.eligible(X) === false);
  now = 2500; check('A case 48: still false mid-window with no FAIL/BIND between', D.eligible(X) === false);
  D.fail(X, 'y');
  check('A exhausted FAIL re-latches at now + R_refill, attempts stays A_max', D.get(X).attempts === 3 && D.get(X).dueAt === 3500 && D.get(X).token === 0);
  now = 3500; D.eligible(X); D.consume(X); now = 4500; D.eligible(X); D.consume(X); now = 5500;
  check('A case 43: across three refill windows exactly three exhausted issues (consumed 4 = 1 below A_max + 3)', D.stats().consumed === 4 && D.get(X).dueAt === 5500, String(D.stats().consumed));
  // wall-clock step moves no schedule
  wall += 3_600_000; check('A a wall-clock step of +1h moves no schedule', D.get(X).dueAt === 5500 && D.eligible(X) === true);
  wall -= 7_200_000; check('A a wall-clock step of -1h moves no schedule', D.get(X).dueAt === 5500);
  // BIND deletes; next loss starts at 1
  D.bind(X); check('A BIND deletes the mark', !D.has(X) && D.eligible(X) === true);
  D.fail(X, 'z'); check('A after BIND the next loss starts at attempts 1', D.get(X).attempts === 1);
  D.clear();

  // (case 45's actual refused reservation is exercised on the kernel in part B)

  // R10/13-C: two bounds, one Map. M_marks bounds LOSS marks, M_policy bounds
  // POLICY marks; policy marks never latch MARKS-FULL and are never evicted.
  const Q = new DeadPeers({ M_marks: 2, M_hyst: 1, M_policy: 4, now: () => now, wall: () => wall });
  Q.mark(30n, { kind: 'policy' }); Q.mark(31n, { kind: 'policy' }); Q.mark(32n, { kind: 'policy' });
  check('A C: three policy marks with M_marks 2: size 3, loss 0, policy 3, MARKS-FULL not latched', Q.size === 3 && Q.lossCount() === 0 && Q.stats().policy === 3 && !Q.marksFull());
  Q.fail(33n); Q.fail(34n);
  check('A C: two loss marks then latch MARKS-FULL at M_marks 2 (policy marks not counted)', Q.lossCount() === 2 && Q.marksFull());
  check('A C: a third loss mark is refused (no exhausted mark to evict), policy marks untouched', Q.fail(35n) === null && Q.size === 5 && Q.stats().policy === 3);
  Q.mark(36n, { kind: 'policy' });
  check('A C: fourth policy mark fills M_policy; a fifth is refused and counted', Q.policyFull() && (Q.mark(37n, { kind: 'policy' }), !Q.has(37n)) && Q.stats().policyRefused === 1);
  Q.mark(33n, { kind: 'policy' });
  check('A C: a loss mark becoming policy is refused at POLICY-FULL and stays a loss mark', Q.get(33n).kind === 'loss');
  Q.delete(30n); Q.mark(33n, { kind: 'policy' });
  check('A C: with a policy slot free, a loss mark becomes a policy mark and leaves the loss count (1); MARKS-FULL still latched at hysteresis (lifts below M_marks − M_hyst = 1)', Q.get(33n).kind === 'policy' && Q.lossCount() === 1 && Q.marksFull());
  Q.delete(34n);
  check('A C: loss count 0 → MARKS-FULL lifts; policy marks remain', Q.lossCount() === 0 && !Q.marksFull() && Q.stats().policy === 4);
  Q.clear();

  // add(): membership with attempts 0; composition rules from row 1
  const F = mk(); const Z = 3n;
  F.add(Z);
  check('A add(): mark with attempts 0, cause unknown, due after one B', F.get(Z).attempts === 0 && F.get(Z).cause === 'unknown' && F.get(Z).dueAt === now + 100);
  F.fail(Z, 'pong-timeout');
  check('A add then FAIL: the first failure is counted once', F.get(Z).attempts === 1 && F.get(Z).cause === 'pong-timeout');
  F.add(Z);
  check('A FAIL then add: add() preserves the mark (row 1)', F.get(Z).attempts === 1 && F.get(Z).cause === 'pong-timeout');
  F.mark(Z, { cause: 'send-failed' });
  check('A mark() is the FAIL input: latest cause, schedule advanced', F.get(Z).cause === 'send-failed' && F.get(Z).attempts === 2);
  F.clear();

  // policy marks and POLICY-FULL (M_policy 2)
  const P = mk();
  P.mark(10n, { kind: 'policy', cause: 'identity', at: 5 });
  check('A policy mark: never eligible; at kept', P.eligible(10n) === false && P.get(10n).at === 5 && P.stats().policy === 1);
  P.fail(10n, 'loss'); check('A FAIL on a policy mark changes nothing', P.get(10n).kind === 'policy' && P.get(10n).attempts === 0);
  P.mark(11n, { kind: 'policy' });
  check('A POLICY-FULL at M_policy: an unmarked identity is ineligible', P.policyFull() && P.eligible(99n) === false && P.stats().ineligibleUnmarked >= 1);
  P.mark(12n, { kind: 'policy' });
  check('A POLICY-FULL: a new policy mark is refused and counted', !P.has(12n) && P.stats().policyRefused === 1);
  P.delete(10n);
  check('A a deleted policy mark frees the set; unmarked eligible again', !P.policyFull() && P.eligible(99n) === true);
  P.clear();

  // MARKS-FULL (M_marks 4, M_hyst 1): eviction of exhausted-and-waiting only; hysteresis
  const Mf = mk();
  for (const id of [20n, 21n, 22n]) Mf.fail(id);
  check('A below the bound: unmarked eligible', Mf.eligible(99n) === true && !Mf.marksFull());
  Mf.fail(23n);
  check('A at the bound: MARKS-FULL latched; unmarked ineligible', Mf.marksFull() && Mf.eligible(99n) === false);
  const r = Mf.fail(24n);
  check('A case 38: new loss mark with nothing exhausted to evict is NOT written and refused', r === null && !Mf.has(24n) && Mf.stats().refusedFull === 1);
  // exhaust 20n (A_max 3) so it is exhausted-and-waiting
  Mf.fail(20n); Mf.fail(20n);
  check('A 20n exhausted and waiting', Mf.get(20n).attempts === 3 && Mf.get(20n).token === 0);
  const r2 = Mf.fail(24n);
  check('A case 38: a new loss mark evicts the oldest exhausted-and-waiting mark only', r2 !== null && Mf.has(24n) && !Mf.has(20n) && Mf.stats().evicted === 1 && Mf.size === 4);
  check('A a policy mark is never evicted', (() => {
    Mf.mark(21n, { kind: 'policy' });            // 21n leaves the loss count (loss 3 of M_marks 4)
    Mf.fail(26n);                                 // loss back at the bound (4)
    Mf.fail(22n); Mf.fail(22n); Mf.fail(23n); Mf.fail(23n);   // 22n, 23n exhausted and waiting
    const before = Mf.has(21n);
    const x = Mf.fail(25n);                       // must evict the OLDEST exhausted-and-waiting loss mark (22n), never 21n
    return before && Mf.has(21n) && x !== null && !Mf.has(22n) && Mf.has(23n) && Mf.get(21n).kind === 'policy';
  })());
  // the evicted identity returns as a stranger
  check('A the evicted identity returns as a stranger (no mark); while MARKS-FULL it is ineligible like any stranger', !Mf.has(20n) && Mf.eligible(20n) === false);
  Mf.delete(24n); Mf.delete(25n);
  check('A hysteresis: size 2 < M_marks − M_hyst (3) lifts MARKS-FULL', !Mf.marksFull() && Mf.eligible(20n) === true);
  Mf.clear();
}

// ── Part B: the kernel ────────────────────────────────────────────────
async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}

(async () => {
  const net = new SimNetwork(); const domain = new AxonaDomain();
  {
    const a = await makePeer(net, domain, 1, 1, {});
    let now = 1000;
    a.node._deadPeers = new DeadPeers({ B: 100, factor: 2, A_max: 2, R_refill: 1000, now: () => now });
    const b = await makePeer(net, domain, 2, 2, {});
    const Z = fromHex(H(0x33));                              // an identity with no node: every open to it fails
    // B1: eligibility filter, not membership
    a.node._deadPeers.fail(Z, 'pong-timeout');
    check('B1 marked, not due: not an eligible candidate', a.peer._isEligibleCandidate(Z) === false);
    now = 1100;
    check('B1 marked, due: eligible candidate (membership alone would say no)', a.peer._isEligibleCandidate(Z) === true && a.node._deadPeers.has(Z));
    // B2: _considerCandidate refuses an ineligible identity before any probe
    now = 1000; a.node._deadPeers.fail(Z, 'x');              // attempts 2 = A_max → exhausted, due at now+1000
    const opens = []; const origOpen = a.transport.openConnection.bind(a.transport);
    a.transport.openConnection = async (id) => { opens.push(id); return origOpen(id); };
    await a.peer._considerCandidate(Z, 'test');
    check('B2 ineligible: no open attempted, counted', opens.length === 0 && a.peer._dialIneligible === 1);
    now = 2000; // refill due
    await a.peer._considerCandidate(Z, 'test');
    check('B2 eligible: the attempt issues (open fails: no such node) and CONSUME advanced the window', opens.length === 1 && a.node._deadPeers.get(Z)?.dueAt === 3000 && a.node._deadPeers.stats().consumed === 1, JSON.stringify(a.node._deadPeers.get(Z)));
    await a.peer._considerCandidate(Z, 'test');
    check('B2 same window: refused again', opens.length === 1 && a.peer._dialIneligible === 2);
    // case 45, an ACTUAL refused reservation: at the probe bound _considerCandidate
    // returns before issue; the mark is eligible but nothing is consumed.
    now = 3000;                                            // window due again
    const savedProbes = a.peer._verifyProbes; a.peer._verifyProbes = 8;   // MAX_VERIFY_PROBES
    const mBefore = { ...a.node._deadPeers.get(Z) }; const consumedBefore = a.node._deadPeers.stats().consumed;
    await a.peer._considerCandidate(Z, 'test');
    const mAfter = a.node._deadPeers.get(Z);
    // The predicate's lazy REFILL may set the token during the eligibility
    // test; that is evaluation, not issue. What a refused reservation must
    // leave alone is the WINDOW (dueAt) and the consumed count.
    check('B2 case 45: reservation refused at the probe bound → no open, no CONSUME, dueAt unchanged, still eligible', opens.length === 1 && a.node._deadPeers.stats().consumed === consumedBefore && mAfter.dueAt === mBefore.dueAt && a.peer._isEligibleCandidate(Z) === true, JSON.stringify({ before: mBefore, after: mAfter }));
    a.peer._verifyProbes = savedProbes;
    a.transport.openConnection = origOpen;
    // B3: a bind on our channel deletes the mark. NOT IMPLEMENTED / NOT
    // ACCEPTED (Aster 4c07cab5): v0.7 case 44 has identify consult ELIGIBLE
    // before a NEW pending record is created; the web transport binds before
    // the kernel can answer, so an inbound bind of a marked identity is
    // accepted and deletes the mark. This check records the behaviour as it
    // is, outbound-only evidence stands separately above.
    a.node._deadPeers.fail(b.big, 'pong-timeout');            // b marked, not due
    await b.transport.openConnection(a.hex); await wait(15);
    check('B3 (recorded, not accepted) inbound bind of a marked identity deletes the mark and admits', !a.node._deadPeers.has(b.big) && a.node.synaptome.has(b.big));
    await a.peer.stop().catch(() => {}); await a.transport.stop().catch(() => {});
    await b.peer.stop().catch(() => {}); await b.transport.stop().catch(() => {});
  }

  // ── Part C: row 13 ────────────────────────────────────────────────
  {
    class FakeDC { constructor() { this.readyState = 'connecting'; } send() {} close() { this.readyState = 'closed'; } open() { this.readyState = 'open'; this.onopen?.(); } }
    class FakePC {
      constructor() { this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; }
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
    const mesh = new MeshManager({ sendSignal: () => {}, log: () => {} });
    const fails = [];
    mesh.onNegotiationFailed((id, r) => fails.push([id, r]));
    const t = new WebRTCTransport({ mesh, log: () => {} });
    await t.start();
    const tfails = [];
    t.onNegotiationFailed((id, r) => tfails.push([id, r]));
    const HEX = 'ab'.repeat(33);
    // C1: a relay-style dial by hex nodeId never opens; negotiation-timeout fires with the identity
    await mesh._initiateTo(HEX); await tick();
    mesh._retire(HEX, 'negotiation-timeout');
    check('C1 mesh fires onNegotiationFailed for a never-opened channel', fails.length === 1 && fails[0][0] === HEX && fails[0][1] === 'negotiation-timeout');
    check('C1 transport resolves the hex meshId to the identity', tfails.length === 1 && tfails[0][0] === fromHex(HEX) && tfails[0][1] === 'negotiation-timeout');
    // C2: a bridge handle names no one
    await mesh._initiateTo('c7'); await tick();
    mesh._retire('c7', 'pc-closed');
    check('C2 bridge handle: mesh fires, transport names nothing', fails.length === 2 && tfails.length === 1);
    // C3: retry and dispose do not fire; an OPENED channel fires onPeerLost not this
    await mesh._initiateTo(HEX); await tick();
    mesh._retire(HEX, 'retry', { keepDeadline: true, notifyLost: false });
    check('C3 retry does not fire', fails.length === 2);
    await mesh._initiateTo(HEX); await tick();
    mesh._peers.get(HEX).dc.open();
    const died = []; t.onPeerDied((id, r) => died.push([id, r]));
    t.bindPeer(fromHex(HEX), HEX);
    mesh._retire(HEX, 'pong-timeout');
    check('C3 an opened channel\'s death is onPeerDied, not onNegotiationFailed', fails.length === 2 && died.length === 1);
    await mesh._initiateTo(HEX); await tick();
    mesh.dispose();
    check('C3 dispose does not fire', fails.length === 2);
    await t.stop().catch(() => {});

    // C4: THE INSTALLED CALLBACK (Aster 1816f5e6 R10/13-B). A started AxonaPeer
    // on a real sim transport WRAPPED to expose onNegotiationFailed (captured)
    // and a controllable isConnected; AxonaPeer.start subscribes exactly as
    // in production, and the failure is delivered through that subscription.
    // A no-op return at the start of the real callback fails these checks.
    {
      const net2 = new SimNetwork(); const dom2 = new AxonaDomain();
      const id2 = await createNodeIdentity({ lat: 3, lng: 3 });
      const sim = simTransport({ network: net2, identity: id2, heartbeatMs: 0 });
      await sim.start(id2.id);
      let captured = null; let connectedOverride = null;
      const wrapped = new Proxy(sim, {
        get(target, prop, recv) {
          if (prop === 'onNegotiationFailed') return (h) => { captured = h; return () => { captured = null; }; };
          if (prop === 'isConnected') return (id) => (connectedOverride == null ? target.isConnected(id) : connectedOverride);
          const v = Reflect.get(target, prop, recv);
          return typeof v === 'function' ? v.bind(target) : v;
        },
      });
      const node2 = new NeuronNode({ id: fromHex(id2.id), lat: 3, lng: 3 }); node2.transport = wrapped;
      const marks = new DeadPeers({ B: 100, A_max: 2, R_refill: 1000 }); node2._deadPeers = marks;
      const peer2 = new AxonaPeer({ domain: dom2, node: node2, nodeIdentity: id2, transport: wrapped });
      const logs = []; const origLog2 = peer2._emitLog.bind(peer2); peer2._emitLog = (l, m, c) => { logs.push([m, c]); return origLog2(l, m, c); };
      await peer2.start();
      check('C4 start subscribed the real onNegotiationFailed callback', typeof captured === 'function' && typeof peer2._onNegotiationFailedUnsub === 'function');
      const ID = fromHex(HEX);
      connectedOverride = true; captured(ID, 'negotiation-timeout');
      check('C4 case 37 (installed callback): beside a LIVE channel → no mark, beside-live logged', !marks.has(ID) && logs.some(([m]) => m === 'negotiation-failed-beside-live'));
      connectedOverride = false; captured(ID, 'negotiation-timeout');
      check('C4 case 35 (installed callback): no open channel → loss mark with the reason, attempts 1', marks.get(ID)?.kind === 'loss' && marks.get(ID)?.cause === 'negotiation-timeout' && marks.get(ID)?.attempts === 1 && logs.some(([m]) => m === 'negotiation-failed-marked'));
      captured(ID, 'pc-closed');
      check('C4 one physical failure → exactly one FAIL increment per delivery', marks.get(ID)?.attempts === 2);
      await peer2.stop();
      check('C4 stop() released the subscription (idempotent cleanup)', captured === null && peer2._onNegotiationFailedUnsub === null);
      await sim.stop().catch(() => {});
    }
    // C5: the lifecycle at the transport and composite layers (R10/13-A)
    {
      const mesh2 = new MeshManager({ sendSignal: () => {}, log: () => {} });
      const t2 = new WebRTCTransport({ mesh: mesh2, log: () => {} });
      const got = []; t2.onNegotiationFailed((id, r) => got.push([String(id), r]));
      await t2.start();
      check('C5 one listener after start', mesh2._negotiationFailedListeners.size === 1);
      await t2.stop();
      check('C5 stop detaches the mesh listener', mesh2._negotiationFailedListeners.size === 0 && t2._unsubNegotiationFailed === null);
      await mesh2._initiateTo('cc'.repeat(33)); await tick(); mesh2._retire('cc'.repeat(33), 'negotiation-timeout');
      check('C5 a failure while stopped reaches no handler', got.length === 0);
      await t2.start();
      check('C5 restart: exactly one listener, not two', mesh2._negotiationFailedListeners.size === 1);
      await mesh2._initiateTo('cc'.repeat(33)); await tick(); mesh2._retire('cc'.repeat(33), 'negotiation-timeout');
      check('C5 one failure → exactly one handler call', got.length === 1);
      await t2.stop(); mesh2.dispose();
      // composite: a late-added sub-transport's registration is removed by the same unsubscribe
      const { CompositeTransport } = await import('../src/transport/web/composite.js');
      const comp = new CompositeTransport({ localNodeId: 1n, log: () => {} });
      const calls = [];
      const un = comp.onNegotiationFailed((id, r) => calls.push(r));
      const meshL = new MeshManager({ sendSignal: () => {}, log: () => {} });
      const late = new WebRTCTransport({ mesh: meshL, log: () => {} });
      await late.start();
      comp.addSubtransport(late);
      check('C5 composite: late sub registered the handler', (late._negotiationFailedHandlers ?? []).length === 1);
      un(); un();
      check('C5 composite: the same unsubscribe removed the late registration; a second call is a no-op', (late._negotiationFailedHandlers ?? []).length === 0 && (comp._negotiationFailedHandlers ?? []).length === 0);
      await late.stop(); meshL.dispose();
    }
    // C6: the real AxonaPeer.start wiring exists (static; labelled as such)
    const { readFileSync } = await import('node:fs');
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    check('C6 (static) AxonaPeer.start subscribes transport.onNegotiationFailed and checks isConnected before marking', /transport\.onNegotiationFailed\(\(peerBig, reason(, inc)?\)/.test(src) && /negotiation-failed-beside-live/.test(src) && /negotiation-failed-marked/.test(src));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
