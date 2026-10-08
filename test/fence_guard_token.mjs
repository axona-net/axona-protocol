// =====================================================================
// fence_guard_token.mjs — Hold-and-Fill v0.15 (axona-docs e4809d2), repair
// row 8: the relay fallback inside the guard. Case 16: `begin(id, k)` once;
// `end(id, k, ·)` exactly once at BIND, CANCEL or DEADLINE; the duplicate
// row never counts.
//
// Part A — the guard alone: begin returns a token; end acts once and only
//   for the live token; end without begin is ignored; sweep ends an attempt
//   in flight past the bound as a failure.
// Part B — the REAL AxonaPeer, started on a sim transport wrapped to make
//   the stranger path reachable (openConnection false for the stranger,
//   connectViaRelay recorded and controllable, isConnected controllable,
//   onPeerBound and onNegotiationFailed captured so the INSTALLED handlers
//   are invoked), driving _considerCandidate and _selfIntegrate:
//   B1 ISSUED: open false → relay issued → the token is HELD (in flight,
//      allow false, attempts 0): before this row it ended before the dial.
//   B2 BIND: the installed peer-bound handler ends it; entry cleared.
//   B3 DEADLINE: the installed negotiation-failed handler, no live channel,
//      ends it as a failure once; a second delivery counts nothing.
//   B4 DUPLICATE: negotiation failed beside a LIVE channel ends as a bind;
//      no failure counted, no mark (row 13's rule stands).
//   B5 CANCEL: relay dial not issued → ended as a failure at once.
//   B6 SWEEP: a held token past inflightMaxMs is ended by the next dial
//      site's sweep.
//   B7 _selfIntegrate holds and releases the token the same way.
//   B8 a bound-only open that succeeds ends the token as a bind.
//   B9 without a guard, _considerCandidate still reaches connectViaRelay
//      (no behaviour change where nothing is armed).
//   G  static: no `end(peerId, opened)` in a `finally` in _considerCandidate.
//
// With the immediate `end` restored before the fallback, B1 fails (the token
// is not held) and B3's first delivery is ignored. With the peer-bound end
// removed, B2 fails.
//
// Run: node test/fence_guard_token.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { DeadPeers }                from '../src/dht/DeadPeers.js';
import { AttemptGuard }             from '../src/dht/attemptGuard.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16) : x));

(async () => {
  console.log('fence_guard_token: row 8 — one token per attempt, ended once at bind, cancel or deadline');

  // ── Part A: the guard alone ────────────────────────────────────────
  console.log('\n  A. the guard');
  {
    const g = new AttemptGuard({ maxAttempts: 3, baseMs: 100, inflightMaxMs: 50 });
    const X = 'ab'.repeat(33), Y = 'cd'.repeat(33);
    check('A end without begin is ignored (no attempt counted)', g.end(X, false) === false && g.attemptsOf(X) === 0 && g.ignoredEnds === 1);
    const k1 = g.begin(X, 1000);
    check('A begin returns a token and marks in flight; allow false while in flight', k1 > 0 && g.inflightOf(X) && g.allow(X, 1000) === false);
    check('A a stale token is ignored', g.end(X, false, 1001, k1 + 99) === false && g.inflightOf(X));
    check('A the live token ends once: failure counted, backoff set', g.end(X, false, 1001, k1) === true && g.attemptsOf(X) === 1 && !g.inflightOf(X) && g.allow(X, 1001) === false && g.allow(X, 1101) === true);
    check('A a second end for the same attempt is ignored', g.end(X, false, 1002) === false && g.attemptsOf(X) === 1);
    const k2 = g.begin(Y, 2000);
    check('A bind clears the entry', g.end(Y, true, 2001, k2) === true && g.attemptsOf(Y) === 0 && !g.inflightOf(Y) && g.allow(Y, 2001) === true);
    const k3 = g.begin(X, 3000);
    check('A sweep before the bound ends nothing', g.sweep(3040) === 0 && g.inflightOf(X));
    check('A sweep past the bound ends the attempt as a failure, once', g.sweep(3060) === 1 && !g.inflightOf(X) && g.attemptsOf(X) === 2 && g.staleEnded === 1 && g.sweep(3061) === 0 && k3 > k1);
    // incarnation correlation (R8-2) and the fresh-record rule (R8-3) at the guard
    const Z = 'ef'.repeat(33);
    const k4 = g.begin(Z, 4000);
    check('A attach records the dial\'s incarnation for the live token only', g.attach(Z, k4, 'inc-a') === true && g.attach(Z, k4 + 1, 'inc-b') === false);
    check('A an end from another incarnation is ignored and counted', g.end(Z, false, 4001, undefined, 'inc-b') === false && g.inflightOf(Z) && (g.staleIncEnds ?? 0) === 1);
    check('A an end with no incarnation (a transport that cannot say) acts by identity', g.end(Z, true, 4002) === true && !g.inflightOf(Z));
    const k5 = g.begin(Z, 5000); g.attach(Z, k5, 'inc-c');
    const fresh = new AttemptGuard({ maxAttempts: 3, baseMs: 100, refillWindowMs: 0 });
    const k6 = fresh.begin(Z, 6000); fresh.end(Z, false, 6001, k6); const k7 = fresh.begin(Z, 6200);
    check('A onFreshRecord while in flight: budget refilled (attempts 0), token kept (still in flight, allow false)', fresh.onFreshRecord(Z, 6300) === true && fresh.inflightOf(Z) && fresh.attemptsOf(Z) === 0 && fresh.allow(Z, 6300) === false && k7 > k6);
    check('A the kept token still ends once', fresh.end(Z, false, 6400, k7) === true && !fresh.inflightOf(Z) && fresh.attemptsOf(Z) === 1);
    check('A onFreshRecord with nothing in flight clears the entry as before', fresh.onFreshRecord(Z, 6500) === true && fresh.attemptsOf(Z) === 0 && fresh.allow(Z, 6500) === true);
    g.end(Z, true, 5001, k5);
  }

  // ── Part B: the real kernel on a wrapped sim transport ──────────────
  console.log('\n  B. the installed handlers');
  const net = new SimNetwork(); const domain = new AxonaDomain();
  async function wrappedPeer(guardOpts) {
    const id = await createNodeIdentity({ lat: 4, lng: 4 });
    const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
    await sim.start(id.id);
    const ctl = { boundCb: null, negCb: null, connected: null, relay: [], relayReturn: true, openOverride: null };
    const wrapped = new Proxy(sim, {
      get(target, prop, recv) {
        if (prop === 'onPeerBound') return (h) => { ctl.boundCb = h; return target.onPeerBound(h); };
        if (prop === 'onNegotiationFailed') return (h) => { ctl.negCb = h; return () => { ctl.negCb = null; }; };
        if (prop === 'isConnected') return (x) => (ctl.connected == null ? target.isConnected(x) : ctl.connected);
        if (prop === 'connectViaRelay') return (hex) => { ctl.relay.push(hex); return ctl.relayReturn; };   // true | false | '<inc>' (row 8: the started negotiation's incarnation)
        if (prop === 'openConnection') return async (x) => (ctl.openOverride != null ? ctl.openOverride(x) : target.openConnection(x));
        const v = Reflect.get(target, prop, recv);
        return typeof v === 'function' ? v.bind(target) : v;
      },
    });
    const node = new NeuronNode({ id: fromHex(id.id), lat: 4, lng: 4 }); node.transport = wrapped;
    node._deadPeers = new DeadPeers({ B: 100, A_max: 4, R_refill: 1000 });
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport: wrapped, ...(guardOpts === null ? {} : { attemptGuard: guardOpts }) });
    await peer.start();
    return { peer, node, id, big: fromHex(id.id), ctl, sim };
  }
  const stranger = (self, seed) => self ^ (1n << BigInt(seed));

  {
    const { peer, node, big, ctl } = await wrappedPeer({ maxAttempts: 4, baseMs: 100, inflightMaxMs: 150 });
    const g = peer._attemptGuard;
    const logs = []; const ol = peer._emitLog.bind(peer); peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
    ctl.openOverride = async () => false;   // every stranger: bound-only open says no binding
    check('B setup: guard installed, handlers captured', g instanceof AttemptGuard && typeof ctl.boundCb === 'function' && typeof ctl.negCb === 'function');

    // B1 issued → held
    const S1 = stranger(big, 210);
    await peer._considerCandidate(S1, 'triadic');
    check('B1 relay dial issued once with the hex', ctl.relay.length === 1 && ctl.relay[0] === toHex(S1), J(ctl.relay));
    check('B1 the token is HELD: in flight, allow false, attempts 0 (not ended before the dial)', g.inflightOf(S1) && g.allow(S1) === false && g.attemptsOf(S1) === 0, `inflight=${g.inflightOf(S1)} attempts=${g.attemptsOf(S1)}`);
    check('B1 a re-nomination while held issues no second dial', (await peer._considerCandidate(S1, 'triadic'), ctl.relay.length === 1));
    check('B1 CONSUME ran at issue (mark window advanced)', node._deadPeers.get(S1) == null || node._deadPeers.get(S1).token === 0);

    // B2 bind ends it
    ctl.boundCb(S1);
    check('B2 BIND via the installed handler ends the token: cleared, allow true', !g.inflightOf(S1) && g.attemptsOf(S1) === 0 && g.allow(S1) === true);
    check('B2 a second bind delivery is ignored', g.end(S1, true) === false);

    // B3 deadline ends it once
    const S2 = stranger(big, 211);
    await peer._considerCandidate(S2, 'triadic');
    check('B3 setup: held', g.inflightOf(S2));
    ctl.connected = false; ctl.negCb(S2, 'negotiation-timeout');
    check('B3 DEADLINE via the installed handler: ended as failure once, backoff armed, mark written', !g.inflightOf(S2) && g.attemptsOf(S2) === 1 && g.allow(S2) === false && node._deadPeers.get(S2)?.kind === 'loss', `attempts=${g.attemptsOf(S2)}`);
    ctl.negCb(S2, 'pc-closed');
    check('B3 a second deadline for the same attempt counts nothing in the guard', g.attemptsOf(S2) === 1 && g.ignoredEnds >= 1);
    ctl.connected = null;

    // B4 duplicate row: beside a live channel
    const S3 = stranger(big, 212);
    await peer._considerCandidate(S3, 'triadic');
    ctl.connected = true; ctl.negCb(S3, 'pc-closed');
    check('B4 negotiation failed BESIDE A LIVE channel: token ends as a bind, no failure, no mark', !g.inflightOf(S3) && g.attemptsOf(S3) === 0 && g.allow(S3) === true && !node._deadPeers.has(S3));
    ctl.connected = null;

    // B5 cancel: relay not issued
    const S4 = stranger(big, 213);
    ctl.relayReturn = false; const before = ctl.relay.length;
    await peer._considerCandidate(S4, 'triadic');
    check('B5 relay dial NOT issued: ended as a failure at once (cancel), backoff armed', ctl.relay.length === before + 1 && !g.inflightOf(S4) && g.attemptsOf(S4) === 1 && g.allow(S4) === false);
    ctl.relayReturn = true;

    // B6 sweep: held past the bound, ended by the next dial site's sweep
    const S5 = stranger(big, 214), S6 = stranger(big, 215);
    await peer._considerCandidate(S5, 'triadic');
    check('B6 setup: held', g.inflightOf(S5));
    await wait(200);
    await peer._considerCandidate(S6, 'triadic');
    check('B6 the sweep at the next dial ended the lost attempt as a failure', !g.inflightOf(S5) && g.attemptsOf(S5) === 1 && g.staleEnded === 1 && g.inflightOf(S6));

    // B8 bound-only open succeeds → ends as bind
    const S7 = stranger(big, 216);
    ctl.openOverride = async (x) => x === S7;
    const relayBefore = ctl.relay.length;
    await peer._considerCandidate(S7, 'triadic');
    check('B8 open succeeded: token ended as a bind, no relay dial', !g.inflightOf(S7) && g.attemptsOf(S7) === 0 && ctl.relay.length === relayBefore);
    ctl.openOverride = async () => false;

    // B10 release: ineligible after the awaited open → token released, nothing counted, nothing issued
    const S9 = stranger(big, 219);
    ctl.openOverride = async (x) => { if (x === S9) { await wait(1); node._deadPeers.fail(S9, 'intervening-loss'); } return false; };
    const rb9 = ctl.relay.length;
    await peer._considerCandidate(S9, 'triadic');
    check('B10 identity marked during the open: no relay dial, token RELEASED (not in flight, attempts 0, allow false only by the mark, not the guard)', ctl.relay.length === rb9 && !g.inflightOf(S9) && g.attemptsOf(S9) === 0 && (g.released ?? 0) >= 1 && (peer._dialIneligibleAfterOpen ?? 0) === 1, `relay=${ctl.relay.length - rb9} inflight=${g.inflightOf(S9)} attempts=${g.attemptsOf(S9)} released=${g.released}`);
    ctl.openOverride = async () => false;

    // B11 (Aster a2c1d79f R8-1): a DUE exhausted mark must spend its window on a dial that goes out.
    // Before: CONSUME ran before the awaited open, moved dueAt, and the post-await recheck refused the
    // dial the mark had just permitted (released, no relay). Now CONSUME runs at the relay issue.
    {
      const S10 = stranger(big, 220);
      const marks = new DeadPeers({ B: 0, A_max: 1, R_refill: 40 }); node._deadPeers = marks;
      marks.fail(S10, 'prior-loss');                    // exhausted at once (A_max 1), refill due in 40 ms
      await wait(60);
      check('B11 setup: due exhausted mark, eligible', marks.eligible(S10) === true && marks.get(S10)?.attempts === 1);
      const rb = ctl.relay.length;
      await peer._considerCandidate(S10, 'triadic');
      check('B11 due exhausted mark: the relay dial GOES OUT and the window is spent by it (token held, consumed)', ctl.relay.length === rb + 1 && g.inflightOf(S10) && marks.get(S10)?.token === 0 && (peer._dialIneligibleAfterOpen ?? 0) === 1, `relay=${ctl.relay.length - rb} inflight=${g.inflightOf(S10)} token=${marks.get(S10)?.token} ineligibleAfter=${peer._dialIneligibleAfterOpen}`);
      ctl.negCb(S10, 'negotiation-timeout', null);     // close it out
      node._deadPeers = new DeadPeers({ B: 100, A_max: 4, R_refill: 1000 });
    }

    // B12 (R8-2): correlation by channel incarnation. An old channel's terminal event after a NEWER dial
    // to the same identity ends nothing and advances nothing.
    {
      const S11 = stranger(big, 221);
      const marks = node._deadPeers;
      const gm = g.inflightMaxMs; g.inflightMaxMs = 1;
      ctl.relayReturn = 'inc-old';
      await peer._considerCandidate(S11, 'triadic');
      check('B12 setup: old dial held with its incarnation', g.inflightOf(S11) && g._state.get(S11.toString(16).padStart(66,'0').slice(2))?.inc === 'inc-old');
      await wait(5); g.sweep();                        // the old dial's fail-safe deadline: attempts 1, backoff 100 ms
      check('B12 sweep ended the old attempt (attempts 1)', !g.inflightOf(S11) && g.attemptsOf(S11) === 1);
      await wait(120);
      ctl.relayReturn = 'inc-new'; g.inflightMaxMs = gm;
      await peer._considerCandidate(S11, 'triadic');
      check('B12 newer dial held with the new incarnation', g.inflightOf(S11) && g.attemptsOf(S11) === 1);
      const markBefore = marks.get(S11)?.attempts ?? 0; const stale0 = g.staleIncEnds ?? 0;
      ctl.connected = false; ctl.negCb(S11, 'negotiation-timeout', 'inc-old');   // the OLD channel's deadline arrives late
      check('B12 old-channel deadline: newer token still held, attempts unchanged, mark unchanged, stale-incarnation logged', g.inflightOf(S11) && g.attemptsOf(S11) === 1 && (marks.get(S11)?.attempts ?? 0) === markBefore && (g.staleIncEnds ?? 0) === stale0 + 1 && logs.some(([m]) => m === 'negotiation-failed-stale-incarnation'), `inflight=${g.inflightOf(S11)} attempts=${g.attemptsOf(S11)} stale=${g.staleIncEnds}`);
      ctl.boundCb(S11, 'm', 'inc-old');                 // an old-channel bind: ignored too
      check('B12 old-channel bind: ignored, newer token still held', g.inflightOf(S11));
      ctl.negCb(S11, 'negotiation-timeout', 'inc-new');
      check('B12 the newer channel\'s own deadline ends it once (attempts 2, mark written)', !g.inflightOf(S11) && g.attemptsOf(S11) === 2 && (marks.get(S11)?.attempts ?? 0) === markBefore + 1);
      ctl.connected = null; ctl.relayReturn = true;
    }

    // B13 (R8-3): freshness refills the budget but does not end a live token.
    {
      const S12 = stranger(big, 222);
      await peer._considerCandidate(S12, 'triadic');
      check('B13 setup: held', g.inflightOf(S12));
      const r1 = g.onFreshRecord(toHex(S12));
      check('B13 onFreshRecord while held: token KEPT (in flight, allow false), refill counted', r1 === true && g.inflightOf(S12) && g.allow(S12) === false, `inflight=${g.inflightOf(S12)} allow=${g.allow(S12)}`);
      ctl.connected = false; ctl.negCb(S12, 'negotiation-timeout', null);
      check('B13 the deadline still ends it exactly once afterwards (attempts 1, backoff)', !g.inflightOf(S12) && g.attemptsOf(S12) === 1 && g.allow(S12) === false);
      ctl.connected = null;
      // interleave: dial → fresh record → bind
      const S13 = stranger(big, 223);
      await peer._considerCandidate(S13, 'triadic');
      g.onFreshRecord(toHex(S13));
      check('B13 fresh record then BIND: bind ends the kept token', g.inflightOf(S13) && (ctl.boundCb(S13, 'm', null), !g.inflightOf(S13) && g.attemptsOf(S13) === 0));
    }

    // B14–B17 (Aster 20904613, R8-2 residuals): a stale channel's BIND must fence its side effects
    // (no mark deletion, no admission) as well as the token, and the REAL composite adapter must carry
    // the incarnation through and not let a rejected stale event swallow the current channel's bind.
    // Admission is observed through a recording stub on _seedSynaptomeWithSponsor: invocation only.
    {
      const marks = node._deadPeers;
      const seedCalls = []; const origSeed = peer._seedSynaptomeWithSponsor;
      peer._seedSynaptomeWithSponsor = (id) => { seedCalls.push(id); };
      const seedsFor = (id) => seedCalls.filter(x => x === id).length;

      // B14 direct installed-handler path
      const S14 = stranger(big, 224);
      ctl.relayReturn = 'inc-new';
      await peer._considerCandidate(S14, 'triadic');
      marks.fail(S14, 'later-loss');                         // a newer mark beside the live attempt
      check('B14 setup: token held on inc-new, mark present', g.inflightOf(S14) && marks.has(S14));
      const stale14 = g.staleIncEnds ?? 0; const logs14 = logs.length;
      const r14 = ctl.boundCb(S14, 'm', 'inc-old');
      check('B14 STALE bind via the installed handler: token held, mark RETAINED, NO seed, staleIncEnds +1, rejected (false), logged',
        r14 === false && g.inflightOf(S14) && marks.has(S14) && seedsFor(S14) === 0 && (g.staleIncEnds ?? 0) === stale14 + 1
          && logs.slice(logs14).some(([m]) => m === 'peer-bound-stale-incarnation'),
        `r=${r14} inflight=${g.inflightOf(S14)} mark=${marks.has(S14)} seeds=${seedsFor(S14)} stale=${g.staleIncEnds}`);
      const r14b = ctl.boundCb(S14, 'm', 'inc-new');
      check('B14 the CURRENT bind afterwards: token ended as bind, mark deleted, seed once, accepted (true)',
        r14b === true && !g.inflightOf(S14) && g.attemptsOf(S14) === 0 && !marks.has(S14) && seedsFor(S14) === 1,
        `r=${r14b} inflight=${g.inflightOf(S14)} mark=${marks.has(S14)} seeds=${seedsFor(S14)}`);

      // B15 the REAL CompositeTransport adapter, fan-out to the installed kernel handler
      const { CompositeTransport } = await import('../src/transport/web/composite.js');
      const comp = new CompositeTransport({ localNodeId: big, log: () => {} });
      const fakeSub = () => { const s = { h: null, onPeerBound(h) { s.h = h; return () => { s.h = null; }; }, onPeerDied() { return () => {}; }, isConnected() { return false; }, async start() {}, async stop() {} }; return s; };
      const subA = fakeSub(); comp.addSubtransport(subA);
      const unsubComp = comp.onPeerBound(ctl.boundCb);
      check('B15 setup: the composite wired the sub (handler installed on it)', typeof subA.h === 'function');
      const S15 = stranger(big, 225);
      await peer._considerCandidate(S15, 'triadic');
      marks.fail(S15, 'later-loss');
      check('B15 setup: held on inc-new, mark present', g.inflightOf(S15) && marks.has(S15));
      const stale15 = g.staleIncEnds ?? 0;
      subA.h(S15, 'm', 'inc-old');
      check('B15 STALE bind through the composite: incarnation delivered (staleIncEnds +1), token held, mark retained, no seed',
        (g.staleIncEnds ?? 0) === stale15 + 1 && g.inflightOf(S15) && marks.has(S15) && seedsFor(S15) === 0,
        `stale=${g.staleIncEnds} inflight=${g.inflightOf(S15)} mark=${marks.has(S15)} seeds=${seedsFor(S15)}`);
      subA.h(S15, 'm', 'inc-new');
      check('B15 then the CURRENT bind through the composite is NOT swallowed by the dedup: token ended, mark deleted, seed once',
        !g.inflightOf(S15) && g.attemptsOf(S15) === 0 && !marks.has(S15) && seedsFor(S15) === 1,
        `inflight=${g.inflightOf(S15)} mark=${marks.has(S15)} seeds=${seedsFor(S15)}`);
      subA.h(S15, 'm', 'inc-new');
      check('B15 a repeated bind of a seen peer is deduplicated (seed count unchanged)', seedsFor(S15) === 1);

      // B16 a LATE-ADDED sub inherits the adapter with the incarnation
      const subB = fakeSub(); comp.addSubtransport(subB);
      check('B16 setup: late sub wired', typeof subB.h === 'function');
      const S16 = stranger(big, 226);
      await peer._considerCandidate(S16, 'triadic');
      subB.h(S16, 'm', 'inc-old');
      check('B16 late sub, STALE bind: token held, no seed', g.inflightOf(S16) && seedsFor(S16) === 0);
      subB.h(S16, 'm', 'inc-new');
      check('B16 late sub, CURRENT bind: token ended, seed once', !g.inflightOf(S16) && g.attemptsOf(S16) === 0 && seedsFor(S16) === 1);

      // B17 LEGACY explicit: a sub that names no incarnation (the bridge) ends by identity; an identity
      // never dialed binds and admits as before; cross-sub dedup still holds.
      const S17 = stranger(big, 227);
      await peer._considerCandidate(S17, 'triadic');
      check('B17 setup: held on inc-new', g.inflightOf(S17));
      subA.h(S17);
      check('B17 no-incarnation bind (legacy sub): ends by identity, seed once', !g.inflightOf(S17) && g.attemptsOf(S17) === 0 && seedsFor(S17) === 1);
      subB.h(S17);
      check('B17 the same peer bound on a second sub fires once (dedup across subs)', seedsFor(S17) === 1);
      const S18 = stranger(big, 228);
      subA.h(S18, 'm', 'inc-x');
      check('B17 an identity the guard never dialed: bind admits (seed once), guard untouched', seedsFor(S18) === 1 && !g.inflightOf(S18) && g.attemptsOf(S18) === 0);

      unsubComp(); ctl.relayReturn = true;
      peer._seedSynaptomeWithSponsor = origSeed;
    }

    // B7 _selfIntegrate: same discipline
    const S8 = stranger(big, 217);
    peer.findKClosest = async () => [S8];
    const rb = ctl.relay.length;
    await peer._selfIntegrate();
    check('B7 _selfIntegrate: relay issued once, token held', ctl.relay.length === rb + 1 && g.inflightOf(S8) && g.attemptsOf(S8) === 0, J(peer._selfIntegrateLast));
    ctl.boundCb(S8);
    check('B7 _selfIntegrate: bind ends it', !g.inflightOf(S8) && g.attemptsOf(S8) === 0);
    await peer.stop().catch(() => {});
  }

  // B9: no guard → unchanged reach of the fallback
  {
    const { peer, big, ctl } = await wrappedPeer(null);
    ctl.openOverride = async () => false;
    check('B9 setup: no guard', peer._attemptGuard == null);
    await peer._considerCandidate(stranger(big, 218), 'triadic');
    check('B9 without a guard _considerCandidate still reaches connectViaRelay (unchanged where nothing is armed)', ctl.relay.length === 1);
    await peer.stop().catch(() => {});
  }

  // G static
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const s = src.indexOf('async _considerCandidate('); const e = src.indexOf('\n  }\n', s);
    const body = src.slice(s, e);
    check('G _considerCandidate has no `end(peerId, opened)` in a finally', !/finally\s*\{[^}]*\.end\(peerId, opened\)/.test(body));
    check('G the issued relay dial attaches the incarnation and returns with the token held', /if \(issued\) \{[\s\S]*?attach\?\.\(peerId, k, inc\);[\s\S]*?return 'held';[^\n]*\n\s*\}/.test(body));
    const gsrc = readFileSync(new URL('../src/dht/attemptGuard.js', import.meta.url), 'utf8');
    check('G guard.end ignores a non-live or stale token', /if \(!s \|\| !s\.inflight \|\| \(k !== undefined && k !== s\.k\)\)/.test(gsrc));
    const csrc = readFileSync(new URL('../src/transport/web/composite.js', import.meta.url), 'utf8');
    // Socket-is-bootstrap v0.5: the per-handler dedup entry is `e`; the
    // pass-through and the un-see are unchanged in substance.
    check('G composite.onPeerBound passes (nodeIdBig, meshId, inc) through and un-sees a rejected event', /e\.handler\(nodeIdBig, meshId, inc\)/.test(csrc) && /if \(r === false\) e\.seen\.delete\(nodeIdBig\)/.test(csrc));
    const bs = src.indexOf('transport.onPeerBound((peerBig, _meshId, inc)'); const be = src.indexOf('\n      });\n', bs);
    const bind = src.slice(bs, be);
    check('G the bind handler returns false on a stale incarnation BEFORE the mark deletion and the seed', bind.indexOf('return false;') > 0 && bind.indexOf('return false;') < bind.indexOf('_deadPeers?.delete(peerBig)') && bind.indexOf('_deadPeers?.delete(peerBig)') < bind.indexOf('_seedSynaptomeWithSponsor(peerBig)'));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
