#!/usr/bin/env node
// fence_mesh_single_pinger — the heartbeat contract (David 2026-10-06):
//
//   ONE pinger per channel. The offerer pings every pingIntervalMs; the other
//   end pongs at once and sends nothing of its own. A side that has received
//   no ping for takeoverMs becomes the pinger (the offerer waits tiebreakMs
//   longer, so a collision converges). A side that RECEIVES a ping stops
//   pinging: when A goes quiet and B takes over, A pongs and does not resume.
//   Liveness counts any receipt: nothing for staleMs → 'stale'; nothing for
//   deadMs → evicted, onPeerLost fires. The ping carries the pinger's last RTT
//   so the ponger learns the latency too.
//
// Two real MeshManagers, each holding one peer state, wired through a pair of
// fake data channels that deliver each other's frames on the next tick. The
// heartbeat is scaled (interval 200 ms, takeover 500, tiebreak 100, stale
// 1000, dead 2000, tick 50, reap 50) so the whole fence runs in seconds.
//
//   A. steady state: only the offerer pings; the responder pongs and never
//      pings; both learn the RTT; two frames per interval on the channel.
//   B. takeover: the offerer goes silent (its loop stopped, its channel still
//      answering) → the responder takes the role after takeoverMs and the
//      offerer, receiving pings, pongs and stays the ponger even once its own
//      loop runs again.
//   C. collision: both start as pingers → within one cycle exactly one pings.
//   D. death: a channel that delivers nothing → both sides go stale at
//      staleMs and are evicted at deadMs with reason 'pong-timeout'.
//   E. recovery: a stale channel that hears again goes back to 'open'.
//   F. static: the defaults are 2000 / 5000 / 1000 / 10000 / 20000 ms; onopen
//      hands to _openHeartbeat; the reaper reads the last receipt; the ping
//      handler yields.
import { readFileSync } from 'node:fs';
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const J = (v) => JSON.stringify(v);
const HB = { pingIntervalMs: 200, takeoverMs: 500, tiebreakMs: 100, staleMs: 1000, deadMs: 2000, tickMs: 50, reapMs: 50 };

/** A pair of fake data channels: what one sends, the other receives next tick. */
function channelPair({ drop = false } = {}) {
  const mk = () => ({ readyState: 'open', sent: [], onmessage: null, onopen: null, onclose: null, onerror: null, peer: null,
    send(data) { this.sent.push(JSON.parse(data)); if (drop) return; const to = this.peer; setTimeout(() => { if (to.readyState === 'open' && to.onmessage) to.onmessage({ data }); }, 1); },
    close() { this.readyState = 'closed'; } });
  const a = mk(), b = mk(); a.peer = b; b.peer = a; return [a, b];
}
/** One mesh holding one open channel to `peerId` in `role`, heartbeat started as onopen would. */
function side(role, peerId, dc, logs) {
  const mesh = new MeshManager({ sendSignal() {}, log: (ev, d) => logs.push([ev, d]), heartbeat: HB });
  const st = mesh._newPeerState(peerId, role);
  mesh._peers.set(peerId, st);
  mesh._wireDataChannel(st, dc);          // installs onmessage (ping/pong handling) and the rest
  st.state = 'open'; st.openedAt = Date.now(); st.dc = dc;
  mesh._openHeartbeat(st);                // what dc.onopen does for the heartbeat
  mesh._armReaper(st);
  return { mesh, st };
}
const pingsSent = (dc) => dc.sent.filter(m => m.type === 'ping').length;
const pongsSent = (dc) => dc.sent.filter(m => m.type === 'pong').length;
const stop = (...sides) => { for (const s of sides) { if (s.st.pingTimer) clearInterval(s.st.pingTimer); if (s.st.reaperTimer) clearInterval(s.st.reaperTimer); } };

(async () => {
  console.log('fence_mesh_single_pinger: one pinger per channel; takeover on silence; stale 10 s, evict 20 s (scaled)');

  // ── A. steady state ─────────────────────────────────────────────────
  {
    const logs = []; const [da, db] = channelPair();
    const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
    await wait(1100);
    const pa = pingsSent(da), pb = pingsSent(db), qa = pongsSent(da), qb = pongsSent(db);
    check('A1 only the OFFERER pings (≈5 in 1.1 s at 200 ms); the responder sends no ping', pa >= 4 && pa <= 7 && pb === 0, J({ pa, pb }));
    check('A1 the responder pongs every ping; the offerer sends no pong', qb === pa && qa === 0, J({ qa, qb }));
    check('A1 roles: offerer pinger, responder ponger; no takeover, no yield', A.st.pinger === true && B.st.pinger === false && A.st.takeovers === 0 && B.st.takeovers === 0 && !logs.some(([e]) => e === 'ping-yield' || e === 'ping-takeover'));
    check('A1 both ends know the RTT (the ponger from the ping\'s rtt field)', A.mesh.getLatency('B') >= 0 && B.mesh.getLatency('A') >= 0, J({ a: A.mesh.getLatency('B'), b: B.mesh.getLatency('A') }));
    check('A1 two frames per interval on the channel (one ping + one pong), not four', (da.sent.length + db.sent.length) <= 2 * pa + 1 && B.st.pongs === 0 && A.st.pongs === pa, J({ total: da.sent.length + db.sent.length, pa }));
    check('A1 both live (not stale), liveness clock advancing on both', A.st.state === 'open' && B.st.state === 'open' && A.st.lastRxAt > A.st.openedAt && B.st.lastRxAt > B.st.openedAt);
    stop(A, B);
  }

  // ── B. takeover when the pinger goes quiet ──────────────────────────
  {
    const logs = []; const [da, db] = channelPair();
    const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
    await wait(450);
    clearInterval(A.st.pingTimer); A.st.pingTimer = null;   // A's loop stops; A's channel still answers
    const pa0 = pingsSent(da);
    await wait(HB.takeoverMs + 300);
    check('B1 after takeoverMs of silence the RESPONDER takes the role and pings', B.st.pinger === true && B.st.takeovers === 1 && pingsSent(db) >= 1 && logs.some(([e, d]) => e === 'ping-takeover' && d.role === 'responder'), J({ pinger: B.st.pinger, takeovers: B.st.takeovers, pb: pingsSent(db) }));
    check('B1 the offerer, receiving pings, PONGS and is the ponger (pinger false); it sent no further ping', A.st.pinger === false && pongsSent(da) >= 1 && pingsSent(da) === pa0, J({ aPinger: A.st.pinger, aPongs: pongsSent(da), pa: pingsSent(da), pa0 }));
    A.mesh._startPingLoop(A.st);                             // A's loop runs again…
    const pb1 = pingsSent(db); const pa1 = pingsSent(da);
    await wait(700);
    check('B1 …and A STAYS the ponger while B continues to ping every interval', A.st.pinger === false && pingsSent(da) === pa1 && pingsSent(db) >= pb1 + 2 && B.st.pinger === true, J({ aPinger: A.st.pinger, pa: pingsSent(da), pb: pingsSent(db) }));
    check('B1 neither side went stale across the handover', A.st.state === 'open' && B.st.state === 'open' && !logs.some(([e]) => e === 'pong-timeout'));
    stop(A, B);
  }

  // ── C. collision: both start as pingers ──────────────────────────────
  {
    const logs = []; const [da, db] = channelPair();
    const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
    B.st.pinger = true;                                        // both pinging
    await wait(HB.takeoverMs + HB.tiebreakMs + 400);
    const one = (A.st.pinger ? 1 : 0) + (B.st.pinger ? 1 : 0);
    check('C1 exactly ONE pinger within one takeover cycle; at least one yield logged', one === 1 && logs.some(([e]) => e === 'ping-yield'), J({ a: A.st.pinger, b: B.st.pinger, yields: logs.filter(([e]) => e === 'ping-yield').length, takeovers: [A.st.takeovers, B.st.takeovers] }));
    const pa = pingsSent(da), pb = pingsSent(db);
    await wait(500);
    const grew = (A.st.pinger && pingsSent(da) >= pa + 2) || (B.st.pinger && pingsSent(db) >= pb + 2);
    check('C1 the surviving pinger keeps pinging and the other stays quiet', grew && (A.st.pinger ? pingsSent(db) === pb : pingsSent(da) === pa), J({ pa: [pa, pingsSent(da)], pb: [pb, pingsSent(db)] }));
    stop(A, B);
  }

  // ── D. death: a channel that delivers nothing ────────────────────────
  {
    const logs = []; const [da, db] = channelPair({ drop: true });
    const lostA = [], lostB = [];
    const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
    A.mesh.onPeerLost((id, reason) => lostA.push({ id, reason })); B.mesh.onPeerLost((id, reason) => lostB.push({ id, reason }));
    await wait(HB.staleMs + 300);
    check('D1 nothing received for staleMs: both sides STALE, neither evicted yet', A.st.state === 'stale' && B.st.state === 'stale' && lostA.length === 0 && lostB.length === 0, J({ a: A.st.state, b: B.st.state }));
    check('D1 the responder took over (silence) and is pinging into the hole; the offerer kept pinging', B.st.pinger === true && pingsSent(db) >= 1 && pingsSent(da) >= 4, J({ pb: pingsSent(db), pa: pingsSent(da) }));
    await wait(HB.deadMs - HB.staleMs + 300);
    check('D1 nothing received for deadMs: both EVICTED with reason pong-timeout, onPeerLost fired once each', lostA.length === 1 && lostA[0].reason === 'pong-timeout' && lostB.length === 1 && lostB[0].reason === 'pong-timeout' && !A.mesh._peers.has('B') && !B.mesh._peers.has('A'), J({ lostA, lostB }));
    stop(A, B);
  }

  // ── E. recovery ──────────────────────────────────────────────────────
  {
    const logs = []; const [da, db] = channelPair();
    const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
    A.st.state = 'stale'; A.st.lastRxAt = Date.now() - HB.staleMs - 100;   // as if silence had passed
    await wait(300);
    check('E1 a stale pinger that hears a pong again is OPEN', A.st.state === 'open' && A.st.lastRxAt > Date.now() - 300);
    stop(A, B);
  }

  // ── G. mixed version (Aster 528c77b6 SP-1): a legacy 1 Hz two-loop peer ─
  // The legacy end is an emulator of 4.104.0's loop: a ping without `hb`
  // every 1 s and an immediate pong to every ping it receives. Aster's
  // pairing with the real main MeshManager is the independent check.
  {
    const legacySide = (dc, pingEveryMs = 1000) => {
      const s = { pingsSent: 0, pongsSent: 0, rtts: [], timer: null };
      dc.onmessage = (ev) => { const m = JSON.parse(ev.data); if (m.type === 'ping') { s.pongsSent++; dc.send(JSON.stringify({ type: 'pong', t: m.t, peerT: Date.now() })); } else if (m.type === 'pong') s.rtts.push(Date.now() - m.t); };
      s.timer = setInterval(() => { if (dc.readyState === 'open') { s.pingsSent++; dc.send(JSON.stringify({ type: 'ping', t: Date.now() })); } }, pingEveryMs);
      return s;
    };
    // G1 new RESPONDER facing a legacy offerer
    {
      const logs = []; const [dl, dn] = channelPair();
      const L = legacySide(dl, 100);                       // scaled: the legacy loop at 100 ms (its 1 s)
      const N = side('responder', 'L', dn, logs);
      await wait(900);
      check('G1 new responder vs legacy pinger: legacy detected, NEVER yields, keeps its own pings (≥3 in 0.9 s at 200 ms) and measures its own RTT', N.st.peerLegacy === true && N.st.pinger === true && pingsSent(dn) >= 3 && N.mesh.getLatency('L') >= 0 && !logs.some(([e]) => e === 'ping-yield') && logs.some(([e]) => e === 'ping-legacy-peer'), J({ legacy: N.st.peerLegacy, pinger: N.st.pinger, pn: pingsSent(dn), lat: N.mesh.getLatency('L') }));
      check('G1 the legacy end is still answered: a pong for every legacy ping (its own liveness holds) and it measures RTT from our pongs', pongsSent(dn) === L.pingsSent && L.rtts.length >= 1, J({ pongs: pongsSent(dn), legacyPings: L.pingsSent, legacyRtts: L.rtts.length }));
      check('G1 liveness on the new end stays live (receipts flowing)', N.st.state === 'open' && N.st.lastRxAt > N.st.openedAt);
      clearInterval(L.timer); stop(N);
    }
    // G1b new OFFERER facing a legacy responder that also pings (the old loop pings from both ends)
    {
      const logs = []; const [dn, dl] = channelPair();
      const N = side('offerer', 'L', dn, logs);
      const L = legacySide(dl, 100);
      await wait(900);
      check('G1b new offerer vs legacy two-loop peer: keeps the role, never yields, legacy detected, RTT known, pongs every legacy ping', N.st.pinger === true && N.st.peerLegacy === true && !logs.some(([e]) => e === 'ping-yield') && N.mesh.getLatency('L') >= 0 && pongsSent(dn) === L.pingsSent, J({ pinger: N.st.pinger, legacy: N.st.peerLegacy, lat: N.mesh.getLatency('L'), pongs: pongsSent(dn), lp: L.pingsSent }));
      clearInterval(L.timer); stop(N);
    }
    // G1c reconnect: a fresh channel to a NEW peer starts clean (no legacy flag carried over)
    {
      const logs = []; const [da, db] = channelPair();
      const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
      await wait(500);
      check('G1c a new channel between two new ends starts without the legacy flag and in the single-pinger steady state', A.st.peerLegacy === false && B.st.peerLegacy === false && A.st.pinger && !B.st.pinger && pingsSent(db) === 0);
      // same-manager retire then reconnect: the legacy flag lives on the retired state and does not carry over
      A.st.peerLegacy = true;
      A.mesh._retire('B', 'test-retire');
      const st2 = A.mesh._newPeerState('B', 'offerer');
      check('G1c same manager, retire then reconnect: the old state is gone and the fresh state carries no legacy flag', !A.mesh._peers.has('B') && st2.peerLegacy === false && st2.pinger === false && st2.lastRxAt === 0);
      stop(A, B);
    }
  }

  // ── H. convergence under crossing delivery, equal tick phase, suspend/resume ─
  {
    // H1 both start as pingers, delivery 150 ms (> tick 50, > tiebreak 100), ticks started in the same macrotask
    {
      const logs = []; const [da, db] = channelPair();
      const slow = (dc) => { const orig = dc.send.bind(dc); dc.send = (data) => { dc.sent.push(JSON.parse(data)); const to = dc.peer; setTimeout(() => { if (to.readyState === 'open' && to.onmessage) to.onmessage({ data }); }, 150); }; return orig; };
      slow(da); slow(db);
      const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
      B.st.pinger = true;                                      // collision
      await wait(700);
      check('H1 crossing pings with slow delivery: the RESPONDER yields on the crossing, the OFFERER keeps; one pinger, zero takeovers, no double-yield', A.st.pinger === true && B.st.pinger === false && A.st.takeovers === 0 && B.st.takeovers === 0 && logs.some(([e, d]) => e === 'ping-yield' && d.cause === 'crossing' && d.role === 'responder') && !logs.some(([e, d]) => e === 'ping-yield' && d.role === 'offerer'), J({ a: A.st.pinger, b: B.st.pinger, logs: logs.filter(([e]) => e.startsWith('ping-')).map(([e, d]) => `${e}:${d.role}:${d.cause ?? ''}`) }));
      const pa = pingsSent(da); await wait(450);
      check('H1 steady afterwards: the offerer pings, the responder is quiet', pingsSent(da) >= pa + 2 && pingsSent(db) <= 2, J({ pa: [pa, pingsSent(da)], pb: pingsSent(db) }));
      stop(A, B);
    }
    // H2 the legitimate takeover carries since ≥ takeoverMs and the quiet side yields even though it is the offerer
    {
      const logs = []; const [da, db] = channelPair();
      const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
      await wait(300);
      clearInterval(A.st.pingTimer); A.st.pingTimer = null;   // A's loop stops (channel still answers)
      await wait(HB.takeoverMs + 300);
      A.mesh._startPingLoop(A.st);                             // A's loop resumes
      await wait(400);
      const y = logs.find(([e, d]) => e === 'ping-yield' && d.role === 'offerer');
      check('H2 after B\'s legitimate takeover, A (its loop stalled) yielded with cause stalled, the far end\'s since ≥ takeoverMs logged, and stays the ponger after its loop resumes', A.st.pinger === false && B.st.pinger === true && !!y && y[1].cause === 'stalled' && y[1].since >= HB.takeoverMs, J({ a: A.st.pinger, b: B.st.pinger, y: y && y[1] }));
      stop(A, B);
    }
    // H4 (Aster 2e70fefc): SHARED suspension — both ends asleep past takeoverMs, both wake in the same
    // tick, both pings carry since ≥ takeoverMs and cross. Both are actively sending, so the role rule
    // decides: the responder yields, the offerer keeps; no zero-pinger window, no repeated takeovers.
    {
      const logs = []; const [da, db] = channelPair();
      const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
      await wait(300);
      clearInterval(A.st.pingTimer); clearInterval(B.st.pingTimer); A.st.pingTimer = B.st.pingTimer = null;
      da.readyState = db.readyState = 'suspended';            // both asleep: nothing sent or received
      await wait(HB.takeoverMs + 300);
      da.readyState = db.readyState = 'open';
      B.st.pinger = true; A.st.pinger = true;                 // both woke as pingers (B's takeover threshold is met; A kept its role)
      A.mesh._startPingLoop(A.st); B.mesh._startPingLoop(B.st);   // same macrotask: the first pings cross
      await wait(500);
      const sinceSeen = logs.filter(([e, d]) => (e === 'ping-yield' || e === 'ping-crossing-kept') && typeof d.since === 'number' && d.since >= HB.takeoverMs).length;
      check('H4 both woke with since ≥ takeoverMs and crossed: the RESPONDER yielded (crossing), the OFFERER kept; exactly one pinger', A.st.pinger === true && B.st.pinger === false && logs.some(([e, d]) => e === 'ping-yield' && d.role === 'responder' && d.cause === 'crossing') && !logs.some(([e, d]) => e === 'ping-yield' && d.role === 'offerer') && sinceSeen >= 1, J({ a: A.st.pinger, b: B.st.pinger, sinceSeen, ping: logs.filter(([e]) => e.startsWith('ping-')).map(([e, d]) => `${e}:${d.role}:${d.cause ?? ''}:${d.since ?? ''}`) }));
      const pa = pingsSent(da); await wait(450);
      check('H4 steady afterwards: the offerer pings, the responder is quiet, no further takeover', pingsSent(da) >= pa + 2 && B.st.pinger === false && B.st.takeovers === 0 && A.st.state !== 'stale' && B.st.state !== 'stale', J({ pa: [pa, pingsSent(da)], bt: B.st.takeovers }));
      stop(A, B);
    }
    // H3 suspend/resume of the WHOLE pinger (no pongs, no handler) then wake: one pinger within an interval, no stale on the waker's side afterwards
    {
      const logs = []; const [da, db] = channelPair();
      const A = side('offerer', 'B', da, logs), B = side('responder', 'A', db, logs);
      await wait(300);
      clearInterval(A.st.pingTimer); A.st.pingTimer = null; da.readyState = 'suspended';   // A asleep: sends and receives nothing
      await wait(HB.takeoverMs + 400);
      check('H3 while A sleeps, B takes over and pings into silence', B.st.pinger === true && B.st.takeovers === 1);
      da.readyState = 'open'; A.mesh._startPingLoop(A.st);     // A wakes
      await wait(600);
      const one = (A.st.pinger ? 1 : 0) + (B.st.pinger ? 1 : 0);
      check('H3 after A wakes: exactly ONE pinger within an interval, both ends live again', one === 1 && A.st.state !== 'stale' && B.st.state !== 'stale' && A.mesh._peers.has('B') && B.mesh._peers.has('A'), J({ a: A.st.pinger, b: B.st.pinger, as: A.st.state, bs: B.st.state }));
      stop(A, B);
    }
  }

  // ── I. frames queued across a pause and delivered on resume (Aster a9150fe4, Vega ff719a58) ─
  // The channel pair can be HELD: frames sent while held are queued in order and delivered on
  // release, before anything new. The transient (zero pingers) and the bound (one pinger within
  // one takeover window = takeoverMs + tiebreakMs + tickMs + delivery) are asserted separately.
  {
    const holdablePair = () => {
      const q = []; let held = false;
      const mk = () => ({ readyState: 'open', sent: [], onmessage: null, peer: null,
        send(data) { this.sent.push(JSON.parse(data)); const to = this.peer; const deliver = () => { if (to.readyState === 'open' && to.onmessage) to.onmessage({ data }); }; if (held) q.push(deliver); else setTimeout(deliver, 1); },
        close() { this.readyState = 'closed'; } });
      const a = mk(), b = mk(); a.peer = b; b.peer = a;
      return { a, b, hold: () => { held = true; }, release: () => { held = false; const items = q.splice(0); items.forEach((d, i) => setTimeout(d, 1 + i)); } };
    };
    const WINDOW = HB.takeoverMs + HB.tiebreakMs + HB.tickMs + 50;
    const pingers = (A, B) => (A.st.pinger ? 1 : 0) + (B.st.pinger ? 1 : 0);
    // I1 SHARED pause: both loops stop (their last frames may still be in flight → queued), both wake,
    //    the queued frames land first.
    {
      const logs = []; const P = holdablePair();
      const A = side('offerer', 'B', P.a, logs), B = side('responder', 'A', P.b, logs);
      await wait(300);
      P.hold();                                                  // delivery pauses: the next frames queue
      await wait(HB.pingIntervalMs + 20);                        // A sends ≥1 ping into the queue; B's pong to an earlier ping may queue too
      clearInterval(A.st.pingTimer); clearInterval(B.st.pingTimer); A.st.pingTimer = B.st.pingTimer = null;
      await wait(HB.takeoverMs + 300);                           // both asleep past the takeover threshold
      const seen = []; const probe = setInterval(() => seen.push(pingers(A, B)), 25);
      A.st.pinger = true; B.st.pinger = true;                    // both wake believing they ping (B's threshold is met)
      A.mesh._startPingLoop(A.st); B.mesh._startPingLoop(B.st);
      P.release();                                               // the pre-pause frames land first, then the wake's pings
      await wait(WINDOW);
      clearInterval(probe);
      check('I1 after a shared pause with frames queued: exactly ONE pinger within one takeover window, no eviction, neither stale', pingers(A, B) === 1 && A.mesh._peers.has('B') && B.mesh._peers.has('A') && A.st.state !== 'stale' && B.st.state !== 'stale', J({ a: A.st.pinger, b: B.st.pinger, seen: seen.join('') }));
      check('I1 the transient is recorded as such: zero or two pingers may appear inside the window, then one holds', seen.length > 0 && seen.slice(-4).every(n => n === 1), J({ seen: seen.join(''), yields: logs.filter(([e]) => e === 'ping-yield').map(([, d]) => `${d.role}:${d.cause}`) }));
      stop(A, B);
    }
    // I2 ASYMMETRIC pause: only A pauses (its loop stops and its channel holds); B keeps pinging after
    //    taking over, its frames queue toward A; on A's resume the queue lands while A's wake ping goes out.
    {
      const logs = []; const P = holdablePair();
      const A = side('offerer', 'B', P.a, logs), B = side('responder', 'A', P.b, logs);
      await wait(300);
      P.hold(); clearInterval(A.st.pingTimer); A.st.pingTimer = null;   // A asleep; everything in flight queues
      await wait(HB.takeoverMs + 400);
      check('I2 while A sleeps (frames queued), B took the role', B.st.pinger === true && B.st.takeovers === 1);
      A.mesh._startPingLoop(A.st); P.release();                  // A wakes with its old role flag; the queue lands
      await wait(WINDOW);
      check('I2 after the asymmetric resume: exactly ONE pinger within one takeover window, no eviction', pingers(A, B) === 1 && A.mesh._peers.has('B') && B.mesh._peers.has('A'), J({ a: A.st.pinger, b: B.st.pinger, yields: logs.filter(([e]) => e === 'ping-yield').map(([, d]) => `${d.role}:${d.cause}`) }));
      stop(A, B);
    }
    // I3 both send before either delivery lands (new crossing, no queue): the role rule alone decides
    {
      const logs = []; const P = holdablePair();
      const A = side('offerer', 'B', P.a, logs), B = side('responder', 'A', P.b, logs);
      P.hold(); B.st.pinger = true;                              // both ping; nothing delivers yet
      await wait(HB.pingIntervalMs + 60);                        // both have sent at least one ping into the queue
      P.release();
      await wait(300);
      check('I3 both sent before either delivery: the RESPONDER yields on the crossing, the OFFERER keeps; one pinger, no takeover', A.st.pinger === true && B.st.pinger === false && logs.some(([e, d]) => e === 'ping-yield' && d.role === 'responder' && d.cause === 'crossing') && A.st.takeovers === 0 && B.st.takeovers === 0, J({ a: A.st.pinger, b: B.st.pinger, yields: logs.filter(([e]) => e === 'ping-yield').map(([, d]) => `${d.role}:${d.cause}`) }));
      stop(A, B);
    }
  }

  // ── F. static ────────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/transport/web/mesh.js', import.meta.url), 'utf8');
    check('F-1 the header states the state machine, the ordered-delivery assumption, EVENTUAL convergence, the bound only under bounded lateness and delivery, the transients, and the eviction exclusion', /THE PER-CHANNEL STATE MACHINE/.test(src) && /ASSUMPTIONS: the data channel is ORDERED/.test(src) && /Eventual convergence:/.test(src) && /takeoverMs > pingIntervalMs \+ tickMs\n\/\/\s+\+ 2D \+ L/.test(src) && /there may be ZERO pingers/.test(src) && /Eviction is excluded only for a pause SHORTER than deadMs/.test(src) && /NOT CLAIMED:/.test(src) && !/within ONE takeover window — takeoverMs/.test(src) && /createDataChannel\(DC_LABEL, \{ ordered: true \}\)/.test(src));
    check('F0 the ping carries hb: 1, since and the last rtt; a ping without hb is a legacy peer (never yield, keep own pings); a stalled pinger yields whatever its role, an active one resolves the crossing by role (responder yields)', /type: 'ping', hb: 1, t: now, since,/.test(src) && /if \(msg\.hb !== 1\) \{[\s\S]*?state\.peerLegacy = true;[\s\S]*?if \(!state\.pinger\) state\.pinger = true;/.test(src) && /const active = state\.lastPingTxAt > 0 && \(now - state\.lastPingTxAt\) <= this\._hb\.pingIntervalMs \+ this\._hb\.tickMs;/.test(src) && /if \(!active\) \{\n\s*state\.pinger = false;[\s\S]*?cause: 'stalled'/.test(src) && /else if \(state\.role === 'responder'\) \{\n\s*state\.pinger = false;[\s\S]*?cause: 'crossing'/.test(src) && !/if \(since >= this\._hb\.takeoverMs\)/.test(src));
    check('F1 defaults: ping 2000, takeover 5000, tiebreak 1000, stale 10000, dead 20000', /pingIntervalMs: 2000,[\s\S]*?takeoverMs:\s+5000,[\s\S]*?tiebreakMs:\s+1000,[\s\S]*?staleMs:\s+10000,[\s\S]*?deadMs:\s+20000,/.test(src));
    check('F1 dc.onopen hands the heartbeat to _openHeartbeat, which gives the OFFERER the role', /this\._openHeartbeat\(state\);\n\s*this\._notify\(\);\n\s*\};/.test(src) && /_openHeartbeat\(state\) \{\n\s*state\.pinger = state\.role === 'offerer';/.test(src));
    check('F1 the reaper\'s liveness clock is the last RECEIPT (ping or pong), falling back to openedAt', /const lastRx = Math\.max\(state\.lastRxAt \?\? 0, state\.lastPongAt \?\? 0, state\.lastPingRxAt \?\? 0, state\.openedAt \?\? 0\);/.test(src));
    check('F1 the ponger takes over after takeoverMs (+ tiebreak for the offerer, defence in depth)', /const wait = hb\.takeoverMs \+ \(state\.role === 'offerer' \? hb\.tiebreakMs : 0\);/.test(src));
    check('F1 no end pings on a fixed 1 Hz loop any more (PING_INTERVAL_MS gone; the scheduler is _heartbeatTick)', !/PING_INTERVAL_MS/.test(src) && /setInterval\(\(\) => this\._heartbeatTick\(state\), this\._hb\.tickMs\)/.test(src));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
