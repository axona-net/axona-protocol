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

  // ── F. static ────────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/transport/web/mesh.js', import.meta.url), 'utf8');
    check('F1 defaults: ping 2000, takeover 5000, tiebreak 1000, stale 10000, dead 20000', /pingIntervalMs: 2000,[\s\S]*?takeoverMs:\s+5000,[\s\S]*?tiebreakMs:\s+1000,[\s\S]*?staleMs:\s+10000,[\s\S]*?deadMs:\s+20000,/.test(src));
    check('F1 dc.onopen hands the heartbeat to _openHeartbeat, which gives the OFFERER the role', /this\._openHeartbeat\(state\);\n\s*this\._notify\(\);\n\s*\};/.test(src) && /_openHeartbeat\(state\) \{\n\s*state\.pinger = state\.role === 'offerer';/.test(src));
    check('F1 the reaper\'s liveness clock is the last RECEIPT (ping or pong), falling back to openedAt', /const lastRx = Math\.max\(state\.lastRxAt \?\? 0, state\.lastPongAt \?\? 0, state\.lastPingRxAt \?\? 0, state\.openedAt \?\? 0\);/.test(src));
    check('F1 receiving a ping YIELDS the role and pongs; the ponger takes over after takeoverMs (+ tiebreak for the offerer)', /if \(state\.pinger\) \{\n\s*state\.pinger = false;\n\s*this\._log\('ping-yield'/.test(src) && /const wait = hb\.takeoverMs \+ \(state\.role === 'offerer' \? hb\.tiebreakMs : 0\);/.test(src));
    check('F1 no end pings on a fixed 1 Hz loop any more (PING_INTERVAL_MS gone; the scheduler is _heartbeatTick)', !/PING_INTERVAL_MS/.test(src) && /setInterval\(\(\) => this\._heartbeatTick\(state\), this\._hb\.tickMs\)/.test(src));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
