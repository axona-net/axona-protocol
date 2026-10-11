// =====================================================================
// fence_lookahead_round.mjs — R2 of Routed-Walk-Terminals v0.5 (axona-docs 10f43fc)
//
// The two-hop lookahead is a ROUND: it probes only the synapses greedy would
// consider (connected, not dead, not on the walk's path), it locks on the
// FIRST reply whose two-hop node is closer than self, and it ends at
// LOOKAHEAD_MS with null when no such reply came. The free pass over
// incomingSynapses runs first and costs no network.
//
// What it was: a Promise.allSettled over the WHOLE synaptome, unfiltered,
// waiting for the slowest probe. One silent synapse cost 5000 ms, which is
// also the upstream request timeout, so the origin reported `exhausted` at
// hop 0 — 650 of 800 walks in ops/p2p-strand-808f-20261010T1855Z, every one
// of them in 4999–6126 ms.
//
// Checks (each named for the mutant that fails it):
//   1  an UNCONNECTED synapse is never probed                  (filter parity)
//   2  a DEAD synapse is never probed                          (filter parity)
//   3  a synapse ON THE PATH is never probed                   (R3 exclusion)
//   4  the round returns the fast closer reply while a connected peer stays
//      silent, well inside LOOKAHEAD_MS                        (first-closer lock)
//   5  targetsNearerThanSelf is 0 although the unconnected synapse is nearer
//      than self                                               (filter parity)
//   6  a round with only silent peers ends at ~LOOKAHEAD_MS with null and
//      counts roundsTimedOut                                   (round budget)
//   7  an incoming reverse channel closer than self answers with NO probes
//   8  at the destination (distance 0) nothing is probed, null  (2026-09-08 bypass)
//   9  the round clock is bucketed
//  10  a reply arriving after the lock is not counted           (bounded round)
//  11  the result carries `via`, the two-hop node the reply named
//
// Mutants run by hand on 2026-10-11 (sed, then restore):
//   remove the isConnected skip → 1 and 5 fail (and 4, since the unconnected
//   peer's send throws → rejected, but nearer-than-self counts);
//   replace lock-on-first with wait-for-all → 4 and 10 fail (silent peer holds
//   the round to the timeout, result null).
//
// Run: node test/fence_lookahead_round.mjs
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// A peer with only what _findCloserInTwoHops reads: _node, _domain, the census.
function fakePeer({ selfId, synapses, incoming = [], dead = [], connected, replies, lookaheadMs = 120 }) {
  const peer = Object.create(AxonaPeer.prototype);
  const probed = [];
  const transport = {
    isConnected: (id) => connected.has(id),
    send: (id, type, payload) => {
      probed.push(id);
      if (type !== 'lookahead_probe') return Promise.reject(new Error('unexpected ' + type));
      if (!connected.has(id)) return Promise.reject(new Error('no channel'));
      const r = replies.get(id);
      if (!r) return new Promise(() => {});                      // silent for ever
      return new Promise((res) => setTimeout(() => res(r.value), r.afterMs ?? 0));
    },
  };
  const synaptome = new Map(synapses.map((id) => [id, { peerId: id }]));
  const incomingSynapses = new Map(incoming.map((id) => [id, { peerId: id }]));
  peer._node = { id: selfId, synaptome, incomingSynapses, _deadPeers: new Set(dead), transport };
  peer._domain = { LOOKAHEAD_MS: lookaheadMs };
  return { peer, probed, LS: () => peer._lookaheadStats };
}

(async () => {
  console.log('fence_lookahead_round: R2 — the lookahead is a bounded round over greedy-eligible peers');
  // The greedy precondition holds here: every ELIGIBLE probe target is farther
  // from T than self (greedy found nobody closer), the two-hop node a reply
  // names is closer, and the one nearer synapse is the UNCONNECTED one — the
  // entry the old unfiltered fan-out would have probed and counted.
  const T    = 0x100000n;              // target
  const SELF = T ^ 0x0F00n;            // self, distance 0x0F00
  const FAST = T ^ 0xA000n;            // connected, farther; replies with a closer node quickly
  const SLOW = T ^ 0xB000n;            // connected, farther; never replies
  const UNC  = T ^ 0x0010n;            // UNCONNECTED and nearer than self
  const DEAD = T ^ 0xC000n;            // connected, farther, marked dead
  const PATH = T ^ 0xD000n;            // connected, farther, on the walk's path
  const VIA  = T ^ 0x0001n;            // the two-hop node FAST names (closer than self)
  const INC  = T ^ 0x0100n;            // an incoming reverse channel, closer than self

  // ── A: the mixed round ─────────────────────────────────────────────────
  {
    const f = fakePeer({
      selfId: SELF, synapses: [FAST, SLOW, UNC, DEAD, PATH], dead: [DEAD],
      connected: new Set([FAST, SLOW, DEAD, PATH]),
      replies: new Map([[FAST, { afterMs: 5, value: { peerId: VIA, latency: 1, terminal: false } }]]),
    });
    const t0 = Date.now();
    const r = await f.peer._findCloserInTwoHops(T, { exclude: new Set([PATH, SELF]) });
    const dt = Date.now() - t0;
    const LS = f.LS();
    check('1 the unconnected synapse was never probed', !f.probed.includes(UNC), JSON.stringify(f.probed.map(String)));
    check('2 the dead synapse was never probed', !f.probed.includes(DEAD));
    check('3 the on-path synapse was never probed', !f.probed.includes(PATH));
    check('4 the fast closer reply wins while SLOW stays silent, inside LOOKAHEAD_MS', r && r.first === FAST && dt < 100, `first=${r && String(r.first)} dt=${dt}ms`);
    check('5 targetsNearerThanSelf is 0 with a nearer unconnected synapse present', LS.targetsNearerThanSelf === 0, String(LS.targetsNearerThanSelf));
    check('11 the result carries via = the two-hop node the reply named', r && r.via === VIA, String(r && r.via));
    check('9 the round clock was bucketed once', LS.lookaheadRoundMs.reduce((a, b) => a + b, 0) === 1, JSON.stringify(LS.lookaheadRoundMs));
    check('A probesEmitted counts only the eligible two (FAST, SLOW)', LS.probesEmitted === 2 && LS.probesSkippedUnconnected === 1 && LS.probesSkippedDead === 1 && LS.probesSkippedOnPath === 1,
      `emitted=${LS.probesEmitted} unc=${LS.probesSkippedUnconnected} dead=${LS.probesSkippedDead} path=${LS.probesSkippedOnPath}`);
    const fulfilledAtLock = LS.probesFulfilled;
    await wait(150);
    check('10 nothing arriving after the lock is counted (SLOW never replies; counters unchanged)', LS.probesFulfilled === fulfilledAtLock && LS.probesEmitted === 2);
  }

  // ── B: only silent peers → the round budget ────────────────────────────
  {
    const f = fakePeer({ selfId: SELF, synapses: [SLOW], connected: new Set([SLOW]), replies: new Map(), lookaheadMs: 120 });
    const t0 = Date.now();
    const r = await f.peer._findCloserInTwoHops(T, { exclude: new Set([SELF]) });
    const dt = Date.now() - t0;
    const LS = f.LS();
    check('6 a round with only silent peers ends at ~LOOKAHEAD_MS with null', r === null && dt >= 110 && dt < 400, `r=${r} dt=${dt}ms`);
    check('6 roundsTimedOut counted', LS.roundsTimedOut === 1 && LS.answeredNull === 1, `timedOut=${LS.roundsTimedOut} null=${LS.answeredNull}`);
  }

  // ── C: the free pass answers first ──────────────────────────────────────
  {
    const f = fakePeer({ selfId: SELF, synapses: [FAST], incoming: [INC], connected: new Set([FAST, INC]),
      replies: new Map([[FAST, { afterMs: 5, value: { peerId: VIA, terminal: false } }]]) });
    const r = await f.peer._findCloserInTwoHops(T, { exclude: new Set([SELF]) });
    const LS = f.LS();
    check('7 an incoming reverse channel closer than self answers with no probes', r && r.first === INC && r.via === null && f.probed.length === 0 && LS.answeredByIncoming === 1,
      `first=${r && String(r.first)} probed=${f.probed.length}`);
  }

  // ── D: at the destination ───────────────────────────────────────────────
  {
    const f = fakePeer({ selfId: T, synapses: [FAST], connected: new Set([FAST]), replies: new Map([[FAST, { value: { peerId: VIA, terminal: false } }]]) });
    const r = await f.peer._findCloserInTwoHops(T, { exclude: new Set([T]) });
    check('8 at distance 0 nothing is probed and the answer is null', r === null && f.probed.length === 0 && f.LS().bypassedAtDestination === 1);
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
