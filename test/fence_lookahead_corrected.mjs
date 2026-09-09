// =====================================================================
// fence_lookahead_corrected.mjs — 4.83.0, extended 4.84.0
//
// FIVE measurements the council falsified, in two rounds. Round one: Aster
// 938e4162, Vega 260f527b, Orion d0c04f27, on council post b4392f8c. Round two,
// against the FIX for round one: Aster fc8146ed, ratified Orion 3d723228.
//
// Every one failed the same way — the counter measured something other than the
// sentence written beside it, and the sentence was what got reported.
//
// 1. answeredByIncoming was only incremented when the probes returned NOTHING:
//
//        const answeredByProbe = bestPeerId !== null;   // BEFORE the incoming pass
//        for (syn of incomingSynapses) if (d < bestDist) bestPeerId = syn.peerId;
//        else if (answeredByProbe) LS.answeredByProbe++;
//
//    So an incoming link that supplied the WINNING next hop still counted to the
//    probes. I reported "answeredByIncoming is 0 everywhere — no cheaper path is
//    being ignored". The zero only ever meant "probes always found something".
//    Now measured independently.
//
// 4. (4.84.0) The fix for (1) mixed UNITS. incomingCouldAnswer incremented once
//    per qualifying SYNAPSE while incomingWonFinal counted CALLS, so the first
//    could exceed `calls` and dividing it by them was dimensionally invalid.
//    Split by name: incomingCandidateLinks (links, summed over calls),
//    incomingCouldAnswerCalls and incomingWonFinalCalls (calls), and
//    incomingCouldAnswerRate = calls/calls.
//
// 5. (4.84.0) rateOfAnswerable conditioned TERMINAL replies out of its
//    denominator. A terminal reply is a live peer answering "I have nothing
//    closer" — evidence about that target, not a missing measurement — so
//    removing it inflated the hit rate. Replaced by rateOfReplies, which
//    excludes only the unreachable. The inflating field is deleted rather than
//    kept alongside: a more-flattering ratio next to honest ones gets quoted.
//
// 2. Rank 0 returned no closer replies on every node, and I read that as
//    structure. Greedy filters CONNECTED/dead/bridge peers; probeTargets is the
//    RAW synaptome and filters none of them. A rank whose probes were all
//    REJECTED is indistinguishable, under closer/sent, from one whose replies
//    were all non-closer — and those mean opposite things. Now partitioned:
//    rejected / terminal / nonCloser / closer per rank.
//
// 3. "K=8 would discard 44-100% of informative replies" counted lost REPLIES.
//    Routing needs ONE closer reply per call and a call may receive several, so
//    discarding surplus costs nothing. topK[] now reports, per call, whether the
//    NEAREST closer reply falls inside K.
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

function mkNode(selfId, peerIds, { reply = null, incoming = [] } = {}) {
  const sent = [];
  return {
    sent,
    node: {
      id: selfId,
      synaptome: new Map(peerIds.map((p, i) => [String(i), { peerId: p }])),
      incomingSynapses: new Map(incoming.map((p, i) => [String(i), { peerId: p }])),
      transport: {
        send: async (peerId, type, payload) => {
          sent.push({ peerId, type, payload });
          const r = typeof reply === 'function' ? reply(peerId) : reply;
          if (r === 'REJECT') throw new Error('probe rejected');
          return r;
        },
      },
    },
  };
}
const mkSelf = (stub) => ({ _node: stub.node });
const call = (self, t) => AxonaPeer.prototype._findCloserInTwoHops.call(self, t);
const stats = (self, o) => AxonaPeer.prototype.lookaheadStats.call(self, o);

const R = (n) => (0x89n << 248n) | BigInt(n);
const TARGET = R(0x0000);
const SELF   = R(0x1000);          // my distance to TARGET is 0x1000

console.log('4.83.0 — the three measurements the council falsified\n');

// ── 1. the free incoming answer, measured on its own ─────────────────
console.log('— incomingCouldAnswer is independent of what the probes did —');
{
  // Probes DO find a candidate (0x0900 < 0x1000), and an incoming link is
  // nearer still (0x0010). Under the old counter this scored answeredByProbe
  // and incomingCouldAnswer was invisible.
  const stub = mkNode(SELF, [R(0x0800)], {
    reply: () => ({ peerId: R(0x0900) }),
    incoming: [R(0x0010)],
  });
  const self = mkSelf(stub);
  const got = await call(self, TARGET);
  const s = stats(self);
  check('1. the incoming link wins the final answer', got === R(0x0010), `got ${got}`);
  check('2. the CALL is recorded as answerable for free, though probes also found one',
        s.incomingCouldAnswerCalls === 1, `got ${s.incomingCouldAnswerCalls}`);
  check('3. and as having beaten the probes', s.incomingWonFinalCalls === 1,
        `got ${s.incomingWonFinalCalls}`);
  check('4. the call still counts as answeredByProbe — that field is unchanged',
        s.answeredByProbe === 1, `got ${s.answeredByProbe}`);
  check('5. THE OLD READING: answeredByIncoming is 0 here, and that 0 must NOT '
        + 'be read as "incoming could not have answered"',
        s.answeredByIncoming === 0 && s.incomingCouldAnswerCalls === 1,
        JSON.stringify({ old: s.answeredByIncoming, corrected: s.incomingCouldAnswerCalls }));
}

// ── 1b. LINKS and CALLS are different units ──────────────────────────
// Aster fc8146ed: the old counter incremented once per qualifying SYNAPSE, so
// it could exceed `calls` and could not be divided by them. Three qualifying
// links in ONE call must read 3 links / 1 call — never 3 calls.
console.log('\n— a Links count is not a Calls count —');
{
  const stub = mkNode(SELF, [R(0x0800)], {
    reply: () => ({ terminal: true }),
    incoming: [R(0x0010), R(0x0020), R(0x0030)],   // all three beat myDist
  });
  const self = mkSelf(stub);
  await call(self, TARGET);
  const s = stats(self);
  check('5a. three qualifying links counted as three LINKS',
        s.incomingCandidateLinks === 3, `got ${s.incomingCandidateLinks}`);
  check('5b. but as ONE call — the unit that is comparable to calls',
        s.incomingCouldAnswerCalls === 1, `got ${s.incomingCouldAnswerCalls}`);
  check('5c. the free-answer rate divides calls by calls, so it cannot exceed 1',
        s.incomingCouldAnswerRate === 1, `got ${s.incomingCouldAnswerRate}`);
  check('5d. links exceed calls here — the exact shape that made the old name wrong',
        s.incomingCandidateLinks > s.incomingCouldAnswerCalls, '');
}

// ── 2. a dead rank is not an unhelpful rank ──────────────────────────
console.log('\n— rejected / terminal / nonCloser are separated per rank —');
{
  const peers = [R(0x0100), R(0x0200), R(0x0400)];   // ranks 0, 1, 2
  const stub = mkNode(SELF, peers, {
    reply: (p) => {
      if (p === R(0x0100)) return 'REJECT';                 // rank 0: DEAD
      if (p === R(0x0200)) return { terminal: true };       // rank 1: terminal
      return { peerId: R(0x9999) };                          // rank 2: farther
    },
  });
  const self = mkSelf(stub);
  await call(self, TARGET);
  const b = Object.fromEntries(stats(self).byRank.map(x => [x.rank, x]));
  check('6. rank 0 records a REJECTION, not a non-closer reply',
        b['0'].rejected === 1 && b['0'].nonCloser === 0, JSON.stringify(b['0']));
  check('7. rank 1 records a terminal reply', b['1'].terminal === 1, JSON.stringify(b['1']));
  check('8. rank 2 records a live, non-closer reply',
        b['2'].nonCloser === 1 && b['2'].rejected === 0, JSON.stringify(b['2']));
  check('9. all three read rate 0 — which is why the partition was needed',
        b['0'].rate === 0 && b['1'].rate === 0 && b['2'].rate === 0, '');
  // CORRECTED (Aster fc8146ed). The old rateOfAnswerable conditioned terminal
  // replies OUT of the denominator; a terminal reply is a LIVE peer reporting no
  // escape, which is evidence about that target, not absence of a measurement.
  // Excluding it inflated the hit rate. Only the unreachable are excluded now.
  check('10. rateOfReplies excludes ONLY the unreachable — rank 0 was rejected, '
        + 'so it has no replies and reads 0',
        b['0'].rateOfReplies === 0, JSON.stringify(b['0']));
  check('10a. a TERMINAL reply stays in the denominator — rank 1 replied and had '
        + 'nothing, which is a real 0 out of 1',
        b['1'].rateOfReplies === 0 && b['1'].terminal === 1, JSON.stringify(b['1']));
  check('10b. the inflating field is gone, not merely renamed',
        b['1'].rateOfAnswerable === undefined, JSON.stringify(Object.keys(b['1'])));
}

// ── 3. top-K is a per-CALL question ──────────────────────────────────
console.log('\n— topK counts calls kept, not replies discarded —');
{
  // 10 peers at increasing distance → ranks 0..9. Only ranks 2 and 7 reply
  // closer. A cut-off at K=4 still answers the call (rank 2 survives), even
  // though it discards the rank-7 reply: 50% of replies lost, 0% of calls lost.
  const peers = Array.from({ length: 10 }, (_, i) => R(0x0100 + i));
  const stub = mkNode(SELF, peers, {
    reply: (p) => (p === R(0x0102) || p === R(0x0107))
      ? { peerId: R(0x0001) } : { terminal: true },
  });
  const self = mkSelf(stub);
  await call(self, TARGET);
  const s = stats(self);
  const k = Object.fromEntries(s.topK.map(x => [x.k, x]));
  check('11. the call is counted once as having a closer reply',
        s.callsWithAnyCloser === 1, `got ${s.callsWithAnyCloser}`);
  check('12. K=1 and K=2 lose the call — nearest closer reply is at rank 2',
        k[1].answered === 0 && k[2].answered === 0, JSON.stringify([k[1], k[2]]));
  check('13. K=4 KEEPS the call although it discards the rank-7 reply',
        k[4].answered === 1 && k[4].retained === 1, JSON.stringify(k[4]));
  check('14. two closer replies, one call — the ratio the old figure used',
        s.probesCloserThanMe === 2 && s.callsWithAnyCloser === 1,
        JSON.stringify({ replies: s.probesCloserThanMe, calls: s.callsWithAnyCloser }));
}

// ── 4. does the probe set contain peers greedy would have taken? ─────
console.log('\n— targetsNearerThanSelf falsifies "all probe targets are farther" —');
{
  // R(0x0100) is NEARER to TARGET than SELF is. Greedy would have taken it had
  // it been eligible; it is in the raw synaptome regardless.
  const stub = mkNode(SELF, [R(0x0100), R(0x9000)], { reply: () => ({ terminal: true }) });
  const self = mkSelf(stub);
  await call(self, TARGET);
  const s = stats(self);
  check('15. counted: the raw synaptome held a nearer-than-self peer',
        s.targetsNearerThanSelf === 1, `got ${s.targetsNearerThanSelf}`);
}

console.log(`\n${failed === 0 ? '✓' : '✗'} ${passed}/${passed + failed}`);
process.exit(failed === 0 ? 0 : 1);
