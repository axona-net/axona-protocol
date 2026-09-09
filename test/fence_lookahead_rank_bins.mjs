// =====================================================================
// fence_lookahead_rank_bins.mjs — 4.82.0
//
// The lookahead fan-out is load-bearing and massively redundant. Measured on a
// live axona.chat tab (571s, kernel 4.81.0):
//
//   probesPerCall        67.3     the entire synaptome
//   usefulProbeRate      0.880    the fan-out answers 88% of routing calls
//   closerReplyRate      0.0271   but only 2.7% of REPLIES name a closer node
//
// So it needs one or two informative replies and sends sixty-seven. The obvious
// remedy is top-K by XOR distance, which 4.78.0's fence note records as
// considered and NOT bundled, on Aster's objection that degree is not global
// completeness.
//
// Top-K only works if informative replies CONCENTRATE in the nearest targets.
// Nobody has checked. This binning answers it: closer-replies over probes-sent,
// per XOR-rank bucket. Concentrated in low ranks => a narrow K keeps the
// answers. Flat across rank => top-K cannot work at any K, and the redundancy
// needs a different mechanism entirely.
//
// A measurement that will decide a design change has to be pinned, and the
// load-bearing property is the one easiest to break by accident: RANK MUST NOT
// CHANGE WHO IS PROBED. The ranking exists for accounting only. If a future
// edit sorts probeTargets in place to compute ranks, the fan-out silently
// becomes ordered by XOR — a behaviour change wearing a measurement's clothes.
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
          return typeof reply === 'function' ? reply(peerId) : reply;
        },
      },
    },
  };
}
const mkSelf = (stub) => ({ _node: stub.node });
const call = (self, target) => AxonaPeer.prototype._findCloserInTwoHops.call(self, target);
const stats = (self, o) => AxonaPeer.prototype.lookaheadStats.call(self, o);

const R = (n) => (0x89n << 248n) | BigInt(n);
const SELF   = R(0x1000);
const TARGET = R(0x0000);          // self is 0x1000 away from target

console.log('4.82.0 — XOR-rank binning of lookahead replies\n');

// ── 1. rank follows XOR distance, NOT insertion order ────────────────
console.log('— rank is by distance, not by the order peers happen to sit in —');
{
  // Deliberately inserted worst-first, so array order is the REVERSE of rank.
  const peers = [R(0x0800), R(0x0400), R(0x0200), R(0x0100)];
  //  distances to TARGET: 0x800, 0x400, 0x200, 0x100 → ranks 3,2,1,0
  const stub = mkNode(SELF, peers, { reply: (p) => ({ peerId: p }) });
  const self = mkSelf(stub);
  await call(self, TARGET);
  const s = stats(self);
  const byRank = Object.fromEntries(s.byRank.map(b => [b.rank, b]));

  check('1. every peer was probed exactly once', stub.sent.length === 4,
        `sent ${stub.sent.length}`);
  check('2. probe ORDER is unchanged — ranking must not reorder the fan-out',
        stub.sent.map(x => x.peerId).join() === peers.join(),
        `got ${stub.sent.map(x => x.peerId).join()}`);
  check('3. one probe recorded in each of ranks 0..3',
        [0,1,2,3].every(r => byRank[String(r)]?.sent === 1),
        JSON.stringify(s.byRank));
  // Every reply here names a node closer than SELF (all are nearer TARGET),
  // so each rank should show one closer reply — which also proves the reply was
  // attributed to the RIGHT rank rather than to its array position.
  check('4. the closest peer scores in rank 0, not in rank 3',
        byRank['0']?.closer === 1 && byRank['0']?.rate === 1,
        JSON.stringify(byRank['0']));
}

// ── 2. bucket boundaries ─────────────────────────────────────────────
console.log('\n— buckets: 0-7 individually, then 8-15, 16-31, 32+ —');
{
  // 40 peers at strictly increasing distance, so peer i has rank i exactly.
  const peers = Array.from({ length: 40 }, (_, i) => R(0x0100 + i));
  const stub = mkNode(SELF, peers, { reply: null });   // no usable replies
  const self = mkSelf(stub);
  await call(self, TARGET);
  const s = stats(self);
  const byRank = Object.fromEntries(s.byRank.map(b => [b.rank, b]));

  check('5. ranks 0-7 are their own buckets',
        [0,1,2,3,4,5,6,7].every(r => byRank[String(r)]?.sent === 1),
        JSON.stringify(s.byRank));
  check('6. 8-15 holds eight', byRank['8-15']?.sent === 8, JSON.stringify(byRank['8-15']));
  check('7. 16-31 holds sixteen', byRank['16-31']?.sent === 16, JSON.stringify(byRank['16-31']));
  check('8. 32+ holds the remaining eight', byRank['32+']?.sent === 8, JSON.stringify(byRank['32+']));
  check('9. sent across buckets equals probes emitted',
        s.byRank.reduce((a, b) => a + b.sent, 0) === s.probesEmitted,
        `${s.byRank.reduce((a,b)=>a+b.sent,0)} vs ${s.probesEmitted}`);
}

// ── 3. a reply that is NOT closer must not score ──────────────────────
console.log('\n— only replies naming a node closer than ME count as closer —');
{
  const peers = [R(0x0100), R(0x0200)];
  // Both replies name a node FARTHER from TARGET than SELF is (0x1000).
  const stub = mkNode(SELF, peers, { reply: () => ({ peerId: R(0x9000) }) });
  const self = mkSelf(stub);
  await call(self, TARGET);
  const s = stats(self);
  check('10. no bucket records a closer reply',
        s.byRank.every(b => b.closer === 0), JSON.stringify(s.byRank));
  check('11. rate is 0 where nothing was closer',
        s.byRank.every(b => b.rate === 0), JSON.stringify(s.byRank));
  check('12. the probes are still counted as sent',
        s.byRank.reduce((a, b) => a + b.sent, 0) === 2, JSON.stringify(s.byRank));
}

// ── 4. the destination bypass still emits nothing at all ─────────────
console.log('\n— the 4.78.0 bypass is upstream of all of this —');
{
  const stub = mkNode(SELF, [R(0x0100), R(0x0200)]);
  const self = mkSelf(stub);
  await call(self, SELF);
  const s = stats(self);
  check('13. zero probes at the destination', stub.sent.length === 0, `sent ${stub.sent.length}`);
  check('14. no rank buckets recorded', s.byRank.length === 0, JSON.stringify(s.byRank));
  check('15. counted as a bypass, not as a probing call',
        s.bypassedAtDestination === 1 && s.probingCalls === 0,
        `bypass=${s.bypassedAtDestination} probing=${s.probingCalls}`);
}

console.log(`\n${failed === 0 ? '✓' : '✗'} ${passed}/${passed + failed}`);
process.exit(failed === 0 ? 0 : 1);
