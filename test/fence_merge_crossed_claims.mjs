#!/usr/bin/env node
// =====================================================================
// fence_merge_crossed_claims.mjs — dual-root packet Part 3 "one election
// relation" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// BETWEEN TWO LIVE CLAIMS EXACTLY ONE WINS, FROM EITHER SIDE'S VIEW. The
// beacon yield at rootElection.js reads `supersedesMine || closer`: a closer
// node at a lower epoch yields to a farther one at a higher epoch, and the
// farther one yields back on closeness. Two yields and no winner — a merge
// run on that predicate crosses offers forever. RootClaim.claimWins orders
// (id, epoch) lexicographically: epoch descending, then XOR distance
// ascending. Tombstoned claims are the caller's to exclude.
//
// Checks: (1) higher epoch wins over a closer lower-epoch claim; (2) equal
// epoch: closer wins; (2b) an UNVERSIONED side (epoch 0, the paths that do
// not learn the winner's epoch) falls back to closeness alone, the kernel's
// standing mixed-version rule; (3) antisymmetry over 300 random pairs
// (versioned or not); (4) transitivity over 300 random VERSIONED triples;
// (5) wins() reads this node's ROOT epoch and treats a non-root as
// unversioned; (6) THE CROSSING SCENE on the
// real beacon path, merge-capable with custody: the closer lower-epoch root
// records the farther higher-epoch root as its candidate, and the farther
// root hearing the closer one logs merge-candidate-loses and keeps its
// claim — one candidate, not two; (7) unarmed, both sides yield as 4.114.1
// (the two-yield behaviour is preserved when the machine is off).
//
// Mutant (red): order claimWins by closeness first → 1 and 6 fail; drop the
// wins() filter in _mergeCandidate → 6 fails.
// Run: node test/fence_merge_crossed_claims.mjs
// =====================================================================
import { RootClaim } from '../src/pubsub/rootClaim.js';
import { makeManager, seedCache, hearBeacon, log, idHex, SELF, T, NEAR, FAR } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const rnd = (() => { let s = 0x9e3779b9n; return () => { s = (s * 6364136223846793005n + 1442695040888963407n) & ((1n << 64n) - 1n); return s; }; })();
const randClaim = (minEpoch = 0) => ({ id: idHex(T ^ (rnd() & 0xffffffn) | 1n), epoch: minEpoch + Number(rnd() % 4n) });
const W = RootClaim.claimWins;

console.log('fence_merge_crossed_claims: Part 3 — one election relation, exactly one winner');

{ // 1, 2 — the order
  check('1 a higher epoch wins over a strictly closer claim at a lower epoch', W(T, { id: idHex(FAR), epoch: 3 }, { id: idHex(NEAR), epoch: 2 }) === true && W(T, { id: idHex(NEAR), epoch: 2 }, { id: idHex(FAR), epoch: 3 }) === false);
  check('2 at equal epoch the XOR-closer claim wins', W(T, { id: idHex(NEAR), epoch: 2 }, { id: idHex(FAR), epoch: 2 }) === true && W(T, { id: idHex(FAR), epoch: 2 }, { id: idHex(NEAR), epoch: 2 }) === false);
  check('2b an unversioned side falls back to closeness alone: a closer claim at epoch 0 wins over a farther one at epoch 3', W(T, { id: idHex(NEAR), epoch: 0 }, { id: idHex(FAR), epoch: 3 }) === true && W(T, { id: idHex(FAR), epoch: 3 }, { id: idHex(NEAR), epoch: 0 }) === false && W(T, { id: idHex(FAR), epoch: 0 }, { id: idHex(NEAR), epoch: 3 }) === false);
}
{ // 3 — antisymmetry
  let bad = 0;
  for (let i = 0; i < 300; i++) {
    const a = randClaim(), b = randClaim();
    if (a.id === b.id && a.epoch === b.epoch) continue;
    if (W(T, a, b) === W(T, b, a)) bad++;
  }
  check('3 antisymmetric: over 300 random distinct pairs exactly one side wins', bad === 0, `bad=${bad}`);
}
{ // 4 — transitivity
  let bad = 0;
  for (let i = 0; i < 300; i++) {
    const a = randClaim(1), b = randClaim(1), c = randClaim(1);
    if (W(T, a, b) && W(T, b, c) && !W(T, a, c)) bad++;
  }
  check('4 transitive: a>b and b>c imply a>c over 300 random VERSIONED triples', bad === 0, `bad=${bad}`);
}
{ // 5 — the instance method reads this node's claim
  const ctx = makeManager({ mergeCapable: true });
  check('5a wins() on a node that holds no root treats self as unversioned: closeness decides (NEAR wins, FAR loses)', ctx.rc.wins(T, { id: idHex(NEAR), epoch: 1 }) === true && ctx.rc.wins(T, { id: idHex(FAR), epoch: 1 }) === false);
  const role = ctx.rc.become(T, 'sub-terminal');
  role.epoch = 5;
  check('5b wins() on a root at epoch 5: a closer claim at epoch 4 loses, a farther claim at epoch 6 wins', ctx.rc.wins(T, { id: idHex(NEAR), epoch: 4 }) === false && ctx.rc.wins(T, { id: idHex(FAR), epoch: 6 }) === true);
}
{ // 6 — THE CROSSING SCENE, armed. Two managers playing the two sides of one pair.
  // Side A: SELF is the CLOSER root at epoch 2; hears FAR's beacon at epoch 3.
  const A = makeManager({ mergeCapable: true });
  const ra = seedCache(A, A.rc.become(T, 'sub-terminal'), 3); ra.epoch = 2;
  hearBeacon(A, FAR, 3);
  const candA = ra.merge?.candidate;
  check('6a the closer root at the lower epoch hears the farther higher-epoch beacon: it records that claim as its candidate and keeps serving', ra.isRoot === true && !!candA && candA.id === idHex(FAR) && candA.epoch === 3 && candA.source === 'epoch-superseded', JSON.stringify(candA));
  check('6a …no SUB was sent and nothing was demoted', A.routed.filter((r) => r.type === 'pubsub:sub').length === 0 && log(A, 'root-transition').filter((l) => l.ctx.isRoot === false).length === 0);
  // Side B: SELF is the FARTHER root at epoch 3; hears NEAR's beacon at epoch 2 (NEAR is closer than SELF).
  const B = makeManager({ mergeCapable: true });
  const rb = seedCache(B, B.rc.become(T, 'sub-terminal'), 3); rb.epoch = 3;
  hearBeacon(B, NEAR, 2);
  check('6b the farther root at the higher epoch hears the closer lower-epoch beacon: the candidate LOSES, no record, claim kept', rb.isRoot === true && !rb.merge?.candidate && log(B, 'merge-candidate-loses').length === 1 && log(B, 'merge-candidate-loses')[0].ctx.source === 'beacon-closer', JSON.stringify(log(B, 'merge-candidate-loses')));
  check('6c exactly one of the two sides holds a candidate: the offers cannot cross', !!candA && !rb.merge?.candidate);
}
{ // 7 — unarmed: 4.114.1 behaviour on both sides (two yields).
  const A = makeManager({ mergeCapable: false });
  const ra = seedCache(A, A.rc.become(T, 'sub-terminal'), 3); ra.epoch = 2;
  hearBeacon(A, FAR, 3);
  const B = makeManager({ mergeCapable: false });
  const rb = seedCache(B, B.rc.become(T, 'sub-terminal'), 3); rb.epoch = 3;
  hearBeacon(B, NEAR, 2);
  check('7 unarmed, both sides yield as 4.114.1 did (epoch-superseded on one, beacon-closer on the other)', ra.isRoot === false && rb.isRoot === false && !ra.merge?.candidate && !rb.merge?.candidate, `A.isRoot=${ra.isRoot} B.isRoot=${rb.isRoot}`);
}

console.log(`\nfence_merge_crossed_claims: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
