#!/usr/bin/env node
// =====================================================================
// fence_merge_commit_guard.mjs — dual-root packet 4(d) commit_guard +
// lease_overlap (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// THE RECHECK IS MADE AT THE COMMIT. The test `now_mono ≤ tB + A` runs
// again, synchronously, immediately before `_set`. A suspension of the run
// between the recheck and the flip longer than P violates assumption 7; L
// cannot prevent it and detects it after the fact: the flip stands, it is
// logged merge-late-flip, NO COMPLETION IS CLAIMED (no merge-bearing SUB),
// L holds the export pinned as W's child, W's record fails at tS + R, and
// no entry leaves before L's TTL. The data is safe; W's union is not
// claimed.
//
// Checks: (A) the monotonic clock jumps past tB + A between the recheck
// and the flip's return: merge-late-flip logged, L demoted (the flip
// stands), completing NOT set, no merge-bearing SUB sent, the export
// pinned at L; W's record fails at tS + R; (B) the ack arrives inside the
// lease but the FIRST check passes and the recheck fails (the clock moves
// between them): no flip, merge-ack-expired, L root; (C) control: no
// suspension → step 5 and completion.
//
// Mutant (red): check once at the ack and not at the commit → B fails
// (flips); drop the late-flip rule → A fails (completion claimed).
// Run: node test/fence_merge_commit_guard.mjs
// =====================================================================
import { MERGE_TIMEOUT_MS, MERGE_RETENTION_MS } from '../src/pubsub/constants.js';
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_commit_guard: 4(d) — the recheck at the commit; a late flip claims no completion');

/** `monoSeq` yields successive monotonic readings at L: [bind, ack-check, recheck, post-flip, ...]. */
async function scene(name, monoSeq) {
  const S = await splitScene(name);
  await S.stamp(S.W, 1); const B = await S.stamp(S.Ln, 2);
  let i = 0; const seq = monoSeq; S.Ln.am._mono = () => (i < seq.length ? seq[i++] : seq[seq.length - 1]);
  const subsFromL = [];
  S.fab.tap = (j) => { if (j.type === 'pubsub:sub' && j.meta.fromId === idHex(S.lId) && j.payload.merge) subsFromL.push(j); return j; };
  await S.trigger();
  return { S, B, subsFromL };
}
const T0 = 1_000_000, A = MERGE_TIMEOUT_MS;
{ // A — suspended between the recheck and the flip's return
  const { S, B, subsFromL } = await scene('commit-guard-A', [T0, T0 + 1000, T0 + 1000, T0 + A + 1]);
  check('A late flip: logged merge-late-flip, the flip stands (L demoted), NO completion claimed (completing null, no merge-bearing SUB), export pinned at L', S.logs(S.Ln, 'merge-late-flip').length === 1 && S.lRole.isRoot === false && S.lRole.merge.completing === null && subsFromL.length === 0 && S.lRole.merge.pins.has(B) && S.logs(S.W, 'merge-complete').length === 0, JSON.stringify({ late: S.logs(S.Ln, 'merge-late-flip').length, isRoot: S.lRole.isRoot, completing: S.lRole.merge.completing, subs: subsFromL.length }));
  S.fab.clock += MERGE_RETENTION_MS + 1; S.W.am._mergeSweep(S.W.am._now());
  check('A W\'s record fails at tS + R; the data is at both ends, the holder is L', S.logs(S.W, 'merge-retention-expired').length === 1 && S.lRole.cacheIds.has(B) && S.wRole.cacheIds.has(B));
}
{ // B — the first check passes, the recheck fails
  const { S } = await scene('commit-guard-B', [T0, T0 + 1000, T0 + A + 1]);
  check('B the recheck at the commit fails: no flip, merge-ack-expired, L root with custody', S.logs(S.Ln, 'merge-ack-expired').length === 1 && S.logs(S.Ln, 'merge-ack-expired')[0].ctx.at === 'commit' && S.lRole.isRoot === true && S.logs(S.Ln, 'merge-step5').length === 0 && S.logs(S.Ln, 'merge-late-flip').length === 0, JSON.stringify(S.Ln.logs.map((l) => l.ev).slice(-4)));
}
{ // C — control
  const { S, subsFromL } = await scene('commit-guard-C', [T0, T0 + 1000, T0 + 1000, T0 + 1001]);
  check('C control: step 5 inside the lease, completion claimed and closed', S.logs(S.Ln, 'merge-step5').length === 1 && subsFromL.length >= 1 && S.logs(S.W, 'merge-complete').length === 1 && S.lRole.merge.completing === null);
}

console.log(`\nfence_merge_commit_guard: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
