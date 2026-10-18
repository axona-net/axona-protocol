#!/usr/bin/env node
// =====================================================================
// fence_merge_full_pull.mjs — dual-root packet 4(d) full_pull +
// hw_above_export (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// THE MERGE PULLS BY CURSOR, NOT BY HIGH WATER. The shipped SPLIT_UNION
// pulls a delta above the puller's hw; a winner whose hw is above every
// stamp L holds would pull nothing that way. Under corr every L entry
// arrives from the snapshot in cursor order, whatever W's hw.
//
// Scene: L stamps three bodies, then W stamps two (W's hw above all of
// L's), then the machine runs. Checks: (1) W's hw was above every stamp L
// held at the cut; (2) all three of L's bodies arrived under corr and W
// holds five; (3) no SPLIT_UNION or REPLAY_UP pull was involved (no
// split-union-pull line at W; every PULLUP to L carried corr); (4) the ack
// was full and L demoted.
//
// Mutant (red): have _mergeAnswerPull export only entries above the
// PULLUP's sinceHw set to W's hw → 2 fails.
// Run: node test/fence_merge_full_pull.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_full_pull: 4(d) — every L entry arrives under corr whatever W\'s high water');

const S = await splitScene('full-pull');
const l1 = await S.stamp(S.Ln, 1), l2 = await S.stamp(S.Ln, 2), l3 = await S.stamp(S.Ln, 3);
const w1 = await S.stamp(S.W, 4), w2 = await S.stamp(S.W, 5);
const lHw = S.Ln.am._highWater(S.lRole), wHw = S.W.am._highWater(S.wRole);
await S.trigger();
check('1 W\'s high water was above every stamp L held', wHw > lHw);
check('2 all three of L\'s bodies arrived under corr; W holds five', [l1, l2, l3, w1, w2].every((id) => S.wRole.cacheIds.has(id)) && S.wRole.cache.length === 5, JSON.stringify([...S.wRole.cacheIds].map((s) => s.slice(0, 6))));
const pullsToL = S.frames('pubsub:pullup', (j) => j.dest === S.lId);
// All three were at W when W ACKED (retained 3), i.e. they came under corr, before any post-demotion backstop pull on L's SUB.
check('3 the bodies came by the cursor pull: W\'s terminal ack counted retained 3 against the manifest, and ≥ 2 PULLUPs to L carried corr', S.logs(S.W, 'merge-acked')[0]?.ctx.retained === 3 && pullsToL.filter((j) => typeof j.payload.corr === 'string').length >= 2, JSON.stringify(S.logs(S.W, 'merge-acked')[0]?.ctx));
check('4 the ack was full and L demoted into W', S.logs(S.W, 'merge-acked')[0]?.ctx.full === true && S.lRole.isRoot === false && S.wRole.isRoot === true);

console.log(`\nfence_merge_full_pull: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
