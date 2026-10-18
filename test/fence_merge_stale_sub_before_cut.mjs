#!/usr/bin/env node
// =====================================================================
// fence_merge_stale_sub_before_cut.mjs — dual-root packet 4(d)
// stale_sub_before_cut + sub_other_corr (axona-docs 14a6bd8; David "build
// it" 2026-10-17).
//
// COMPLETION IS CORRELATED. An ordinary renewal completes nothing: a SUB L
// sent before the cut (no merge block) arriving at W after `acked` releases
// no pin; a merge block naming another corr, or other epochs, is logged
// merge-stale and completes nothing. Only a block matching the acked record
// on all five fields completes.
//
// Checks, with W's record acked and L's merge-bearing SUBs held: (1) a SUB
// from L without a merge block: no completion, pins intact; (2) a block
// with another corr: merge-stale, pins intact; (3) a block with another
// winnerEpoch: merge-stale; (4) another loserEpoch: merge-stale; (5)
// another hash: merge-stale; (6) the matching block completes:
// MERGE_CLOSED, pins released.
//
// Mutant (red): complete on any SUB from the loser (match on corr only or
// not at all) → 1–5 fail.
// Run: node test/fence_merge_stale_sub_before_cut.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_stale_sub_before_cut: 4(d) — only the correlated block completes');

const S = await splitScene('stale-sub-before-cut');
await S.stamp(S.W, 1); const B = await S.stamp(S.Ln, 2);
let held = [];
S.fab.tap = (j) => { if (j.type === 'pubsub:sub' && j.meta.fromId === idHex(S.lId) && j.payload.merge) { held.push(j); return null; } return j; };
await S.trigger();
const corr = S.frames('pubsub:mergeoffer')[0].payload.corr;
const p = S.wRole.sync.openPulls.get(corr);
if (!p || p.state !== 'acked' || !S.lRole.merge.completing) { console.log('setup failed', { p: p?.state, completing: !!S.lRole.merge.completing }); process.exit(1); }
const good = held[0].payload.merge;
const subAtW = async (merge) => {
  const base = { topicId: idHex(S.Lt.topicId), via: [idHex(S.wId)], subscriberId: idHex(S.lId), since: 0, hw: 0, lw: 0, latest: false };
  await S.W.handlers.get('pubsub:sub')(merge ? { ...base, merge } : base, { targetId: S.wId, isTerminal: true, hopCount: 1, fromId: idHex(S.lId) });
  await S.fab.settle();
};
const stale = () => S.logs(S.W, 'merge-stale').length;
const complete = () => S.logs(S.W, 'merge-complete').length;
await subAtW(null);
check('1 a SUB from L without a merge block (sent before the cut): no completion, pins intact', complete() === 0 && S.wRole.merge.pins.has(B) && p.state === 'acked');
const n0 = stale();
await subAtW({ ...good, corr: 'f'.repeat(32) });
check('2 a block with another corr: merge-stale, pins intact', complete() === 0 && stale() === n0 + 1 && S.wRole.merge.pins.has(B));
await subAtW({ ...good, winnerEpoch: good.winnerEpoch + 1 });
check('3 another winnerEpoch: merge-stale', complete() === 0 && stale() === n0 + 2);
await subAtW({ ...good, loserEpoch: good.loserEpoch + 1 });
check('4 another loserEpoch: merge-stale', complete() === 0 && stale() === n0 + 3);
await subAtW({ ...good, hash: 'deadbeef' });
check('5 another hash: merge-stale', complete() === 0 && stale() === n0 + 4 && S.wRole.merge.pins.has(B));
S.fab.tap = null;
await subAtW(good);
check('6 the matching block completes: MERGE_CLOSED routed to L, pins released, record closed', complete() === 1 && S.wRole.merge.pins.size === 0 && S.wRole.sync.openPulls.size === 0 && S.frames('pubsub:mergeclosed').length === 1 && S.lRole.merge.completing === null);

console.log(`\nfence_merge_stale_sub_before_cut: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
