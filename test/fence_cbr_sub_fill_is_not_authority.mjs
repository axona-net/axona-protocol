#!/usr/bin/env node
// =====================================================================
// fence_cbr_sub_fill_is_not_authority.mjs — Closer-Born Relay Inherits the
// Root v0.4, Parts 3e and I3 (axona-docs e38601e; David "go ahead with the
// cutover state machine" 2026-11-03).
//
// Live case (2026-11-03, ops/evidence/council-split-20261102): an
// axona.chat client C, a root for council at epoch 1, found slot 19 (W,
// closer, born root and empty) on its verify and offered the merge; W's pull
// timed out (01:41:17Z); W nonetheless ended holding C's 79 bodies with no
// merge-admitted row. Aster's seat A, root at epoch 2 with the live stream,
// stayed a separate root: A and W cannot reach each other.
//
// CORRECTION (2026-11-03): the first version of this fence had C send W a
// SUB carrying hw while C stayed root. The kernel never does that: a root
// SUBs a closer node only after demoting toward it, and the demotion here is
// the merge machine's LEGACY FALLBACK (an offer unanswered past
// MERGE_OFFER_TIMEOUT_MS demotes L; its next SUB carries hw; the winner pulls
// by REPLAY_UP, no corr, no merge row). So the live fill is C merging into
// the closer W by the fallback, and the remaining split is A against W, cut
// both ways, which the packet's condition C2 excludes from what any cutover
// can heal (it needs a route in at least one direction; that is the reply
// route's job across the island, a separate fence).
//
// Scene: W closest, born root empty; C farther, root, stamps three; A a
// third root stamping its own; A<->W cut both ways (the island). C's verify
// visits W and the machine runs: once with the first corr reply dropped (the
// live timeout), then a re-verify.
//   F1 (cutover on): exactly one root among C and W; W holds C's three; C is
//      W's child; the handover completed by the correlated machine (a
//      merge-acked row at W), never by the legacy fallback.
//   F2 (cutover off, 4.130.0): the aborted round is offered again, so the
//      same scene ends with one root by the correlated machine. Through
//      4.129.1 _mergeAbort kept the candidate without cutover and the scene
//      stopped at merge-abort with two roots.
//   F3 recorded, not asserted: A stays a separate root (C2: no route either
//      way between A and W).
// Run: node test/fence_cbr_sub_fill_is_not_authority.mjs
// =====================================================================
import { splitScene, freshId, idHex } from './lib/mergeFixture.mjs';
import { MERGE_TIMEOUT_MS, MERGE_OFFER_TIMEOUT_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
console.log('fence_cbr_sub_fill_is_not_authority: 3e — a closer born-empty root inherits by the correlated machine, never by the legacy fallback');

async function run(cutover) {
  const opts = cutover ? { cutover: true } : {};
  const S = await splitScene(`cbr-sub-fill-${cutover}`, { wOpts: opts, lOpts: opts });
  const C = S.Ln, cRole = S.lRole, W = S.W, wRole = S.wRole;
  const c1 = await S.stamp(C, 1, 4000), c2 = await S.stamp(C, 2, 4000), c3 = await S.stamp(C, 3, 4000);
  let aId; do { aId = await freshId(); } while ((aId ^ S.Lt.topicId) < (S.wId ^ S.Lt.topicId));
  const A = S.fab.addNode(aId, opts); const aRole = A.rc.become(S.Lt.topicId, 'sub-terminal');
  const cut = (j) => { const f = j.meta?.fromId ? BigInt('0x' + j.meta.fromId) : null; return (f === aId && j.dest === S.wId) || (f === S.wId && j.dest === aId); };
  let corrReplies = 0;
  S.fab.tap = (j) => { if (cut(j)) return null; if (j.type === 'pubsub:replayup' && typeof j.payload?.corr === 'string' && ++corrReplies > 1) return null; return j; };
  await S.trigger();
  S.fab.tap = (j) => cut(j) ? null : j;
  S.fab.clock += Math.max(MERGE_TIMEOUT_MS, MERGE_OFFER_TIMEOUT_MS) + 1;
  for (const n of [W, C, A]) n.am._mergeSweep(S.fab.clock);
  await S.fab.settle();
  await S.trigger();
  const ev = (n) => n.logs.map((l) => l.ev.replace('pubsub:', ''));
  return { S, C, W, A, cRole, wRole, aRole, ids: [c1, c2, c3], ev };
}

const on = await run(true);
console.log('  probe (on):  W', on.ev(on.W).filter((e) => /merge/.test(e)).join(','), '| C', on.ev(on.C).filter((e) => /merge/.test(e)).slice(-5).join(','));
const roots = [on.cRole.isRoot, on.wRole.isRoot].filter(Boolean).length;
check('F1 exactly one root among C and W', roots === 1, `roots=${roots}`);
check('F1 … W is that root and holds C\'s three', on.wRole.isRoot && on.ids.every((id) => on.wRole.cacheIds.has(id)), `W root ${on.wRole.isRoot} cache ${on.wRole.cache.length}`);
check('F1 … by the correlated machine (merge-acked at W), never the legacy fallback at C', on.ev(on.W).includes('merge-acked') && !on.ev(on.C).includes('merge-legacy-fallback'), on.ev(on.C).filter((e) => /merge/.test(e)).join(','));

const off = await run(false);
console.log('  probe (off): W', off.ev(off.W).filter((e) => /merge/.test(e)).join(','), '| C', off.ev(off.C).filter((e) => /merge/.test(e)).slice(-5).join(','));
const offRoots = [off.cRole.isRoot, off.wRole.isRoot].filter(Boolean).length;
check('F2 cutover off: the aborted round is retried — one root, by the correlated machine', offRoots === 1 && off.wRole.isRoot && off.ev(off.W).includes('merge-acked') && !off.ev(off.C).includes('merge-legacy-fallback'), `roots=${offRoots}`);

console.log(`  F3 recorded: A is a separate root (C2, cut both ways): on ${on.aRole.isRoot}, off ${off.aRole.isRoot}`);
console.log(`\nfence_cbr_sub_fill_is_not_authority: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
