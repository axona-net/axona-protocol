#!/usr/bin/env node
// =====================================================================
// fence_cbr_partial_pull_never_serves.mjs — Closer-Born Relay Inherits the
// Root v0.4, Part 3e step 6 (axona-docs e38601e; David "Go ahead"
// 2026-11-03). PROBE MODE first: prints what today's kernel does when the
// winner's merge pull times out before its terminal ack.
//
// Live case (2026-11-03 01:38:02Z, ops/evidence/council-split-20261102):
// slot 19 (closer, born root) merge-pulled 79 from an axona.chat client,
// logged merge-pull-timeout at 01:41:17Z, kept the bodies, and served them
// as a second root that never received the loser's later posts.
//
// Scene: splitScene; L stamps three; the tap drops every corr-tagged
// REPLAYUP after the first, so W stays 'pulling'; the clock passes
// MERGE_TIMEOUT_MS and both sweeps run; L then stamps a fourth.
// Invariant asserted once 3e lands: (1) after the timeout exactly one of
// W, L is root; (2) L's fourth body reaches whichever node is root.
// Measured on v4.128.0 (9e66eb5), 2026-11-03: W's pull times out with no
// body kept (check 3 holds), L aborts and clears its gate, a re-verify does
// NOT re-offer, and two roots stand (check 1 RED). EXPECTED RED until 3e;
// not in test/manifest.json until it turns green.
// Run: node test/fence_cbr_partial_pull_never_serves.mjs
// =====================================================================
import { splitScene } from './lib/mergeFixture.mjs';
import { MERGE_TIMEOUT_MS, MERGE_OFFER_TIMEOUT_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
console.log('fence_cbr_partial_pull_never_serves: 3e — a timed-out pull never leaves two serving roots');

const S = await splitScene('cbr-partial-pull');
const l1 = await S.stamp(S.Ln, 1, 4000), l2 = await S.stamp(S.Ln, 2, 4000), l3 = await S.stamp(S.Ln, 3, 4000);
let seenCorrReplies = 0;
S.fab.tap = (j) => {
  const corrish = j.payload && typeof j.payload.corr === 'string';
  if (j.type === 'pubsub:replayup' && corrish) { seenCorrReplies++; if (seenCorrReplies > 1) return null; }
  return j;
};
await S.trigger();
const types = {}; for (const j of S.fab.sent || []) types[j.type] = (types[j.type] || 0) + 1;
console.log('  probe: frame types', JSON.stringify(types), 'corr REPLAYUPs seen', seenCorrReplies);
console.log('  probe: W logs', S.W.logs.filter((l) => /merge/.test(l.ev)).map((l) => l.ev.replace('pubsub:', '')).join(','));
console.log('  probe: L logs', S.Ln.logs.filter((l) => /merge/.test(l.ev)).map((l) => l.ev.replace('pubsub:', '')).join(','));
S.fab.tap = null;
S.fab.clock += Math.max(MERGE_TIMEOUT_MS, MERGE_OFFER_TIMEOUT_MS) + 1;
S.W.am._mergeSweep(S.fab.clock); S.Ln.am._mergeSweep(S.fab.clock);
await S.fab.settle();
console.log('  probe: after timeout W', S.W.logs.filter((l) => /merge/.test(l.ev)).map((l) => l.ev.replace('pubsub:', '')).slice(-4).join(','), '| L', S.Ln.logs.filter((l) => /merge/.test(l.ev)).map((l) => l.ev.replace('pubsub:', '')).slice(-4).join(','));
console.log('  probe: W isRoot', S.wRole.isRoot, 'cache', S.wRole.cache.length, '| L isRoot', S.lRole.isRoot, 'cache', S.lRole.cache.length);
await S.trigger();   // the live verify re-runs every ROOT_VERIFY_MS; give the machine its retry
console.log('  probe: after re-verify W', S.W.logs.filter((l) => /merge/.test(l.ev)).map((l) => l.ev.replace('pubsub:', '')).slice(-4).join(','), '| L', S.Ln.logs.filter((l) => /merge/.test(l.ev)).map((l) => l.ev.replace('pubsub:', '')).slice(-4).join(','), '| W root', S.wRole.isRoot, 'cache', S.wRole.cache.length, '| L root', S.lRole.isRoot);
const l4 = await S.stamp(S.Ln, 4);
const roots = [S.wRole.isRoot, S.lRole.isRoot].filter(Boolean).length;
check('1 after the timeout exactly one of W, L is root', roots === 1, `roots=${roots}`);
const rootRole = S.wRole.isRoot ? S.wRole : S.lRole;
check('2 L\'s later body reaches the root', S.wRole.isRoot ? S.wRole.cacheIds.has(l4) : true, `W cache ${S.wRole.cache.length}`);
check('3 W holds no body it pulled under a timed-out corr while serving as a second root', !(roots === 2 && [l1, l2, l3].some((id) => S.wRole.cacheIds.has(id))), 'two roots and W serves pulled bodies');
console.log(`\nfence_cbr_partial_pull_never_serves: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
