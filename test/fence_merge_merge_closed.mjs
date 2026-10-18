#!/usr/bin/env node
// =====================================================================
// fence_merge_merge_closed.mjs — dual-root packet 4(d) MERGE_CLOSED
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// L CLEARS `completing` ONLY ON A MERGE_CLOSED MATCHING ITS corr AND
// winnerEpoch, and keeps re-sending the block until then. On the match L
// releases its pinned export (custody is W's now) and drops the record.
//
// Checks, with L completing (its merge-bearing SUB held so W cannot close):
// (1) a MERGE_CLOSED with another corr: merge-stale, completing kept,
// pins kept; (2) another winnerEpoch: merge-stale; (3) the matching one:
// completing cleared, pins released, record dropped, merge-closed logged;
// (4) a second matching MERGE_CLOSED after that is stale (idempotent, no
// crash).
//
// Mutant (red): accept any MERGE_CLOSED for the topic → 1 and 2 fail.
// Run: node test/fence_merge_merge_closed.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_merge_closed: 4(d) — the loser clears completing only on the matching MERGE_CLOSED');

const S = await splitScene('merge-closed');
await S.stamp(S.W, 1); const B = await S.stamp(S.Ln, 2);
S.fab.tap = (j) => (j.type === 'pubsub:sub' && j.payload.merge) ? null : j;
await S.trigger();
const c = S.lRole.merge.completing;
if (!c) { console.log('setup failed: not completing'); process.exit(1); }
const closedAtL = async (payload) => { await S.Ln.handlers.get('pubsub:mergeclosed')({ topicId: idHex(S.Lt.topicId), ...payload }, { targetId: S.lId, isTerminal: true, hopCount: 1, fromId: idHex(S.wId) }); };
const stale = () => S.logs(S.Ln, 'merge-stale').length;
const n0 = stale();
await closedAtL({ corr: 'e'.repeat(32), winnerEpoch: c.winnerEpoch });
check('1 a MERGE_CLOSED with another corr: merge-stale, completing kept, pins kept', stale() === n0 + 1 && S.lRole.merge.completing === c && S.lRole.merge.pins.has(B));
await closedAtL({ corr: c.corr, winnerEpoch: c.winnerEpoch + 1 });
check('2 another winnerEpoch: merge-stale, completing kept', stale() === n0 + 2 && S.lRole.merge.completing === c);
await closedAtL({ corr: c.corr, winnerEpoch: c.winnerEpoch });
check('3 the matching MERGE_CLOSED: completing cleared, pins released, record dropped, merge-closed logged', S.lRole.merge.completing === null && S.lRole.merge.pins.size === 0 && S.lRole.sync.openMerges.size === 0 && S.logs(S.Ln, 'merge-closed').length === 1);
await closedAtL({ corr: c.corr, winnerEpoch: c.winnerEpoch });
check('4 a second matching MERGE_CLOSED is stale (idempotent)', stale() === n0 + 3 && S.logs(S.Ln, 'merge-closed').length === 1);

console.log(`\nfence_merge_merge_closed: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
