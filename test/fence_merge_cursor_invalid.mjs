#!/usr/bin/env node
// =====================================================================
// fence_merge_cursor_invalid.mjs — dual-root packet 4(d) "The cursor"
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// L VALIDATES EVERY INCOMING CURSOR AGAINST lastCursorSent: equal means
// continue, anything else is answered with nothing (W restarts with a new
// corr after its timeout). The export is fixed at the cut: an entry the TTL
// expires out of the live cache during the merge stays in the snapshot, so
// a long merge does not lose its own head.
//
// Checks, with a bound round: (1) a PULLUP whose cursor is not the last
// one sent is logged merge-cursor-invalid and answered with no frame; (2)
// the correct cursor is answered; (3) an entry expired by TTL mid-merge
// (clock advanced, _expireCache run) is gone from the live cache and still
// in the frame; (4) the cursor pair is (publishTs, msgId) of the last entry
// sent, not a count.
//
// Mutant (red): answer a wrong cursor with the next frame anyway → 1 fails;
// export from the live cache instead of the snapshot → 3 fails.
// Run: node test/fence_merge_cursor_invalid.mjs
// =====================================================================
import { TTL_MS } from '../src/pubsub/constants.js';
import { realTopic, makeManagerAt, rootWithBodies, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_cursor_invalid: 4(d) — the cursor is validated; the export is the snapshot');

const L = await realTopic('cursor-invalid');
const meta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
const ctx = makeManagerAt(L.self);
const { role, ids } = await rootWithBodies(ctx, L, 3);
ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
const offer = ctx.routed.find((r) => r.type === 'pubsub:mergeoffer');
const corr = offer.payload.corr;
const pull = (cursor) => ctx.am._onPullUp({ topicId: idHex(L.topicId), corr, parentId: idHex(L.near), winnerEpoch: 3, cursor, sinceHw: 0, timing: offer.payload.timing }, meta(ctx, L.near));
const replays = () => ctx.routed.filter((r) => r.type === 'pubsub:replayup' && r.payload.corr === corr);
pull(null); for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
const n0 = replays().length;                                   // the header
pull('999:nope');
check('1 a PULLUP with a cursor that is not the last one sent: merge-cursor-invalid, no frame', log(ctx, 'merge-cursor-invalid').length === 1 && replays().length === n0, `frames=${replays().length}`);
// the TTL expires the oldest entry out of the live cache mid-merge
ctx.clock.t += TTL_MS + 1_000;
ctx.am._expireCache(role, ctx.clock.t);
check('3a the live cache lost its head to the TTL mid-merge', !role.cacheIds.has(ids[0]) && role.cache.length < 3);
pull('h');
const f1 = replays()[n0];
check('2 the correct cursor (the header\'s) is answered with the first data frame', !!f1 && Array.isArray(f1.payload.msgs) && f1.payload.msgs.length === 3 && f1.payload.more === false);
check('3 the expired entry is still in the frame: the export is the snapshot fixed at the cut', !!f1 && f1.payload.msgs.some((m) => m.msgId === ids[0]));
const last = f1?.payload?.msgs?.[2];
check('4 the frame\'s lastCursor is the (publishTs, msgId) pair of the last entry sent', !!last && f1.payload.lastCursor === `${last.publishTs}:${last.msgId}` && role.sync.openMerges.get(corr).lastCursorSent === f1.payload.lastCursor);

console.log(`\nfence_merge_cursor_invalid: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
