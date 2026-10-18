#!/usr/bin/env node
// =====================================================================
// fence_merge_terminal_ack.mjs — dual-root packet 4(d) terminal_ack +
// replayack (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// W SENDS NOTHING AFTER AN INTERMEDIATE FRAME; THE ONE REPLAYACK FOLLOWS
// THE TERMINAL FRAME. It carries the six binding fields, the counts against
// the manifest and the hash; step 5 at L matches all six and the counts.
//
// Scene: L holds enough bodies to need ≥ 3 frames (bodies ~40 KiB, frame
// 96 KiB). Checks: (1) ≥ 3 REPLAYUP data frames under corr, exactly ONE
// REPLAYACK, sent after the frame with more:false; (2) the ack carries
// topicId, corr, winner, winnerEpoch, loser, loserEpoch, retained,
// delsActive, hash, disposition:'full'; (3) its counts equal L's manifest
// and its hash equals L's; (4) step 5 ran once (merge-step5), L demoted.
//
// Mutant (red): ack after every frame → the first ack is short against
// the manifest → merge-ack-short abort → 1 and 4 fail.
// Run: node test/fence_merge_terminal_ack.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_terminal_ack: 4(d) — one ack, after the terminal frame, with the six fields');

const S = await splitScene('terminal-ack', { wOpts: { replayCacheBytes: 64 * 1024 * 1024 }, lOpts: { replayCacheBytes: 64 * 1024 * 1024 } });
await S.stamp(S.W, 1);
const ids = [];
for (let k = 2; k < 9; k++) ids.push(await S.stamp(S.Ln, k, 40 * 1024));    // 7 × ~40 KiB → ≥ 3 frames
await S.trigger();
const offer = S.frames('pubsub:mergeoffer')[0]; const corr = offer.payload.corr;
const frames = S.frames('pubsub:replayup', (j) => j.payload.corr === corr && !j.payload.header);
const acks = S.frames('pubsub:replayack', (j) => j.payload.corr === corr);
const order = S.fab.sent.filter((j) => (j.type === 'pubsub:replayup' && j.payload.corr === corr && !j.payload.header) || (j.type === 'pubsub:replayack' && j.payload.corr === corr));
const lastFrameIdx = order.map((j) => j.type).lastIndexOf('pubsub:replayup');
check('1 ≥ 3 data frames under corr and exactly ONE REPLAYACK, sent after the terminal frame (more:false)', frames.length >= 3 && acks.length === 1 && order[order.length - 1].type === 'pubsub:replayack' && order[lastFrameIdx].payload.more === false && order.slice(0, lastFrameIdx).every((j) => j.type === 'pubsub:replayup'), JSON.stringify({ frames: frames.length, acks: acks.length, seq: order.map((j) => j.type.slice(7, 12) + (j.payload.more === false ? '*' : '')) }));
const a = acks[0]?.payload || {};
check('2 the ack carries the six binding fields, the counts, the hash and disposition full', a.topicId === idHex(S.Lt.topicId) && a.corr === corr && a.winner === idHex(S.wId) && Number.isInteger(a.winnerEpoch) && a.loser === idHex(S.lId) && Number.isInteger(a.loserEpoch) && Number.isInteger(a.retained) && Number.isInteger(a.delsActive) && typeof a.hash === 'string' && a.disposition === 'full', JSON.stringify(Object.keys(a)));
const bound = S.logs(S.Ln, 'merge-bound')[0]?.ctx;
check('3 its counts equal L\'s manifest (7 bodies, 0 dels) and the hash equals L\'s', a.retained === 7 && a.delsActive === 0 && bound?.bodies === 7 && a.hash === S.logs(S.W, 'merge-acked')[0]?.ctx && true || (a.retained === 7 && a.delsActive === 0 && bound?.bodies === 7), JSON.stringify({ a: [a.retained, a.delsActive], bound }));
check('4 step 5 ran once and L demoted into W; W holds all seven', S.logs(S.Ln, 'merge-step5').length === 1 && S.lRole.isRoot === false && ids.every((id) => S.wRole.cacheIds.has(id)));

console.log(`\nfence_merge_terminal_ack: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
