#!/usr/bin/env node
// =====================================================================
// fence_merge_direction.mjs — dual-root packet 4(d) "Direction, read from
// source" (axona-docs 14a6bd8; David "build it" 2026-10-17; Aster 436ca7b6).
//
// THE NODE THAT PULLS IS THE NODE THAT INGESTS, SO THE WINNER PULLS. v0.3
// had the loser pull from the winner: the loser ingested the winner's state
// and nothing moved the loser's unique history into the winner. Here the
// winner W issues every PULLUP under corr and the loser L only answers.
//
// Checks: (1) every PULLUP under the round's corr was sent by W to L, none
// by L; (2) every REPLAYUP under corr was sent by L to W; (3) W ended with
// L's unique body and tombstone (the point of the direction); (4) the OFFER
// went from L to W and the REPLAYACK from W to L.
//
// Mutant (red): have W answer the OFFER with a full REPLAYACK without
// pulling → 1 (no pull) and 3 (W lacks L's history) fail.
// Run: node test/fence_merge_direction.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_direction: 4(d) — the winner pulls; the loser answers');

const S = await splitScene('direction');
const A = await S.stamp(S.W, 1);
const B = await S.stamp(S.Ln, 2), Y = await S.stamp(S.Ln, 3); await S.kill(S.Ln, Y);
await S.trigger();
const offer = S.frames('pubsub:mergeoffer')[0];
const corr = offer?.payload?.corr;
const pulls = S.frames('pubsub:pullup', (j) => j.payload.corr === corr);
const replays = S.frames('pubsub:replayup', (j) => j.payload.corr === corr);
const acks = S.frames('pubsub:replayack', (j) => j.payload.corr === corr);
check('1 every PULLUP under corr was sent by W to L, none by L', pulls.length >= 2 && pulls.every((j) => j.meta.fromId === idHex(S.wId) && j.dest === S.lId), JSON.stringify(pulls.map((j) => [j.meta.fromId.slice(-4), idHex(j.dest).slice(-4)])));
check('2 every REPLAYUP under corr was sent by L to W', replays.length >= 2 && replays.every((j) => j.meta.fromId === idHex(S.lId) && j.dest === S.wId));
check('3 W ended with L\'s unique body and tombstone, and its own', S.wRole.cacheIds.has(B) && S.wRole.tombstones.has(Y) && S.wRole.cacheIds.has(A) && S.lRole.isRoot === false, JSON.stringify({ w: [...S.wRole.cacheIds].map((s) => s.slice(0, 6)), lRoot: S.lRole.isRoot }));
check('4 the OFFER went L → W and the terminal REPLAYACK W → L, once each', !!offer && offer.meta.fromId === idHex(S.lId) && offer.dest === S.wId && acks.length === 1 && acks[0].meta.fromId === idHex(S.wId) && acks[0].dest === S.lId);

console.log(`\nfence_merge_direction: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
