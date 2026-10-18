#!/usr/bin/env node
// =====================================================================
// fence_merge_timing_bound.mjs — dual-root packet 4(d) "Shared parameters,
// bound to the round" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// THE INEQUALITY IS ABOUT NUMBERS, NOT NAMES. The OFFER carries timing
// {version, A, E}; W refuses with refused-timing before any data if the
// version is not its own or A or E is outside its limits; W echoes timing
// on the PULLUP and L binds only if the echo equals what it offered, else
// it refuses before the cut. A mismatch never reaches the gate.
//
// Checks: (1) an OFFER with another version: W answers refused-timing, no
// pull, no cut at L, L keeps custody; (2) an OFFER with A below A_MIN:
// refused-timing; (3) A above A_MAX: refused-timing; (4) E above E_MAX:
// refused-timing; (5) a PULLUP echoing a different A than offered: L
// refuses before the cut (merge-refused-timing, no merge-cut), the record
// dropped; (6) the correct echo binds (merge-cut).
//
// Mutant (red): compare timing by presence instead of value (sameTiming
// returns !!a && !!b) → 5 fails; drop the limits check → 2–4 fail.
// Run: node test/fence_merge_timing_bound.mjs
// =====================================================================
import { MERGE_TIMING_VERSION, MERGE_TIMEOUT_MS, FUTURE_TOLERANCE_MS, MERGE_A_MIN_MS, MERGE_A_MAX_MS, MERGE_E_MAX_MS } from '../src/pubsub/constants.js';
import { realTopic, makeManagerAt, rootWithBodies, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_timing_bound: 4(d) — timing is refused by value, before any data');

const L = await realTopic('timing-bound');
const meta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
/** W side: a root receiving an OFFER from a farther loser with the given timing. */
async function offerAtW(timing) {
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 1);
  ctx.am._onMergeOffer({ topicId: idHex(L.topicId), corr: 'c'.repeat(32), loser: idHex(L.far), loserEpoch: 1, hw: 0, lw: 0, count: 1, timing }, meta(ctx, L.far));
  const acks = ctx.routed.filter((r) => r.type === 'pubsub:replayack');
  const pulls = ctx.routed.filter((r) => r.type === 'pubsub:pullup');
  return { ctx, role, acks, pulls };
}
const good = { version: MERGE_TIMING_VERSION, A: MERGE_TIMEOUT_MS, E: FUTURE_TOLERANCE_MS };
{
  const { acks, pulls, ctx } = await offerAtW({ ...good, version: MERGE_TIMING_VERSION + 1 });
  check('1 another timing version: W answers refused-timing, no pull', acks.length === 1 && acks[0].payload.disposition === 'refused-timing' && pulls.length === 0 && log(ctx, 'merge-refused-timing').length === 1);
}
{ const { acks, pulls } = await offerAtW({ ...good, A: MERGE_A_MIN_MS - 1 }); check('2 A below A_MIN: refused-timing', acks.length === 1 && acks[0].payload.disposition === 'refused-timing' && pulls.length === 0); }
{ const { acks, pulls } = await offerAtW({ ...good, A: MERGE_A_MAX_MS + 1 }); check('3 A above A_MAX: refused-timing', acks.length === 1 && acks[0].payload.disposition === 'refused-timing' && pulls.length === 0); }
{ const { acks, pulls } = await offerAtW({ ...good, E: MERGE_E_MAX_MS + 1 }); check('4 E above E_MAX: refused-timing', acks.length === 1 && acks[0].payload.disposition === 'refused-timing' && pulls.length === 0); }
{ // L side: the echo
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  const offer = ctx.routed.find((r) => r.type === 'pubsub:mergeoffer');
  const corr = offer.payload.corr;
  ctx.am._onPullUp({ topicId: idHex(L.topicId), corr, parentId: idHex(L.near), winnerEpoch: 3, cursor: null, sinceHw: 0, timing: { ...offer.payload.timing, A: offer.payload.timing.A + 1 } }, meta(ctx, L.near));
  for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  check('5 a PULLUP echoing a different A than offered: refused before the cut, record dropped, L root with custody', log(ctx, 'merge-refused-timing').length === 1 && log(ctx, 'merge-cut').length === 0 && role.sync.openMerges.size === 0 && role.isRoot === true && !role.merge.gate, JSON.stringify(ctx.logs.map((l) => l.ev).slice(-3)));
  // a fresh offer, then the correct echo
  role.merge.candidate = null;
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  const offer2 = ctx.routed.filter((r) => r.type === 'pubsub:mergeoffer')[1];
  if (offer2) ctx.am._onPullUp({ topicId: idHex(L.topicId), corr: offer2.payload.corr, parentId: idHex(L.near), winnerEpoch: 3, cursor: null, sinceHw: 0, timing: { ...offer2.payload.timing } }, meta(ctx, L.near));
  for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  check('6 the correct echo binds: merge-cut', !!offer2 && log(ctx, 'merge-cut').length === 1 && role.sync.openMerges.get(offer2.payload.corr)?.state === 'bound');
}

console.log(`\nfence_merge_timing_bound: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
