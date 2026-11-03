#!/usr/bin/env node
// =====================================================================
// fence_cbr_silence_never_wins.mjs — Closer-Born Relay Inherits the Root
// v0.4, Part 3e step 6 (David "go ahead with the cutover state machine",
// 2026-11-03): timeouts never transfer authority; silence is never a winner.
//
// Today an OFFER unanswered past MERGE_OFFER_TIMEOUT_MS runs the LEGACY
// FALLBACK: L demotes toward a winner that never answered (mergeMachine
// _mergeSweep). Under cutover the round closes, the candidate clears, L
// keeps its root and offers again on the next verify.
//
// Scene: splitScene; L stamps three; every MERGE_OFFER is dropped (a winner
// that cannot hear, or a lost frame); the clock passes the offer timeout and
// both sweeps run; then the drop lifts and L re-verifies.
//   S1 (on): after the timeout L is still root, no merge-legacy-fallback row,
//      a merge-offer-timeout row with cutover true.
//   S2 (on): the re-verify offers again and the handover completes: W root
//      with L's three, L demoted.
//   S3 (off, control): the timeout runs the legacy fallback (L demotes on
//      silence) — today's behaviour.
// Run: node test/fence_cbr_silence_never_wins.mjs
// =====================================================================
import { splitScene } from './lib/mergeFixture.mjs';
import { MERGE_OFFER_TIMEOUT_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
console.log('fence_cbr_silence_never_wins: 3e — an unanswered offer never demotes the loser');

async function run(cutover) {
  const opts = cutover ? { cutover: true } : {};
  const S = await splitScene(`cbr-silence-${cutover}`, { wOpts: opts, lOpts: opts });
  const ids = [await S.stamp(S.Ln, 1), await S.stamp(S.Ln, 2), await S.stamp(S.Ln, 3)];
  S.fab.tap = (j) => j.type === 'pubsub:mergeoffer' ? null : j;
  await S.trigger();
  S.fab.clock += MERGE_OFFER_TIMEOUT_MS + 1;
  S.Ln.am._mergeSweep(S.fab.clock); S.W.am._mergeSweep(S.fab.clock);
  await S.fab.settle();
  const afterTimeout = { lRoot: S.lRole.isRoot, ev: S.Ln.logs.map((l) => l.ev.replace('pubsub:', '')), row: S.Ln.logs.find((l) => l.ev === 'pubsub:merge-offer-timeout')?.ctx };
  S.fab.tap = null;
  await S.trigger();
  return { S, ids, afterTimeout };
}
const on = await run(true);
check('S1 on: after the timeout L is still root and no legacy fallback ran', on.afterTimeout.lRoot === true && !on.afterTimeout.ev.includes('merge-legacy-fallback'), on.afterTimeout.ev.filter((e) => /merge/.test(e)).join(','));
check('S1 … the timeout is logged with cutover true', on.afterTimeout.row?.cutover === true, JSON.stringify(on.afterTimeout.row));
check('S2 on: the re-verify offers again and the handover completes (W root with L\'s three, L demoted)', on.S.wRole.isRoot && !on.S.lRole.isRoot && on.ids.every((id) => on.S.wRole.cacheIds.has(id)), `W ${on.S.wRole.isRoot}/${on.S.wRole.cache.length} L ${on.S.lRole.isRoot}`);
const off = await run(false);
check('S3 off (control): the timeout runs the legacy fallback — L demotes on silence', off.afterTimeout.ev.includes('merge-legacy-fallback') && off.afterTimeout.lRoot === false, off.afterTimeout.ev.filter((e) => /merge/.test(e)).join(','));
console.log(`\nfence_cbr_silence_never_wins: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
