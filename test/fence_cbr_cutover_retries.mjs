#!/usr/bin/env node
// =====================================================================
// fence_cbr_cutover_retries.mjs — Closer-Born Relay Inherits the Root,
// Part 3e step 6 under `cutover`: Orion's CUT-1 and CUT-2 (council
// c0a842d2) against d105ceb.
//
// CUT-2: a TRANSIENT refusal (refused-capacity, refused-timing) closed the
// corr and left the candidate standing; a standing candidate refuses its own
// re-nomination, so no later verify offered again and two roots stood.
//   C1 (on): W's first answer is refused-capacity; the next verify offers
//      again and the handover completes (W root with L's three, L demoted).
//   C2 (off, control): the next verify offers nothing; both stay root.
// CUT-1: _mergeWinnerReminted recorded the re-minted winner, then aborted,
// and under cutover the abort cleared that record: the new epoch was lost
// and nothing was offered.
//   C3 (on): after W re-mints mid-round, L offers again at once under a new
//      corr carrying W's new epoch.
//   C4 (off, control): the legacy path keeps the record and waits.
// Mutants (2026-11-03): drop the transient clause (C1 red); restore the
// record-then-abort order under cutover (C3 red).
// Run: node test/fence_cbr_cutover_retries.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
console.log('fence_cbr_cutover_retries: 3e step 6 — a transient refusal or a re-minted winner is offered again');

async function refusal(cutover) {
  const opts = cutover ? { cutover: true } : {};
  const S = await splitScene(`cbr-retry-refusal-${cutover}`, { wOpts: opts, lOpts: opts });
  const ids = [await S.stamp(S.Ln, 1), await S.stamp(S.Ln, 2), await S.stamp(S.Ln, 3)];
  let refused = 0;
  S.fab.tap = (j) => {
    if (j.type === 'pubsub:mergeoffer' && refused === 0) {
      refused++;
      const p = j.payload;
      return { dest: S.lId, type: 'pubsub:replayack', meta: { targetId: S.lId, isTerminal: true, hopCount: 1, fromId: idHex(S.wId) },
        payload: { topicId: p.topicId, corr: p.corr, winner: idHex(S.wId), winnerEpoch: S.wRole.epoch | 0, loser: p.loser, loserEpoch: p.loserEpoch, disposition: 'refused-capacity' } };
    }
    return j;
  };
  await S.trigger();
  const afterRefusal = { l: S.lRole.isRoot, w: S.wRole.isRoot, refusedRow: S.logs(S.Ln, 'merge-refused').length };
  await S.trigger();
  return { S, ids, refused, afterRefusal, offers: S.logs(S.Ln, 'merge-offer').length };
}
const on = await refusal(true);
check('C1 on: a refused-capacity answer, then the next verify offers again and the handover completes',
  on.refused === 1 && on.afterRefusal.refusedRow === 1 && on.offers === 2 && on.S.wRole.isRoot && !on.S.lRole.isRoot && on.ids.every((id) => on.S.wRole.cacheIds.has(id)),
  JSON.stringify({ refused: on.refused, after: on.afterRefusal, offers: on.offers, w: on.S.wRole.isRoot, l: on.S.lRole.isRoot }));
const off = await refusal(false);
check('C2 off (control): the next verify offers nothing and both stay root',
  off.refused === 1 && off.offers === 1 && off.S.wRole.isRoot && off.S.lRole.isRoot,
  JSON.stringify({ offers: off.offers, w: off.S.wRole.isRoot, l: off.S.lRole.isRoot }));

async function reminted(cutover) {
  const opts = cutover ? { cutover: true } : {};
  const S = await splitScene(`cbr-retry-remint-${cutover}`, { wOpts: opts, lOpts: opts });
  await S.stamp(S.Ln, 1); await S.stamp(S.Ln, 2);
  S.fab.tap = (j) => (j.type === 'pubsub:replayack' ? null : j);   // hold the round open at L
  await S.trigger();
  const gate = S.lRole.merge?.gate;
  const corr0 = gate?.corr ?? null;
  const got = (S.wRole.epoch | 0) + 1;
  S.Ln.am._mergeWinnerReminted({ topicBig: S.Lt.topicId, rootHex: idHex(S.wId).toLowerCase(), merge: { corr: corr0 }, epoch: S.wRole.epoch | 0 }, got);
  const offers = S.logs(S.Ln, 'merge-offer');
  const last = offers[offers.length - 1]?.ctx;
  return { S, corr0, got, offers: offers.length, last, cand: S.lRole.merge?.candidate ?? null };
}
const r1 = await reminted(true);
check('C3 on: after W re-mints mid-round L offers again at once, new corr, W\'s new epoch',
  r1.corr0 && r1.offers === 2 && r1.last?.winnerEpoch === r1.got && r1.last?.corr !== r1.corr0.slice(0, 8),
  JSON.stringify({ corr0: r1.corr0?.slice(0, 8), offers: r1.offers, last: r1.last, got: r1.got }));
const r0 = await reminted(false);
check('C4 off (control): the legacy path keeps the re-minted record and offers nothing yet',
  r0.corr0 && r0.offers === 1 && r0.cand?.epoch === r0.got,
  JSON.stringify({ offers: r0.offers, cand: r0.cand }));
console.log(`\nfence_cbr_cutover_retries: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
