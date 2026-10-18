#!/usr/bin/env node
// =====================================================================
// fence_merge_record_expiry.mjs — dual-root packet 4(d) "Expiry without
// evidence" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// AFTER STEPDOWN_HOLD_MS WITH NO BEACON FROM W, THE RECORD AND THE HOLD
// EXPIRE TOGETHER and L may promote on the next terminal request, minting a
// fresh epoch. If W was alive and silent the two claims meet again and the
// machine runs with L's new epoch against W's — a round, not a direct
// demote. A tombstone for W's claim (dead-root eviction) clears the record
// and the hold at once.
//
// Checks: (1) after the merge L holds the record and the hold; (2) at
// STEPDOWN_HOLD_MS with no beacon both expire (merge-record-expired,
// root-hold-expired); (3) L promotes on the next bare request and mints a
// fresh epoch above W's; (4) the next contact (W's beacon) runs a ROUND at
// the loser — W now loses on epoch, so W records L as its candidate and
// offers (merge-offer at W), rather than demoting directly; (5) a flight
// eviction tombstoning W's claim clears the record and the hold at once
// (merge-record-cleared).
//
// Mutant (red): demote directly on the next contact (bypass
// _mergeCandidate at the beacon site) → 4 fails; keep the record past the
// hold → 3 fails.
// Run: node test/fence_merge_record_expiry.mjs
// =====================================================================
import { splitScene, makeManagerAt, realTopic, rootWithBodies, idHex } from './lib/mergeFixture.mjs';
import { INGEST_ACK_MS, FLIGHT_PROBE_MS } from '../src/pubsub/writeFlight.js';
import { makeMergeState } from '../src/pubsub/rootClaim.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_record_expiry: 4(d) — the record expires with the hold; the next contact is a round');

{
  const S = await splitScene('record-expiry');
  await S.stamp(S.W, 1); await S.stamp(S.Ln, 2);
  await S.trigger();
  const hold = () => S.Ln.am._stepDownHold.get(S.Lt.topicId);
  check('1 after the merge L holds the accepted-winner record and the hold toward W', S.lRole.isRoot === false && S.lRole.merge.acceptedWinner?.id === idHex(S.wId) && !!hold());
  const wEpoch = S.wRole.epoch;
  S.fab.clock += S.Ln.am._stepDownHoldMs + 1;
  const h = S.Ln.rc.holdFor(S.Lt.topicId);
  check('2 at STEPDOWN_HOLD_MS with no beacon from W the record and the hold expire together', h === null && S.lRole.merge.acceptedWinner === null && S.logs(S.Ln, 'merge-record-expired').length === 1 && S.logs(S.Ln, 'root-hold-expired').length === 1);
  await S.stamp(S.Ln, 3);                                        // a bare request at L
  check('3 L promotes on the next bare request and mints a fresh epoch above W\'s', S.lRole.isRoot === true && S.lRole.epoch > wEpoch, JSON.stringify({ isRoot: S.lRole.isRoot, l: S.lRole.epoch, w: wEpoch }));
  // the next contact: W hears L's beacon (L's epoch is higher → W loses → W offers)
  S.W.handlers.get('pubsub:rootbeacon')({ root: idHex(S.lId), topics: [idHex(S.Lt.topicId)], epochs: [S.lRole.epoch], beaconId: 'b-l-1', layer: 0 }, { targetId: S.wId, isTerminal: false, hopCount: 1 });
  await S.fab.settle();
  check('4 the next contact runs a ROUND: W records L as its candidate and offers; no direct demote at W', S.logs(S.W, 'merge-candidate').length >= 1 && S.logs(S.W, 'merge-offer').length >= 1 && S.logs(S.W, 'root-transition').filter((l) => !l.ctx.isRoot && l.ctx.why !== 'merge-step5').length === 0, JSON.stringify(S.W.logs.map((l) => l.ev).slice(-6)));
}
{ // 5 — a tombstone for W's claim clears the record and the hold at once
  const L = await realTopic('record-expiry-evict');
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role } = await rootWithBodies(ctx, L, 1);
  role.merge ??= makeMergeState();
  role.merge.acceptedWinner = { id: idHex(L.near), epoch: 3, at: ctx.clock.t, exp: ctx.clock.t + ctx.am._stepDownHoldMs };
  ctx.am._stepDownHold.set(L.topicId, { to: idHex(L.near), at: ctx.clock.t, lastLog: 0 });
  // a write flight toward W at its epoch that never gets a receipt: evicted
  ctx.am._rootBeacons.set(L.topicId, { root: idHex(L.near), at: ctx.clock.t, exp: ctx.clock.t + 60_000, epoch: 3 });
  ctx.am._flightOpen(L.topicId, idHex(L.near), 'pubsub:pub', { topicId: idHex(L.topicId), json: '{"msgId":"zz"}' }, null, 3);
  ctx.clock.t += INGEST_ACK_MS + 1; ctx.am._flightSweep();
  ctx.clock.t += FLIGHT_PROBE_MS + 1; ctx.am._flightSweep();
  ctx.clock.t += INGEST_ACK_MS + 1; ctx.am._flightSweep();
  ctx.clock.t += FLIGHT_PROBE_MS + 1; ctx.am._flightSweep();
  const evicted = ctx.logs.some((l) => l.ev === 'pubsub:root-evicted');
  check('5 a flight eviction tombstoning W\'s claim clears the record and the hold at once', evicted && role.merge.acceptedWinner === null && !ctx.am._stepDownHold.has(L.topicId) && ctx.logs.some((l) => l.ev === 'pubsub:merge-record-cleared'), JSON.stringify({ evicted, aw: role.merge.acceptedWinner, hold: ctx.am._stepDownHold.has(L.topicId) }));
}

console.log(`\nfence_merge_record_expiry: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
