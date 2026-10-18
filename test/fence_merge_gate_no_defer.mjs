#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_no_defer.mjs — dual-root packet 4(d) "Target: two
// differences make it a sibling and not a call" (axona-docs 14a6bd8; David
// "build it" 2026-10-17).
//
// THE GATED FORWARD DEMOTES ON NO VERDICT AND OPENS ITS FLIGHT UNDER THE
// BOUND winnerEpoch. _forwardToRoot's handling of a `consumed` verdict
// attributed to the target demotes the sender with `defer-confirmed`; a
// verdict is hop-local evidence and the only thing that moves L's claim is
// step 5. And _forwardToRoot's flight takes its epoch from the beacon
// record (or 0); the gated forward's flight is keyed (topic, W,
// winnerEpoch) whatever the beacon says.
//
// Checks: (1) a consumed verdict attributed to W on a gated forward: L is
// still root, no custody-refused, no SUB, the candidate record untouched
// (no 'defer-confirmed' entry); (2) the flight is keyed by the bound
// winnerEpoch, not the beacon's epoch, and carries the round's corr; (3)
// the gate is still set.
//
// Mutant (red): route the gated forward through the unmodified
// _forwardToRoot → 1 (a merge-candidate 'defer-confirmed' line) and 2 (the
// flight keyed by the beacon's epoch) fail.
// Run: node test/fence_merge_gate_no_defer.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, bareMeta, pubsTo, flightKeyOf, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_gate_no_defer: 4(d) — a verdict on a gated forward moves nothing');

const L = await realTopic('gate-no-defer');
{
  const ctx = makeManagerAt(L.self, { verdicts: true, route: (target) => ({ consumed: true, atNode: idHex(target) }) });
  const { role } = await rootWithBodies(ctx, L, 3);
  ctx.am._rootBeacons.set(L.topicId, { root: idHex(L.near), at: ctx.clock.t, exp: ctx.clock.t + 60_000, epoch: 7 });   // a beacon at ANOTHER epoch
  gateRoot(ctx, L.topicId, L.near, 3, 'c0000042');
  const w = await signedPub(ctx, L.desc, 100);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  check('1 a consumed verdict attributed to W: L still root, no custody-refused, no SUB, no defer-confirmed candidate', role.isRoot === true && pubsTo(ctx, idHex(L.near)).length === 1 && log(ctx, 'custody-refused').length === 0 && ctx.routed.filter((r) => r.type === 'pubsub:sub').length === 0 && !log(ctx, 'merge-candidate').some((l) => l.ctx.source === 'defer-confirmed') && !role.merge.candidate, JSON.stringify(ctx.logs.map((l) => l.ev).slice(-5)));
  const f = ctx.am._writeFlights.get(flightKeyOf(L.topicId, L.near, 3));
  const fBeacon = ctx.am._writeFlights.get(flightKeyOf(L.topicId, L.near, 7));
  check('2 the flight is keyed by the bound winnerEpoch (3), not the beacon\'s (7), and carries the round\'s corr', !!f && f.epoch === 3 && f.merge?.corr === 'c0000042' && !fBeacon, JSON.stringify([...ctx.am._writeFlights.keys()].map((k) => k.slice(-6))));
  check('3 the gate is still set', !!role.merge.gate && role.merge.gate.corr === 'c0000042');
}

console.log(`\nfence_merge_gate_no_defer: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
