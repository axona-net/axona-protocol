#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_epoch_authority.mjs — dual-root packet 4(d) "the epoch
// authority" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A MERGE FLIGHT MUST NOT DEPEND ON WHETHER A BEACON FOR W HAPPENS TO BE
// PRESENT OR FRESH. _flightOpen reads _rootBeacons and stores 0 when no
// record exists, and _flightComplete accepts an ack at ANY epoch from the
// right node when the flight's epoch is 0. The gated forward opens its
// flight with the explicit bound winnerEpoch: the key is (topic, W,
// winnerEpoch), the epoch field is never 0, and the unversioned exemption
// does not apply.
//
// Checks, L holding NO beacon for W: (1) the flight opened at winnerEpoch
// (not 0); (2) W's ack at winnerEpoch discharges the write (flight closed);
// (3) L's claim did not move. L holding a STALE beacon at another epoch:
// (4) an ack at the beacon's epoch does NOT discharge (epoch mismatch);
// (5) an ack at winnerEpoch does; (6) L's claim did not move either way.
//
// Mutant (red): make _flightOpen ignore expectedEpoch → 1 fails (epoch 0)
// and 4 fails (any epoch accepted).
// Run: node test/fence_merge_gate_epoch_authority.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, bareMeta, pubsTo, flightKeyOf, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const ack = (ctx, msgId, epoch) => ctx.am._onIngestAck({ topicId: idHex(L.topicId), msgId, epoch, op: 'pub' }, { fromId: idHex(L.near), targetId: ctx.self });

console.log('fence_merge_gate_epoch_authority: 4(d) — the merge flight binds the bound winnerEpoch, never the beacon');

const L = await realTopic('gate-epoch-authority');
{ // no beacon
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  ctx.am._rootBeacons.delete(L.topicId);
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 120);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  const f = ctx.am._writeFlights.get(flightKeyOf(L.topicId, L.near, 3));
  check('1 with no beacon for W the flight is opened at winnerEpoch 3, not 0', !!f && f.epoch === 3 && !ctx.am._writeFlights.get(flightKeyOf(L.topicId, L.near, 0)), JSON.stringify([...ctx.am._writeFlights.keys()].map((k) => k.slice(-4))));
  await ack(ctx, w.msgId, 3);
  check('2 W\'s ack at winnerEpoch discharges the write: the flight is closed', !ctx.am._writeFlights.has(flightKeyOf(L.topicId, L.near, 3)));
  check('3 L\'s claim did not move', role.isRoot === true && !!role.merge.gate && log(ctx, 'root-transition').filter((l) => !l.ctx.isRoot).length === 0);
}
{ // stale beacon at another epoch
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  ctx.am._rootBeacons.set(L.topicId, { root: idHex(L.near), at: ctx.clock.t - 50_000, exp: ctx.clock.t + 10_000, epoch: 7 });
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 121);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  const key = flightKeyOf(L.topicId, L.near, 3);
  // an ack at the BEACON's epoch is the re-mint signal (packet: W re-minted → abort); here we
  // assert the discharge rule only, on a fresh flight: open, wrong-epoch ack, still open.
  await ack(ctx, w.msgId, 7);
  const stillOpen = ctx.am._writeFlights.has(key) && ctx.am._writeFlights.get(key).entries.size === 1;
  check('4 an ack at the stale beacon\'s epoch (7) does not discharge the write (write-flight-ack-epoch-mismatch)', stillOpen && log(ctx, 'write-flight-ack-epoch-mismatch').length === 1, JSON.stringify(log(ctx, 'write-flight-ack-epoch-mismatch').map((l) => l.ctx)));
  await ack(ctx, w.msgId, 3);
  check('5 an ack at winnerEpoch (3) discharges it', !ctx.am._writeFlights.has(key));
  check('6 L\'s claim did not move either way (the mismatch aborted the round; the claim is untouched)', role.isRoot === true && log(ctx, 'root-transition').filter((l) => !l.ctx.isRoot).length === 0 && pubsTo(ctx, idHex(L.near)).length === 1);
}

console.log(`\nfence_merge_gate_epoch_authority: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
