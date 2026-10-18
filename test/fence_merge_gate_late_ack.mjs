#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_late_ack.mjs — dual-root packet 4(d) "a late ack from
// W after the gate has cleared closes its flight entry and changes nothing
// about L's claim" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// Also the MERGE_TIMEOUT_MS bound (packet: "MERGE_TIMEOUT_MS bounds ONE
// gate interval, measured from the bind"): the refreshTick sweep aborts a
// gate past it, L keeps root under the same epoch, and the publisher's
// re-send is stamped at L.
//
// Checks: (1) the sweep one tick before the bound leaves the gate; (2) at
// the bound the round aborts (merge-abort timeout), the gate clears, L is
// root at its epoch; (3) the publisher's re-send is stamped at L; (4) W's
// ack for the gated forward, arriving after the gate cleared, closes the
// flight entry, logs merge-late-ack, and L's claim does not move; (5) a
// merge flight that passes INGEST_ACK_MS with no ack ends its round
// (merge-abort winner-no-ack) through the flight sweep.
//
// Mutant (red): make _mergeSweep never abort → 2 and 3 fail; make
// _flightSweep skip _mergeFlightSilent → 5 fails.
// Run: node test/fence_merge_gate_late_ack.mjs
// =====================================================================
import { MERGE_TIMEOUT_MS } from '../src/pubsub/constants.js';
import { INGEST_ACK_MS } from '../src/pubsub/writeFlight.js';
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, bareMeta, pubsTo, acksFrom, flightKeyOf, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_gate_late_ack: 4(d) — the gate\'s bound, and an ack after it moves nothing');

const L = await realTopic('gate-late-ack');
{ // 1–4
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  const epoch0 = role.epoch;
  const t0 = ctx.clock.t;
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 140);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  const key = flightKeyOf(L.topicId, L.near, 3);
  ctx.clock.t = t0 + MERGE_TIMEOUT_MS - 1; ctx.am._mergeSweep(ctx.clock.t);
  check('1 one tick before MERGE_TIMEOUT_MS the gate stands', !!role.merge.gate);
  ctx.clock.t = t0 + MERGE_TIMEOUT_MS; ctx.am._mergeSweep(ctx.clock.t);
  const abort = log(ctx, 'merge-abort');
  check('2 at the bound the round aborts (timeout), the gate clears, L is root at its epoch', abort.length === 1 && abort[0].ctx.why === 'timeout' && role.merge.gate === null && role.isRoot === true && role.epoch === epoch0, JSON.stringify(abort.map((l) => l.ctx)));
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));   // the publisher's re-send
  check('3 the publisher\'s re-send is stamped at L and acked', role.cacheIds.has(w.msgId) && acksFrom(ctx).length === 1 && pubsTo(ctx, idHex(L.near)).length === 1);
  await ctx.am._onIngestAck({ topicId: idHex(L.topicId), msgId: w.msgId, epoch: 3, op: 'pub' }, { fromId: idHex(L.near), targetId: ctx.self });   // W's ack, late
  check('4 W\'s late ack closes the flight entry, logs merge-late-ack, and L\'s claim does not move', !ctx.am._writeFlights.has(key) && log(ctx, 'merge-late-ack').length === 1 && role.isRoot === true && log(ctx, 'root-transition').filter((l) => !l.ctx.isRoot).length === 0);
}
{ // 5 no ack within INGEST_ACK_MS
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role } = await rootWithBodies(ctx, L, 2);
  const t0 = ctx.clock.t;
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 141);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  ctx.clock.t = t0 + INGEST_ACK_MS + 1; ctx.am._flightSweep();
  const abort = log(ctx, 'merge-abort');
  check('5 a merge flight past INGEST_ACK_MS with no ack from W ends the round (winner-no-ack); L root', abort.length === 1 && abort[0].ctx.why === 'winner-no-ack' && role.merge.gate === null && role.isRoot === true, JSON.stringify(abort.map((l) => l.ctx)));
}

console.log(`\nfence_merge_gate_late_ack: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
