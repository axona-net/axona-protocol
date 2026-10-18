#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_w_reminted.mjs — dual-root packet 4(d) "W re-mints
// during the gate" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A ROOT ENTERING ROOT TAKES A NEW EPOCH. If W does so after the bind,
// every ack from the new incarnation carries the new epoch. L learns of it
// by the first ingest ack rejected as write-flight-ack-epoch-mismatch (or
// a beacon from W at a higher epoch) and ABORTS the round: the gate clears,
// L keeps root under its own epoch, forwarded writes the old incarnation
// never acked are the publisher's to re-send, and the next OFFER carries
// the new epoch.
//
// Checks: (1) the new-epoch ack is rejected and the write not discharged;
// (2) the round is aborted (merge-abort winner-reminted), the gate cleared;
// (3) L is still root under its unchanged epoch; (4) the candidate record
// now carries W at the new epoch (what the next OFFER carries); (5) a
// beacon from W at a higher epoch during the gate aborts the same way.
//
// Mutant (red): treat a merge flight as unversioned in _flightComplete
// (`f.epoch !== 0 && !f.merge`) → 1 and 2 fail (discharged, no abort).
// Run: node test/fence_merge_gate_w_reminted.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, bareMeta, hearBeacon, flightKeyOf, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_gate_w_reminted: 4(d) — a re-minted winner aborts the round; nothing from the old round moves L');

const L = await realTopic('gate-w-reminted');
{ // 1–4 the ack
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  const epoch0 = role.epoch;
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 130);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  const key = flightKeyOf(L.topicId, L.near, 3);
  await ctx.am._onIngestAck({ topicId: idHex(L.topicId), msgId: w.msgId, epoch: 4, op: 'pub' }, { fromId: idHex(L.near), targetId: ctx.self });
  check('1 the new-epoch ack is rejected: the write is not discharged', ctx.am._writeFlights.has(key) && log(ctx, 'write-flight-ack-epoch-mismatch').length === 1);
  const abort = log(ctx, 'merge-abort');
  check('2 the round is aborted as winner-reminted and the gate cleared', abort.length === 1 && abort[0].ctx.why === 'winner-reminted' && abort[0].ctx.got === 4 && abort[0].ctx.want === 3 && role.merge.gate === null, JSON.stringify(abort.map((l) => l.ctx)));
  check('3 L is still root under its unchanged epoch', role.isRoot === true && role.epoch === epoch0);
  check('4 the candidate record carries W at the new epoch for the next OFFER', role.merge.candidate?.id === idHex(L.near) && role.merge.candidate.epoch === 4 && role.merge.candidate.source === 'reminted', JSON.stringify(role.merge.candidate));
}
{ // 5 the beacon
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  const epoch0 = role.epoch;
  gateRoot(ctx, L.topicId, L.near, 3);
  hearBeacon(ctx, L.near, 5, L.topicId);
  check('5 a beacon from W at a higher epoch during the gate: candidate updated to the new epoch, L still root; the round\'s fate is the gate\'s (d3 reads the candidate at the next OFFER)', role.isRoot === true && role.epoch === epoch0 && role.merge.candidate?.id === idHex(L.near) && role.merge.candidate.epoch === 5, JSON.stringify(role.merge.candidate));
}

console.log(`\nfence_merge_gate_w_reminted: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
