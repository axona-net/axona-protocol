#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_w_unreachable.mjs — dual-root packet 4(d) "Unreachable
// or refusing W" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A GATED FORWARD WHOSE VERDICT IS `failed` ENDS THE ROUND: the gate
// clears, L keeps root under its epoch, and the publisher's re-send lands
// and is stamped at L. A winner that cannot take a write cannot take a
// frame either.
//
// Checks: (1) the failed verdict aborts the round (merge-abort
// winner-unreachable), the gate is cleared, L is root at its unchanged
// epoch; (2) the publisher's re-send of the same write is stamped at L and
// acked; (3) a `consumed` verdict does not abort (contrast).
//
// Mutant (red): ignore the failed verdict in _forwardToWinner → 1 and 2
// fail (the gate stays, the re-send is forwarded into the void).
// Run: node test/fence_merge_gate_w_unreachable.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, bareMeta, pubsTo, acksFrom, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_gate_w_unreachable: 4(d) — a failed gated forward ends the round; L keeps root');

const L = await realTopic('gate-w-unreachable');
{
  const ctx = makeManagerAt(L.self, { verdicts: true, route: () => ({ consumed: false, exhausted: true }) });
  const { role } = await rootWithBodies(ctx, L, 3);
  const epoch0 = role.epoch;
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 110);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  const abort = log(ctx, 'merge-abort');
  check('1 the failed verdict aborts the round: gate cleared, L root at its unchanged epoch', abort.length === 1 && abort[0].ctx.why === 'winner-unreachable' && role.merge.gate === null && role.isRoot === true && role.epoch === epoch0 && pubsTo(ctx, idHex(L.near)).length === 1, JSON.stringify(abort.map((l) => l.ctx)));
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));   // the publisher's re-send
  check('2 the publisher\'s re-send is stamped at L and acked', role.cacheIds.has(w.msgId) && acksFrom(ctx).length === 1 && pubsTo(ctx, idHex(L.near)).length === 1);
}
{
  const ctx = makeManagerAt(L.self, { verdicts: true, route: (target) => ({ consumed: true, atNode: idHex(target) }) });
  const { role } = await rootWithBodies(ctx, L, 1);
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 111);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  check('3 a consumed verdict does not abort: the gate stays', !!role.merge.gate && log(ctx, 'merge-abort').length === 0);
}

console.log(`\nfence_merge_gate_w_unreachable: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
