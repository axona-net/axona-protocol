#!/usr/bin/env node
// =====================================================================
// fence_merge_gated_ingest_refused.mjs — dual-root packet 4(d) "a whole
// transfer at a gated L is refused before anything is applied" (axona-docs
// 14a6bd8; David "build it" 2026-10-17).
//
// Checks: (1) a HANDOFF at a gated L applies no del and no body and sends
// NO HANDOFFACK (the silence the leaver already understands: it retries
// another heir and cohort-sprays), logged handoff-refused-gated; (2) a
// REPLAYUP at a gated L is rejected entry by entry (every entry rejected,
// nothing applied); (3) a REPLICATE at a gated root likewise; (4) the same
// HANDOFF at the same node with the gate cleared is ingested and acked.
//
// Mutant (red): delete the gate check at the top of the HANDOFF branch →
// 1 fails (ingested, or at least acked short).
// Run: node test/fence_merge_gated_ingest_refused.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, signedKill, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const hacks = (ctx) => ctx.routed.filter((r) => r.type === 'pubsub:handoffack');

console.log('fence_merge_gated_ingest_refused: 4(d) — nothing foreign enters a gated L behind its export');

const L = await realTopic('gated-ingest-refused');
async function transfer(ctx, ids) {
  const b = await signedPub(ctx, L.desc, 40);
  const k = await signedKill(ctx, L.topicId, ids[0]);
  return { body: b, payload: { topicId: idHex(L.topicId), from: idHex(L.far), msgs: [{ msgId: b.msgId, json: b.json, publishTs: ctx.clock.t, seq: 60 }], dels: [{ del: true, msgId: ids[0], killTs: ctx.clock.t, signer: k.signerPubkey, seq: 61, publishTs: ctx.clock.t }] } };
}
{ // 1 HANDOFF
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  const { body, payload } = await transfer(ctx, ids);
  await ctx.am._onHandoff(payload, { targetId: L.self, isTerminal: true, hopCount: 1, fromId: idHex(L.far) });
  check('1 a HANDOFF at a gated L: no del, no body, NO HANDOFFACK, handoff-refused-gated logged', !role.cacheIds.has(body.msgId) && !role.tombstones.has(ids[0]) && hacks(ctx).length === 0 && log(ctx, 'handoff-refused-gated').length === 1 && log(ctx, 'handoff-ingested').length === 0, JSON.stringify({ acks: hacks(ctx).length, logs: ctx.logs.map((l) => l.ev).slice(-4) }));
}
{ // 2 REPLAYUP
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  const { body, payload } = await transfer(ctx, ids);
  await ctx.am._syncIngest(payload, { targetId: L.self }, 'REPLAY_UP');
  check('2 a REPLAYUP at a gated L: every entry refused at its mutation, nothing applied', !role.cacheIds.has(body.msgId) && !role.tombstones.has(ids[0]) && log(ctx, 'cut-refused').length >= 1, JSON.stringify(log(ctx, 'cut-refused').map((l) => l.ctx.at)));
}
{ // 3 REPLICATE at a gated root (UNION_AT_ROOT)
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  const { body, payload } = await transfer(ctx, ids);
  await ctx.am._syncIngest(payload, { targetId: L.self }, 'REPLICATE');
  check('3 a REPLICATE at a gated root: refused, nothing applied', !role.cacheIds.has(body.msgId) && !role.tombstones.has(ids[0]));
}
{ // 4 gate cleared: ingested
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  ctx.am._mergeGateClear(role, 'test');
  const { body, payload } = await transfer(ctx, ids);
  await ctx.am._onHandoff(payload, { targetId: L.self, isTerminal: true, hopCount: 1, fromId: idHex(L.far) });
  check('4 with the gate cleared (gen moved, no gate) a fresh HANDOFF is ingested and acked', role.cacheIds.has(body.msgId) && role.tombstones.has(ids[0]) && hacks(ctx).length === 1 && hacks(ctx)[0].payload.held === 1 && log(ctx, 'handoff-ingested').length === 1);
}

console.log(`\nfence_merge_gated_ingest_refused: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
