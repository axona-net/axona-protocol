#!/usr/bin/env node
// =====================================================================
// fence_merge_tombstone_only_handoff_gated.mjs — dual-root packet 4(d)
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A TOMBSTONE-ONLY HANDOFF AT A GATED L GETS NO ACK. `held = sent = 0`
// reads as success at the leaver (it marks the topic acked and departs),
// so a refusal here must be SILENT, whether the gate is read at the top of
// the branch or the refusal happens at the del's mutation.
//
// Checks: (1) gated at the top: no tombstone, no ack; (2) the gate set
// while the transfer's proof verify is in flight (so the top check passed
// and the del is refused at _applyKill): still no ack, no tombstone; (3)
// ungated, the same transfer applies the tombstone and acks held=sent=0
// honestly (the leaver's existing contract for a tombstone-only transfer).
//
// Mutant (red): ignore _applyDels's refusal in the HANDOFF branch (delsOk
// always true) → 2 fails: an ack with held = sent = 0 leaves L.
// Run: node test/fence_merge_tombstone_only_handoff_gated.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedKill, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const hacks = (ctx) => ctx.routed.filter((r) => r.type === 'pubsub:handoffack');

console.log('fence_merge_tombstone_only_handoff_gated: 4(d) — a tombstone-only transfer refused at a gated L is refused silently');

const L = await realTopic('tomb-only-handoff-gated');
const meta = (ctx) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(L.far) });
async function tombOnly(ctx, ids) {
  const k = await signedKill(ctx, L.topicId, ids[0]);
  return { topicId: idHex(L.topicId), from: idHex(L.far), msgs: [], dels: [{ del: true, msgId: ids[0], killTs: ctx.clock.t, signer: k.signerPubkey, seq: 70, publishTs: ctx.clock.t, kill: k }] };
}
{ // 1
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  await ctx.am._onHandoff(await tombOnly(ctx, ids), meta(ctx));
  check('1 gated at the top: no tombstone, no ack', !role.tombstones.has(ids[0]) && hacks(ctx).length === 0 && log(ctx, 'handoff-refused-gated').length === 1);
}
{ // 2 gate set during the proof verify
  const ctx = makeManagerAt(L.self, { extra: { tombstoneAuth: true } });
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  let release; const g = new Promise((r) => { release = r; });
  ctx.am._taVerifyBoundKill = async () => { await g; return false; };
  const p = ctx.am._onHandoff(await tombOnly(ctx, ids), meta(ctx));     // past the top check, parked in the proof verify
  gateRoot(ctx, L.topicId, L.near, 3);
  release(); await p;
  check('2 gate set during the proof verify: the del is refused at _applyKill, no tombstone, NO ack (never held = sent = 0)', !role.tombstones.has(ids[0]) && hacks(ctx).length === 0 && log(ctx, 'handoff-refused-cut').length === 1 && log(ctx, 'cut-refused').some((l) => l.ctx.at === 'applyKill'), JSON.stringify({ acks: hacks(ctx).map((a) => a.payload), logs: ctx.logs.map((l) => l.ev).slice(-5) }));
}
{ // 3 ungated
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  await ctx.am._onHandoff(await tombOnly(ctx, ids), meta(ctx));
  check('3 ungated, the tombstone applies and the honest ack is held = sent = 0', role.tombstones.has(ids[0]) && hacks(ctx).length === 1 && hacks(ctx)[0].payload.held === 0 && hacks(ctx)[0].payload.sent === 0);
}

console.log(`\nfence_merge_tombstone_only_handoff_gated: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
