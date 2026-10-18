#!/usr/bin/env node
// =====================================================================
// fence_merge_migrated_delete_across_cut.mjs — dual-root packet 4(d)
// "tombstones are mutations too, and they commit first" (axona-docs
// 14a6bd8; David "build it" 2026-10-17).
//
// _applyDels awaits proof verification and then calls _applyKill for each
// entry. A transfer admitted before the cut whose proof verify is in flight
// when the cut happens must have its delete refused AT _applyKill, with the
// transfer's entry-time capture, and the tombstone must be absent from L's
// export (_activeDels). A second del behind the refused one is not applied
// either: the whole transfer is refused.
//
// Checks: (1) the delete is refused at applyKill, the tombstone absent;
// (2) the export (_activeDels) does not carry it; (3) the transfer's bodies
// behind it were not applied; (4) a second del behind the first is not
// applied; (5) the same transfer with no cut applies everything.
//
// Mutant (red): make _applyKill ignore `cut` (check the gate only... no,
// the gate is set here too — make _applyKill skip the _cutHolds check
// entirely) → 1, 2 fail.
// Run: node test/fence_merge_migrated_delete_across_cut.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, signedKill, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_migrated_delete_across_cut: 4(d) — a migrated delete paused across the cut is refused at _applyKill');

const L = await realTopic('migrated-delete-across-cut');
async function run({ cut }) {
  const ctx = makeManagerAt(L.self, { extra: { tombstoneAuth: true } });
  const { role, ids } = await rootWithBodies(ctx, L, 3);
  let release; const g = new Promise((r) => { release = r; });
  ctx.am._taVerifyBoundKill = async () => { await g; return false; };
  const k0 = await signedKill(ctx, L.topicId, ids[0]);
  const k1 = await signedKill(ctx, L.topicId, ids[1], 2);
  const body = await signedPub(ctx, L.desc, 80);
  const payload = { topicId: idHex(L.topicId), msgs: [{ msgId: body.msgId, json: body.json, publishTs: ctx.clock.t, seq: 81 }],
    dels: [{ del: true, msgId: ids[0], killTs: ctx.clock.t, signer: k0.signerPubkey, seq: 82, publishTs: ctx.clock.t, kill: k0 },
           { del: true, msgId: ids[1], killTs: ctx.clock.t, signer: k1.signerPubkey, seq: 83, publishTs: ctx.clock.t, kill: k1 }] };
  const p = ctx.am._syncIngest(payload, { targetId: L.self }, 'REPLAY_UP');    // parked inside _applyDels's proof await
  if (cut) gateRoot(ctx, L.topicId, L.near, 3);
  release(); await p;
  return { ctx, role, ids, body };
}
{
  const { ctx, role, ids, body } = await run({ cut: true });
  const refused = log(ctx, 'cut-refused').filter((l) => l.ctx.at === 'applyKill');
  check('1 the delete is refused at applyKill with the entry-time capture; no tombstone', refused.length === 1 && refused[0].ctx.gen0 === 0 && refused[0].ctx.gen === 1 && !role.tombstones.has(ids[0]), JSON.stringify(refused.map((l) => l.ctx)));
  check('2 the export (_activeDels) does not carry it', !ctx.am._activeDels(role).some((d) => d.msgId === ids[0]));
  check('3 the transfer\'s body behind it was not applied', !role.cacheIds.has(body.msgId));
  check('4 the second del behind the refused one was not applied (one refusal, whole transfer refused)', !role.tombstones.has(ids[1]) && refused.length === 1 && role.cacheIds.has(ids[1]));
}
{
  const { role, ids, body } = await run({ cut: false });
  check('5 with no cut the same transfer applies both tombstones and the body', role.tombstones.has(ids[0]) && role.tombstones.has(ids[1]) && role.cacheIds.has(body.msgId));
}

console.log(`\nfence_merge_migrated_delete_across_cut: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
