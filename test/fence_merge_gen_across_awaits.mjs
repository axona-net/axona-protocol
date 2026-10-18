#!/usr/bin/env node
// =====================================================================
// fence_merge_gen_across_awaits.mjs — dual-root packet 4(d) "the gate is
// not the test" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A cut followed by a successful demotion clears the gate. A check against
// the gate alone, or against a `gen` re-read after the await, would pass a
// write that was admitted before the cut. The entry-time {role0, gen0} pair
// fails it: a cut increments gen and nothing decrements it.
//
// Three admitted operations, each parked in its await: a BODY (REPLAY_UP
// transfer, parked in verifyEnvelope), a DELETE (a transfer's del, parked
// in the proof verify of _applyDels) and a KILL (_onKill, parked in
// verifyKill). Two sequences: (A) a cut with the gate still set when they
// resume; (B) a cut, then a demotion with the release token, then the gate
// cleared — no gate at resume. Every operation must be refused at its
// mutation in both, and the transfer's tally must say refused.
//
// (C) the role is REPLACED in axonRoles while the operations await: all
// three refused, the replacement untouched (Aster c6719863 C3, map
// membership; a role absent from the map is not refused — fixtures and some
// construction paths push before registering).
//
// Mutant (red): make _cutHolds check the gate only (ignore cut) → B's three
// checks and C fail.
// Run: node test/fence_merge_gen_across_awaits.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, signedKill, bareMeta, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_gen_across_awaits: 4(d) — the entry-time generation, not the gate, is the test');

const L = await realTopic('gen-across-awaits');

/** Park three operations, run `cutThen`, resume, report. */
async function scene(cutThen) {
  const ctx = makeManagerAt(L.self, { neighbors: [L.near], extra: { tombstoneAuth: true } });
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  // the DELETE's proof verify is parked until we release it
  let release; const gate = new Promise((r) => { release = r; });
  ctx.am._taVerifyBoundKill = async () => { await gate; return false; };
  const body = await signedPub(ctx, L.desc, 30);
  const killA = await signedKill(ctx, L.topicId, ids[0]);              // the DELETE (carried in a transfer's dels)
  const killB = await signedKill(ctx, L.topicId, ids[1]);              // the KILL (a routed write)
  const pBody = ctx.am._syncIngest({ topicId: idHex(L.topicId), msgs: [{ msgId: body.msgId, json: body.json, publishTs: ctx.clock.t, seq: 50 }], dels: [] }, { targetId: L.self }, 'REPLAY_UP');
  const pDel = ctx.am._syncIngest({ topicId: idHex(L.topicId), msgs: [], dels: [{ del: true, msgId: ids[0], killTs: ctx.clock.t, signer: killA.signerPubkey, seq: 51, publishTs: ctx.clock.t, kill: killA }] }, { targetId: L.self }, 'REPLAY_UP');
  const pKill = ctx.am._onKill({ topicId: idHex(L.topicId), kill: killB }, bareMeta(ctx, L.publisher));
  cutThen(ctx, role);                                                  // synchronous: the parked operations have not resumed
  release();
  await Promise.all([pBody, pDel, pKill]);
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  const refusedAt = log(ctx, 'cut-refused').map((l) => l.ctx.at);
  return { ctx, role, ids, body, refusedAt };
}

{ // A — cut, gate still set
  const { role, ids, body, refusedAt } = await scene((ctx) => { gateRoot(ctx, L.topicId, L.near, 3); });
  check('A body: refused at cachePush, not in the cache', refusedAt.includes('cachePush') && !role.cacheIds.has(body.msgId), JSON.stringify(refusedAt));
  check('A delete: refused at applyKill, no tombstone for it', refusedAt.filter((a) => a === 'applyKill').length >= 1 && !role.tombstones.has(ids[0]));
  check('A kill: refused at applyKill, no tombstone for it, the body it targeted still cached', refusedAt.filter((a) => a === 'applyKill').length === 2 && !role.tombstones.has(ids[1]) && role.cacheIds.has(ids[1]));
}
{ // B — cut, demotion with the token, gate cleared: no gate at resume
  const { ctx, role, ids, body, refusedAt } = await scene((c, r) => {
    gateRoot(c, L.topicId, L.near, 3);
    r.merge.releaseToken = 'tok-step5';
    const ok = c.rc.demote(L.topicId, idHex(L.near), 'merge-step5', { releaseToken: 'tok-step5' });
    c.am._mergeGateClear(r, 'completed');
    if (!ok || r.isRoot || r.merge.gate) throw new Error('B setup: demotion or gate clear failed');
  });
  check('B (no gate at resume) body: refused at cachePush by the moved generation', refusedAt.includes('cachePush') && !role.cacheIds.has(body.msgId) && role.isRoot === false && role.merge.gate === null, JSON.stringify(refusedAt));
  check('B delete: refused at applyKill', refusedAt.filter((a) => a === 'applyKill').length >= 1 && !role.tombstones.has(ids[0]));
  check('B kill: refused at applyKill; the routed KILL went on to the upstream W as a forward', refusedAt.filter((a) => a === 'applyKill').length === 2 && !role.tombstones.has(ids[1]) && ctx.routed.some((r) => r.type === 'pubsub:kill' && r.payload.via?.[0] === idHex(L.near)), JSON.stringify(ctx.routed.map((r) => r.type)));
}

{ // C — the role was REPLACED in the map while the operations awaited (retired and re-admitted): refused at the mutation (Aster c6719863 C3)
  const { ctx, role, ids, body, refusedAt } = await scene((c, r) => { c.am.axonRoles.delete(L.topicId); c.rc.become(L.topicId, 'sub-terminal'); });
  const live = ctx.am.axonRoles.get(L.topicId);
  check('C (role replaced in axonRoles at resume) body, delete and kill are all refused at their mutations; the live replacement is untouched', refusedAt.includes('cachePush') && refusedAt.filter((a) => a === 'applyKill').length === 2 && !role.cacheIds.has(body.msgId) && !role.tombstones.has(ids[0]) && !role.tombstones.has(ids[1]) && live !== role && live.cache.length === 0 && live.tombstones.size === 0, JSON.stringify(refusedAt));
}

console.log(`\nfence_merge_gen_across_awaits: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
