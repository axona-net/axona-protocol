#!/usr/bin/env node
// =====================================================================
// fence_merge_cut_async_ingress.mjs — dual-root packet 4(d) "a check at
// the handler's entry is not a cut" (axona-docs 14a6bd8; David "build it"
// 2026-10-17).
//
// _onPub enters _ingestPublish, which awaits verifyEnvelope and
// deriveTopicIdBig before the stamp. A write admitted before the cut can
// resume after it and commit into a cache the export has already left. The
// cut is therefore checked at the stamp against the entry-time capture.
//
// Checks: (1) a PUB whose verifier is in flight when the cut happens is
// refused at the stamp (cut-refused at:'stamp'), never enters L's cache,
// and is forwarded to W by the gate; (2) no ingest ack left L for it;
// (3) a PUB that completed before the cut is in the cache; (4) the same
// race for a KILL: refused at _applyKill, no tombstone, forwarded to W.
//
// Mutant (red): delete the _cutHolds check before the stamp in
// _ingestPublish (keep the intercept at entry) → 1 fails.
// Run: node test/fence_merge_cut_async_ingress.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, signedKill, bareMeta, pubsTo, killsTo, acksFrom, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_cut_async_ingress: 4(d) — a write paused across the cut never commits at L');

const L = await realTopic('cut-async-ingress');
{ // 1–3 PUB
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  const before = await signedPub(ctx, L.desc, 20);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: before.json }, bareMeta(ctx, L.publisher));
  const w = await signedPub(ctx, L.desc, 21);
  const p = ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));   // NOT awaited: parked at verifyEnvelope
  gateRoot(ctx, L.topicId, L.near, 3);                                                                 // THE CUT, synchronously, while the verifier runs
  await p;
  const refused = log(ctx, 'cut-refused').filter((l) => l.ctx.at === 'stamp');
  check('1 a PUB paused in its verifier across the cut is refused at the stamp, never cached, and forwarded to W', refused.length === 1 && !role.cacheIds.has(w.msgId) && pubsTo(ctx, idHex(L.near)).length === 1 && pubsTo(ctx, idHex(L.near))[0].payload.json === w.json, JSON.stringify({ refused: refused.map((l) => l.ctx), cache: role.cache.length }));
  check('2 no ingest ack left L for the refused write (one left for the write before the cut)', acksFrom(ctx).length === 1);
  check('3 the write that completed before the cut is in L\'s cache', role.cacheIds.has(before.msgId) && role.cache.length === 3);
}
{ // 4 KILL
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  const kill = await signedKill(ctx, L.topicId, ids[1]);
  const p = ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));          // parked at verifyKill
  gateRoot(ctx, L.topicId, L.near, 3);
  await p;
  const refused = log(ctx, 'cut-refused').filter((l) => l.ctx.at === 'applyKill');
  check('4 a KILL paused in its verifier across the cut is refused at _applyKill, sets no tombstone, and is forwarded to W', refused.length === 1 && !role.tombstones.has(ids[1]) && role.cacheIds.has(ids[1]) && killsTo(ctx, idHex(L.near)).length === 1 && acksFrom(ctx).length === 0, JSON.stringify(refused.map((l) => l.ctx)));
}

console.log(`\nfence_merge_cut_async_ingress: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
