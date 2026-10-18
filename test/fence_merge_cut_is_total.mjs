#!/usr/bin/env node
// =====================================================================
// fence_merge_cut_is_total.mjs — dual-root packet 4(d) "the cut at the
// mutation point" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A WRITE REACHING L AFTER THE BIND IS NOT STAMPED AT L. Every path into
// the stamp is cut: the bare write is intercepted after the hold and
// forwarded to W by name (via, gateFwd, ackTo); a via-pinned write that
// reaches the stamp is refused there and forwarded the same way; a bare
// KILL likewise; nothing enters L's cache or tombstones, and L sends no
// ingest ack for a write it did not stamp. Without a gate the same writes
// are stamped as before.
//
// Checks: (1) bare PUB at a gated L: forwarded to W with via, gateFwd and
// the flight's ack routing, merge-gate-forward logged; (2) L's cache is
// unchanged and no ingest ack left L; (3) a via-pinned PUB that reaches the
// stamp is refused at the stamp (cut-refused at:'stamp') and forwarded to
// W; (4) a bare KILL at a gated L is forwarded and no tombstone is set;
// (5) with the gate cleared the same bare PUB is stamped at L; (6) gen
// moved exactly once at the cut and never back.
//
// Mutant (red): make _cutHolds ignore the gate and delete the intercept
// call in _onPub → 1–3 fail (the write is stamped at L).
// Run: node test/fence_merge_cut_is_total.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, signedKill, bareMeta, pubsTo, killsTo, acksFrom, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_cut_is_total: 4(d) — a write reaching L after the bind is not stamped at L');

const L = await realTopic('cut-is-total');
{ // 1, 2 — bare PUB
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 3);
  const gen0 = role.gen;
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 10);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  const f = pubsTo(ctx, idHex(L.near));
  check('1 a bare PUB at a gated L is forwarded to W by name with gateFwd and the flight\'s ack routing', f.length === 1 && typeof f[0].payload.gateFwd === 'string' && typeof f[0].payload.ackTo === 'string' && typeof f[0].payload.flightNonce === 'string' && log(ctx, 'merge-gate-forward').length === 1, JSON.stringify(ctx.routed.map((r) => r.type)));
  check('2 L\'s cache is unchanged and no ingest ack left L', role.cache.length === 3 && !role.cacheIds.has(w.msgId) && acksFrom(ctx).length === 0);
  check('6 gen moved exactly once at the cut', role.gen === gen0 + 1);
}
{ // 3 — via-pinned write reaching the stamp
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 11);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json, via: [idHex(L.self)] }, { targetId: L.self, isTerminal: true, hopCount: 1, fromId: idHex(L.far) });
  const refused = log(ctx, 'cut-refused').filter((l) => l.ctx.at === 'stamp');
  check('3 a via-pinned PUB that reaches the stamp is refused at the stamp and forwarded to W', refused.length === 1 && !role.cacheIds.has(w.msgId) && pubsTo(ctx, idHex(L.near)).length === 1 && acksFrom(ctx).length === 0, JSON.stringify(refused.map((l) => l.ctx)));
}
{ // 4 — bare KILL
  const ctx = makeManagerAt(L.self);
  const { role, ids } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  await ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));
  check('4 a bare KILL at a gated L is forwarded to W and no tombstone is set', killsTo(ctx, idHex(L.near)).length === 1 && !role.tombstones.has(ids[0]) && role.cacheIds.has(ids[0]) && acksFrom(ctx).length === 0);
}
{ // 5 — no gate: stamped
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 1);
  const w = await signedPub(ctx, L.desc, 12);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  check('5 with no gate the same bare PUB is stamped at L and acked', role.cacheIds.has(w.msgId) && acksFrom(ctx).length === 1 && log(ctx, 'merge-gate-forward').length === 0);
}

console.log(`\nfence_merge_cut_is_total: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
