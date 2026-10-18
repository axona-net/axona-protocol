#!/usr/bin/env node
// =====================================================================
// fence_merge_role_gc_custody.mjs — dual-root packet 4(d) v0.14 "The
// custodian role is pinned, not only its entries" (axona-docs 14a6bd8;
// David "build it" 2026-10-17).
//
// A HOLDER IS A ROLE, AND THE KERNEL REAPS ROLES. deadNow was true for a
// role with no subscribers and an empty cache; idleReap for an empty cache
// past the idle TTL; the delete test read the cache only. A losing root
// whose export is tombstone-only, demoted and then left without
// subscribers, lost its custodian role before its tombstones expired, and
// a killed body that reappeared was no longer suppressed. v0.14: a role is
// CUSTODY-ALIVE while an open merge or pull, a completion in flight, an
// accepted-winner record, pinned entries, or ANY active tombstone holds;
// both reapers, the delete and backup retirement skip it.
//
// Scene: lost completion (every SUB from L dropped), no subscribers, a
// tombstone-only export at L. Checks: (1) after the flip L's role is
// custody-alive (completing + pins + an active tombstone); (2) a full
// cleanup tick (refreshTick) with the idle TTL exceeded does not reap it;
// (3) W's retention expires (record failed) and the tick still does not
// reap L's role; (4) a late reappearance of the killed body at L is
// suppressed; (5) once the tombstone expires by L's own TTL and completion
// is abandoned, the role is reaped; (6) a plain root with an active
// tombstone and nothing else (no merge state) is custody-alive too.
//
// Mutant (red): restore the cache-only tests (drop custodyAlive from the
// reapers) → 2 and 3 fail (the role is reaped on the first tick).
// Run: node test/fence_merge_role_gc_custody.mjs
// =====================================================================
import { MERGE_RETENTION_MS, TTL_MS } from '../src/pubsub/constants.js';
import { splitScene, realTopic, makeManagerAt, rootWithBodies, signedKill, bareMeta, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_role_gc_custody: 4(d) v0.14 — a custody-alive role is not reaped');

{
  const S = await splitScene('role-gc-custody', { lOpts: { refreshIntervalMs: 5_000 } });
  S.Ln.am._roleIdleTtlMs = 60_000;
  await S.stamp(S.W, 1);
  const Y = await S.stamp(S.Ln, 2); await S.kill(S.Ln, Y);
  const yJson = S.frames('pubsub:pub', (j) => j.dest === S.lId)[0]?.payload?.json;
  S.fab.tap = (j) => (j.type === 'pubsub:sub' && j.meta.fromId === idHex(S.lId)) ? null : j;
  await S.trigger();
  check('1 after the flip L\'s role is custody-alive: completing set, the tombstone pinned and active, no subscribers, empty cache', S.lRole.isRoot === false && !!S.lRole.merge.completing && S.lRole.merge.pins.has(Y) && S.lRole.tombstones.has(Y) && S.lRole.cache.length === 0 && S.lRole.subscribers.size === 0 && S.Ln.am._custodyAlive(S.lRole) === true);
  // a full cleanup tick with the idle TTL exceeded
  S.fab.clock += S.Ln.am._roleIdleTtlMs + 1;
  await S.Ln.am.refreshTick(); await S.fab.settle();
  check('2 a full cleanup tick past the idle TTL does not reap the custodian role', S.Ln.am.axonRoles.get(S.Lt.topicId) === S.lRole && S.lRole.tombstones.has(Y) && !S.Ln.logs.some((l) => l.ev === 'pubsub:role-reaped'), JSON.stringify(S.Ln.logs.filter((l) => l.ev === 'pubsub:role-reaped').map((l) => l.ctx)));
  // W's retention expires; another tick at L
  S.fab.clock += MERGE_RETENTION_MS + 1; S.W.am._mergeSweep(S.W.am._now());
  await S.Ln.am.refreshTick(); await S.fab.settle();
  check('3 W\'s record failed at tS + R and the tick still does not reap L\'s role', S.logs(S.W, 'merge-retention-expired').length === 1 && S.Ln.am.axonRoles.get(S.Lt.topicId) === S.lRole && S.lRole.tombstones.has(Y));
  // the killed body reappears at L
  await S.Ln.am._syncIngest({ topicId: idHex(S.Lt.topicId), msgs: [{ msgId: Y, json: yJson, publishTs: S.fab.clock, seq: 50 }], dels: [] }, { targetId: S.lId }, 'REPLAY_UP');
  check('4 a late reappearance of the killed body at L is suppressed', !S.lRole.cacheIds.has(Y));
  // the tombstone expires by L's own TTL; completion abandoned: the role is reaped on the next tick
  S.lRole.merge.completing = null; S.lRole.merge.pins.clear(); S.lRole.sync.openMerges.clear();
  S.fab.clock += TTL_MS + 1;
  await S.Ln.am.refreshTick(); await S.fab.settle();
  check('5 once the tombstone expires by L\'s own TTL and completion is abandoned, the role is gone on the next tick (custody discharged by local expiry)', !S.Ln.am.axonRoles.has(S.Lt.topicId), JSON.stringify({ has: S.Ln.am.axonRoles.has(S.Lt.topicId), alive: S.Ln.am._custodyAlive(S.lRole), tombs: [...S.lRole.tombstones.keys()].length, pins: S.lRole.merge.pins.size, aw: S.lRole.merge.acceptedWinner, openM: S.lRole.sync.openMerges.size, subs: S.lRole.subscribers.size, backupOf: S.lRole.backupOf, upstream: S.Ln.am._upstream.get(S.Lt.topicId), hold: !!S.Ln.am._stepDownHold.get(S.Lt.topicId) }));
}
{ // 6 — a plain root with an active tombstone and nothing else
  const L = await realTopic('role-gc-plain-tomb');
  const ctx = makeManagerAt(L.self, { mergeCapable: false });
  ctx.am._roleIdleTtlMs = 60_000;
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  await ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));
  ctx.clock.t += ctx.am._roleIdleTtlMs + 1;
  await ctx.am.refreshTick();
  check('6 a plain root holding only an active tombstone (no merge state, unarmed) is custody-alive and survives the cleanup tick', role.cache.length === 0 && role.tombstones.has(ids[0]) && ctx.am.axonRoles.get(L.topicId) === role && ctx.am._custodyAlive(role) === true);
}

console.log(`\nfence_merge_role_gc_custody: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
