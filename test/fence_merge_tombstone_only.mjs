#!/usr/bin/env node
// =====================================================================
// fence_merge_tombstone_only.mjs — dual-root packet 4(d) tombstone_only
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A TOMBSTONE-ONLY ROLE TRANSFERS ITS DELS. L's whole state is one active
// tombstone (its body killed): the export carries it (header bodies 0,
// dels 1), W applies it, the ack is full with delsActive 1, L demotes, and
// a late reappearance of the killed body at W is suppressed. The legacy
// path too: _syncAnswerPull used to return on an empty cache and answered
// nothing for a role whose state is deletions.
//
// Checks: (1) the header reported bodies 0, dels 1; (2) W holds the
// tombstone and the ack was full (retained 0, delsActive 1); (3) L
// demoted; (4) the killed body re-delivered at W is suppressed; (5) legacy:
// _syncAnswerPull on a tombstone-only role answers a REPLAYUP carrying the
// del.
//
// Mutant (red): restore the `!role.cache.length` return in
// _syncAnswerPull → 5 fails; export bodies only → 2 fails (short).
// Run: node test/fence_merge_tombstone_only.mjs
// =====================================================================
import { splitScene, realTopic, makeManagerAt, rootWithBodies, signedKill, bareMeta, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_tombstone_only: 4(d) — a tombstone-only export transfers its deletion');

{
  const S = await splitScene('tombstone-only');
  const A = await S.stamp(S.W, 1);
  const Y = await S.stamp(S.Ln, 2); await S.kill(S.Ln, Y);
  if (S.lRole.cache.length !== 0 || !S.lRole.tombstones.has(Y)) { console.log('setup failed'); process.exit(1); }
  const yJson = S.frames('pubsub:pub', (j) => j.dest === S.lId)[0]?.payload?.json;   // the killed body's json, for the reappearance
  await S.trigger();
  const bound = S.logs(S.Ln, 'merge-bound')[0];
  check('1 the header reported bodies 0, dels 1', !!bound && bound.ctx.bodies === 0 && bound.ctx.dels === 1, JSON.stringify(bound?.ctx));
  const acked = S.logs(S.W, 'merge-acked')[0];
  check('2 W holds the tombstone; the ack was full with retained 0, delsActive 1', S.wRole.tombstones.has(Y) && !!acked && acked.ctx.full === true && acked.ctx.retained === 0 && acked.ctx.delsActive === 1, JSON.stringify(acked?.ctx));
  check('3 L demoted into W', S.lRole.isRoot === false && S.wRole.isRoot === true && S.wRole.cacheIds.has(A));
  // the killed body reappears at W (a late replica, a stale relay)
  const beforeN = S.wRole.cache.length;
  await S.W.am._syncIngest({ topicId: idHex(S.Lt.topicId), msgs: [{ msgId: Y, json: S.Ln.am._taBodies?.get?.(Y) ?? yJson ?? '{}', publishTs: S.fab.clock, seq: 99 }], dels: [] }, { targetId: S.wId }, 'REPLAY_UP');
  check('4 the killed body re-delivered at W is suppressed by the transferred tombstone', !S.wRole.cacheIds.has(Y) && S.wRole.cache.length === beforeN);
}
{ // 5 legacy path
  const L = await realTopic('tombstone-only-legacy');
  const ctx = makeManagerAt(L.self, { mergeCapable: false });
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  await ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));
  const n0 = ctx.routed.filter((r) => r.type === 'pubsub:replayup').length;
  ctx.am._syncAnswerPull({ topicId: idHex(L.topicId), sinceHw: 0, parentId: idHex(L.far) });
  const r = ctx.routed.filter((x) => x.type === 'pubsub:replayup')[n0];
  check('5 legacy: _syncAnswerPull on a tombstone-only role answers a REPLAYUP carrying the del', role.cache.length === 0 && !!r && r.payload.dels.length === 1 && r.payload.dels[0].msgId === ids[0] && r.payload.msgs.length === 0);
}

console.log(`\nfence_merge_tombstone_only: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
