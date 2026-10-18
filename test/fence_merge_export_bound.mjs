#!/usr/bin/env node
// =====================================================================
// fence_merge_export_bound.mjs — dual-root packet 4(d) "EXPORT_BYTES_MAX,
// the serialized bound" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// AT THE CUT L MEASURES ITS SERIALIZED EXPORT. If it exceeds
// EXPORT_BYTES_MAX the round is refused with refused-export-bound before
// the header, L keeps custody, the gate set for that measurement clears,
// and W closes its pull. The bound is derived so that a cache at
// CACHE_BYTES plus the tombstone cap at maximum proof size fits under it;
// here the cache is placed directly above the bound.
//
// Checks: (1) the derivation: EXPORT_BYTES_MAX ≥ CACHE_BYTES + CACHE_MAX ×
// MERGE_TOMB_PROOF_MAX_BYTES, and F_MAX = ⌈EXPORT_BYTES_MAX /
// REPLAY_CHUNK_BYTES⌉ + 1; (2) an export measured above the bound at the
// cut: refused-export-bound logged, no header sent, a refused REPLAYUP sent
// to W, the gate cleared, L root with its cache intact; (3) W on the
// refused frame closes its pull (merge-pull-refused); (4) an export under
// the bound binds and sends the header.
//
// Mutant (red): skip the measure (bound F by the body cache alone) → 2
// fails (a header is sent).
// Run: node test/fence_merge_export_bound.mjs
// =====================================================================
import { EXPORT_BYTES_MAX, MERGE_F_MAX, CACHE_BYTES, CACHE_MAX, MERGE_TOMB_PROOF_MAX_BYTES, REPLAY_CHUNK_BYTES } from '../src/pubsub/constants.js';
import { realTopic, makeManagerAt, rootWithBodies, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_export_bound: 4(d) — the serialized export is measured at the cut against EXPORT_BYTES_MAX');

check('1 the derivation: the bound covers a full body cache plus the tombstone cap at maximum proof size; F_MAX is the frame count plus the header', EXPORT_BYTES_MAX >= CACHE_BYTES + CACHE_MAX * MERGE_TOMB_PROOF_MAX_BYTES && MERGE_F_MAX === Math.ceil(EXPORT_BYTES_MAX / REPLAY_CHUNK_BYTES) + 1, JSON.stringify({ EXPORT_BYTES_MAX, MERGE_F_MAX }));

const L = await realTopic('export-bound');
const meta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
{ // 2, 3
  const ctx = makeManagerAt(L.self, { extra: { replayCacheBytes: 64 * 1024 * 1024 } });
  const { role } = await rootWithBodies(ctx, L, 1);
  // place the cache directly above the bound: five 4 MiB bodies (not through the stamp; the cap is 16 MiB)
  const big = 'y'.repeat(4 * 1024 * 1024);
  for (let k = 0; k < 5; k++) { const json = JSON.stringify({ k, big }); role.cache.push({ msgId: `big-${k}`, publishTs: ctx.clock.t + k, json, seq: 10 + k, bytes: json.length + 80 }); role.cacheIds.add(`big-${k}`); role.cacheBytes += json.length + 80; }
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  const offer = ctx.routed.find((r) => r.type === 'pubsub:mergeoffer');
  ctx.am._onPullUp({ topicId: idHex(L.topicId), corr: offer.payload.corr, parentId: idHex(L.near), winnerEpoch: 3, cursor: null, sinceHw: 0, timing: offer.payload.timing }, meta(ctx, L.near));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  const refused = log(ctx, 'refused-export-bound')[0];
  const replays = ctx.routed.filter((r) => r.type === 'pubsub:replayup');
  check('2 an export above the bound: refused-export-bound, no header, a refused REPLAYUP to W, gate cleared, L root with its cache intact', !!refused && refused.ctx.bytes > EXPORT_BYTES_MAX && replays.length === 1 && replays[0].payload.refused === 'export-bound' && !replays[0].payload.header && role.merge.gate === null && role.isRoot === true && role.cache.length === 6 && role.sync.openMerges.size === 0, JSON.stringify({ refused: refused?.ctx, replays: replays.map((r) => Object.keys(r.payload)) }));
  // W side: the refused frame closes the pull
  const W = makeManagerAt(L.near);
  const wRole = W.rc.become(L.topicId, 'sub-terminal');
  wRole.sync.openPulls.set(offer.payload.corr, { loser: idHex(L.self), loserEpoch: role.epoch, cursor: null, framesIn: 0, held: 0, sent: 0, startedAt: W.clock.t, timing: offer.payload.timing, state: 'pulling', manifest: { bodies: [], dels: [] }, header: null, tS: 0 });
  await W.am._mergeIngestFrame(replays[0].payload, { targetId: L.near });
  check('3 W on the refused frame closes its pull', wRole.sync.openPulls.size === 0 && log(W, 'merge-pull-refused').length === 1);
}
{ // 4
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 3);
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  const offer = ctx.routed.find((r) => r.type === 'pubsub:mergeoffer');
  ctx.am._onPullUp({ topicId: idHex(L.topicId), corr: offer.payload.corr, parentId: idHex(L.near), winnerEpoch: 3, cursor: null, sinceHw: 0, timing: offer.payload.timing }, meta(ctx, L.near));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  const replays = ctx.routed.filter((r) => r.type === 'pubsub:replayup');
  check('4 an export under the bound binds and sends the header', log(ctx, 'refused-export-bound').length === 0 && replays.length === 1 && !!replays[0].payload.header && replays[0].payload.header.bodies === 3 && !!role.merge.gate);
}

console.log(`\nfence_merge_export_bound: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
