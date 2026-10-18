#!/usr/bin/env node
// =====================================================================
// fence_merge_reserve_exhaustion.mjs — dual-root packet 4(d) "W's own
// writes, fail closed" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// W'S WRITES DURING A ROUND CONSUME THE RESERVE ADMITTED WITH THE HEADER.
// Past it, W refuses its own writes with refused-merge-reserve, the
// writer's flight recovers after the round, nothing is queued, and
// NOTHING PINNED IS EVICTED. After the round the same write is stamped.
//
// Checks: (1) during the round, MERGE_WRITE_RESERVE_ENTRIES writes at W
// are stamped and the next is refused with refused-merge-reserve; (2) no
// pinned entry (W's baseline or the manifest) was evicted; (3) after the
// round (completed), the refused write re-sent is stamped; (4) the
// reserve is cleared with the pins at completion.
//
// Mutant (red): admit past the reserve → 1 fails (and with a tight cap,
// 2: a pin evicted).
// Run: node test/fence_merge_reserve_exhaustion.mjs
// =====================================================================
import { MERGE_WRITE_RESERVE_ENTRIES } from '../src/pubsub/constants.js';
import { splitScene, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { author } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_reserve_exhaustion: 4(d) — W\'s writes past the reserve are refused; nothing pinned moves');

// W's cap: baseline 2 + export 3 + reserve 64 = 69 exactly, so the reserve is the whole slack.
const S = await splitScene('reserve-exhaustion', { wOpts: { replayCacheSize: MERGE_WRITE_RESERVE_ENTRIES + 5 } });
const w1 = await S.stamp(S.W, 1), w2 = await S.stamp(S.W, 2);
const l = []; for (let k = 3; k < 6; k++) l.push(await S.stamp(S.Ln, k));
// Hold the round open: drop the terminal REPLAYACK so W stays in 'pulling'… no — the reserve is live from the header to
// completion. Simplest: intercept the loser's merge-bearing SUB so completion waits while W writes.
let held = [];
S.fab.tap = (j) => { if (j.type === 'pubsub:sub' && j.payload.merge) { held.push(j); return null; } return j; };
await S.trigger();
const admitted = S.logs(S.W, 'merge-admitted')[0];
const acked = S.logs(S.W, 'merge-acked')[0];
if (!admitted || !acked || !S.wRole.merge.reserve) { console.log('setup: round not open at W', { admitted: !!admitted, acked: !!acked, reserve: S.wRole.merge.reserve }); process.exit(1); }
// W writes during the round (its own publishers), past the reserve
const writeAtW = async (k) => {
  S.fab.clock += 1;
  const env = await buildEnvelope({ topic: S.Lt.desc, message: { k }, seq: k, identity: author, ts: S.fab.clock });
  await S.W.handlers.get('pubsub:pub')({ topicId: idHex(S.Lt.topicId), json: JSON.stringify(env) }, S.meta(S.W));
  return env;
};
let stamped = 0; let refusedEnv = null;
for (let k = 0; k < MERGE_WRITE_RESERVE_ENTRIES + 1; k++) {
  const env = await writeAtW(1000 + k);
  if (S.wRole.cacheIds.has(env.msgId)) stamped++; else refusedEnv = env;
}
await S.fab.settle();
check('1 MERGE_WRITE_RESERVE_ENTRIES writes at W are stamped during the round; the next is refused with refused-merge-reserve', stamped === MERGE_WRITE_RESERVE_ENTRIES && !!refusedEnv && S.logs(S.W, 'refused-merge-reserve').length === 1, JSON.stringify({ stamped, refused: S.logs(S.W, 'refused-merge-reserve').length }));
check('2 nothing pinned was evicted: W\'s baseline and the manifest are all held', [w1, w2, ...l].every((id) => S.wRole.cacheIds.has(id)) && S.wRole.cache.length === 2 + 3 + MERGE_WRITE_RESERVE_ENTRIES, `cache=${S.wRole.cache.length}`);
// release the held SUB: completion
S.fab.tap = null; for (const j of held) S.fab.queue.push(j); held = []; await S.fab.settle();
const completed = S.logs(S.W, 'merge-complete').length === 1;
if (refusedEnv) await S.W.handlers.get('pubsub:pub')({ topicId: idHex(S.Lt.topicId), json: JSON.stringify(refusedEnv) }, S.meta(S.W));
check('3 after the round the refused write re-sent is stamped (the writer\'s recovery)', completed && !!refusedEnv && S.wRole.cacheIds.has(refusedEnv.msgId), JSON.stringify({ completed, stale: S.logs(S.W, 'merge-stale').map((x) => x.ctx.why) }));
check('4 the reserve and the pins are cleared at completion', S.wRole.merge.reserve === null && S.wRole.merge.pins.size === 0);

console.log(`\nfence_merge_reserve_exhaustion: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
