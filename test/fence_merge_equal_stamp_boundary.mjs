#!/usr/bin/env node
// =====================================================================
// fence_merge_equal_stamp_boundary.mjs — dual-root packet 4(d) "The
// cursor": a frame never ends inside an equal-stamp group unless the group
// alone exceeds a frame (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// The cache defines no order among entries that share a stamp, so the
// export sorts by (publishTs, msgId) and a frame boundary that fell inside
// a group would make the pair cursor ambiguous across a restart. Two
// scenes, bodies sized against REPLAY_CHUNK_BYTES (96 KiB):
//   A: t1 (50 KiB), then t2a + t2b (30 KiB each, one stamp). A frame of
//      96 KiB would fit t1 + t2a and end inside the t2 group → the first
//      frame carries t1 alone and the second carries t2a + t2b.
//   B: three 40 KiB bodies sharing one stamp: the group alone exceeds a
//      frame → the pair cursor advances within it across frames and all
//      three arrive, none dropped, in (stamp, msgId) order.
//
// Checks: (1) A: frame 1 = [t1], frame 2 = [t2a, t2b]; (2) A: cursor
// continuity across the two frames; (3) B: all three arrive across ≥ 2
// frames with cursor continuity, in msgId order within the stamp.
//
// Mutant (red): delete the equal-stamp rule in _mergeAnswerPull → 1 fails
// (frame 1 = [t1, t2a]).
// Run: node test/fence_merge_equal_stamp_boundary.mjs
// =====================================================================
import { REPLAY_CHUNK_BYTES } from '../src/pubsub/constants.js';
import { realTopic, makeManagerAt, signedPub, log, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { author } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const KB = 1024;

console.log('fence_merge_equal_stamp_boundary: 4(d) — a frame never ends inside an equal-stamp group');

const L = await realTopic('equal-stamp-boundary');
const meta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
/** Seed bodies directly with chosen stamps (the stamp rule is the root's; here we need equal stamps). */
async function seed(ctx, role, specs) {
  const ids = [];
  for (const [k, ts, size] of specs) {
    const env = await buildEnvelope({ topic: L.desc, message: { k, pad: 'x'.repeat(size) }, seq: k, identity: author, ts: ctx.clock.t });
    const json = JSON.stringify(env);
    ctx.am._cachePush(role, { msgId: env.msgId, publishTs: ts, json, seq: k });
    ids.push(env.msgId);
  }
  return ids;
}
async function run(specs) {
  const ctx = makeManagerAt(L.self, { extra: { replayCacheBytes: 64 * 1024 * 1024 } });
  const role = ctx.rc.become(L.topicId, 'sub-terminal');
  const ids = await seed(ctx, role, specs);
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  const offer = ctx.routed.find((r) => r.type === 'pubsub:mergeoffer');
  const corr = offer.payload.corr;
  const pull = (cursor) => ctx.am._onPullUp({ topicId: idHex(L.topicId), corr, parentId: idHex(L.near), winnerEpoch: 3, cursor, sinceHw: 0, timing: offer.payload.timing }, meta(ctx, L.near));
  const replays = () => ctx.routed.filter((r) => r.type === 'pubsub:replayup' && r.payload.corr === corr);
  pull(null); for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  let cursor = 'h'; const frames = [];
  for (let g = 0; g < 10; g++) {
    pull(cursor);
    const f = replays()[replays().length - 1];
    if (!f || f.payload.cursor !== cursor) break;
    frames.push(f.payload);
    if (!f.payload.more) break;
    cursor = f.payload.lastCursor;
  }
  return { ctx, role, ids, frames };
}
{ // A
  const t = 1_000_500;
  const { ids, frames } = await run([[1, t, 50 * KB], [2, t + 1, 30 * KB], [3, t + 1, 30 * KB]]);
  const [t1, a2, b2] = ids;
  const sortedT2 = [a2, b2].sort();
  check('A1 frame 1 = [t1] alone; frame 2 = the whole t2 group', frames.length === 2 && frames[0].msgs.map((m) => m.msgId).join() === t1 && frames[1].msgs.map((m) => m.msgId).join() === sortedT2.join(), JSON.stringify(frames.map((f) => f.msgs.map((m) => m.msgId.slice(0, 6)))));
  check('A2 cursor continuity: frame 2 asked with frame 1\'s lastCursor; frame 2 terminal', frames.length === 2 && frames[1].cursor === frames[0].lastCursor && frames[1].more === false && frames[0].more === true);
}
{ // B
  const t = 2_000_500;
  const { ids, frames } = await run([[1, t, 40 * KB], [2, t, 40 * KB], [3, t, 40 * KB]]);
  const all = frames.flatMap((f) => f.msgs.map((m) => m.msgId));
  const sorted = ids.slice().sort();
  const contiguous = frames.every((f, i) => i === 0 ? f.cursor === 'h' : f.cursor === frames[i - 1].lastCursor);
  check('B all three equal-stamp bodies arrive across ≥ 2 frames, in msgId order, with cursor continuity (the group alone exceeds a frame)', frames.length >= 2 && all.join() === sorted.join() && contiguous && frames[frames.length - 1].more === false, JSON.stringify({ frames: frames.length, sizes: frames.map((f) => f.msgs.length), chunk: REPLAY_CHUNK_BYTES }));
}

console.log(`\nfence_merge_equal_stamp_boundary: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
