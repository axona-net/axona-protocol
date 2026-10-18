#!/usr/bin/env node
// =====================================================================
// fence_merge_stale_round.mjs — dual-root packet 4(d) "bound by corr,
// winnerEpoch and loserEpoch in every frame" (axona-docs 14a6bd8; David
// "build it" 2026-10-17).
//
// A REPLAYACK carrying a superseded corr, or a stale loserEpoch, or the
// wrong winner, demotes nothing: step 5 matches all six fields. A PULLUP
// under an unknown corr from a node that does not win over L opens no
// record; one from a node that does win opens a record without an offer.
//
// Checks, with a bound round at L: (1) an ack under another corr →
// merge-stale, L still root; (2) an ack with a stale loserEpoch →
// merge-stale; (3) an ack naming another winner → merge-stale; (4) an ack
// naming another winnerEpoch → merge-stale; (5) a PULLUP under an unknown
// corr from a FARTHER node at the same epoch → merge-stale, no record;
// (6) a PULLUP under an unknown corr from a CLOSER node at a higher epoch
// opens a record and binds (merge-cut) — the trigger reached W first.
//
// Mutant (red): drop the six-field match in _onReplayAck → 1–4 fail.
// Run: node test/fence_merge_stale_round.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_stale_round: 4(d) — nothing stale moves a claim or opens a record');

const L = await realTopic('stale-round');
const meta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
/** L root with custody, an offer to W bound by W's PULLUP under corr. */
async function boundRound() {
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  const offer = ctx.routed.find((r) => r.type === 'pubsub:mergeoffer');
  const corr = offer.payload.corr;
  ctx.am._onPullUp({ topicId: idHex(L.topicId), corr, parentId: idHex(L.near), winnerEpoch: 3, cursor: null, sinceHw: 0, timing: offer.payload.timing }, meta(ctx, L.near));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  const m = role.sync.openMerges.get(corr);
  if (!m || m.state !== 'bound') throw new Error('setup: not bound ' + JSON.stringify(m && m.state));
  return { ctx, role, corr, m, offer };
}
const ackBase = (ctx, corr, m) => ({ topicId: idHex(L.topicId), corr, winner: idHex(L.near), winnerEpoch: 3, loser: idHex(L.self), loserEpoch: m.loserEpoch, retained: 2, delsActive: 0, hash: m.hash, disposition: 'full' });
{
  const { ctx, role, corr, m } = await boundRound();
  const n0 = log(ctx, 'merge-stale').length;
  ctx.am._onReplayAck({ ...ackBase(ctx, corr, m), corr: 'ffffffffffffffffffffffffffffffff' }, meta(ctx, L.near));
  check('1 an ack under another corr: merge-stale, L still root', role.isRoot === true && log(ctx, 'merge-stale').length === n0 + 1);
  ctx.am._onReplayAck({ ...ackBase(ctx, corr, m), loserEpoch: m.loserEpoch - 1 }, meta(ctx, L.near));
  check('2 an ack with a stale loserEpoch: merge-stale, L still root', role.isRoot === true && log(ctx, 'merge-stale').length === n0 + 2);
  ctx.am._onReplayAck({ ...ackBase(ctx, corr, m), winner: idHex(L.far) }, meta(ctx, L.near));
  check('3 an ack naming another winner: merge-stale', role.isRoot === true && log(ctx, 'merge-stale').length === n0 + 3);
  ctx.am._onReplayAck({ ...ackBase(ctx, corr, m), winnerEpoch: 4 }, meta(ctx, L.near));
  check('4 an ack naming another winnerEpoch: merge-stale; the round is still bound', role.isRoot === true && log(ctx, 'merge-stale').length === n0 + 4 && m.state === 'bound' && !!role.merge.gate);
}
{
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  ctx.am._onPullUp({ topicId: idHex(L.topicId), corr: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', parentId: idHex(L.far), winnerEpoch: role.epoch, cursor: null, sinceHw: 0, timing: ctx.am._mergeTiming() }, meta(ctx, L.far));
  check('5 a PULLUP under an unknown corr from a farther node at the same epoch: merge-stale, no record, no cut', log(ctx, 'merge-stale').length === 1 && role.sync.openMerges.size === 0 && log(ctx, 'merge-cut').length === 0 && role.isRoot === true);
  ctx.am._onPullUp({ topicId: idHex(L.topicId), corr: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', parentId: idHex(L.near), winnerEpoch: role.epoch + 1, cursor: null, sinceHw: 0, timing: ctx.am._mergeTiming() }, meta(ctx, L.near));
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  check('6 a PULLUP under an unknown corr from a winning claim opens a record without an offer and binds', role.sync.openMerges.get('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')?.state === 'bound' && log(ctx, 'merge-cut').length === 1 && ctx.routed.some((r) => r.type === 'pubsub:replayup' && r.payload.header), JSON.stringify(ctx.logs.map((l) => l.ev).slice(-4)));
}

console.log(`\nfence_merge_stale_round: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
