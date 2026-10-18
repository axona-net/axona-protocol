#!/usr/bin/env node
// =====================================================================
// fence_merge_authority_single_path.mjs — dual-root packet Part 4(d) "one
// candidate entry" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// EVERY PATH THAT SHEDS A ROOT CLAIM NAMES ITS CANDIDATE AT ONE ENTRY.
// Five sites called rootClaim.demote() directly: the beacon yield, the
// self-verify, the defer-confirmed verdict in _forwardToRoot, the SUB defer
// in _deferToRoot, and the handoff heir check. A root with custody on a
// merge-capable node now reaches none of them with the flag: each site calls
// _mergeCandidate, which records the winner under the election relation and
// leaves the claim and the cache in place for the exchange.
//
// Checks (armed, custody, each site in turn): (1) beacon yield → candidate
// {source:'beacon-closer'}, still root, no SUB; (2) self-verify →
// 'verify-closer'; (3) defer-confirmed (a consumed verdict attributed to the
// named root) → 'defer-confirmed'; (4) _deferToRoot → 'defer-terminal';
// (5) handoffArrived → 'handoff-better-heir'; (6) a candidate naming SELF
// is dropped; (7) two sites naming two winners in one tick converge on the
// relation's answer (the later, losing candidate does not replace the
// standing one; a later winning one does); (8) STRUCTURAL: no module outside
// rootClaim.js calls `.demote(` — the source is read, not assumed.
//
// Mutant (red): restore `this._rootClaim.demote(` at the beacon site → 1 and
// 8 fail.
// Run: node test/fence_merge_authority_single_path.mjs
// =====================================================================
import { readFileSync, readdirSync } from 'node:fs';
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';
import { makeManager, seedCache, hearBeacon, log, subsTo, settle, idHex, SELF, T, NEAR, NEAR2, FAR } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const held = (ctx, role, source, id = NEAR) => role.isRoot === true && role.cache.length === 3 && role.merge?.candidate?.id === idHex(id) && role.merge.candidate.source === source
  && ctx.routed.filter((r) => r.type === 'pubsub:sub').length === 0 && log(ctx, 'merge-candidate').length === 1 && log(ctx, 'root-transition').filter((l) => l.ctx.isRoot === false).length === 0;
const why = (ctx, role) => JSON.stringify({ isRoot: role.isRoot, cand: role.merge?.candidate, subs: ctx.routed.filter((r) => r.type === 'pubsub:sub').length, lines: log(ctx, 'merge-candidate').map((l) => l.ctx) });

console.log('fence_merge_authority_single_path: 4(d) — every claim-shedding path names its candidate at one entry');

{ // 1 beacon
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  hearBeacon(ctx, NEAR, 0);
  check('1 beacon yield: candidate recorded as beacon-closer, still root, no SUB', held(ctx, role, 'beacon-closer'), why(ctx, role));
}
{ // 2 verify
  const ctx = makeManager({ mergeCapable: true, lookupPath: [SELF, FAR, NEAR] });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  role.formedAt = ctx.clock.t - ROOT_VERIFY_FIRST_MS - 1; role.lastVerify = 0;
  ctx.am._verifyRoots(ctx.clock.t); await settle();
  check('2 self-verify: candidate recorded as verify-closer, still root, no SUB', held(ctx, role, 'verify-closer'), why(ctx, role));
}
{ // 3 defer-confirmed
  const ctx = makeManager({ mergeCapable: true, verdicts: true, route: (target) => ({ consumed: true, atNode: idHex(target) }) });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  ctx.am._forwardToRoot(T, 'pubsub:pub', { topicId: idHex(T), msgId: 'x', json: '{}', publishTs: ctx.clock.t }, idHex(NEAR));
  await settle();
  check('3 defer-confirmed (consumed verdict attributed to the named root): candidate recorded, still root, no SUB', held(ctx, role, 'defer-confirmed'), why(ctx, role));
}
{ // 4 defer-terminal
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  ctx.am._deferToRoot(T, 'pubsub:sub', { topicId: idHex(T), subscriberId: idHex(FAR), since: 0 }, idHex(NEAR));
  const fwd = ctx.routed.filter((r) => r.type === 'pubsub:sub');
  check('4 _deferToRoot: candidate recorded as defer-terminal, still root; the stranded SUB is forwarded, no SUB of our own', role.isRoot === true && role.merge?.candidate?.source === 'defer-terminal' && fwd.length === 1 && fwd[0].payload.subscriberId === idHex(FAR) && log(ctx, 'merge-candidate').length === 1, why(ctx, role));
}
{ // 5 handoff heir
  const ctx = makeManager({ mergeCapable: true, neighbors: [NEAR] });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  ctx.am._rootBeacons.set(T, { root: idHex(NEAR), at: ctx.clock.t, exp: ctx.clock.t + 60_000, epoch: 0 });
  ctx.rc.handoffArrived(T, idHex(FAR));
  check('5 handoffArrived with a closer live root: candidate recorded as handoff-better-heir, still root, no SUB', held(ctx, role, 'handoff-better-heir'), why(ctx, role));
}
{ // 6 self
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  const r = ctx.am._mergeCandidate(T, idHex(SELF), null, 'beacon-closer');
  check('6 a candidate naming self is dropped', r === false && !role.merge?.candidate && log(ctx, 'merge-candidate').length === 0);
}
{ // 7 convergence
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  ctx.am._mergeCandidate(T, idHex(NEAR), 1, 'beacon-closer');             // NEAR at epoch 1 (self is at epoch 1: closeness decides, NEAR wins)
  ctx.am._mergeCandidate(T, idHex(NEAR2), 1, 'verify-closer');            // NEAR2 is farther than NEAR, same epoch: loses to the standing one
  const keptNear = role.merge?.candidate?.id === idHex(NEAR) && log(ctx, 'merge-candidate').length === 1;
  ctx.am._mergeCandidate(T, idHex(FAR), 2, 'beacon-closer');              // FAR at epoch 2 beats NEAR at epoch 1
  check('7 two winners in one tick converge on the relation: a losing later candidate does not replace the standing one; a winning one does', keptNear && role.merge.candidate.id === idHex(FAR) && role.merge.candidate.epoch === 2 && log(ctx, 'merge-candidate')[1]?.ctx.replaced === true, why(ctx, role));
}
{ // 8 structural
  // The THREE permitted direct calls: inside _mergeCandidate itself (the "not
  // capable or no custody → demote directly" branch), inside _onReplayAck
  // (step 5: the one call _set accepts on a role with custody, carrying the
  // release token) and inside _mergeSweep (the legacy fallback, d4, minting
  // its own token after a reached offer timed out). Each body is found by its
  // header and the next method-level close brace; nothing else may call demote.
  const dir = new URL('../src/pubsub/', import.meta.url);
  const offenders = []; let inside = 0;
  const bodyRange = (lines, header) => {
    let start = -1;
    for (let i = 0; i < lines.length; i++) {
      if (start < 0 && header.test(lines[i])) start = i;
      else if (start >= 0 && /^  },?$/.test(lines[i])) return [start, i];
    }
    return null;
  };
  for (const f of readdirSync(dir)) {
    if (!f.endsWith('.js') || f === 'rootClaim.js') continue;
    const src = readFileSync(new URL(f, dir), 'utf8');
    const lines = src.split('\n');
    const ranges = [bodyRange(lines, /^  _mergeCandidate\(/), bodyRange(lines, /^  _onReplayAck\(/), bodyRange(lines, /^  _mergeSweep\(/)].filter(Boolean);
    lines.forEach((line, i) => {
      if (!/\.demote\(/.test(line) || /^\s*\/\//.test(line)) return;
      if (ranges.some(([a, b]) => i > a && i < b)) inside++; else offenders.push(`${f}:${i + 1}`);
    });
  }
  check('8 structural: the only .demote( calls outside rootClaim.js are the direct branch inside _mergeCandidate, step 5 inside _onReplayAck, and the legacy fallback inside _mergeSweep (each minting the token it needs)', offenders.length === 0 && inside === 3, `offenders=${offenders.join(' ')} inside=${inside}`);
}

console.log(`\nfence_merge_authority_single_path: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
