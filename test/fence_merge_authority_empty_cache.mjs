#!/usr/bin/env node
// =====================================================================
// fence_merge_authority_empty_cache.mjs — dual-root packet Part 3 "custody"
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// CUSTODY IS A BODY OR AN ACTIVE TOMBSTONE, NOTHING ELSE. A root holding
// nothing has nothing to reconcile: on a merge-capable node every path that
// sheds its claim demotes directly, as 4.114.1 did, with no candidate record
// and no custody refusal. A root with an empty cache but an ACTIVE tombstone
// holds custody (a merge that dropped it would resurrect the killed
// message at the winner); an EXPIRED tombstone is not custody.
//
// Checks (armed): (1) hasCustody: empty → false, body → true, active
// tombstone alone → true, expired tombstone alone → false; (2) the beacon
// yield at an empty root demotes directly; (3) the verify at an empty root
// demotes directly; (4) ADOPT at an empty root adopts; (5) handoffArrived
// at an empty root with a closer live root demotes directly; (6) an active
// tombstone alone makes the beacon yield record a candidate instead;
// (7) once the tombstone expires the next beacon yield demotes directly.
//
// Mutant (red): make hasCustody ignore tombstones → 1 and 6 fail.
// Run: node test/fence_merge_authority_empty_cache.mjs
// =====================================================================
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';
import { makeManager, seedCache, seedTombstone, hearBeacon, log, subsTo, upstreamOf, settle, idHex, SELF, T, NEAR, FAR } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const direct = (ctx, role) => role.isRoot === false && upstreamOf(ctx) === idHex(NEAR) && subsTo(ctx, idHex(NEAR)).length === 1 && log(ctx, 'merge-candidate').length === 0 && log(ctx, 'custody-refused').length === 0;

console.log('fence_merge_authority_empty_cache: Part 3 — a root holding nothing yields directly; custody is a body or an active tombstone');

{ // 1
  const ctx = makeManager({ mergeCapable: true });
  const e = ctx.rc.become(T, 'sub-terminal');
  const empty = ctx.rc.hasCustody(e);
  seedCache(ctx, e, 1); const body = ctx.rc.hasCustody(e);
  const t2 = ctx.rc.become(T ^ 0x7n, 'sub-terminal'); seedTombstone(ctx, t2); const activeTomb = ctx.rc.hasCustody(t2);
  const t3 = ctx.rc.become(T ^ 0x9n, 'sub-terminal'); seedTombstone(ctx, t3, { expired: true }); const expiredTomb = ctx.rc.hasCustody(t3);
  check('1 hasCustody: empty false, body true, active tombstone alone true, expired tombstone alone false', empty === false && body === true && activeTomb === true && expiredTomb === false, JSON.stringify({ empty, body, activeTomb, expiredTomb }));
}
{ // 2 beacon
  const ctx = makeManager({ mergeCapable: true });
  const role = ctx.rc.become(T, 'sub-terminal');
  hearBeacon(ctx, NEAR, 0);
  check('2 the beacon yield at an empty root demotes directly (no candidate, no refusal)', direct(ctx, role));
}
{ // 3 verify
  const ctx = makeManager({ mergeCapable: true, lookupPath: [SELF, FAR, NEAR] });
  const role = ctx.rc.become(T, 'sub-terminal');
  role.formedAt = ctx.clock.t - ROOT_VERIFY_FIRST_MS - 1; role.lastVerify = 0;
  ctx.am._verifyRoots(ctx.clock.t); await settle();
  check('3 the self-verify at an empty root demotes directly', direct(ctx, role) && log(ctx, 'root-verify-demote').length === 1);
}
{ // 4 adopt
  const ctx = makeManager({ mergeCapable: true });
  const role = ctx.rc.become(T, 'sub-terminal');
  ctx.am._onAdopt({ topicId: idHex(T), parent: idHex(NEAR), subs: [], nonce: 'n1' }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('4 ADOPT at an empty root adopts (child under the parent, acked)', role.isRoot === false && upstreamOf(ctx) === idHex(NEAR) && ctx.routed.some((r) => r.type === 'pubsub:adoptack' && !r.payload.refused) && log(ctx, 'adopt-refused').length === 0);
}
{ // 5 handoff
  const ctx = makeManager({ mergeCapable: true, neighbors: [NEAR] });
  const role = ctx.rc.become(T, 'sub-terminal');
  ctx.am._rootBeacons.set(T, { root: idHex(NEAR), at: ctx.clock.t, exp: ctx.clock.t + 60_000, epoch: 0 });
  ctx.rc.handoffArrived(T, idHex(FAR));
  check('5 handoffArrived at an empty root with a closer live root demotes directly', direct(ctx, role));
}
{ // 6, 7 active tombstone alone
  const ctx = makeManager({ mergeCapable: true });
  const role = seedTombstone(ctx, ctx.rc.become(T, 'sub-terminal'));
  hearBeacon(ctx, NEAR, 0, T, 1);
  check('6 an active tombstone alone is custody: the beacon yield records a candidate and keeps the claim', role.isRoot === true && role.merge?.candidate?.id === idHex(NEAR) && log(ctx, 'merge-candidate').length === 1 && subsTo(ctx, idHex(NEAR)).length === 0, JSON.stringify(role.merge?.candidate));
  ctx.clock.t += 61_000;                                     // past the tombstone's exp
  hearBeacon(ctx, NEAR, 0, T, 2);
  check('7 once the tombstone expires the next beacon yield demotes directly', role.isRoot === false && upstreamOf(ctx) === idHex(NEAR) && subsTo(ctx, idHex(NEAR)).length === 1 && log(ctx, 'merge-candidate').length === 1);
}

console.log(`\nfence_merge_authority_empty_cache: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
