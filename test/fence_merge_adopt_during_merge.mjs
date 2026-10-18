#!/usr/bin/env node
// =====================================================================
// fence_merge_adopt_during_merge.mjs — dual-root packet Part 4(d), Aster's
// "adopt bypass" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// ADOPT NEVER CALLED demote(). A delegating parent's ADOPT reached
// adoptChild → _set(role, false) with no gate in the way: a root holding
// eleven entries became a child relay on one routed frame and the entries
// sat behind a non-root. The custody gate at _set now covers it, and the
// child ANSWERS: ADOPTACK {accepted} on success, {refused:'custody'} when
// the flip is refused.
//
// Checks (armed): (1) a root with custody that receives ADOPT stays root;
// (2) no upstream pinned, no SUB toward the parent; (3) the delegated
// subscribers are NOT seated here; (4) adopt-refused logged; (5) ADOPTACK
// routed to the parent with refused:'custody', the nonce and no accepted
// ids. (6) The same node with an EMPTY cache adopts as before and acks the
// seated ids. (7) UNARMED with custody: adopts as 4.114.1 and acks. (8) A
// legacy ADOPT without a nonce gets no ack either way.
//
// Mutant (red): make adoptChild ignore the flip result → 1–5 fail.
// Run: node test/fence_merge_adopt_during_merge.mjs
// =====================================================================
import { makeManager, seedCache, log, subsTo, upstreamOf, idHex, SELF, T, NEAR, FAR } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const S1 = idHex(T ^ 0x5001n), S2 = idHex(T ^ 0x5002n);
const adopt = (ctx, { nonce = 'n0001' } = {}) => ctx.am._onAdopt({ topicId: idHex(T), parent: idHex(NEAR), subs: [{ subscriberId: S1, since: 0 }, { subscriberId: S2, since: 0 }], ...(nonce ? { nonce } : {}) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
const acks = (ctx) => ctx.routed.filter((r) => r.type === 'pubsub:adoptack');

console.log('fence_merge_adopt_during_merge: 4(d) — ADOPT at a root with custody is refused and answered');

{ // 1–5 armed, custody
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  adopt(ctx);
  check('1 the root with custody stays root', role.isRoot === true);
  check('2 no upstream pinned toward the parent and no SUB sent to it', upstreamOf(ctx) === null && subsTo(ctx, idHex(NEAR)).length === 0);
  check('3 the delegated subscribers are not seated here', !role.subscribers.has(S1) && !role.subscribers.has(S2));
  check('4 adopt-refused is logged with the parent', log(ctx, 'adopt-refused').length === 1 && log(ctx, 'adopt-refused')[0].ctx.parent === idHex(NEAR).slice(0, 12));
  const a = acks(ctx);
  check('5 ADOPTACK goes to the parent: refused custody, the nonce, no accepted ids', a.length === 1 && a[0].target === NEAR && a[0].payload.refused === 'custody' && a[0].payload.nonce === 'n0001' && Array.isArray(a[0].payload.accepted) && a[0].payload.accepted.length === 0, JSON.stringify(a.map((x) => x.payload)));
}
{ // 6 armed, no custody
  const ctx = makeManager({ mergeCapable: true });
  const role = ctx.rc.become(T, 'sub-terminal');
  adopt(ctx);
  const a = acks(ctx);
  check('6 the same node with an empty cache adopts: child, upstream = parent, both seated, ack names them', role.isRoot === false && upstreamOf(ctx) === idHex(NEAR) && role.subscribers.has(S1) && role.subscribers.has(S2) && a.length === 1 && !a[0].payload.refused && a[0].payload.accepted.length === 2 && a[0].payload.accepted.includes(S1), JSON.stringify(a.map((x) => x.payload)));
}
{ // 7 unarmed, custody
  const ctx = makeManager({ mergeCapable: false });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  adopt(ctx);
  const a = acks(ctx);
  check('7 unarmed with custody: adopts as 4.114.1 and acks the two ids', role.isRoot === false && role.subscribers.has(S1) && a.length === 1 && a[0].payload.accepted.length === 2 && log(ctx, 'adopt-refused').length === 0);
}
{ // 8 legacy parent (no nonce)
  const ctx = makeManager({ mergeCapable: true });
  seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  adopt(ctx, { nonce: null });
  const ctx2 = makeManager({ mergeCapable: true });
  ctx2.rc.become(T, 'sub-terminal');
  adopt(ctx2, { nonce: null });
  check('8 an ADOPT without a nonce (legacy parent) gets no ack, refused or accepted', acks(ctx).length === 0 && acks(ctx2).length === 0 && ctx2.am.axonRoles.get(T).subscribers.has(S1));
}

console.log(`\nfence_merge_adopt_during_merge: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
