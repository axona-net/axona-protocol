#!/usr/bin/env node
// =====================================================================
// fence_merge_transition_atomic.mjs — dual-root packet Part 4(d) "the
// transition is a transaction" (axona-docs 14a6bd8; David "build it"
// 2026-10-17; Aster: the custody gate sits AT _set, not at its callers).
//
// _set RETURNS {committed, reason} AND EVERY CALLER BRANCHES ON IT. Before
// this, _set returned nothing: demote() pinned upstream, armed the hold and
// sent the confirming SUB whether or not the flag moved, and ADOPT reached
// _set with no demote() in the way at all. On a MERGE-CAPABLE node a
// ROOT→non-ROOT flip of a role WITH CUSTODY (a body, or an active tombstone)
// is refused unless ctx carries the release token the merge's step 5 minted;
// a refused flip runs none of the side effects.
//
// Checks (armed, custody): (1) demote() returns false and the role is still
// root; (2) no upstream pinned; (3) no SUB sent; (4) no step-down hold armed;
// (5) custody-refused logged, nextCandidate recorded on the role; (6) a
// wrong token is refused the same way; (7) the minted token commits: not
// root, upstream pinned, SUB sent, hold armed; (8) the token is consumed by
// the flip; (9) a non-ROOT→ROOT flip is never gated; (10) UNARMED with the
// same cache the flip commits as 4.114.1; (11) a role already in the target
// state reports committed:'noop' without logging a transition.
//
// Mutant (red): delete `if (!flip.committed) return false;` in demote() → 2
// and 3 fail; make _set return nothing → every check throws.
// Run: node test/fence_merge_transition_atomic.mjs
// =====================================================================
import { makeManager, seedCache, log, subsTo, upstreamOf, idHex, T, NEAR } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_transition_atomic: 4(d) — _set is a transaction with a custody gate');

{ // 1–8 armed with custody
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  const r = ctx.rc.demote(T, idHex(NEAR), 'beacon-closer');
  check('1 demote() of a root with custody returns false and the role is still root', r === false && role.isRoot === true);
  check('2 no upstream was pinned', upstreamOf(ctx) === null);
  check('3 no confirming SUB was sent', subsTo(ctx, idHex(NEAR)).length === 0 && ctx.routed.filter((x) => x.type === 'pubsub:sub').length === 0);
  check('4 no step-down hold was armed', !ctx.am._stepDownHold.has(T));
  const refused = log(ctx, 'custody-refused');
  check('5 custody-refused is logged and the refused flip is recorded as nextCandidate', refused.length === 1 && refused[0].ctx.why === 'beacon-closer' && refused[0].ctx.cache === 3 && role.merge?.nextCandidate?.why === 'beacon-closer' && role.merge.nextCandidate.ctx.to === idHex(NEAR).slice(0, 10), JSON.stringify(refused));
  role.merge.releaseToken = 'tok-right';
  const r6 = ctx.rc.demote(T, idHex(NEAR), 'merge-step5', { releaseToken: 'tok-wrong' });
  check('6 a wrong release token is refused the same way', r6 === false && role.isRoot === true && log(ctx, 'custody-refused').length === 2 && role.merge.releaseToken === 'tok-right');
  const r7 = ctx.rc.demote(T, idHex(NEAR), 'merge-step5', { releaseToken: 'tok-right' });
  check('7 the minted token commits: not root, upstream pinned, SUB sent, hold armed', r7 === true && role.isRoot === false && upstreamOf(ctx) === idHex(NEAR) && subsTo(ctx, idHex(NEAR)).length === 1 && ctx.am._stepDownHold.get(T)?.to === idHex(NEAR));
  check('8 the token is consumed by the flip and never appears in the transition log', role.merge.releaseToken === null && log(ctx, 'root-transition').every((l) => !('releaseToken' in l.ctx)));
}
{ // 9 — promotion is never gated
  const ctx = makeManager({ mergeCapable: true });
  const role = ctx.rc.become(T, 'sub-terminal');
  seedCache(ctx, role, 2);
  role.merge = null;
  role.isRoot = false;                                        // hand-place a non-root with a body
  const r = ctx.rc._set(role, true, 'terminal-promote');
  check('9 a non-ROOT→ROOT flip of a role with a body is never gated', r.committed === true && role.isRoot === true);
}
{ // 10 — unarmed
  const ctx = makeManager({ mergeCapable: false });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  const r = ctx.rc.demote(T, idHex(NEAR), 'beacon-closer');
  check('10 unarmed, the same flip commits as 4.114.1: not root, upstream pinned, SUB sent, no custody line', r === true && role.isRoot === false && upstreamOf(ctx) === idHex(NEAR) && subsTo(ctx, idHex(NEAR)).length === 1 && log(ctx, 'custody-refused').length === 0);
}
{ // 11 — no-op
  const ctx = makeManager({ mergeCapable: true });
  const role = seedCache(ctx, ctx.rc.become(T, 'sub-terminal'), 3);
  const n = log(ctx, 'root-transition').length;
  const r = ctx.rc._set(role, true, 'again');
  check('11 a flip to the state the role is already in reports committed:noop and logs nothing', r.committed === true && r.reason === 'noop' && log(ctx, 'root-transition').length === n);
}

console.log(`\nfence_merge_transition_atomic: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
