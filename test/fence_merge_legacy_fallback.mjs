#!/usr/bin/env node
// =====================================================================
// fence_merge_legacy_fallback.mjs — dual-root packet 4(d) "Mixed versions"
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// L MERGE-CAPABLE, W LEGACY: W drops MERGE_OFFER as an unknown frame and a
// PULLUP from W never carries corr. If no bound arrives within
// MERGE_OFFER_TIMEOUT_MS and the offer was reached, L logs
// merge-legacy-fallback and takes today's path: demote() with the SUB
// carrying hw and lw, so the legacy W pulls what the shipped SPLIT_UNION
// rule pulls — the delta-only weakness the packet names. An offer whose
// route FAILED is not a legacy sign: the record is dropped and the next
// trigger retries.
//
// Checks: (1) with W unarmed (mergeCapable:false), the OFFER arrives and
// is ignored (no PULLUP under corr, no MERGE_OFFER handler effect); (2) at
// MERGE_OFFER_TIMEOUT_MS L logs merge-legacy-fallback and demotes into W
// with a SUB carrying hw and lw; (3) the legacy W pulls L's history by
// SPLIT_UNION and ends with L's body; (4) an offer whose route failed:
// merge-offer-timeout, no fallback, L still root; (5) control, both armed:
// the round binds inside the window and no fallback fires.
//
// Mutant (red): wait forever (never fall back) → 2 and 3 fail.
// Run: node test/fence_merge_legacy_fallback.mjs
// =====================================================================
import { MERGE_OFFER_TIMEOUT_MS } from '../src/pubsub/constants.js';
import { splitScene, realTopic, makeManagerAt, rootWithBodies, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_legacy_fallback: 4(d) — a legacy winner gets today\'s demote after the offer window');

{
  const S = await splitScene('legacy-fallback', { wOpts: { mergeCapable: false } });
  await S.stamp(S.W, 1); const B = await S.stamp(S.Ln, 2);
  await S.trigger();
  const offer = S.frames('pubsub:mergeoffer')[0];
  check('1 the OFFER reached the legacy W and was ignored: no PULLUP under corr, L still root', !!offer && S.frames('pubsub:pullup', (j) => typeof j.payload.corr === 'string').length === 0 && S.lRole.isRoot === true && S.lRole.sync.openMerges.size === 1);
  S.fab.clock += MERGE_OFFER_TIMEOUT_MS + 1;
  S.Ln.am._mergeSweep(S.Ln.am._now()); await S.fab.settle();
  const sub = S.frames('pubsub:sub', (j) => j.meta.fromId === idHex(S.lId) && j.payload.via?.[0] === idHex(S.wId))[0];
  check('2 at MERGE_OFFER_TIMEOUT_MS L logs merge-legacy-fallback and demotes into W with a SUB carrying hw and lw', S.logs(S.Ln, 'merge-legacy-fallback').length === 1 && S.lRole.isRoot === false && !!sub && Number.isFinite(sub.payload.hw) && Number.isFinite(sub.payload.lw) && sub.payload.lw > 0, JSON.stringify({ fb: S.logs(S.Ln, 'merge-legacy-fallback').length, isRoot: S.lRole.isRoot, sub: sub?.payload && { hw: sub.payload.hw, lw: sub.payload.lw } }));
  // today's path: W reads hw/lw on the SUB and pulls — REPLAY_UP above its hw here (L's history is newer), SPLIT_UNION when older
  const legacyPull = S.frames('pubsub:pullup', (j) => j.meta.fromId === idHex(S.wId) && j.dest === S.lId && typeof j.payload.corr !== 'string');
  check('3 the legacy W pulled L\'s history by today\'s hw/lw rule (a PULLUP without corr) and holds L\'s body', legacyPull.length >= 1 && S.wRole.cacheIds.has(B), JSON.stringify({ pulls: legacyPull.length, hasB: S.wRole.cacheIds.has(B) }));
}
{ // 4 — an offer whose route failed
  const L = await realTopic('legacy-fallback-unreachable');
  const ctx = makeManagerAt(L.self, { verdicts: true, route: (t, type) => (type === 'pubsub:mergeoffer' ? { consumed: false, exhausted: true } : { consumed: true, atNode: idHex(t) }) });
  const { role } = await rootWithBodies(ctx, L, 2);
  ctx.am._mergeCandidate(L.topicId, idHex(L.near), 3, 'verify-closer');
  for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  ctx.clock.t += MERGE_OFFER_TIMEOUT_MS + 1; ctx.am._mergeSweep(ctx.clock.t);
  check('4 an offer whose route failed: merge-offer-timeout, no fallback, L still root with custody', ctx.logs.some((l) => l.ev === 'pubsub:merge-offer-timeout') && !ctx.logs.some((l) => l.ev === 'pubsub:merge-legacy-fallback') && role.isRoot === true && role.cache.length === 2);
}
{ // 5 — control
  const S = await splitScene('legacy-fallback-control');
  await S.stamp(S.W, 1); await S.stamp(S.Ln, 2);
  await S.trigger();
  S.fab.clock += MERGE_OFFER_TIMEOUT_MS + 1; S.Ln.am._mergeSweep(S.Ln.am._now());
  check('5 control, both armed: the round bound inside the window and no fallback fires', S.logs(S.Ln, 'merge-bound').length === 1 && S.logs(S.Ln, 'merge-legacy-fallback').length === 0 && S.logs(S.Ln, 'merge-step5').length === 1);
}

console.log(`\nfence_merge_legacy_fallback: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
