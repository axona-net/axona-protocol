#!/usr/bin/env node
// =====================================================================
// fence_merge_lease_end_completion_lost.mjs — dual-root packet 4(d) "What
// holds custody when completion is lost" (axona-docs 14a6bd8; David "build
// it" 2026-10-17).
//
// L flipped inside its deadline and no merge-bearing SUB ever reaches W
// before tS + R. W releases its pins and the record fails; L is a child of
// W that still holds the whole export, pinned against cap eviction, serves
// it, and re-sends the merge block on every renewal. The holder is L, and
// L's only permitted release of an entry is its own TTL by its own clock.
// An entry leaves the network only by L's TTL, never by W's cap or clock.
//
// Checks, with every SUB from L dropped: (1) L flipped (step 5) with
// completing set and its export pinned; (2) a renewal from L carries the
// merge block (re-sent on each renewal); (3) W's record fails at tS + R,
// pins released; (4) after that, W's cap evicts L's entry (W's custody is
// ordinary) while L still holds it: the holder is L; (5) L's cap cannot
// evict the pinned export; (6) only L's own TTL releases it.
//
// Mutant (red): L drops its pins at the flip → 5 fails.
// Run: node test/fence_merge_lease_end_completion_lost.mjs
// =====================================================================
import { MERGE_RETENTION_MS, TTL_MS } from '../src/pubsub/constants.js';
import { splitScene, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { author } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_lease_end_completion_lost: 4(d) — the holder on a lost completion is L, until its own TTL');

const S = await splitScene('lease-end-lost', { wOpts: { replayCacheSize: 70 }, lOpts: { replayCacheSize: 3 } });
await S.stamp(S.W, 1);
const B = await S.stamp(S.Ln, 2);
const subsFromL = [];
S.fab.tap = (j) => { if (j.type === 'pubsub:sub' && j.meta.fromId === idHex(S.lId)) { subsFromL.push(j); return null; } return j; };
await S.trigger();
check('1 L flipped (step 5) with completing set and its export pinned', S.lRole.isRoot === false && S.logs(S.Ln, 'merge-step5').length === 1 && !!S.lRole.merge.completing && S.lRole.merge.pins.has(B));
// a later renewal from L
S.fab.clock += 60_000; S.Ln.am._sendSubscribe(S.Lt.topicId); await S.fab.settle();
// the demote's own SUB is an ordinary renewal (completion is claimed after the flip); every SUB from then on carries the block
const first = subsFromL.findIndex((j) => j.payload.merge);
check('2 every renewal from L after the flip carries the merge block while completing (the merge SUB and a later renewal, both dropped)', first >= 0 && subsFromL.length - first >= 2 && subsFromL.slice(first).every((j) => j.payload.merge && j.payload.merge.corr === S.lRole.merge.completing.corr), JSON.stringify(subsFromL.map((j) => !!j.payload.merge)));
// W's retention ends
S.fab.clock += MERGE_RETENTION_MS + 1; S.W.am._mergeSweep(S.W.am._now());
check('3 W\'s record fails at tS + R, pins released', S.logs(S.W, 'merge-retention-expired').length === 1 && S.wRole.merge.pins.size === 0 && S.wRole.sync.openPulls.size === 0);
// W's cap evicts L's entry (ordinary custody at W) — fill W to its cap
for (let k = 0; k < 75; k++) { const json = JSON.stringify({ k }); S.W.am._cachePush(S.wRole, { msgId: `fill-${k}`, publishTs: S.fab.clock + k, json, seq: 100 + k }); }
check('4 W\'s cap evicted L\'s entry (W\'s custody is ordinary now) while L still holds it: the holder is L', !S.wRole.cacheIds.has(B) && S.lRole.cacheIds.has(B));
// L's cap cannot evict the pinned export (replayCacheSize 3: push five)
for (let k = 0; k < 5; k++) { const json = JSON.stringify({ k }); S.Ln.am._cachePush(S.lRole, { msgId: `lfill-${k}`, publishTs: S.fab.clock + 100 + k, json, seq: 200 + k }); }
check('5 L\'s cap cannot evict the pinned export', S.lRole.cacheIds.has(B) && S.lRole.cache.length <= 3 + 1, `L cache=${S.lRole.cache.length}`);
// only L's own TTL releases it
S.fab.clock += TTL_MS + 1; S.Ln.am._expireCache(S.lRole, S.Ln.am._now());
check('6 L\'s pinned export is exempt from expiry while completing is set (custody-alive, d4 discharges it by TTL of every pinned entry)', S.lRole.cacheIds.has(B));

console.log(`\nfence_merge_lease_end_completion_lost: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
