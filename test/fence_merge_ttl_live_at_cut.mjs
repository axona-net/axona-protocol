#!/usr/bin/env node
// =====================================================================
// fence_merge_ttl_live_at_cut.mjs — dual-root packet 4(d) "the export
// carries EVERY body and tombstone live at L at the cut, by L's clock, with
// no margin" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// v0.10 excluded entries near expiry from the export; an entry near expiry
// is live, and excluding it let L demote while W lacked it. v0.11: no
// margin at the cut; W's retention is defined through pins exempt from
// expiry, whatever W's clock says.
//
// Scene: a body and a tombstone at L two minutes from expiry (stamped
// TTL − 2 min ago by L's clock), absent at W; W's clock runs five minutes
// AHEAD of L's (so by W's clock both are already past the TTL). Checks:
// (1) both are in the export (header bodies 1, dels 1); (2) both retained
// at W at the ack (full) although W's clock would expire them; (3) W's
// expiry sweep during the round removes neither (pins exempt); (4) L
// demoted on the full ack; (5) after completion the pins release and W's
// own TTL applies (the body expires on W's next sweep).
//
// Mutant (red): apply a margin at the cut (exclude entries within 2 min
// of expiry) → 1 and 2 fail.
// Run: node test/fence_merge_ttl_live_at_cut.mjs
// =====================================================================
import { TTL_MS } from '../src/pubsub/constants.js';
import { splitScene } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_ttl_live_at_cut: 4(d) — live at the cut means exported; pinned means not expired at W');

const S = await splitScene('ttl-live-at-cut');
await S.stamp(S.W, 1);
const B = await S.stamp(S.Ln, 2), Y = await S.stamp(S.Ln, 3); await S.kill(S.Ln, Y);
// age L's entries to two minutes from expiry (by the shared fabric clock, which is L's)
const t0 = S.fab.clock;
S.fab.clock = t0 + TTL_MS - 2 * 60_000;
// W's clock runs five minutes ahead
const lNow = S.Ln.am._now; S.W.am._now = () => lNow() + 5 * 60_000;
let sweptDuring = null;
const origHash = S.W.am._mergeManifestHash.bind(S.W.am);
S.W.am._mergeManifestHash = async (b, d) => { S.W.am._expireCache(S.wRole, S.W.am._now()); sweptDuring = { body: S.wRole.cacheIds.has(B), tomb: S.wRole.tombstones.has(Y) }; return origHash(b, d); };
await S.trigger();
const bound = S.logs(S.Ln, 'merge-bound')[0]?.ctx;
check('1 both are in the export: header bodies 1, dels 1 (no margin at the cut)', !!bound && bound.bodies === 1 && bound.dels === 1, JSON.stringify(bound));
const acked = S.logs(S.W, 'merge-acked')[0]?.ctx;
check('2 both retained at W at the ack (full) although W\'s clock is past their TTL', !!acked && acked.full === true && acked.retained === 1 && acked.delsActive === 1, JSON.stringify(acked));
check('3 W\'s expiry sweep during the round removed neither (pins exempt)', sweptDuring?.body === true && sweptDuring?.tomb === true, JSON.stringify(sweptDuring));
check('4 L demoted on the full ack; completion closed the round', S.lRole.isRoot === false && S.logs(S.W, 'merge-complete').length === 1);
S.W.am._expireCache(S.wRole, S.W.am._now());
check('5 after completion the pins are released and W\'s own TTL applies: the body expires on the next sweep', S.wRole.merge.pins.size === 0 && !S.wRole.cacheIds.has(B));

console.log(`\nfence_merge_ttl_live_at_cut: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
