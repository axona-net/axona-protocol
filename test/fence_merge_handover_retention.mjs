#!/usr/bin/env node
// =====================================================================
// fence_merge_handover_retention.mjs — dual-root packet 4(d) "W does not
// close the round when it SENDS the ack" (axona-docs 14a6bd8; David "build
// it" 2026-10-17).
//
// The ack's delivery to L is delayed; meanwhile W's clock passes an
// exported entry's TTL expiry and W's sweep runs. W must still hold every
// manifest entry when L releases (step 5) and until MERGE_CLOSED, because
// its pins stay from admission to completion, not to the ack's send.
//
// Checks: (1) with the REPLAYACK held back, W's record is `acked` with its
// pins intact; (2) W's expiry sweep past the entry's TTL (by W's clock)
// removes nothing pinned; (3) the ack released: L flips (step 5) and W still
// holds every manifest entry; (4) the merge-bearing SUB completes:
// MERGE_CLOSED, pins released; (5) only now does W's TTL apply.
//
// Mutant (red): release pins at ack send → 2 fails (the entry expires
// before L flips).
// Run: node test/fence_merge_handover_retention.mjs
// =====================================================================
import { TTL_MS } from '../src/pubsub/constants.js';
import { splitScene } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_handover_retention: 4(d) — pins hold from admission to completion, not to the ack\'s send');

const S = await splitScene('handover-retention');
await S.stamp(S.W, 1);
const B = await S.stamp(S.Ln, 2), Y = await S.stamp(S.Ln, 3); await S.kill(S.Ln, Y);
let heldAck = [];
S.fab.tap = (j) => { if (j.type === 'pubsub:replayack') { heldAck.push(j); return null; } return j; };
await S.trigger();
const corr = S.frames('pubsub:mergeoffer')[0].payload.corr;
const p = S.wRole.sync.openPulls.get(corr);
check('1 with the ack held back, W\'s record is acked with its pins intact', !!p && p.state === 'acked' && S.wRole.merge.pins.has(B) && S.wRole.cacheIds.has(B) && heldAck.length === 1, JSON.stringify({ state: p?.state, pins: S.wRole.merge.pins.size }));
// W's clock passes the entry's TTL and its sweep runs
const wNow = S.W.am._now; S.W.am._now = () => wNow() + TTL_MS + 60_000;
S.W.am._expireCache(S.wRole, S.W.am._now());
S.W.am._expireTombstones(S.wRole, S.W.am._now());
check('2 W\'s sweep past the TTL (by W\'s clock) removes nothing pinned', S.wRole.cacheIds.has(B) && S.wRole.tombstones.has(Y));
// the ack reaches L (L's clock is the fabric clock; W's offset is W's alone)
S.fab.tap = null; for (const j of heldAck) S.fab.queue.push(j); heldAck = [];
await S.fab.settle();
check('3 the ack released: L flipped (step 5) and W still holds every manifest entry', S.lRole.isRoot === false && S.logs(S.Ln, 'merge-step5').length === 1 && S.wRole.cacheIds.has(B) && S.wRole.tombstones.has(Y));
check('4 the merge-bearing SUB completed: MERGE_CLOSED at L, pins released at W', S.logs(S.W, 'merge-complete').length === 1 && S.logs(S.Ln, 'merge-closed').length === 1 && S.wRole.merge.pins.size === 0);
S.W.am._expireCache(S.wRole, S.W.am._now());
check('5 only now does W\'s TTL apply to the entry', !S.wRole.cacheIds.has(B));

console.log(`\nfence_merge_handover_retention: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
