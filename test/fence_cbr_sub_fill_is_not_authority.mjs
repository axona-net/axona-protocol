#!/usr/bin/env node
// =====================================================================
// fence_cbr_sub_fill_is_not_authority.mjs — Closer-Born Relay Inherits the
// Root v0.4, Parts 3e and I3 (axona-docs e38601e; David "go ahead with the
// second fence" 2026-11-03).
//
// Live case (2026-11-03, ops/evidence/council-split-20261102): an
// axona.chat client holding council's history as a root at epoch 1 reached
// slot 19, the closer relay that is born root and empty on every fresh
// read. Slot 19 ended up holding 79 bodies with no merge-admitted row and a
// timed-out merge pull, then served them as a second root while Aster's
// seat stayed root with the live stream. Fence 1 showed the merge pull keeps
// nothing on a timeout, so the fill came another way.
//
// Mechanism under test (wireHandlers.js _onSub): a SUB whose hw is above
// the root's own makes the root pull the delta from that SUBSCRIBER
// (REPLAY_UP), with no log line, no corr and no terminal ack.
//
// Scene: W (closest) born root empty; A (farther) root, stamps three, the
// live root; C (client) root holding a copy of A's three. The A<->W link is
// cut both ways, as across the islands. C sends W a SUB carrying C's hw.
// Then A stamps a fourth.
// Invariant asserted once 3e lands: W does not become an independent
// serving root on a subscriber's fill; either W stages and hands the role
// back to the history's root, or the later body reaches W.
// Measured on v4.128.0 (9e66eb5), 2026-11-03: W 0 -> 3 bodies by the SUB
// path with no merge or split-union line (check 0 holds: the live slot-19
// fill reproduced); A stamps a fourth, W never receives it, both stay root
// (check 1 RED). EXPECTED RED until 3e; not in test/manifest.json until green.
// Run: node test/fence_cbr_sub_fill_is_not_authority.mjs
// =====================================================================
import { splitScene, Fabric, freshId, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
console.log('fence_cbr_sub_fill_is_not_authority: 3e/I3 — a subscriber\'s REPLAY_UP fill never makes a second serving root');

const S = await splitScene('cbr-sub-fill');
const A = S.Ln, aRole = S.lRole, W = S.W, wRole = S.wRole;   // A = the live root (farther), W = closer, born empty
const a1 = await S.stamp(A, 1), a2 = await S.stamp(A, 2), a3 = await S.stamp(A, 3);

// C: a client holding a copy of A's history as its own root (the chat tab, epoch 1)
let cId; do { cId = await freshId(); } while ((cId ^ S.Lt.topicId) < (S.wId ^ S.Lt.topicId));
const C = S.fab.addNode(cId);
const cRole = C.rc.become(S.Lt.topicId, 'sub-terminal');
for (const e of aRole.cache) { cRole.cache.push({ ...e }); cRole.cacheIds.add(e.msgId); }

// The islands: nothing passes between A and W in either direction.
S.fab.tap = (j) => {
  const from = j.meta?.fromId ? BigInt('0x' + j.meta.fromId) : null;
  if ((from === S.lId && j.dest === S.wId) || (from === S.wId && j.dest === S.lId)) return null;
  return j;
};

const before = { w: wRole.cache.length, merges: W.logs.filter((l) => /merge|split-union/.test(l.ev)).length };
const subHw = C.am._highWater(cRole), subLw = C.am._lowWater(cRole);
await W.handlers.get('pubsub:sub')({ topicId: idHex(S.Lt.topicId), via: [], subscriberId: idHex(cId), since: 0, hw: subHw, lw: subLw }, { targetId: S.Lt.topicId, isTerminal: true, hopCount: 1, fromId: idHex(cId) });
await S.fab.settle();
const filled = [a1, a2, a3].filter((id) => wRole.cacheIds.has(id)).length;
const mergeLines = W.logs.filter((l) => /merge|split-union/.test(l.ev)).length - before.merges;
console.log(`  probe: W cache ${before.w} -> ${wRole.cache.length}, A's bodies at W ${filled}/3, new merge/split-union lines at W ${mergeLines}, W isRoot ${wRole.isRoot}`);

const a4 = await S.stamp(A, 4);
console.log(`  probe: after A stamps a fourth: A cache ${aRole.cache.length} root ${aRole.isRoot} | W cache ${wRole.cache.length} root ${wRole.isRoot} has a4 ${wRole.cacheIds.has(a4)}`);

check('0 scene: the fill happened by the subscriber path, silently (A\'s three at W, no merge or split-union line)', filled === 3 && mergeLines === 0, `filled=${filled} mergeLines=${mergeLines}`);
check('1 W is not an independent serving root over a subscriber-filled copy while A stamps', !(wRole.isRoot && aRole.isRoot && filled > 0 && !wRole.cacheIds.has(a4)), 'two roots; W serves a frozen copy');
console.log(`\nfence_cbr_sub_fill_is_not_authority: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
