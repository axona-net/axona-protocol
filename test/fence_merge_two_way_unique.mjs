#!/usr/bin/env node
// =====================================================================
// fence_merge_two_way_unique.mjs — dual-root packet 4(d), the whole
// exchange end to end (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// W HOLDS {a, tomb(x)}, L HOLDS {b, tomb(y)}. The split is that L does not
// hear W. L's self-verify then visits W (the (b) fix) and names it; with
// custody on a merge-capable node that is a CANDIDATE, not a demotion, and
// the machine runs: OFFER L→W, PULLUP W→L with corr, the bind (cut + gate),
// the header, the frames with their cursors, the terminal REPLAYACK, step 5
// at L with the release token, the merge-bearing SUB, MERGE_CLOSED. At the
// end W holds a and b, x and y are tombstoned at W, L is W's child, and L
// converges to W's state by the shipped since-all replay.
//
// Checks: (1) the trigger recorded a candidate and sent ONE offer; (2) W
// pulled under the offer's corr and L bound (cut + gate; header with two
// manifest entries); (3) W admitted on the header, pinned its baseline;
// (4) the frames arrived in cursor order and W acked FULL (retained 1,
// delsActive 1, hash matching); (5) step 5: L demoted with the token,
// completing set, the gate cleared, L's export pinned; (6) the
// merge-bearing SUB completed the handover at W: MERGE_CLOSED, pins
// released, completing cleared at L; (7) UNION: W holds a and b minus x
// and y (x, y tombstoned at W); (8) L as child converges: holds a, lacks
// x (suppressed); (9) exactly one root remains.
//
// Mutant (red): drop L's dels from the frames (export bodies only) → 4
// (short ack: delsActive 0) and 7 fail.
// Run: node test/fence_merge_two_way_unique.mjs
// =====================================================================
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';
import { realTopic, Fabric, freshId, signedPub, signedKill, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const logs = (n, ev) => n.logs.filter((l) => l.ev === `pubsub:${ev}`);

console.log('fence_merge_two_way_unique: 4(d) — the exchange end to end; W ends with the union');

const L = await realTopic('two-way-unique');
const fab = new Fabric();
const a = await freshId(), b = await freshId();
const [wId, lId] = ((a ^ L.topicId) < (b ^ L.topicId)) ? [a, b] : [b, a];
const W = fab.addNode(wId); const Ln = fab.addNode(lId);
W.am._announceRoot = () => {};                                   // the split: L does not hear W
// W's lookup from L visits W; W's own lookup visits only itself.
Ln.am.dht.lookup = async () => ({ path: [idHex(lId), idHex(wId)], hops: 1, time: 1, found: false });
const wRole = W.rc.become(L.topicId, 'sub-terminal');
const lRole = Ln.rc.become(L.topicId, 'sub-terminal');
const PUBm = { targetId: null, isTerminal: true, hopCount: 2, fromId: idHex(L.publisher) };
async function stamp(node, k) {
  fab.clock += 10;
  const w = await signedPub({ clock: { t: fab.clock } }, L.desc, k);
  await node.handlers.get('pubsub:pub')({ topicId: idHex(L.topicId), json: w.json }, { ...PUBm, targetId: node.id });
  await fab.settle();
  return w.msgId;
}
async function kill(node, msgId) {
  fab.clock += 10;
  const k = await signedKill({ clock: { t: fab.clock } }, L.topicId, msgId, 1);
  await node.handlers.get('pubsub:kill')({ topicId: idHex(L.topicId), kill: k }, { ...PUBm, targetId: node.id });
  await fab.settle();
}
// W: a, x then kill(x). L: b, y then kill(y).
const A = await stamp(W, 1), X = await stamp(W, 2); await kill(W, X);
const B = await stamp(Ln, 3), Y = await stamp(Ln, 4); await kill(Ln, Y);
const setupOk = wRole.cacheIds.has(A) && wRole.tombstones.has(X) && !wRole.cacheIds.has(X) && lRole.cacheIds.has(B) && lRole.tombstones.has(Y) && wRole.isRoot && lRole.isRoot;
if (!setupOk) { console.log('setup failed', { w: [...wRole.cacheIds], wt: [...wRole.tombstones.keys()], l: [...lRole.cacheIds], lt: [...lRole.tombstones.keys()] }); process.exit(1); }

// THE TRIGGER: L's self-verify visits W.
lRole.formedAt = fab.clock - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
Ln.am._verifyRoots(fab.clock);
for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
await fab.settle();
const offers = Ln.routed.filter((r) => r.type === 'pubsub:mergeoffer');
check('1 the verify recorded W as the candidate and L sent ONE offer', lRole.merge?.candidate?.id === idHex(wId) && offers.length === 1 && logs(Ln, 'merge-offer').length === 1, JSON.stringify({ cand: lRole.merge?.candidate, offers: offers.length }));
const corr = offers[0]?.payload?.corr;
const pulls = W.routed.filter((r) => r.type === 'pubsub:pullup' && r.payload.corr === corr);
check('2 W pulled under the offer\'s corr; L bound: cut + gate, header with one body and one del', pulls.length >= 1 && logs(Ln, 'merge-cut').length === 1 && logs(Ln, 'merge-bound').length === 1 && logs(Ln, 'merge-bound')[0].ctx.bodies === 1 && logs(Ln, 'merge-bound')[0].ctx.dels === 1, JSON.stringify({ pulls: pulls.length, bound: logs(Ln, 'merge-bound').map((l) => l.ctx) }));
check('3 W admitted on the header and pinned its baseline', logs(W, 'merge-admitted').length === 1 && logs(W, 'merge-admitted')[0].ctx.baselineN === 1 && logs(W, 'refused-capacity').length === 0);
const acked = logs(W, 'merge-acked')[0];
check('4 the frames arrived in cursor order and W acked FULL (retained 1, delsActive 1, hash matched)', !!acked && acked.ctx.full === true && acked.ctx.retained === 1 && acked.ctx.delsActive === 1 && logs(W, 'merge-cursor-invalid').length === 0 && logs(Ln, 'merge-cursor-invalid').length === 0, JSON.stringify(acked?.ctx));
const step5 = logs(Ln, 'merge-step5')[0];
check('5 step 5 at L: demoted with the token, gate cleared, export pinned, no late flip', !!step5 && lRole.isRoot === false && (Ln.am._upstream.get(L.topicId) || [])[0] === idHex(wId) && lRole.merge.gate === null && logs(Ln, 'merge-late-flip').length === 0 && logs(Ln, 'custody-refused').length === 0, JSON.stringify({ step5: step5?.ctx, isRoot: lRole.isRoot, refused: logs(Ln, 'custody-refused').length }));
check('6 the merge-bearing SUB completed the handover: MERGE_CLOSED at L, pins released at W, completing cleared', logs(W, 'merge-complete').length === 1 && logs(Ln, 'merge-closed').length === 1 && lRole.merge.completing === null && wRole.merge.pins.size === 0 && wRole.sync.openPulls.size === 0 && lRole.sync.openMerges.size === 0, JSON.stringify({ complete: logs(W, 'merge-complete').length, closed: logs(Ln, 'merge-closed').length, stale: logs(W, 'merge-stale').map((l) => l.ctx.why) }));
check('7 UNION at W: holds a and b; x and y tombstoned, neither cached', wRole.cacheIds.has(A) && wRole.cacheIds.has(B) && wRole.tombstones.has(X) && wRole.tombstones.has(Y) && !wRole.cacheIds.has(X) && !wRole.cacheIds.has(Y), JSON.stringify({ w: [...wRole.cacheIds].map((s) => s.slice(0, 6)), wt: [...wRole.tombstones.keys()].map((s) => s.slice(0, 6)) }));
check('8 L as W\'s child converges by the shipped replay: holds a, and x stays suppressed', lRole.cacheIds.has(A) && lRole.cacheIds.has(B) && !lRole.cacheIds.has(X) && lRole.tombstones.has(X), JSON.stringify({ l: [...lRole.cacheIds].map((s) => s.slice(0, 6)), lt: [...lRole.tombstones.keys()].map((s) => s.slice(0, 6)) }));
check('9 exactly one root remains', wRole.isRoot === true && lRole.isRoot === false);

console.log(`\nfence_merge_two_way_unique: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
