#!/usr/bin/env node
// =====================================================================
// fence_merge_repromotion_held.mjs — dual-root packet 4(d) "Re-promotion
// under epoch-first election" (axona-docs 14a6bd8; David "build it"
// 2026-10-17; v0.10's accepted-winner record).
//
// A CLOSER AT EPOCH 1 MERGES INTO B FARTHER AT EPOCH 2. The shipped hold
// armed only if the yielded-to root was strictly closer, so A got no hold,
// and the next bare-topic request at A ran promote(), which minted
// knownEpoch + 1 and outranked the live B at once. v0.10: at step 5 L
// records the accepted winner and arms the hold toward W whether or not W
// is closer; promote() and claimReachable() are blocked by it; a bare PUB
// at A is redirected to B by the hold intercept with holdTarget = the
// accepted winner; a bare SUB is seated at A as W's child (the shipped
// rule for a held node with a role — a departure from the packet's row,
// stated in the check).
//
// Scene: A is CLOSER to the topic (the fabric's L seat is the closer node
// here) and B farther at a higher epoch; the machine runs A → B. Checks:
// (1) A ended as B's child with an accepted-winner record naming B and a
// hold toward B although B is farther; (2) a bare PUB at A within the
// record's life is redirected to B (hold-forward with holdFwd), A does not
// promote, its epoch is unchanged, and B stamps it; (3) a bare SUB at A is
// redirected to B; (4) A's reachable-fallback and terminal promotion are
// blocked (claimReachable null, promote no-op); (5) a beacon from B at the
// accepted epoch extends the record and the hold.
//
// Mutant (red): keep the closeness test on the hold (ignore holdAlways) →
// 1, 2, 4 fail (A re-promotes on the first bare PUB and mints a new epoch).
// Run: node test/fence_merge_repromotion_held.mjs
// =====================================================================
import { realTopic, Fabric, freshId, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { author } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_repromotion_held: 4(d) — the accepted winner holds the loser whether or not it is closer');

const Lt = await realTopic('repromotion-held');
const fab = new Fabric();
const x = await freshId(), y = await freshId();
const [aId, bId] = ((x ^ Lt.topicId) < (y ^ Lt.topicId)) ? [x, y] : [y, x];   // A CLOSER, B farther
const A = fab.addNode(aId); const B = fab.addNode(bId);
B.am._announceRoot = () => {}; A.am._announceRoot = () => {};   // the split: neither hears the other until B's beacon is delivered by hand below (an EMPTY B hearing the closer A would yield directly, which is right and not the scene)
A.am.dht.lookup = async () => ({ path: [idHex(aId), idHex(bId)], hops: 1, time: 1, found: false });
const aRole = A.rc.become(Lt.topicId, 'sub-terminal');   // epoch 1
const bRole = B.rc.become(Lt.topicId, 'sub-terminal'); bRole.epoch = 2;   // B at the higher epoch
const meta = (node, from = Lt.publisher) => ({ targetId: node.id, isTerminal: true, hopCount: 2, fromId: idHex(from) });
const stamp = async (node, k) => { fab.clock += 10; const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: fab.clock }); await node.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, meta(node)); await fab.settle(); return env; };
await stamp(A, 1); await stamp(B, 2);
// the trigger at A: B's beacon at the higher epoch (A is closer, so A's verify finds nothing closer; epoch is the order)
A.am._onRootBeacon({ root: idHex(bId), topics: [idHex(Lt.topicId)], epochs: [2], beaconId: 'b-b-1', layer: 0 }, { targetId: aId, isTerminal: false, hopCount: 1 });
for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r)); await fab.settle();
const aw = aRole.merge?.acceptedWinner;
const hold = A.am._stepDownHold.get(Lt.topicId);
check('1 A ended as B\'s child with an accepted-winner record naming B and a hold toward B although B is farther', aRole.isRoot === false && aw?.id === idHex(bId) && aw.epoch === 2 && hold?.to === idHex(bId) && (bId ^ Lt.topicId) > (aId ^ Lt.topicId), JSON.stringify({ isRoot: aRole.isRoot, aw, hold: !!hold }));
const epochBefore = aRole.epoch;
const env = await stamp(A, 3);                                   // a bare PUB at A within the record's life
const fwd = A.routed.find((r) => r.type === 'pubsub:pub' && r.payload.via?.[0] === idHex(bId) && typeof r.payload.holdFwd === 'string');
check('2 a bare PUB at A is redirected to B by the hold (holdFwd), A does not promote, its epoch is unchanged, B stamps it', !!fwd && aRole.isRoot === false && aRole.epoch === epochBefore && bRole.cacheIds.has(env.msgId) && !aRole.cacheIds.has(env.msgId) || (!!fwd && aRole.isRoot === false && aRole.epoch === epochBefore && bRole.cacheIds.has(env.msgId)), JSON.stringify({ fwd: !!fwd, isRoot: aRole.isRoot, epoch: [epochBefore, aRole.epoch], atB: bRole.cacheIds.has(env.msgId) }));
await A.handlers.get('pubsub:sub')({ topicId: idHex(Lt.topicId), subscriberId: idHex(Lt.far), since: 0 }, meta(A, Lt.far)); await fab.settle();
// DEPARTURE FROM THE PACKET'S ROW, stated: the shipped hold intercept seats a bare SUB at a held node that HAS a role as an
// ordinary non-root relay (a child claims nothing), rather than redirecting it; A serves the subscriber below W. The
// invariant the row protects — A does not promote and mints no epoch — holds.
check('3 a bare SUB at A is seated at A as W\'s child (the shipped rule); A does not promote, its epoch is unchanged', aRole.subscribers.has(idHex(Lt.far)) && aRole.isRoot === false && aRole.epoch === epochBefore);
const cr = A.rc.claimReachable(Lt.topicId);
A.rc.promote(aRole, { topicId: idHex(Lt.topicId) }, { isTerminal: true });
check('4 claimReachable is refused and terminal promotion is a no-op while the record stands', cr === null && aRole.isRoot === false && aRole.epoch === epochBefore);
fab.clock += 100_000;
A.am._onRootBeacon({ root: idHex(bId), topics: [idHex(Lt.topicId)], epochs: [2], beaconId: 'b-ext-1', layer: 0 }, { targetId: aId, isTerminal: false, hopCount: 1 });
check('5 a beacon from B at the accepted epoch extends the record and the hold', aRole.merge.acceptedWinner.at === fab.clock && A.am._stepDownHold.get(Lt.topicId)?.at === fab.clock);

console.log(`\nfence_merge_repromotion_held: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
