#!/usr/bin/env node
// =====================================================================
// fence_topic_late_renewal.mjs — THE LATE RENEWAL and the whole framework
// coming down, on a fabric of real managers routed by XOR closeness (Topic
// Lifetime 4.5, 4.8; axona-docs architecture/Axona-Topic-Lifetime-v0.1.md
// v0.9 @ 11288d7; David 2026-10-20: "It is possible that a child node may
// renew while the system is being shut down and before it receives the
// termination message. This re-subscription may regenerate the root on some
// node, but this too will time out quickly and the topic infrastructure will
// be trivially removed in the next termination pass.").
//
// Scene: a publisher stamps one body at the root R; R replicates to its two
// cohort backups B1 and B2, which seat at R as standbys; a reader A seats at
// R. A unsubscribes; the body expires; R has no body and no demand and after
// one DROP window dismantles: TOPIC_TERMINATED to B1, B2; both drop. A late
// standby SUB from B2, already in flight when the notice lands, regenerates
// an empty root at R with one standby seat; that seat lapses at DROP_MS and
// the regenerated root terminates by the same rule, with nothing to notify.
// After that no node holds a role, a seat, a tombstone or a record for the
// topic. Bounds (4.8): everything delivered → gone within one DROP window
// and a few ticks; the regenerated root → one more DROP window and two ticks.
// Run: TOPIC_LIFETIME=1 node test/fence_topic_late_renewal.mjs
// =====================================================================
import { realTopic, Fabric, freshId, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { DROP_MS, TTL_MS, T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };
const TICK = 5_000;

console.log('fence_topic_late_renewal: the framework comes down, a late renewal regenerates a root, and that goes too');

const L = await realTopic('lifetime-late-renewal');
const fab = new Fabric();
// four real ids, sorted by closeness to the topic: R (root), B1, B2 (its cohort), A (a reader, farthest)
const ids = [];
for (let i = 0; i < 4; i++) ids.push(await freshId());
ids.sort((a, b) => { const da = a ^ L.topicId, db = b ^ L.topicId; return da < db ? -1 : da > db ? 1 : 0; });
const [rId, b1Id, b2Id, aId] = ids;
const opts = { topicLifetime: true, rootReplicas: 2, mergeCapable: false };
const R = fab.addNode(rId, opts), B1 = fab.addNode(b1Id, opts), B2 = fab.addNode(b2Id, opts), A = fab.addNode(aId, opts);
for (const n of [R, B1, B2, A]) { n.am._mono = () => fab.clock; n.am._announceRoot = () => {}; }
const roles = () => [R, B1, B2, A].map((n) => n.am.axonRoles.has(L.topicId) ? 1 : 0);
const tickAll = async (dt = TICK) => { fab.clock += dt; for (const n of [R, B1, B2, A]) if (n.alive) await n.am.refreshTick(); await fab.settle(); };

console.log('\n[1] the tree: A reads at R; one body; R replicates to B1 and B2; they seat at R as standbys');
A.am.pubsubSubscribe(L.topicId); await fab.settle();
await tickAll();
const env = await buildEnvelope({ topic: L.desc, message: { k: 1 }, seq: 1, identity: author, ts: fab.clock });
A.am.pubsubPublish(L.topicId, JSON.stringify(env)); await fab.settle();
await tickAll(); await tickAll();
const rRole = R.am.axonRoles.get(L.topicId);
check('R is root with the body; A is seated as a reader', rRole?.isRoot && rRole.cache.length === 1 && rRole.subscribers.get(idHex(aId).toLowerCase())?.kind === 'reader');
check('B1 and B2 are backups of R, appointed by a gen-bearing grant', [B1, B2].every((n) => n.am.axonRoles.get(L.topicId)?.backupOf === idHex(rId).toLowerCase() && n.am.axonRoles.get(L.topicId)?.standbyGen?.root === idHex(rId).toLowerCase()));
check('…and seated at R as standbys: demand at R reads 1 (A alone)', [b1Id, b2Id].every((b) => rRole.subscribers.get(idHex(b).toLowerCase())?.kind === 'standby') && R.am._roleDemand(rRole) === 1);

console.log('\n[2] A deletes its message and leaves: R holds only a tombstone, demand 0; after one DROP window R dismantles and B1 and B2 drop the generation, tombstone included');
const kill = await buildKill({ topicId: idHex(L.topicId), msgId: env.msgId, ts: fab.clock, seq: 2, identity: author });
A.am.pubsubKill(L.topicId, kill); await fab.settle();
await tickAll();
check('the kill landed: no body at R, one tombstone at R and at both backups', rRole.cache.length === 0 && rRole.tombstones.size === 1 && [B1, B2].every((n) => n.am.axonRoles.get(L.topicId)?.tombstones.size === 1));
A.am.pubsubUnsubscribe(L.topicId); await fab.settle();
check('A\'s UNSUB dropped its seat: demand 0; R stands on its standby seats', R.am._roleDemand(rRole) === 0 && roles().join('') === '1110');
let ticks = 0;
while (!rRole.dismantling && ticks < 60) { await tickAll(); ticks++; }
check(`R began dismantling within one DROP window of A's last renewal (+ ticks): ${ticks} ticks`, !!rRole.dismantling && ticks * TICK <= DROP_MS + 2 * TICK, ticks);
await tickAll();
check('B1 and B2 received the notice and dropped the generation: no role, no backup duty, no record, the tombstone gone with it', [B1, B2].every((n) => !n.am.axonRoles.has(L.topicId) && !n.am._backupTopics.has(L.topicId)) && [B1, B2].every((n) => n.logs.some((l) => l.ev === 'pubsub:role-terminated' && l.ctx.tombstonesDropped === 1)));
let gone = 0;
while (R.am.axonRoles.has(L.topicId) && gone < 10) { await tickAll(); gone++; }
check('R\'s role is gone after its rounds', !R.am.axonRoles.has(L.topicId) && R.am.inspectAdmission().reaped.terminated === 1);
check('nothing anywhere: no role, no seat, no tombstone, no LEFT record, no standby duty', roles().join('') === '0000' && [R, B1, B2, A].every((n) => n.am._leftRecords.size === 0 && n.am._backupTopics.size === 0));

console.log('\n[3] THE LATE RENEWAL: a standby SUB from B2 lands at R after R\'s role is gone and regenerates an empty root with one seat');
// B2 as it was: a backup that has not yet heard the notice, renewing its standby seat
const late = { topicId: idHex(L.topicId), via: [], subscriberId: idHex(b2Id), since: 0, hw: 0, lw: 0, latest: false, standby: true, gen: { root: idHex(rId).toLowerCase(), epoch: 1 } };
await R.handlers.get(T.SUB)(late, { targetId: rId, isTerminal: true, hopCount: 1, fromId: idHex(b2Id) });
await fab.settle();
const rr = R.am.axonRoles.get(L.topicId);
check('a new, empty root at R: epoch above the ended generation, one standby seat, demand 0', rr?.isRoot && rr.epoch === 2 && rr.cache.length === 0 && rr.subscribers.size === 1 && R.am._roleDemand(rr) === 0, rr && `epoch=${rr.epoch} seats=${rr.subscribers.size}`);
const regenAt = fab.clock;
let t2 = 0;
while (R.am.axonRoles.has(L.topicId) && t2 < 60) { await tickAll(); t2++; }
check(`the regenerated root ends by the same rule within one DROP window and two ticks: ${t2} ticks, nothing to notify`, !R.am.axonRoles.has(L.topicId) && t2 * TICK <= DROP_MS + 2 * TICK && R.am.inspectAdmission().reaped.terminated === 2, t2);
check('B2 was sent no second notice (its seat was the only target and it never renewed again… so one round went to it)', R.logs.filter((l) => l.ev === 'pubsub:topic-terminated-sent').length >= 1);
check('after the pass nothing is referenced anywhere', roles().join('') === '0000' && [R, B1, B2, A].every((n) => n.am._leftRecords.size === 0));
void regenAt;

console.log(`\nfence_topic_late_renewal: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
