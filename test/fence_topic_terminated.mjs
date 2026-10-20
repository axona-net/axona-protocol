#!/usr/bin/env node
// =====================================================================
// fence_topic_terminated.mjs — TERMINATION (Topic Lifetime 4.5; axona-docs
// architecture/Axona-Topic-Lifetime-v0.1.md v0.9 @ 11288d7; David "Build"
// 2026-10-20) and DEMAND, counted once (4.3).
//
// A root terminates its generation when, on one tick, its cache holds no
// body, no custody is open, there is no local intent, and demand has been
// zero for DROP_MS. Tombstones do not hold a topic open. `dismantling` is
// installed synchronously BEFORE the first TOPIC_TERMINATED; from then the
// role grants no seat, takes no write, replicates nothing and beacons
// nothing; the notice goes to every seat and replica once per tick for up to
// three ticks; the role goes on the tick after its last send, tombstones with
// it. On receipt a backup drops the generation, a backup holding a BODY does
// not obey, a child forwards then retires, a reader's subscription ENDS with
// exactly one `{ terminated, generation }` row.
//
// Mutants (red): make a standby SUB stamp lastDemandAt → T1/T2 (never
// terminates); drop `if (role.dismantling) return false` from _cutHolds → T6;
// read _custodyAlive instead of _custodyOpen in the sweep → T4 (a tombstone
// holds it open); drop the holds-body exception in _onTopicTerminated → R2;
// let a dismantling role grant a seat → T7; drop the generation binding → R5.
// Run: TOPIC_LIFETIME=1 node test/fence_topic_terminated.mjs
// =====================================================================
import { manager, seed, sub, replicate, terminated, deliver, attachNonceFor, gen, meta, idHex, lc, TP, NODE, SELF, T0, MIN, T } from './lib/lifetimeFixture.mjs';
import { DROP_MS, TOPIC_TERMINATE_TRIES, TTL_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };
const B1 = NODE(1), B2 = NODE(2), R1 = NODE(3), R2 = NODE(4), C1 = NODE(5);
const TICK = 5_000;

console.log('fence_topic_terminated: a root with no body and no demand dismantles its framework');

console.log('\n[T1] demand counted once: standby 0, child n, bare SUB 1 — only demand stamps lastDemandAt');
{
  const ctx = manager();
  const role = seed(ctx, TP(1));
  await sub(ctx, TP(1), B1, { standby: true, gen: gen(SELF) });
  check('a standby SUB seats at kind=standby and demand reads 0', role.subscribers.get(lc(idHex(B1)))?.kind === 'standby' && ctx.am._roleDemand(role) === 0);
  check('…and does not stamp lastDemandAt', role.lastDemandAt === T0);
  ctx.clock.t += 1000;
  await sub(ctx, TP(1), C1, { demand: 0 });
  check('a child declaring 0 contributes 0 and is not counted as a seat of demand', ctx.am._roleDemand(role) === 0 && role.lastDemandAt === T0);
  ctx.clock.t += 1000;
  await sub(ctx, TP(1), C1, { demand: 3 });
  check('a child declaring 3 contributes 3 and stamps', ctx.am._roleDemand(role) === 3 && role.lastDemandAt === T0 + 2000);
  ctx.clock.t += 1000;
  await sub(ctx, TP(1), R1);
  check('a bare SUB (an older kernel, an app) counts 1 and stamps', ctx.am._roleDemand(role) === 4 && role.lastDemandAt === T0 + 3000);
  check('a child is counted once: three seats, demand 4 (3 + 1 + 0)', role.subscribers.size === 3 && ctx.am._roleDemand(role) === 4);
}

console.log('\n[T2] a root with no demand and no bodies terminates after one window; the standbys are notified; the role goes after three rounds');
{
  const ctx = manager();
  const role = seed(ctx, TP(2));
  role.replicas.set(lc(idHex(B1)), { at: T0, via: 'consumed' });
  role.replicas.set(lc(idHex(B2)), { at: T0, via: 'consumed' });
  await sub(ctx, TP(2), B1, { standby: true, gen: gen(SELF) });
  await sub(ctx, TP(2), B2, { standby: true, gen: gen(SELF) });
  // standbys renew every tick: their SUBs must not hold the topic
  const renewStandbys = async () => { for (const b of [B1, B2]) await sub(ctx, TP(2), b, { standby: true, gen: gen(SELF) }); };
  for (let at = T0; at <= T0 + DROP_MS; at += TICK) { await renewStandbys(); await ctx.tick(at); }
  check('at exactly DROP_MS of zero demand the root still stands (strictly greater, like the seat lease)', ctx.held(TP(2)) && !role.dismantling);
  await renewStandbys();
  await ctx.tick(T0 + DROP_MS + TICK);
  const t1 = ctx.clock.t;
  check('one tick later termination begins: dismantling installed, logged with the idle age', !!role.dismantling && ctx.log('topic-terminating').length === 1 && ctx.log('topic-terminating')[0].ctx.idleMs > DROP_MS);
  const sent1 = ctx.sent(T.TOPIC_TERMINATED);
  check('round 1: TOPIC_TERMINATED to both standbys (seats ∪ replicas), naming the generation', sent1.length === 2 && sent1.every((r) => r.payload.gen.root === lc(idHex(SELF)) && r.payload.gen.epoch === 1 && r.at === t1), sent1.length);
  const subsBefore = ctx.sent(T.SUB).length;
  await renewStandbys();                                               // a standby whose notice is in flight renews once more
  check('a SUB at a dismantling role is refused (no seat renewed, logged)', ctx.log('seat-refused-dismantling').length === 2);
  await ctx.tick(t1 + TICK);
  await ctx.tick(t1 + 2 * TICK);
  check(`rounds 2 and 3 sent on the next two ticks (${TOPIC_TERMINATE_TRIES} tries)`, ctx.sent(T.TOPIC_TERMINATED).length === 2 * TOPIC_TERMINATE_TRIES && ctx.held(TP(2)));
  check('while dismantling nothing is replicated and nothing is beaconed', ctx.sent(T.REPLICATE).length === 0 && ctx.sent(T.ROOTBEACON).length === 0);
  await ctx.tick(t1 + 3 * TICK);
  check('the tick after the last send retires the role', !ctx.held(TP(2)) && ctx.log('topic-terminated').length === 1 && ctx.log('topic-terminated')[0].ctx.sends === TOPIC_TERMINATE_TRIES);
  check('counted as reaped.terminated; no record of the topic remains', ctx.am.inspectAdmission().reaped.terminated === 1 && ctx.am._leftRecords.size === 0 && !ctx.am._upstream.has(TP(2)));
  void subsBefore;
}

console.log('\n[T3] what holds a topic open: one renewing demand seat; one body; local intent; a metrics lease; open custody');
{
  const ctx = manager();
  const hold = async (label, t, arm) => {
    const role = seed(ctx, t);
    arm(role);
    for (let at = T0; at <= T0 + DROP_MS + 3 * TICK; at += TICK) { if (role._renew) await role._renew(); await ctx.tick(at); }
    check(`${label} holds the root past the window`, ctx.held(t) && !ctx.role(t).dismantling);
  };
  await hold('a reader seat that renews', TP(31), (r) => { r._renew = () => sub(ctx, TP(31), R1); });
  await hold('a child declaring one reader that renews', TP(32), (r) => { r._renew = () => sub(ctx, TP(32), C1, { demand: 1 }); });
  await hold('one body', TP(33), (r) => { r.cache.push({ msgId: 'b', publishTs: ctx.clock.t, json: '{}', bytes: 80 }); r.cacheIds.add('b'); });
  await hold('peer.sub() intent', TP(34), () => { ctx.am.mySubscriptions.set(TP(34), { since: 0, lastRenewSent: T0, interval: 60_000 }); });
  await hold('peer.host() intent', TP(35), () => { ctx.am._hostedTopics.add(TP(35)); });
  await hold('a live metrics lease', TP(36), (r) => { r.metricsOn = T0 + 24 * 60 * MIN; });
  await hold('an open merge (custody)', TP(37), (r) => { r.sync.openMerges.set('c1', {}); });
  await hold('an open pull (custody)', TP(38), (r) => { r.sync.openPulls.set('c2', {}); });
}

console.log('\n[T4] tombstones do not hold a topic open: a tombstone-only root terminates and drops them (David: no deletion record)');
{
  const ctx = manager();
  const role = seed(ctx, TP(4));
  role.tombstones.set('k1', { exp: T0 + TTL_MS, killTs: T0, signer: null, seq: 1 });
  role.tombstones.set('k2', { exp: T0 + TTL_MS, killTs: T0, signer: null, seq: 2 });
  for (let at = T0; at <= T0 + DROP_MS + TICK; at += TICK) await ctx.tick(at);
  check('with two active tombstones and nothing else the root begins termination after the window', !!role.dismantling || !ctx.held(TP(4)));
  await ctx.tick(ctx.clock.t + TICK);
  check('with no seat and no replica there is nobody to notify: the role goes on the next tick, tombstones with it', !ctx.held(TP(4)) && ctx.log('topic-terminated')[0]?.ctx.tombstonesDropped === 2 && ctx.sent(T.TOPIC_TERMINATED).length === 0);
}

console.log('\n[T5] a stamped write BEFORE the install cancels a termination that never began');
{
  const ctx = manager();
  const role = seed(ctx, TP(5));
  const standby = () => sub(ctx, TP(5), B1, { standby: true, gen: gen(SELF) });   // a standby seat, so only termination can end the role
  for (let at = T0; at <= T0 + DROP_MS - TICK; at += TICK) { await standby(); await ctx.tick(at); }
  role.lastDemandAt = ctx.clock.t;                                   // what _ingestPublish does at the stamp (fence_topic_lifetime_demand pins the real path)
  await standby(); await ctx.tick(T0 + DROP_MS + 2 * TICK);
  check('the window restarts from the write: no termination', ctx.held(TP(5)) && !role.dismantling && ctx.log('topic-terminating').length === 0);
  for (let at = ctx.clock.t + TICK; at <= T0 + DROP_MS - TICK + DROP_MS + 2 * TICK; at += TICK) { await standby(); await ctx.tick(at); }
  check('…and runs its full length from the write before termination begins', !!role.dismantling && ctx.log('topic-terminating')[0].ctx.idleMs > DROP_MS);
}

console.log('\n[T6] a write AFTER the install is refused at the cut and never ingested; the generation moved');
{
  const ctx = manager();
  const role = seed(ctx, TP(6));
  await sub(ctx, TP(6), B1, { standby: true, gen: gen(SELF) });
  const gen0 = role.gen | 0;
  for (let at = T0; at <= T0 + DROP_MS + TICK; at += TICK) { await sub(ctx, TP(6), B1, { standby: true, gen: gen(SELF) }); await ctx.tick(at); }
  check('dismantling is installed and the cut generation moved', !!role.dismantling && (role.gen | 0) === gen0 + 1);
  const cut = ctx.am._cutCapture(role);
  const ok = ctx.am._cachePush(role, { msgId: 'late', publishTs: ctx.clock.t, json: '{}' }, cut);
  check('a body write at the dismantling role is refused (cut-refused) and nothing is cached', ok === false && role.cache.length === 0 && ctx.log('cut-refused').length === 1);
  const okKill = ctx.am._applyKill(role, TP(6), { msgId: 'late', killTs: ctx.clock.t, signer: null, seq: 9 }, cut);
  check('a kill at the dismantling role is refused too; no tombstone written', okKill === false && role.tombstones.size === 0);
}

console.log('\n[T7] a dismantling root refuses seats and the subscriber renews into the next generation');
{
  const ctx = manager();
  const role = seed(ctx, TP(7));
  await sub(ctx, TP(7), B1, { standby: true, gen: gen(SELF) });
  for (let at = T0; at <= T0 + DROP_MS + TICK; at += TICK) { await sub(ctx, TP(7), B1, { standby: true, gen: gen(SELF) }); await ctx.tick(at); }
  check('dismantling', !!role.dismantling);
  await sub(ctx, TP(7), R1);
  check('a READER SUB at the dismantling role is refused and stamps no demand', !role.subscribers.has(lc(idHex(R1))) && ctx.log('seat-refused-dismantling').length === 1 && ctx.am._roleDemand(role) === 0);
  await ctx.tick(ctx.clock.t + TICK); await ctx.tick(ctx.clock.t + TICK); await ctx.tick(ctx.clock.t + TICK);
  check('the role is gone', !ctx.held(TP(7)));
  await sub(ctx, TP(7), R1);
  const next = ctx.role(TP(7));
  check('the reader\'s next renewal forms the NEXT generation at a clean role: epoch 2, one reader seat, demand 1', next && next.isRoot && next.epoch === 2 && next.subscribers.has(lc(idHex(R1))) && ctx.am._roleDemand(next) === 1 && !next.dismantling);
}

console.log('\n[T8] armed, a keyspace host\'s empty root with no demand ends by termination at DROP_MS, ahead of the 5-minute grace');
{
  const ctx = manager({ keyspace: true });
  const role = seed(ctx, TP(16));
  for (let at = T0; at <= T0 + DROP_MS; at += TICK) await ctx.tick(at);
  check('at DROP_MS the keyspace pin still holds it (no seat, no body)', ctx.held(TP(16)) && !role.dismantling);
  await ctx.tick(T0 + DROP_MS + TICK); await ctx.tick(T0 + DROP_MS + 2 * TICK);
  check('one tick past DROP_MS it terminates (nobody to notify) and is counted terminated, not empty', !ctx.held(TP(16)) && ctx.am.inspectAdmission().reaped.terminated === 1 && ctx.am.inspectAdmission().reaped.empty === 0);
}

console.log('\n[R1] receipt at a BACKUP: the appointment ends, tombstones go, the role goes; a duplicate notice ends nothing twice');
{
  const ctx = manager();
  await replicate(ctx, TP(8), R1, { gen: gen(R1), issuedAt: T0, dels: [{ del: true, msgId: 'd1', killTs: T0 - 1000, signer: null, seq: 1, publishTs: T0 - 1000 }] });
  const role = ctx.role(TP(8));
  check('a backup appointed by a gen-bearing REPLICATE holds the tombstone it was given', role && role.backupOf === lc(idHex(R1)) && role.tombstones.size === 1 && ctx.am._backupTopics.has(TP(8)));
  await terminated(ctx, TP(8), R1, gen(R1));
  check('TOPIC_TERMINATED from its principal: role gone, backup duty gone, tombstone dropped, no record', !ctx.held(TP(8)) && !ctx.am._backupTopics.has(TP(8)) && ctx.log('role-terminated').length === 1 && ctx.log('role-terminated')[0].ctx.tombstonesDropped === 1 && ctx.am._leftRecords.size === 0);
  await terminated(ctx, TP(8), R1, gen(R1));
  check('a duplicate notice ends nothing twice (no role, no second log, no throw)', !ctx.held(TP(8)) && ctx.log('role-terminated').length === 1);
}

console.log('\n[R2] a backup holding a BODY the root lacks does NOT obey (it elects by 4.4(1)); one holding only tombstones does');
{
  const ctx = manager();
  await replicate(ctx, TP(9), R1, { gen: gen(R1), issuedAt: T0 });
  const role = ctx.role(TP(9));
  role.cache.push({ msgId: 'b', publishTs: T0, json: '{}', bytes: 80 }); role.cacheIds.add('b');
  await terminated(ctx, TP(9), R1, gen(R1));
  check('with a body: the notice is ignored (holds-body), the role and the body stay', ctx.held(TP(9)) && role.cache.length === 1 && ctx.log('terminated-ignored')[0]?.ctx.why === 'holds-body');
}

console.log('\n[R3] receipt at a CHILD: forwarded to each reader seat, then the role goes; a standby seat is not forwarded to');
{
  const ctx = manager();
  const role = seed(ctx, TP(10), { isRoot: false });
  ctx.am._upstream.set(TP(10), [lc(idHex(R1))]);
  await sub(ctx, TP(10), C1, {}, { viaSelf: true }); await sub(ctx, TP(10), R2, {}, { viaSelf: true }); await sub(ctx, TP(10), B1, { standby: true }, { viaSelf: true });
  check('three seats on the child; it is still a child (a via-pinned SUB promotes nothing)', role.subscribers.size === 3 && !role.isRoot);
  const before = ctx.sent(T.TOPIC_TERMINATED).length;
  await terminated(ctx, TP(10), R1, gen(R1));
  const fwd = ctx.sent(T.TOPIC_TERMINATED).slice(before);
  check('two forwards, to the two readers, naming the same generation and from this node', fwd.length === 2 && fwd.every((f) => f.payload.gen.root === lc(idHex(R1)) && lc(f.payload.from) === lc(idHex(SELF))) && !fwd.some((f) => f.target === B1), fwd.length);
  check('the child role and its list are gone; upstream dropped', !ctx.held(TP(10)) && !ctx.am._upstream.has(TP(10)) && ctx.log('terminated-forwarded')[0]?.ctx.seats === 2);
}

console.log('\n[R4] receipt at a READER: exactly one terminated row, the subscription ENDS, no renewal follows');
{
  const ctx = manager();
  const rows = [];
  ctx.am.onPubsubDelivery((t, json) => rows.push({ t: idHex(t), body: JSON.parse(json) }));
  ctx.am.pubsubSubscribe(TP(11));
  ctx.am._upstream.set(TP(11), [lc(idHex(R1))]);
  const subsBefore = ctx.sent(T.SUB).length;
  await terminated(ctx, TP(11), R1, gen(R1));
  check('one row { terminated: true, generation } delivered to the app', rows.length === 1 && rows[0].body.terminated === true && rows[0].body.generation.root === lc(idHex(R1)) && rows[0].body.generation.epoch === 1, JSON.stringify(rows));
  check('the subscription is ended, not paused: mySubscriptions and the pin are gone', !ctx.am.mySubscriptions.has(TP(11)) && !ctx.am._upstream.has(TP(11)) && ctx.am.inspectAdmission().lifetime.subscriptionsEnded === 1);
  for (let i = 1; i <= 4; i++) await ctx.tick(ctx.clock.t + TICK);
  check('the kernel does not renew it', ctx.sent(T.SUB).length === subsBefore);
  await terminated(ctx, TP(11), R1, gen(R1));
  check('a duplicate notice delivers no second row', rows.length === 1);
}

console.log('\n[R5] the notice is bound to the generation: a foreign root\'s notice is ignored; a root ignores another root\'s notice');
{
  const ctx = manager();
  await replicate(ctx, TP(12), R1, { gen: gen(R1), issuedAt: T0 });
  await terminated(ctx, TP(12), R2, gen(R2));
  check('a backup of R1 ignores a notice from R2 (foreign generation)', ctx.held(TP(12)) && ctx.log('terminated-ignored')[0]?.ctx.why === 'foreign-generation');
  const mine = seed(ctx, TP(13));
  await terminated(ctx, TP(13), R1, gen(R1));
  check('a root ignores another root\'s notice for its topic (a split\'s halves each end by their own rule)', ctx.held(TP(13)) && mine.isRoot && ctx.log('terminated-ignored').some((r) => r.ctx.why === 'i-am-root'));
}

console.log('\n[B01] EXACT generation binding (Aster c5adedec): a delayed notice of an OLD generation ends nothing in the NEW one');
{
  const ctx = manager();
  // a backup of R at epoch 2 receives R's delayed notice for epoch 1
  await replicate(ctx, TP(17), R1, { gen: gen(R1, 2), issuedAt: T0 });
  await terminated(ctx, TP(17), R1, gen(R1, 1));
  check('a backup appointed at (R1, 2) ignores a notice naming (R1, 1): same root, old epoch', ctx.held(TP(17)) && ctx.am._backupTopics.has(TP(17)) && ctx.log('terminated-ignored').some((r) => r.ctx.why === 'foreign-generation'));
  await terminated(ctx, TP(17), R1, gen(R1, 2));
  check('…and obeys the notice naming (R1, 2)', !ctx.held(TP(17)));
  // a reader attached through its DELIVERs to (R1, 2) ignores the (R1, 1) notice from its upstream, then obeys (R1, 2)
  const rows = []; ctx.am.onPubsubDelivery((t, json) => rows.push(JSON.parse(json)));
  ctx.am.pubsubSubscribe(TP(18));
  await deliver(ctx, TP(18), R1, { gen: gen(R1, 2) });
  check('a DELIVER naming (R1, 2) pins the upstream and records the attachment', ctx.am._upstream.get(TP(18))[0] === lc(idHex(R1)) && ctx.am._attachedGen.get(TP(18))?.epoch === 2);
  await terminated(ctx, TP(18), R1, gen(R1, 1));
  check('the (R1, 1) notice from the same upstream ends nothing: subscription intact, no row', ctx.am.mySubscriptions.has(TP(18)) && rows.length === 0);
  await terminated(ctx, TP(18), R1, gen(R1, 2));
  check('the (R1, 2) notice ends it with one row', !ctx.am.mySubscriptions.has(TP(18)) && rows.length === 1 && rows[0].generation.epoch === 2);
  // a CHILD attached to (R1, 2) forwards the exact generation and ignores the old one
  const child = seed(ctx, TP(19), { isRoot: false });
  ctx.am._upstream.set(TP(19), [lc(idHex(R1))]);
  await deliver(ctx, TP(19), R1, { gen: gen(R1, 2) });
  await sub(ctx, TP(19), C1, {}, { viaSelf: true });
  await terminated(ctx, TP(19), R1, gen(R1, 1));
  check('the child attached to (R1, 2) ignores (R1, 1) and keeps its seat', ctx.held(TP(19)) && child.subscribers.size === 1);
  const before = ctx.sent(T.TOPIC_TERMINATED).length;
  await terminated(ctx, TP(19), R1, gen(R1, 2));
  const fwd = ctx.sent(T.TOPIC_TERMINATED).slice(before);
  check('…obeys (R1, 2) and forwards exactly that generation', !ctx.held(TP(19)) && fwd.length === 1 && fwd[0].payload.gen.epoch === 2);
  // a renewed reader: re-seated in the next generation, the old notice is harmless
  ctx.am.pubsubSubscribe(TP(20));
  await deliver(ctx, TP(20), R1, { gen: gen(R1, 1) });
  await deliver(ctx, TP(20), R1, { gen: gen(R1, 2) });
  await terminated(ctx, TP(20), R1, gen(R1, 1));
  check('a reader re-attached to (R1, 2) survives a late (R1, 1) notice', ctx.am.mySubscriptions.has(TP(20)));
  // an UNVERSIONED attachment (an older-kernel upstream named no generation) still binds by upstream
  ctx.am.pubsubSubscribe(TP(21));
  await deliver(ctx, TP(21), R1, {  });
  await terminated(ctx, TP(21), R1, gen(R1, 1));
  check('with no generation attached (older-kernel upstream) a notice from the upstream still ends it', !ctx.am.mySubscriptions.has(TP(21)));
}

console.log('\n[B04] attachment moves FORWARD only within a root (Aster 848688cb): a delayed older DELIVER cannot roll it back');
{
  const ctx = manager();
  const rows = []; ctx.am.onPubsubDelivery((t, json) => rows.push(JSON.parse(json)));
  ctx.am.pubsubSubscribe(TP(22));
  await deliver(ctx, TP(22), R1, { gen: gen(R1, 2) });
  await deliver(ctx, TP(22), R1, { gen: gen(R1, 1), authorized: false });
  check('a delayed (R1, 1) DELIVER after (R1, 2) leaves the attachment at epoch 2', ctx.am._attachedGen.get(TP(22))?.epoch === 2);
  await terminated(ctx, TP(22), R1, gen(R1, 1));
  check('the old (R1, 1) notice then ends nothing', ctx.am.mySubscriptions.has(TP(22)) && rows.length === 0);
  await deliver(ctx, TP(22), R2, { gen: gen(R2, 1) });
  check('a DIFFERENT root re-attaches whatever its epoch (no global order across roots)', ctx.am._attachedGen.get(TP(22))?.root === lc(idHex(R2)) && ctx.am._attachedGen.get(TP(22)).epoch === 1);
  // the same rule on a relay role
  const child = seed(ctx, TP(23), { isRoot: false });
  ctx.am._upstream.set(TP(23), [lc(idHex(R1))]);
  await deliver(ctx, TP(23), R1, { gen: gen(R1, 3) });
  await deliver(ctx, TP(23), R1, { msgs: [{ msgId: 'old', publishTs: T0, json: '{}', seq: 1 }], gen: gen(R1, 2), authorized: false });
  check('a relay keeps epoch 3 through a delayed epoch-2 DELIVER, and still caches that DELIVER\'s body', child.attachedGen.epoch === 3 && child.cache.length === 1);
  await terminated(ctx, TP(23), R1, gen(R1, 2));
  check('the old notice is foreign to it', ctx.held(TP(23)));
}

console.log('\n[B07] a root switch needs a reply to THIS node\'s own SUB (Aster 0cfdd82d): a delayed old DELIVER from a previous root is not a selection');
{
  const ctx = manager();
  const rows = []; ctx.am.onPubsubDelivery((t, json) => rows.push(JSON.parse(json)));
  ctx.am.pubsubSubscribe(TP(24));
  await deliver(ctx, TP(24), R1, { gen: gen(R1, 1) });
  check('attached to (R1, 1) by the reply to the first SUB', ctx.am._attachedGen.get(TP(24))?.root === lc(idHex(R1)));
  ctx.am._emitSubscribe(TP(24), []);                                    // the next renewal mints a new nonce
  await deliver(ctx, TP(24), R2, { gen: gen(R2, 1) });
  check('the reply from replacement root R2 to that renewal switches the attachment to (R2, 1)', ctx.am._attachedGen.get(TP(24))?.root === lc(idHex(R2)));
  await deliver(ctx, TP(24), R1, { gen: gen(R1, 1), authorized: false });
  check('a delayed old (R1, 1) DELIVER that answers no SUB of ours does NOT re-attach to R1', ctx.am._attachedGen.get(TP(24))?.root === lc(idHex(R2)));
  await terminated(ctx, TP(24), R1, gen(R1, 1));
  check('…so the old (R1, 1) notice ends nothing', ctx.am.mySubscriptions.has(TP(24)) && rows.length === 0);
  ctx.am._emitSubscribe(TP(24), []);
  await deliver(ctx, TP(24), R1, { gen: gen(R1, 2) });
  check('a reply from R1 to a NEW SUB of ours is a selection again: attached to (R1, 2)', ctx.am._attachedGen.get(TP(24))?.epoch === 2 && ctx.am._attachedGen.get(TP(24)).root === lc(idHex(R1)));
  // a fan-out DELIVER cannot make a FIRST attachment either
  ctx.am.pubsubSubscribe(TP(25));
  await deliver(ctx, TP(25), R1, { gen: gen(R1, 1), authorized: false });
  check('a fan-out DELIVER before any reply makes no attachment', !ctx.am._attachedGen.has(TP(25)));
  // the previous nonce still answers (two renewals may overlap)
  ctx.am.pubsubSubscribe(TP(26));
  const n1 = attachNonceFor(ctx, TP(26));
  ctx.am._emitSubscribe(TP(26), []);
  await ctx.am._onDeliver({ topicId: idHex(TP(26)), from: idHex(R1), msgs: [], gen: gen(R1, 1), attach: n1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a reply carrying the PREVIOUS nonce still attaches (overlapping renewals)', ctx.am._attachedGen.get(TP(26))?.root === lc(idHex(R1)));
}

console.log('\n[R6] unarmed: the frame is consumed without effect; the tick terminates nothing');
{
  const ctx = manager({ armed: false });
  const role = seed(ctx, TP(14));
  await sub(ctx, TP(14), B1, { standby: true, gen: gen(SELF) });
  for (let at = T0; at <= T0 + 2 * DROP_MS; at += TICK) { await sub(ctx, TP(14), B1, { standby: true, gen: gen(SELF) }); await ctx.tick(at); }
  check('unarmed: a root with only a standby seat is retained as 4.116.0 retained it (its seat), no termination', ctx.held(TP(14)) && !role.dismantling && ctx.sent(T.TOPIC_TERMINATED).length === 0);
  await replicate(ctx, TP(15), R1, {});
  await terminated(ctx, TP(15), R1, gen(R1));
  check('unarmed: TOPIC_TERMINATED is consumed and changes nothing', ctx.held(TP(15)) && ctx.log('role-terminated').length === 0);
}

console.log(`\nfence_topic_terminated: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
