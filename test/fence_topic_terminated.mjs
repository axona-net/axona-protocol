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
import { manager, seed, sub, replicate, terminated, deliver, attachNonceFor, attachOf, gen, meta, idHex, lc, TP, NODE, SELF, T0, MIN, T } from './lib/lifetimeFixture.mjs';
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
  check('a DELIVER naming (R1, 2) pins the upstream and records the attachment', ctx.am._upstream.get(TP(18))[0] === lc(idHex(R1)) && attachOf(ctx, TP(18))?.gen?.epoch === 2);
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
  check('a delayed (R1, 1) DELIVER after (R1, 2) leaves the attachment at epoch 2', attachOf(ctx, TP(22))?.gen?.epoch === 2);
  await terminated(ctx, TP(22), R1, gen(R1, 1));
  check('the old (R1, 1) notice then ends nothing', ctx.am.mySubscriptions.has(TP(22)) && rows.length === 0);
  await deliver(ctx, TP(22), R2, { gen: gen(R2, 1) });
  check('a DIFFERENT root re-attaches whatever its epoch (no global order across roots)', attachOf(ctx, TP(22))?.gen?.root === lc(idHex(R2)) && attachOf(ctx, TP(22))?.gen.epoch === 1);
  // the same rule on a relay role
  const child = seed(ctx, TP(23), { isRoot: false });
  ctx.am._upstream.set(TP(23), [lc(idHex(R1))]);
  await deliver(ctx, TP(23), R1, { gen: gen(R1, 3) });
  await deliver(ctx, TP(23), R1, { msgs: [{ msgId: 'old', publishTs: T0, json: '{}', seq: 1 }], gen: gen(R1, 2), authorized: false });
  check('a relay keeps epoch 3 through a delayed epoch-2 DELIVER, and still caches that DELIVER\'s body', attachOf(ctx, TP(23))?.gen?.epoch === 3 && child.cache.length === 1);
  await terminated(ctx, TP(23), R1, gen(R1, 2));
  check('the old notice is foreign to it', ctx.held(TP(23)));
}

console.log('\n[B07] a root switch needs a reply to THIS node\'s own SUB (Aster 0cfdd82d): a delayed old DELIVER from a previous root is not a selection');
{
  const ctx = manager();
  const rows = []; ctx.am.onPubsubDelivery((t, json) => rows.push(JSON.parse(json)));
  ctx.am.pubsubSubscribe(TP(24));
  await deliver(ctx, TP(24), R1, { gen: gen(R1, 1) });
  check('attached to (R1, 1) by the reply to the first SUB', attachOf(ctx, TP(24))?.gen?.root === lc(idHex(R1)));
  ctx.am._emitSubscribe(TP(24), []);                                    // the next renewal mints a new nonce
  await deliver(ctx, TP(24), R2, { gen: gen(R2, 1) });
  check('the reply from replacement root R2 to that renewal switches the attachment to (R2, 1)', attachOf(ctx, TP(24))?.gen?.root === lc(idHex(R2)));
  await deliver(ctx, TP(24), R1, { gen: gen(R1, 1), authorized: false });
  check('a delayed old (R1, 1) DELIVER that answers no SUB of ours does NOT re-attach to R1', attachOf(ctx, TP(24))?.gen?.root === lc(idHex(R2)));
  await terminated(ctx, TP(24), R1, gen(R1, 1));
  check('…so the old (R1, 1) notice ends nothing', ctx.am.mySubscriptions.has(TP(24)) && rows.length === 0);
  ctx.am._emitSubscribe(TP(24), []);
  await deliver(ctx, TP(24), R1, { gen: gen(R1, 2) });
  check('a reply from R1 to a NEW SUB of ours is a selection again: attached to (R1, 2)', attachOf(ctx, TP(24))?.gen?.epoch === 2 && attachOf(ctx, TP(24))?.gen.root === lc(idHex(R1)));
  // a fan-out DELIVER cannot make a FIRST attachment either
  ctx.am.pubsubSubscribe(TP(25));
  await deliver(ctx, TP(25), R1, { gen: gen(R1, 1), authorized: false });
  check('a fan-out DELIVER before any reply makes no attachment', (attachOf(ctx, TP(25))?.gen == null));
  // the previous nonce still answers (two renewals may overlap)
  ctx.am.pubsubSubscribe(TP(26));
  const n1 = attachNonceFor(ctx, TP(26));
  ctx.am._emitSubscribe(TP(26), []);
  await ctx.am._onDeliver({ topicId: idHex(TP(26)), from: idHex(R1), msgs: [], gen: gen(R1, 1), attach: n1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a reply carrying the PREVIOUS nonce still attaches (overlapping renewals)', attachOf(ctx, TP(26))?.gen?.root === lc(idHex(R1)));
}

console.log('\n[B11] a reply to an OLDER request never replaces a newer committed selection (Aster 535a6741); the pin follows the decision');
{
  const ctx = manager();
  const rows = []; ctx.am.onPubsubDelivery((t, json) => rows.push(JSON.parse(json)));
  // cross-root: attached R1/1; renewals n1 then n2; R2's reply to n2 commits; R1's delayed reply to n1 must not switch back
  ctx.am.pubsubSubscribe(TP(27));
  await deliver(ctx, TP(27), R1, { gen: gen(R1, 1) });
    ctx.am._emitSubscribe(TP(27), []); const n1 = attachOf(ctx, TP(27)).nonce;
  ctx.am._emitSubscribe(TP(27), []); const n2 = attachOf(ctx, TP(27)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(27)), from: idHex(R2), msgs: [], gen: gen(R2, 1), attach: n2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('R2\'s reply to the newer request n2 commits (R2, 1) and pins R2', attachOf(ctx, TP(27))?.gen?.root === lc(idHex(R2)) && ctx.am._upstream.get(TP(27))[0] === lc(idHex(R2)));
  await ctx.am._onDeliver({ topicId: idHex(TP(27)), from: idHex(R1), msgs: [], gen: gen(R1, 1), attach: n1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('R1\'s delayed reply to the OLDER request n1 does not switch back, and does not re-pin', attachOf(ctx, TP(27))?.gen?.root === lc(idHex(R2)) && ctx.am._upstream.get(TP(27))[0] === lc(idHex(R2)) && ctx.log('deliver-attachment-ignored').length >= 1);
  await terminated(ctx, TP(27), R1, gen(R1, 1));
  check('…so the old (R1, 1) notice ends nothing', ctx.am.mySubscriptions.has(TP(27)) && rows.length === 0);
  await ctx.am._onDeliver({ topicId: idHex(TP(27)), from: idHex(R2), msgs: [], gen: gen(R2, 1), attach: n2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a repeated reply to the already-committed attempt changes nothing (data only)', attachOf(ctx, TP(27))?.gen?.root === lc(idHex(R2)));
  // same-root: n2's reply commits R1/2; n1's delayed reply naming R1/1 must not roll back through the nonce path
  ctx.am.pubsubSubscribe(TP(28));
  await deliver(ctx, TP(28), R1, { gen: gen(R1, 1) });
    ctx.am._emitSubscribe(TP(28), []); const m1 = attachOf(ctx, TP(28)).nonce;
  ctx.am._emitSubscribe(TP(28), []); const m2 = attachOf(ctx, TP(28)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(28)), from: idHex(R1), msgs: [], gen: gen(R1, 2), attach: m2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  await ctx.am._onDeliver({ topicId: idHex(TP(28)), from: idHex(R1), msgs: [], gen: gen(R1, 1), attach: m1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('same root: the delayed reply to m1 naming epoch 1 does not roll back the committed epoch 2', attachOf(ctx, TP(28))?.gen?.epoch === 2);
  await terminated(ctx, TP(28), R1, gen(R1, 1));
  check('…and the old notice ends nothing', ctx.am.mySubscriptions.has(TP(28)));
  // liveness kept: an older request's reply still commits when NO newer reply has
  ctx.am.pubsubSubscribe(TP(29));
    ctx.am._emitSubscribe(TP(29), []); const p1 = attachOf(ctx, TP(29)).nonce;
  ctx.am._emitSubscribe(TP(29), []);
  await ctx.am._onDeliver({ topicId: idHex(TP(29)), from: idHex(R1), msgs: [], gen: gen(R1, 1), attach: p1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('with no newer reply committed, the reply to the previous attempt still attaches (overlap liveness)', attachOf(ctx, TP(29))?.gen?.root === lc(idHex(R1)));
  // the same order rule on a relay role
  const child = seed(ctx, TP(30), { isRoot: false });
  ctx.am._upstream.set(TP(30), [lc(idHex(R1))]);
  await deliver(ctx, TP(30), R1, { gen: gen(R1, 1) });
  ctx.am._emitSubscribe(TP(30), []); const c1 = attachOf(ctx, TP(30)).nonce;
  ctx.am._emitSubscribe(TP(30), []); const c2 = attachOf(ctx, TP(30)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(30)), from: idHex(R2), msgs: [], gen: gen(R2, 1), attach: c2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  await ctx.am._onDeliver({ topicId: idHex(TP(30)), from: idHex(R1), msgs: [{ msgId: 'late', publishTs: T0, json: '{}', seq: 1 }], gen: gen(R1, 1), attach: c1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a relay: the older reply does not switch back, is not pinned, but its body is still cached (data only)', attachOf(ctx, TP(30))?.gen?.root === lc(idHex(R2)) && ctx.am._upstream.get(TP(30))[0] === lc(idHex(R2)) && child.cache.length === 1 && !(child.upstreamHeld && child.upstreamHeld.has('late')));
}

console.log('\n[B12] same generation, two parents (Aster c59cb0be): an obsolete correlated reply, or fan-out from a parent not selected, moves nothing');
{
  const ctx = manager();
  const C1 = NODE(21), C2 = NODE(22);                                  // two child relays under the same root generation (R1, 1)
  // READER: select C1 (reply to n1), then C2 (reply to n2); C1's delayed reply to n1 must not re-pin
  ctx.am.pubsubSubscribe(TP(31));
  const n1 = attachOf(ctx, TP(31)).nonce;                                           // pubsubSubscribe already sent SUB #1
  await ctx.am._onDeliver({ topicId: idHex(TP(31)), from: idHex(C1), msgs: [], gen: gen(R1, 1), attach: n1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('selected C1 under (R1, 1)', ctx.am._upstream.get(TP(31))[0] === lc(idHex(C1)) && attachOf(ctx, TP(31))?.gen?.root === lc(idHex(R1)));
  ctx.am._emitSubscribe(TP(31), []); const n2 = attachOf(ctx, TP(31)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(31)), from: idHex(C2), msgs: [], gen: gen(R1, 1), attach: n2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('the reply to n2 from C2, SAME generation, commits C2', ctx.am._upstream.get(TP(31))[0] === lc(idHex(C2)));
  await ctx.am._onDeliver({ topicId: idHex(TP(31)), from: idHex(C1), msgs: [], gen: gen(R1, 1), attach: n1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('C1\'s delayed reply to n1 (same generation) is data only: still pinned to C2', ctx.am._upstream.get(TP(31))[0] === lc(idHex(C2)) && ctx.log('deliver-attachment-ignored').some((r) => r.ctx.reader === true && r.ctx.k === 1));
  await ctx.am._onDeliver({ topicId: idHex(TP(31)), from: idHex(C1), msgs: [], gen: gen(R1, 1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('fan-out from C1, not the selected parent, re-pins nothing either', ctx.am._upstream.get(TP(31))[0] === lc(idHex(C2)));
  await ctx.am._onDeliver({ topicId: idHex(TP(31)), from: idHex(C2), msgs: [], gen: gen(R1, 2) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('fan-out from the SELECTED parent C2 naming the root\'s next epoch moves the attachment forward', attachOf(ctx, TP(31))?.gen?.epoch === 2 && ctx.am._upstream.get(TP(31))[0] === lc(idHex(C2)));
  await ctx.am._onDeliver({ topicId: idHex(TP(31)), from: idHex(C2), msgs: [], gen: gen(R1, 1), attach: 'never-ours' }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a nonce we never issued is data only', attachOf(ctx, TP(31))?.gen?.epoch === 2);
  // RELAY: the same two-parent reordering; the old parent's body is cached but earns no custody credit
  const child = seed(ctx, TP(32), { isRoot: false });
  ctx.am._upstream.set(TP(32), [lc(idHex(C1))]);
  ctx.am._emitSubscribe(TP(32), []); const r1 = attachOf(ctx, TP(32)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(32)), from: idHex(C1), msgs: [], gen: gen(R1, 1), attach: r1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  ctx.am._emitSubscribe(TP(32), []); const r2 = attachOf(ctx, TP(32)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(32)), from: idHex(C2), msgs: [], gen: gen(R1, 1), attach: r2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('relay: committed to C2 under the same generation', ctx.am._upstream.get(TP(32))[0] === lc(idHex(C2)) && child.upstreamHeldFrom === lc(idHex(C2)));
  await ctx.am._onDeliver({ topicId: idHex(TP(32)), from: idHex(C1), msgs: [{ msgId: 'c1body', publishTs: T0, json: '{}', seq: 1 }], gen: gen(R1, 1), attach: r1 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('C1\'s delayed reply: body cached, no re-pin, no custody credit', child.cache.length === 1 && ctx.am._upstream.get(TP(32))[0] === lc(idHex(C2)) && child.upstreamHeldFrom === lc(idHex(C2)) && !(child.upstreamHeld && child.upstreamHeld.has('c1body')));
  await ctx.am._onDeliver({ topicId: idHex(TP(32)), from: idHex(C1), msgs: [{ msgId: 'c1fan', publishTs: T0, json: '{}', seq: 2 }], gen: gen(R1, 1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('fan-out from the unselected parent: body cached, nothing else', child.cache.length === 2 && !(child.upstreamHeld && child.upstreamHeld.has('c1fan')) && ctx.am._upstream.get(TP(32))[0] === lc(idHex(C2)));
  await ctx.am._onDeliver({ topicId: idHex(TP(32)), from: idHex(C2), msgs: [{ msgId: 'c2fan', publishTs: T0, json: '{}', seq: 3 }], gen: gen(R1, 1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('fan-out from the selected parent credits custody', child.upstreamHeld.has('c2fan'));
}

console.log('\n[B13] a hybrid node (Aster daa31e63): a notice ends only the obligation it matches; the role object goes only when no live duty remains');
{
  // CHILD + reader: one attachment per topic, so the role and the reader move together; the old notice is foreign to both
  const ctx = manager();
  const rows = []; ctx.am.onPubsubDelivery((t, json) => rows.push(JSON.parse(json)));
  const child = seed(ctx, TP(33), { isRoot: false });
  ctx.am._upstream.set(TP(33), [lc(idHex(R1))]);
  await deliver(ctx, TP(33), R1, { gen: gen(R1, 1) });
  await sub(ctx, TP(33), C1, {}, { viaSelf: true });
  ctx.am.pubsubSubscribe(TP(33));                                        // the node adds its own reader; the SUB renews the SAME attachment
  await deliver(ctx, TP(33), R2, { gen: gen(R2, 1) });                   // the reply selects (R2, 1) for role and reader alike
  check('child + reader share one attachment, now (R2, 1); the pin is R2', attachOf(ctx, TP(33))?.gen?.root === lc(idHex(R2)) && ctx.am._upstream.get(TP(33))[0] === lc(idHex(R2)));
  await terminated(ctx, TP(33), R1, gen(R1, 1));
  check('the old (R1, 1) notice is foreign: role kept with its seat, reader kept, pin kept, no row', ctx.held(TP(33)) && child.subscribers.size === 1 && ctx.am.mySubscriptions.has(TP(33)) && ctx.am._upstream.get(TP(33))[0] === lc(idHex(R2)) && rows.length === 0);
  await terminated(ctx, TP(33), R2, gen(R2, 1));
  check('the (R2, 1) notice ends both: seat forwarded, reader ended with one row, role gone', !ctx.held(TP(33)) && !ctx.am.mySubscriptions.has(TP(33)) && rows.length === 1 && ctx.sent(T.TOPIC_TERMINATED).length === 1);
  // BACKUP + reader, old-appointment notice FIRST: the appointment ends, the reader attached to R2 continues on its pin
  const ctx2 = manager();
  const rows2 = []; ctx2.am.onPubsubDelivery((t, json) => rows2.push(JSON.parse(json)));
  await replicate(ctx2, TP(34), R1, { gen: gen(R1, 1), issuedAt: T0, dels: [{ del: true, msgId: 'd', killTs: T0, signer: null, seq: 1, publishTs: T0 }] });
  const bk = ctx2.role(TP(34));
  ctx2.am.pubsubSubscribe(TP(34));
  await deliver(ctx2, TP(34), R2, { gen: gen(R2, 1) });
  check('backup of (R1, 1) with a tombstone; own reader attached to (R2, 1), pinned R2', bk.backupOf === lc(idHex(R1)) && bk.tombstones.size === 1 && attachOf(ctx2, TP(34))?.gen?.root === lc(idHex(R2)) && ctx2.am._upstream.get(TP(34))[0] === lc(idHex(R2)));
  await terminated(ctx2, TP(34), R1, gen(R1, 1));
  check('(R1, 1) first: the appointment ends (backup duty and tombstone gone), the role stays for the reader, the reader and its pin are intact, no row', ctx2.held(TP(34)) && bk.backupOf === null && !ctx2.am._backupTopics.has(TP(34)) && bk.tombstones.size === 0 && ctx2.am.mySubscriptions.has(TP(34)) && ctx2.am._upstream.get(TP(34))[0] === lc(idHex(R2)) && rows2.length === 0 && ctx2.log('obligation-terminated').length === 1);
  await terminated(ctx2, TP(34), R2, gen(R2, 1));
  check('then (R2, 1): the reader ends with one row and the role goes', !ctx2.held(TP(34)) && !ctx2.am.mySubscriptions.has(TP(34)) && rows2.length === 1);
  // BACKUP + reader, attachment notice FIRST: the reader ends, the backup appointment stays until its own notice
  const ctx3 = manager();
  const rows3 = []; ctx3.am.onPubsubDelivery((t, json) => rows3.push(JSON.parse(json)));
  await replicate(ctx3, TP(35), R1, { gen: gen(R1, 1), issuedAt: T0 });
  const bk3 = ctx3.role(TP(35));
  ctx3.am.pubsubSubscribe(TP(35));
  await deliver(ctx3, TP(35), R2, { gen: gen(R2, 1) });
  await terminated(ctx3, TP(35), R2, gen(R2, 1));
  check('(R2, 1) first: the reader ends with one row; the role stays as a backup of (R1, 1), still renewing standby', ctx3.held(TP(35)) && !ctx3.am.mySubscriptions.has(TP(35)) && rows3.length === 1 && bk3.backupOf === lc(idHex(R1)) && ctx3.am._backupTopics.has(TP(35)));
  await terminated(ctx3, TP(35), R1, gen(R1, 1));
  check('then (R1, 1): the appointment ends and the role goes', !ctx3.held(TP(35)) && !ctx3.am._backupTopics.has(TP(35)));
}

console.log('\n[B14] the selected parent hands a delegated seat to its child; a cold subscribe\'s burst of attempts is all recognised');
{
  const ctx = manager();
  const C1 = NODE(21), C2 = NODE(22);
  ctx.am.pubsubSubscribe(TP(36));
  await deliver(ctx, TP(36), R1, { gen: gen(R1, 1) });
  check('selected R1', ctx.am._upstream.get(TP(36))[0] === lc(idHex(R1)));
  await ctx.am._onDeliver({ topicId: idHex(TP(36)), from: idHex(C2), msgs: [], gen: gen(R1, 1), delegate: idHex(C1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a delegate from a node that is NOT the selected parent is ignored', ctx.am._upstream.get(TP(36))[0] === lc(idHex(R1)) && ctx.log('delegate-ignored').length === 1);
  await ctx.am._onDeliver({ topicId: idHex(TP(36)), from: idHex(R1), msgs: [], gen: gen(R1, 1), delegate: idHex(C1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('the selected parent R1 hands the seat to C1: pin and attachment parent move, generation and attempt unchanged', ctx.am._upstream.get(TP(36))[0] === lc(idHex(C1)) && attachOf(ctx, TP(36)).from === lc(idHex(C1)) && attachOf(ctx, TP(36)).gen.root === lc(idHex(R1)) && ctx.log('delegated-by-parent').length === 1);
  await ctx.am._onDeliver({ topicId: idHex(TP(36)), from: idHex(C1), msgs: [{ msgId: 'c1', publishTs: T0, json: '{}', seq: 1 }], gen: gen(R1, 1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('the child\'s fan-out is now from the selected parent: accepted (forward), pin stays C1', ctx.am._upstream.get(TP(36))[0] === lc(idHex(C1)));
  // the window: a cold subscribe emits several SUBs before the first reply; the reply to the FIRST still commits
  ctx.am.pubsubSubscribe(TP(37));
  const first = attachOf(ctx, TP(37)).nonce;
  for (let i = 0; i < 5; i++) ctx.am._emitSubscribe(TP(37), []);          // the steer and the burst retries
  check('six attempts in the window, the first still known', attachOf(ctx, TP(37)).attempts.size === 6 && attachOf(ctx, TP(37)).attempts.has(first));
  await ctx.am._onDeliver({ topicId: idHex(TP(37)), from: idHex(R1), msgs: [], gen: gen(R1, 1), attach: first }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('the reply to the first attempt commits and pins', ctx.am._upstream.get(TP(37))[0] === lc(idHex(R1)) && attachOf(ctx, TP(37)).committed === 1);
  for (let i = 0; i < 20; i++) ctx.am._emitSubscribe(TP(37), []);
  check('the window is bounded (16): the oldest attempts fall off', attachOf(ctx, TP(37)).attempts.size === 16 && !attachOf(ctx, TP(37)).attempts.has(first));
}

console.log('\n[X1] a deletion belongs to the obligation that delivered it (Aster b0748c4c): an appointment\'s end never drops the live attachment\'s tombstone');
{
  const ctx = manager();
  await replicate(ctx, TP(38), R1, { gen: gen(R1, 1), issuedAt: T0, dels: [{ del: true, msgId: 'r1dead', killTs: T0, signer: null, seq: 1, publishTs: T0 }] });
  const role = ctx.role(TP(38));
  ctx.am.pubsubSubscribe(TP(38));
  await deliver(ctx, TP(38), R2, { gen: gen(R2, 1), msgs: [{ del: true, msgId: 'r2dead', killTs: T0, signer: null, publishTs: T0, seq: 1 }] });
  check('two tombstones: one from the appointment (R1, 1), one from the attachment (R2, 1), each tagged', role.tombstones.size === 2 && role.tombstones.get('r1dead')?.gen?.root === lc(idHex(R1)) && role.tombstones.get('r2dead')?.gen?.root === lc(idHex(R2)));
  await deliver(ctx, TP(38), R2, { msgs: [{ msgId: 'r2dead', publishTs: T0, json: '{}', seq: 2 }], gen: gen(R2, 1), authorized: false });
  check('control: a reordered R2 body for the deleted id is suppressed (not cached)', !role.cacheIds.has('r2dead'));
  await terminated(ctx, TP(38), R1, gen(R1, 1));
  check('(R1, 1) ends the appointment and drops ITS tombstone only; R2\'s stays with the reader and its pin', role.backupOf === null && role.tombstones.size === 1 && role.tombstones.has('r2dead') && ctx.am.mySubscriptions.has(TP(38)) && ctx.am._upstream.get(TP(38))[0] === lc(idHex(R2)));
  await deliver(ctx, TP(38), R2, { msgs: [{ msgId: 'r2dead', publishTs: T0, json: '{}', seq: 2 }], gen: gen(R2, 1), authorized: false });
  check('…so the same reordered R2 body is still suppressed after the appointment ended', !role.cacheIds.has('r2dead'));
  await terminated(ctx, TP(38), R2, gen(R2, 1));
  check('(R2, 1) ends the attachment: its tombstone goes with it and the role is gone', !ctx.held(TP(38)));
  // mirror: the attachment ends first; the appointment's tombstone stays with the backup
  const ctx2 = manager();
  await replicate(ctx2, TP(39), R1, { gen: gen(R1, 1), issuedAt: T0, dels: [{ del: true, msgId: 'r1dead', killTs: T0, signer: null, seq: 1, publishTs: T0 }] });
  const role2 = ctx2.role(TP(39));
  ctx2.am.pubsubSubscribe(TP(39));
  await deliver(ctx2, TP(39), R2, { gen: gen(R2, 1), msgs: [{ del: true, msgId: 'r2dead', killTs: T0, signer: null, publishTs: T0, seq: 1 }] });
  await terminated(ctx2, TP(39), R2, gen(R2, 1));
  check('(R2, 1) first: the attachment\'s tombstone goes, the appointment\'s stays with the backup', ctx2.held(TP(39)) && role2.tombstones.size === 1 && role2.tombstones.has('r1dead') && role2.backupOf === lc(idHex(R1)));
}

console.log('\n[X2] the handover obeys the same decision as every control effect (Aster b0748c4c)');
{
  const ctx = manager();
  const C1 = NODE(21);
  ctx.am.pubsubSubscribe(TP(40));
  await deliver(ctx, TP(40), R1, { gen: gen(R1, 2) });
  check('selected R1 at epoch 2', ctx.am._upstream.get(TP(40))[0] === lc(idHex(R1)) && attachOf(ctx, TP(40)).gen.epoch === 2);
  await ctx.am._onDeliver({ topicId: idHex(TP(40)), from: idHex(R1), msgs: [], gen: gen(R1, 1), delegate: idHex(C1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a delayed epoch-1 delegate from the pinned parent moves nothing', ctx.am._upstream.get(TP(40))[0] === lc(idHex(R1)) && attachOf(ctx, TP(40)).gen.epoch === 2);
  const n1 = attachOf(ctx, TP(40)).nonce;
  ctx.am._emitSubscribe(TP(40), []); const n2 = attachOf(ctx, TP(40)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(40)), from: idHex(R1), msgs: [], gen: gen(R1, 2), attach: n2 }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  await ctx.am._onDeliver({ topicId: idHex(TP(40)), from: idHex(R1), msgs: [], gen: gen(R1, 2), attach: n1, delegate: idHex(C1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('a delegate echoing the obsolete attempt n1 after n2 committed moves nothing', ctx.am._upstream.get(TP(40))[0] === lc(idHex(R1)));
  await ctx.am._onDeliver({ topicId: idHex(TP(40)), from: idHex(R1), msgs: [], gen: gen(R1, 2), delegate: idHex(C1) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('positive control: a current-generation delegate from the selected parent (fan-out) hands over', ctx.am._upstream.get(TP(40))[0] === lc(idHex(C1)) && attachOf(ctx, TP(40)).from === lc(idHex(C1)));
  ctx.am._emitSubscribe(TP(40), []); const n3 = attachOf(ctx, TP(40)).nonce;
  await ctx.am._onDeliver({ topicId: idHex(TP(40)), from: idHex(C1), msgs: [], gen: gen(R1, 2), attach: n3, delegate: idHex(R2) }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('positive control: a delegate riding the reply to a live newer attempt hands over', ctx.am._upstream.get(TP(40))[0] === lc(idHex(R2)));
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
