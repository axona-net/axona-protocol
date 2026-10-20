#!/usr/bin/env node
// =====================================================================
// fence_standby_lease.mjs — THE STANDBY LEASE and THE GRANT RULE (Topic
// Lifetime 4.1, 4.2, 4.4; axona-docs architecture/Axona-Topic-Lifetime-v0.1.md
// v0.9 @ 11288d7; David "Build" 2026-10-20).
//
// A backup's appointment is granted and renewed ONLY by a REPLICATE from the
// generation's root; nothing the backup sends moves it. L = DROP_MS. A
// REPLICATE is an appointment or a renewal only if its apparent age is within
// D + S (75 s); older is dropped whatever it names. A generation-less grant
// (an older kernel) is an appointment only from the receiver's CURRENT
// principal and never creates a role from nothing. A lapsed appointment
// writes a LEFT record for L + S + D under which a grant of THAT generation
// is refused; a different root or a later epoch is a new generation and is
// admitted. A lapse ends the APPOINTMENT and nothing else: bodies or active
// tombstones keep the election obligation (4.4(1)); a reader keeps reading;
// an empty backup is reaped on the tick.
//
// Mutants (red): let the standby SUB stamp lastReplicaAt → S1; drop the
// D + S age check → G1/G2; drop the LEFT check before becomeBackup → L1;
// drop the legacy-not-principal rule → G4; drop the bodies/tombstones
// exception in _standbyLeaseSweep → S3 (data lost).
// Run: TOPIC_LIFETIME=1 node test/fence_standby_lease.mjs
// =====================================================================
import { manager, seed, sub, replicate, gen, idHex, lc, TP, NODE, SELF, T0, MIN, T } from './lib/lifetimeFixture.mjs';
import { STANDBY_LEASE_MS, APPOINTMENT_DELAY_MS, APPOINTMENT_SKEW_MS, DROP_MS, TTL_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };
const R1 = NODE(3), R2 = NODE(4), X = NODE(7);
const TICK = 5_000, L = STANDBY_LEASE_MS, D = APPOINTMENT_DELAY_MS, S = APPOINTMENT_SKEW_MS;

console.log('fence_standby_lease: an appointment is a lease on the root\'s REPLICATE');
check(`L = DROP_MS = ${DROP_MS} ms; D + S = ${D + S} ms`, L === DROP_MS && D === 60_000 && S === 15_000);

console.log('\n[S1] the backup\'s own standby SUB does not move lastReplicaAt; an empty backup lapses after L with no election and no further SUB');
{
  const ctx = manager();
  await replicate(ctx, TP(1), R1, { gen: gen(R1), issuedAt: T0 });
  const role = ctx.role(TP(1));
  check('appointed: backupOf = R1, standbyGen = (R1, 1), lastReplicaAt = T0', role && role.backupOf === lc(idHex(R1)) && role.standbyGen.root === lc(idHex(R1)) && role.standbyGen.epoch === 1 && role.lastReplicaAt === T0);
  await ctx.tick(T0 + TICK);
  const subs = ctx.sent(T.SUB);
  check('the tick renews the standby SUB toward the root, declared standby with the generation', subs.length === 1 && subs[0].payload.standby === true && subs[0].payload.gen.root === lc(idHex(R1)), JSON.stringify(subs[0]?.payload));
  check('…and lastReplicaAt is unmoved by it', role.lastReplicaAt === T0);
  let at = T0 + TICK;
  while (at + TICK <= T0 + L) { at += TICK; await ctx.tick(at); }
  check('at L past the grant it is still appointed (strictly greater)', at === T0 + L && ctx.am._backupTopics.has(TP(1)) && role.backupOf !== null);
  const subsBefore = ctx.sent(T.SUB).length;
  await ctx.tick(at + TICK);
  check('one tick later the appointment lapses: retireBackup(lease-lapsed), a LEFT record written, no election', !ctx.am._backupTopics.has(TP(1)) && ctx.log('standby-lapsed').length === 1 && ctx.am._leftRecords.has(TP(1)) && ctx.log('root-transition').length === 0);
  check('the empty ex-backup is reaped by the on-sight rule on the same tick', !ctx.held(TP(1)) && ctx.rows.some((r) => r.event.endsWith('role-reaped') && r.ctx.why === 'dead'));
  for (let i = 0; i < 20; i++) await ctx.tick(ctx.clock.t + TICK);
  check('no further standby SUB over twenty ticks', ctx.sent(T.SUB).length === subsBefore);
  check('counted: lifetime.standbyLapsed = 1', ctx.am.inspectAdmission().lifetime.standbyLapsed === 1);
}

console.log('\n[S2] a keepalive from the root renews the lease; the lease is measured from the LAST grant');
{
  const ctx = manager();
  await replicate(ctx, TP(2), R1, { gen: gen(R1), issuedAt: T0 });
  const role = ctx.role(TP(2));
  for (let at = T0 + TICK; at <= T0 + 10 * MIN; at += TICK) {
    ctx.clock.t = at;
    await replicate(ctx, TP(2), R1, { gen: gen(R1), issuedAt: at });   // the root's per-tick keepalive
    await ctx.tick(at);
  }
  check('ten minutes of keepalives: still appointed, lastReplicaAt = the last grant', ctx.am._backupTopics.has(TP(2)) && role.lastReplicaAt === T0 + 10 * MIN);
  const lastGrant = ctx.clock.t;
  for (let at = lastGrant + TICK; at <= lastGrant + L + TICK; at += TICK) await ctx.tick(at);
  check('…and lapses L + one tick after the last grant', !ctx.am._backupTopics.has(TP(2)) && ctx.log('standby-lapsed')[0].ctx.ageMs > L);
}

console.log('\n[S3] a lapse ends the APPOINTMENT and nothing else: a backup with a body keeps the election obligation; one with a tombstone too; a reader keeps its seat');
{
  const ctx = manager();
  await replicate(ctx, TP(3), R1, { gen: gen(R1), issuedAt: T0, msgs: [] });
  const withBody = ctx.role(TP(3)); withBody.cache.push({ msgId: 'b', publishTs: T0, json: '{}', bytes: 80 }); withBody.cacheIds.add('b');
  await replicate(ctx, TP(4), R1, { gen: gen(R1), issuedAt: T0, dels: [{ del: true, msgId: 'd', killTs: T0, signer: null, seq: 1, publishTs: T0 }] });
  const withTomb = ctx.role(TP(4));
  await replicate(ctx, TP(5), R1, { gen: gen(R1), issuedAt: T0 });
  ctx.am.mySubscriptions.set(TP(5), { since: 0, lastRenewSent: T0, interval: 60_000, createdAt: T0 });   // also a reader
  for (let at = T0 + TICK; at <= T0 + L + 2 * TICK; at += TICK) await ctx.tick(at);
  check('with a body: still a backup, still subscribing (the 4.94.0 election path, unchanged)', ctx.am._backupTopics.has(TP(3)) && withBody.backupOf !== null && ctx.held(TP(3)));
  check('with an active tombstone: likewise held (custody)', ctx.am._backupTopics.has(TP(4)) && withTomb.tombstones.size === 1 && ctx.held(TP(4)));
  check('a reader-backup: the appointment lapsed, the reader seat and the role continue', !ctx.am._backupTopics.has(TP(5)) && ctx.am.mySubscriptions.has(TP(5)) && ctx.held(TP(5)));
}

console.log('\n[G1] the grant rule: apparent age ≤ D + S appoints; older is dropped whatever it names');
{
  const ctx = manager();
  ctx.clock.t = T0 + 10 * MIN;
  await replicate(ctx, TP(6), R1, { gen: gen(R1), issuedAt: ctx.clock.t - (D + S) + 1000 });
  check('a grant reading 74 s old is an appointment', ctx.held(TP(6)) && ctx.am._backupTopics.has(TP(6)));
  await replicate(ctx, TP(7), R1, { gen: gen(R1), issuedAt: ctx.clock.t - (D + S) - 1000 });
  check('a grant reading 76 s old creates nothing and is logged stale', !ctx.held(TP(7)) && ctx.log('grant-refused').some((r) => r.ctx.why === 'stale'));
  await replicate(ctx, TP(8), R1, { gen: gen(R1), issuedAt: ctx.clock.t + 10_000 });
  check('a grant from a clock ahead of ours (negative age) is an appointment', ctx.held(TP(8)));
  check('counted: grantsRefused = 1', ctx.am.inspectAdmission().lifetime.grantsRefused === 1);
}

console.log('\n[G2] Aster\'s 181 s traces: a grant issued before a lapse and delivered at 181 s after it meets a LEFT record AND is past D + S; the same under 2 s of skew');
{
  const ctx = manager();
  await replicate(ctx, TP(9), R1, { gen: gen(R1), issuedAt: T0 });
  const issued = T0;                                                    // the root's last grant, issued at the lapse's root cause
  let at = T0;
  while (at <= T0 + L + TICK) { at += TICK; await ctx.tick(at); }       // lapse
  const lapsedAt = ctx.clock.t;
  check('lapsed, LEFT record for (R1, 1)', !ctx.am._backupTopics.has(TP(9)) && ctx.am._leftRecords.get(TP(9))?.rootId === lc(idHex(R1)));
  ctx.clock.t = issued + 181_000;
  await replicate(ctx, TP(9), R1, { gen: gen(R1), issuedAt: issued });
  check('perfect clocks, delivered at 181 s: refused (left-record), creates nothing', !ctx.held(TP(9)) && ctx.log('grant-refused').some((r) => r.ctx.why === 'left-record'));
  ctx.clock.t = issued + 181_000;
  await replicate(ctx, TP(9), R1, { gen: gen(R1), issuedAt: issued + 2000 });   // reads 179 s old under 2 s of skew
  check('under 2 s of skew (reads 179 s): refused likewise', !ctx.held(TP(9)));
  ctx.clock.t = lapsedAt + L + S + D + 1000;
  ctx.am._leftPrune();
  check('at L + S + D + 1 after the lapse the record has expired', !ctx.am._leftRecords.has(TP(9)));
  await replicate(ctx, TP(9), R1, { gen: gen(R1), issuedAt: issued });
  check('…and the same grant is now dropped by AGE alone', !ctx.held(TP(9)) && ctx.log('grant-refused').filter((r) => r.ctx.why === 'stale').length === 1);
}

console.log('\n[L1] a LEFT record refuses only the generation it names: a new root, or the same root at a higher epoch, is admitted');
{
  const ctx = manager();
  await replicate(ctx, TP(10), R1, { gen: gen(R1), issuedAt: T0 });
  let at = T0;
  while (at <= T0 + L + TICK) { at += TICK; await ctx.tick(at); }
  check('lapsed with a record', ctx.am._leftRecords.has(TP(10)) && !ctx.held(TP(10)));
  await replicate(ctx, TP(10), R1, { gen: gen(R1, 1), issuedAt: ctx.clock.t });
  check('a FRESH grant of the SAME generation (R1, 1) is still refused by the record', !ctx.held(TP(10)));
  await replicate(ctx, TP(10), R2, { gen: gen(R2, 1), issuedAt: ctx.clock.t });
  check('a grant from a different root (R2, 1) is a new generation: appointed', ctx.held(TP(10)) && ctx.role(TP(10)).backupOf === lc(idHex(R2)));
  ctx.am._retireRole(TP(10), ctx.role(TP(10)), 'test');
  ctx.am._backupTopics.delete(TP(10));
  await replicate(ctx, TP(10), R1, { gen: gen(R1, 2), issuedAt: ctx.clock.t });
  check('a grant from R1 at epoch 2 is a new generation too: appointed', ctx.held(TP(10)) && ctx.role(TP(10)).standbyGen.epoch === 2);
}

console.log('\n[G3] a wall-clock jump beyond S at the receiver holds grants for one D');
{
  const ctx = manager();
  await ctx.tick(T0);
  ctx.am._now = () => ctx.clock.t + 60_000;                           // the wall clock jumps a minute ahead; the monotonic clock did not
  await ctx.tick(T0 + TICK);
  check('the jump is detected and logged', ctx.log('clock-jump').length === 1 && ctx.am._grantHoldUntil > 0);
  await replicate(ctx, TP(11), R1, { gen: gen(R1), issuedAt: ctx.am._now() });
  check('a fresh grant during the hold is refused', !ctx.held(TP(11)) && ctx.log('grant-refused').some((r) => r.ctx.why === 'stale'));
  ctx.clock.t += D + TICK;
  await replicate(ctx, TP(11), R1, { gen: gen(R1), issuedAt: ctx.am._now() });
  check('after one D the hold is over and the grant appoints', ctx.held(TP(11)));
}

console.log('\n[G4] a generation-less grant (an older kernel): a renewal only from the current principal; never a role from nothing');
{
  const ctx = manager();
  await replicate(ctx, TP(12), R1, {});
  check('no gen, no role here: refused (legacy-not-principal), nothing created', !ctx.held(TP(12)) && ctx.log('grant-refused').some((r) => r.ctx.why === 'legacy-not-principal'));
  await replicate(ctx, TP(12), R1, { gen: gen(R1), issuedAt: T0 });
  const role = ctx.role(TP(12));
  ctx.clock.t = T0 + 30_000;
  await replicate(ctx, TP(12), R1, {});
  check('no gen, from the current principal: renews (lastReplicaAt moves), generation kept', role.lastReplicaAt === T0 + 30_000 && role.standbyGen.epoch === 1);
  await replicate(ctx, TP(12), X, {});
  check('no gen, from a stranger: refused, principal unchanged', role.backupOf === lc(idHex(R1)) && ctx.log('grant-refused').filter((r) => r.ctx.why === 'legacy-not-principal').length === 2);
}

console.log('\n[B03] a gen-bearing grant with NO issue stamp is refused, as a creation and as a renewal (Aster c5adedec)');
{
  const ctx = manager();
  await replicate(ctx, TP(17), R1, { gen: gen(R1) });
  check('no stamp, no role here: refused (no-stamp), nothing created', !ctx.held(TP(17)) && ctx.log('grant-refused').some((r) => r.ctx.why === 'no-stamp'));
  await replicate(ctx, TP(17), R1, { gen: gen(R1), issuedAt: T0 });
  const role = ctx.role(TP(17));
  ctx.clock.t = T0 + 30_000;
  await replicate(ctx, TP(17), R1, { gen: gen(R1) });
  check('no stamp, from the principal, gen present: refused too; lastReplicaAt unmoved', role.lastReplicaAt === T0 && ctx.log('grant-refused').filter((r) => r.ctx.why === 'no-stamp').length === 2);
  await replicate(ctx, TP(17), R1, { gen: gen(R1), issuedAt: 'soon' });
  check('a non-finite stamp is no stamp', role.lastReplicaAt === T0 && ctx.log('grant-refused').filter((r) => r.ctx.why === 'no-stamp').length === 3);
}

console.log('\n[B10] grants move forward only (Aster 0cfdd82d): a still-fresh older-epoch grant does not roll the appointment back; an old root\'s delayed grant beyond S is not a new principal');
{
  const ctx = manager();
  ctx.clock.t = T0 + 10 * MIN;
  await replicate(ctx, TP(18), R1, { gen: gen(R1, 2), issuedAt: ctx.clock.t });
  const role = ctx.role(TP(18));
  await replicate(ctx, TP(18), R1, { gen: gen(R1, 1), issuedAt: ctx.clock.t - 1000 });
  check('a grant naming (R1, 1) one second older than the (R1, 2) appointment is refused (obsolete-generation); the appointment stays at epoch 2', role.standbyGen.epoch === 2 && ctx.log('grant-refused').some((r) => r.ctx.why === 'obsolete-generation'));
  await ctx.am._onTopicTerminated({ topicId: idHex(TP(18)), from: idHex(R1), gen: gen(R1, 1) }, { targetId: SELF, isTerminal: true, hopCount: 1, fromId: idHex(R1) });
  check('…so the old (R1, 1) notice does not retire it', ctx.held(TP(18)) && ctx.am._backupTopics.has(TP(18)));
  await replicate(ctx, TP(18), R2, { gen: gen(R2, 1), issuedAt: ctx.clock.t - 60_000 });
  check('a DIFFERENT root\'s grant issued a minute before the current appointment (beyond S) is refused (obsolete-grant)', role.backupOf === lc(idHex(R1)) && ctx.log('grant-refused').some((r) => r.ctx.why === 'obsolete-grant'));
  await replicate(ctx, TP(18), R2, { gen: gen(R2, 1), issuedAt: ctx.clock.t + 1000 });
  check('a different root\'s grant issued after it re-principals, as today', role.backupOf === lc(idHex(R2)) && role.standbyGen.root === lc(idHex(R2)));
}

console.log('\n[U1] unarmed: a REPLICATE carries no gen or issuedAt, any grant appoints, and no lease ever lapses (4.116.0)');
{
  const ctx = manager({ armed: false });
  await replicate(ctx, TP(13), R1, {});
  check('unarmed: a generation-less grant from nobody\'s principal still creates a backup', ctx.held(TP(13)) && ctx.am._backupTopics.has(TP(13)));
  for (let at = T0 + TICK; at <= T0 + 2 * L; at += TICK) await ctx.tick(at);
  check('unarmed: still appointed after 2 L with no grant', ctx.am._backupTopics.has(TP(13)) && ctx.log('standby-lapsed').length === 0);
  const root = seed(ctx, TP(14)); root.replicas.set(lc(idHex(R1)), { at: T0, via: 'consumed' });
  const p = await ctx.am._syncPush(R1, TP(14), root, 'COHORT_REPLICATE', { full: true });
  const rep = ctx.sent(T.REPLICATE).at(-1);
  check('unarmed: the pushed REPLICATE carries neither gen nor issuedAt', rep && rep.payload.gen === undefined && rep.payload.issuedAt === undefined);
  void p;
}

console.log('\n[A1] armed: the pushed REPLICATE names the generation and its issue stamp; a departing backup names its principal\'s');
{
  const ctx = manager();
  const root = seed(ctx, TP(15));
  await ctx.am._syncPush(R1, TP(15), root, 'COHORT_REPLICATE', { full: true });
  const rep = ctx.sent(T.REPLICATE).at(-1);
  check('from a root: gen = (self, epoch 1), issuedAt = now', rep.payload.gen.root === lc(idHex(SELF)) && rep.payload.gen.epoch === 1 && rep.payload.issuedAt === ctx.clock.t);
  await replicate(ctx, TP(16), R2, { gen: gen(R2, 4), issuedAt: ctx.clock.t });
  await ctx.am._syncPush(X, TP(16), ctx.role(TP(16)), 'REPLICATE', { full: true });
  const rep2 = ctx.sent(T.REPLICATE).at(-1);
  check('from a backup: gen = its principal\'s (R2, 4)', rep2.payload.gen.root === lc(idHex(R2)) && rep2.payload.gen.epoch === 4);
}

console.log(`\nfence_standby_lease: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
