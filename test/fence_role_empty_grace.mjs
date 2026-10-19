#!/usr/bin/env node
// =====================================================================
// fence_role_empty_grace.mjs — the EMPTY-ROLE GRACE (David 2026-10-19:
// "address the root issue with unreaped topics raised by Aster").
//
// WHAT WAS MEASURED BEFORE A LINE WAS WRITTEN. Aster's 2026-10-18 fleet census
// (council f64a8dbd) counted 1,011 ROOT roles with zero seated subscribers and
// zero cached bodies, 483 of them on Air. The sfo3 grizzly1 journal over seven
// days of 4.114.1 showed the reapers doing exactly what their fences say:
// 1,315 idle root reaps, 2,266 reaps in the 24 h bucket and none later except
// 52 standby backups discharged by rehome; and the same relay forming 190–285
// roots a day for single-use test topics in region 128. Its census row read
// 188/188 empty: one day of churn. Nothing was missed; every empty root was
// inside the idle TTL, retained by the keyspace pin with NOTHING in it.
//
// THE RULE. A keyspace host's pin on an empty root is a bounded GRACE
// (ROLE_EMPTY_GRACE_MS, 5 min), not an exemption for the whole idle TTL. A role
// HOLDS NOTHING when every retention reason is absent by the node's own state:
// no bodies, no active tombstone or merge custody, no seated subscriber or
// child, not a backup seat, no metrics lease, no local intent. `emptySince` is
// the first tick that saw it so and is cleared the moment it holds anything,
// so the grace restarts from the LAST time the role held something — never
// from a beacon, a renewal or a duplicate kill. A spent grace drops the pin and
// the on-sight rule takes the role, tagged why=empty and counted apart. The
// idle TTL, the on-sight rule and every exemption are otherwise unchanged;
// ROLE_EMPTY_GRACE_MS 0 restores the eternal pin.
//
// AND THE LAUNDERING. _onKill stamped lastTs/seq BEFORE _applyKill's
// tombstone gate, so a re-sent kill advanced the idle anchor and burned a seq
// slot (Aster lifecycle v0.2 item 3). A duplicate kill is now acked without a
// stamp.
//
// Mutants (red): drop `&& !graceSpent` from keyspacePinned → E1–E5, E8 fail;
// drop the existingTomb guard in _onKill → E9 fails.
// Run: node test/fence_role_empty_grace.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { ROLE_EMPTY_GRACE_MS, ROLE_IDLE_TTL_MS } from '../src/pubsub/constants.js';
import { depositDispatchCapability } from '../src/registry/index.js';
import { realTopic, makeManagerAt, rootWithBodies, signedKill, bareMeta, idHex as fxHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };

const SELF = (0x89n << 248n) | 0x2000n;
const hex = (b) => b.toString(16).padStart(66, '0');
const T = 1_700_000_000_000;
const MIN = 60_000, HOUR = 3_600_000;
const GRACE = ROLE_EMPTY_GRACE_MS;

function manager({ keyspace = true } = {}) {
  const rows = [];
  const dht = {
    verdictsSupported: true,
    routeMessage: async () => ({ consumed: false }),
    getSelfId: () => hex(SELF),
    onRoutedMessage() {}, onDirectMessage() {},
    neighbors: () => [], bridgeId: () => null,
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
  };
  depositDispatchCapability(dht, { routed: (type, h) => dht.onRoutedMessage(type, h) });
  const m = new AxonaManager({ dht });
  m._log = (level, event, ctx) => rows.push({ event, ctx });
  m.clock = T;
  m._now = () => m.clock;
  m._hostKeyspace = keyspace;
  m.rows = rows;
  return m;
}
function seed(m, topic, { isRoot = true, cache = 0, subs = 0, lastTs = T } = {}) {
  const role = makeRole(topic, isRoot, T);
  role.lastTs = lastTs;
  for (let i = 0; i < cache; i++) { role.cache.push({ msgId: `m${i}`, publishTs: T, json: '{}', bytes: 80 }); role.cacheIds.add(`m${i}`); }
  for (let i = 0; i < subs; i++) role.subscribers.set(`s${i}`.padEnd(66, '0'), { since: 0, lastRenewed: T });
  m.axonRoles.set(topic, role);
  return role;
}
const TP = (k) => BigInt(k) << 240n;   // distinct 12-hex log prefixes per topic
const held = (m, t) => m.axonRoles.has(t);
const tick = async (m, at) => { m.clock = at; await m.refreshTick(); };
const reapRow = (m, t) => m.rows.find((r) => r.event === 'pubsub:role-reaped' && r.ctx.topic === hex(t).slice(0, 12));

console.log('fence_role_empty_grace: the keyspace pin on an empty root is a bounded grace');
check('ROLE_EMPTY_GRACE_MS is 5 min, two orders above the birth probe / re-send / verify windows', GRACE === 5 * MIN, GRACE);

console.log('\n[E1] retained within the grace, reaped the tick after it, tagged why=empty and counted apart');
{
  const m = manager();
  seed(m, TP(1));
  await tick(m, T);
  check('tick 0: the empty root is held and emptySince is stamped', held(m, TP(1)) && m.axonRoles.get(TP(1)).emptySince === T);
  await tick(m, T + GRACE);                                 // exactly the grace: strictly greater is required
  check('at exactly the grace it is still held (strictly greater, like the TTL)', held(m, TP(1)));
  await tick(m, T + GRACE + 1);
  check('one ms past the grace it is reaped', !held(m, TP(1)));
  const row = reapRow(m, TP(1));
  check('the row says why=empty with emptyMs = the grace + 1', row && row.ctx.why === 'empty' && row.ctx.emptyMs === GRACE + 1, JSON.stringify(row?.ctx));
  const rep = m.inspectAdmission().reaped;
  check('counted as reaped.empty, not dead or idle', rep.empty === 1 && rep.dead === 0 && rep.idle === 0, JSON.stringify(rep));
}

console.log('\n[E2] the grace restarts from the LAST time the role held something: a seat');
{
  const m = manager();
  const r = seed(m, TP(2), { subs: 1 });
  await tick(m, T);
  check('with a seated subscriber emptySince stays null', held(m, TP(2)) && r.emptySince === null);
  for (const s of r.subscribers.values()) s.lastRenewed = T + 10 * MIN;   // the seat renews (DROP_MS is the kernel's own eviction, not this rule)
  await tick(m, T + 10 * MIN);
  check('…for as long as the seat lasts (10 min, well past the grace)', held(m, TP(2)) && r.emptySince === null);
  r.subscribers.clear();                                    // the subscriber leaves at T+10m
  await tick(m, T + 10 * MIN);
  check('the tick after the seat drops stamps emptySince then', r.emptySince === T + 10 * MIN);
  await tick(m, T + 10 * MIN + GRACE);
  check('held through a full grace from the drop', held(m, TP(2)));
  await tick(m, T + 10 * MIN + GRACE + 1);
  check('reaped one ms after that', !held(m, TP(2)));
}

console.log('\n[E3] …or a body: a cached message clears emptySince, and its expiry restarts the clock');
{
  const m = manager();
  const r = seed(m, TP(3));
  await tick(m, T);
  r.cache.push({ msgId: 'b', publishTs: T + 2 * MIN, json: '{}', bytes: 80 }); r.cacheIds.add('b');   // a body lands at T+2m
  await tick(m, T + 2 * MIN);
  check('a body clears emptySince', held(m, TP(3)) && r.emptySince === null);
  r.cache.length = 0; r.cacheIds.clear();                   // emptied at T+4m (TTL expiry, or a kill whose tombstone has gone)
  await tick(m, T + 4 * MIN);
  check('emptied again: emptySince restarts at T+4m, not at T', r.emptySince === T + 4 * MIN);
  await tick(m, T + 4 * MIN + GRACE);
  check('held for a full grace from the second emptying', held(m, TP(3)));
  await tick(m, T + 4 * MIN + GRACE + 1);
  check('then reaped', !held(m, TP(3)));
}

console.log('\n[E4] an ACTIVE TOMBSTONE is custody (4.115.0 _custodyAlive): held past the grace until it expires');
{
  const m = manager();
  const r = seed(m, TP(4));
  r.tombstones.set('k', { exp: T + 2 * HOUR, killTs: T, signer: null, seq: 1 });
  await tick(m, T + HOUR);
  check('one hour in, the tombstone holds the role and emptySince is null', held(m, TP(4)) && r.emptySince === null);
  await tick(m, T + 2 * HOUR + 1);                         // the tick expires the tombstone, then reads the role as empty
  check('the tick that expires the tombstone stamps emptySince on that tick', held(m, TP(4)) && r.emptySince === T + 2 * HOUR + 1 && r.tombstones.size === 0);
  await tick(m, T + 2 * HOUR + 1 + GRACE + 1);
  check('reaped one grace after the tombstone expired', !held(m, TP(4)));
}

console.log('\n[E5] every other exemption still holds past the grace; the control is reaped');
{
  const m = manager();
  seed(m, TP(5)); m.mySubscriptions.set(TP(5), { since: 0 });
  seed(m, TP(6)); m._hostedTopics.add(TP(6));
  seed(m, TP(7)); m._backupTopics.add(TP(7));
  const lease = seed(m, TP(8)); lease.metricsOn = T + HOUR;
  const child = seed(m, TP(9), { isRoot: false, cache: 0, subs: 1 });   // a child with a seat
  const bk = seed(m, TP(10), { isRoot: false }); bk.backupOf = hex(SELF);
  seed(m, TP(11));                                             // the control: nothing at all
  await tick(m, T);
  for (const s of child.subscribers.values()) s.lastRenewed = T + GRACE;   // the seat renews within DROP_MS
  await tick(m, T + GRACE + 1);
  check('peer.sub() intent is held', held(m, TP(5)));
  check('peer.host(topic) intent is held', held(m, TP(6)));
  check('a standby seat (_backupTopics) is held', held(m, TP(7)));
  check('a live metrics lease is held', held(m, TP(8)));
  check('a seated child is held', held(m, TP(9)));
  check('a pushed backup is held', held(m, TP(10)));
  check('the control — nothing at all — is reaped as empty', !held(m, TP(11)) && reapRow(m, TP(11))?.ctx.why === 'empty');
  check('exactly one empty reap', m.inspectAdmission().reaped.empty === 1);
}

console.log('\n[E6] not a keyspace host: the on-sight rule is unchanged (why=dead, on the first tick)');
{
  const m = manager({ keyspace: false });
  seed(m, TP(12));
  await tick(m, T);
  const rep = m.inspectAdmission().reaped;
  check('reaped on sight as dead, not empty', !held(m, TP(12)) && reapRow(m, TP(12))?.ctx.why === 'dead' && rep.dead === 1 && rep.empty === 0, JSON.stringify(rep));
}

console.log('\n[E7] ROLE_EMPTY_GRACE_MS 0 restores the eternal pin — bounded only by the idle TTL');
{
  const m = manager();
  m._roleEmptyGraceMs = 0;
  seed(m, TP(13), { lastTs: 0 });                              // never published: idle anchor = createdAt = T
  await tick(m, T + HOUR);
  check('with the grace disabled an empty root is held for an hour', held(m, TP(13)));
  await tick(m, T + ROLE_IDLE_TTL_MS + 1);
  check('…and the idle TTL still takes it, tagged why=idle', !held(m, TP(13)) && reapRow(m, TP(13))?.ctx.why === 'idle');
}

console.log('\n[E8] the grace runs ahead of the idle TTL: a root whose kill was 23 h ago goes at the grace, not at 24 h');
{
  const m = manager();
  seed(m, TP(14), { lastTs: T - 23 * HOUR });                 // last stamp = a kill 23 h ago; tombstone long gone
  await tick(m, T);
  check('held on the first tick that sees it empty', held(m, TP(14)));
  await tick(m, T + GRACE + 1);
  const row = reapRow(m, TP(14));
  check('reaped as empty with idleMs ≈ 23 h, inside the idle TTL', !held(m, TP(14)) && row?.ctx.why === 'empty' && row.ctx.idleMs > 22 * HOUR && row.ctx.idleMs < ROLE_IDLE_TTL_MS, JSON.stringify(row?.ctx));
}

console.log('\n[E9] a DUPLICATE kill is acked without a stamp: lastTs and seq do not move, the tombstone keeps its stamp');
{
  const L = await realTopic('empty-grace-dup-kill');
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  ctx.clock.t += 1000;
  await ctx.am._onKill({ topicId: fxHex(L.topicId), kill }, bareMeta(ctx, L.publisher));
  const ts1 = role.lastTs, seq1 = role.seq, tomb1 = { ...role.tombstones.get(ids[0]) };
  check('first kill: tombstone recorded with the root stamp, body gone', role.tombstones.size === 1 && tomb1.killTs === ts1 && tomb1.seq === seq1 && role.cache.length === 0);
  const acksBefore = ctx.routed.filter((r) => r.type === 'pubsub:ingestack').length;
  ctx.clock.t += 60_000;
  await ctx.am._onKill({ topicId: fxHex(L.topicId), kill }, bareMeta(ctx, L.publisher));   // the same kill again, a minute later
  const tomb2 = role.tombstones.get(ids[0]);
  check('duplicate kill: lastTs and seq unchanged', role.lastTs === ts1 && role.seq === seq1, `${role.lastTs}/${ts1} ${role.seq}/${seq1}`);
  check('the tombstone keeps its original killTs and seq', tomb2.killTs === tomb1.killTs && tomb2.seq === tomb1.seq && role.tombstones.size === 1);
  check('…and the duplicate was still acked (idempotent success)', ctx.routed.filter((r) => r.type === 'pubsub:ingestack').length === acksBefore + 1);
}

console.log(`\nfence_role_empty_grace: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
