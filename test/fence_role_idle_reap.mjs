// fence_role_idle_reap.mjs — the idle-role reap (David 2026-09-23).
//
// THE GAP THIS FENCES. A node that wins ROOT for a topic kept the seat for ever.
// Measured on the west production bridge, 2026-09-23: 141 roles over 141
// DISTINCT topics, 45 of them rooted, with ZERO children and ZERO cached
// messages, re-accruing to 193 within three minutes of a restart. That drove its
// `saturated` flag and ~39% of one core while it held a single connection. The
// repair plane's teardown could not reach them: it fires only when a role has no
// subscribers AND is not root-with-history AND is not keyspace-pinned AND is not
// a backup AND has no metrics lease AND is not locally subscribed or hosted.
//
// THE RULE. An EMPTY cache whose LAST MESSAGE is older than ROLE_IDLE_TTL_MS
// (24 h) ends the role, whatever else would retain it: root or child, with
// subscribers or without. `role.lastTs` is the stamp of the last message the
// role emitted or held and SURVIVES the cache emptying, because `_expireCache`
// removes cache entries and never touches it — so it is the honest answer to
// "when was the last message". A role that never carried a message is measured
// from its admission stamp; a role with neither is left alone rather than
// guessed at.
//
// TWO DELIBERATE EXEMPTIONS: `mySubscriptions` and `_hostedTopics`. Those are
// this node's own explicit intent through peer.sub() and peer.host(). Reaping
// them would break a local API contract instead of reclaiming junk, and on a
// bridge they are a handful of directory topics.
//
// RECOVERABLE BY CONSTRUCTION: a subscriber that still wants the topic renews on
// its own cadence and the role is rebuilt, which is why reaping a role that
// still has subscribers is safe.
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { ROLE_IDLE_TTL_MS } from '../src/pubsub/constants.js';
import { depositDispatchCapability } from '../src/registry/index.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };

const SELF = (0x89n << 248n) | 0x1000n;
const hex = (b) => b.toString(16).padStart(66, '0');
const DAY = 24 * 60 * 60 * 1000;
let T = 1_000_000_000_000;

function manager(opts = {}) {
  const routed = new Map();
  const dht = {
    verdictsSupported: true,
    routeMessage: async () => ({ consumed: false }),
    getSelfId: () => hex(SELF),
    onRoutedMessage: (type, h) => routed.set(type, h),
    onDirectMessage() {},
    neighbors: () => [], bridgeId: () => null,
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
  };
  depositDispatchCapability(dht, { routed: (type, h) => dht.onRoutedMessage(type, h) });
  const m = new AxonaManager({ dht, ...opts });
  m._log = () => {};
  m._now = () => T;
  return m;
}
/** Seed a role directly into the manager, as the wire handlers would. */
function seed(m, topic, { isRoot = true, lastTs = 0, createdAt = T, cache = 0, subs = 0, children = 0 } = {}) {
  const role = makeRole(topic, isRoot, createdAt);
  role.lastTs = lastTs;
  for (let i = 0; i < cache; i++) {
    role.cache.push({ msgId: `m${i}`, publishTs: lastTs || T, json: '{}', bytes: 80 });
    role.cacheIds.add(`m${i}`);
  }
  for (let i = 0; i < subs; i++) {
    const h = `sub${i}`.padEnd(66, '0');
    role.subscribers.set(h, { since: 0, lastRenewed: T });
    if (i < children) role.children.add(h);
  }
  m.axonRoles.set(topic, role);
  return role;
}
const held = (m, t) => m.axonRoles.has(t);

console.log('[R0] the constant and the idle measure');
{
  check('ROLE_IDLE_TTL_MS is 24 h', ROLE_IDLE_TTL_MS === DAY, String(ROLE_IDLE_TTL_MS));
  const m = manager();
  const r1 = makeRole(1n, true, T - 5 * DAY); r1.lastTs = T - 2 * DAY;
  check('idle is measured from the LAST MESSAGE when there was one', m._roleIdleMs(r1, T) === 2 * DAY, m._roleIdleMs(r1, T));
  const r2 = makeRole(2n, true, T - 5 * DAY);
  check('…and from the admission stamp when there never was one', m._roleIdleMs(r2, T) === 5 * DAY, m._roleIdleMs(r2, T));
  const r3 = makeRole(3n, true, null);
  check('a role with neither stamp reports 0, so age can never reap it', m._roleIdleMs(r3, T) === 0);
  const r4 = makeRole(4n, true, T + 60_000);
  check('a stamp in the future never goes negative', m._roleIdleMs(r4, T) === 0);
}

console.log('\n[R1] an idle EMPTY role is reaped even as root WITH subscribers and children');
{
  const m = manager();
  seed(m, 10n, { isRoot: true, lastTs: T - (DAY + 60_000), subs: 3, children: 2 });
  await m.refreshTick();
  check('reaped: empty cache, last message older than 24 h', !held(m, 10n));
  check('counted', m._rolesReapedIdle === 1, m._rolesReapedIdle);
}

console.log('\n[R2] what the reap must NOT touch');
{
  const m = manager();
  seed(m, 20n, { lastTs: T - 1_000, cache: 1 });                          // holds history (fresh entry)
  seed(m, 21n, { lastTs: T - (DAY - 60_000), subs: 1 });                  // inside the TTL
  seed(m, 22n, { lastTs: 0, createdAt: T - 60_000, subs: 1 });            // young, never published
  seed(m, 23n, { lastTs: 0, createdAt: null, subs: 1 });                  // no stamp at all
  await m.refreshTick();
  check('a role still holding cached messages is kept', held(m, 20n));
  check('an empty cache inside the TTL is kept', held(m, 21n));
  check('a young role that never published is kept', held(m, 22n));
  check('a role with no stamp at all is kept (never guessed at)', held(m, 23n));
  check('none of them counted as reaped', (m._rolesReapedIdle || 0) === 0, m._rolesReapedIdle);
}

console.log('\n[R2b] the intended interaction with cache expiry');
{
  // A cache whose entries are older than the message TTL is emptied by
  // _expireCache EARLIER IN THE SAME TICK, and the role is then reapable. That
  // is the rule working as written, not a bypass of the "empty cache" premise:
  // the messages are gone and the last one is older than 24 h.
  const m = manager();
  const r = seed(m, 24n, { lastTs: T - (DAY + 60_000), cache: 1, subs: 1 });
  check('precondition: the role starts with a cached message', r.cache.length === 1);
  await m.refreshTick();
  check('the stale entry expires and the role is then reaped in the same tick', !held(m, 24n));
  check('counted as an idle reap', m._rolesReapedIdle === 1, m._rolesReapedIdle);
}

console.log('\n[R3] a role that never carried a message ages out on its admission stamp');
{
  const m = manager();
  seed(m, 30n, { lastTs: 0, createdAt: T - (DAY + 60_000), subs: 1 });
  await m.refreshTick();
  check('reaped on createdAt when lastTs is 0 — the west shape exactly', !held(m, 30n));
}

console.log('\n[R4] the two deliberate exemptions: this node\'s own intent');
{
  const m = manager();
  seed(m, 40n, { lastTs: T - (DAY + 60_000) });
  seed(m, 41n, { lastTs: T - (DAY + 60_000) });
  m.mySubscriptions.set(40n, { since: 0 });
  m._hostedTopics.add(41n);
  await m.refreshTick();
  check('a locally SUBSCRIBED topic is exempt (peer.sub contract)', held(m, 40n));
  check('a locally HOSTED topic is exempt (peer.host contract)', held(m, 41n));
}

console.log('\n[R5] the TTL is configurable and 0 disables the reap');
{
  const m = manager();
  m._roleIdleTtlMs = 0;
  seed(m, 50n, { lastTs: T - 400 * DAY, subs: 1 });   // subscribers, so only the idle reap could take it
  await m.refreshTick();
  check('TTL 0 keeps even a role idle for over a year', held(m, 50n));
  const m2 = manager();
  m2._roleIdleTtlMs = 60_000;
  seed(m2, 51n, { lastTs: T - 120_000, subs: 1 });
  await m2.refreshTick();
  check('a shorter TTL reaps sooner', !held(m2, 51n));
}

console.log('\n[R6] the pre-existing teardown path still works (regression)');
{
  const m = manager();
  seed(m, 60n, { isRoot: false, lastTs: T, createdAt: T, subs: 0 });      // fresh, no subscribers
  await m.refreshTick();
  check('a subscriber-less, cache-less, fresh child still tears down as before', !held(m, 60n));
  check('…and is NOT counted as an idle reap', (m._rolesReapedIdle || 0) === 0, m._rolesReapedIdle);
}

console.log('\n[R7] the reap is emitted as an observable row');
{
  const rows = [];
  const m = manager();
  m._log = (level, event, ctx) => rows.push({ level, event, ctx });
  seed(m, 70n, { isRoot: true, lastTs: T - (DAY + 5_000), subs: 2, children: 1 });
  await m.refreshTick();
  const row = rows.find((r) => r.event === 'pubsub:role-reaped-idle');
  check('one pubsub:role-reaped-idle row', !!row, JSON.stringify(rows.map((r) => r.event)));
  check('it carries isRoot, subscribers, children, idleMs and everPublished',
    row && row.ctx.isRoot === true && row.ctx.subscribers === 2 && row.ctx.children === 1
    && row.ctx.idleMs >= DAY && row.ctx.everPublished === true, JSON.stringify(row?.ctx));
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
