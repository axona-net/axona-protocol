// fence_role_dead_reap.mjs — reap a topic with no subscribers and no messages
// ON SIGHT (David 2026-09-23: "We should always reap any topic that has no
// subscribers and no messages immediately").
//
// WHAT WAS ACTUALLY PINNING THEM. The 24 h idle rule bounds a role that still
// has subscribers. It does nothing for the west production bridge's accrual,
// because those roles were PUSHED BACKUP REPLICAS: a root replicates to the
// K-closest, the receiver sets `role.backupOf`, and the teardown clause
// `!role.backupOf` then held the seat for ever — even with an empty cache, which
// replicates nothing. West carried 141 roles over 141 distinct topics with zero
// children and zero cached messages, and 96 of them were not even root.
//
// THE RULE. subscribers === 0 AND cache empty ⇒ gone this tick. What it uniquely
// overrides is a pushed backup replica, because an empty replica replicates
// nothing and the principal re-pushes the moment there is history.
//
// "NO SUBSCRIBERS" INCLUDES THIS NODE. peer.sub(), peer.host() and keyspace
// hosting never seat the node in its own role — a root's own SUB self-loops
// without seating — so those three are read AS subscribers here.
// `_backupTopics` is NOT one of them: its only writer is becomeBackup on an
// INBOUND replica, so it is the same inbound state as role.backupOf. Exempting
// it in 4.91.0 blocked the reap on exactly the roles it was written for — west
// held 144 roles with zero children and zero cache and logged ZERO reaps. Without that, "no subscribers" would silently reap a topic
// the node itself asked for. A live metrics lease is left alone too; it is
// short-lived soft state that expires on its own.
//
// AN OVERREACH THE SUITE CAUGHT: the first version of this also overrode the
// KEYSPACE PIN, and smoke_keyspace_hosting failed three checks. Keyspace hosting
// exists precisely to retain an empty root as a durable home, so it belongs with
// the local-intent group, not with the pushed replica. D7 pins that.
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { depositDispatchCapability } from '../src/registry/index.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };

const SELF = (0x89n << 248n) | 0x2000n;
const hex = (b) => b.toString(16).padStart(66, '0');
const T = 1_700_000_000_000;

function manager() {
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
  const m = new AxonaManager({ dht });
  m._log = () => {};
  m._now = () => T;
  return m;
}
function seed(m, topic, { isRoot = false, backupOf = null, cache = 0, subs = 0, lastTs = T, metricsOn = 0 } = {}) {
  const role = makeRole(topic, isRoot, T);
  role.lastTs = lastTs;
  role.backupOf = backupOf;
  role.metricsOn = metricsOn;
  for (let i = 0; i < cache; i++) {
    role.cache.push({ msgId: `m${i}`, publishTs: T, json: '{}', bytes: 80 });
    role.cacheIds.add(`m${i}`);
  }
  for (let i = 0; i < subs; i++) role.subscribers.set(`s${i}`.padEnd(66, '0'), { since: 0, lastRenewed: T });
  m.axonRoles.set(topic, role);
  return role;
}
const held = (m, t) => m.axonRoles.has(t);

console.log('[D1] the west shape: an EMPTY PUSHED BACKUP is reaped on sight');
{
  const m = manager();
  seed(m, 1n, { isRoot: false, backupOf: hex(SELF) });        // a replica holding nothing
  await m.refreshTick();
  check('empty pushed backup replica is gone this tick', !held(m, 1n));
  check('counted as dead, not idle', m._rolesReapedDead === 1 && (m._rolesReapedIdle || 0) === 0,
    `${m._rolesReapedDead}/${m._rolesReapedIdle}`);
}

// NOTE on coverage, from the negative test: with `deadNow` removed from the
// teardown condition ONLY D1 fails. D2 and D4-D6 are satisfied by the
// pre-existing path, which already reaps a subscriber-less empty role that is
// neither a backup nor keyspace-pinned. The new clause's unique contribution is
// exactly the empty PUSHED BACKUP (and the keyspace pin, dead code on a bridge),
// which is what west was accruing. D2 states the required behaviour rather than
// covering the new line.
console.log('\n[D2] immediacy: a role created THIS INSTANT is still reaped');
{
  const m = manager();
  seed(m, 2n, { isRoot: true, lastTs: T });                    // last message is NOW
  await m.refreshTick();
  check('an empty, subscriber-less root goes now — the 24 h rule is not needed', !held(m, 2n));
}

console.log('\n[D3] a BACKUP THAT HOLDS HISTORY is kept — the replica still replicates');
{
  const m = manager();
  seed(m, 3n, { backupOf: hex(SELF), cache: 1 });
  await m.refreshTick();
  check('a backup with cached history survives', held(m, 3n));
}

console.log('\n[D4] anything with a subscriber survives, and falls to the 24 h rule instead');
{
  const m = manager();
  seed(m, 4n, { isRoot: true, subs: 1, lastTs: T });           // fresh, subscribed
  await m.refreshTick();
  check('a subscribed empty role is NOT dead-reaped', held(m, 4n));
  check('…and was not counted', (m._rolesReapedDead || 0) === 0);
}

console.log('\n[D5] "no subscribers" counts THIS NODE\'S OWN intent as a subscriber');
{
  const m = manager();
  seed(m, 5n, { isRoot: true }); m.mySubscriptions.set(5n, { since: 0 });
  seed(m, 6n, { isRoot: true }); m._hostedTopics.add(6n);
  seed(m, 7n, { isRoot: true }); m._backupTopics.add(7n);   // inbound state, NOT local intent
  seed(m, 8n, { isRoot: true, metricsOn: T + 60_000 });
  await m.refreshTick();
  check('a topic this node SUBSCRIBED to is kept (peer.sub never seats itself)', held(m, 5n));
  check('a topic this node HOSTS is kept (peer.host contract)', held(m, 6n));
  check('_backupTopics does NOT exempt: its only writer is becomeBackup on an inbound replica, so it is the SAME thing as role.backupOf and must not protect an empty seat', !held(m, 7n));
  check('a live metrics lease is left to expire on its own', held(m, 8n));
  check('exactly ONE of the four was reaped — the inbound backup membership', (m._rolesReapedDead || 0) === 1, m._rolesReapedDead);
}

console.log('\n[D6] the reap is one observable row, tagged why=dead');
{
  const rows = [];
  const m = manager();
  m._log = (level, event, ctx) => rows.push({ event, ctx });
  seed(m, 9n, { isRoot: false, backupOf: hex(SELF) });
  await m.refreshTick();
  const row = rows.find((r) => r.event === 'pubsub:role-reaped');
  check('one pubsub:role-reaped row', !!row, JSON.stringify(rows.map((r) => r.event)));
  check('tagged why=dead and backup=true', row && row.ctx.why === 'dead' && row.ctx.backup === true, JSON.stringify(row?.ctx));
}

console.log('\n[D7] keyspace hosting is LOCAL INTENT and is not overridden');
{
  const m = manager();
  m._hostKeyspace = true;
  seed(m, 11n, { isRoot: true });                      // empty root, no subscribers
  seed(m, 12n, { isRoot: false });                     // empty NON-root: the pin only covers roots
  await m.refreshTick();
  check('a keyspace-pinned empty ROOT is retained (smoke_keyspace_hosting pins this)', held(m, 11n));
  check('a non-root empty role is still reaped — the pin covers roots only', !held(m, 12n));
}

console.log('\n[D8] the reap is COUNTED WHERE AN OPERATOR CAN SEE IT (inspectAdmission)');
{
  // A climbing role count is ambiguous unless you can tell whether the reaper is
  // firing. The counters existed on the manager and NOTHING surfaced them, so a
  // bridge holding 84 empty roles read exactly like a bridge whose reaper had
  // stopped. inspectAdmission is what /healthz and /diag already publish.
  const m = manager();
  const before = m.inspectAdmission().reaped;
  check('inspectAdmission reports a reaped block from the start', before && before.dead === 0 && before.idle === 0, JSON.stringify(before));
  seed(m, 13n, { isRoot: false, backupOf: hex(SELF) });
  await m.refreshTick();
  const after = m.inspectAdmission().reaped;
  check('a dead reap increments reaped.dead and leaves reaped.idle alone', after.dead === 1 && after.idle === 0, JSON.stringify(after));
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
