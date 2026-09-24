// fence_role_dead_reap.mjs — reap a topic with no subscribers and no messages
// ON SIGHT (David 2026-09-23: "We should always reap any topic that has no
// subscribers and no messages immediately") — EXCEPT a backup seat, which is a
// standby successor (David 2026-09-24, after Orion and Aster).
//
// THE RULE, in one line: subscribers === 0 AND cache empty ⇒ gone this tick,
// unless the role is a BACKUP, this node's own intent, keyspace-pinned, or
// holding a live metrics lease.
//
// THIS FENCE HAS BEEN WRONG TWICE AND THE HISTORY IS THE POINT.
//
//   4.90.0  the 24 h idle rule. Bounds a role that still has subscribers; does
//           nothing for what west was accruing.
//   4.91.0  the dead reap, with `_backupTopics` exempted. It reaped NOTHING in
//           production — west held 144 roles and logged zero reaps — because
//           the exemption covered exactly the roles the rule was written for.
//           The exemption was added BY ASSOCIATION: _backupTopics sits beside
//           mySubscriptions and _hostedTopics in wants(), so I treated it as
//           local intent. It is not; its only writer is becomeBackup, reached
//           when this node RECEIVES a pushed replica.
//   4.92.0  the exemption removed. West went 144 → 23 and I called it fixed.
//           What it actually did was prune the ROOT ELECTION: a backup renews a
//           subscribe toward its topic every tick, and repairPlane says in as
//           many words that a vanished-root backup which has not re-homed "must
//           never be pruned, or a split-brain topic gets NO root". Measured on
//           4.93.0: an empty backup emits ONE subscribe and is reaped on that
//           same tick. Production showed the residue as a create-and-destroy
//           cycle at 2.8/s on the west bridge, re-seating on every keepalive.
//   4.94.0  the exemption restored DELIBERATELY, knowing what it costs, with
//           the discharge named: loop 1b-bak retires a backup that has re-homed
//           under a live upstream AND heard nothing for BACKUP_EVICT_MS, and it
//           runs earlier in the same tick — after which both reapers apply
//           normally. fence_backup_standby.mjs holds the standby behaviour and
//           the multi-tick cases; this fence holds the dead reap itself.
//
// WHY 4.91.0 AND 4.94.0 ARE NOT THE SAME CHANGE, since the code is nearly
// identical: 4.91.0 exempted backups by mistake and could not say why, so the
// role count looked like an unexplained leak. 4.94.0 exempts them because a
// backup seat is the standby successor for a topic this node is XOR-closest to.
// The count is MEMBERSHIP, and the lever on it is bounding cohort size — not
// reaping standbys. Same line, opposite epistemic position.
//
// "NO SUBSCRIBERS" INCLUDES THIS NODE. peer.sub(), peer.host() and keyspace
// hosting never seat the node in its own role — a root's own SUB self-loops
// without seating — so those three are read AS subscribers here. A live metrics
// lease is left alone too; it is short-lived soft state that expires on its own.
//
// AN OVERREACH THE SUITE CAUGHT (4.92.0): the first version also overrode the
// KEYSPACE PIN, and smoke_keyspace_hosting failed three checks. Keyspace hosting
// exists precisely to retain an empty root as a durable home. D7 pins that.
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

console.log('[D1] AN EMPTY PUSHED BACKUP IS RETAINED — it is the standby successor');
{
  const m = manager();
  seed(m, 1n, { isRoot: false, backupOf: hex(SELF) });        // a replica holding nothing
  await m.refreshTick();
  check('an empty pushed backup survives the dead reap (4.94.0; 4.92.0 took it)', held(m, 1n));
  check('nothing was counted as reaped', (m._rolesReapedDead || 0) === 0, m._rolesReapedDead);
}

console.log('\n[D2] immediacy, for a role that is NOT a backup: reaped the instant it is empty');
{
  const m = manager();
  seed(m, 2n, { isRoot: true, lastTs: T });                    // last message is NOW
  await m.refreshTick();
  check('an empty, subscriber-less root goes now — the 24 h rule is not needed', !held(m, 2n));
}

console.log('\n[D3] a BACKUP THAT HOLDS HISTORY is kept — it was never the question');
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

console.log('\n[D5] the exemption table: what "no subscribers" reads as a subscriber');
{
  const m = manager();
  seed(m, 5n, { isRoot: true }); m.mySubscriptions.set(5n, { since: 0 });
  seed(m, 6n, { isRoot: true }); m._hostedTopics.add(6n);
  seed(m, 7n, { isRoot: true }); m._backupTopics.add(7n);   // standby successor (4.94.0)
  seed(m, 8n, { isRoot: true, metricsOn: T + 60_000 });
  seed(m, 9n, { isRoot: true });                             // nothing at all: the control
  await m.refreshTick();
  check('a topic this node SUBSCRIBED to is kept (peer.sub never seats itself)', held(m, 5n));
  check('a topic this node HOSTS is kept (peer.host contract)', held(m, 6n));
  check('_backupTopics DOES exempt: the seat renews the subscribe that elects the next root, and repairPlane says that path must never be pruned', held(m, 7n));
  check('a live metrics lease is left to expire on its own', held(m, 8n));
  check('the control — no exemption at all — IS reaped, so the table is not vacuous', !held(m, 9n));
  check('exactly ONE of the five was reaped', (m._rolesReapedDead || 0) === 1, m._rolesReapedDead);
}

console.log('\n[D6] the reap is one observable row, tagged why=dead');
{
  const rows = [];
  const m = manager();
  m._log = (level, event, ctx) => rows.push({ event, ctx });
  seed(m, 10n, { isRoot: true });                              // not a backup: reapable
  await m.refreshTick();
  const row = rows.find((r) => r.event === 'pubsub:role-reaped');
  check('one pubsub:role-reaped row', !!row, JSON.stringify(rows.map((r) => r.event)));
  check('tagged why=dead and backup=false', row && row.ctx.why === 'dead' && row.ctx.backup === false, JSON.stringify(row?.ctx));
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
  seed(m, 13n, { isRoot: true });
  await m.refreshTick();
  const after = m.inspectAdmission().reaped;
  check('a dead reap increments reaped.dead and leaves reaped.idle alone', after.dead === 1 && after.idle === 0, JSON.stringify(after));
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
