// fence_backup_standby.mjs — a BACKUP seat is a standby successor, and neither
// reaper may take it. Only the rehomed-idle path retires a backup.
//
// THE DEFECT (mine, 4.92.0, shipped to production 2026-09-24). `deadNow` reaps a
// role with no subscribers and an empty cache on sight, and I deliberately
// removed the backup exemption to do it. An empty backup is not dead weight:
// while it exists it renews a subscribe toward the topic EVERY TICK, and that
// renewal is the root election. repairPlane's own comment at loop 1b-bak:
// "a backup whose root vanished and hasn't re-homed stays subscribed so it can
// win the election (that path must never be pruned, or a split-brain topic gets
// NO root)." 4.92.0 pruned it. Measured on 4.93.0: an empty backup emits ONE
// subscribe and is reaped on that same tick; what production showed was a
// create-and-destroy cycle at 2.8/s on the west bridge, re-seating on every
// keepalive, for 2.4 hours.
//
// THE RULE, from Orion's reading and Aster's correction together. Orion: a
// backup is the designated standby successor and an empty cache is normal for a
// quiet, new or signalling topic. Aster, and this is the load-bearing half:
// sparing a seat only while its root is still KEEPALIVING protects exactly the
// case that does not need protecting, because the obligation is about the root
// being GONE — once freshness lapses, this reaper or the 24 h one takes the same
// seat and defeats the same contract. So a backup seat is exempt from BOTH
// reapers, with no freshness test and no rehomed test, because the design
// already holds the discharge: loop 1b-bak retires a backup that has re-homed
// under a live upstream AND heard nothing for BACKUP_EVICT_MS, and it runs
// earlier in the SAME tick. A retired backup arrives at the teardown loop as an
// ordinary role and both reapers apply to it normally.
//
// WHAT IT COSTS: the resident role count goes back up — those seats are the
// cohort this node is XOR-closest to, so the count is MEMBERSHIP, and the lever
// on it is bounding cohort size, not reaping standbys.
//
// OPEN (Aster): a backup whose root vanished and never re-homes is retained for
// ever, because nothing yet DISCHARGES a standby obligation in that state. S3
// pins the discharge that DOES exist; the missing one is a separate agreement.
//
// Run: node test/fence_backup_standby.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { BACKUP_EVICT_MS, ROLE_IDLE_TTL_MS } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};

const REG   = 0x87n << 248n;
const hex   = (b) => b.toString(16).padStart(66, '0');
const SELF  = REG | 0x011n;
const ROOTN = REG | 0xaa0n;               // the principal: a reachable neighbour
const GONE  = REG | 0xfff0n;              // a principal that is NOT in the mesh
const T     = REG | 0x7001n;
const DAY   = 24 * 60 * 60 * 1000;

function mk({ neighbors = [hex(ROOTN)] } = {}) {
  const clock = { t: 1_000_000 };
  const sends = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => hex(SELF),
    onRoutedMessage: () => {}, onDirectMessage: () => {},
    routeMessage: (target, type, payload) => (sends.push({ target, type, payload }), { consumed: true }),
    neighbors: () => neighbors,
    bridgeId: () => null,
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, sends };
}
/** Seat a backup exactly as an inbound REPLICATE does: empty payload, principal set. */
function seatBackup(am, clock, principal = ROOTN) {
  const role = makeRole(T, false, clock.t);
  am.axonRoles.set(T, role);
  am._rootClaim.becomeBackup(T, role, hex(principal).toLowerCase());
  return role;
}
const subs = (sends) => sends.filter((s) => s.type === 'pubsub:sub');
const held = (am) => am.axonRoles.has(T);

console.log('backup standby — neither reaper may take a standby successor\n');

// ── S1. ROOT LIVE AND KEEPALIVING, MANY TICKS ────────────────────────────
// The multi-tick case the predecessor suites could not see: they run ONE tick,
// and on 4.92.0 the seat survives exactly that long.
{
  const { am, clock, sends } = mk();
  const role = seatBackup(am, clock);
  let ticksWithSub = 0;
  for (let i = 0; i < 10; i++) {
    sends.length = 0;
    clock.t += 5_000;
    role.lastReplicaAt = clock.t;                 // the principal keeps keepaliving
    await am.refreshTick();
    if (subs(sends).length) ticksWithSub++;
  }
  ok('1a. the seat survives 10 ticks under a live root', held(am));
  ok('1b. and renews its subscribe on EVERY tick, not just the first',
    ticksWithSub === 10, `ticks with a SUB: ${ticksWithSub}/10`);
  ok('1c. nothing was reaped', (am._rolesReapedDead || 0) === 0 && (am._rolesReapedIdle || 0) === 0);
}

// ── S2. VANISHED ROOT, NO RE-HOME — the case the contract exists for ─────
{
  const { am, clock, sends } = mk({ neighbors: [] });     // principal not in the mesh
  const role = seatBackup(am, clock, GONE);
  role.lastReplicaAt = clock.t;                           // last contact: now, then silence
  clock.t += BACKUP_EVICT_MS * 3;                         // far past the evict window
  sends.length = 0;
  await am.refreshTick();
  ok('2a. a backup whose root VANISHED is retained past BACKUP_EVICT_MS', held(am));
  ok('2b. …and is still subscribing, which is how it wins the election',
    subs(sends).length > 0, JSON.stringify(sends.map((s) => s.type)));
  ok('2c. it never re-homed, so the rehomed-idle path correctly did not fire',
    am._backupTopics.has(T) && (am.axonRoles.get(T)?.backupOf ?? null) !== null);

  // …and the 24 h idle reaper must not finish what the dead reaper could not.
  // THIS IS ASTER'S CORRECTION: a freshness-only exemption would lose here.
  clock.t += DAY + 60_000;
  sends.length = 0;
  await am.refreshTick();
  ok('2d. the 24 h IDLE reaper does not take it either — freshness-only protection would have',
    held(am), `idleReaps=${am._rolesReapedIdle || 0}`);
  ok('2e. still subscribing a day later', subs(sends).length > 0);
}

// ── S3. THE DISCHARGE THAT EXISTS: re-home, go idle, retire, THEN reap ───
{
  const { am, clock, sends } = mk();                      // ROOTN is reachable
  const role = seatBackup(am, clock);
  role.lastReplicaAt = clock.t;
  am._upstream.set(T, [hex(ROOTN).toLowerCase()]);        // re-homed under a LIVE root
  clock.t += BACKUP_EVICT_MS + 60_000;                    // and it went quiet
  await am.refreshTick();
  ok('3a. a REHOMED, idle backup is retired by the pre-existing path', !am._backupTopics.has(T));
  ok('3b. …and, no longer a backup, it is reaped in the SAME tick', !held(am));
  ok('3c. counted as a dead reap, which is the ordinary path doing its job',
    (am._rolesReapedDead || 0) === 1, `dead=${am._rolesReapedDead}`);
}

// ── S4. PROMOTION: a backup that wins is a root, not a standby ───────────
{
  const { am, clock } = mk();
  const role = seatBackup(am, clock);
  role.cache.push({ msgId: 'm1', publishTs: clock.t, json: '{}', bytes: 80 });
  role.cacheIds.add('m1');
  am._rootClaim.retireBackup(T, role, 'promoted');
  role.isRoot = true;
  clock.t += 5_000;
  await am.refreshTick();
  ok('4a. a promoted backup keeps its role and its history', held(am) && am.axonRoles.get(T).isRoot);
  ok('4b. …and is no longer counted as a backup seat', !am._backupTopics.has(T));
}

// ── S5. BOTH REAPERS, ONE TABLE — what is and is not exempt ─────────────
{
  const { am, clock } = mk();
  const b = seatBackup(am, clock);                        // backup, empty, ancient
  b.lastTs = clock.t - (400 * DAY);
  const plain = makeRole(REG | 0x7002n, false, clock.t - (400 * DAY));
  plain.lastTs = clock.t - (400 * DAY);
  am.axonRoles.set(REG | 0x7002n, plain);                 // NOT a backup, same shape
  clock.t += 5_000;
  await am.refreshTick();
  ok('5a. the backup survives both reapers at 400 days idle', held(am));
  ok('5b. an identical NON-backup role is still reaped — the exemption is narrow',
    !am.axonRoles.has(REG | 0x7002n));
  ok('5c. ROLE_IDLE_TTL_MS is unchanged at 24 h', ROLE_IDLE_TTL_MS === DAY);
}

// ── S6. REGRESSION: the ordinary dead reap is untouched ─────────────────
{
  const { am, clock } = mk();
  am.axonRoles.set(T, makeRole(T, true, clock.t));        // empty root, no subscribers
  clock.t += 5_000;
  await am.refreshTick();
  ok('6. an empty, subscriber-less, NON-backup root is still reaped on sight', !held(am));
}

// `n` counts passes only (it is the ok-number), so it IS the pass count —
// subtracting `fail` from it under-reports on a red run, which is exactly when
// the number matters.
console.log(`\nResult: ${n} passed, ${fail} failed`);
if (fail) process.exit(1);
