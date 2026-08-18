// fence_subscribe_unpin.mjs — a subscriber must not renew forever toward a corpse.
//
// THE DEFECT (David, 2026-08-01, from a wedged axona.chat window that only a
// RELOAD recovered): "perhaps the sub-node it was attempting to resubscribe via
// was no longer available, but the re-subscription was not properly forwarded
// after that failure in routing."
//
// _upstream is the pin — "the relay we renew toward", written by _onDeliver from
// the DELIVER `from`. _sendSubscribe ALWAYS prefers it and consults _rootHint_
// only when the pin is EMPTY. So the pin has to be dropped by something, and
// there are exactly two droppers:
//
//   repairPlane.pubsubPeerDied  — fires only for a peer we hold a CHANNEL to
//   refreshTick role teardown   — fires only when the role is being reaped
//
// A relay reached through ROUTING — no direct synapse — can die with neither
// firing. We never see a channel close, so pubsubPeerDied is silent; the SUB
// exhausts somewhere in the mesh; and _emitSubscribe stamps the renewal
// obligation DISCHARGED the instant the send is on the wire:
//
//     if (sub) sub.lastRenewSent = nowAt;   // ← stamped on dispatch, not arrival
//
// …so every subsequent renewal reports success to the pressure system while
// reaching nobody. The pin outlives its target and nothing in the process can
// learn otherwise. Reload works because a fresh peer starts with an empty
// _upstream and re-resolves through _rootHint_.
//
// The comment above pubsubPeerDied states the assumption this fence breaks:
// "the next renewal routed toward it is popped at the live terminal ('reroute')
// and re-seats at the true root". That is true only when the via chain REACHES a
// live node. When the pinned node is simply gone, no live terminal is ever
// reached, nobody pops anything, and there is no reroute.
//
// WHY THE FIX IS ONLY NOW POSSIBLE. _route discarded routeMessage's promise
// until v4.57.0 and did not classify it until v4.58.0. Before this week a failed
// renewal returned `undefined` — there was literally nothing to check. This
// fence is Q2's instrument turned on the READ path.
//
// SAME CLASSIFIER, NO SECOND SEMANTICS. Capability is DECLARED, never inferred
// (v4.58.0). An adapter that does not declare verdictsSupported must NOT have
// its silence read as a dead pin — that would unpin every healthy subscriber on
// every sim adapter in the tree. Section 3 pins that.
//
// EXPECTED RED against f0bafba: 1c/1d/2b fail — the pin survives an exhausted
// renewal and every later SUB still carries the dead waypoint.
//
// Run: node test/fence_subscribe_unpin.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const DEAD = idHex(REG | 0xd00n);          // the relay that died out of our sight
const NB1  = idHex(REG | 0xaa0n);
const TICK = 6_000;                        // > RENEW_FAST_MS (5s), so each tick renews

function mk(report, { verdictsSupported = true } = {}) {
  const clock = { t: 1_000_000 };
  const subs = [];                          // every SUB that reached the transport
  const dht = {
    verdictsSupported,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      if (payload && payload.subscriberId) subs.push({ via: [...(payload.via || [])] });
      return report();
    },
    neighbors: () => [NB1],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, subs };
}

// Pin through the PRODUCTION writer (_onDeliver, wireHandlers.js) rather than by
// poking _upstream — a fence that installs its own premise certifies nothing.
function subscribePinned(am, T) {
  am.pubsubSubscribe(T);
  am._onDeliver({ topicId: idHex(T), from: DEAD, msgs: [] }, { targetId: SELF });
}

const EXHAUSTED = () => ({ consumed: false, atNode: NB1, hops: 4, exhausted: true });
const CONSUMED  = () => ({ consumed: true,  atNode: DEAD, hops: 1 });
const SILENT    = () => undefined;

console.log('subscribe unpin — a renewal that reached nobody must not keep its waypoint\n');

// ── 1. A DEAD PIN IS DROPPED ───────────────────────────────────────────────
{
  const T = REG | 0x1001n;
  const { am, clock, subs } = mk(EXHAUSTED);
  subscribePinned(am, T);
  ok('1a. the DELIVER pinned us to the relay (production writer, not the fence)',
    (am._upstream.get(T) || [])[0] === DEAD, JSON.stringify(am._upstream.get(T)));

  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));   // the verdict arrives on a later turn

  const pinnedSub = subs.find(s => s.via[0] === DEAD);
  ok('1b. the renewal really was addressed through the dead waypoint',
    !!pinnedSub, JSON.stringify(subs));
  ok('1c. routing EXHAUSTED — the pin is dropped, not kept',
    !am._upstream.has(T), JSON.stringify(am._upstream.get(T)));

  const before = subs.length;
  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  const after = subs.slice(before);
  ok('1d. …and the NEXT renewal no longer carries the dead waypoint',
    after.length > 0 && after.every(s => s.via[0] !== DEAD), JSON.stringify(after));
}

// ── 2. THE RENEWAL CLOCK IS RESET, NOT LEFT DISCHARGED ─────────────────────
// Dropping the pin without un-stamping leaves the subscription believing it was
// serviced. It would re-home only after the adaptive interval expired — up to
// RENEW_MS = 60s of silence — which is the same latency pubsubPeerDied resets
// the clock to avoid. Mirror that recovery exactly.
{
  const T = REG | 0x2001n;
  const { am, clock } = mk(EXHAUSTED);
  subscribePinned(am, T);
  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  const s = am.mySubscriptions.get(T);
  ok('2a. the subscription still exists (we re-home, we do not unsubscribe)', !!s);
  ok('2b. lastRenewSent is the null "renew now" sentinel (C2), not a timestamp',
    s && s.lastRenewSent === null, JSON.stringify(s && s.lastRenewSent));
  ok('2c. …and the adaptive interval is snapped back to the fast floor',
    s && s.interval === am.renewFastMs, String(s && s.interval));
}

// ── 2b. THE ROLE STAMP — THE HALF I LEFT UNDONE ────────────────────────────
// Aster, council seq 110. _emitSubscribe stamps role.sync.lastRenewAt for EVERY
// role (AxonaManager.js), and OBLIGATIONS reads that stamp for CHILD, BACKUP and
// HOLDER. _unpinIfWaypointDead reset only sub.lastRenewSent — so a RELAY role
// whose pinned renewal reached nobody still read DISCHARGED to the D0 pressure
// system. That is the exact tried-vs-landed defect this file exists to fix, left
// standing in the other half of the same funnel because I scoped it out as
// "keep it minimal".
//
// Driven through a real non-app role: pubsubHost() with NO app subscription, so
// mySubscriptions is empty and only the role stamp can carry the obligation.
// refreshTick renews hosted topics through the same _sendSubscribe funnel.
// A BACKUP is used because it is the non-app role the tick demonstrably renews:
// repairPlane's _backupTopics loop calls the same _sendSubscribe funnel, and it
// requires a role to exist (it deletes the topic otherwise). Both transitions
// below are production methods on RootClaim — adoptChild is what forms a non-root
// relay and writes the _upstream pin; becomeBackup is what syncEngine calls on a
// REPLICATE ingest. Nothing here poke internals directly.
function backupPinned(am, T, principal) {
  const role = am._rootClaim.adoptChild(T, principal);   // non-root role + pin
  am._rootClaim.becomeBackup(T, role, principal);        // → _backupTopics, tick renews it
  role.cache.push({ msgId: 'm1', ts: am._now(), json: '{}' });
  return role;
}
{
  const T = REG | 0x2201n;
  const { am, clock } = mk(EXHAUSTED);
  const role = backupPinned(am, T, DEAD);
  ok('2b-i. precondition — a real non-app BACKUP role, no app subscription',
    !!role && !role.isRoot && am._backupTopics.has(T) && !am.mySubscriptions.has(T));
  ok('2b-ii. precondition — pinned to the (about to be dead) principal',
    (am._upstream.get(T) || [])[0] === DEAD, JSON.stringify(am._upstream.get(T)));

  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  ok('2b-iii. the renewal stamped the ROLE (this is the obligation D0 reads)',
    role.sync.lastRenewAt !== undefined, JSON.stringify(role.sync.lastRenewAt));
  ok('2b-iv. the role stamp is reset to the null "renew now" sentinel — a ' +
     'renewal that reached nobody must not read as discharged',
    role.sync.lastRenewAt === null, JSON.stringify(role.sync.lastRenewAt));
  ok('2b-v. …and the pin is dropped on the role path too',
    !am._upstream.has(T), JSON.stringify(am._upstream.get(T)));
  ok('2b-vi. the role is RETAINED and retryable — we re-home, we do not resign',
    am.axonRoles.has(T) && am._backupTopics.has(T));
}
{
  // CONTROL for the role path. Without it, "always null the role stamp" passes.
  const T = REG | 0x2301n;
  const { am, clock } = mk(CONSUMED);
  const role = backupPinned(am, T, DEAD);
  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  ok('2b-vii. CONTROL — a role renewal that WAS consumed keeps its stamp',
    typeof role.sync.lastRenewAt === 'number', JSON.stringify(role.sync.lastRenewAt));
  ok('2b-viii. CONTROL — …and keeps its pin',
    (am._upstream.get(T) || [])[0] === DEAD, JSON.stringify(am._upstream.get(T)));
}

// ── 3. CAPABILITY IS DECLARED, NEVER INFERRED ──────────────────────────────
// The v4.58.0 rule, applied to the read path. An adapter that does not report
// verdicts must not have its silence read as "the pin is dead" — that would
// unpin every healthy subscriber on every sim adapter in the tree on the very
// first renewal. Silence is not evidence in EITHER direction.
{
  const T = REG | 0x3001n;
  const { am, clock } = mk(SILENT, { verdictsSupported: false });
  subscribePinned(am, T);
  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  ok('3a. a declared-NON-reporting adapter never unpins — silence is not a death',
    (am._upstream.get(T) || [])[0] === DEAD, JSON.stringify(am._upstream.get(T)));
}
{
  // The OTHER no-evidence case, which section 3a did not cover: an adapter that
  // CLAIMS to report verdicts and returns nothing. That is a contract violation
  // — loud — but it is still not evidence the waypoint is dead, so it must keep
  // the pin exactly as declared-non-reporting does. Requested by Aster.
  const T = REG | 0x3101n;
  const { am, clock } = mk(SILENT, { verdictsSupported: true });
  subscribePinned(am, T);
  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  ok('3b. a declared-REPORTING adapter returning VOID keeps the pin too — a ' +
     'contract breach is loud, but it is not evidence of a dead waypoint',
    (am._upstream.get(T) || [])[0] === DEAD, JSON.stringify(am._upstream.get(T)));
}
{
  // CONTROL. Without this, "never unpin" passes sections 1 and 3 is vacuous.
  const T = REG | 0x4001n;
  const { am, clock } = mk(CONSUMED);
  subscribePinned(am, T);
  clock.t += TICK; await am.refreshTick();
  await new Promise(r => setImmediate(r));
  ok('3b. CONTROL — a renewal that WAS consumed keeps its pin',
    (am._upstream.get(T) || [])[0] === DEAD, JSON.stringify(am._upstream.get(T)));
  const s = am.mySubscriptions.get(T);
  ok('3c. …and its renewal clock is left discharged, not reset',
    s && typeof s.lastRenewSent === 'number', JSON.stringify(s && s.lastRenewSent));
}

// ── 4. BOUNDED: RE-HOMING MUST NOT SPIN ────────────────────────────────────
// _rerouteDeclined carries the scar: the wrong fall-through "spins unbounded and
// took the east production bridge down for ~50 min on 2026-07-27". Unpinning is
// self-limiting because there is nothing left to unpin — but that has to be a
// property of the code, not of my confidence in it. Drive ten consecutive
// all-failing ticks and require the SUB count to stay proportional to ticks.
{
  const T = REG | 0x5001n;
  const { am, clock, subs } = mk(EXHAUSTED);
  subscribePinned(am, T);
  for (let i = 0; i < 10; i++) {
    clock.t += TICK; await am.refreshTick();
    await new Promise(r => setImmediate(r));
  }
  // Aster: "<= 20" was loose enough to pass a real regression — a doubling to two
  // sends per tick sits comfortably under it. The bound that is actually
  // falsifiable is ONE SEND PER TICK: any re-entry, any retry-on-failure, any
  // unpin that re-enters _sendSubscribe, immediately exceeds it.
  //
  // I first asserted EXACTLY ten and measured three, then instrumented rather
  // than loosening it. The explanation is in repairPlane's renew loop: once
  // unpinned the subscriber is unattached, and an unattached subscriber that is
  // the closest reachable node CLAIMS THE ROOT (claimReachable) and stops
  // renewing — correctly, since a root has no upstream to renew toward. In this
  // single-node fence SELF is trivially closest, so it self-roots on tick 2.
  // That is the designed re-home terminus, not a stall, and 4c pins it so the
  // low number is explained rather than merely tolerated.
  ok('4a. ten failing ticks never exceed ONE subscribe per tick — the bound a ' +
     'spin or a retry-on-failure would break immediately',
    subs.length <= 10, `subs=${subs.length} over 10 ticks (max 10)`);
  ok('4b. the pin stays absent — a failed UNPINNED renewal cannot unpin again',
    !am._upstream.has(T), JSON.stringify(am._upstream.get(T)));
  // Pins WHY 4a is small, so the number is explained rather than tolerated. If a
  // future change stops the re-home terminating here, 4a's count will climb and
  // this check will say which assumption broke.
  const role = am.axonRoles.get(T);
  ok('4c. the re-home TERMINATED: unpinned and closest-reachable, the subscriber ' +
     'claimed the root and correctly stopped renewing toward an upstream',
    !!role && role.isRoot, JSON.stringify({ role: !!role, isRoot: role?.isRoot }));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
