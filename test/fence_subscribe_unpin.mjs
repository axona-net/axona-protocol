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
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
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
  ok('4a. ten failing ticks emit O(ticks) subscribes, not a storm',
    subs.length <= 20, `subs=${subs.length} over 10 ticks`);
  ok('4b. the pin stays absent — a failed UNPINNED renewal cannot unpin again',
    !am._upstream.has(T), JSON.stringify(am._upstream.get(T)));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
