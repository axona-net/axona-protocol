// =====================================================================
// fence_root_obligation_deadline.mjs — the ROOT obligation's deadline is
// THREE push periods, not one (4.113.0; council 11a8c5d5, 2026-10-13).
//
// THE DEFECT. OBLIGATIONS.ROOT.deadline was ROOT_REPLICATE_FULL_MS (60 s), the
// same number at which repairPlane re-arms the full push (`age >=
// ROOT_REPLICATE_FULL_MS`, stamped `lastFullAt = now` when it fires). So on a
// node holding ANY root with state, the ROOT age swept 0 → 60 s every minute
// and servicePressure swept 0 → 1.0 with it, crossing SATURATION_PRESSURE 0.6
// at 36 s. A root serviced ON SCHEDULE read saturated for the last 24 s of
// every minute: canAcceptRole soft-refused, HANDOFF was refused, and the
// bridges' /healthz reported "degraded" (prod 2026-10-13: east 0.874 / west
// 0.589 in one read, two phases of the same cycle, refusals 0). Every other
// obligation measures a 60 s renewal against DROP_MS 180 s; ROOT measured 60
// against 60. The fix is the same 3:1 ratio for ROOT. The push cadence does
// not move.
//
// WHAT IS PINNED:
//   A. the table: ROOT.deadline === 3 × ROOT_REPLICATE_FULL_MS, derived from
//      the push period (a later edit to the period keeps the ratio).
//   B. the real path: a root with state, driven through refreshTick with an
//      injected clock at the real 5 s tick for 30 simulated minutes. The full
//      push fires on its own cadence (lastFullAt advances every ≤ 65 s), and
//      servicePressure NEVER reaches SATURATION_PRESSURE; saturated() stays
//      false on every tick; overdue stays 0. The ratio of pushes to minutes is
//      read back so the check cannot pass because the push stopped.
//   C. the metric still reports failure: with ticks suspended (the stall D0
//      was written for), pressure reaches 0.6 only after the push is ~48 s
//      LATE (108 s of age), overdue counts 1 only past 180 s (two pushes
//      missed), and a stamp recovers it.
//   D. the symptom at the border: a root whose push lands exactly on the
//      period plus one tick (65 s, the worst on-schedule case) peaks below
//      0.6 — the number the old table put at 1.08.
//
// MUTANT (run by hand 2026-10-13): set OBLIGATIONS.ROOT.deadline back to
// ROOT_REPLICATE_FULL_MS → A fails; B fails on the first tick past 36 s of
// age (pressure 0.6+, saturated true); D fails (65/60 = 1.08). C's numbers
// move but C alone would still pass — C is the half that keeps the fix from
// turning into "never saturate".
//
// Run: node test/fence_root_obligation_deadline.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import {
  OBLIGATIONS, DROP_MS, RENEW_MS, SATURATION_PRESSURE,
  ROOT_REPLICATE_FULL_MS, ROOT_FULL_DEADLINE_MS, TICK_LAG_WINDOW,
} from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const TICK = 5_000;                          // refreshIntervalMs default on a relay

/** refreshTick fires _replicateRoots fire-and-forget; let the pushes settle. */
async function tick(am, clock) {
  clock.t += TICK;
  await am.refreshTick();
  await new Promise((r) => setImmediate(r));
}

function mk() {
  const clock = { t: 1_000_000 };
  const sends = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    // A cohort that CONSUMES: only a 'consumed' verdict seats a replica, and a
    // seated cohort is what lets the delta gate fall back to the 60 s backstop.
    // With verdicts undeclared every tick would be a full push (unseeded cohort)
    // and the cadence under test would never appear.
    verdictsSupported: true,
    routeMessage: (target, type, payload) => (sends.push({ target, type, payload }), { consumed: true }),
    neighbors: () => [idHex(REG | 0xaa0n), idHex(REG | 0xaafn)],   // a two-node cohort for the full push
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, sends };
}

/** A self-rooted topic that HOLDS something, so it owes the full push. */
function rootWithState(am, clock, T) {
  am._becomeRoot(T);
  const role = am.axonRoles.get(T);
  role.cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });
  return role;
}

console.log('fence_root_obligation_deadline: a root serviced on schedule is not saturated\n');

// ── A. the table ─────────────────────────────────────────────────────────
{
  check('A1 ROOT.deadline is three push periods', OBLIGATIONS.ROOT.deadline === 3 * ROOT_REPLICATE_FULL_MS, `got ${OBLIGATIONS.ROOT.deadline}`);
  check('A2 the row reads the exported constant, not a literal', OBLIGATIONS.ROOT.deadline === ROOT_FULL_DEADLINE_MS);
  check('A3 ROOT is not measured against its own period', OBLIGATIONS.ROOT.deadline !== ROOT_REPLICATE_FULL_MS);
  const ratio = OBLIGATIONS.ROOT.deadline / ROOT_REPLICATE_FULL_MS;
  const renewRatio = DROP_MS / RENEW_MS;
  check('A4 the same ratio as the renewal rows (deadline : cadence)', ratio === renewRatio, `root ${ratio} vs renew ${renewRatio}`);
  // On-schedule geometry, stated once so the numbers below have a source.
  const onSchedulePeak = (ROOT_REPLICATE_FULL_MS + TICK) / OBLIGATIONS.ROOT.deadline;
  check('A5 a push on schedule plus one tick peaks under SATURATION_PRESSURE', onSchedulePeak < SATURATION_PRESSURE, `${onSchedulePeak.toFixed(3)} vs ${SATURATION_PRESSURE}`);
  console.log(`     on-schedule peak ${onSchedulePeak.toFixed(3)}; saturation at ${SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline / 1000} s of age (push ${(SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline - ROOT_REPLICATE_FULL_MS) / 1000} s late); overdue at ${OBLIGATIONS.ROOT.deadline / 1000} s`);
}

// ── B. the real path: refreshTick at 5 s for 30 simulated minutes ────────
{
  const { am, clock, sends } = mk();
  const T = REG | 0x7001n;
  const role = rootWithState(am, clock, T);
  const minutes = 30;
  const ticks = (minutes * 60_000) / TICK;
  let peak = 0, worst = null, satTicks = 0, overdueTicks = 0, maxGap = 0, pushes = 0;
  let lastSeen = role.sync.lastFullAt || 0;
  for (let i = 0; i < ticks; i++) {
    await tick(am, clock);
    const at = role.sync.lastFullAt || 0;
    if (at !== lastSeen) { if (lastSeen) maxGap = Math.max(maxGap, at - lastSeen); lastSeen = at; pushes++; }
    const c = am.inspectCapacity();
    if (c.servicePressure > peak) { peak = c.servicePressure; worst = c.worstObligation; }
    if (am.saturated()) satTicks++;
    if (c.overdue > 0) overdueTicks++;
  }
  check(`B1 the full push fired on its own cadence (${pushes} pushes in ${minutes} min, max gap ${maxGap / 1000} s)`,
    pushes >= minutes - 1 && pushes <= minutes + 2 && maxGap >= ROOT_REPLICATE_FULL_MS && maxGap <= ROOT_REPLICATE_FULL_MS + TICK, `pushes=${pushes} maxGap=${maxGap}`);
  check('B1b the pushes went on the wire (REPLICATE sends to the cohort)', sends.some((s) => /REPLICATE/i.test(String(s.type))), JSON.stringify([...new Set(sends.map((s) => s.type))]));
  check(`B2 servicePressure never reached SATURATION_PRESSURE (peak ${peak.toFixed(3)}, worst ${worst})`, peak < SATURATION_PRESSURE, `peak=${peak}`);
  check(`B3 saturated() was false on every one of ${ticks} ticks`, satTicks === 0, `saturated on ${satTicks} ticks`);
  check('B4 overdue stayed 0 throughout', overdueTicks === 0, `overdue on ${overdueTicks} ticks`);
  check('B5 a pushed role is admitted on every phase of the cycle', am.admitPushedRole(REG | 0x7999n) === true);
}

// ── C. the metric still reports a real failure ───────────────────────────
{
  const { am, clock } = mk();
  const T = REG | 0x7002n;
  const role = rootWithState(am, clock, T);
  await tick(am, clock);                                         // first push lands, lastFullAt stamped
  const t0 = role.sync.lastFullAt;
  check('C0 the first tick stamped lastFullAt', Number.isFinite(t0) && t0 > 0);
  // Ticks SUSPENDED from here: the stall D0 was written to expose.
  clock.t = t0 + ROOT_REPLICATE_FULL_MS + TICK;                  // 65 s: the push is one tick late
  let c = am.inspectCapacity();
  check('C1 one tick late is not saturated', c.servicePressure < SATURATION_PRESSURE && am.saturated() === false, `pressure=${c.servicePressure}`);
  clock.t = t0 + Math.ceil(SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline) - 1_000;   // 107 s
  c = am.inspectCapacity();
  check('C2 just under the saturation age: still admissible', c.servicePressure < SATURATION_PRESSURE && am.saturated() === false, `pressure=${c.servicePressure}`);
  clock.t = t0 + Math.ceil(SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline) + 1_000;   // 109 s: push ~48 s late
  c = am.inspectCapacity();
  check('C3 a push most of a period late saturates and names ROOT', c.servicePressure >= SATURATION_PRESSURE && c.worstObligation === 'ROOT' && am.saturated() === true, `pressure=${c.servicePressure} worst=${c.worstObligation}`);
  check('C3b …and a pushed role is refused', am.admitPushedRole(REG | 0x7998n) === false);
  check('C3c …but it is not yet overdue (under the deadline)', c.overdue === 0, `overdue=${c.overdue}`);
  clock.t = t0 + OBLIGATIONS.ROOT.deadline + 1_000;             // 181 s: two pushes missed
  c = am.inspectCapacity();
  check('C4 past the deadline the obligation counts overdue', c.overdue >= 1 && c.servicePressure >= 1, `overdue=${c.overdue} pressure=${c.servicePressure}`);
  // Ticks RESUME: the next refreshTick owes a full push (age ≥ period) and stamps.
  await tick(am, clock);
  c = am.inspectCapacity();
  check('C5 one resumed tick discharges the ROOT obligation: lastFullAt advances, service pressure falls',
    role.sync.lastFullAt > t0 && c.servicePressure < SATURATION_PRESSURE && c.overdue === 0,
    `lastFullAt advanced=${role.sync.lastFullAt > t0} pressure=${c.servicePressure}`);
  // The 186 s gap between tick starts IS an event-loop stall by the metric's
  // other half (helloPressure = rolling tick-lag max / HELLO_DEADLINE_MS), so
  // saturated() stays true for TICK_LAG_WINDOW ticks on THAT signal, correctly.
  check('C5b the resumed node is still saturated, now on helloPressure (the stall), not on ROOT',
    am.saturated() === true && c.helloPressure >= SATURATION_PRESSURE && c.servicePressure < SATURATION_PRESSURE,
    `hello=${c.helloPressure} worst=${c.worstObligation}`);
  for (let i = 0; i < TICK_LAG_WINDOW + 1; i++) await tick(am, clock);
  c = am.inspectCapacity();
  check('C5c once the lag window has rolled past, saturated clears and admission reopens',
    am.saturated() === false && am.admitPushedRole(REG | 0x7997n) === true,
    `pressure=${c.servicePressure} hello=${c.helloPressure}`);
}

// ── D. the border: the worst ON-SCHEDULE phase, read directly ────────────
{
  const { am, clock } = mk();
  const T = REG | 0x7003n;
  const role = rootWithState(am, clock, T);
  await tick(am, clock);
  const t0 = role.sync.lastFullAt;
  // The push re-arms at age ≥ 60 s; with a 5 s tick the latest it can land is
  // 64.999 s, so the instant before that tick is the worst phase of a healthy
  // cycle. The old table read this instant as 1.08: overdue, saturated.
  clock.t = t0 + ROOT_REPLICATE_FULL_MS + TICK - 1;
  const c = am.inspectCapacity();
  check(`D1 the worst on-schedule phase reads ${c.servicePressure.toFixed(3)}, under SATURATION_PRESSURE`, c.servicePressure < SATURATION_PRESSURE && c.overdue === 0 && am.saturated() === false, `pressure=${c.servicePressure} overdue=${c.overdue}`);
  check('D2 the same phase under the old table would have been overdue (the defect, stated as a number)', (ROOT_REPLICATE_FULL_MS + TICK - 1) / ROOT_REPLICATE_FULL_MS > 1);
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
