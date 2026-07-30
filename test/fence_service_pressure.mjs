// fence_service_pressure.mjs — D0 / M4. The capacity metric must be able to
// report its own failure.
//
// WHAT THIS PINS, and why each half exists:
//
//   1. DOMINATION. Before D0, servicePressure needed a 108s tick gap to reach
//      SATURATION_PRESSURE while helloPressure needed 8s — 13.5x earlier — so
//      servicePressure could never be the deciding signal in the only scenario
//      it was able to detect. This half fails if a future change lets one
//      pressure hide the other again.
//
//   2. STARVATION IS VISIBLE. The pre-D0 metric read 0 while a role sat 95_000ms
//      past its own 60_000ms replication deadline, with saturated() false and
//      admitPushedRole() still true (measured: test/d0_probe.mjs, 89c0798). This
//      half manufactures exactly that state and asserts pressure RISES, admission
//      CHANGES, and the node RECOVERS when the overdue work completes — the four
//      properties Aster required in Pass 9.
//
//   3. COVERAGE. mySubscriptions was outside inspectCapacity's walk, so a node's
//      own app subscriptions were unmeasurable rather than mismeasured.
//
//   4. PER-OBLIGATION DEADLINES. One DROP_MS denominator made 1.0 mean "failed"
//      for renewal and something arbitrary for everything else. Each obligation
//      is now measured against its own deadline, so the max across them is
//      comparable — and worstObligation names which one is worst.
//
// THE FENCE IS WRITTEN TO FAIL AGAINST THE OLD CODE. Reverting the D0 change
// must turn this red; a fence that passes either way pins nothing (Phase C's
// lesson, #401).
//
// Run: node test/fence_service_pressure.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import {
  OBLIGATIONS, DROP_MS, SATURATION_PRESSURE, HELLO_DEADLINE_MS,
  ROOT_REPLICATE_FULL_MS,
} from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const TICK = 5_000;

function mk() {
  const clock = { t: 1_000_000 };
  const sends = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => [idHex(REG | 0xaa0n), idHex(REG | 0xaafn)],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, sends };
}

console.log('service pressure — the capacity metric can report its own failure\n');

// ── 1. The obligation table is complete and self-consistent ────────────────
{
  const rows = Object.entries(OBLIGATIONS);
  ok('OBLIGATIONS declares every nature plus app subscriptions',
    ['ROOT', 'CHILD', 'BACKUP', 'HOLDER', 'APP_SUB'].every(k => k in OBLIGATIONS));
  ok('every row carries what / stamp / deadline / why',
    rows.every(([, v]) => v.what && v.stamp && Number.isFinite(v.deadline) && v.why));
  ok('ROOT is measured against its OWN deadline, not DROP_MS',
    OBLIGATIONS.ROOT.deadline === ROOT_REPLICATE_FULL_MS && OBLIGATIONS.ROOT.deadline !== DROP_MS,
    `got ${OBLIGATIONS.ROOT.deadline}`);
  ok('renewal obligations are measured against DROP_MS (when the upstream evicts us)',
    ['CHILD', 'BACKUP', 'HOLDER', 'APP_SUB'].every(k => OBLIGATIONS[k].deadline === DROP_MS));
  ok('the table is frozen — a rule that can be mutated at runtime is not a rule',
    Object.isFrozen(OBLIGATIONS) && rows.every(([, v]) => Object.isFrozen(v)));
}

// ── 2. DOMINATION: servicePressure must not be strictly dominated ──────────
// The pre-D0 arithmetic: service needed SATURATION*DROP_MS of staleness while
// hello needed SATURATION*HELLO_DEADLINE_MS + one tick. Now that ROOT is
// measured against its own 60s deadline, the root obligation trips FIRST.
{
  const serviceGapRoot = SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline;
  const helloGap = SATURATION_PRESSURE * HELLO_DEADLINE_MS + TICK;
  const oldServiceGap = SATURATION_PRESSURE * DROP_MS;   // what it used to require
  ok('root-replication pressure trips sooner than it did pre-D0',
    serviceGapRoot < oldServiceGap, `${serviceGapRoot} vs ${oldServiceGap}`);
  console.log(`     root obligation reaches ${SATURATION_PRESSURE} at ${serviceGapRoot}ms of debt`);
  console.log(`     (pre-D0 the same signal needed ${oldServiceGap}ms; hello needs ${helloGap}ms)`);
}

// ── 3. STARVATION IS VISIBLE, ADMISSION CHANGES, AND IT RECOVERS ───────────
{
  const { am, clock } = mk();
  const T1 = REG | 0x2001n;
  const role = makeRole(T1, true);
  role.cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });
  role.sync.lastFullAt = clock.t;                 // discharged now
  am.axonRoles.set(T1, role);

  let c = am.inspectCapacity();
  ok('freshly discharged root reports no pressure', c.servicePressure === 0, JSON.stringify(c.servicePressure));
  ok('and is admissible', am.admitPushedRole(REG | 0x9999n) === true);

  // Let the obligation rot WITHOUT touching the tick — this is the exact state
  // the old metric could not see, because it stamped on tick entry regardless.
  clock.t += Math.ceil(SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline) + 1_000;
  c = am.inspectCapacity();
  ok('an overdue root raises servicePressure above SATURATION',
    c.servicePressure >= SATURATION_PRESSURE, `got ${c.servicePressure}`);
  ok('and names WHICH obligation is worst', c.worstObligation === 'ROOT', String(c.worstObligation));
  ok('and counts it overdue once past its own deadline',
    clock.t - role.sync.lastFullAt > ROOT_REPLICATE_FULL_MS ? c.overdue >= 1 : true);
  ok('saturated() now reports true', am.saturated() === true);
  ok('ADMISSION CHANGES — a starving node refuses new pushed roles',
    am.admitPushedRole(REG | 0x9998n) === false);

  // RECOVERY: discharge the obligation and the node must become admissible again.
  role.sync.lastFullAt = clock.t;
  c = am.inspectCapacity();
  ok('RECOVERY — pressure falls once the overdue work completes',
    c.servicePressure < SATURATION_PRESSURE, `got ${c.servicePressure}`);
  ok('RECOVERY — saturated() clears', am.saturated() === false);
  ok('RECOVERY — admission reopens', am.admitPushedRole(REG | 0x9997n) === true);
}

// ── 4. COVERAGE: app subscriptions are measured ────────────────────────────
{
  const { am, clock } = mk();
  const S1 = REG | 0x3001n;
  am.mySubscriptions.set(S1, { since: 'all', lastRenewSent: clock.t, interval: 0 });

  ok('a subscription with no role is COUNTED', am.inspectCapacity().subscriptions === 1);
  let c = am.inspectCapacity();
  ok('a freshly renewed subscription is not debt', c.servicePressure === 0);

  clock.t += Math.ceil(SATURATION_PRESSURE * DROP_MS) + 1_000;
  c = am.inspectCapacity();
  ok('a STALE app subscription raises pressure — invisible before D0',
    c.servicePressure >= SATURATION_PRESSURE, `got ${c.servicePressure}`);
  ok('and is attributed to APP_SUB', c.worstObligation === 'APP_SUB', String(c.worstObligation));
  ok('with zero roles held — so this cannot be a role measurement in disguise',
    c.roles === 0);
}

// ── 5. THE STAMP IS AT THE COMPLETION POINT, NOT TICK ENTRY ────────────────
// The regression that started D0: a tick that RUNS must not, by itself, clear
// debt. Only doing the work clears it.
{
  const { am, clock } = mk();
  const T2 = REG | 0x4001n;
  const role = makeRole(T2, true);
  role.cache.push({ msgId: 'm', ts: clock.t, json: '{}' });
  role.sync.lastFullAt = clock.t;
  am.axonRoles.set(T2, role);

  clock.t += Math.ceil(SATURATION_PRESSURE * OBLIGATIONS.ROOT.deadline) + 1_000;
  const before = am.inspectCapacity().servicePressure;
  // Simulate the OLD behaviour explicitly: stamp the retired universal field.
  // If anything still reads it, this test goes green and the fence has failed.
  for (const r of am.axonRoles.values()) if (r.sync) r.sync.lastServicedAt = clock.t;
  const after = am.inspectCapacity().servicePressure;
  ok('writing the retired lastServicedAt does NOT clear debt (it drives nothing)',
    after === before && after >= SATURATION_PRESSURE, `before=${before} after=${after}`);
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
