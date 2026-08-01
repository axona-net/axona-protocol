// d0_probe.mjs — DIAGNOSTIC for D0. Not a fence; named so the manifest guard
// ignores it (TEST_FILE only matches smoke_/fence_).
//
// QUESTION: can servicePressure ever report starvation?
//
// It is defined as "age of my least-recently-serviced role / DROP_MS", and its
// docstring claims it "catches every cause at once — skipped ticks, event-loop
// stalls, BUDGET STARVATION, GC pauses — because it measures the outcome". But
// repairPlane.js:61 stamps lastServicedAt on EVERY role at the TOP of every
// tick, before any work. So the stamp means "a tick began while this role
// existed", not "this role's obligation was discharged".
//
// Three things to measure:
//   1. DOMINATION       — the tick must die for ~108s before servicePressure
//                         trips; helloPressure trips at an 8s gap.
//   2. BUDGET STARVATION— REPLICATE_FULL_BUDGET defers roles "whole — no sends,
//                         no ledger updates". Named in the docstring as a cause
//                         this metric catches. Does it?
//   3. COVERAGE         — inspectCapacity walks axonRoles only. Are the node's
//                         own app subscriptions (mySubscriptions) in there?
//
// Run: node test/d0_probe.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import {
  T, DROP_MS, SATURATION_PRESSURE, HELLO_DEADLINE_MS,
  ROOT_REPLICATE_FULL_MS, REPLICATE_FULL_BUDGET,
} from '../src/pubsub/constants.js';

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const TICK = 5_000;
const settle = () => new Promise(r => setImmediate(r));

function mk() {
  const sends = [];
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: false,   // audited: returns a push-count / undefined, never a verdict
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => [idHex(REG | 0xaa0n), idHex(REG | 0xaafn)],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, sends, clock };
}

console.log('D0 probe — is servicePressure capable of reporting starvation?\n');

// ─────────────────────────────────────────────────────────────────────────────
console.log('PART 1 — domination (arithmetic over shipped constants)');
const gapForService = SATURATION_PRESSURE * DROP_MS;
const gapForHello = SATURATION_PRESSURE * HELLO_DEADLINE_MS + TICK;
console.log(`  tick interval / DROP_MS / SATURATION   ${TICK} / ${DROP_MS} ms / ${SATURATION_PRESSURE}`);
console.log(`  tick gap for servicePressure ≥ ${SATURATION_PRESSURE}      ${gapForService} ms`);
console.log(`  tick gap for helloPressure   ≥ ${SATURATION_PRESSURE}      ${gapForHello} ms`);
console.log(`  tick gap for overdue > 0                ${ROOT_REPLICATE_FULL_MS} ms`);
console.log(`  → servicePressure trips ${(gapForService / gapForHello).toFixed(1)}x LATER than helloPressure`);
console.log(`  → ${gapForService > gapForHello ? 'STRICTLY DOMINATED: it can never be the deciding signal' : 'reachable'}\n`);

// ─────────────────────────────────────────────────────────────────────────────
// PART 2 — BUDGET STARVATION. N roots all carrying content, N >> the per-tick
// full-push budget. A deferred role gets NO sends and NO ledger update, yet is
// stamped fresh at the top of the same tick.
// ─────────────────────────────────────────────────────────────────────────────
console.log(`PART 2 — budget starvation (REPLICATE_FULL_BUDGET = ${REPLICATE_FULL_BUDGET})`);
{
  // Ground truth for "was this role's replication obligation discharged" is
  // role.sync.lastFullAt — the code's OWN bookkeeping, re-armed every
  // ROOT_REPLICATE_FULL_MS. Counting REPLICATE sends does not work: keepalives
  // are unbudgeted and go out every tick, so every role looks serviced. Only a
  // FULL push is budgeted, and a deferred role returns before sending anything.
  //
  // A full sweep costs ceil(N / BUDGET) ticks. It falls behind the 60s deadline
  // once ceil(N/32) * 5000 > 60000, i.e. N > 384. MAX_ROLES is 96, and
  // saturated()'s telemetry-dead backstop is maxRoles*8 = 768. So 385..767
  // roles is a band where the node provably misses its own deadline while the
  // backstop stays silent. N = 640 sits inside it.
  const N = REPLICATE_FULL_BUDGET * 20;          // 640 → 20 ticks/sweep vs a 12-tick deadline
  const { am, clock } = mk();
  for (let i = 0; i < N; i++) {
    const t = REG | BigInt(0x100000 + i);
    const role = makeRole(t, true);
    role.cache.push({ msgId: `m${i}`, ts: clock.t, json: '{}' });
    am.axonRoles.set(t, role);
  }

  const TICKS = 40;                              // 200 s simulated — two sweeps' worth
  const stampedFresh = new Map();
  const seen = new Map();
  const snaps = [];
  let worstOverdueMs = 0;

  for (let i = 0; i < TICKS; i++) {
    await am.refreshTick();
    await settle();
    for (const [t, role] of am.axonRoles) {
      const at = role.sync?.lastServicedAt || 0;
      if (seen.get(t) !== at) { stampedFresh.set(t, (stampedFresh.get(t) || 0) + 1); seen.set(t, at); }
    }
    // How overdue is the most-overdue role's FULL push, right now?
    for (const role of am.axonRoles.values()) {
      const age = clock.t - (role.sync.lastFullAt || 0);
      if (role.sync.lastFullAt && age > worstOverdueMs) worstOverdueMs = age;
    }
    snaps.push(am.inspectCapacity());
    clock.t += TICK;
  }

  const alwaysStamped = [...stampedFresh.values()].filter(c => c === TICKS).length;
  const neverFull = [...am.axonRoles.values()].filter(r => !r.sync.lastFullAt).length;
  const last = snaps[snaps.length - 1];
  const maxPressure = Math.max(...snaps.map(s => s.servicePressure));
  const maxOverdue = Math.max(...snaps.map(s => s.overdue));

  console.log(`  roles                                  ${N}  (MAX_ROLES = 96; backstop = 768)`);
  console.log(`  ticks driven                           ${TICKS} (${TICKS * TICK / 1000}s simulated)`);
  console.log(`  roles stamped fresh on ALL ${TICKS} ticks   ${alwaysStamped} / ${N}`);
  console.log(`  roles that NEVER got a full push       ${neverFull}`);
  console.log(`  worst age of an un-refreshed full push  ${worstOverdueMs} ms`);
  console.log(`  ROOT_REPLICATE_FULL_MS (own deadline)  ${ROOT_REPLICATE_FULL_MS} ms`);
  console.log(`  → obligation demonstrably overdue?     ${worstOverdueMs > ROOT_REPLICATE_FULL_MS ? 'YES' : 'no'}`);
  console.log(`  ── what the node says about itself:`);
  console.log(`  servicePressure  max over run          ${maxPressure}`);
  console.log(`  overdue          max over run          ${maxOverdue}`);
  console.log(`  worstAgeMs       last                  ${last.worstAgeMs}`);
  console.log(`  saturated()                            ${am.saturated()}`);
  console.log(`  admitPushedRole() — take even more?    ${am.admitPushedRole(REG | 0x999999n)}\n`);
}

// ─────────────────────────────────────────────────────────────────────────────
// PART 3 — COVERAGE. Does the metric even see the node's app subscriptions?
// ─────────────────────────────────────────────────────────────────────────────
console.log('PART 3 — coverage: are app subscriptions measured at all?');
{
  const { am, clock } = mk();
  const SUB_T = REG | 0xc01n;
  am.mySubscriptions.set(SUB_T, { since: 'all', lastRenewSent: 0, interval: 0 });
  am._upstream.set(SUB_T, [idHex(REG | 0xaa0n)]);
  await am.refreshTick(); await settle();
  clock.t += TICK;
  const c = am.inspectCapacity();
  console.log(`  mySubscriptions.size                   ${am.mySubscriptions.size}`);
  console.log(`  axonRoles.size                         ${am.axonRoles.size}`);
  console.log(`  inspectCapacity().roles                ${c.roles}`);
  console.log(`  → app subscriptions ${am.mySubscriptions.size > 0 && c.roles === 0 ? 'are INVISIBLE to capacity' : 'are counted'}`);
}
