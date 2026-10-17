#!/usr/bin/env node
// =====================================================================
// fence_probe_rearm_after_routable.mjs — dual-root packet (a), second half
// (axona-docs 14a6bd8, Part 4(a); David "build it" 2026-10-17).
//
// A DEFERRED PROBE RE-ARMS, BOUNDED. The refreshTick sweep re-runs a deferred
// try once the node can route (servingCapable) or after 4 × the probe
// interval, at most EMPTY_ROOT_PROBE_MAX deferrals; a counted try keeps the
// old interval; past the deferral bound the probe stops and the 45 s
// self-verify is what remains. The whole point: a root born into a forming
// mesh (the 2026-10-16 Windows relay) probes again when the mesh is there,
// and fills from the holder, instead of spending its budget on corpses.
//
// Mutant (red): count the try at the top of _emptyRootProbe as before →
// three failing tries exhaust the budget and check 4 (the probe fires again
// once routable, and fills) fails.
// Run: node test/fence_probe_rearm_after_routable.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { EMPTY_ROOT_PROBE_INTERVAL_MS, EMPTY_ROOT_PROBE_MAX } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const idHex = (b) => b.toString(16).padStart(66, '0');
const REG  = 0x89n << 248n;
const SELF = REG | 0x1000n;
const T    = SELF ^ 0x100n;
const CANDS = [T ^ 0x2n, T ^ 0x3n, T ^ 0x4n, T ^ 0x5n, T ^ 0x6n];

function makeManager() {
  const routed = [];
  const clock = { t: 1_000_000 };
  const state = { routable: false, consumed: false };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: true,
    routeMessage: (target, type, payload) => { routed.push({ target, type, payload }); return { consumed: state.consumed }; },
    neighbors: () => [],
    bridgeId: () => null,
    findKClosest: async () => CANDS.map(idHex),
    lookup: async () => ({ path: [], hops: 0, found: false }),
    servingCapable: () => state.routable,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht) });
  am._now = () => clock.t;
  const role = am._rootClaim.become(T, 'sub-terminal');
  return { am, role, routed, clock, state };
}
const pulls = (routed) => routed.filter((r) => r.type === 'pubsub:pullup').length;
async function sweepAt(c, t) { c.clock.t = t; c.am._emptyRootProbeSweep(c.clock.t); for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r)); }

console.log('fence_probe_rearm_after_routable: (a) — a deferred probe re-arms once routable, bounded');

const c = makeManager();
const t0 = c.clock.t;
await c.am._emptyRootProbe(T);                                   // birth probe: nothing reachable
const p0 = pulls(c.routed);
check('1 the birth try is deferred (routes failing)', c.role.sync.probeDeferred === true && c.role.sync.probeTries === 0 && p0 > 0);

await sweepAt(c, t0 + EMPTY_ROOT_PROBE_INTERVAL_MS + 1);         // the old interval alone does not re-arm
check('2 after one interval, not routable: the sweep does NOT re-run a deferred try', pulls(c.routed) === p0, `pulls=${pulls(c.routed)}`);

await sweepAt(c, t0 + 4 * EMPTY_ROOT_PROBE_INTERVAL_MS + 1);     // the wait re-arms it once
check('3 after 4 × interval, still not routable: the sweep re-runs it once (second deferral)', pulls(c.routed) === 2 * p0 && c.role.sync.probeDeferrals === 2, `pulls=${pulls(c.routed)} deferrals=${c.role.sync.probeDeferrals}`);

c.state.routable = true; c.state.consumed = true;                // the mesh is there now
await sweepAt(c, c.clock.t + 1);                                 // servingCapable re-arms at once
check('4 once routable, the sweep re-runs it at once and the try COUNTS (fills from the holder)', pulls(c.routed) === 3 * p0 && c.role.sync.probeTries === 1 && c.role.sync.probeDeferred === false, `pulls=${pulls(c.routed)} tries=${c.role.sync.probeTries} deferred=${c.role.sync.probeDeferred}`);

{ // 5: the deferral bound — a node that never becomes routable stops probing
  const d = makeManager();
  let t = d.clock.t;
  await d.am._emptyRootProbe(T);
  for (let i = 1; i < EMPTY_ROOT_PROBE_MAX + 3; i++) { t += 4 * EMPTY_ROOT_PROBE_INTERVAL_MS + 1; await sweepAt(d, t); }
  check(`5 never routable: deferrals stop at EMPTY_ROOT_PROBE_MAX (${EMPTY_ROOT_PROBE_MAX}) and the probe goes quiet`, d.role.sync.probeDeferrals === EMPTY_ROOT_PROBE_MAX && pulls(d.routed) === EMPTY_ROOT_PROBE_MAX * p0, `deferrals=${d.role.sync.probeDeferrals} pulls=${pulls(d.routed)}`);
}
{ // 6: a counted try keeps the plain interval (no change to the old cadence)
  const e = makeManager(); e.state.consumed = true;
  await e.am._emptyRootProbe(T);
  const p1 = pulls(e.routed);
  await sweepAt(e, e.clock.t + 1);
  check('6 a counted try is not re-run inside the plain interval', pulls(e.routed) === p1 && e.role.sync.probeTries === 1);
  await sweepAt(e, e.clock.t + EMPTY_ROOT_PROBE_INTERVAL_MS + 1);
  // The second try ROTATES to the candidates not yet probed (the #364-A2 rule),
  // so it sends fewer pulls than the first: more pulls, one more counted try.
  check('7 …and re-runs after it, as before, rotating to unprobed candidates', pulls(e.routed) > p1 && e.role.sync.probeTries === 2, `pulls=${pulls(e.routed)} p1=${p1} tries=${e.role.sync.probeTries}`);
}

console.log(`\nfence_probe_rearm_after_routable: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
