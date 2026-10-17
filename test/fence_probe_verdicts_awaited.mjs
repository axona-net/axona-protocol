#!/usr/bin/env node
// =====================================================================
// fence_probe_verdicts_awaited.mjs — dual-root packet (a), first half
// (axona-docs 14a6bd8, Part 4(a); David "build it" 2026-10-17).
//
// A BORN ROOT'S PROBE SEES ITS OWN VERDICTS, AND A TRY THAT REACHED NOBODY
// IS NOT A TRY. _syncPull used to return `true` before the route resolved
// and _emptyRootProbe counted the try at its top; on 2026-10-16 17:21Z the
// Windows relay that became #council's root fired all three tries inside
// the 20 s after its restart while its routed sends were failing 6–12 per
// tick, exhausted EMPTY_ROOT_PROBE_MAX, and never probed again.
//
// Checks: all pulls consumed:false → deferred, not counted, targets still
// eligible (1–3); one pull consumed:true → counted, targets recorded (4–5);
// an adapter that declares no verdicts counts the try as before (6); a
// transport error is a failed pull, not a crash (7); a second try does not
// stack on one in flight (8).
//
// Mutant (red): count the try at the top of _emptyRootProbe as before
// (`probeTries++` before the awaits) → checks 1 and 2 fail.
// Run: node test/fence_probe_verdicts_awaited.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { EMPTY_ROOT_PROBE_FANOUT } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const idHex = (b) => b.toString(16).padStart(66, '0');
const REG  = 0x89n << 248n;
const SELF = REG | 0x1000n;
const T    = SELF ^ 0x100n;
const CANDS = [T ^ 0x2n, T ^ 0x3n, T ^ 0x4n, T ^ 0x5n, T ^ 0x6n];

function makeManager({ verdict, declares = true }) {
  const routed = [];
  const logs = [];
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: declares,
    routeMessage: (target, type, payload) => { routed.push({ target, type, payload }); return typeof verdict === 'function' ? verdict(target) : verdict; },
    neighbors: () => [],
    bridgeId: () => null,
    findKClosest: async () => CANDS.map(idHex),
    lookup: async () => ({ path: [], hops: 0, found: false }),
    servingCapable: () => true,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) });
  am._now = () => clock.t;
  const role = am._rootClaim.become(T, 'sub-terminal');
  return { am, role, routed, logs, clock };
}
const pulls = (routed) => routed.filter((r) => r.type === 'pubsub:pullup');
const log = (logs, ev) => logs.filter((l) => l.ev === `pubsub:${ev}`);

console.log('fence_probe_verdicts_awaited: (a) — a try that reached nobody is not a try');

{ // 1–3: every pull refused → deferred
  const c = makeManager({ verdict: { consumed: false } });
  await c.am._emptyRootProbe(T);
  check('1 all pulls consumed:false → probeTries stays 0', c.role.sync.probeTries === 0, `tries=${c.role.sync.probeTries}`);
  check('2 …the try is DEFERRED, counted as a deferral, logged', c.role.sync.probeDeferred === true && c.role.sync.probeDeferrals === 1 && log(c.logs, 'empty-root-probe-deferred').length === 1, JSON.stringify(c.logs.map((l) => l.ev)));
  check('3 …the targets stay eligible (none recorded as probed) and the fanout was sent', (!c.role.sync.probed || c.role.sync.probed.size === 0) && pulls(c.routed).length === EMPTY_ROOT_PROBE_FANOUT, `probed=${c.role.sync.probed?.size} pulls=${pulls(c.routed).length}`);
}
{ // 4–5: one pull consumed → counted
  let i = 0;
  const c = makeManager({ verdict: () => ({ consumed: (++i === 2) }) });
  await c.am._emptyRootProbe(T);
  check('4 one pull consumed:true → the try COUNTS (probeTries 1), not deferred', c.role.sync.probeTries === 1 && c.role.sync.probeDeferred === false, `tries=${c.role.sync.probeTries} deferred=${c.role.sync.probeDeferred}`);
  check('5 …its targets are recorded as probed and the log carries reached', c.role.sync.probed?.size === EMPTY_ROOT_PROBE_FANOUT && log(c.logs, 'empty-root-probe')[0]?.ctx.reached === 1, JSON.stringify(log(c.logs, 'empty-root-probe')[0]?.ctx));
}
{ // 6: a non-reporting adapter: no evidence either way → counted as the pre-(a) kernel did
  const c = makeManager({ verdict: undefined, declares: false });
  await c.am._emptyRootProbe(T);
  check('6 an adapter declaring no verdicts counts the try as before (silence is not failure)', c.role.sync.probeTries === 1 && c.role.sync.probeDeferred === false);
}
{ // 7: a throwing route is a failed pull, contained
  const c = makeManager({ verdict: () => { throw new Error('transport down'); } });
  let threw = false;
  try { await c.am._emptyRootProbe(T); } catch { threw = true; }
  check('7 a transport error on every pull is a deferral, never a throw', !threw && c.role.sync.probeDeferred === true && c.role.sync.probeTries === 0);
}
{ // 8: one try at a time
  let resolve; const gate = new Promise((r) => { resolve = r; });
  const c = makeManager({ verdict: () => gate.then(() => ({ consumed: true })) });
  const p1 = c.am._emptyRootProbe(T);
  await new Promise((r) => setImmediate(r));
  const p2 = c.am._emptyRootProbe(T);               // in flight: must not stack a second fanout
  resolve(); await p1; await p2;
  check('8 a second try while one is in flight does not stack (one fanout, one count)', pulls(c.routed).length === EMPTY_ROOT_PROBE_FANOUT && c.role.sync.probeTries === 1, `pulls=${pulls(c.routed).length} tries=${c.role.sync.probeTries}`);
}

console.log(`\nfence_probe_verdicts_awaited: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
