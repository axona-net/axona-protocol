// =====================================================================
// smoke_frame_registry_canary.mjs — REF-1.1 M1: the telemetry-only canary surface.
//
// The canary runs a node with the Boundary-1 frame-contract registry armed in
// SHADOW (observe-only) and reports the shadow INVARIANT over live traffic. This
// smoke proves the two pieces the canary depends on:
//
//   1. THREADING — AxonaPeer accepts `frameRegistry` (DEFAULT-OFF) and threads it
//      into its default AxonaManager, so a relay/standalone peer (not just the web
//      transport) can arm Boundary-1. Default-off ≡ unarmed.
//   2. THE FOLD — AxonaManager.frameRegistrySummary() reports MONOTONIC lifetime
//      counters { total, faults, dropped, faultKinds, verdicts, byType } maintained
//      at ingestion (_ingestFrameTrace), so a fault that scrolls out of the bounded
//      1024-entry ring is STILL counted (council F1 blocker regression). The canary
//      passes iff faults===0 and no 'threw'/'trace-fault' verdict; built===false is
//      not-ready, never a clean pass.
//
// This is a shadow surface: nothing here reads or changes dispatch. The verdict
// EQUIVALENCE (flag-on ≡ flag-off, byte-identical) is owned by
// smoke_boundary1_registry.mjs (D-block); this smoke owns the peer threading and
// the summary fold the canary reads.
//
// Run: node test/smoke_frame_registry_canary.mjs
// =====================================================================
import { AxonaManager }             from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';
import { frameRegistryCanaryVerdict, shadowEnabled, setShadowEnabled } from '../src/registry/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

const fakeDht = () => ({ getSelfId: () => 1n, onRoutedMessage: () => {}, verdictsSupported: false, routeMessage: () => {} });

console.log('\nREF-1.1 M1 — frame-registry canary surface (shadow)\n');

// ── A. Manager fold, DEFAULT-OFF: inert, no armed registry ──
{
  const am = new AxonaManager({ dht: fakeDht() });
  const sh = am.frameRegistryShadow();
  const su = am.frameRegistrySummary();
  check('A. default-off: frameRegistryShadow().built === false, rows 0, no traces',
    sh.built === false && sh.rows === 0 && sh.traces.length === 0);
  check('A. default-off: frameRegistrySummary() inert { built:false, total:0, faults:0 }',
    su.built === false && su.total === 0 && su.faults === 0
    && Object.keys(su.verdicts).length === 0 && Object.keys(su.byType).length === 0);
}

// ── B. Manager fold, ARMED but no traffic: built, empty distribution ──
{
  const am = new AxonaManager({ dht: fakeDht(), frameRegistry: true });
  const sh = am.frameRegistryShadow();
  const su = am.frameRegistrySummary();
  check('B. armed: frameRegistryShadow().built === true, rows > 0 (the Boundary-1 table)',
    sh.built === true && sh.rows > 0);
  check('B. armed, no traffic: summary { built:true, total:0, faults:0 }',
    su.built === true && su.rows === sh.rows && su.total === 0 && su.faults === 0);
}

// ── C. THE FOLD: traces driven through the real ingestion path fold into the
//    exact invariant counters. Drive via _ingestFrameTrace (the registry sink's
//    single path), NOT a raw ring push, so lifetime counters are exercised. ──
{
  const am = new AxonaManager({ dht: fakeDht(), frameRegistry: true });
  // Mix clean + fault + a verdict that BREAKS the invariant (an observer that
  // threw), so we prove the fold surfaces a violation rather than hiding it.
  for (const r of [
    { type: 'pubsub:SUB',  verdict: 'passed',   faults: [] },
    { type: 'pubsub:PUB',  verdict: 'consumed', faults: [] },
    { type: 'pubsub:PUB',  verdict: 'passed',   faults: [] },
    { type: 'pubsub:SUB',  verdict: 'other',    faults: ['unregistered'] },
    { type: 'pubsub:KILL', verdict: 'threw',    faults: ['schema:type-mismatch'] },
  ]) am._ingestFrameTrace(r);
  const s = am.frameRegistrySummary();
  check('C. fold: total counts every trace (5)', s.total === 5);
  check('C. fold: faults counts traces with non-empty faults[] (2) — the canary-fail signal',
    s.faults === 2);
  check('C. fold: faultKinds tallies each fault kind',
    s.faultKinds['unregistered'] === 1 && s.faultKinds['schema:type-mismatch'] === 1);
  check('C. fold: verdicts distribution is exact',
    s.verdicts['passed'] === 2 && s.verdicts['consumed'] === 1 && s.verdicts['other'] === 1 && s.verdicts['threw'] === 1);
  check('C. fold: byType distribution is exact',
    s.byType['pubsub:SUB'] === 2 && s.byType['pubsub:PUB'] === 2 && s.byType['pubsub:KILL'] === 1);
  // The invariant the canary checks: faults===0 AND no threw/trace-fault. Here it
  // is VIOLATED — prove the fold makes that visible.
  const invariantHolds = s.faults === 0 && !s.verdicts['threw'] && !s.verdicts['trace-fault'];
  check('C. invariant: a fault + a threw verdict → invariant does NOT hold (fold surfaces it)',
    invariantHolds === false);
}

// ── F. RING-ROLLOVER (council F1 BLOCKER regression): one fault, then MORE than
//    1024 clean traces, evicts the fault from the bounded ring. The lifetime
//    counters MUST still report it — otherwise the canary false-passes a dirty
//    window. Proves faults/verdicts survive rollover and dropped/ringSize track. ──
{
  const am = new AxonaManager({ dht: fakeDht(), frameRegistry: true });
  am._ingestFrameTrace({ type: 'pubsub:KILL', verdict: 'threw', faults: ['schema:type-mismatch'] });
  for (let i = 0; i < 1030; i++) am._ingestFrameTrace({ type: 'pubsub:PUB', verdict: 'passed', faults: [] });
  const ring = am.frameRegistryShadow();
  const s = am.frameRegistrySummary();
  // The ring alone lost the fault — the exact false-pass the council reproduced.
  const ringFaults = ring.traces.filter((t) => Array.isArray(t.faults) && t.faults.length).length;
  check('F. ring evicted the early fault (ring capped at 1024, fault gone from the suffix)',
    ring.traces.length === 1024 && ringFaults === 0);
  check('F. lifetime total counts every trace (1031), not just the ring', s.total === 1031);
  check('F. lifetime faults STILL reports the evicted fault (1) — no false-pass', s.faults === 1);
  check('F. lifetime verdicts.threw survives rollover (1)', s.verdicts['threw'] === 1);
  check('F. dropped counts the ring evictions (7 = 1031 - 1024); ringSize === 1024',
    s.dropped === 7 && s.ringSize === 1024);
  const invariantHolds = s.faults === 0 && !s.verdicts['threw'] && !s.verdicts['trace-fault'];
  check('F. invariant correctly FAILS the dirty window despite the clean ring suffix',
    invariantHolds === false);
}

// ── G. THE CANARY INVARIANT helper (REF-1.1 M1b): frameRegistryCanaryVerdict —
//    the ONE canonical FAIL-CLOSED pass/fail every consumer reads. Pass iff
//    built===true AND observing===true AND faults===0 AND no threw/trace-fault,
//    over VALID telemetry. Both blind windows (F1) and malformed summaries (F2)
//    must fail closed, never coerce to clean (council: Aster seq 1073, Vega 1074). ──
{
  // A clean PASS window now also requires COVERAGE (M1c): total>0 and covered===total
  // (unobserved===0). A summary without them fails the coverage gate.
  const clean = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, unobserved: 0, total: 5, verdicts: { passed: 5 } });
  check('G. clean armed+observing+covered window → pass:true, ready:true, no reasons',
    clean.pass === true && clean.ready === true && clean.reasons.length === 0);
  const unarmed = frameRegistryCanaryVerdict({ built: false, observing: false, faults: 0, verdicts: {} });
  check('G. built:false → NOT ready, pass:false (never a clean pass)',
    unarmed.pass === false && unarmed.ready === false);
  // F1 — built but shadow gate OFF: zero traces, looks clean, but observed nothing.
  const blind = frameRegistryCanaryVerdict({ built: true, observing: false, faults: 0, verdicts: {} });
  check('G/F1. built:true but observing:false → pass:false, ready:false (blind window)',
    blind.pass === false && blind.ready === false && blind.reasons.some(r => r.includes('not-observing')));
  const faulted = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 2, verdicts: { passed: 1 } });
  check('G. faults>0 → pass:false', faulted.pass === false && faulted.reasons.some(r => r.includes('faults=2')));
  // A wrap that threw with an EMPTY faults[] — faults alone reads clean.
  const threwEmpty = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, verdicts: { threw: 1 } });
  check('G. threw verdict with empty faults[] → pass:false (both checks, not faults alone)',
    threwEmpty.pass === false && threwEmpty.reasons.some(r => r.includes('threw')));
  const traceFault = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, verdicts: { 'trace-fault': 1 } });
  check('G. trace-fault verdict → pass:false', traceFault.pass === false);
  // F2 — malformed / truncated telemetry must FAIL CLOSED, not coerce to clean.
  const f2 = [
    ['null summary',           null],
    ['array summary',          []],
    ['missing faults',         { built: true, observing: true, verdicts: {} }],
    ['faults null',            { built: true, observing: true, faults: null, verdicts: {} }],
    ['faults negative',        { built: true, observing: true, faults: -1, verdicts: {} }],
    ['faults non-finite',      { built: true, observing: true, faults: Infinity, verdicts: {} }],
    ['verdicts null',          { built: true, observing: true, faults: 0, verdicts: null }],
    ['threw wrong-typed',      { built: true, observing: true, faults: 0, verdicts: { threw: '1' } }],
  ];
  let f2AllFail = true;
  for (const [, s] of f2) { if (frameRegistryCanaryVerdict(s).pass !== false) f2AllFail = false; }
  check('G/F2. malformed telemetry (null/array/missing/negative/non-finite/wrong-typed) → all pass:false',
    f2AllFail === true);
  check('G/F2. an invalid summary reports an invalid-summary reason (not silently clean)',
    frameRegistryCanaryVerdict({ built: true, observing: true, faults: null, verdicts: {} })
      .reasons.some(r => r.includes('invalid-summary')));
  // F2 recut-2 (Aster/Vega): the predicate must be TOTAL — never throw on malformed
  // telemetry — and verdicts must be a plain record with EVERY own count valid, not
  // just the two named keys. Each case must return pass:false WITHOUT throwing.
  const totalPassFalse = (label, s) => {
    let res;
    try { res = frameRegistryCanaryVerdict(s); }
    catch (e) { check(`G/F2b. ${label} → non-throwing`, false, `threw ${e}`); return; }
    check(`G/F2b. ${label} → non-throwing pass:false`, res.pass === false);
  };
  totalPassFalse('BigInt faults (1n)',        { built: true, observing: true, faults: 1n, verdicts: {} });
  totalPassFalse('BigInt verdict count',      { built: true, observing: true, faults: 0, verdicts: { threw: 1n } });
  totalPassFalse('Date verdicts',             { built: true, observing: true, faults: 0, verdicts: new Date(0) });
  totalPassFalse('Map verdicts',              { built: true, observing: true, faults: 0, verdicts: new Map() });
  totalPassFalse('invalid sibling counter',   { built: true, observing: true, faults: 0, verdicts: { passed: 'five' } });

  // F3 (Aster CHANGES-REQUIRED 507ed5f; Vega 1085): the predicate is advertised
  // TOTAL — it must NEVER throw, even on HOSTILE JavaScript values (reflection
  // traps, throwing accessors, Symbol keys). These cannot come out of an honest
  // frameRegistrySummary(), but this is the single rollout gate, so an escape must
  // fail closed. Two layers close it: an outer try/catch, and descriptor-based
  // reads (own DATA props only; Reflect.ownKeys so a Symbol key can't slip past).
  // F3.1 — reflection traps / throwing accessors → non-throwing pass:false.
  const proxyProtoThrow = new Proxy(
    { built: true, observing: true, faults: 0, verdicts: {} },
    { getPrototypeOf() { throw new Error('trap'); } });
  totalPassFalse('Proxy getPrototypeOf trap throws', proxyProtoThrow);

  const faultsGetterThrow = { built: true, observing: true, verdicts: {} };
  Object.defineProperty(faultsGetterThrow, 'faults', { enumerable: true, configurable: true, get() { throw new Error('boom'); } });
  totalPassFalse('throwing faults getter (accessor field)', faultsGetterThrow);

  const builtGetterThrow = { observing: true, faults: 0, verdicts: {} };
  Object.defineProperty(builtGetterThrow, 'built', { enumerable: true, configurable: true, get() { throw new Error('boom'); } });
  totalPassFalse('throwing built getter (accessor field)', builtGetterThrow);

  const vGetterThrow = Object.create(null);
  Object.defineProperty(vGetterThrow, 'threw', { enumerable: true, configurable: true, get() { throw new Error('boom'); } });
  totalPassFalse('verdicts w/ throwing enumerable counter getter', { built: true, observing: true, faults: 0, verdicts: vGetterThrow });

  const vOwnKeysThrow = new Proxy({}, { ownKeys() { throw new Error('trap'); } });
  totalPassFalse('verdicts Proxy ownKeys trap throws', { built: true, observing: true, faults: 0, verdicts: vOwnKeysThrow });

  // F3.2 — a Symbol-key verdict sibling carrying a non-counter value. Object.keys
  // (the 507ed5f impl) skipped it → false pass:true; Reflect.ownKeys catches it.
  totalPassFalse('Symbol-key verdict sibling', { built: true, observing: true, faults: 0, verdicts: { [Symbol('x')]: 'five' } });
  const symOnly = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, verdicts: { [Symbol('x')]: 5 } });
  check('G/F3.2. clean-looking window with only a Symbol-key verdict → pass:false (no Object.keys bypass)',
    symOnly.pass === false && symOnly.reasons.some(r => r.includes('invalid-summary')));

  // ── G/COV. COVERAGE gate (REF-1.1 M1c — Decision-1 synthesis, Aster + Vega). The
  //    predicate must fail a window that observed NO contracts even with faults===0.
  //    total>0 (liveness, now IN the predicate) and unobserved===0 (covered===total).
  //    This is what makes splitting 'unbranded-source' out of `faults` safe: an
  //    all-unbranded window still fails — F1 cannot be renamed. ──
  const covClean = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, total: 3, unobserved: 0, verdicts: { passed: 3 } });
  check('G/COV. covered live window (total>0, unobserved:0, faults:0) → pass:true',
    covClean.pass === true && covClean.reasons.length === 0);
  const covEmpty = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, total: 0, unobserved: 0, verdicts: {} });
  check('G/COV. total===0 (no traffic) → pass:false (liveness now in the predicate, not just the script)',
    covEmpty.pass === false && covEmpty.reasons.some(r => r.includes('no-traffic')));
  // The exact M1 canary window BEFORE the mint: total>0, faults:0, but every frame
  // unobserved (unbranded). Splitting unbranded out of faults must NOT let this pass.
  const covUncovered = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, total: 1250, unobserved: 1250, verdicts: { unobserved: 1250 } });
  check('G/COV. all-unbranded window (total>0, faults:0, unobserved===total) → pass:false (F1 not renamed)',
    covUncovered.pass === false && covUncovered.reasons.some(r => r.includes('uncovered')));
  const covPartial = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, total: 10, unobserved: 3, verdicts: { passed: 7, unobserved: 3 } });
  check('G/COV. partial coverage (covered<total) → pass:false',
    covPartial.pass === false && covPartial.reasons.some(r => r.includes('uncovered')));
  const covMissing = frameRegistryCanaryVerdict({ built: true, observing: true, faults: 0, total: 5, verdicts: { passed: 5 } });
  check('G/COV. missing unobserved field → invalid-summary (fail-closed on the coverage field)',
    covMissing.pass === false && covMissing.reasons.some(r => r.includes('invalid-summary') && r.includes('unobserved')));
  const covTotAcc = { built: true, observing: true, faults: 0, unobserved: 0, verdicts: { passed: 1 } };
  Object.defineProperty(covTotAcc, 'total', { enumerable: true, configurable: true, get() { throw new Error('boom'); } });
  check('G/COV. total is a throwing accessor → non-throwing pass:false (fail-closed)',
    frameRegistryCanaryVerdict(covTotAcc).pass === false);
}

// ── G2. observing reflects the LIVE runtime shadow gate on a real manager ──
{
  const prior = shadowEnabled();
  const am = new AxonaManager({ dht: fakeDht(), frameRegistry: true });
  setShadowEnabled(true);
  check('G2. armed manager + shadow ON → summary.observing===true, verdict ready',
    am.frameRegistrySummary().observing === true && frameRegistryCanaryVerdict(am.frameRegistrySummary()).ready === true);
  setShadowEnabled(false);
  check('G2. armed manager + shadow OFF → summary.observing===false, verdict pass:false (blind)',
    am.frameRegistrySummary().observing === false && frameRegistryCanaryVerdict(am.frameRegistrySummary()).pass === false);
  setShadowEnabled(prior);   // restore global flag
}

// ── D/E. PEER THREADING over a real SimTransport peer ──

// ── D/E. PEER THREADING over a real SimTransport peer ──
async function makePeer(net, domain, lat, lng, opts) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  peer._requireAxonaManager('m1-canary-test');  // force the lazy default-manager build
  return { peer, transport };
}

async function main() {
  const net = new SimNetwork();
  const domain = new AxonaDomain();

  // D. armed peer: the flag threads AxonaPeer → default AxonaManager → registry.
  const armed = await makePeer(net, domain, 10, 20, { frameRegistry: true });
  const dsh = armed.peer.frameRegistryShadow();
  const dsu = armed.peer.frameRegistrySummary();
  check('D. peer frameRegistry:true → default AxonaManager armed (shadow.built===true, rows>0)',
    dsh.built === true && dsh.rows > 0);
  check('D. peer.frameRegistrySummary() reachable + invariant-clean at rest (built, faults 0)',
    dsu.built === true && dsu.faults === 0);

  // E. default-off peer: unarmed, inert — proves default is OFF end to end.
  const off = await makePeer(net, domain, -10, -20, {});
  const esh = off.peer.frameRegistryShadow();
  const esu = off.peer.frameRegistrySummary();
  check('E. peer WITHOUT the flag → registry unarmed (shadow.built===false)',
    esh.built === false && esh.rows === 0);
  check('E. default-off peer summary inert (built:false, total:0, faults:0)',
    esu.built === false && esu.total === 0 && esu.faults === 0);
  // Council F1 requirement: an unarmed/not-yet-built summary is NOT a clean pass.
  // The canary readiness gate is built===true; built===false means not-ready and
  // must never be read as faults:0 == healthy.
  const canaryReady = (sm) => sm.built === true;
  check('E. built:false reads as NOT-ARMED / not-ready, never a clean pass',
    canaryReady(esu) === false && canaryReady(dsu) === true);
  // Council F2 (6dd4344): the PEER fallback (before the lazy manager build, when
  // this._axonaManager is still null) MUST return the SAME key set as the built
  // manager summary — so a canary reading summary.dropped / .ringSize pre-build
  // gets 0, not undefined. Exercise that exact branch by nulling the manager.
  const keysOf = (o) => Object.keys(o).sort().join(',');
  const armedPeer = armed.peer;
  const savedAm = armedPeer._axonaManager;
  armedPeer._axonaManager = null;                 // simulate pre-lazy-build
  const fb = armedPeer.frameRegistrySummary();    // hits the peer fallback branch
  const fbShadow = armedPeer.frameRegistryShadow();
  armedPeer._axonaManager = savedAm;              // restore
  check('F2. peer fallback summary shape === built summary shape (dropped/ringSize present, ===0)',
    keysOf(fb) === keysOf(dsu) && fb.dropped === 0 && fb.ringSize === 0 && fb.built === false);
  check('F2. peer fallback shadow is inert (built:false, rows 0, no traces)',
    fbShadow.built === false && fbShadow.rows === 0 && fbShadow.traces.length === 0);

  try { await armed.peer.leave?.(); } catch { /* */ }
  try { await off.peer.leave?.(); } catch { /* */ }
  try { armed.transport.stop?.(); off.transport.stop?.(); } catch { /* */ }

  console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
