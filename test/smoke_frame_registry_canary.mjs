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
