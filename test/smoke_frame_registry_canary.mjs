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
//   2. THE FOLD — AxonaManager.frameRegistrySummary() folds the bounded trace ring
//      into the invariant counters { total, faults, faultKinds, verdicts, byType }.
//      The canary passes iff faults===0 and no 'threw'/'trace-fault' verdict.
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

// ── C. THE FOLD: synthetic traces fold into the exact invariant counters ──
{
  const am = new AxonaManager({ dht: fakeDht(), frameRegistry: true });
  // Push directly into the ring the wrap would fill. Mix clean + fault + a
  // verdict that BREAKS the invariant (an observer that threw), so we prove the
  // fold surfaces a violation rather than hiding it.
  am._frameTraces.push(
    { type: 'pubsub:SUB',  verdict: 'passed',   faults: [] },
    { type: 'pubsub:PUB',  verdict: 'consumed', faults: [] },
    { type: 'pubsub:PUB',  verdict: 'passed',   faults: [] },
    { type: 'pubsub:SUB',  verdict: 'other',    faults: ['unregistered'] },
    { type: 'pubsub:KILL', verdict: 'threw',    faults: ['schema:type-mismatch'] },
  );
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

  try { await armed.peer.leave?.(); } catch { /* */ }
  try { await off.peer.leave?.(); } catch { /* */ }
  try { armed.transport.stop?.(); off.transport.stop?.(); } catch { /* */ }

  console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
