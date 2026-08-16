// smoke_boundary6_registry.mjs — REF-1.1 E2.0: Boundary-6 (direct messaging)
// row-contract smoke. The registry that exercises the (wire, transportKind)
// composite key: axona:direct is ONE wire on TWO primitives. Asserts the three
// frames are fully specified and that registerFrame binds each leg distinctly,
// while a bare axona:direct (no primitive named) is refused. Contract-only.
//
// Run: node test/smoke_boundary6_registry.mjs
import { buildBoundary6Registry, boundary6Rows, rowDefs, frameWiring } from '../src/dht/boundary6Registry.js';
import { registerFrame, frameWiringKey } from '../src/registry/index.js';

let passed = 0, failed = 0;
const check = (l, ok, x = '') => { console.log(`  ${ok ? '✓' : '✗'} ${l}${ok ? '' : ' ' + x}`); ok ? passed++ : failed++; };
const threw = (fn) => { try { fn(); return false; } catch { return true; } };
console.log('\nREF-1.1 E2.0 — Boundary-6 (direct messaging) registry\n');

const defs = rowDefs();
const rows = boundary6Rows();
const w = frameWiring(defs);

check('T1. exactly 3 rows, all minted, all ONE_WAY, owningService DirectMessaging, versionRange {4,4}',
  rows.length === 3 && rows.every((r) => r.kind === 'ONE_WAY' && r.owningService === 'DirectMessaging' && r.versionRange.min === 4 && r.versionRange.max === 4));
check('T2. axona:direct is ONE wire on TWO primitives (request + notification); __tunneled_direct__ is routed',
  defs.filter((d) => d.wire === 'axona:direct').length === 2
  && new Set(defs.filter((d) => d.wire === 'axona:direct').map((d) => d.transportKind)).size === 2
  && defs.find((d) => d.wire === '__tunneled_direct__').transportKind === 'routed');
check('W1. wiring: 3 COMPOSITE (wire,transportKind) keys; the two axona:direct legs are distinct keys; each value carries { wire, transportKind }',
  w.size === 3
  && w.has(frameWiringKey('axona:direct', 'request')) && w.has(frameWiringKey('axona:direct', 'notification'))
  && w.has(frameWiringKey('__tunneled_direct__', 'routed'))
  && [...w.values()].every((v) => v.wire && v.transportKind));

// registerFrame binds each leg to the RIGHT primitive; a bare axona:direct cannot pick a leg.
const recv = { onRequest: () => 'REQ', onNotification: () => 'NOTIFY', onRoutedMessage: () => 'ROUTED' };
const reg = buildBoundary6Registry();
check('R1. axona:direct/request → onRequest', registerFrame(recv, 'axona:direct', () => {}, { registry: reg, transportKind: 'request' }) === 'REQ');
check('R2. axona:direct/notification → onNotification', registerFrame(recv, 'axona:direct', () => {}, { registry: reg, transportKind: 'notification' }) === 'NOTIFY');
check('R3. __tunneled_direct__/routed → onRoutedMessage', registerFrame(recv, '__tunneled_direct__', () => {}, { registry: reg, transportKind: 'routed' }) === 'ROUTED');
check('R4. a bare axona:direct (no transportKind) is REFUSED — cannot silently pick a leg',
  threw(() => registerFrame(recv, 'axona:direct', () => {}, { registry: reg })));
check('R5. axona:direct on the WRONG primitive for a recv lacking it still resolves the row then fails on the missing primitive (fail-closed)',
  threw(() => registerFrame({ onRequest: () => 'x' }, 'axona:direct', () => {}, { registry: reg, transportKind: 'notification' })));

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
