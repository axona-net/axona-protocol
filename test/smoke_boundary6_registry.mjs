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

const byType6 = Object.fromEntries(rows.map((r) => [r.type, r]));
check('T1. exactly 3 rows, all minted, owningService DirectMessaging, versionRange {4,4}; the axona:direct REQUEST leg is REQUEST_RESPONSE (TransportRpcRef), notify + tunneled are ONE_WAY',
  rows.length === 3 && rows.every((r) => r.owningService === 'DirectMessaging' && r.versionRange.min === 4 && r.versionRange.max === 4)
  && byType6['direct:axona-direct-request'].kind === 'REQUEST_RESPONSE'
  && byType6['direct:axona-direct-request'].correlation?.kind === 'TransportRpcRef'
  && byType6['direct:axona-direct-request'].correlation.requires.length === 0
  && byType6['direct:axona-direct-request'].correlation.transportScope === 'request-return'
  && byType6['direct:axona-direct-notify'].kind === 'ONE_WAY'
  && byType6['direct:tunneled'].kind === 'ONE_WAY');
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

// ── NEGATIVE (Aster ASTER-E2-REVIEW pt.2): each leg resolves ONLY with its own ──
// transportKind; a valid-but-undeclared kind refuses; a malformed kind is rejected.
check('R6. axona:direct/request resolves ONLY via request — the notify recv path is never taken',
  registerFrame({ onRequest: () => 'REQ-ONLY', onNotification: () => { throw new Error('wrong leg'); } },
    'axona:direct', () => {}, { registry: reg, transportKind: 'request' }) === 'REQ-ONLY');
check('R7. axona:direct/routed is REFUSED — B6 declares no routed axona:direct row (kind mismatch, no bare fallback)',
  threw(() => registerFrame(recv, 'axona:direct', () => {}, { registry: reg, transportKind: 'routed' })));
check('R8. __tunneled_direct__ on the WRONG kind (request/notification) is REFUSED — only routed is declared',
  threw(() => registerFrame(recv, '__tunneled_direct__', () => {}, { registry: reg, transportKind: 'request' }))
  && threw(() => registerFrame(recv, '__tunneled_direct__', () => {}, { registry: reg, transportKind: 'notification' })));
check('R9. a MALFORMED transportKind (not routed|notification|request) is rejected by registerFrame',
  threw(() => registerFrame(recv, 'axona:direct', () => {}, { registry: reg, transportKind: 'garbage' })));

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
