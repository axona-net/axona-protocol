// =====================================================================
// smoke_strict_door_boundaries.mjs — REF-1.1 E2.0: the STRICT composite/bare door
// rule proven SEPARATELY in EACH B1–B5 registry, plus B6 (Aster E2.0 re-review
// 2795636a cond.2 / correction a767b880: "Prove the supplied-kind miss refusal
// separately in each B1–B5 registry, not just B5/B6").
//
// The door rule (registerFrame.js):
//   * transportKind SUPPLIED → composite-key lookup ONLY; a composite miss REFUSES,
//     with NO fallback to a bare-wire row (not even one whose own transportKind
//     matches). B1–B5 carry NO composite keys, so a supplied transportKind ALWAYS
//     refuses there — for EVERY kind, on a REAL declared wire.
//   * transportKind OMITTED → bare-wire lookup ONLY. B1–B5 bind this way.
//   * B6 is the sole composite registry (axona:direct on two primitives): a NAMED
//     primitive binds via the composite key; an OMITTED kind refuses.
//
// Run: node test/smoke_strict_door_boundaries.mjs
// =====================================================================
import { registerFrame } from '../src/registry/index.js';
import { sealByOwnMethods } from './lib/testCapability.mjs';
import { buildBoundary1Registry } from '../src/pubsub/boundary1Registry.js';
import { buildBoundary2Registry } from '../src/transport/boundary2Registry.js';
import { buildBoundary3Registry } from '../src/transport/boundary3Registry.js';
import { buildBoundary4Registry } from '../src/transport/boundary4Registry.js';
import { buildBoundary5Registry } from '../src/dht/boundary5Registry.js';
import { buildBoundary6Registry } from '../src/dht/boundary6Registry.js';

let passed = 0, failed = 0;
const check = (l, ok, x = '') => { console.log(`  ${ok ? '✓' : '✗'} ${l}${ok ? '' : ' ' + x}`); ok ? passed++ : failed++; };
const threw = (fn) => { try { fn(); return false; } catch { return true; } };
// A receiver carrying all three primitives, each returning a tag so a bound
// registration is observable.
const recv = () => sealByOwnMethods({ onRequest: (w) => `req:${w}`, onNotification: (w) => `notif:${w}`, onRoutedMessage: (w) => `routed:${w}` });
const KINDS = ['request', 'notification', 'routed'];

console.log('\nREF-1.1 E2.0 — strict composite/bare door, proven in EACH B1–B5 registry + B6\n');

// ── B1–B5: single-primitive, BARE-keyed. A supplied transportKind ALWAYS refuses. ──
const SINGLE = [
  ['B1', buildBoundary1Registry],
  ['B2', buildBoundary2Registry],
  ['B3', buildBoundary3Registry],
  ['B4', buildBoundary4Registry],
  ['B5', buildBoundary5Registry],
];
for (const [name, build] of SINGLE) {
  const reg = build();
  const entries = [...reg.wiring.entries()];
  // A REAL declared wire in this registry (bare key). Pick one that carries a
  // dispatch transportKind so the omitted-kind bind is observable; fall back to the
  // first key for registries whose values omit transportKind (B4 owns no target).
  const withTk = entries.find(([, v]) => v && v.transportKind);
  const [wire, val] = withTk || entries[0];

  // (a) a supplied transportKind refuses for EVERY kind — composite miss, no fallback.
  const allRefuse = KINDS.every((tk) => threw(() => registerFrame(recv(), wire, () => {}, { registry: reg, transportKind: tk })));
  check(`${name}. a SUPPLIED transportKind REFUSES on the real wire "${wire}" — for all 3 kinds (composite-only, B1–B5 carry no composite keys)`, allRefuse);

  // (b) prove that wire IS declared, so the refusal above is the strict composite-miss, not an unknown wire.
  check(`${name}. that wire is declared (bare key present) — the refusal is the strict composite-miss, not an undeclared wire`, reg.wiring.get(wire) != null);

  // (c) OMITTING transportKind binds via the bare wire (where the row carries a dispatch kind).
  if (val && val.transportKind) {
    const got = registerFrame(recv(), wire, () => {}, { registry: reg });
    check(`${name}. OMITTING transportKind BINDS via the bare wire ("${wire}" → ${val.transportKind})`, typeof got === 'string' && got.endsWith(`:${wire}`));
  } else {
    check(`${name}. (rows carry no dispatch transportKind — B4 owns no migration target; supplied-kind refusal is the operative proof)`, true);
  }
}

// ── B6: the composite registry. NAMED primitive binds; OMITTED refuses. ──
{
  const reg = buildBoundary6Registry();
  check('B6. axona:direct WITH transportKind:request BINDS via the composite key',
    registerFrame(recv(), 'axona:direct', () => {}, { registry: reg, transportKind: 'request' }) === 'req:axona:direct');
  check('B6. axona:direct WITH transportKind:notification BINDS the OTHER leg via its composite key',
    registerFrame(recv(), 'axona:direct', () => {}, { registry: reg, transportKind: 'notification' }) === 'notif:axona:direct');
  check('B6. axona:direct WITHOUT transportKind REFUSES — the composite registry has no bare key, so it cannot silently pick a leg',
    threw(() => registerFrame(recv(), 'axona:direct', () => {}, { registry: reg })));
  check('B6. axona:direct WITH transportKind:routed REFUSES — B6 declares no routed axona:direct row (composite miss, no fallback)',
    threw(() => registerFrame(recv(), 'axona:direct', () => {}, { registry: reg, transportKind: 'routed' })));
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
