// smoke_boundary5_registry.mjs — REF-1.1 E2.0: Boundary-5 (DHT routing/core)
// row-contract smoke. Asserts the ten frames are fully specified (type, wire,
// transportKind, kind, owningService) and that registerFrame binds each via the
// bare-wire key (composite keying is scoped to B6). Contract-only; no dispatch.
//
// Run: node test/smoke_boundary5_registry.mjs
import { buildBoundary5Registry, boundary5Rows, rowDefs, frameWiring } from '../src/dht/boundary5Registry.js';
import { registerFrame } from '../src/registry/index.js';

let passed = 0, failed = 0;
const check = (l, ok, x = '') => { console.log(`  ${ok ? '✓' : '✗'} ${l}${ok ? '' : ' ' + x}`); ok ? passed++ : failed++; };
console.log('\nREF-1.1 E2.0 — Boundary-5 (DHT routing/core) registry\n');

const defs = rowDefs();
const rows = boundary5Rows();
const byType = Object.fromEntries(rows.map((r) => [r.type, r]));
const w = frameWiring(defs);

const REQUEST = ['dht:lookup_step', 'dht:lookahead_probe', 'dht:local_probe', 'dht:find_closest_set', 'dht:route_msg'];
const NOTIFY = ['dht:reinforce', 'dht:triadic_introduce', 'dht:hop_cache', 'dht:lateral_spread', 'dht:peer-leaving'];

check('T1. exactly 10 rows, all minted (defineRow-valid), versionRange {4,4}; request legs are REQUEST_RESPONSE, notification legs ONE_WAY',
  rows.length === 10 && rows.every((r) => r.versionRange.min === 4 && r.versionRange.max === 4)
  && REQUEST.every((t) => byType[t].kind === 'REQUEST_RESPONSE')
  && NOTIFY.every((t) => byType[t].kind === 'ONE_WAY'));
check('T1b. each request leg carries the TransportRpcRef channel subject — REQUEST_RESPONSE with empty requires + transportScope "request-return" (the reply obligation, Aster ASTER-E2-CHANNEL-SUBJECT)',
  REQUEST.every((t) => byType[t].correlation?.kind === 'TransportRpcRef'
    && byType[t].correlation.requires.length === 0 && byType[t].correlation.transportScope === 'request-return'));
check('T2. 5 request legs + 5 notification legs (from the raw defs\' transportKind)',
  defs.filter((d) => d.transportKind === 'request').length === 5 && defs.filter((d) => d.transportKind === 'notification').length === 5);
check('T3. owningService planes: DhtRouting (5 routing legs), SynaptomeLearning (4), PeerLifecycle (1)',
  REQUEST.every((t) => byType[t].owningService === 'DhtRouting')
  && ['dht:reinforce', 'dht:triadic_introduce', 'dht:hop_cache', 'dht:lateral_spread'].every((t) => byType[t].owningService === 'SynaptomeLearning')
  && byType['dht:peer-leaving'].owningService === 'PeerLifecycle');
check('W1. wiring: 10 bare-wire keys; each value carries { wire, transportKind }; no composite keys',
  w.size === 10 && [...w.entries()].every(([k, v]) => v.wire === k && (v.transportKind === 'request' || v.transportKind === 'notification')));

// registerFrame binds every B5 wire — bare-wire resolution (single-primitive), with OR without an explicit transportKind.
const recv = { onRequest: (wire) => `req:${wire}`, onNotification: (wire) => `notif:${wire}` };
const reg = buildBoundary5Registry();
let boundOk = 0;
for (const d of defs) {
  const got = registerFrame(recv, d.wire, () => {}, { registry: reg, transportKind: d.transportKind });
  const bare = registerFrame(recv, d.wire, () => {}, { registry: reg });   // no transportKind → still resolves (single-primitive)
  if (got.endsWith(`:${d.wire}`) && bare.endsWith(`:${d.wire}`)) boundOk++;
}
check('R1. registerFrame binds all 10 wires via the correct primitive — with AND without an explicit transportKind', boundOk === 10, `\n   ${boundOk}/10`);
check('R2. an undeclared wire is refused', (() => { try { registerFrame(recv, 'not_a_dht_wire', () => {}, { registry: reg, transportKind: 'request' }); return false; } catch { return true; } })());
// STRICT KIND (Aster ASTER-E2-STRICT-KIND, Vega a42549d1): a supplied transportKind
// that MISMATCHES a bare-wire row's own kind must REFUSE — it must not fall through
// and dispatch the bare row's primitive. lookup_step is a request; naming it
// notification must refuse (regression proof for the composite-miss fall-through).
{
  const threw = (fn) => { try { fn(); return false; } catch { return true; } };
  check('R3. a MISMATCHED supplied transportKind refuses — lookup_step(request) named as notification does NOT fall through to the request row',
    threw(() => registerFrame(recv, 'lookup_step', () => {}, { registry: reg, transportKind: 'notification' })));
  check('R4. reinforce(notification) named as request refuses too — no bare-wire fall-through for either direction',
    threw(() => registerFrame(recv, 'reinforce', () => {}, { registry: reg, transportKind: 'request' })));
  check('R5. a malformed transportKind is rejected', threw(() => registerFrame(recv, 'lookup_step', () => {}, { registry: reg, transportKind: 'bogus' })));
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
