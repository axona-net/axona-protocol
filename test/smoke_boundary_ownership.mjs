// =====================================================================
// smoke_boundary_ownership.mjs — REF-1.1 S5: the cross-boundary ownership
// FENCE, checked against a COMPLETE, FAIL-CLOSED, source-derived live inventory.
//
// Aster S5 F1 (369495f): a fence that derives its wire universe from the four
// registry maps only proves internal consistency. Recut-1 (d184bfd) fixed that
// by extracting the live surface from source — but only scanned two files, so it
// MISSED src/dht/AxonaPeer.js:711 onRoutedMessage('mesh:signal') (a second live
// B3 signal ingress, routed→deliverMeshSignal→b3observe('signal')). Because that
// endpoint shares the registry wire `signal`, the bidirectional checks stayed
// green — the fence had its own blind spot.
//
// This recut makes the extractor COMPLETE and FAIL-CLOSED across EVERY in-scope
// registration surface in src/, and classifies each site:
//   · routed        — on(T.X,…) in src/pubsub/wireHandlers.js  → B1 (19).
//   · routed-dht    — <recv>.onRoutedMessage('<label>',…) anywhere → literal routes
//                     (mesh:signal → B3 registry wire `signal`; __tunneled_direct__
//                     is the direct-messaging plane, OUT OF SCOPE).
//   · bridge-ws     — case '<wire>': inside web/index.js signaling.dispatch (8).
//   · <recv>-notif  — <recv>.onNotification('<label>',…) anywhere → bridge/webrtc
//                     (in scope) vs transport/t (learning + direct planes, OUT).
// EVERY discovered site must be classified IN_SCOPE (→ boundary + registry wire)
// or listed in OUT_OF_SCOPE with a reason; an UNCLASSIFIED site FAILS INV0. That
// is the fail-closed property: a newly shipped in-scope wire — anywhere, not just
// wireHandlers.js — cannot slip in silently (Aster F1 recut-1).
//
// The OUT_OF_SCOPE planes (synaptome-learning gossip: reinforce/triadic_introduce/
// hop_cache/lateral_spread; membership gossip: peer-leaving; direct messaging:
// axona:direct/__tunneled_direct__) are NOT part of REF-1.1's four frame-contract
// boundaries and are not yet registry-fied. They are excluded explicitly, not
// silently — a new handler on any of these planes still forces a classification.
// =====================================================================
import { readFileSync } from 'node:fs';
import { buildBoundary1Registry } from '../src/pubsub/boundary1Registry.js';
import { buildBoundary2Registry } from '../src/transport/boundary2Registry.js';
import { buildBoundary3Registry } from '../src/transport/boundary3Registry.js';
import { buildBoundary4Registry } from '../src/transport/boundary4Registry.js';
import { T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const src = (rel) => readFileSync(new URL(rel, import.meta.url), 'utf8');

console.log('\nREF-1.1 S5 — cross-boundary ownership fence (complete, fail-closed live inventory)\n');

// ── Registries: boundary -> Set(wire) + wiring (for type namespace) ──
const R1 = buildBoundary1Registry().wiring, R2 = buildBoundary2Registry().wiring,
      R3 = buildBoundary3Registry().wiring, R4 = buildBoundary4Registry().wiring;
const REG = { B1: new Set(R1.keys()), B2: new Set(R2.keys()), B3: new Set(R3.keys()), B4: new Set(R4.keys()) };
const WIRING = { B1: R1, B2: R2, B3: R3, B4: R4 };
const REG_PREFIX = { B1: 'pubsub:', B2: 'transport:', B3: 'mesh:', B4: 'bridge:' };

// ── Extract EVERY registration site across the in-scope source files ──
const FILES = {
  wireHandlers: src('../src/pubsub/wireHandlers.js'),
  web:  src('../src/transport/web/index.js'),
  peer: src('../src/dht/AxonaPeer.js'),
};
const sites = [];   // { surface, wire, site }
// (1) B1 routed handlers: on(T.NAME, …) in wireHandlers.js → resolve NAME via T.
for (const m of FILES.wireHandlers.matchAll(/\bon\(T\.([A-Z_]+),/g))
  sites.push({ surface: 'routed', wire: T[m[1]], site: `wireHandlers on(T.${m[1]})` });
// (2) literal routed registrations: <recv>.onRoutedMessage('<label>', …) — ANY file.
for (const [f, s] of Object.entries(FILES)) for (const m of s.matchAll(/\.onRoutedMessage\('([\w:-]+)'/g))
  sites.push({ surface: 'routed-dht', wire: m[1], site: `${f} onRoutedMessage('${m[1]}')` });
// (3) notification registrations: <recv>.onNotification('<label>', …) — receiver disambiguates.
for (const [f, s] of Object.entries(FILES)) for (const m of s.matchAll(/(\w+)\.onNotification\('([\w:-]+)'/g))
  sites.push({ surface: `${m[1]}-notif`, wire: m[2], site: `${f} ${m[1]}.onNotification('${m[2]}')` });
// (4) bridge-ws dispatch cases inside signaling.dispatch (bounded slice).
const slice = FILES.web.slice(FILES.web.indexOf('const signaling = {'), FILES.web.indexOf("log('bridge-frame-unhandled'"));
for (const m of slice.matchAll(/case '([a-z][a-z-]*)':/g))
  sites.push({ surface: 'bridge-ws', wire: m[1], site: `web signaling.dispatch case '${m[1]}'` });

// ── Classification: IN_SCOPE (→ boundary + registry wire) vs OUT_OF_SCOPE ──
// key = "surface|wire". routed (B1) is a rule; routed-dht/notif/bridge-ws are mapped.
const IN_SCOPE = new Map([
  ['bridge-ws|welcome', ['B2', 'welcome']], ['bridge-ws|turn', ['B4', 'turn']],
  ['bridge-ws|peer-list', ['B3', 'peer-list']], ['bridge-ws|peer-joined', ['B3', 'peer-joined']],
  ['bridge-ws|peer-left', ['B3', 'peer-left']], ['bridge-ws|signal', ['B3', 'signal']],
  ['bridge-ws|pong', ['B4', 'pong']], ['bridge-ws|version-gate', ['B4', 'version-gate']],
  ['bridge-notif|hello', ['B2', 'hello']], ['bridge-notif|hello-ack', ['B2', 'hello-ack']],
  ['webrtc-notif|hello', ['B3', 'hello']], ['webrtc-notif|hello-sig', ['B3', 'hello-sig']],
  ['webrtc-notif|cap-attest', ['B2', 'cap-attest']],
  // Aster F1 recut-1: the DHT-relay signalling ingress. routed wire `mesh:signal`
  // maps to the B3 registry wire `signal` (same frame, second dispatch surface).
  ['routed-dht|mesh:signal', ['B3', 'signal']],
]);
// Documented exclusions — planes NOT part of the four frame-contract boundaries.
const OUT_OF_SCOPE = new Map([
  ['routed-dht|__tunneled_direct__', 'direct-messaging tunnel plane'],
  ['transport-notif|reinforce', 'synaptome-learning gossip'],
  ['transport-notif|triadic_introduce', 'synaptome-learning gossip'],
  ['transport-notif|hop_cache', 'routing-hint learning'],
  ['transport-notif|lateral_spread', 'routing-hint learning'],
  ['transport-notif|peer-leaving', 'membership-departure gossip'],
  ['t-notif|axona:direct', 'direct-messaging plane'],
]);
const classify = (e) => {
  if (e.surface === 'routed') return ['B1', e.wire];               // every wireHandlers on(T.X) is B1
  const key = `${e.surface}|${e.wire}`;
  if (IN_SCOPE.has(key)) return IN_SCOPE.get(key);
  if (OUT_OF_SCOPE.has(key)) return 'OUT';
  return null;                                                     // unclassified → fail-closed
};

// ── INV0. FAIL-CLOSED: every discovered site is classified (in-scope or excluded) ──
{
  const unclassified = sites.filter((e) => classify(e) === null);
  check('INV0. fail-closed extractor: EVERY onRoutedMessage/onNotification/dispatch site is classified in-scope or documented-exclusion — no unclassified live registration',
    unclassified.length === 0, `\n   unclassified: ${unclassified.map((e) => e.site).join(', ')}`);
}

// ── In-scope live endpoints L: { key, boundary, regWire, site } ──
const L = sites.map((e) => ({ e, c: classify(e) })).filter((x) => Array.isArray(x.c))
  .map((x) => ({ key: `${x.e.surface}|${x.e.wire}`, boundary: x.c[0], regWire: x.c[1], surface: x.e.surface, wire: x.e.wire }));
const A = (surface, wire) => L.some((x) => x.surface === surface && x.wire === wire);

check(`A1. extraction found the live surface: ${sites.length} sites, ${L.length} in-scope, ${OUT_OF_SCOPE.size} excluded (routed=${L.filter((x)=>x.boundary==='B1').length})`,
  L.filter((x) => x.boundary === 'B1').length === 19 && L.length >= 33);

// ── INV1. forward: every in-scope endpoint's boundary registry contains its regWire ──
{
  const miss = L.filter((x) => !REG[x.boundary].has(x.regWire));
  check('INV1. forward coverage: every live in-scope endpoint’s boundary registry contains its (mapped) wire',
    miss.length === 0, `\n   miss: ${miss.map((x) => `${x.key}->${x.boundary}:${x.regWire}`).join(', ')}`);
}
// ── INV2. backward: every registry wire is a live endpoint of that boundary OR TABLE_ONLY ──
const TABLE_ONLY = new Set(['B4:client-hello', 'B4:ping', 'B4:turn-refresh', 'B4:peer-list-request']);
{
  const liveByB = {};
  for (const x of L) (liveByB[x.boundary] ??= new Set()).add(x.regWire);
  const miss = [];
  for (const [b, wires] of Object.entries(REG)) for (const w of wires)
    if (!liveByB[b]?.has(w) && !TABLE_ONLY.has(`${b}:${w}`)) miss.push(`${b}:${w}`);
  check('INV2. backward coverage: every registry wire is a live in-scope endpoint of that boundary OR a documented TABLE_ONLY frame',
    miss.length === 0, `\n   miss: ${miss.join(', ')}`);
}
// ── INV3. BOTH B3 signal endpoints pinned by dispatch surface (Aster F1) ──
check('INV3. the B3 `signal` frame has TWO live endpoints pinned by surface: bridge-ws|signal (bridge-relayed) AND routed-dht|mesh:signal (DHT-relayed), both → B3 wire signal',
  A('bridge-ws', 'signal') && A('routed-dht', 'mesh:signal')
  && L.find((x) => x.surface === 'routed-dht' && x.wire === 'mesh:signal')?.regWire === 'signal' && REG.B3.has('signal'));
// ── INV4. the two hello endpoints pinned by dispatch surface ──
check('INV4. hello is TWO endpoints pinned by surface: bridge-notif|hello→B2 AND webrtc-notif|hello→B3, each in its registry',
  A('bridge-notif', 'hello') && A('webrtc-notif', 'hello') && REG.B2.has('hello') && REG.B3.has('hello')
  && L.find((x) => x.key === 'bridge-notif|hello')?.boundary === 'B2' && L.find((x) => x.key === 'webrtc-notif|hello')?.boundary === 'B3');

// ── O1. type namespace matches boundary ──
{
  const bad = [];
  for (const b of Object.keys(REG)) for (const [wire, info] of WIRING[b]) if (!String(info.type).startsWith(REG_PREFIX[b])) bad.push(`${b}:${wire}`);
  check('O1. every registered row type is namespaced to its boundary', bad.length === 0, `\n   ${bad.join(', ')}`);
}
// ── E1-E5. recorded edge cases ──
check('E1. welcome → B2 only, never B3', L.find((x) => x.key === 'bridge-ws|welcome')?.boundary === 'B2' && !REG.B3.has('welcome'));
check('E2. cap-attest is the WIRE (carries write-flight-ack-v1 capability codec) → B2; no separate `write-flight-ack` wire',
  L.find((x) => x.key === 'webrtc-notif|cap-attest')?.boundary === 'B2' && REG.B2.has('cap-attest') && !REG.B2.has('write-flight-ack'));
check('E3. peer-list → B3 only, never B4', L.find((x) => x.key === 'bridge-ws|peer-list')?.boundary === 'B3' && !REG.B4.has('peer-list'));
check('E4. peer-list-request → B4 only + TABLE_ONLY (bridge-server-ingested)', REG.B4.has('peer-list-request') && !REG.B3.has('peer-list-request') && TABLE_ONLY.has('B4:peer-list-request'));
check('E5. turn → B4; no `turn` wire in B2 (welcome carries turn as a FIELD)', L.find((x) => x.key === 'bridge-ws|turn')?.boundary === 'B4' && !REG.B2.has('turn'));

// ── NEG. the fence has teeth — each perturbation must FAIL its invariant ──
// NEG1: an unowned live in-scope endpoint fails forward (INV1).
check('NEG1. an in-scope endpoint whose registry lacks the wire FAILS INV1',
  [...L, { boundary: 'B2', regWire: 'ghost-wire' }].some((x) => !REG[x.boundary].has(x.regWire)));
// NEG2: a wrong reassignment fails forward.
check('NEG2. a wrong reassignment (pong B4→B2) FAILS INV1 (B2 has no pong)',
  L.map((x) => x.key === 'bridge-ws|pong' ? { ...x, boundary: 'B2' } : x).some((x) => !REG[x.boundary].has(x.regWire)));
// NEG3: a registered-but-not-live wire (not table-only) fails backward (INV2).
{
  const liveByB = {}; for (const x of L) (liveByB[x.boundary] ??= new Set()).add(x.regWire);
  check('NEG3. a registered-but-not-live wire (not table-only) FAILS INV2',
    !liveByB.B1?.has('phantom-wire') && !TABLE_ONLY.has('B1:phantom-wire'));
}
// NEG4 (Aster): a NEW routed registration outside wireHandlers.js, unclassified, FAILS INV0 (fail-closed).
check('NEG4. a new routed registration outside wireHandlers.js that is unclassified FAILS the fail-closed extractor (INV0)',
  classify({ surface: 'routed-dht', wire: 'brand-new-route', site: 'synthetic' }) === null);

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
