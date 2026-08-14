// =====================================================================
// smoke_boundary_ownership.mjs — REF-1.1 S5: the cross-boundary ownership
// FENCE, checked against an INDEPENDENT live-dispatch inventory.
//
// Aster S5 F1: a fence that derives its wire universe from the four registry
// maps only proves those maps are internally consistent — it cannot see a wire
// dispatched live but registered nowhere, and "every wire is owned" is
// tautological when both sides come from the same maps. So this fence builds its
// wire universe from the ACTUAL live dispatch surface, extracted from source
// (NOT from the registries), and checks the registries against it BIDIRECTIONALLY:
//
//   LIVE INVENTORY L (source-derived, independent of the registries):
//     · routed        — every `on(T.X, …)` handler registered in
//                        src/pubsub/wireHandlers.js (the B1 pub/sub plane).
//     · bridge-ws      — every `case '<wire>':` in web/index.js signaling.dispatch.
//     · bridge-notif   — every `bridge.onNotification('<wire>', …)` in web/index.js.
//     · webrtc-notif   — every `webrtc.onNotification('<wire>', …)` in web/index.js.
//   Each endpoint is keyed by (surface, wire). A newly shipped in-scope wire
//   appears in L the moment its handler is registered, whether or not any registry
//   knows about it — that is what closes F1.
//
//   DECLARED OWNER (the S5 assignment being policed): routed → B1; each transport
//   (surface,wire) → its boundary via TRANSPORT_OWNER. The two `hello` endpoints
//   are pinned by their DISPATCH IDENTITY: bridge-notif/hello → B2 (bridge-link
//   auth), webrtc-notif/hello → B3 (mesh-link auth) — not a bare-string collision.
//
//   BIDIRECTIONAL: INV1 every live endpoint has a declared owner (no unowned live
//   wire); INV1b no declared transport endpoint is stale (every entry is live);
//   INV2 the declared owner's registry actually contains the wire; INV3 every
//   registry wire is a live endpoint of that boundary OR a documented table-only
//   frame (the B4 direction-split: bridge-server-ingested, no kernel dispatch).
//   NEG1-3 prove the fence has teeth: a live-only/unowned endpoint, a wrong
//   reassignment, and a registered-but-not-live wire each FAIL.
//
// This fence proves the registries partition the LIVE in-scope wire surface and
// catches future omission/reassignment. TABLE_ONLY and TRANSPORT_OWNER are the
// two maintained lists; drift in either against the live surface fails a check.
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

console.log('\nREF-1.1 S5 — cross-boundary ownership fence (vs independent live inventory)\n');

// ── Registries: boundary -> Set(wire), from the built .wiring dispatch index ──
const REG = {
  B1: new Set(buildBoundary1Registry().wiring.keys()),
  B2: new Set(buildBoundary2Registry().wiring.keys()),
  B3: new Set(buildBoundary3Registry().wiring.keys()),
  B4: new Set(buildBoundary4Registry().wiring.keys()),
};
const REG_PREFIX = { B1: 'pubsub:', B2: 'transport:', B3: 'mesh:', B4: 'bridge:' };
const regTypeOf = { B1: buildBoundary1Registry().wiring, B2: buildBoundary2Registry().wiring, B3: buildBoundary3Registry().wiring, B4: buildBoundary4Registry().wiring };

// ── L: the INDEPENDENT live inventory, extracted from source dispatch sites ──
const wh = src('../src/pubsub/wireHandlers.js');
const web = src('../src/transport/web/index.js');
// B1 routed plane: on(T.NAME, …) → resolve NAME to the wire value via T.
const routedNames = [...wh.matchAll(/\bon\(T\.([A-Z_]+),/g)].map((m) => m[1]);
const routedWires = routedNames.map((n) => T[n]);
// bridge-ws: only the case labels INSIDE signaling.dispatch (bounded slice).
const dispatchSlice = web.slice(web.indexOf('const signaling = {'), web.indexOf("log('bridge-frame-unhandled'"));
const bridgeWsWires = [...dispatchSlice.matchAll(/case '([a-z][a-z-]*)':/g)].map((m) => m[1]);
const bridgeNotif   = [...web.matchAll(/bridge\.onNotification\('([a-z][a-z-]*)'/g)].map((m) => m[1]);
const webrtcNotif   = [...web.matchAll(/webrtc\.onNotification\('([a-z][a-z-]*)'/g)].map((m) => m[1]);

const L = new Set();   // "surface|wire"
for (const w of routedWires)  L.add(`routed|${w}`);
for (const w of bridgeWsWires) L.add(`bridge-ws|${w}`);
for (const w of bridgeNotif)   L.add(`bridge-notif|${w}`);
for (const w of webrtcNotif)   L.add(`webrtc-notif|${w}`);
const wireOf = (key) => key.slice(key.indexOf('|') + 1);

// ── Declared owner: the S5 assignment being policed (routed→B1; transport map) ──
// hello is pinned by dispatch identity: bridge-notif→B2 (bridge auth), webrtc→B3 (mesh auth).
const TRANSPORT_OWNER = new Map([
  ['bridge-ws|welcome', 'B2'], ['bridge-ws|turn', 'B4'], ['bridge-ws|peer-list', 'B3'],
  ['bridge-ws|peer-joined', 'B3'], ['bridge-ws|peer-left', 'B3'], ['bridge-ws|signal', 'B3'],
  ['bridge-ws|pong', 'B4'], ['bridge-ws|version-gate', 'B4'],
  ['bridge-notif|hello', 'B2'], ['bridge-notif|hello-ack', 'B2'],
  ['webrtc-notif|hello', 'B3'], ['webrtc-notif|hello-sig', 'B3'], ['webrtc-notif|cap-attest', 'B2'],
]);
const declaredOwner = (key) => key.startsWith('routed|') ? 'B1' : (TRANSPORT_OWNER.get(key) || null);
// TABLE_ONLY: registry wires with NO live kernel dispatch — the B4 direction-split
// (bridge-server-ingested, out of kernel scope; carried in the B4 TABLE only).
const TABLE_ONLY = new Set(['B4:client-hello', 'B4:ping', 'B4:turn-refresh', 'B4:peer-list-request']);

// ── Pure fence functions over (L, ownerFn, REG, tableOnly) so NEG can perturb ──
const unownedLive   = (l, owner) => [...l].filter((k) => owner(k) == null);
const staleDeclared = (l) => [...TRANSPORT_OWNER.keys()].filter((k) => !l.has(k));
const forwardMiss   = (l, owner, reg) => [...l].filter((k) => !reg[owner(k)]?.has(wireOf(k)));   // owned but its registry lacks the wire
const backwardMiss  = (l, owner, reg, tableOnly) => {
  const liveByBoundary = {};
  for (const k of l) { const b = owner(k); if (b) (liveByBoundary[b] ??= new Set()).add(wireOf(k)); }
  const miss = [];
  for (const [b, wires] of Object.entries(reg)) for (const w of wires)
    if (!liveByBoundary[b]?.has(w) && !tableOnly.has(`${b}:${w}`)) miss.push(`${b}:${w}`);
  return miss;
};

// ── A. inventory sanity: the extraction actually found the live surface ──
check(`A1. live inventory extracted from source: routed=${routedWires.length}, bridge-ws=${bridgeWsWires.length}, bridge-notif=${bridgeNotif.length}, webrtc-notif=${webrtcNotif.length}`,
  routedWires.length === 19 && bridgeWsWires.length === 8 && bridgeNotif.length === 2 && webrtcNotif.length === 3 && routedWires.every((w) => typeof w === 'string' && w.length > 0),
  `\n   routed=${routedWires.join(',')}`);

// ── INV1/1b. every live endpoint is owned; no declared endpoint is stale ──
check('INV1. every LIVE endpoint has a declared boundary owner — no unowned live wire (the anti-tautology: L is source-derived, not registry-derived)',
  unownedLive(L, declaredOwner).length === 0, `\n   unowned: ${unownedLive(L, declaredOwner).join(', ')}`);
check('INV1b. no declared transport endpoint is stale — every TRANSPORT_OWNER entry maps to a live dispatch site',
  staleDeclared(L).length === 0, `\n   stale: ${staleDeclared(L).join(', ')}`);

// ── INV2. forward: the declared owner's registry actually contains the wire ──
check('INV2. forward coverage: every live endpoint’s declared-owner registry contains its wire (a live wire registered nowhere / in the wrong registry fails)',
  forwardMiss(L, declaredOwner, REG).length === 0, `\n   miss: ${forwardMiss(L, declaredOwner, REG).join(', ')}`);

// ── INV3. backward: every registry wire is live OR documented table-only ──
check('INV3. backward coverage: every registry wire is a live endpoint of that boundary OR a documented table-only frame (a registered wire with no live dispatch, undocumented, fails)',
  backwardMiss(L, declaredOwner, REG, TABLE_ONLY).length === 0, `\n   miss: ${backwardMiss(L, declaredOwner, REG, TABLE_ONLY).join(', ')}`);

// ── INV4. the two hello endpoints pinned by dispatch identity, both real ──
check('INV4. hello is TWO endpoints, pinned by dispatch surface: bridge-notif/hello→B2 (bridge auth) AND webrtc-notif/hello→B3 (mesh auth), each present live AND in its registry',
  L.has('bridge-notif|hello') && L.has('webrtc-notif|hello')
  && declaredOwner('bridge-notif|hello') === 'B2' && declaredOwner('webrtc-notif|hello') === 'B3'
  && REG.B2.has('hello') && REG.B3.has('hello'));

// ── O1. type namespace matches boundary (internal-consistency backstop) ──
{
  const bad = [];
  for (const b of Object.keys(REG)) for (const [wire, info] of regTypeOf[b]) if (!String(info.type).startsWith(REG_PREFIX[b])) bad.push(`${b}:${wire}->${info.type}`);
  check('O1. every registered row type is namespaced to its boundary (pubsub:/transport:/mesh:/bridge:)', bad.length === 0, `\n   off-prefix: ${bad.join(', ')}`);
}

// ── E1-E5. the recorded edge cases pin to exactly their resolved owner ──
check('E1. welcome (TURN + session) → B2 only, never B3', declaredOwner('bridge-ws|welcome') === 'B2' && !REG.B3.has('welcome'));
check('E2. cap-attest is the WIRE (carrying the write-flight-ack-v1 capability codec) — rides the mesh but is auth → B2; there is NO separate `write-flight-ack` wire (Vega S5 read)',
  declaredOwner('webrtc-notif|cap-attest') === 'B2' && REG.B2.has('cap-attest') && !REG.B2.has('write-flight-ack'));
check('E3. peer-list (discovery) → B3 only, never B4', declaredOwner('bridge-ws|peer-list') === 'B3' && !REG.B4.has('peer-list'));
check('E4. peer-list-request → B4 only, never B3 (and is TABLE-ONLY: bridge-server-ingested, no kernel dispatch)', REG.B4.has('peer-list-request') && !REG.B3.has('peer-list-request') && TABLE_ONLY.has('B4:peer-list-request'));
check('E5. turn → B4; no `turn` wire in B2 (welcome carries turn as a FIELD, not a wire)', declaredOwner('bridge-ws|turn') === 'B4' && !REG.B2.has('turn'));

// ── NEG1-3. the fence has teeth: each perturbation must FAIL its invariant ──
{
  const Lghost = new Set([...L, 'bridge-ws|ghost-frame']);   // a live wire owned by nobody
  check('NEG1. a live-only/unowned endpoint FAILS INV1 (proves the fence catches a newly-shipped wire registered nowhere)',
    unownedLive(Lghost, declaredOwner).length === 1);
}
{
  const wrongOwner = (key) => key === 'bridge-ws|pong' ? 'B2' : declaredOwner(key);   // pong misassigned B4→B2
  check('NEG2. a wrong reassignment FAILS INV2 (B2 registry has no `pong`)',
    forwardMiss(L, wrongOwner, REG).some((k) => k === 'bridge-ws|pong'));
}
{
  const regPhantom = { ...REG, B1: new Set([...REG.B1, 'phantom-wire']) };   // registered, not live, not table-only
  check('NEG3. a registered-but-not-live wire (not table-only) FAILS INV3',
    backwardMiss(L, declaredOwner, regPhantom, TABLE_ONLY).includes('B1:phantom-wire'));
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
