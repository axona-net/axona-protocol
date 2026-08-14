// =====================================================================
// smoke_boundary_ownership.mjs — REF-1.1 S5: the cross-boundary ownership
// FENCE. Every wire frame belongs to exactly one of the four per-boundary
// registries. This test proves the four boundaries PARTITION the wire surface
// (no wire claimed by two registries) and pins each recorded cross-boundary
// edge case to its resolved owner — so a future edit that reassigns or
// double-claims a frame surfaces here instead of drifting silently (the
// gap-class behind #414: a retired guard let ownership drift unnoticed once).
//
// The four registries partition the wire surface by type namespace:
//   B1 pubsub:*    — pub/sub + DHT data plane      (src/pubsub/boundary1Registry.js)
//   B2 transport:* — transport hello/auth/session + CAP_ATTEST
//   B3 mesh:*      — WebRTC signalling + mesh-auth + peer discovery
//   B4 bridge:*    — bridge administration
//
// RECORDED EDGE CASES (resolved in-code at each definition; PINNED here):
//   * `welcome` carries TURN + session data → B2, explicitly NOT B3
//     (boundary3Registry: "the `welcome` frame is also Boundary-2, not here").
//   * `cap-attest` / write-flight-ack rides the mesh but is auth → B2
//     (boundary2Registry: "belongs to the transport/auth boundary even though
//     the frame rides the mesh").
//   * peer-list / peer-joined / peer-left (discovery) → B3, NOT B4
//     (boundary3Registry: "peer discovery is Boundary-3 … not Boundary-4").
//   * `peer-list-request` (solicits the B3 peer-list) → B4.
//   * TURN: `turn` / `turn-refresh` are B4 wires; `welcome` carries turn as a
//     FIELD, not a `turn` wire — so no `turn` wire collides across B2/B4.
//   * `hello` — the ONE sanctioned dual-registration: the same conceptual
//     authenticated-hello dispatched on TWO channels. B2 = bridge-link auth
//     (`bridge.onNotification('hello')`, web/index.js:939); B3 = mesh-link auth
//     (`webrtc.onNotification('hello')`, web/index.js:982). Same wire label, two
//     transports, disambiguated by the dispatch channel — recorded in
//     boundary3Registry.js's header. It is NOT renamed (the wire labels are
//     shipped; a rename is a behavior change), so it is recorded as an exception.
// =====================================================================
import { buildBoundary1Registry } from '../src/pubsub/boundary1Registry.js';
import { buildBoundary2Registry } from '../src/transport/boundary2Registry.js';
import { buildBoundary3Registry } from '../src/transport/boundary3Registry.js';
import { buildBoundary4Registry } from '../src/transport/boundary4Registry.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

console.log('\nREF-1.1 S5 — cross-boundary ownership fence\n');

// `defineRow` freezes rows without the raw `wire` field; the authoritative
// wire->row map is the built registry's `.wiring` (the dispatch index every
// boundary smoke reads via `reg.wiring.get(wire)`). Its KEYS are the wire labels.
const BOUNDARIES = [
  { id: 'B1', prefix: 'pubsub:',    wiring: buildBoundary1Registry().wiring },
  { id: 'B2', prefix: 'transport:', wiring: buildBoundary2Registry().wiring },
  { id: 'B3', prefix: 'mesh:',      wiring: buildBoundary3Registry().wiring },
  { id: 'B4', prefix: 'bridge:',    wiring: buildBoundary4Registry().wiring },
];

// Per-boundary WIRE set = the dispatch-index keys (one entry per wire; variant
// rows sharing a wire — e.g. B1 INGESTACK signed+legacy — collapse to one key).
const wiresOf = (b) => new Set(b.wiring.keys());
const bWires = (id) => wiresOf(BOUNDARIES.find((x) => x.id === id));
const has = (id, wire) => bWires(id).has(wire);

// ── O1. TYPE NAMESPACE = BOUNDARY: every wire's row type prefix matches its registry ──
{
  const bad = [];
  for (const b of BOUNDARIES) for (const [wire, info] of b.wiring) if (!String(info.type).startsWith(b.prefix)) bad.push(`${b.id}:${wire}->${info.type}`);
  check('O1. every registered row type is namespaced to its boundary (pubsub:/transport:/mesh:/bridge:) — ownership is encoded in the type, not inferred',
    bad.length === 0, `\n   off-prefix: ${bad.join(', ')}`);
}

// ── O2. DISJOINT WIRES modulo recorded exceptions: no UNSANCTIONED wire is ──
// claimed by two boundaries. The drift fence: a future edit that double-claims a
// NEW wire (the ambiguity S5 resolves) surfaces here instead of shipping silently.
// SANCTIONED lists the exact recorded dual-registrations (resolve-or-record).
const SANCTIONED = new Map([['hello', ['B2', 'B3']]]);
{
  const owner = new Map();   // wire -> [boundary ids]
  for (const b of BOUNDARIES) for (const w of wiresOf(b)) {
    if (!owner.has(w)) owner.set(w, []);
    owner.get(w).push(b.id);
  }
  const collisions = [...owner].filter(([, ids]) => ids.length > 1);
  const isSanctioned = ([w, ids]) => {
    const s = SANCTIONED.get(w);
    return s && s.length === ids.length && [...ids].sort().join(',') === [...s].sort().join(',');
  };
  const unsanctioned = collisions.filter((c) => !isSanctioned(c));
  check('O2. the four boundaries partition the wire surface EXCEPT recorded exceptions — no UNSANCTIONED wire appears in two registries',
    unsanctioned.length === 0, `\n   unsanctioned: ${unsanctioned.map(([w, ids]) => `${w}(${ids.join('+')})`).join(', ')}`);
  // The recorded exception must be LIVE, not stale: `hello` really is B2+B3.
  check('O2b. the recorded `hello` dual-registration is present exactly as recorded (B2 bridge-auth + B3 mesh-auth), so the exception is not stale',
    (owner.get('hello') || []).slice().sort().join(',') === 'B2,B3', `\n   hello owners: ${(owner.get('hello') || []).join(',')}`);
}

// ── E. RECORDED EDGE CASES pinned to their resolved owner (resolve-or-record) ──
check('E1. welcome (TURN + session) → B2 ONLY, never B3',
  has('B2', 'welcome') && !has('B3', 'welcome'), `\n   B2=${has('B2','welcome')} B3=${has('B3','welcome')}`);
check('E2. cap-attest / write-flight-ack (rides the mesh but is auth) → B2',
  has('B2', 'cap-attest'), `\n   B2 has cap-attest=${has('B2','cap-attest')}`);
check('E3. peer discovery (peer-list) → B3 ONLY, never B4',
  has('B3', 'peer-list') && !has('B4', 'peer-list'), `\n   B3=${has('B3','peer-list')} B4=${has('B4','peer-list')}`);
check('E4. peer-list-request (solicits the B3 peer-list) → B4 ONLY, never B3',
  has('B4', 'peer-list-request') && !has('B3', 'peer-list-request'), `\n   B4=${has('B4','peer-list-request')} B3=${has('B3','peer-list-request')}`);
check('E5. TURN: turn is a B4 wire; no `turn` wire leaks into B2 (welcome carries turn as a FIELD, not a wire)',
  has('B4', 'turn') && !has('B2', 'turn'), `\n   B4=${has('B4','turn')} B2=${has('B2','turn')}`);

// ── C1. COVERAGE: the ONLY cross-boundary overlap is the recorded exception ──
{
  const total = BOUNDARIES.reduce((n, b) => n + wiresOf(b).size, 0);
  const union = new Set(BOUNDARIES.flatMap((b) => [...wiresOf(b)]));
  const overlap = total - union.size;                       // Σ − |union| = duplicated-wire count
  const sanctionedOverlap = [...SANCTIONED.values()].reduce((n, ids) => n + (ids.length - 1), 0);
  check(`C1. every wire is owned; the ONLY cross-boundary overlap is the sanctioned exception(s) (Σ=${total}, union=${union.size}, overlap=${overlap}, sanctioned=${sanctionedOverlap})`,
    overlap === sanctionedOverlap, `\n   overlap=${overlap} expected=${sanctionedOverlap}`);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
