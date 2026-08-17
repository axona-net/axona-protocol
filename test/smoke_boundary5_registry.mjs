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

// registerFrame binds every B5 wire via the BARE wire (transportKind OMITTED). B5
// is single-primitive and bare-keyed, so under the STRICT composite/bare partition
// (Aster E2.0 re-review 2795636a cond.2, correction a767b880) a SUPPLIED
// transportKind is composite-only and ALWAYS refuses here — bind only by omitting.
const recv = { onRequest: (wire) => `req:${wire}`, onNotification: (wire) => `notif:${wire}` };
const reg = buildBoundary5Registry();
const threw = (fn) => { try { fn(); return false; } catch { return true; } };
let boundOk = 0, suppliedRefused = 0;
for (const d of defs) {
  const bare = registerFrame(recv, d.wire, () => {}, { registry: reg });   // OMITTED → binds via the bare wire
  if (bare.endsWith(`:${d.wire}`)) boundOk++;
  // SUPPLIED — even the row's OWN kind — is composite-only, so it misses (B5 has no
  // composite keys) and refuses. No bare-row-of-matching-kind fallback.
  if (threw(() => registerFrame(recv, d.wire, () => {}, { registry: reg, transportKind: d.transportKind }))) suppliedRefused++;
}
check('R1. registerFrame binds all 10 wires via the BARE wire when transportKind is OMITTED', boundOk === 10, `\n   ${boundOk}/10`);
check('R1b. a SUPPLIED transportKind — even the row\'s OWN kind — refuses on all 10 wires (strict composite-only; B5 is bare-keyed, no fallback)', suppliedRefused === 10, `\n   ${suppliedRefused}/10`);
check('R2. an undeclared wire is refused', threw(() => registerFrame(recv, 'not_a_dht_wire', () => {}, { registry: reg })));
// STRICT COMPOSITE-ONLY (Aster 2795636a cond.2 + correction a767b880): a supplied
// transportKind takes the composite key ONLY; a miss refuses with NO bare-row
// fallback — NOT EVEN when the bare row's own kind matches. That matching-bare-row
// path was the alternate lookup Aster refused; these prove it is gone.
check('R3. lookup_step named as its OWN kind (request) REFUSES — the matching-bare-row fallback path is gone',
  threw(() => registerFrame(recv, 'lookup_step', () => {}, { registry: reg, transportKind: 'request' })));
check('R4. lookup_step named as a MISMATCHED kind (notification) refuses too — composite miss, no fallback in either direction',
  threw(() => registerFrame(recv, 'lookup_step', () => {}, { registry: reg, transportKind: 'notification' })));
check('R5. a malformed transportKind is rejected', threw(() => registerFrame(recv, 'lookup_step', () => {}, { registry: reg, transportKind: 'bogus' })));

// ── D. FLAG-OFF IDENTITY (the migration's byte-identical property) ──────────
// R1–R5 prove registerFrame ROUTES each bare wire to the right primitive. This block
// proves the leg it wires runs the ORIGINAL handler VERBATIM with ZERO traces when the
// shadow flag is off — the property the E2.5 migration must not break. A capturing recv
// hands back the wrapped handler; enabled:false → the wrap is inert (byte-identical).
{
  const tr = [];
  const reg5 = buildBoundary5Registry({ enabled: false, sink: (t) => tr.push(t) });
  const capture = { onRequest: (wire, h) => h, onNotification: (wire, h) => h };
  let allVerbatim = true, allRethrow = true;
  for (const d of defs) {
    const wrapped = registerFrame(capture, d.wire, (a, b) => `ran:${d.wire}:${a}`, { registry: reg5 });   // bare — transportKind OMITTED
    if (wrapped('X', 'Y') !== `ran:${d.wire}:X`) allVerbatim = false;
    let rethrew = false;
    const boom = registerFrame(capture, d.wire, () => { throw new Error(`boom:${d.wire}`); }, { registry: reg5 });
    try { boom('Z'); } catch (e) { rethrew = e.message === `boom:${d.wire}`; }
    if (!rethrew) allRethrow = false;
  }
  check('D1. flag-off: EVERY B5 leg (all 10 wires) runs its handler VERBATIM (return identical)', allVerbatim);
  check('D2. flag-off: a throwing handler on every leg rethrows VERBATIM', allRethrow);
  check('D3. flag-off: ZERO traces emitted across all legs and invocations (inert wrap, byte-identical)', tr.length === 0, `traces=${tr.length}`);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
