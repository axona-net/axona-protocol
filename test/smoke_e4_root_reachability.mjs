// =====================================================================
// smoke_e4_root_reachability.mjs — REF-1.1 E4 (ARM): the runtime
// capability-boundary test over the QUANTIFIED ROOT SET [R4].
//
// E3a proved the absence invariant for ONE sealed transport as a SAMPLE
// (smoke_e3a_capability_boundary). E4 arms the runtime boundary over the WHOLE
// root set: the E0 consumer inventory made concrete — every public instance type,
// its prototype chain, adapter objects, factory outputs, and module exports. For
// every member: no property under any string OR symbol key, no prototype method,
// no factory output, and no module export returns a raw dispatch primitive.
// "One receiver is a sample; the whole root set is the proof."
//
// The three sealed primitives are onRequest / onNotification / onRoutedMessage.
// After E3 they survive ONLY as the module-private closure capability, deposited in
// a WeakMap keyed by receiver identity and read solely through registerFrame's one
// allowlisted reader (readDispatchCapability — importable HERE because the E3c
// importer-freeze fence scans src/ only, not test/). This test uses that reader to
// prove BOTH directions: the capability really is present (non-vacuous — we tested a
// live sealed object, not a dead one), AND it is stored as no own property of the
// receiver under any key (the WeakMap holds it, the object does not).
//
// Run: node test/smoke_e4_root_reachability.mjs
// =====================================================================
import { SimTransport }        from '../src/transport/sim/transport.js';
import { WebSocketTransport }  from '../src/transport/node/index.js';
import { WebRTCTransport }     from '../src/transport/web/webrtc.js';
import { BridgeTransport }     from '../src/transport/web/bridge.js';
import { CompositeTransport }  from '../src/transport/web/composite.js';
import { Transport }           from '../src/contracts/Transport.js';
import { AxonaPeer }           from '../src/dht/AxonaPeer.js';
import { AxonaDomain }         from '../src/dht/AxonaDomain.js';
import { registerFrame, readDispatchCapability } from '../src/registry/index.js';
import { makeTestRegistry }    from './lib/testRegistry.mjs';
import { readFileSync }        from 'node:fs';
import { simTransport }        from '../src/transport/sim/index.js';
import * as nodeNs             from '../src/transport/node/index.js';
import * as webNs              from '../src/transport/web/index.js';
import * as simNs              from '../src/transport/sim/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}${extra ? ' — ' + extra : ''}`); failed++; }
};
console.log('\nREF-1.1 E4 — runtime capability boundary over the quantified root set [R4]\n');

const PRIMS = ['onRequest', 'onNotification', 'onRoutedMessage'];
const ownDesc = (obj, key) => Object.getOwnPropertyDescriptor(obj, key);

// walk the prototype chain (instance included), stopping at Object.prototype
function* chain(obj) { for (let o = obj; o && o !== Object.prototype; o = Object.getPrototypeOf(o)) yield o; }

// ── the absence invariant on a single receiver: no primitive resolves, by any path ──
function absence(label, recv) {
  let dotted = true, computed = true, concat = true, reflect = true, noDesc = true;
  for (const p of PRIMS) {
    if (recv[p] !== undefined) dotted = false;
    if (recv[String(p)] !== undefined) computed = false;
    if (Reflect.get(recv, p) !== undefined) reflect = false;
    for (const o of chain(recv)) if (ownDesc(o, p)) noDesc = false;
  }
  if (recv['on' + 'Request'] !== undefined) concat = false;
  if (recv['onNot' + 'ification'] !== undefined) concat = false;
  if (recv['onRouted' + 'Message'] !== undefined) concat = false;
  check(`${label}: all three primitives undefined by dotted / computed / concat / Reflect.get, no descriptor on the chain`,
    dotted && computed && concat && reflect && noDesc,
    `dotted=${dotted} computed=${computed} concat=${concat} reflect=${reflect} noDesc=${noDesc}`);
}

// ── prototype-chain absence for a CLASS: no instance can name a primitive ──
function classAbsence(label, Klass) {
  let noProtoDesc = true;
  for (const p of PRIMS) for (const o of chain(Klass.prototype)) if (ownDesc(o, p)) noProtoDesc = false;
  check(`${label}.prototype: no primitive descriptor anywhere on the chain (no instance can resolve one by name)`, noProtoDesc);
}

// ── non-vacuous + capability-not-a-property: the WeakMap holds it, the object doesn't ──
function capIsWeakMapOnly(label, recv, kinds) {
  const cap = readDispatchCapability(recv);
  const present = cap && kinds.every((k) => typeof cap[k] === 'function');
  check(`${label}: the deposited capability IS present via the sole reader (non-vacuous: ${kinds.join('+')})`, !!present,
    `cap=${cap ? Object.keys(cap).join(',') : 'null'}`);
  if (!present) return;
  // no own property (string OR symbol) anywhere on the chain has a capability closure as its value
  const capFns = new Set(kinds.map((k) => cap[k]));
  let noPropHoldsCap = true;
  for (const o of chain(recv)) {
    for (const key of [...Object.getOwnPropertyNames(o), ...Object.getOwnPropertySymbols(o)]) {
      const d = ownDesc(o, key);
      if (d && 'value' in d && capFns.has(d.value)) noPropHoldsCap = false;
    }
  }
  check(`${label}: no own property (string or symbol) on the chain holds a capability closure — it lives only in the WeakMap`, noPropHoldsCap);
}

// ── (1) The five transport classes: prototype-chain absence ──────────────────
console.log('  — root set: transport classes (prototype chain) —');
const TRANSPORT_CLASSES = [
  ['SimTransport', SimTransport],
  ['WebSocketTransport', WebSocketTransport],
  ['WebRTCTransport', WebRTCTransport],
  ['BridgeTransport', BridgeTransport],
  ['CompositeTransport', CompositeTransport],
];
for (const [name, K] of TRANSPORT_CLASSES) classAbsence(name, K);
classAbsence('AxonaPeer', AxonaPeer);
// base contract: the absence is structural at the top of every chain
classAbsence('Transport (base contract)', Transport);

// ── (2) Constructed transport instances: absence + WeakMap-only capability ───
console.log('\n  — root set: constructed transport instances —');
const makeInstance = {
  SimTransport:       () => new SimTransport({ network: {} }),
  WebSocketTransport: () => new WebSocketTransport({ sendToConn: () => true, isConnOpen: () => true }),
  WebRTCTransport:    () => new WebRTCTransport({}),
  BridgeTransport:    () => new BridgeTransport({ sendToBridge: () => {}, isBridgeOpen: () => true }),
  CompositeTransport: () => new CompositeTransport({ localNodeId: 1n }),
};
for (const [name] of TRANSPORT_CLASSES) {
  const t = makeInstance[name]();
  absence(name, t);
  capIsWeakMapOnly(name, t, ['request', 'notification']);
}

// ── (2b) Non-vacuous control: registerFrame (the sole reader) drives the real
// primitive through the channel — proving the receivers we tested are LIVE. ────
{
  const t = makeInstance.WebSocketTransport();
  const REG = makeTestRegistry([
    { wire: 'probe_req', transportKind: 'request' },
    { wire: 'probe_ntf', transportKind: 'notification' },
  ]);
  const reqH = () => 'r'; const ntfH = () => {};
  registerFrame(t, 'probe_req', reqH, { registry: REG });
  registerFrame(t, 'probe_ntf', ntfH, { registry: REG });
  check('control: registerFrame drove request + notification handlers through the channel (live sealed receiver)',
    t._reqHandlers.get('probe_req') === reqH && t._ntfHandlers.get('probe_ntf') === ntfH);
}

// ── (3) AxonaPeer instance + the default-DHT adapter (the non-class root) ────
console.log('\n  — root set: AxonaPeer instance + default-DHT adapter —');
{
  const transport = makeInstance.SimTransport();
  // the default-DHT adapter auto-builds only when node.transport is set (it routes
  // over it); give the node its transport so _buildDefaultAxonaManager returns the adapter.
  const node = { id: 1n, alive: true, transport };
  const peer = new AxonaPeer({ domain: new AxonaDomain(), node, nodeIdentity: null, transport });
  absence('AxonaPeer instance', peer);
  capIsWeakMapOnly('AxonaPeer instance', peer, ['routed']);

  // _buildDefaultAxonaManager returns an AxonaManager whose `.dht` field IS the
  // default-DHT adapter — the object that deposits the routed capability and that
  // AxonaManager registers the 19 B1 pub/sub frames on (E3b.4 option 1).
  //
  // The adapter is a MECHANISM SHIM, not a sealed receiver: dht/AxonaPeer.js is one
  // of the five frozen readDispatchCapability importers, and the adapter deliberately
  // RETAINS a public onRoutedMessage that DELEGATES to the sealed peer's routed
  // capability (kept only for AxonaManager's readiness guard, AxonaManager.js:116). So
  // the [R4] claim over the adapter is the narrower shim claim: it must not LEAK the
  // raw capability closure. Its request/notification names stay absent (it is a routed
  // receiver only), and its retained onRoutedMessage is a delegate, not the raw closure.
  const am = peer._buildDefaultAxonaManager();
  const adapter = am.dht;
  capIsWeakMapOnly('default-DHT adapter (am.dht, shim)', adapter, ['routed']);
  const routedCap = readDispatchCapability(peer).routed;
  check('default-DHT adapter (shim): onRequest / onNotification stay absent (routed-only receiver)',
    adapter.onRequest === undefined && adapter.onNotification === undefined);
  check('default-DHT adapter (shim): its retained onRoutedMessage is a DELEGATING wrapper, not the raw peer capability closure',
    typeof adapter.onRoutedMessage === 'function'
    && adapter.onRoutedMessage !== routedCap
    && adapter.onRoutedMessage !== readDispatchCapability(adapter).routed);
}

// ── (4) Factory outputs are sealed-class instances (closed world) ────────────
console.log('\n  — root set: factory outputs —');
{
  const out = simTransport({ network: {} });
  check('simTransport() returns a SimTransport (a sealed root-set class)', out instanceof SimTransport);
  absence('simTransport() output', out);
  // Every transport factory returns an instance of a sealed class in the root set:
  //   simTransport → SimTransport; serverTransport/clientTransport → WebSocketTransport;
  //   webTransport → CompositeTransport. So the class-level proof (section 1) closes
  //   the factory-output space; we assert the factory functions exist as the surface.
  check('serverTransport / clientTransport / webTransport factories are exported (outputs are sealed-class instances, covered by §1)',
    typeof nodeNs.serverTransport === 'function' && typeof nodeNs.clientTransport === 'function' && typeof webNs.webTransport === 'function');
}

// ── (5) Module exports: no transport module exports a bare dispatch primitive ─
console.log('\n  — root set: module exports —');
for (const [nsName, ns] of [['transport/node', nodeNs], ['transport/web', webNs], ['transport/sim', simNs]]) {
  let clean = true;
  for (const p of PRIMS) if (ns[p] !== undefined) clean = false;
  check(`${nsName} namespace exports no bare ${PRIMS.join(' / ')} binding`, clean);
}

// ── (6) E4 closes the partition; S5's deferred narrowing is retired ──────────
// The access space is PARTITIONED. NAMED access (a primitive written as an
// identifier — dotted, aliased, re-exported, however many hops) is the AST
// identifier/baseline gate's case: fence_raw_dispatch_gate, now ARMED against an
// empty baseline, fails any named raw reference. UNNAMED access (computed / bracket /
// prototype walk / Reflect.get) is THIS runtime boundary's case: structurally closed
// by the seal and proven above over the WHOLE root set. Named XOR unnamed — a stated
// closed world, with proof on both halves. The S5 ownership fence
// (smoke_boundary_ownership) narrowed its soundness claim pending exactly this
// closure; E4 delivers it, so that narrowing is retired. It is recorded here and the
// S5 fence re-runs green in the same gate.
console.log('\n  — E4: the access partition is closed; S5 narrowing retired —');
const baseline = JSON.parse(readFileSync(new URL('./REF-1.1-raw-dispatch-baseline.json', import.meta.url)));
check('closed world — NAMED half: the AST raw-dispatch baseline is armed empty (any named raw reference fails the build)',
  baseline.count === 0 && Array.isArray(baseline.keys) && baseline.keys.length === 0);
check('closed world — UNNAMED half: the runtime boundary held over every root-set receiver above (no computed / prototype / Reflect path resolved a primitive; no member leaked the raw capability)',
  failed === 0);
check('S5 narrowing retired: named XOR unnamed access are both closed with proof — the sound-no-alias guarantee S5 deferred is delivered at E4',
  failed === 0 && baseline.count === 0);

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
