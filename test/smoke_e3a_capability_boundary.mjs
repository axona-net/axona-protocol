// =====================================================================
// smoke_e3a_capability_boundary.mjs — REF-1.1 E3: the runtime capability
// boundary for the sealed node WebSocketTransport.
//
// E3a introduced the capability channel + reroute and sealed this transport's own
// methods; E3b.1 sealed the base Transport contract so no inherited stub remains.
// Together they establish Aster's E3 ABSENCE INVARIANT (ASTER-E3A-SEAL-REVIEW): on a
// sealed transport, the registering dispatch primitive is unreachable by name via
// EVERY access path — dotted, computed, prototype walk, Reflect.get — all resolve to
// undefined. "inert/throwing is not the absence invariant": there is no residual
// throwing stub; there is nothing.
//
//   (A) All three primitives resolve to undefined by every access path, with no
//       descriptor anywhere on the prototype chain.
//   (B) Non-vacuous control: registerFrame — sole reader of the module-private
//       WeakMap channel (kind-keyed) — DOES register a handler through the channel.
//   (C) Transitional dual path (E3b removes it): a receiver NOT in the channel is
//       still driven via the literal named method.
//   (D) Contract-level seal: the base Transport prototype declares no dispatch
//       method — the absence is structural at the contract, not per-subclass.
//
// Run: node test/smoke_e3a_capability_boundary.mjs
// =====================================================================
import { WebSocketTransport } from '../src/transport/node/index.js';
import { Transport }          from '../src/contracts/Transport.js';
import { AxonaPeer }          from '../src/dht/AxonaPeer.js';
import { registerFrame }      from '../src/registry/index.js';
import { makeTestRegistry }   from './lib/testRegistry.mjs';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}${extra ? ' — ' + extra : ''}`); failed++; }
};
console.log('\nREF-1.1 E3 — capability boundary (sealed node WebSocketTransport)\n');

const mkSealed = () => new WebSocketTransport({ sendToConn: () => true, isConnOpen: () => true });
const REG = makeTestRegistry([
  { wire: 'probe_req', transportKind: 'request' },
  { wire: 'probe_ntf', transportKind: 'notification' },
]);
const PRIMS = ['onRequest', 'onNotification', 'onRoutedMessage'];
const ownDesc = (obj, key) => Object.getOwnPropertyDescriptor(obj, key);

// ── (A) Absence invariant: undefined by every access path, no residual ──────
{
  const t = mkSealed();
  let allDotted = true, allComputed = true, allConcat = true, allReflect = true, noResidual = true;
  for (const p of PRIMS) {
    if (t[p] !== undefined) allDotted = false;
    if (t[String(p)] !== undefined) allComputed = false;
    if (Reflect.get(t, p) !== undefined) allReflect = false;
    for (let o = t; o; o = Object.getPrototypeOf(o)) if (ownDesc(o, p)) noResidual = false;
  }
  if (t['on' + 'Request'] !== undefined) allConcat = false;
  if (t['onNot' + 'ification'] !== undefined) allConcat = false;
  if (t['onRouted' + 'Message'] !== undefined) allConcat = false;

  check('A1. all three primitives are undefined by dotted access', allDotted);
  check('A2. all three are undefined by computed (bracket) access', allComputed);
  check('A3. all three are undefined by concatenated computed keys', allConcat);
  check('A4. all three are undefined by Reflect.get', allReflect);
  check('A5. no descriptor for any primitive anywhere on the prototype chain (no residual stub)', noResidual);
}

// ── (B) Not vacuous — registerFrame (sole reader) drives the real primitive ──
{
  const t = mkSealed();
  const reqH = (from, body) => `req:${body}`;
  const ntfH = () => {};
  registerFrame(t, 'probe_req', reqH, { registry: REG });
  registerFrame(t, 'probe_ntf', ntfH, { registry: REG });
  check('B1. registerFrame registered the request handler through the channel',
    t._reqHandlers.get('probe_req') === reqH);
  check('B2. registerFrame registered the notification handler through the channel',
    t._ntfHandlers.get('probe_ntf') === ntfH);
  check('B3. exactly the two door-registered wires are present (no leakage)',
    t._reqHandlers.size === 1 && t._ntfHandlers.size === 1);
}

// ── (C) Transitional dual path — an UNSEALED receiver uses the named method ───
{
  const unsealed = {
    _req: new Map(), _ntf: new Map(),
    onRequest(type, h) { this._req.set(type, h); },
    onNotification(type, h) { this._ntf.set(type, h); },
  };
  const h = () => {};
  registerFrame(unsealed, 'probe_ntf', h, { registry: REG });
  check('C1. an unsealed receiver still exposes the public primitive (typeof function)',
    typeof unsealed.onNotification === 'function');
  check('C2. registerFrame drove the unsealed receiver via the named fallback (this preserved)',
    unsealed._ntf.get('probe_ntf') === h);
}

// ── (D) Contract-level seal — the base Transport declares no dispatch method ──
{
  // E3b.1 removed the throwing "not implemented" stubs from the base contract, so
  // the absence is structural: no subclass inherits a reachable dispatch name.
  check('D1. base Transport.prototype declares no own onRequest',
    !ownDesc(Transport.prototype, 'onRequest'));
  check('D2. base Transport.prototype declares no own onNotification',
    !ownDesc(Transport.prototype, 'onNotification'));
  check('D3. a bare Transport instance resolves the dispatch names to undefined',
    (() => { const b = Object.create(Transport.prototype); return b.onRequest === undefined && b.onNotification === undefined; })());
}

// ── (E) The AxonaPeer routed primitive is sealed too (E3b.2c) ─────────────────
{
  // The peer is the routed-dispatch receiver: registerFrame(peer, 'mesh:signal' | the
  // B1 pub/sub wires) binds through its deposited `routed` capability. E3b.2c removed
  // the public onRoutedMessage method, so the name is unreachable on the class — the
  // absence invariant now holds for the peer exactly as it does for every transport.
  let noProtoDesc = true;
  for (let o = AxonaPeer.prototype; o && o !== Object.prototype; o = Object.getPrototypeOf(o)) {
    if (ownDesc(o, 'onRoutedMessage')) noProtoDesc = false;
  }
  check('E1. AxonaPeer.prototype resolves onRoutedMessage to undefined (method removed)',
    AxonaPeer.prototype.onRoutedMessage === undefined);
  check('E2. no onRoutedMessage descriptor anywhere on the AxonaPeer prototype chain (no residual)',
    noProtoDesc);
  check('E3. the OTHER routed access paths are undefined too (computed, Reflect.get)',
    AxonaPeer.prototype['onRouted' + 'Message'] === undefined &&
    Reflect.get(AxonaPeer.prototype, 'onRoutedMessage') === undefined);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
