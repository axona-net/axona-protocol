// =====================================================================
// smoke_e3a_capability_boundary.mjs — REF-1.1 E3a: the runtime capability
// boundary for the first sealed transport (node WebSocketTransport).
//
// E3a settles decision 1 (the capability-channel shape) in code: a sealed transport's
// dispatch primitives become closures captured in a module-private WeakMap, keyed by
// dispatch KIND (request|notification|routed) — NOT by the public method names — and
// read solely by registerFrame. This test proves the boundary on a REAL constructed
// transport:
//
//   (A) The REGISTERING PRIMITIVE is unreachable by name. The sealed class defines no
//       onRequest / onNotification method — no own property on the instance, none on
//       the WebSocketTransport prototype. onRoutedMessage never existed on a transport
//       and is undefined by every access path (dotted, computed, Reflect.get) with no
//       residual on the chain.
//   (B) Non-vacuous control: registerFrame — the sole reader of the channel — DOES
//       register a handler, populating the real handler maps. The capability works;
//       only the public name is gone.
//   (C) Transitional dual path (E3a): an unsealed receiver is still driven via the
//       literal named method; removed in E3b once every transport is sealed.
//   (D) HONEST E3a BOUNDARY: the ONLY name that still resolves for onRequest /
//       onNotification is the inert base Transport contract stub (the interface
//       declaration), reached by walking PAST the sealed subclass. It throws "not
//       implemented" and registers nothing — it is not the primitive. E3b seals the
//       base contract (Transport.js), at which point these names resolve to undefined
//       by every path; E3a proves the CAPABILITY is off-door-unreachable and the
//       residual stub is inert.
//
// Run: node test/smoke_e3a_capability_boundary.mjs
// =====================================================================
import { WebSocketTransport } from '../src/transport/node/index.js';
import { Transport }          from '../src/contracts/Transport.js';
import { registerFrame }      from '../src/registry/index.js';
import { makeTestRegistry }   from './lib/testRegistry.mjs';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}${extra ? ' — ' + extra : ''}`); failed++; }
};
console.log('\nREF-1.1 E3a — capability boundary (sealed node WebSocketTransport)\n');

const mkSealed = () => new WebSocketTransport({ sendToConn: () => true, isConnOpen: () => true });
const REG = makeTestRegistry([
  { wire: 'probe_req', transportKind: 'request' },
  { wire: 'probe_ntf', transportKind: 'notification' },
]);

const ownDesc = (obj, key) => Object.getOwnPropertyDescriptor(obj, key);

// ── (A) The registering primitive is unreachable by name ────────────────────
{
  const t = mkSealed();
  // The sealed class defines no own dispatch method on the instance…
  check('A1. no own onRequest/onNotification on the sealed instance',
    !ownDesc(t, 'onRequest') && !ownDesc(t, 'onNotification'));
  // …nor on the WebSocketTransport prototype (the overrides were removed).
  check('A2. WebSocketTransport.prototype defines no own onRequest/onNotification',
    !ownDesc(WebSocketTransport.prototype, 'onRequest') && !ownDesc(WebSocketTransport.prototype, 'onNotification'));

  // onRoutedMessage never existed on a transport or on the base contract — undefined
  // by every access path, with no residual descriptor anywhere on the chain.
  let routedResidual = false;
  for (let o = t; o; o = Object.getPrototypeOf(o)) if (ownDesc(o, 'onRoutedMessage')) routedResidual = true;
  check('A3. onRoutedMessage is undefined by dotted/computed/Reflect access with no residual on the chain',
    !routedResidual
    && t.onRoutedMessage === undefined
    && t['onRoutedMessage'] === undefined
    && t['onRouted' + 'Message'] === undefined
    && Reflect.get(t, 'onRoutedMessage') === undefined);

  // The capability channel is NOT keyed by the public method names, so the seal
  // removes those names from the running program: they survive nowhere the caller
  // can name to register — not on the instance, not on the subclass prototype.
  check('A4. neither instance nor subclass prototype exposes a callable request/notification primitive',
    typeof ownDesc(t, 'onRequest')?.value !== 'function'
    && typeof ownDesc(WebSocketTransport.prototype, 'onNotification')?.value !== 'function');
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

// ── (D) Honest boundary — the residual base contract stub is inert ───────────
{
  const t = mkSealed();
  // The nearest onRequest reachable up the chain is the BASE Transport contract stub,
  // reached only by walking PAST the sealed subclass (which defines none). It is the
  // interface declaration, NOT the primitive: it throws and registers nothing. E3b
  // seals it, tightening t.onRequest to undefined.
  let nearest, nearestProto;
  for (let o = t; o; o = Object.getPrototypeOf(o)) {
    const d = ownDesc(o, 'onRequest');
    if (d) { nearest = d; nearestProto = o; break; }
  }
  check('D1. the nearest onRequest on the chain is the base Transport.prototype stub (not the subclass)',
    nearestProto === Transport.prototype && typeof nearest?.value === 'function');
  let stubThrew = false;
  try { t.onRequest('x', () => {}); } catch (e) { stubThrew = /not implemented/.test(e.message); }
  check('D2. calling the reachable name hits the inert stub: throws "not implemented", registers nothing',
    stubThrew && t._reqHandlers.size === 0);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
