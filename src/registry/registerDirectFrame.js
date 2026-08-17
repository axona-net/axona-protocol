// =====================================================================
// registerDirectFrame.js — REF-1.1 E3 decision 2: the ONE named registrar for the
// onDirectMessage `direct_${type}` family (Aster ASTER-E3-DESIGN / council-ratified).
//
// registerFrame's literal-wire gate [V2] refuses a COMPUTED wire, and direct
// messaging needs `direct_${type}` for an app-chosen `type`. Rather than grow a
// parameterized variant into the canonical door, the direct family gets this single
// named low-level registrar — an enumerated mechanism shim, frozen by MODULE IDENTITY
// in the S5 ownership fence (decision 3). It is the SOLE place in the kernel that
// constructs a `direct_*` wire.
//
// The seal-relevant invariant it enforces is a SHAPE, not a hardcoded type list
// (council decision 2, option (b), David 2026-08-17): the wire is always
// `direct_` + a well-formed literal type token, never a computed/arbitrary string.
// A non-string, empty, or already-`direct_`-prefixed `type` is refused. App-level
// admission (which types are allowed) stays with the caller's finite, immutable-at-
// construction directMessageTypes gate (AxonaPeer._gateDirectType); this registrar
// adds the structural guarantee that no computed wire escapes the sealed primitive.
//
// It reaches the notification primitive through the allowlisted capability reader
// (a sealed transport) or, transitionally, by literal name (an unsealed transport);
// E3b removes the named fallback once every transport is sealed.
// =====================================================================
import { readDispatchCapability } from './registerFrame.js';

/**
 * Bind `handler` to the `direct_${type}` notification leg of `recv`.
 * @param {object} recv    the transport carrying the notification primitive
 * @param {string} type    a well-formed direct-message type (NOT `direct_`-prefixed)
 * @param {(fromId, payload) => void} handler
 * @returns {void}
 */
export function registerDirectFrame(recv, type, handler) {
  if (!recv || (typeof recv !== 'object' && typeof recv !== 'function')) {
    throw new TypeError('registerDirectFrame: recv (the transport) required');
  }
  // SHAPE invariant (negative tests): the wire is `direct_` + a literal type token.
  // Reject non-string / empty / already-prefixed / computed-looking types so no
  // arbitrary or computed wire can reach the sealed primitive through this registrar.
  if (typeof type !== 'string' || type.length === 0 || type.startsWith('direct_')) {
    throw new TypeError(`registerDirectFrame: malformed direct-message type ${JSON.stringify(type)} — must be a non-empty string, not 'direct_'-prefixed`);
  }
  if (typeof handler !== 'function') {
    throw new TypeError(`registerDirectFrame(${type}): handler function required`);
  }
  const wire = `direct_${type}`;

  // Read the sealed receiver's notification closure through the allowlisted reader;
  // fall back to the literal-named public method for an unsealed transport (removed
  // when E3b seals every transport).
  const cap = readDispatchCapability(recv);
  if (cap) {
    if (typeof cap.notification !== 'function') {
      throw new TypeError(`registerDirectFrame(${type}): sealed recv has no notification dispatch capability`);
    }
    return cap.notification(wire, handler);
  }
  if (typeof recv.onNotification !== 'function') {
    throw new TypeError(`registerDirectFrame(${type}): recv has no onNotification() primitive`);
  }
  return recv.onNotification(wire, handler);
}

export default registerDirectFrame;
