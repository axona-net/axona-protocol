// =====================================================================
// registerFrame.js — REF-1.1 E1: the ONE canonical frame-registration door.
//
// The enforcement cutover makes this function the sole path by which a frame
// handler is wired to a raw dispatch primitive. E1 lands the door and proves it;
// it does NOT migrate the existing call sites (E2) and does NOT seal the raw
// primitives (E3). At E1 the raw methods are still public and still used by the
// frozen baseline of legacy sites; registerFrame is the intended sole door going
// forward, exercised and proven on Boundary-1's rows.
//
// Design: REF-1.1-Enforcement-Cutover-Design-v5.md. This module realizes exit
// criterion 1 (one canonical path), the shadow-wrap reuse [V5], and the
// registration-time ownership refuse (exit criterion 4).
//
//   registerFrame(recv, wire, handler, { registry })
//
// - `recv`     the receiver carrying the raw dispatch primitive (e.g. the DHT
//              adapter for Boundary-1; a transport for Boundary-2/3/4 at E2).
// - `wire`     the frame-type constant. The AST wire-literal gate [V2] enforces
//              that call sites pass a literal / frozen-constant here; this function
//              enforces at runtime that the registry declares it.
// - `handler`  the frame handler; wrapped by the shared ShadowRegistry.wrap [V5].
// - registry   the boundary ShadowRegistry (carries `.wiring` wire->row, `.wrap`,
//              and `.mintLive`). The row's `transportKind` selects the primitive
//              INTERNALLY — one door, not two (exit criterion 1 [V2]).
//
// THE OBSERVATION CERTIFIER IS REGISTRY-OWNED, NOT A CALLER ARGUMENT (Aster E1
// review F1). Each boundary registry carries its own `mintLive` (B1 =
// certifyBigint∘encode); registerFrame reads it from the registry and threads it
// into the wrap. There is NO public caller-supplied path to live-observation
// certification. And observation itself only fires when the runtime shadow flag is
// on (default off), so a public registerFrame call is shadow-only regardless.
//
// SHADOW-MODE ONLY, wire unchanged, WIRE_VERSION 4.0. When the shadow flag is off
// (default) the wrap runs the handler verbatim, so flag-off is byte-identical.
// =====================================================================

export function registerFrame(recv, wire, handler, { registry } = {}) {
  if (!recv || (typeof recv !== 'object' && typeof recv !== 'function')) {
    throw new TypeError('registerFrame: recv (an object carrying the dispatch primitive) required');
  }
  if (typeof wire !== 'string' || !wire) {
    throw new TypeError('registerFrame: wire (non-empty frame-type string) required');
  }
  if (typeof handler !== 'function') {
    throw new TypeError(`registerFrame(${wire}): handler function required`);
  }
  if (!registry || typeof registry.wiring?.get !== 'function' || typeof registry.wrap !== 'function') {
    throw new TypeError(`registerFrame(${wire}): a boundary registry with .wiring and .wrap required`);
  }

  // Ownership refuse (exit criterion 4): the registry rows are the single source
  // of truth for which (boundary, wire) pairs exist. A wire no row declares is a
  // mis-bound registration — refuse it AT REGISTRATION, do not silently wire it.
  const row = registry.wiring.get(wire);
  if (!row) {
    throw new Error(`registerFrame: no registry row declares wire "${wire}" — refusing an undeclared (recv, wire) binding`);
  }

  // The observation certifier is REGISTRY-OWNED (Aster F1): read it from the
  // registry, never from a caller argument. undefined is fine (the wrap treats a
  // missing mintLive as the unbranded-source path).
  const mintLive = registry.mintLive;

  // [V5] Reuse the EXISTING shadow wrap — not a second wrapper. Flag-off it runs
  // the handler verbatim (byte-identical); flag-on it observes a certified snapshot
  // beside the handler and emits a trace, never mutating args or the return value.
  const wrapped = registry.wrap(row.type, handler, {
    ...(mintLive ? { mintLive } : {}),
    ...(row.variantBy ? { variantBy: row.variantBy } : {}),
  });

  // Select the raw dispatch primitive INTERNALLY from the row's transport kind —
  // one door, not two (exit criterion 1 [V2]). This module is the ALLOWLISTED
  // holder of the raw primitives, keyed by module identity in the AST gate: it
  // reaches them by NAME here (never by computed access, the shape the runtime
  // boundary forbids), and at E3 these named methods become the closure capability
  // this door closes over. No other module may name them once E3 seals them.
  switch (row.transportKind) {
    case 'routed':
      if (typeof recv.onRoutedMessage !== 'function') throw new TypeError(`registerFrame(${wire}): recv has no onRoutedMessage() primitive`);
      return recv.onRoutedMessage(wire, wrapped);
    case 'notification':
      if (typeof recv.onNotification !== 'function') throw new TypeError(`registerFrame(${wire}): recv has no onNotification() primitive`);
      return recv.onNotification(wire, wrapped);
    case 'request':
      if (typeof recv.onRequest !== 'function') throw new TypeError(`registerFrame(${wire}): recv has no onRequest() primitive`);
      return recv.onRequest(wire, wrapped);
    default:
      throw new Error(`registerFrame(${wire}): row.transportKind "${row.transportKind}" has no known dispatch primitive`);
  }
}

export default registerFrame;
