// testRegistry.mjs — REF-1.1 E3: a minimal boundary registry for tests that need
// to register a handler on a SEALED transport through the canonical door.
//
// After E3 seals a transport, its onRequest/onNotification are no longer public
// methods; the only way to register a handler is registerFrame(recv, wire, handler,
// { registry }). A transport contract test that exercises dispatch over a synthetic
// wire ('tick', 'echo', …) therefore registers through registerFrame with a tiny
// registry declaring that wire.
//
// This adds NO reader of the capability channel — registerFrame is still the sole
// reader (see src/registry/registerFrame.js). This is test-only DATA (a wiring map)
// plus a passthrough wrap that runs the handler verbatim, matching flag-off
// (byte-identical) dispatch.
//
// makeTestRegistry([{ wire, transportKind }, …]) → a registry object with the exact
// surface registerFrame consumes: { wiring: Map<wire, row>, wrap }.

export function makeTestRegistry(rows) {
  const wiring = new Map();
  for (const r of rows) {
    if (!r || typeof r.wire !== 'string' || !r.wire) {
      throw new TypeError('makeTestRegistry: each row needs a non-empty wire');
    }
    if (r.transportKind !== 'request' && r.transportKind !== 'notification' && r.transportKind !== 'routed') {
      throw new TypeError(`makeTestRegistry(${r.wire}): transportKind must be request|notification|routed`);
    }
    // Bare-wire key (single-primitive, the B1–B5 shape): registerFrame reads the
    // row's own transportKind to select the leg. type === wire (registerFrame passes
    // row.type to wrap; the passthrough ignores it).
    wiring.set(r.wire, { type: r.wire, transportKind: r.transportKind });
  }
  return { wiring, wrap: (_type, handler) => handler };
}

export default makeTestRegistry;
