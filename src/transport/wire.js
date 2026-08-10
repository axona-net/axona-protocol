// =====================================================================
// wire.js — shared JSON codec for Axona wire frames.
//
// Axona protocol values include `bigint` (XOR distances, low-level
// routing math) and `Set` (per-lookup `queried`) — neither survives a
// vanilla `JSON.stringify`.  We work around with a string-suffix
// convention:
//
//   BigInt 0xabc        →  "2748n"   (decimal digits + "n" sentinel)
//   Set([id1, id2, …])  →  [id1, id2, …]
//
// At the API boundary node IDs are 66-char hex strings — they need no
// special encoding.  The codec is for the internal payload types the
// protocol carries (XOR-distance bigints inside routing-decision
// envelopes, queried-set membership maps).
//
// Used by:
//   - src/transport/web/  (WebRTC data channels + bridge WebSocket)
//   - src/transport/node/ (WebSocket transport in the bridge)
//
// The bridge mirrors these conventions so every WS / DC channel uses
// the same wire format.  The protocol layer is responsible for
// wrapping incoming arrays back into a `Set` where it expects one
// (e.g., the `lookup_step` handler re-coerces `payload.queried`).
// =====================================================================

/** JSON.stringify replacer.  Emits BigInt as "<digits>n", Set as array. */
export function bigintReplacer(_key, value) {
  if (typeof value === 'bigint') return value.toString() + 'n';
  if (value instanceof Set)      return [...value];
  return value;
}

/** JSON.parse reviver.  Inverts the "<digits>n" suffix back to BigInt. */
export function bigintReviver(_key, value) {
  if (typeof value === 'string' && /^-?\d+n$/.test(value)) {
    return BigInt(value.slice(0, -1));
  }
  return value;
}

/** Convenience: `JSON.stringify` with the Axona replacer. */
export function encode(msg) {
  return JSON.stringify(msg, bigintReplacer);
}

/** Convenience: `JSON.parse` with the Axona reviver. */
export function decode(text) {
  return JSON.parse(text, bigintReviver);
}

// MAX_FRAME_BYTES — the transport contract's HARD pre-parse ceiling on a single
// inbound wire frame, in UTF-8 BYTES. This is the frame/envelope guard that the
// decoders (node/index.js, web/index.js, web/mesh.js) own; it is NOT the
// registry's per-scalar budget cap (registry/types.js MAX_BYTES_CEILING = 64 KiB),
// which bounds one projected scalar, a different resource.
//
// Justification against the largest LEGITIMATE frame, measured per ingress
// variant (test/smoke_registry_core §10 builds these fixtures):
//   - peer.pub caps an enveloped publish message at MAX_PUBLISH_BYTES = 256 KiB
//     measured as json.length — i.e. JS string-length CHARS (AxonaPeer.js:1812).
//   - The certifier's F7 ceiling counts UTF-8 BYTES (a char is up to 3 bytes for
//     the BMP; a surrogate pair is 4 bytes per 2 chars = 2 bytes/char), so the
//     worst-case body is 3 * 262144 = 786432 bytes.
//   - node WS / web-bridge WS additionally wrap it in {type:'axona', payload:…}
//     and every variant adds the routed envelope (fromId/targetId/type/hopCount) —
//     all ASCII hex/enum, well under 4 KiB. Mesh has no outer wrapper, so the mesh
//     frame is strictly smaller than the node/bridge frame.
// 1 << 20 (1 MiB) covers 786432 + envelope with headroom while bounding the
// pre-parse allocation. A registered row / decoder variant MAY pass a TIGHTER
// ceiling to a certifier, but this hard cap can never be raised past — a supplied
// ceiling above it is rejected, not honored (see snapshotMint._certify).
export const MAX_FRAME_BYTES = 1 << 20;   // 1048576
