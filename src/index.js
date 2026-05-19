// =====================================================================
// @axona/protocol — public barrel export.
//
// Pure-JS protocol kernel for the Axona peer-to-peer mesh.  Three
// contract surfaces, one per-node DHT implementation (AxonaPeer with
// NH-1 routing + axonal pub/sub), supporting state classes, and
// geographic / hashing helpers.
//
// Consumers (axona-peer, axona-bridge, dht-sim) import named symbols
// from here.  Sub-path imports (e.g. `@axona/protocol/contracts/DHT.js`)
// are also supported via the `exports` map in package.json.
// =====================================================================

// ── Contracts ────────────────────────────────────────────────────────
export { Transport }        from './contracts/Transport.js';
export { DHT }              from './contracts/DHT.js';
export { BootstrapService } from './contracts/BootstrapService.js';

// ── Errors ────────────────────────────────────────────────────────────
export {
  AxonaError,
  IdentityError,
  TransportError,
  PublishError,
  SubscribeError,
  PullError,
  MetricsError,
  UpgradeRequiredError,
  ErrorCodes,
  isWireError,
  fromWire,
} from './errors.js';

// ── Per-node DHT implementation (NH-1) ──────────────────────────────
export { AxonaPeer } from './dht/AxonaPeer.js';
export { DHTNode, GEO_CELL_BITS } from './dht/DHTNode.js';
export { NeuronNode } from './dht/NeuronNode.js';
export { Synapse }    from './dht/Synapse.js';

// ── Pub/sub primitives ─────────────────────────────────────────────
export { AxonManager } from './pubsub/AxonManager.js';
export { AxonPubSub }  from './pubsub/AxonPubSub.js';
export {
  makePost,
  deriveTopicId,
  verifyPostHash,
  verifyTopicOwnership,
  verifySignature,
} from './pubsub/post.js';

// ── Ed25519 helpers (Web Crypto wrapper) ─────────────────────────
// Optional companion to post.js for runtimes that support Web Crypto
// Ed25519 (Chrome 110+, Safari 17+, Firefox 130+, Node 20+).
// Applications on older runtimes can substitute @noble/ed25519 with
// the same shape — post.js's signer/verifier contracts are
// implementation-agnostic.
export {
  generateKeyPair,
  exportPublicKey,
  importPublicKey,
  sign,
  verify,
  makeSigner,
  makeVerifier,
} from './pubsub/ed25519.js';

// ── Utilities ──────────────────────────────────────────────────────
// The big ones the protocol uses directly are re-exported; the
// remaining geo.js helpers (haversine, roundTripLatency, continent
// detection, XOR routing-table builders, etc.) are reachable via
// the `@axona/protocol/utils/geo.js` sub-path import for consumers
// that need them.

// 264-bit identifier math — node ID and topic ID share the same
// keyspace: [8-bit S2 prefix] || [256-bit hash].
export {
  ID_BITS,
  HASH_BITS,
  S2_BITS,
  HEX_CHARS,
  MAX_ID,
  MAX_HASH,
  MAX_S2,
  toHex,
  fromHex,
  isHexId,
  assembleId,
  extractS2Prefix,
  extractHash,
  s2PrefixOfHex,
  xorDistance,
  stratumOf,
  clz264,
  randomU256,
} from './utils/hexid.js';

export {
  randomU32,
  roundTripLatency,
  haversine,
} from './utils/geo.js';

export { geoCellId } from './utils/s2.js';
