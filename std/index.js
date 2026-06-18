// =====================================================================
// @axona/protocol/std — Axona's standard library of app-layer helpers.
//
// Utilities built ONLY on the public AxonaPeer API (no kernel internals),
// shipped with the package so every consumer shares one tested implementation
// instead of re-inventing them. Loosely the role C's stdio/stdlib plays:
// common, optional, sits beside the core rather than inside it.
//
//   import { chunkBytes, receiveChunkedBytes } from '@axona/protocol/std';
//   import { chunkBytes } from '@axona/protocol/std/chunk';   // or per-module
//
// Modules:
//   chunk     — reliable large-payload chunking + reassembly over pub/sub.
//   publisher — manage/persist publish IDs (the dedup token), decoupled from
//               the ephemeral transport id.
//   (image downsampling and other helpers will land here as sibling modules.)
// =====================================================================
export * from './chunk.js';
