// registry/index.js — frame contract registries (refactor Phase 1, REF-1.1).
// Shadow-mode only in Phase 1: validate + trace beside the handlers, change no
// acceptance behavior. See types.js (the row shape) and shadowRegistry.js (the
// wrapper + flag).
export { defineRow, FrameKind, EvidenceLevel, CorrelationSubjectKind, Proves, FactType } from './types.js';
export { ShadowRegistry, shadowEnabled, setShadowEnabled } from './shadowRegistry.js';
// NOTE: the snapshot mint (certify) is deliberately NOT exported. Provenance is
// a decoder-private capability (snapshotMint.js, blocked from package exports),
// so no public consumer can mint the trusted brand (Aster S1f).
