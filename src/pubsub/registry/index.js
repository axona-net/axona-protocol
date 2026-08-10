// registry/index.js — frame contract registries (refactor Phase 1, REF-1.1).
// Shadow-mode only in Phase 1: validate + trace beside the handlers, change no
// acceptance behavior. See types.js (the row shape) and shadowRegistry.js (the
// wrapper + flag).
export { defineRow, FrameKind, EvidenceLevel, CorrelationSubjectKind, Proves } from './types.js';
export { ShadowRegistry, shadowEnabled, setShadowEnabled } from './shadowRegistry.js';
