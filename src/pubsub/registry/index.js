// registry/index.js — frame contract registries (refactor Phase 1, REF-1.1).
// Shadow-mode only in Phase 1: validate + trace beside the handlers, change no
// acceptance behavior. See types.js (the row shape) and shadowRegistry.js (the
// wrapper + flag).
export { defineRow, FrameKind, EvidenceLevel, CorrelationSubjectKind, Proves, FactType } from './types.js';
export { ShadowRegistry, shadowEnabled, setShadowEnabled } from './shadowRegistry.js';
// NOTE: the snapshot mint (certify) is deliberately NOT re-exported, and its
// subpath is blocked in package.json. That is API ENCAPSULATION / hygiene, NOT a
// security boundary — a consumer can still resolve the file by URL (Aster S1g).
// The security property does not rely on unreachability: certify takes text, and
// the dispatcher classifies nodes without touching any prototype or constructor,
// so even a reachable certify cannot produce a value whose observation fires a
// trap. This holds under the module's stated trust boundary — intact realm
// intrinsics/prototypes for the whole certification-and-dispatch lifetime (see
// snapshotMint.js). This module does not claim post-load intrinsic-tamper
// resistance; same-realm intrinsic replacement is out of scope kernel-wide.
