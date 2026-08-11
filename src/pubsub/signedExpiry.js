// =====================================================================
// signedExpiry.js — REF-1.1 S2.0c: signed immutable message expiry, the
// Option-1 V2 content-address + committed-deadline machinery.
//
// STATUS: PURE + FENCED. This module provides the V2 msgId + expiry functions
// ONLY. It does NOT change the default envelope wire path: buildEnvelope /
// verifyEnvelope / computeMsgId in envelope.js still produce the V1 content
// address, so the running network (all on msgId V1) is unaffected and the full
// suite stays green. Making V2 the active envelope format is the FLAG-DAY
// cutover — a separate, David-gated deploy step, not this tranche. Building the
// crypto core fenced-and-tested first, then cutting over under its own gate, is
// the same review shape the council accepted for Phase 1 (tombstoneAuth) and
// for S2.0a/b/c.
//
// WHAT THIS IS (the accepted design, REF-1.1-S2.0c-Signed-Expiry-Design-v6.md):
//   msgId(V2) = sha256( canonical({ d, exp, message, publisher, topicId }) )
//     d        = "axona:pubsub-msgid:v2"   (literal domain tag)
//     publisher = exactly 64 lowercase hex (32-byte Ed25519 key) OR null
//                 (the fenced anonymous case — local-only, non-transferable)
//     topicId   = exactly 66 lowercase hex (region byte + 32)
//   Both widths are validated BEFORE hashing; any other form (66-hex publisher,
//   64-hex topicId, upper-case, non-hex, wrong length) is rejected pre-hash.
//   canonical() key-sorts, so the serialized order is d, exp, message,
//   publisher, topicId. The legacy V1 id is byte-preserved: no `d` field,
//   sha256(canonical({publisher, message})), never re-hashed. V1-vs-V2 is
//   decided at the flag-day cutoff.
//   Lifetime: ts < exp <= ts + TTL_CEILING; one effectiveDeath = exp + CLOCK_SKEW
//   used everywhere; a body is fresh iff now <= effectiveDeath.
//
// Committing exp into the content address is what makes the deadline immutable
// and COLD-VERIFIABLE: a receiver recomputes msgId from (publisher, message,
// topicId, exp) and any tampered exp yields a different id, so a migrated kill
// naming an msgId cannot silently carry a longer life than the author signed.
//
// PROFILE NOTE: 64/66 are the normative PRODUCTION widths. Under the shrunk sim
// keyspace profile (configureKeyspace / AUTH_VERIFY_RELAXED) the ids are the
// profile's truncated widths and enter computeMsgIdV2 at that width, exactly as
// they do today; callers on that path pass { enforceWidths: false }. The sim
// profile is not a production identity and its records are non-transferable to
// production across the flag-day cutoff.
//
// PURITY: no Date.now() here — every time function takes `now`/`ts` from the
// caller, so it is deterministic and testable.
// =====================================================================

import { canonical, sha256Hex } from './post.js';
import { TTL_MS } from './constants.js';

export const MSGID_DOMAIN_V2 = 'axona:pubsub-msgid:v2';

// Lifetime constants. NOTE: tombstoneAuth.js (Phase 1) currently carries its
// own CLOCK_SKEW / TTL_CEILING copies with these same values; the flag-day
// integration collapses those to a single import from this module (this is the
// natural home for the expiry layer). Tracked; not reconciled here because
// Phase 1 is already accepted and both modules are unwired.
export const TTL_CEILING = TTL_MS;   // 24h absolute hold ceiling (constants.TTL_MS)
export const CLOCK_SKEW  = 5_000;    // symmetric clock-skew allowance (ms)

// Production identity widths. Lowercase-hex only, exact length — no normalize.
const PUBLISHER_RE = /^[0-9a-f]{64}$/;   // 32-byte Ed25519 verification key
const TOPICID_RE   = /^[0-9a-f]{66}$/;   // region byte + 32

/** Typed error for pre-hash / lifetime rejections. `.code` names the failure. */
export class SignedExpiryError extends Error {
  constructor(message, code) { super(message); this.name = 'SignedExpiryError'; this.code = code; }
}

/**
 * Validate the V2 identity widths BEFORE hashing (production profile).
 * publisher: exactly 64 lowercase hex, or JSON null (anonymous, local-only).
 * topicId:   exactly 66 lowercase hex.
 * Throws SignedExpiryError on any other form. This is the pre-hash gate the
 * golden/rejection vectors exercise.
 */
export function assertV2Widths(publisher, topicId) {
  if (publisher !== null && !(typeof publisher === 'string' && PUBLISHER_RE.test(publisher)))
    throw new SignedExpiryError('publisher must be exactly 64 lowercase hex or null', 'BAD_PUBLISHER_WIDTH');
  if (!(typeof topicId === 'string' && TOPICID_RE.test(topicId)))
    throw new SignedExpiryError('topicId must be exactly 66 lowercase hex', 'BAD_TOPICID_WIDTH');
}

/**
 * The exact V2 preimage string that will be hashed. Exposed so tests and
 * reviewers can pin it byte-for-byte. canonical() key-sorts to
 * d, exp, message, publisher, topicId.
 */
export function msgIdV2Preimage({ publisher, message, topicId, exp }) {
  return canonical({ d: MSGID_DOMAIN_V2, exp, message, publisher, topicId });
}

/**
 * Compute the V2 (Option-1) content address.
 * @param {{publisher: string|null, message: *, topicId: string, exp: number}} core
 * @param {{enforceWidths?: boolean}} [opts]  enforceWidths (default true) applies the
 *        production 64/66 gate; pass false only on the sim-relaxed keyspace path.
 * @returns {Promise<string>} 64-char hex sha256
 */
export function computeMsgIdV2({ publisher, message, topicId, exp }, { enforceWidths = true } = {}) {
  if (!Number.isInteger(exp)) throw new SignedExpiryError('exp must be an integer ms timestamp', 'BAD_EXP');
  if (enforceWidths) assertV2Widths(publisher, topicId);
  return sha256Hex(msgIdV2Preimage({ publisher, message, topicId, exp }));
}

/**
 * The legacy V1 content address, byte-preserved: NO `d` field,
 * sha256(canonical({publisher, message})). Retained so V1 records are never
 * re-hashed across the flag day and V1-vs-V2 can be proven distinct.
 */
export function computeMsgIdV1({ publisher = null, message }) {
  return sha256Hex(canonical({ publisher, message }));
}

// ---- committed lifetime (D1/D2) --------------------------------------------

/** The maximal exp a publisher may sign for a message minted at `ts`. */
export function clampExp(ts) { return ts + TTL_CEILING; }

/**
 * Validate a committed exp against its mint ts: ts < exp <= ts + TTL_CEILING.
 * A normal 24h message sets exp = ts + TTL_CEILING. Throws on out-of-range so
 * an ingress can fail closed; returns the exp on success.
 */
export function validateExp(ts, exp) {
  if (!Number.isInteger(ts) || !Number.isInteger(exp))
    throw new SignedExpiryError('ts and exp must be integer ms timestamps', 'BAD_EXP');
  if (!(exp > ts))            throw new SignedExpiryError('exp must be strictly after ts', 'EXP_NOT_AFTER_TS');
  if (exp > ts + TTL_CEILING) throw new SignedExpiryError('exp exceeds ts + TTL_CEILING', 'EXP_OVER_CEILING');
  return exp;
}

/** The one committed death used everywhere: effectiveDeath = exp + CLOCK_SKEW. */
export function effectiveDeath(exp) { return exp + CLOCK_SKEW; }

/** A body is fresh (deliverable / suppressible) iff now <= effectiveDeath(exp). */
export function isBodyFresh(now, exp) { return now <= effectiveDeath(exp); }
