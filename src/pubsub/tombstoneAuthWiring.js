// =====================================================================
// tombstoneAuthWiring.js — REF-1.1 S2.0c Phase 3: SHADOW-MODE wiring of the
// accepted tombstoneAuth core (src/pubsub/tombstoneAuth.js) into AxonaManager's
// kill / body / expiry funnels.
//
// STATUS: DEFAULT-OFF, OBSERVE-ONLY. When the `tombstoneAuth` construction flag
// is set, AxonaManager builds ONE per-node TombstoneAuthority and this module
// feeds it the real inbound body / kill / evict event stream at the existing
// single funnels (_cachePush, _applyKill, _expireCache). It NEVER mutates role
// state, the cache, the tombstones, the fanout, or app delivery — the legacy
// path stays the sole source of truth. Flag-OFF, none of this runs and behavior
// is byte-identical (the observer object is null; every hook is a guarded no-op).
//
// This is the same shadow-then-cutover shape S1 used (flag-on OBSERVES and
// records, verdicts unchanged). The ENFORCEMENT cutover — making the authority
// the suppression source of truth and closing the del-fanout / migration
// trust-the-upstream-signer gaps the recon flagged — is a SEPARATE later gate.
// It also needs the SIGNED `exp` from the envelope V2 flag day, so it pairs with
// that cutover, not this tranche.
//
// INTERIM DEADLINE (marked, not hidden): the committed `effectiveDeath` the
// authority reasons against is meant to come from the body's SIGNED `exp`. V1
// envelopes carry no signed exp yet (that lands at the envelope flag day), so the
// shadow derives an INTERIM, UNSIGNED death from the wire publishTs:
//   effectiveDeath = publishTs + TTL_CEILING + CLOCK_SKEW.
// This exercises the store / expiry / capacity machinery under live timing
// WITHOUT claiming the cold-verifiable immutability the V2 identity provides.
// When V2 ships, this derivation is replaced by the signed exp at one site.
//
// SAFETY: every observer is gated on `this._tombAuthority` and wrapped so it can
// never throw into the hot path — an internal error increments a counter and is
// swallowed, because a SHADOW must never be able to affect the live pipeline.
// =====================================================================

import { TombstoneAuthority, RELAY_CAPS, TTL_CEILING, CLOCK_SKEW } from './tombstoneAuth.js';
import { idHex, lc } from './ids.js';

// Build the per-node authority + observation counters. Called from the
// AxonaManager constructor ONLY when the tombstoneAuth flag is set.
export function makeTombstoneAuthority(profile = RELAY_CAPS) {
  return {
    authority: new TombstoneAuthority(profile),
    stats: { bodies: 0, kills: 0, evicts: 0, reclaims: 0, errors: 0, verdicts: {} },
  };
}

const bump = (m, k) => { m[k] = (m[k] || 0) + 1; };

export const tombstoneAuthWiringMethods = {
  // Interim, UNSIGNED committed death derived from the wire publishTs (see header).
  _taDeath(publishTs) { return publishTs + TTL_CEILING + CLOCK_SKEW; },

  // A body just entered this node's cache (the single _cachePush funnel). Feed
  // the shadow authority; never touch role state. publisher = the envelope's
  // signerPubkey (or null for anonymous). Observe-only; never throws.
  _taObserveBody(role, entry) {
    const ta = this._tombAuthority; if (!ta || !role) return;
    try {
      let publisher = null;
      try { publisher = JSON.parse(entry.json)?.signerPubkey ?? null; } catch { /* anonymous / opaque */ }
      if (publisher) publisher = lc(publisher);
      const topicId = idHex(role.topicId);
      const v = ta.authority.onBody(topicId, entry.msgId, publisher, this._taDeath(entry.publishTs), null, this._now());
      ta.stats.bodies++; bump(ta.stats.verdicts, 'body:' + String(v).split(':')[0]);
    } catch { ta.stats.errors++; }
  },

  // A tombstone install just happened (the single _applyKill funnel). Feed the
  // shadow authority the signed kill; the authority makes its OWN co-located
  // authorization decision (candidate vs authoritative) in parallel — where it
  // holds a migrated/fanned kill as a non-authoritative candidate while the
  // legacy path installs an authoritative tombstone is exactly the divergence
  // the enforcement cutover will close.
  _taObserveKill(role, topicBig, m) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      const signer = m.signer ? lc(m.signer) : null;
      const topicId = idHex(topicBig);
      // Byte-accounting proxy for the wire kill marker: the shadow does not carry
      // the full signed kill envelope at this funnel (enforcement will), so size
      // the record from the marker fields the authority keys and bounds on.
      const killBytes = JSON.stringify({ msgId: m.msgId, killTs: m.killTs, signer, seq: m.seq });
      const v = ta.authority.onKill(topicId, m.msgId, signer, killBytes, this._now());
      ta.stats.kills++; bump(ta.stats.verdicts, 'kill:' + String(v).split(':')[0]);
    } catch { ta.stats.errors++; }
  },

  // A cache entry aged out (the single _expireCache funnel). Demote the key's
  // pending candidate in the shadow so its co-location basis tracks the cache.
  _taObserveEvict(role, msgId) {
    const ta = this._tombAuthority; if (!ta || !role) return;
    try { ta.authority.evictBody(idHex(role.topicId), msgId); ta.stats.evicts++; } catch { ta.stats.errors++; }
  },

  // Periodic reclamation + deferred-candidate retry (driven off the expiry tick).
  _taReclaim() {
    const ta = this._tombAuthority; if (!ta) return;
    try { ta.authority.reclaimAndRetry(this._now()); ta.stats.reclaims++; } catch { ta.stats.errors++; }
  },

  // Inspectable observation surface (tests + future telemetry). Reading it never
  // affects behavior. Flag-off returns { enabled:false }.
  tombstoneAuthShadow() {
    const ta = this._tombAuthority; if (!ta) return { enabled: false };
    const a = ta.authority;
    return {
      enabled: true,
      profile:  { tombMaxCount: a.tomb.maxCount, candMax: a.cand.max },
      stats:    { ...ta.stats, verdicts: { ...ta.stats.verdicts } },
      fx:       { ...a.fx },
      sizes:    { tombstones: a.tomb.map.size, candidates: a.cand.total, bodies: a.bodies.map.size },
    };
  },
};

export default tombstoneAuthWiringMethods;
