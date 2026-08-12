// =====================================================================
// tombstoneAuthWiring.js — REF-1.1 S2.0c Phase 3: SHADOW-MODE wiring of the
// accepted tombstoneAuth core (src/pubsub/tombstoneAuth.js) into AxonaManager.
//
// STATUS: DEFAULT-OFF, OBSERVE-ONLY. When the `tombstoneAuth` construction flag
// is set, AxonaManager builds ONE per-node TombstoneAuthority and this module
// feeds it the LOCALLY-VERIFIED body / kill stream, plus cache-eviction and
// role-teardown events, so its parallel deletion-state tracks what this node has
// actually verified and still holds. It NEVER mutates role state, the cache, the
// tombstones, the fanout, or app delivery — the legacy path stays the sole
// source of truth. Flag-OFF (default): the authority is null and every hook is a
// guarded no-op, byte-identical to today.
//
// AUTHORITY IS EARNED FROM LOCAL VERIFICATION (Aster Phase-3 review ec7a5a38).
// The accepted invariant is that a tombstone is authoritative ONLY from a
// locally B-4-verified body plus a locally verified signed kill; migrated/fanned
// markers are non-authoritative. So the observers are driven ONLY from the
// verified ingress points and are handed VERIFIED material:
//   - body  ← _ingestPublish / _ingestStamped, AFTER verifyEnvelope() passed,
//             and only if the entry SURVIVED the cache write (see the survived
//             guard at the call sites). The verified envelope's signerPubkey is
//             the publisher; we never parse an unverified JSON body for authority.
//   - kill  ← _onKill, AFTER verifyKill() passed, handed the signed kill object
//             and its verified signerPubkey; the kill's topicId is bound to this
//             topic before it can reach onKill(). The del-fanout / migration
//             paths (which carry only an unsigned marker) are DELIBERATELY not
//             observed — they are exactly the non-authoritative case.
// Anything without local proof never becomes a candidate or a tombstone.
//
// CACHE FIDELITY (Aster Phase-3 review, class 2). The shadow body mirror must not
// retain a body the live cache no longer holds, or it would be a false
// co-location basis. So: body observation reflects the FINAL cache outcome
// (survived-guard); TTL and byte-cap evictions call _taObserveEvict; a role
// teardown purges that topic's shadow bodies (_taPurgeTopic); and resetState()
// rebuilds the authority (_taReset).
//
// INTERIM DEADLINE (marked, not hidden): the committed effectiveDeath is meant to
// come from the body's SIGNED exp. V1 envelopes carry none yet (that lands at the
// envelope V2 flag day), so the shadow derives an INTERIM, UNSIGNED death from
// the wire publishTs: publishTs + TTL_CEILING + CLOCK_SKEW. This exercises the
// store/expiry/capacity machinery WITHOUT the cold-verifiable immutability the V2
// identity provides. Replaced by the signed exp at one site when V2 ships.
//
// ENFORCEMENT (making the authority the suppression source of truth, pre-gate
// feeding, closing the del-fanout/migration trust gaps) is a SEPARATE later gate
// that also needs the signed exp, so it pairs with the V2 cutover.
//
// SAFETY: every observer is gated on `this._tombAuthority` and wrapped so it can
// never throw into the hot path — an internal error increments a counter and is
// swallowed, because a SHADOW must never be able to affect the live pipeline.
// =====================================================================

import { TombstoneAuthority, RELAY_CAPS, TTL_CEILING, CLOCK_SKEW } from './tombstoneAuth.js';
import { canonical } from './post.js';
import { KILL_DOMAIN } from './kill.js';
import { idHex, idBig, lc } from './ids.js';

// Build the per-node authority + observation counters. Called from the
// AxonaManager constructor ONLY when the tombstoneAuth flag is set.
export function makeTombstoneAuthority(profile = RELAY_CAPS) {
  return {
    authority: new TombstoneAuthority(profile),
    stats: { bodies: 0, kills: 0, evicts: 0, reclaims: 0, purges: 0, resets: 0, errors: 0, skipped: 0, verdicts: {} },
  };
}

const bump = (m, k) => { m[k] = (m[k] || 0) + 1; };

export const tombstoneAuthWiringMethods = {
  // Interim, UNSIGNED committed death derived from the wire publishTs (see header).
  _taDeath(publishTs) { return publishTs + TTL_CEILING + CLOCK_SKEW; },

  // A LOCALLY-VERIFIED body just entered this node's cache and SURVIVED the write
  // (callers pass the verifyEnvelope()-verified `env` and gate on cache survival).
  // publisher = the verified envelope's signerPubkey (or null for anonymous). We
  // never derive authority by parsing an unverified JSON body.
  _taObserveBody(topicBig, env, publishTs) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      if (!env || typeof env.msgId !== 'string') { ta.stats.skipped++; return; }
      const publisher = env.signerPubkey ? lc(env.signerPubkey) : null;
      const topicId = idHex(topicBig);
      const v = ta.authority.onBody(topicId, env.msgId, publisher, this._taDeath(publishTs), null, this._now());
      ta.stats.bodies++; bump(ta.stats.verdicts, 'body:' + String(v).split(':')[0]);
    } catch { ta.stats.errors++; }
  },

  // A LOCALLY-VERIFIED signed kill (caller ran verifyKill() and passes its
  // signerPubkey). The signed kill's topicId is bound to THIS topic before it can
  // reach onKill(): a missing signer, a non-string msgId, or a topicId that does
  // not resolve to this topic is skipped — never a candidate, never a tombstone.
  _taObserveKill(topicBig, kill, signerPubkey) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      if (!kill || !signerPubkey || typeof kill.msgId !== 'string') { ta.stats.skipped++; return; }
      let bound = false;
      try { bound = kill.topicId != null && idBig(kill.topicId) === topicBig; } catch { bound = false; }
      if (!bound) { ta.stats.skipped++; return; }          // mismatched/absent topic binding
      const topicId = idHex(topicBig);
      const signer  = lc(signerPubkey);
      // Faithful byte-accounting: size the record from the signed kill's canonical core.
      const killBytes = canonical({ d: KILL_DOMAIN, topicId: kill.topicId, msgId: kill.msgId, ts: kill.ts, seq: kill.seq });
      const v = ta.authority.onKill(topicId, kill.msgId, signer, killBytes, this._now());
      ta.stats.kills++; bump(ta.stats.verdicts, 'kill:' + String(v).split(':')[0]);
    } catch { ta.stats.errors++; }
  },

  // A cache entry aged out / was byte-capped (the _cachePush + _expireCache
  // funnels). Keep the shadow body mirror in step with the live cache.
  _taObserveEvict(role, msgId) {
    const ta = this._tombAuthority; if (!ta || !role) return;
    try { ta.authority.evictBody(idHex(role.topicId), msgId); ta.stats.evicts++; } catch { ta.stats.errors++; }
  },

  // Periodic reclamation + deferred-candidate retry (driven off the expiry tick).
  _taReclaim() {
    const ta = this._tombAuthority; if (!ta) return;
    try { ta.authority.reclaimAndRetry(this._now()); ta.stats.reclaims++; } catch { ta.stats.errors++; }
  },

  // A role was torn down (empty-role teardown / graceful leave): the node no
  // longer holds this topic's bodies, so purge their shadow co-location basis.
  _taPurgeTopic(topicBig) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      const prefix = idHex(topicBig) + '|';
      for (const k of [...ta.authority.bodies.map.keys()]) if (k.startsWith(prefix)) ta.authority.bodies.evict(k);
      ta.stats.purges++;
    } catch { ta.stats.errors++; }
  },

  // resetState(): the node dropped ALL roles — rebuild the shadow authority so no
  // stale body/candidate/tombstone survives to seed a later false co-location.
  // Cumulative observation counters carry over (they are a lifetime tally).
  _taReset() {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      const fresh = makeTombstoneAuthority(ta.authority.profile);
      fresh.stats = ta.stats; fresh.stats.resets++;
      this._tombAuthority = fresh;
    } catch { ta.stats.errors++; }
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
