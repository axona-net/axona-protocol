// smoke_tombstone_auth.mjs — REF-1.1 S2.0c-AUTH-B Phase 1: the bounded
// deletion-state core (src/pubsub/tombstoneAuth.js) in ISOLATION, driven
// directly. This is the pure state machine only; nothing is wired to the wire
// or AxonaManager yet. Assertions are mapped to the accepted Gate B
// implementation-test-plan classes reachable at the module level:
//   A commit-order/atomicity · C retry precondition re-checks · D committed-
//   expiry fail-closed · E capacity/saturation · K final-slot contention ·
//   L receive-path dominance · M crypto-prefilter + local-receipt retention ·
//   N retry scheduling/expiry. Wire classes (B, F, G, H, J) land with Phases 2/3.
//
// Faithful to the Gate-A saturation reference (23/23), extended with explicit
// K/L/M/N cases. Run: node test/smoke_tombstone_auth.mjs
import { randomBytes } from 'node:crypto';
import {
  TombstoneAuthority, CandidateStore, RELAY_CAPS, BROWSER_CAPS,
  claimRetention, authKey, TTL_CEILING, CLOCK_SKEW, FUTURE_TOLERANCE_MS,
  TOMBSTONE_RECORD_MAX,
} from '../src/pubsub/tombstoneAuth.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };

const hex = (b) => randomBytes(b).toString('hex');
const newTopicId = () => hex(33), newMsgId = () => hex(32), newSigner = () => hex(32);
const newSig = () => 'ed25519:' + hex(64);
const KILL_DOMAIN = 'axona:pubsub-kill:v1';
// A stand-in for the canonical signed-kill bytes a real kill carries. Phase 3
// substitutes the real serialized kill; the core only accounts its bytes.
const killBytesFor = (t, m, s, now) => JSON.stringify({ d: KILL_DOMAIN, topicId: t, msgId: m, ts: now - 1000, seq: 1, signature: newSig(), signerPubkey: s });

// A committed effectiveDeath a normal 24h message would carry.
const ed = (now) => now + TTL_CEILING + CLOCK_SKEW;
// Small test caps so limits bind without building 40k entries.
const caps = () => ({ tombMaxCount: 4, candMax: 4, recordMax: TOMBSTONE_RECORD_MAX, enabled: true });

console.log('REF-1.1 S2.0c-AUTH-B Phase 1 — bounded deletion-state core (pure, unwired)\n');

const now = 1e12;

// ---- A: commit-order / atomicity ------------------------------------------
{
  const A = new TombstoneAuthority(caps(), { bodyMax: 8 });
  for (let i = 0; i < 4; i++) { const t = newTopicId(), m = newMsgId(), s = newSigner(); A.onBody(t, m, s, ed(now), null, now); A.onKill(t, m, s, killBytesFor(t, m, s, now), now); }
  ok('A: four co-located author kills all SUPPRESS', A.fx.suppressions === 4);
  // atomicity: a co-located suppress removes the body AND leaves no candidate
  const t = newTopicId(), m = newMsgId(), s = newSigner();
  A.tomb.map.clear(); A.tomb.bytes = 0; A.tomb.perSigner.clear(); A.tomb.perTopic.clear(); A.tomb.minDeath = Infinity; // free slots for this probe
  A.onBody(t, m, s, ed(now), null, now);
  const r = A.onKill(t, m, s, killBytesFor(t, m, s, now), now);
  ok('A: suppress commits tombstone + removes body + no candidate (all-or-nothing)',
    r === 'SUPPRESSED' && A.tomb.has(authKey(t, m)) && !A.bodies.has(authKey(t, m)) && A.cand.get(authKey(t, m)).length === 0);
}

// ---- E: capacity / saturation ---------------------------------------------
{
  const E = new TombstoneAuthority(caps(), { bodyMax: 16 });
  for (let i = 0; i < 4; i++) { const t = newTopicId(), m = newMsgId(), s = newSigner(); E.onBody(t, m, s, ed(now), null, now); E.onKill(t, m, s, killBytesFor(t, m, s, now), now); }
  const fx0 = { ...E.fx };
  const t5 = newTopicId(), m5 = newMsgId(), s5 = newSigner(); E.onBody(t5, m5, s5, ed(now), null, now);
  const r5 = E.onKill(t5, m5, s5, killBytesFor(t5, m5, s5, now), now);
  ok('E: N+1 genuine kill at full tombstone store -> bounded pending, not suppressed',
    r5.startsWith('PENDING_CAPACITY') && r5.includes('ADMITTED'), r5);
  ok('E: refusal has NO side effects (no extra fanout/removal/purge/eviction)',
    E.fx.fanouts === fx0.fanouts && E.fx.cacheRemovals === fx0.cacheRemovals &&
    E.fx.candidatePurges === fx0.candidatePurges && E.fx.liveEvictions === fx0.liveEvictions);
  ok('E: a live tombstone is NEVER evicted to admit a newcomer', E.tomb.map.size === 4);
}

// ---- E/N: pending-capacity is itself bounded ------------------------------
{
  const B = new TombstoneAuthority({ tombMaxCount: 1, candMax: 2, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const occ = () => { const t = newTopicId(), m = newMsgId(), s = newSigner(); B.onBody(t, m, s, ed(now), null, now); return B.suppress(t, m, s, killBytesFor(t, m, s, now), ed(now), now); };
  occ();                                                     // fill the single tombstone slot
  let admit = 0, refused = 0;
  for (let i = 0; i < 3; i++) { const t = newTopicId(), m = newMsgId(), s = newSigner(); B.onBody(t, m, s, ed(now), null, now); const r = B.onKill(t, m, s, killBytesFor(t, m, s, now), now); if (r.includes('ADMITTED')) admit++; else if (r.includes('REFUSED_CAND_GLOBAL')) refused++; }
  ok('E: pending-capacity candidates are bounded (cand global cap refuses overflow)', admit === 2 && refused === 1, `admit=${admit} ref=${refused}`);
}

// ---- M: cryptographic prefilter shape + duplicate-kill dedup --------------
{
  const D = new TombstoneAuthority({ tombMaxCount: 0, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const t = newTopicId(), m = newMsgId(), s = newSigner();
  const d1 = D.onKill(t, m, s, killBytesFor(t, m, s, now), now);
  const d2 = D.onKill(t, m, s, killBytesFor(t, m, s, now), now);
  const d3 = D.onKill(t, m, s, killBytesFor(t, m, s, now), now);
  ok('M: duplicate KILL flood collapses to one candidate (dedup by signer)',
    d1 === 'ADMITTED' && d2 === 'DUP' && d3 === 'DUP' && D.cand.total === 1, `${d1}/${d2}/${d3} total=${D.cand.total}`);
}

// ---- M: ClaimRetention derives from LOCAL receipt, not attacker kill.ts ----
{
  ok('M: claimRetention = localReceipt + FUTURE_TOLERANCE + TTL_CEILING + CLOCK_SKEW (independent of kill.ts)',
    claimRetention(now) === now + FUTURE_TOLERANCE_MS + TTL_CEILING + CLOCK_SKEW);
  const R = new TombstoneAuthority({ tombMaxCount: 0, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const t = newTopicId(), m = newMsgId(), s = newSigner();
  // an attacker kill with a far-future ts must NOT extend retention
  R.onKill(t, m, s, killBytesFor(t, m, s, now + 10 * TTL_CEILING), now);
  const before = R.cand.total; R.cand.reclaimExpired(claimRetention(now) + 1);
  ok('M: candidate expires at LOCAL-receipt ClaimRetention regardless of kill.ts', before === 1 && R.cand.total === 0);
}

// ---- C: retry re-checks body present + author + committed deadline ---------
{
  // body-cache overflow demotes A's pending candidate, so retry finds no body
  const O = new TombstoneAuthority({ tombMaxCount: 0, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 2 });
  const A = [newTopicId(), newMsgId(), newSigner()], Bx = [newTopicId(), newMsgId(), newSigner()], C = [newTopicId(), newMsgId(), newSigner()];
  O.onBody(A[0], A[1], A[2], ed(now), null, now); O.onKill(A[0], A[1], A[2], killBytesFor(A[0], A[1], A[2], now), now);   // A pending (tomb full at 0)
  ok('C: A is pending after tomb-full refusal', O.cand.find(authKey(A[0], A[1]), A[2]).tag === 'pending');
  O.onBody(Bx[0], Bx[1], Bx[2], ed(now), null, now + 1); O.onBody(C[0], C[1], C[2], ed(now), null, now + 2);   // overflow bodyMax=2 evicts A's body
  ok('C: body-cache overflow demoted A (auto-eviction reported)', O.fx.demotions === 1 && O.cand.find(authKey(A[0], A[1]), A[2]).tag === 'plain', `dem=${O.fx.demotions}`);
  O.reclaimAndRetry(now + 3);
  ok('C: demoted-A is NOT suppressed on retry (body gone)', O.fx.suppressions === 0);

  // committed-death: a slot frees AFTER the candidate body's committed death -> reject
  const X = new TombstoneAuthority({ tombMaxCount: 1, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const tx = newTopicId(), mx = newMsgId(), sx = newSigner();
  X.suppress(tx, mx, sx, killBytesFor(tx, mx, sx, now), now + 1000, now);   // occupy slot, short death
  const ty = newTopicId(), my = newMsgId(), sy = newSigner(); const deathY = now + 500;
  X.onBody(ty, my, sy, deathY, null, now); X.onKill(ty, my, sy, killBytesFor(ty, my, sy, now), now);   // Y pending, committed death now+500
  X.reclaimAndRetry(now + 1001);                                            // slot frees at 1001 > Y's death 500
  ok('C: retry after committed effectiveDeath is rejected (arrival cannot extend authorization)',
    X.tomb.has(authKey(ty, my)) === false, `suppressions=${X.fx.suppressions}`);
}

// ---- D: SUPPRESS is fail-closed on the committed deadline (every path) ------
{
  const mk = () => new TombstoneAuthority({ tombMaxCount: 10, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  { const g = mk(), t = newTopicId(), m = newMsgId(), s = newSigner(), d = now + 1000; g.onBody(t, m, s, d, null, now);
    ok('D: direct KILL exactly AT effectiveDeath suppresses (boundary)', g.onKill(t, m, s, killBytesFor(t, m, s, now), d) === 'SUPPRESSED'); }
  { const g = mk(), t = newTopicId(), m = newMsgId(), s = newSigner(), d = now + 1000; g.onBody(t, m, s, d, null, now);
    ok('D: direct KILL at effectiveDeath+1 dropped, NO tombstone', g.onKill(t, m, s, killBytesFor(t, m, s, now), d + 1) === 'DROP_EXPIRED' && !g.tomb.has(authKey(t, m))); }
  { const g = mk(), t = newTopicId(), m = newMsgId(), s = newSigner(), d = now + 1000; g.onKill(t, m, s, killBytesFor(t, m, s, now), now);
    ok('D: late matching body past committed death does NOT suppress', g.onBody(t, m, s, d, killBytesFor(t, m, s, now), d + 1) === 'DROP_EXPIRED' && !g.tomb.has(authKey(t, m))); }
  { const g = mk(), t = newTopicId(), m = newMsgId(), s = newSigner();
    ok('D: suppress() directly rejects an already-expired authorization', g.suppress(t, m, s, killBytesFor(t, m, s, now), now - 1, now) === 'REFUSED_EXPIRED' && !g.tomb.has(authKey(t, m))); }
  { const g = mk(), t = newTopicId(), m = newMsgId(), s = newSigner();
    // forged non-author kill on a body-present node is dropped as author-mismatch
    g.onBody(t, m, s, ed(now), null, now);
    ok('D: non-author kill on a body-present node is dropped (never suppresses)', g.onKill(t, m, newSigner(), killBytesFor(t, m, newSigner(), now), now) === 'DROP_NONAUTHOR_BODYPRESENT'); }
}

// ---- L: receive-path dominance --------------------------------------------
{
  const L = new TombstoneAuthority({ tombMaxCount: 10, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const t = newTopicId(), m = newMsgId(), s = newSigner(), d = now + 1000;
  L.onBody(t, m, s, d, null, now); L.onKill(t, m, s, killBytesFor(t, m, s, now), now);
  ok('L: while the tombstone is live, the msgId is suppressed', L.isSuppressed(t, m, now) === true);
  ok('L: at effectiveDeath+1 the msgId is no longer suppressed', L.isSuppressed(t, m, d + 1) === false);
  // a replayed body while suppressed must not resurrect: onBody returns non-DELIVERED (suppress/expired)
  const rr = L.onBody(t, m, s, d, killBytesFor(t, m, s, now), now + 1);
  ok('L: a replayed body while suppressed is not DELIVERED', rr !== 'DELIVERED', rr);
}

// ---- K: concurrent contention for the final tombstone slot -----------------
{
  // Two body-verified kills race for the last free slot; exactly one commits,
  // the other becomes a bounded pending candidate. Cap/accounting equals the
  // limit exactly and never overshoots; the loser has no tombstone.
  const K = new TombstoneAuthority({ tombMaxCount: 1, candMax: 4, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const a = [newTopicId(), newMsgId(), newSigner()], b = [newTopicId(), newMsgId(), newSigner()];
  K.onBody(a[0], a[1], a[2], ed(now), null, now); K.onBody(b[0], b[1], b[2], ed(now), null, now);
  const ra = K.onKill(a[0], a[1], a[2], killBytesFor(a[0], a[1], a[2], now), now);
  const rb = K.onKill(b[0], b[1], b[2], killBytesFor(b[0], b[1], b[2], now), now);
  const committed = [ra, rb].filter(r => r === 'SUPPRESSED').length;
  const pending   = [ra, rb].filter(r => r.startsWith('PENDING_CAPACITY')).length;
  ok('K: exactly one of two racing kills commits; the other is bounded pending',
    committed === 1 && pending === 1 && K.tomb.map.size === 1, `${ra} | ${rb} tomb=${K.tomb.map.size}`);
  ok('K: the tombstone count equals its limit exactly (no overshoot)', K.tomb.map.size === K.tomb.maxCount);
}

// ---- N: retry scheduling serves oldest-body-first, no starvation -----------
{
  const N = new TombstoneAuthority({ tombMaxCount: 1, candMax: 8, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  // fill the single slot with a SHORT-lived tombstone so it reclaims soon
  const occ = [newTopicId(), newMsgId(), newSigner()];
  N.onBody(occ[0], occ[1], occ[2], now + 100, null, now); N.onKill(occ[0], occ[1], occ[2], killBytesFor(occ[0], occ[1], occ[2], now), now);
  // three body-verified kills defer as pending, with distinct body-arrival times
  const P = [];
  for (let i = 0; i < 3; i++) { const t = newTopicId(), m = newMsgId(), s = newSigner(); N.onBody(t, m, s, ed(now), null, now + (3 - i)); N.onKill(t, m, s, killBytesFor(t, m, s, now), now + (3 - i)); P.push([t, m, s]); }
  const order = N.cand.pendingOldestFirst().map(x => x.c.bodyArrivedAt);
  ok('N: pending retry order is oldest-body-first', order.length === 3 && order[0] <= order[1] && order[1] <= order[2], JSON.stringify(order));
  // free the slot (occ tombstone dead at 100) and retry: the oldest pending commits
  N.reclaimAndRetry(now + 200);
  ok('N: after reclamation a pending candidate is served (not starved)', N.fx.suppressions >= 2);
}

// ---- E-regression: byte / oversized / per-signer / per-topic on both stores -
{
  // tombstone byte cap
  { const g = new TombstoneAuthority({ tombMaxCount: 100, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
    g.tomb.maxBytes = 900;   // force a tight byte cap for the probe
    const t1 = newTopicId(), m1 = newMsgId(), s1 = newSigner(); g.onBody(t1, m1, s1, ed(now), null, now); const a = g.onKill(t1, m1, s1, killBytesFor(t1, m1, s1, now), now);
    const t2 = newTopicId(), m2 = newMsgId(), s2 = newSigner(); g.onBody(t2, m2, s2, ed(now), null, now); const b = g.onKill(t2, m2, s2, killBytesFor(t2, m2, s2, now), now);
    ok('E: tombstone byte-cap refuses the 2nd (no eviction)', a === 'SUPPRESSED' && b.includes('REFUSED_GLOBAL_BYTES'), `${a}/${b}`); }
  // oversized record refused (both stores)
  { const g = new TombstoneAuthority(RELAY_CAPS, { bodyMax: 10 });
    const t = newTopicId(), m = newMsgId(), s = newSigner();
    ok('E: tombstone oversized-record refused', g.suppress(t, m, s, 'x'.repeat(2000), ed(now), now) === 'REFUSED_RECORD_TOO_LARGE'); }
  { const c = new CandidateStore({ max: 10, maxBytes: 1 << 20, perSignerMax: 10, perTopicMax: 10, recordMax: TOMBSTONE_RECORD_MAX });
    const t = newTopicId(), m = newMsgId(), s = newSigner();
    const snap = () => JSON.stringify({ total: c.total, bytes: c.bytes });
    const b = snap(); const r = c.admit(authKey(t, m), s, 'x'.repeat(2000), now);
    ok('E: candidate oversized-record refused, accounting unchanged', r === 'REFUSED_RECORD_TOO_LARGE' && snap() === b, r); }
  // per-signer sublimit binds (tombstone)
  { const g = new TombstoneAuthority({ tombMaxCount: 100, candMax: 20, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 20 });
    g.tomb.perSignerMax = 2;
    const s = newSigner(); let okc = 0; for (let i = 0; i < 3; i++) { const t = newTopicId(), m = newMsgId(); g.onBody(t, m, s, ed(now), null, now); if (g.onKill(t, m, s, killBytesFor(t, m, s, now), now) === 'SUPPRESSED') okc++; }
    ok('E: tombstone per-signer sublimit binds (2 of 3)', okc === 2, `ok=${okc}`); }
  // per-topic sublimit binds (tombstone)
  { const g = new TombstoneAuthority({ tombMaxCount: 100, candMax: 20, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 20 });
    g.tomb.perTopicMax = 2;
    const tp = newTopicId(); let okc = 0; for (let i = 0; i < 3; i++) { const m = newMsgId(), s = newSigner(); g.onBody(tp, m, s, ed(now), null, now); if (g.onKill(tp, m, s, killBytesFor(tp, m, s, now), now) === 'SUPPRESSED') okc++; }
    ok('E: tombstone per-topic sublimit binds (2 of 3)', okc === 2, `ok=${okc}`); }
}

// ---- profile sanity: relay enabled + within record cap; browser disabled ---
{
  ok('profile: RELAY_CAPS enabled with the Gate-A caps 32768/8192', RELAY_CAPS.enabled === true && RELAY_CAPS.tombMaxCount === 32768 && RELAY_CAPS.candMax === 8192);
  ok('profile: BROWSER_CAPS 512/128 present but DISABLED (COI required to enable)', BROWSER_CAPS.enabled === false && BROWSER_CAPS.tombMaxCount === 512 && BROWSER_CAPS.candMax === 128);
  ok('profile: record cap is the Gate-A 768 B', TOMBSTONE_RECORD_MAX === 768);
}

// ---- Aster Phase-1 review fixes (council msgId 1b2b3715) -------------------
// FIX 1: profile recordMax is honored by BOTH stores (was silently dropped).
{
  const P = { tombMaxCount: 10, candMax: 10, recordMax: 100, enabled: true };
  const A = new TombstoneAuthority(P, { bodyMax: 10 });
  ok('FIX1: tombstone store retained recordMax from the profile', A.tomb.recordMax === 100);
  ok('FIX1: candidate store retained recordMax from the profile', A.cand.recordMax === 100);
  const t = newTopicId(), m = newMsgId(), s = newSigner(); A.onBody(t, m, s, ed(now), null, now);
  const r = A.onKill(t, m, s, killBytesFor(t, m, s, now), now);   // ~200 B record > 100 cap
  ok('FIX1: tombstone store refuses an oversized record under the small profile cap', r.includes('REFUSED_RECORD_TOO_LARGE'), r);
  const t2 = newTopicId(), m2 = newMsgId(), s2 = newSigner();
  ok('FIX1: candidate store refuses an oversized record under the small profile cap', A.onKill(t2, m2, s2, killBytesFor(t2, m2, s2, now), now) === 'REFUSED_RECORD_TOO_LARGE');
}

// FIX 2: suppress() handles an existing tombstone first — idempotent confirm, mismatch fail-closed.
{
  const A = new TombstoneAuthority({ tombMaxCount: 10, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const t = newTopicId(), m = newMsgId(), s = newSigner(); const kb = killBytesFor(t, m, s, now);
  const snap = () => JSON.stringify({ size: A.tomb.map.size, bytes: A.tomb.bytes, sig: A.tomb.perSigner.get(s), top: A.tomb.perTopic.get(t), fan: A.fx.fanouts, sup: A.fx.suppressions });
  const r1 = A.suppress(t, m, s, kb, ed(now), now); const after1 = snap();
  const r2 = A.suppress(t, m, s, kb, ed(now), now);   // identical -> CONFIRMED, zero side effects
  ok('FIX2: repeated identical suppress is CONFIRMED with zero side effects (no double-count)', r1 === 'SUPPRESSED' && r2 === 'CONFIRMED' && snap() === after1, `${r1}/${r2}`);
  const r3 = A.suppress(t, m, newSigner(), kb, ed(now), now);   // different signer -> fail closed, no overwrite
  ok('FIX2: a different signer cannot overwrite the authoritative tombstone (fail closed)', r3 === 'REFUSED_MISMATCH_TOMB' && A.tomb.get(authKey(t, m)).signerPubkey === s && snap() === after1, r3);
}

// FIX 3: onKill() reclaims / deadline-checks before confirming an expired tombstone.
{
  const A = new TombstoneAuthority({ tombMaxCount: 10, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const t = newTopicId(), m = newMsgId(), s = newSigner(); const d = now + 1000;
  A.suppress(t, m, s, killBytesFor(t, m, s, now), d, now);
  const r = A.onKill(t, m, s, killBytesFor(t, m, s, now), d + 1);   // AFTER death, before any scheduled sweep
  ok('FIX3: onKill at effectiveDeath+1 does NOT confirm and reclaims the expired tombstone', r !== 'CONFIRMED' && A.tomb.has(authKey(t, m)) === false, r);
}

// FIX 4: onBody() checks the committed deadline BEFORE caching; expired body leaves state unchanged.
{
  const A = new TombstoneAuthority({ tombMaxCount: 10, candMax: 10, recordMax: TOMBSTONE_RECORD_MAX, enabled: true }, { bodyMax: 10 });
  const t = newTopicId(), m = newMsgId(), s = newSigner();
  A.onKill(t, m, s, killBytesFor(t, m, s, now), now);   // body-absent candidate present
  const candBefore = A.cand.total, tombBefore = A.tomb.map.size;
  const r = A.onBody(t, m, s, now + 500, killBytesFor(t, m, s, now), now + 501);   // body past its committed death
  ok('FIX4: expired body is DROP_EXPIRED and NOT cached (no false co-location basis)', r === 'DROP_EXPIRED' && A.bodies.has(authKey(t, m)) === false, r);
  ok('FIX4: expired-body path leaves tombstone + candidate accounting unchanged (candidate retained)', A.tomb.map.size === tombBefore && A.cand.total === candBefore, `tomb=${A.tomb.map.size} cand=${A.cand.total}`);
}

console.log(`\nRESULT: ${n} checks passed, ${fail} failed.`);
process.exit(fail ? 1 : 0);
