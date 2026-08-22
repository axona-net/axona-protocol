// =====================================================================
// smoke_synaptome_delay_failure_matrix.mjs — the council-agreed delay/failure
// matrix for hold-or-improve + the candidate attempt guard.
//
// Definition doc: axona-docs architecture/Axona-Connection-Quality-Hold-or-
// Improve-v0.1.md (axona-docs 1811fc9). Council record: findings concurrence
// Aster 1bc728b3 / Orion df536c99 / Vega 8b99b40b on synthesis 25fe09c4.
//
// Every scenario asserts BOTH halves of the guarantee — the BOUND and the
// RECOVERY (Aster a1afc0a6: "a strict attempt bound and eventual successful
// recovery"). A run that shows one without the other fails.
//
//   1. slow-then-bind      : attempt cap holds        / edge forms once bindable
//   2. fail-then-bind      : backoff held through fail / edge forms after failures stop
//   2b. expire-then-late-bind : exclusion holds, stale record resets nothing /
//       fresh authenticated (monotonic) record restores eligibility, edge binds
//       (closure condition (a); Aster ac99fe7f, Orion 53e6daa4 scenario spec)
//   3. permanent-fail      : attempts expire and stop  / budget redirects elsewhere
//   4. newcomer vs saturated mesh : lane bounded+rate-limited / newcomer reaches degree
//   5. correlated inbound eviction: acceptor growth bounded   / victim recovers to floor
//   6. empty-stratum pressure     : deficit backoff engages   / resumes on fresh record
//   7. seed-path gate (closure (e)): pins today's gap (over-cap insert via
//      _seedSynaptomeWithSponsor) and models the compare-and-swap gate AT that
//      entrypoint — occupancy pinned at cap, admits paired with evictions
//   8. both doors open (finding 3): join + emergency refill concurrently under a
//      shared per-node in-flight dial cap C_dial — cap holds, both complete
//   9. event-driven arm (Vega alternative): no timer search — drop-driven
//      deficits heal from a remembered-peers cache; cold deficits measured as cost
//
// The guard and backoff are REFERENCE MODELS in this harness — the kernel is
// NOT modified. The matrix measures the real kernel's maintenance loop
// (_maintainSynaptome → _considerCandidate → openConnection) dialing through
// them, plus a modeled hold-or-improve acceptance gate on the sim transport's
// bilateral-admission hook (_acceptConnection). Constants below are the
// matrix's job to exercise, not fixed policy (Vega 8b99b40b).
//
// Run: node test/smoke_synaptome_delay_failure_matrix.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';
import { clz264 }                   from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));
const xcmp  = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const now   = () => Date.now();

// ── Reference model: candidate-level attempt guard ────────────────────
// In-flight dedup + bounded retry with exponential backoff + expiry on bind
// or exhaustion (Aster a1afc0a6 / Orion df536c99 item 2).
class AttemptGuard {
  constructor({ maxAttempts = 4, baseMs = 60, factor = 2 } = {}) {
    this.maxAttempts = maxAttempts; this.baseMs = baseMs; this.factor = factor;
    this.state = new Map();   // key -> {attempts, inflight, nextAt, expired, times[]}
  }
  _s(key) { let s = this.state.get(key); if (!s) { s = { attempts: 0, inflight: false, nextAt: 0, expired: false, times: [] }; this.state.set(key, s); } return s; }
  allow(key, t = now()) {
    const s = this._s(key);
    return !s.expired && !s.inflight && t >= s.nextAt;
  }
  begin(key, t = now()) { const s = this._s(key); s.inflight = true; s.times.push(t); }
  end(key, bound, t = now()) {
    const s = this._s(key); s.inflight = false;
    if (bound) { this.state.delete(key); return; }          // expiry on bind
    s.attempts++;
    if (s.attempts >= this.maxAttempts) { s.expired = true; return; }  // expiry on exhaustion
    s.nextAt = t + this.baseMs * Math.pow(this.factor, s.attempts - 1);
  }
  attemptsOf(key) { return this.state.get(key)?.times.length ?? 0; }
  timesOf(key)    { return this.state.get(key)?.times ?? []; }
  expiredOf(key)  { return this.state.get(key)?.expired ?? false; }
  // Candidate-level reset (closure condition (a)): an exhausted candidate
  // becomes eligible again ONLY on a fresh routing record with a monotonically
  // higher generation than any seen for that candidate. A stale or duplicate
  // record resets nothing and refills no attempt budget. `gens` survives state
  // resets on purpose — the monotonic watermark is what defeats replay.
  onFreshRecord(key, generation) {
    if (!this.gens) this.gens = new Map();
    const seen = this.gens.get(key) ?? -Infinity;
    if (!(generation > seen)) return false;                 // stale/duplicate: no refill
    this.gens.set(key, generation);
    this.state.delete(key);                                 // one fresh budget, re-eligible
    return true;
  }
}

// ── Reference model: deficit-level backoff ────────────────────────────
// A stratum whose searches keep returning nothing gets exponentially rarer
// searches; a fresh routing record for the prefix resets it (finding 5).
class DeficitBackoff {
  constructor({ baseMs = 40, factor = 2 } = {}) { this.baseMs = baseMs; this.factor = factor; this.state = new Map(); }
  _s(k) { let s = this.state.get(k); if (!s) { s = { empties: 0, nextAt: 0 }; this.state.set(k, s); } return s; }
  allow(k, t = now()) { return t >= this._s(k).nextAt; }
  onEmpty(k, t = now()) { const s = this._s(k); s.empties++; s.nextAt = t + this.baseMs * Math.pow(this.factor, s.empties - 1); }
  onRecord(k) { this.state.delete(k); }                     // fresh routing record for the prefix
}

// ── Mesh scaffold (same construction as the storm test) ───────────────
async function makePeer(net, domain, lat, lng, maintain = null) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, synaptomeMaintain: maintain });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}

async function scenario(maintain, N = 12) {
  const net = new SimNetwork(); const domain = new AxonaDomain();
  const base = [];
  for (let i = 0; i < N; i++) base.push(await makePeer(net, domain, (i*17)%80-40, (i*53)%360-180, null));
  for (let i = 0; i < base.length; i++)
    for (let j = i + 1; j < base.length; j++)
      await base[i].transport.openConnection(base[j].id.id);
  await wait(20);
  const KNEAR = 5;
  const t = await makePeer(net, domain, 5, 5, maintain && { kNear: KNEAR, intervalMs: 999999, maxPerTick: 3 });
  await t.transport.openConnection(base[0].id.id);          // sponsor only
  await wait(10);
  const nearest = base.map(b => b.big).sort((a, b) => xcmp(t.big ^ a, t.big ^ b)).slice(0, KNEAR);
  const recOf = (big) => base.find(b => b.big === big);
  return { net, domain, base, t, KNEAR, nearest, recOf };
}

// Route the test node's openConnection probes through an AttemptGuard.
function guardDials(t, guard) {
  const orig = t.transport.openConnection.bind(t.transport);
  t.transport.openConnection = async (peerId, ...a) => {
    const key = typeof peerId === 'bigint' ? toHex(peerId) : String(peerId);
    if (!guard.allow(key)) return false;                    // dedup / backoff / expiry
    guard.begin(key);
    let ok = false;
    try { ok = await orig(peerId, ...a); } finally { guard.end(key, !!ok); }
    return ok;
  };
  return orig;
}

// Tear one edge as node `t` sees it (channel gone + onPeerDied), like a
// heartbeat-timeout drop. Same shape as the storm test's realDrop.
function realDrop(t, victimHex) {
  const nh = t.transport._normPeerId ? t.transport._normPeerId(victimHex) : victimHex;
  t.transport._openTo.delete(nh);
  t.transport._latency.delete(nh);
  const hb = t.transport._heartbeats?.get(nh);
  if (hb) { clearInterval(hb); t.transport._heartbeats.delete(nh); }
  t.transport._fireDied(nh);
}

async function main() {
  console.log('Axona delay/failure matrix — hold-or-improve + attempt guard (real kernel, guard as reference model)\n');

  // ════ Scenarios 1–3: one controllable near-successor, guard engaged ════
  // The candidate's bind behavior is controlled through the sim transport's
  // bilateral-admission hook: refuse → openConnection false (the never-bind
  // condition c16d12b reproduced); allow → binds and admits via onPeerBound.

  // ── 1. slow-then-bind ──────────────────────────────────────────────
  {
    const { t, nearest, recOf } = await scenario(true);
    const cand = recOf(nearest[0]); const key = cand.hex;
    let bindable = false;
    cand.transport._acceptConnection = () => bindable;      // slow negotiation: refuses at first
    const guard = new AttemptGuard({ maxAttempts: 4, baseMs: 60, factor: 2 });
    guardDials(t, guard);
    setTimeout(() => { bindable = true; }, 150);            // becomes bindable inside the backoff window
    for (let k = 0; k < 24; k++) { await t.peer._maintainSynaptome(); await wait(30); }
    const attempts = guard.attemptsOf(key);
    const bound = t.node.synaptome.has(cand.big);
    // guard.state clears on bind, so attemptsOf is 0 after success; count via bound + cap check
    check('1 BOUND: attempts to slow candidate never exceed cap', attempts <= 4, `(tracked=${attempts})`);
    check('1 RECOVERY: edge forms once candidate binds', bound);
  }

  // ── 2. fail-then-bind ──────────────────────────────────────────────
  {
    const { t, nearest, recOf } = await scenario(true);
    const cand = recOf(nearest[0]); const key = cand.hex;
    let fails = 0, bindable = false;
    cand.transport._acceptConnection = () => { if (!bindable) { fails++; return false; } return true; };
    const guard = new AttemptGuard({ maxAttempts: 8, baseMs: 50, factor: 2 });
    guardDials(t, guard);
    // run through exactly 3 failed attempts (condition-driven, not tick-counted —
    // a fixed tick count let expiry race the bind flip and flake the recovery),
    // capture spacing, then let it bind and drive until the edge forms.
    const t0 = now();
    while (guard.attemptsOf(key) < 3 && now() - t0 < 1500) { await t.peer._maintainSynaptome(); await wait(25); }
    const times = [...guard.timesOf(key)];
    bindable = true;
    const t1 = now();
    while (!t.node.synaptome.has(cand.big) && now() - t1 < 2500) { await t.peer._maintainSynaptome(); await wait(30); }
    const gaps = times.slice(1).map((x, i) => x - times[i]);
    const monotone = gaps.every((g, i) => i === 0 || g >= gaps[i - 1] - 15);   // 15ms timer slack
    check('2 BOUND: backoff spacing non-decreasing through failures', times.length >= 2 && monotone, `(gaps=${gaps.map(g=>g|0).join(',')})`);
    check('2 BOUND: failed attempts stayed under cap', fails <= 8, `(fails=${fails})`);
    check('2 RECOVERY: edge forms after failures stop', t.node.synaptome.has(cand.big));
  }

  // ── 2b. expire-then-late-bind (closure condition (a)) ──────────────
  // The candidate exhausts its attempts FIRST, then becomes bindable. Without
  // a reset it is excluded forever — Orion 53e6daa4's scenario spec verbatim:
  // fail-to-expiry → bindable → still excluded → fresh authenticated record →
  // guard resets → edge binds cleanly. Stale/duplicate records reset nothing.
  {
    const { t, nearest, recOf } = await scenario(true);
    const cand = recOf(nearest[0]); const key = cand.hex;
    let bindable = false;
    cand.transport._acceptConnection = () => bindable;
    const guard = new AttemptGuard({ maxAttempts: 3, baseMs: 30, factor: 2 });
    guardDials(t, guard);
    const t0 = now();
    while (!guard.expiredOf(key) && now() - t0 < 1500) { await t.peer._maintainSynaptome(); await wait(25); }
    check('2b SETUP: candidate exhausted to expiry', guard.expiredOf(key));
    bindable = true;                                        // comes up cleanly — too late
    const attemptsAtExpiry = guard.attemptsOf(key);
    for (let k = 0; k < 8; k++) { await t.peer._maintainSynaptome(); await wait(25); }
    check('2b BOUND: exclusion holds after late bind — no attempts, no edge',
      guard.attemptsOf(key) === attemptsAtExpiry && !t.node.synaptome.has(cand.big),
      `(attempts=${guard.attemptsOf(key)} vs ${attemptsAtExpiry})`);
    // A fresh record arrives (generation 7). A replay of the SAME generation
    // afterwards must not grant a second budget.
    const freshOk = guard.onFreshRecord(key, 7);
    const t1 = now();
    while (!t.node.synaptome.has(cand.big) && now() - t1 < 1500) { await t.peer._maintainSynaptome(); await wait(25); }
    check('2b RECOVERY: fresh record restores eligibility; edge binds', freshOk && t.node.synaptome.has(cand.big));
    check('2b BOUND: replayed record (same generation) resets nothing', guard.onFreshRecord(key, 7) === false);
    // Guard-model monotonicity, standalone: exhaust → reset on 5 → exhaust →
    // duplicate 5 rejected → 6 accepted.
    const g = new AttemptGuard({ maxAttempts: 2, baseMs: 1 });
    const burn = () => { for (let i = 0; i < 2; i++) { g.begin('u', now()); g.end('u', false, now()); } };
    burn();
    const r1 = g.onFreshRecord('u', 5); burn();
    const r2 = g.onFreshRecord('u', 5);                     // duplicate: stale
    const stillExpired = g.expiredOf('u');                  // capture BEFORE r3 resets state
    const r3 = g.onFreshRecord('u', 6);                     // fresh again
    check('2b BOUND: generation watermark is monotonic across resets', r1 === true && r2 === false && stillExpired && r3 === true, `(r1=${r1} r2=${r2} expired=${stillExpired} r3=${r3})`);
  }

  // ── 3. permanent-fail ──────────────────────────────────────────────
  {
    const { t, nearest, recOf } = await scenario(true);
    const cand = recOf(nearest[0]);
    cand.transport._acceptConnection = () => false;         // never binds
    const guard = new AttemptGuard({ maxAttempts: 3, baseMs: 40, factor: 2 });
    guardDials(t, guard);
    for (let k = 0; k < 20; k++) { await t.peer._maintainSynaptome(); await wait(25); }
    const attempts = guard.attemptsOf(cand.hex);
    // Budget redirected: the OTHER near-successors still get bound.
    const others = nearest.slice(1).filter(id => t.node.synaptome.has(id)).length;
    check('3 BOUND: permanent-fail candidate expires and attempts stop', guard.expiredOf(cand.hex) && attempts === 3, `(attempts=${attempts}, expired=${guard.expiredOf(cand.hex)})`);
    check('3 RECOVERY: budget redirects — other successors still bound', others >= 3, `(${others}/${nearest.length - 1})`);
    // Contrast, unguarded (c16d12b): 3.0 probes/tick sustained — 20 ticks would be ~60.
    check('3 CONTRAST: guarded total is far under the unguarded storm rate', attempts <= 3, `(3 vs ~60 unguarded over 20 ticks)`);
  }

  // ════ Scenario 4: newcomer joins a saturated mesh ════
  // Hold-or-improve acceptance modeled on every base peer via the bilateral-
  // admission hook: at cap, refuse (improve-gates shut — the stranding
  // precondition of finding 2). The join lane admits a bounded, rate-limited
  // number of probationers outside the cap.
  {
    const { net, domain, base } = await scenario(false);
    // Per-peer cap snapshot: every peer is saturated at ITS OWN built size
    // (base[0] carries one extra edge — the test sponsor — so a single global
    // cap would leave the other eleven below threshold and admitting freely).
    const capOf = new Map(base.map(b => [b.hex, b.node.synaptome.size]));
    const lane = new Map();                                 // hex -> {slots, lastAt}
    const LANE_SLOTS = 2, LANE_RATE_MS = 120;
    let laneAdmits = 0, refusals = 0;
    for (const b of base) {
      lane.set(b.hex, { used: 0, lastAt: 0 });
      b.transport._acceptConnection = () => {
        if (b.node.synaptome.size < capOf.get(b.hex)) return true;  // below cap: hold-all admits
        const L = lane.get(b.hex);
        if (!L.enabled) { refusals++; return false; }       // improve-gate shut, no lane
        const tnow = now();
        if (L.used >= LANE_SLOTS || tnow - L.lastAt < LANE_RATE_MS) { refusals++; return false; }
        L.used++; L.lastAt = tnow; laneAdmits++;
        return true;                                        // probation admit, outside cap
      };
    }
    // Control: no lane — the newcomer strands.
    const n1 = await makePeer(net, domain, 6, 6, null);
    for (const b of base) { try { await n1.transport.openConnection(b.hex); } catch { /* */ } }
    await wait(20);
    check('4 CONTROL: improve-gates shut, no lane → newcomer strands', n1.node.synaptome.size === 0, `(degree=${n1.node.synaptome.size}, refusals=${refusals})`);
    // Lane on: the newcomer reaches working degree; acceptor growth stays bounded.
    for (const L of lane.values()) L.enabled = true;
    const n2 = await makePeer(net, domain, 7, 7, null);
    for (let round = 0; round < 3; round++) {
      for (const b of base) { try { await n2.transport.openConnection(b.hex); } catch { /* */ } }
      await wait(140);                                      // respect the lane rate limit
    }
    const deg = n2.node.synaptome.size;
    const worstOver = Math.max(...base.map(b => b.node.synaptome.size - capOf.get(b.hex)));
    check('4 RECOVERY: newcomer reaches working degree through the lane', deg >= 3, `(degree=${deg})`);
    check('4 BOUND: acceptor tables bounded at cap + lane slots', worstOver <= LANE_SLOTS, `(worst over-cap=${worstOver}, lane=${LANE_SLOTS})`);
    check('4 BOUND: lane admissions rate-limited, not a bypass', laneAdmits <= base.length * LANE_SLOTS, `(admits=${laneAdmits})`);
  }

  // ════ Scenario 5: correlated inbound eviction ════
  // Every other peer evicts the victim at once. Hold-or-improve at the victim
  // does nothing — the loss is inbound (finding 3). Emergency refill below the
  // degree floor overrides the improve-gate; acceptors' lanes give it a door.
  {
    const { base } = await scenario(false);
    const victim = base[base.length - 1];
    const peersOf = [...victim.node.synaptome.keys()].map(big => base.find(b => b.big === big)).filter(Boolean);
    const known = peersOf.map(p => p.hex);                  // remembered peers (recent-peers cache stand-in)
    // Correlated eviction: each peer tears its edge to the victim; the victim
    // sees real drops (channel gone + onPeerDied), same path as churn.
    for (const p of peersOf) {
      realDrop(p, victim.hex);                              // p's side
      realDrop(victim, p.hex);                              // victim's side
      p.node.synaptome.delete(victim.big);
      victim.node.synaptome.delete(p.big);
    }
    await wait(20);
    check('5 SETUP: victim bled to zero by remote evictions alone', victim.node.synaptome.size === 0);
    // Acceptors at cap, improve-gates shut, lane = the emergency door.
    const CAP = Math.max(...base.slice(0, -1).map(b => b.node.synaptome.size));
    const LANE_SLOTS = 2; let admits = 0;
    for (const b of base.slice(0, -1)) {
      let used = 0;
      b.transport._acceptConnection = () => {
        if (b.node.synaptome.size < CAP) return true;
        if (used >= LANE_SLOTS) return false;
        used++; admits++; return true;
      };
    }
    // Emergency refill: below the floor the victim admits/dials ANYONE it
    // remembers — below-cap behavior, improve-gate not consulted. Guarded.
    const FLOOR = 5;
    const guard = new AttemptGuard({ maxAttempts: 3, baseMs: 40 });
    guardDials(victim, guard);
    let dialRounds = 0;
    while (victim.node.synaptome.size < FLOOR && dialRounds < 6) {
      dialRounds++;
      for (const hex of known) {
        if (victim.node.synaptome.size >= FLOOR) break;
        try { await victim.transport.openConnection(hex); } catch { /* */ }
      }
      await wait(50);
    }
    check('5 RECOVERY: victim recovers to the degree floor', victim.node.synaptome.size >= FLOOR, `(degree=${victim.node.synaptome.size}, floor=${FLOOR}, rounds=${dialRounds})`);
    check('5 BOUND: recovery bounded — no storm to get there', dialRounds <= 6 && admits <= (base.length - 1) * LANE_SLOTS, `(rounds=${dialRounds}, admits=${admits})`);
  }

  // ════ Scenario 6: empty-stratum pressure ════
  // The deficit is unfillable — searches return nothing because nobody
  // occupies the band. Without the brake this is the storm as lookups
  // (finding 5). Deficit backoff engages; a fresh routing record resets it.
  {
    const { t } = await scenario(true);
    const backoff = new DeficitBackoff({ baseMs: 40, factor: 2 });
    let searches = 0, suppressed = 0;
    t.peer.findKClosest = async () => {
      if (!backoff.allow('near')) { suppressed++; return []; }
      searches++;
      backoff.onEmpty('near');
      return [];                                            // empty stratum: nobody there
    };
    const TICKS = 30;
    for (let k = 0; k < TICKS; k++) { await t.peer._maintainSynaptome(); await wait(25); }
    // 30 ticks over ~750ms with 40ms base doubling: allowed searches ≈ log2 growth, far under tick count.
    check('6 BOUND: deficit backoff engages — searches ≪ ticks', searches >= 2 && searches <= 8 && suppressed >= TICKS - searches - 2, `(searches=${searches}, suppressed=${suppressed}, ticks=${TICKS})`);
    // Fresh routing record for the prefix → the next tick searches again.
    const before = searches;
    backoff.onRecord('near');
    await t.peer._maintainSynaptome(); await wait(10);
    check('6 RECOVERY: fresh routing record resumes the search immediately', searches === before + 1, `(before=${before}, after=${searches})`);
  }

  // ════ Scenario 7: the seed path (closure condition (e)) ════
  // The production admit path on binding transports is _seedSynaptomeWithSponsor
  // (AxonaPeer.js:1581) — today a direct insert, no cap, no compare. Two parts:
  // pin that gap as the before-state, then model the compare-and-swap gate AT
  // that entrypoint and show occupancy pins at cap with admits paired to evicts.
  {
    const { net, domain, base } = await scenario(false);
    // 7a — GAP PIN: today's kernel admits OVER cap through the seed path.
    const open = base[0];
    open.node._maxSynaptome = open.node.synaptome.size;     // declare it at-cap
    const gp = await makePeer(net, domain, 8, 8, null);
    await gp.transport.openConnection(open.hex);
    await wait(15);
    check('7a GAP PIN: current seed path admits over cap (the code gap, pinned)',
      open.node.synaptome.size > open.node._maxSynaptome,
      `(size=${open.node.synaptome.size}, cap=${open.node._maxSynaptome})`);
    // 7b — MODELED GATE at the entrypoint: id-derivable compare-and-swap.
    const gated = base[1];
    const CAP = gated.node.synaptome.size;
    gated.node._maxSynaptome = CAP;
    const group = (big) => clz264(gated.big ^ big) >> 2;    // anneal-group granularity
    const origSeed = gated.peer._seedSynaptomeWithSponsor.bind(gated.peer);
    let admits = 0, evictions = 0, refusals = 0, maxSize = gated.node.synaptome.size;
    gated.peer._seedSynaptomeWithSponsor = (sponsor) => {
      const syn = gated.node.synaptome;
      if (syn.has(sponsor)) return;
      if (syn.size < CAP) { admits++; origSeed(sponsor); maxSize = Math.max(maxSize, syn.size); return; }
      // at cap: candidate must fill a strictly sparser band than the densest
      const counts = new Map();
      for (const id of syn.keys()) { const gk = group(id); counts.set(gk, (counts.get(gk) ?? 0) + 1); }
      const candCount = counts.get(group(sponsor)) ?? 0;
      let denseG = -1, denseN = -1;
      for (const [gk, n] of counts) if (n > denseN) { denseN = n; denseG = gk; }
      if (candCount >= denseN) {                            // no structural improvement
        refusals++;
        gated.node.transport.closeConnection(toHex(sponsor)).catch?.(() => {});
        return;
      }
      const victim = [...syn.keys()].find(id => group(id) === denseG);
      syn.delete(victim); evictions++;
      gated.node.transport.closeConnection(toHex(victim)).catch?.(() => {});
      admits++; origSeed(sponsor);
      maxSize = Math.max(maxSize, syn.size);
    };
    for (let i = 0; i < 6; i++) {
      const nc = await makePeer(net, domain, 9 + i, 9 + i, null);
      try { await nc.transport.openConnection(gated.hex); } catch { /* */ }
      await wait(10);
    }
    const atCapAdmits = admits;                             // all admits here happened at cap
    check('7b BOUND: gated seed path pins occupancy at cap', maxSize <= CAP && gated.node.synaptome.size <= CAP, `(max=${maxSize}, cap=${CAP})`);
    check('7b RECOVERY: structural improvers still admitted, paired with evictions',
      refusals + atCapAdmits === 6 && atCapAdmits === evictions,
      `(admits=${atCapAdmits}, evictions=${evictions}, refusals=${refusals})`);
  }

  // ════ Scenario 8: both doors open at once (finding 3) ════
  // Partition heal: a newcomer joins (join lane) while a bled victim runs
  // emergency refill — concurrently, on one signaling plane. The shared
  // per-node in-flight cap C_dial bounds concurrency; both recoveries complete.
  {
    const { net, domain, base } = await scenario(false);
    const capOf = new Map(base.map(b => [b.hex, b.node.synaptome.size]));
    for (const b of base) {
      let used = 0;
      b.transport._acceptConnection = () => {
        if (b.node.synaptome.size < capOf.get(b.hex)) return true;
        if (used >= 2) return false;                        // lane: 2 slots
        used++; return true;
      };
    }
    // Bleed a victim (correlated eviction, as scenario 5).
    const victim = base[base.length - 1];
    const peersOf = [...victim.node.synaptome.keys()].map(big => base.find(b => b.big === big)).filter(Boolean);
    const known = peersOf.map(p => p.hex);
    for (const p of peersOf) {
      realDrop(p, victim.hex); realDrop(victim, p.hex);
      p.node.synaptome.delete(victim.big); victim.node.synaptome.delete(p.big);
    }
    const joiner = await makePeer(net, domain, 11, 11, null);
    // Shared in-flight cap across BOTH dialers (per-NODE in the spec; shared
    // here to exercise the worst case of one plane under two doors).
    const C_DIAL = 2; let inflight = 0, peak = 0;
    const capDials = (rec) => {
      const orig = rec.transport.openConnection.bind(rec.transport);
      rec.transport.openConnection = async (id, ...a) => {
        while (inflight >= C_DIAL) await wait(5);
        inflight++; peak = Math.max(peak, inflight);
        try { await wait(12); return await orig(id, ...a); }  // negotiation takes time
        finally { inflight--; }
      };
    };
    capDials(joiner); capDials(victim);
    const FLOOR = 5;
    await Promise.all([
      (async () => { for (const b of base.slice(0, -1)) { try { await joiner.transport.openConnection(b.hex); } catch { /* */ } } })(),
      (async () => {
        let rounds = 0;
        while (victim.node.synaptome.size < FLOOR && rounds++ < 6) {
          for (const hex of known) {
            if (victim.node.synaptome.size >= FLOOR) break;
            try { await victim.transport.openConnection(hex); } catch { /* */ }
          }
        }
      })(),
    ]);
    check('8 BOUND: shared in-flight dial cap held under both doors', peak <= C_DIAL, `(peak=${peak}, C_dial=${C_DIAL})`);
    check('8 RECOVERY: join completed under the cap', joiner.node.synaptome.size >= 3, `(joiner degree=${joiner.node.synaptome.size})`);
    check('8 RECOVERY: emergency refill completed under the cap', victim.node.synaptome.size >= FLOOR, `(victim degree=${victim.node.synaptome.size}, floor=${FLOOR})`);
  }

  // ════ Scenario 9: the event-driven arm (Vega's alternative) ════
  // No timer search at all: on a peer death, dial replacements from a small
  // remembered cache. Measures BOTH what the shape saves (zero searches — the
  // empty-band storm cannot exist) and what it costs (a deficit with no drop
  // event never heals).
  {
    const { base, net, domain } = await scenario(false);
    const subject = base[2];
    const remembered = [...subject.node.synaptome.keys()]
      .map(big => base.find(b => b.big === big)).filter(Boolean).map(p => p.hex);
    const preDegree = subject.node.synaptome.size;
    let searches = 0;
    subject.peer.findKClosest = async () => { searches++; return []; };   // would-be timer search
    const guard = new AttemptGuard({ maxAttempts: 3, baseMs: 30 });
    // Event-driven refill: on died, dial from the cache (guarded).
    subject.transport.onPeerDied(async () => {
      for (const hex of remembered) {
        if (subject.node.synaptome.size >= preDegree) break;
        if (!guard.allow(hex)) continue;
        guard.begin(hex);
        let ok = false;
        try { ok = await subject.transport.openConnection(hex); } catch { /* */ }
        guard.end(hex, !!ok);
      }
    });
    // Drop 4 edges — real died events on the subject.
    const victims = [...subject.node.synaptome.keys()].slice(0, 4)
      .map(big => base.find(b => b.big === big)).filter(Boolean);
    for (const v of victims) { realDrop(v, subject.hex); realDrop(subject, v.hex); }
    await wait(250);
    check('9 RECOVERY: drop-driven deficits heal from the cache', subject.node.synaptome.size >= preDegree, `(degree=${subject.node.synaptome.size}, pre=${preDegree})`);
    check('9 BOUND: zero searches — the empty-band storm cannot exist here', searches === 0, `(searches=${searches})`);
    // The cost: a COLD deficit (no drop event) never heals under this shape.
    const cold = await makePeer(net, domain, 12, 12, null); // knows only a sponsor
    await cold.transport.openConnection(base[0].hex);
    await wait(200);                                        // no drops occur → no events → no refill
    check('9 COST: a cold deficit stays unfilled without a drop event', cold.node.synaptome.size <= 2, `(degree=${cold.node.synaptome.size} — the arm heals only what it watches break)`);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('matrix threw:', err?.stack || err); process.exit(2); });
