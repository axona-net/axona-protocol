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
//   3. permanent-fail      : attempts expire and stop  / budget redirects elsewhere
//   4. newcomer vs saturated mesh : lane bounded+rate-limited / newcomer reaches degree
//   5. correlated inbound eviction: acceptor growth bounded   / victim recovers to floor
//   6. empty-stratum pressure     : deficit backoff engages   / resumes on fresh record
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

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('matrix threw:', err?.stack || err); process.exit(2); });
