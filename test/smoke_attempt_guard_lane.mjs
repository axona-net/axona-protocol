// =====================================================================
// smoke_attempt_guard_lane.mjs — the candidate attempt guard, deficit
// backoff, and join lane IN THE KERNEL (Connection-Quality v0.7;
// implementation slice 3), with the dht:presence record (slice 2) as the
// guard's live release valve.
//
// This closes the circle opened at c16d12b: the isolated storm — a
// never-binding near-successor re-probed on every nomination, forever —
// re-run through the REAL kernel paths with the guard armed, and bounded.
//
// Covers:
//   1. OPT-IN: flag off is byte-identical — the storm still exists
//      (re-probe on every nomination; arming is a deployment decision).
//   2. GUARD: armed, probes to a never-binding candidate stop at
//      maxAttempts with non-decreasing backoff spacing; expiry holds.
//   3. VALVE, end to end: expire → candidate becomes bindable → still
//      excluded → a REAL dht:presence record for the candidate arrives
//      (relayed) → guard refills → the next nomination probes and BINDS.
//      Scenario 2b, on the wire.
//   4. PACING: a flood of valid increasing gens refills at most once per
//      window (refills=1, rest coalesced) — matrix 2c, on the wire.
//   5. DEFICIT: a maintenance pass that attempts nothing backs the next
//      search off; a verified presence record resets it.
//   6. LANE: with kJoin, hold-all stops at cap − kJoin; a qualified
//      newcomer lane-admits ('gate-lane'); one admission per id per
//      window; lane cooldown holds; the table never exceeds cap;
//      kJoin absent = slice-1 behavior exactly.
//
// Run: node test/smoke_attempt_guard_lane.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { buildPresenceRecord }      from '../src/dht/presence.js';
import { AttemptGuard, identitySuffix } from '../src/dht/attemptGuard.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));

async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}

function craft(rec, xorSeed, weight = 0.5) {
  const id = rec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'crafted';
  rec.node.synaptome.set(id, syn);
  return id;
}

async function main() {
  console.log('Axona attempt guard + deficit backoff + join lane smoke (slice 3, kernel)\n');

  // ── 1. OPT-IN: flag off, the storm is preserved ───────────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const A = await makePeer(net, domain, 5, 5, {});                     // no guard
    const C = await makePeer(net, domain, 6, 6, {});
    let probes = 0; C.transport._acceptConnection = () => { probes++; return false; };
    for (let i = 0; i < 8; i++) await A.peer._considerCandidate(C.big, 'triadic');
    check('1 OPT-IN: flag off — a never-binding candidate is re-probed on EVERY nomination (the c16d12b storm, preserved)', probes === 8, `(probes=${probes})`);
    check('1 OPT-IN: no guard state exists on an un-armed peer', A.peer._attemptGuard === null && A.peer._deficitBackoff === null);
  }

  // ── 2 + 3 + 4. guard armed, presence as the live valve ────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const A = await makePeer(net, domain, 5, 5, { attemptGuard: { maxAttempts: 3, baseMs: 30, factor: 2, refillWindowMs: 150 } });
    const R = await makePeer(net, domain, 6, 6, {});                     // relay stand-in, bound to A
    await R.transport.openConnection(A.hex); await wait(15);
    const C = await makePeer(net, domain, 7, 7, { presence: { announceOnStart: false } });
    let bindable = false, probes = 0; const times = [];
    C.transport._acceptConnection = () => { probes++; times.push(Date.now()); return bindable; };

    // 2: nominations drive probes to maxAttempts, then stop.
    for (let i = 0; i < 12; i++) { await A.peer._considerCandidate(C.big, 'triadic'); await wait(25); }
    const gaps = times.slice(1).map((x, i) => x - times[i]);
    check('2 GUARD: probes stop at maxAttempts under repeated nominations', probes === 3 && A.peer._attemptGuard.expiredOf(C.big), `(probes=${probes})`);
    check('2 GUARD: backoff spacing non-decreasing', gaps.every((g, i) => i === 0 || g >= gaps[i - 1] - 15), `(gaps=${gaps.map(g => g | 0).join(',')})`);

    // 3: late bind stays excluded until a REAL presence record arrives.
    bindable = true;
    for (let i = 0; i < 4; i++) { await A.peer._considerCandidate(C.big, 'triadic'); await wait(20); }
    check('3 VALVE: late-bindable candidate still excluded (no record yet)', probes === 3 && !A.node.synaptome.has(C.big));
    const rec1 = await buildPresenceRecord({ identity: C.id, gen: 1 });
    await R.transport.notify(A.big, 'presence', { ...rec1, hop: 1 });     // relayed record reaches A
    await wait(30);
    await A.peer._considerCandidate(C.big, 'triadic'); await wait(30);
    check('3 VALVE: verified record refills the budget — next nomination probes and BINDS', probes === 4 && A.node.synaptome.has(C.big), `(probes=${probes}, bound=${A.node.synaptome.has(C.big)})`);

    // 4: pacing — a flood of valid gens refills once per window.
    const g = A.peer._attemptGuard;
    const refillsBefore = g.refills;
    for (let gen = 2; gen <= 8; gen++) {
      const r = await buildPresenceRecord({ identity: C.id, gen });
      await R.transport.notify(A.big, 'presence', { ...r, hop: 1 });
      await wait(10);
    }
    check('4 PACING: 7 valid increasing gens inside the window → at most one refill, rest coalesced',
      g.refills - refillsBefore <= 1 && g.coalesced >= 5,
      `(refills=${g.refills - refillsBefore}, coalesced=${g.coalesced})`);
    check('4 PACING: freshness never lost — watermark reached the latest gen', A.peer._presenceWatermarks.get(C.hex.slice(2)) === 8);
  }

  // ── 5. deficit backoff on the maintenance search ──────────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const A = await makePeer(net, domain, 5, 5, {
      synaptomeMaintain: { kNear: 3, intervalMs: 999999, maxPerTick: 3 },
      attemptGuard: { deficitBaseMs: 200, deficitFactor: 2 },
    });
    const B = await makePeer(net, domain, 6, 6, {});
    await A.transport.openConnection(B.hex); await wait(15);             // quota satisfiable = already connected
    const r1 = await A.peer._maintainSynaptome();                        // attempts 0 (nothing new to try)
    const blocked = !A.peer._deficitBackoff.allow();
    const r2 = await A.peer._maintainSynaptome();                        // inside backoff → skipped
    check('5 DEFICIT: an attempt-nothing pass engages the backoff; the next search is skipped', r1 === 0 && blocked && r2 === 0);
    // a verified presence record resets the backoff
    const X = await createNodeIdentity({ lat: 9, lng: 9 });
    const rec = await buildPresenceRecord({ identity: X, gen: 1 });
    await B.transport.notify(A.big, 'presence', { ...rec, hop: 1 });
    await wait(30);
    check('5 DEFICIT: a verified presence record resets the backoff (fresh routing evidence)', A.peer._deficitBackoff.allow() === true);
  }

  // ── 6. join lane: reserve-from-cap + qualification ────────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const A = await makePeer(net, domain, 5, 5, { admissionGate: { kNear: 3, sparseFloor: 2, kJoin: 2, laneCooldownMs: 80, laneWindowMs: 60000 } });
    // cap 8: hold-all room 6 (cap − kJoin), lane 2.
    [1n, 2n, 3n].forEach(x => craft(A, x));                              // kNear
    craft(A, 1n << 259n); craft(A, 1n << 258n);                          // sparse band
    craft(A, (1n << 263n) + 1n, 0.9);                                    // one dense filler → size 6
    A.node._maxSynaptome = 8;
    // Ordinary hold-all is EXHAUSTED at operational cap (6): the next two
    // admits must be LANE admissions, marked and bounded.
    const n1 = await makePeer(net, domain, 10, 10, {});
    await n1.transport.openConnection(A.hex); await wait(20);
    check('6 LANE: at operational cap the admit is a lane admission', A.node.synaptome.get(n1.big)?._addedBy === 'gate-lane', `(addedBy=${A.node.synaptome.get(n1.big)?._addedBy})`);
    // cooldown: an immediate second joiner is refused …
    const n2 = await makePeer(net, domain, 11, 11, {});
    await n2.transport.openConnection(A.hex); await wait(10);
    const refusedInCooldown = !A.node.synaptome.has(n2.big);
    await wait(100);                                                     // … and admitted after it
    await n2.transport.openConnection(A.hex); await wait(20);
    check('6 LANE: cooldown refuses, then admits — both lane slots fill', refusedInCooldown && A.node.synaptome.get(n2.big)?._addedBy === 'gate-lane');
    check('6 LANE: table never exceeds cap (lane is INSIDE the cap)', A.node.synaptome.size === 8, `(size=${A.node.synaptome.size})`);
    // one admission per id per window: drop n1 and re-dial inside the window → refused.
    A.node.synaptome.delete(n1.big);
    await A.transport.closeConnection(n1.hex); await wait(80 + 20);      // past cooldown, inside window
    await n1.transport.openConnection(A.hex); await wait(20);
    check('6 LANE: one admission per id per window — the same id re-dialing is refused', !A.node.synaptome.has(n1.big));
    // full table: the compare-and-swap gate still governs (slice-1 semantics on top).
    const dense0 = await makePeer(net, domain, 12, 12, {});
    // (band-0 with overwhelming probability; if it improves it swaps, else refused — either way ≤ cap)
    await dense0.transport.openConnection(A.hex); await wait(20);
    check('6 LANE: at FULL cap the slice-1 gate governs — still never over cap', A.node.synaptome.size <= 8, `(size=${A.node.synaptome.size})`);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
