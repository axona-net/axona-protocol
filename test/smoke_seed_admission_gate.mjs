// =====================================================================
// smoke_seed_admission_gate.mjs — the hold-or-improve admission gate at
// _seedSynaptomeWithSponsor (Connection-Quality definition v0.6, axona-docs
// 0e4d75a, council-closed 2026-08-24; first implementation slice).
//
// Covers:
//   1. OPT-IN: default (flag off) is byte-identical — including the historical
//      over-cap direct insert (the 7a gap the definition names).
//   2. HOLD-ALL: below cap, gate admits any distinct live peer.
//   3. REFUSE: at cap, a candidate in the densest band is refused AND its
//      just-bound channel is closed on both ends (no channel outside the budget).
//   4. SWAP: a candidate whose band stays sparser after the swap (integer
//      margin: densest >= candCount + 2) is admitted, paired with the eviction
//      of the lowest-vitality evictable edge in the densest band; occupancy
//      stays pinned at cap.
//   5. PROTECTION, re-verified at decision time: kNear nearest successors and
//      sparse-band (<= sparseFloor) members are never the victim.
//   6. EVICTION IS NOT DEATH: the victim is not dead-marked.
//
// Table shapes are CRAFTED (synthetic BigInt ids XOR-placed into exact bands
// relative to the acceptor) so every structural case is deterministic; the
// candidates are REAL peers with real bound channels so the refusal-closes-
// channel and admission paths run end to end.
//
// Run: node test/smoke_seed_admission_gate.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';

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

// Craft a synthetic synaptome entry at an exact XOR band relative to `self`.
// xorSeed pins clz(self ^ id): id = self ^ xorSeed. Not a live peer — the
// gate consults ids and vitality only, and closeConnection on an unknown id
// is a safe no-op on the sim transport.
function craft(peerRec, xorSeed, weight = 0.5) {
  const id = peerRec.big ^ xorSeed;
  const stratum = clz264(peerRec.big ^ id);
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'crafted';
  peerRec.node.synaptome.set(id, syn);
  return id;
}
// Band helper matching the gate: anneal group of clz >> 2, clamped.
const groupOf = (rec, big, domain) => Math.min(domain.STRATA_GROUPS - 1, clz264(rec.big ^ big) >>> 2);

// Mint a REAL peer whose id lands in the wanted band relative to `rec`.
async function mintInGroup(net, domain, rec, wantGroup, maxTries = 400) {
  for (let i = 0; i < maxTries; i++) {
    const p = await makePeer(net, domain, (i * 13) % 80 - 40, (i * 29) % 340 - 170, {});
    if (groupOf(rec, p.big, domain) === wantGroup) return p;
    await p.transport.stop?.();
  }
  return null;
}

async function main() {
  console.log('Axona seed-path admission gate smoke (hold-or-improve v0.6, first kernel slice)\n');

  // ── 1. flag off: byte-identical, over-cap insert preserved ────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const a = await makePeer(net, domain, 10, 10, {});          // no gate
    check('1 OPT-IN: no gate config by default', a.peer._gateCfg === null);
    a.node._maxSynaptome = 2;
    craft(a, 1n); craft(a, 2n);                                  // at declared cap
    const c = await makePeer(net, domain, 11, 11, {});
    await c.transport.openConnection(a.hex); await wait(15);
    check('1 OPT-IN: flag off admits over cap (legacy behavior preserved)', a.node.synaptome.size === 3, `(size=${a.node.synaptome.size}, cap=2)`);
  }

  // ── 2-6. gate armed ──────────────────────────────────────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const a = await makePeer(net, domain, 5, 5, { admissionGate: { kNear: 5, sparseFloor: 2 } });
    check('2 gate config armed with defaults overridable', a.peer._gateCfg?.kNear === 5 && a.peer._gateCfg?.sparseFloor === 2);

    // Below cap: hold-all — proven with a real peer, which is then REMOVED
    // so the at-cap table below is 100% crafted. A random real id left in
    // the table perturbs band counts (in band 1 it un-protects the sparse
    // pair and shifts the improver's candidate count) and made the refusal/
    // swap cases nondeterministic — Aster 67820521, one repeat at 21/24.
    a.node._maxSynaptome = 11;
    const below = await makePeer(net, domain, 6, 6, {});
    await below.transport.openConnection(a.hex); await wait(15);
    check('2 HOLD-ALL: below cap the gate admits any live peer', a.node.synaptome.has(below.big));
    a.node.synaptome.delete(below.big);
    await a.transport.closeConnection(below.hex); await wait(10);

    // Craft to exactly cap = 11 — crafted ids ONLY, every band pinned by seed:
    //  - 5 kNear successors: xor 1..5 (deepest band; protected by rank)
    //  - 2 sparse band-1 members: clz 4 and 5 (band count 2 <= sparseFloor)
    //  - 4 dense band-0 fillers: clz 0..3 (top-bit region), LOW vitality on one
    const nearIds   = [1n, 2n, 3n, 4n, 5n].map(x => craft(a, x));
    const sparseIds = [craft(a, 1n << 259n), craft(a, 1n << 258n)];
    const denseIds  = [craft(a, (1n << 263n) + 7n, 0.9), craft(a, (1n << 262n) + 3n, 0.9),
                       craft(a, (1n << 261n) + 9n, 0.05), craft(a, (1n << 260n) + 5n, 0.9)];
    const weakest = denseIds[2];                                 // vitality 0.05 — the intended victim
    check('3 SETUP: crafted table exactly at cap, no random ids', a.node.synaptome.size === 11 && !a.node.synaptome.has(below.big),
      `(size=${a.node.synaptome.size})`);

    // REFUSE: a real candidate in the dense band (band 0). Its band count is
    // the densest, so no post-swap improvement exists -> refuse + channel closed.
    const dense0 = await mintInGroup(net, domain, a, 0);
    check('3 SETUP: dense-band candidate minted', dense0 !== null);
    const sizeBefore = a.node.synaptome.size;
    await dense0.transport.openConnection(a.hex); await wait(20);
    const refused = !a.node.synaptome.has(dense0.big) && a.node.synaptome.size === sizeBefore;
    const channelClosedBoth = !dense0.transport.isConnected(a.hex) && !a.transport.isConnected(dense0.hex);
    check('3 REFUSE: densest-band candidate refused at cap — occupancy pinned', refused, `(size=${a.node.synaptome.size})`);
    check('3 REFUSE: refused candidate\'s channel closed on BOTH ends', channelClosedBoth);

    // SWAP: a real candidate in band >= 1 (its band count <= 3 while band 0
    // holds >= 5 with `below`) -> admitted, weakest dense evictable evicted.
    const improver = await mintInGroup(net, domain, a, 1) ?? await mintInGroup(net, domain, a, 2);
    check('4 SETUP: sparser-band candidate minted', improver !== null);
    await improver.transport.openConnection(a.hex); await wait(20);
    check('4 SWAP: structural improver admitted at cap', a.node.synaptome.has(improver.big));
    check('4 SWAP: occupancy stays pinned at cap', a.node.synaptome.size === 11, `(size=${a.node.synaptome.size})`);
    check('4 SWAP: victim is the lowest-vitality dense evictable', !a.node.synaptome.has(weakest));
    check('4 SWAP: admitted entry marked gate-swap', a.node.synaptome.get(improver.big)?._addedBy === 'gate-swap');

    // PROTECTION: kNear + sparse-band members all survived both decisions.
    const nearHeld   = nearIds.every(id => a.node.synaptome.has(id));
    const sparseHeld = sparseIds.every(id => a.node.synaptome.has(id));
    check('5 PROTECTION: kNear successors never evicted', nearHeld);
    check('5 PROTECTION: sparse-band (<= sparseFloor) members never evicted', sparseHeld);
    check('5 PROTECTION: incumbent table stayed crafted-only through both decisions (below removed, dense0 refused)', !a.node.synaptome.has(below.big) && !a.node.synaptome.has(dense0.big));

    // EVICTION IS NOT DEATH.
    check('6 EVICTION: victim not dead-marked', !(a.node._deadPeers?.has?.(weakest)));
  }

  // ── 7. margin boundary trio (council b41e2a88 / 70f85cc7 / 3b2cf359) ──
  // The operative bound is victimCount >= candCount + 2. Pinned exactly:
  // V=C+1 refused; V=C+2 admitted with post-swap TIE (and the evicted edge is
  // not re-admissible against the candidate — no ping-pong); V=C+3 admitted
  // with the candidate's band staying strictly sparser. Tables are crafted;
  // the candidate is a synthetic id driven straight through _admitOrImprove.
  {
    const mkCase = async (denseCount) => {
      const net = new SimNetwork(); const domain = new AxonaDomain();
      const a = await makePeer(net, domain, 5, 5, { admissionGate: { kNear: 5, sparseFloor: 2 } });
      [1n, 2n, 3n, 4n, 5n].forEach(x => craft(a, x));                       // kNear, protected (group 3)
      craft(a, 1n << 255n); craft(a, (1n << 255n) + 3n);                    // candidate band (group 2), C = 2
      const dense = [];
      for (let m = 0; m < denseCount; m++) dense.push(craft(a, (1n << 263n) + BigInt(m), m === 0 ? 0.05 : 0.9)); // group 0
      a.node._maxSynaptome = a.node.synaptome.size;                         // exactly at cap
      const cand = a.big ^ ((1n << 254n) + 7n);                             // group 2 candidate (C=2)
      return { a, cand, dense };
    };
    // V = C+1 (3 vs 2): refused.
    const c1 = await mkCase(3);
    check('7 BOUNDARY V=C+1: refused', c1.a.peer._admitOrImprove(c1.cand) === false && !c1.a.node.synaptome.has(c1.cand));
    // V = C+2 (4 vs 2): admitted; post-swap bands tie; evicted edge cannot reverse.
    const c2 = await mkCase(4);
    const ok2 = c2.a.peer._admitOrImprove(c2.cand);
    const evicted2 = c2.dense.find(id => !c2.a.node.synaptome.has(id));
    check('7 BOUNDARY V=C+2: admitted, occupancy pinned, weakest dense evicted', ok2 === true && c2.a.node.synaptome.has(c2.cand) && evicted2 === c2.dense[0] && c2.a.node.synaptome.size === c2.a.node._maxSynaptome);
    check('7 BOUNDARY V=C+2: post-swap tie — evicted edge NOT re-admissible (no ping-pong)', c2.a.peer._admitOrImprove(evicted2) === false);
    // V = C+3 (5 vs 2): admitted; candidate band stays strictly sparser.
    const c3 = await mkCase(5);
    const ok3 = c3.a.peer._admitOrImprove(c3.cand);
    const g = (big) => Math.min(c3.a.peer._domain.STRATA_GROUPS - 1, clz264(c3.a.big ^ big) >>> 2);
    const countIn = (grp) => [...c3.a.node.synaptome.keys()].filter(k => g(typeof k === 'bigint' ? k : fromHex(k)) === grp).length;
    check('7 BOUNDARY V=C+3: admitted, candidate band stays strictly sparser', ok3 === true && countIn(2) < countIn(0), `(g2=${countIn(2)}, g0=${countIn(0)})`);
  }

  // ── 8. legacy hex-string key regression (council b41e2a88 / 70f85cc7) ──
  // The seed path accommodates hex-string synaptome keys from older sessions;
  // an armed at-cap gate must DECIDE over them, not throw on BigInt ^ string.
  // The hex-keyed entry is fully structural: counted in its band, protectable,
  // evictable — and when it is the weakest dense edge, it IS the victim,
  // deleted by its original string key.
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const a = await makePeer(net, domain, 5, 5, { admissionGate: { kNear: 5, sparseFloor: 2 } });
    [1n, 2n, 3n, 4n, 5n].forEach(x => craft(a, x));                         // kNear (group 3)
    const denseBigs = [craft(a, (1n << 263n) + 1n, 0.9), craft(a, (1n << 262n) + 1n, 0.9), craft(a, (1n << 261n) + 1n, 0.9)];
    // one LEGACY entry: same dense band, keyed by hex STRING, weakest vitality
    const legacyBig = a.big ^ ((1n << 260n) + 9n);
    const legacyHex = toHex(legacyBig);
    const ls = new Synapse({ peerId: legacyBig, latencyMs: 50, stratum: clz264(a.big ^ legacyBig) });
    ls.weight = 0.01; ls.inertia = 0; ls._addedBy = 'legacy';
    a.node.synaptome.set(legacyHex, ls);                                    // hex-string map key
    a.node._maxSynaptome = a.node.synaptome.size;                           // at cap (9), dense band count 4
    const cand = a.big ^ ((1n << 254n) + 11n);                              // group 2, C=0 → margin clears (4 >= 2)
    let threw = false, admitted = false;
    try { admitted = a.peer._admitOrImprove(cand); } catch (e) { threw = true; }
    check('8 LEGACY: armed at-cap decision over a hex-string key does not throw', threw === false);
    check('8 LEGACY: gate decided — candidate admitted, occupancy pinned', admitted === true && a.node.synaptome.has(cand) && a.node.synaptome.size === a.node._maxSynaptome);
    check('8 LEGACY: the weakest dense edge was the hex-keyed entry, deleted by its ORIGINAL string key', !a.node.synaptome.has(legacyHex) && denseBigs.every(id => a.node.synaptome.has(id)));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
