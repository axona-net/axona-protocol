// =====================================================================
// fence_near_displacement.mjs — Perimeter v0.1 §6 / v0.3 §4,§8 (Aster
// 388fda2d, fd1ff07b): at cap, a candidate STRICTLY CLOSER than the kNear-th
// nearest held peer is admitted by displacing an unprotected edge; the
// farthest current near peer keeps its channel only by another reason.
//
// The defect it closes: until 4.111.0 the at-cap swap was keyed to band
// density with an anti-oscillation margin, so a newcomer closer than every
// held peer was refused whenever its band was already the densest — "a
// closer node pushes one off" (David 565bcf0b) did not exist.
//
// On a real AxonaPeer over the sim transport, gate kNear 2, sparseFloor 1,
// cap 6 (node._maxSynaptome), the table filled to cap with two near entries
// (distances 2 and 4), three far entries in one dense band and one lone
// entry in a sparse band:
//   1  a candidate at distance 3 (between the two near) is admitted by
//      gate-swap with near:true; the victim is a dense-band entry, never a
//      near, band-protected or service peer; the table stays at cap
//   2  the displaced near peer (distance 4) is STILL HELD, it only lost the
//      NEAR reason; retentionOf reads near:false for it
//   3  a candidate NOT closer than the kNear-th nearest, in the dense band,
//      is refused 'no-swap' under the margin exactly as before
//   4  monotone: three successively closer candidates are each admitted and
//      the kNear-th held distance strictly falls each time
//   5  when every unprotected edge holds a duty, a closer candidate is
//      refused with the duty named (swap-blocked), and when every edge is
//      protected the refusal is 'no-swap-near' and counted
//   6  SUSPECT: with the nearest held peer stale, the kNear-th NON-stale
//      distance is the ceiling, and the stale peer is not what the swap
//      closes while a lower-vitality unprotected edge exists
//
// Mutants (run by hand 2026-10-13): force nearImprove=false in
// _gateDecision → 1, 4 fail; rank the sponsor out of the near set → 2 fails.
//
// Run: node test/fence_near_displacement.mjs
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
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 10) : x));

async function makePeer(net, domain, ctl) {
  const id = await createNodeIdentity({ lat: 40 + Math.random(), lng: -74 });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const transport = new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'isStale') return ctl.isStale ?? undefined;
      if (prop === 'bridgeNodeIdBig') return ctl.bridge ?? undefined;
      if (prop === 'closeConnection') return async (x) => { ctl.closed.push(x); return target.closeConnection(x); };
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 });
  node.transport = transport;
  node._maxSynaptome = 6;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, admissionGate: { kNear: 2, sparseFloor: 1 } });
  peer._requireAxonaManager('fence');
  await peer.start();
  const logs = []; const ol = peer._emitLog.bind(peer); peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
  return { peer, node, transport, sim, big: fromHex(id.id), logs };
}
function craft(rec, dist, weight = 0.5) {
  const id = rec.big ^ dist;
  const syn = new Synapse({ peerId: id, latencyMs: 20, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'fence';
  rec.node.synaptome.set(id, syn);
  return id;
}
// Bands are clz264 >>> 2 capped at STRATA_GROUPS-1: near distances share the
// closest group (STRATA_GROUPS is 4: clz ≥ 12 saturates); the dense band
// sits at bit 260 (clz 3, group 0), the lone peer at bit 258 (clz 5, group 1).
const FAR = 1n << 260n;
const LONE = 1n << 258n;
function fillToCap(rec) {
  const n1 = craft(rec, 2n), n2 = craft(rec, 4n);
  const f = [craft(rec, FAR, 0.9), craft(rec, FAR | 1n, 0.3), craft(rec, FAR | 2n, 0.6)];   // dense band, lowest vitality = f[1]
  const lone = craft(rec, LONE);
  return { n1, n2, f, lone };
}

(async () => {
  console.log('fence_near_displacement: a strictly closer candidate takes a NEAR slot at cap');
  const domain = new AxonaDomain();
  const net = new SimNetwork();

  // ── 1, 2, 3 ─────────────────────────────────────────────────────────────
  {
    const ctl = { closed: [] };
    const X = await makePeer(net, domain, ctl);
    const { n1, n2, f, lone } = fillToCap(X);
    check('setup: at cap', X.node.synaptome.size === 6 && X.peer._retentionMap().nearCeil === 4n, J({ size: X.node.synaptome.size, ceil: X.peer._retentionMap().nearCeil }));
    const cand = X.big ^ 3n;
    const d = X.peer.gatePreflight(cand);
    check('1 preflight: strictly closer than the 2nd nearest → gate-swap with near:true', d.admit === true && d.how === 'gate-swap' && d.near === true, J(d));
    check('1 the victim is a dense-band entry: never near, band-protected or service', f.includes(d.victimKey) && d.victimKey !== n1 && d.victimKey !== n2 && d.victimKey !== lone, J({ victim: d.victimKey }));
    check('1 the victim is the lowest-vitality unprotected edge', d.victimKey === f[1]);
    const admitted = X.peer._admitOrImprove(cand);
    check('1 committed: candidate in the table, victim out, still at cap, near-improve logged and counted',
      admitted && X.node.synaptome.has(cand) && !X.node.synaptome.has(f[1]) && X.node.synaptome.size === 6
      && X.logs.some(([m]) => m === 'near-improve') && X.peer._perimeterStats.nearImproves === 1, J({ size: X.node.synaptome.size, st: X.peer._perimeterStats }));
    const r2 = X.peer.retentionOf(n2);
    check('2 the displaced near peer (distance 4) is still held and reads near:false', X.node.synaptome.has(n2) && r2.near === false && X.peer.retentionOf(cand).near === true, J(r2));
    // 3. not closer, dense band → the old margin rule
    const far = X.big ^ (FAR | 7n);
    const d3 = X.peer.gatePreflight(far);
    check('3 a candidate not closer than the 2nd nearest, in the dense band, is refused no-swap under the margin', d3.admit === false && d3.why === 'no-swap', J(d3));
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 4: monotone ──────────────────────────────────────────────────────────
  {
    const ctl = { closed: [] };
    const X = await makePeer(net, domain, ctl);
    fillToCap(X);
    // refill the dense band so there are enough victims for three swaps
    craft(X, FAR | 3n, 0.4); craft(X, FAR | 4n, 0.45); X.node._maxSynaptome = 8;
    const ceils = [X.peer._retentionMap().nearCeil];
    let all = true;
    for (const dist of [3n, 1n]) {
      const c = X.big ^ dist;
      const ok = X.peer._admitOrImprove(c);
      all = all && ok;
      ceils.push(X.peer._retentionMap().nearCeil);
    }
    check('4 successively closer candidates are each admitted', all);
    check('4 the kNear-th held distance strictly falls each time: 4 → 3 → 2', ceils.length === 3 && ceils[0] === 4n && ceils[1] === 3n && ceils[2] === 2n, J(ceils));
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 5: duties and full protection ────────────────────────────────────────
  {
    const ctl = { closed: [] };
    const X = await makePeer(net, domain, ctl);
    const { n2, f } = fillToCap(X);
    const am = X.peer._axonaManager; const orig = am.obligationsOf.bind(am);
    const dutyHex = new Set(f.map((b) => toHex(b).toLowerCase()));
    am.obligationsOf = (hex) => (dutyHex.has(hex) ? ['principal'] : orig(hex));
    const d = X.peer.gatePreflight(X.big ^ 3n);
    check('5 every unprotected edge holds a duty: the closer candidate is refused with the duty named', d.admit === false && d.why === 'duty' && d.duty === 'principal', J(d));
    check('5 ...and the protected edges were never candidates for the victim', X.logs.filter(([m]) => m === 'swap-blocked').length === 1);
    am.obligationsOf = orig;
    // make every edge protected: shrink the dense band to one (band-protected),
    // and give the near peer the sponsor would displace another reason
    // (SERVICE: it is the bridge's node) — a displaced near peer with no other
    // reason IS evictable, by design, so without this the swap would admit.
    X.node.synaptome.delete(f[1]); X.node.synaptome.delete(f[2]); X.node._maxSynaptome = 4;
    ctl.bridge = n2;
    const before = X.peer._perimeterStats.nearImproveRefused;
    const d2 = X.peer.gatePreflight(X.big ^ 3n);
    check('5 every edge protected: refused no-swap-near and counted', d2.admit === false && d2.why === 'no-swap-near' && X.peer._perimeterStats.nearImproveRefused === before + 1, J(d2));
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 6: SUSPECT ───────────────────────────────────────────────────────────
  {
    const ctl = { closed: [] };
    const X = await makePeer(net, domain, ctl);
    const { n1, n2, f } = fillToCap(X);
    ctl.isStale = (big) => big === n1;                      // the nearest is stale
    const R = X.peer._retentionMap();
    check('6 the ceiling is the kNear-th NON-stale distance: n2 then the lone far peer, not n1', R.nearCeil === LONE && R.map.get(n1).near === false && R.map.get(n2).near === true, J({ ceil: R.nearCeil }));
    const cand = X.big ^ 5n;                                // closer than the new ceiling (the 2nd non-stale is far)
    const d = X.peer.gatePreflight(cand);
    check('6 a candidate closer than the non-stale ceiling is admitted by near displacement', d.admit === true && d.near === true, J(d));
    check('6 the swap closes the lowest-vitality unprotected edge, not the stale near peer', d.victimKey === f[1] && d.victimKey !== n1);
    await X.peer.stop(); await X.sim.stop();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
