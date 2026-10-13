// =====================================================================
// fence_band_discovery.mjs — Perimeter v0.1 §5 / v0.3 §F (Aster 388fda2d,
// fd1ff07b): an independent band-discovery path, and fairness between the
// near and band classes in the fill's dial order.
//
// The defect it closes: the fill nominated from the near search, lookahead
// answers and the bridge directory only; nothing sought an edge for a band
// that had none, and nearest-first dialing could starve the crossing edges
// (bot 5077d2a2: K-closest alone fragments; the bands are what cross).
//
// Real AxonaPeer over the sim transport, fill armed, kNear 1, sparseFloor 1,
// cap 8, STRATA_GROUPS 4 (bands = clz264 >>> 2, capped at 3; higher = closer).
// findKClosest is driven: it records the target it was asked for and answers
// what the case says.
//   1  with the table held only in the closest band (group 3), the step
//      probes the sparsest band INSIDE the held span — here nothing, because
//      the span is one band and it is not sparse (count ≥ sparseFloor)
//   2  with one entry in group 0 and the rest in group 3, bands 1 and 2 are
//      inside the span and empty: the step probes one of them (the lower,
//      1), with a target whose band is 1, and nominates the answer that is
//      IN that band as 'band' and drops the answer that is not
//   3  the same band is not probed again within four ticks; the other
//      sparse band (2) is probed on the next tick instead
//   4  alternation: with a 'near' candidate nearer and a 'band' candidate
//      farther both cached, an even tick dials the near one first and an
//      odd tick the band one first (the transport's opens record the order)
//   5  at cap the step does nothing (the fill returns before it)
//
// Mutant (run by hand 2026-10-13): make bandFirst always false → 4 fails;
// delete the `R.groupOf(id) !== pick` filter → 2 fails.
//
// Run: node test/fence_band_discovery.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 10) : x));
const ARMED = { synaptomeMaintain: { kNear: 1, maxPerTick: 3, kCache: 8, pPending: 4, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 1, sparseFloor: 1 } };

async function makePeer(net, domain, cap) {
  const id = await createNodeIdentity({ lat: 40 + Math.random(), lng: -74 });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const ctl = { opens: [] };
  const transport = new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'openConnection') return async (x) => { ctl.opens.push(x); return false; };   // record the order; never binds
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 });
  node.transport = transport; node._maxSynaptome = cap;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...ARMED });
  peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  const logs = []; const ol = peer._emitLog.bind(peer); peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
  return { peer, node, sim, big: fromHex(id.id), ctl, logs };
}
function craft(rec, dist, weight = 0.5) {
  const id = rec.big ^ dist;
  const syn = new Synapse({ peerId: id, latencyMs: 20, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'fence';
  rec.node.synaptome.set(id, syn);
  return id;
}
const groupOf = (self, id, groups = 4) => Math.min(groups - 1, clz264(self ^ id) >>> 2);
const tick = async (rec) => { rec.peer._deficitBackoff?.reset(); return rec.peer._maintainSynaptome(); };

(async () => {
  console.log('fence_band_discovery: one probe per tick toward the sparsest held-span band; near and band alternate');
  const domain = new AxonaDomain();
  const net = new SimNetwork();

  // ── 1: span of one non-sparse band → no probe ───────────────────────────
  {
    const X = await makePeer(net, domain, 8);
    craft(X, 2n); craft(X, 4n);                               // group 3, count 2 ≥ sparseFloor
    const asked = []; X.peer.findKClosest = async (t) => { asked.push(t); return []; };
    const out = await X.peer._bandDiscoveryStep(X.big);
    check('1 the held span is one band and it is not sparse: nothing probed', out.probed === false && out.band === null && asked.length === 0, J(out));
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 2, 3: empty bands inside the span ───────────────────────────────────
  {
    const X = await makePeer(net, domain, 8);
    craft(X, 2n); craft(X, 4n); const far = craft(X, 1n << 260n);   // groups 3 and 0 held; 1 and 2 empty
    const asked = [];
    const inBand1 = X.big ^ (1n << 258n) ^ 0x5n;              // clz 5 → group 1
    const inBand3 = X.big ^ 9n;                                // group 3: NOT in the asked band
    X.peer.findKClosest = async (t) => { asked.push(t); return [inBand1, inBand3]; };
    const out = await X.peer._bandDiscoveryStep(X.big);
    check('2 the sparsest band inside the span is probed: band 1, target in band 1', out.probed === true && out.band === 1 && asked.length === 1 && groupOf(X.big, asked[0]) === 1, J({ out, askedGroup: asked.length ? groupOf(X.big, asked[0]) : null }));
    check('2 the answer IN the band is nominated as band; the answer outside it is dropped',
      out.nominated === 1 && X.peer._fillCache.get(inBand1)?.src === 'band' && !X.peer._fillCache.has(inBand3), J({ cache: [...X.peer._fillCache.entries()].map(([k, v]) => [k.toString(16).slice(0, 8), v.src]) }));
    check('2 band-probe logged and counted', X.logs.some(([m]) => m === 'band-probe') && X.peer._perimeterStats.bandProbes === 1 && X.peer._perimeterStats.bandNominated === 1);
    // 3: within four ticks band 1 is skipped; band 2 is probed instead
    X.peer._fillStats.ticks += 1;
    const out2 = await X.peer._bandDiscoveryStep(X.big);
    check('3 the next tick probes the other sparse band (2), not band 1 again', out2.probed === true && out2.band === 2 && groupOf(X.big, asked[1]) === 2, J(out2));
    X.peer._fillStats.ticks += 1;
    const out3 = await X.peer._bandDiscoveryStep(X.big);
    check('3 both probed within four ticks: nothing probed now', out3.probed === false, J(out3));
    X.peer._fillStats.ticks += 4;
    const out4 = await X.peer._bandDiscoveryStep(X.big);
    check('3 after four ticks band 1 is probed again', out4.probed === true && out4.band === 1, J(out4));
    void far;
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 4: alternation in the dial order ───────────────────────────────────
  {
    const X = await makePeer(net, domain, 8);
    craft(X, 2n);
    X.peer.findKClosest = async () => [];
    X.peer._fillDirectoryStep = () => ({ held: false, asked: false, state: 'none' });
    const nearC = X.big ^ 6n;                                  // nearer
    const bandC = X.big ^ (1n << 258n) ^ 0x3n;                 // farther, a band candidate
    const seed = () => { X.peer._fillCache.clear(); X.peer._fillCache.set(nearC, { src: 'near', at: Date.now() }); X.peer._fillCache.set(bandC, { src: 'band', at: Date.now() }); };
    X.peer._fillStats.ticks = 1;                               // the tick increments to 2 → even → near-first
    seed(); X.ctl.opens.length = 0; await tick(X);
    const evenOrder = X.ctl.opens.slice(0, 2);
    X.peer._attemptGuard = new (X.peer._attemptGuard.constructor)({});   // fresh schedule
    X.peer._fillStats.ticks = 2;                               // → 3 → odd → band-first
    seed(); X.ctl.opens.length = 0; await tick(X);
    const oddOrder = X.ctl.opens.slice(0, 2);
    check('4 an even tick dials the near candidate first', evenOrder.length === 2 && evenOrder[0] === nearC && evenOrder[1] === bandC, J(evenOrder));
    check('4 an odd tick dials the band candidate first', oddOrder.length === 2 && oddOrder[0] === bandC && oddOrder[1] === nearC, J(oddOrder));
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 5: at cap the step is not reached ──────────────────────────────────
  {
    const X = await makePeer(net, domain, 2);
    craft(X, 2n); craft(X, 1n << 260n);
    let asked = 0; X.peer.findKClosest = async () => { asked++; return []; };
    await tick(X);
    check('5 at cap: one search (the improvement step), no band probe', asked === 1 && X.peer._perimeterStats.bandProbes === 0 && X.peer._fillLast?.state === 'at-cap', J({ asked, st: X.peer._perimeterStats }));
    await X.peer.stop(); await X.sim.stop();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
