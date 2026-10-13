// =====================================================================
// fence_near_improve_dial.mjs — Perimeter v0.1 §5–§6 (Aster 388fda2d):
// "improve an occupied near slot" — at cap, the fill tick searches the
// nearest and dials AT MOST ONE candidate the gate predicts it will admit by
// near displacement; the dial is covered by the swap (R2-I1's deficit
// deferral is bypassed only for that prediction).
//
// The defect it closes: until 4.111.0 the fill returned 'at-cap' before
// nominating anything, so a node at cap never learned of a closer peer and
// its near set could only improve by chance (a closer peer dialing IT).
//
// Real AxonaPeers over the sim transport, fill armed (guard + gate + maintain),
// kNear 1, cap 3. X holds three crafted FAR entries (bit 260 and low bits:
// one dense band, group 0). Y is a real peer whose random id, sharing X's
// region prefix, is closer to X than any crafted entry. findKClosest is
// driven to return Y (the search itself is fenced elsewhere).
//   1  the at-cap tick runs the improvement step: one search, Y a candidate,
//      one dial, outcome bound; Y is admitted by near displacement; a
//      crafted entry is the victim; the table stays at cap; near-improve-dial
//      and near-improve are logged; counters move
//   2  a second tick finds nothing closer: no dial, no candidate
//   3  a candidate that is NOT closer than the kNear-th held is never dialed
//      at cap (the deficit deferral stands for it)
//   4  the guard's schedule is respected: a candidate on backoff is not dialed
//   5  with the gate unarmed (legacy maintenance) the step is inert
//
// Mutant (run by hand 2026-10-13): delete the `!this._improveAdmits(peerId)`
// clause from the dial's first deficit check → 1 fails (deferred-deficit).
//
// Run: node test/fence_near_improve_dial.mjs
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
const ARMED = { synaptomeMaintain: { kNear: 1, maxPerTick: 3, kCache: 8, pPending: 2, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 1, sparseFloor: 1 } };

async function makePeer(net, domain, opts = {}, cap = null) {
  const id = await createNodeIdentity({ lat: 40 + Math.random(), lng: -74 });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 });
  node.transport = sim;
  if (cap !== null) node._maxSynaptome = cap;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport: sim, ...opts });
  if (opts.admissionGate) peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  const logs = []; const ol = peer._emitLog.bind(peer); peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
  return { peer, node, sim, big: fromHex(id.id), hex: id.id, logs };
}
function craft(rec, dist, weight = 0.5) {
  const id = rec.big ^ dist;
  const syn = new Synapse({ peerId: id, latencyMs: 20, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'fence';
  rec.node.synaptome.set(id, syn);
  return id;
}
const FAR = 1n << 260n;
const tick = async (rec) => { rec.peer._deficitBackoff?.reset(); return rec.peer._maintainSynaptome(); };
const logCount = (logs, m) => logs.filter(([x]) => x === m).length;

(async () => {
  console.log('fence_near_improve_dial: at cap, one dial toward a strictly closer peer, covered by the swap');
  const domain = new AxonaDomain();
  const net = new SimNetwork();

  // ── 1, 2 ───────────────────────────────────────────────────────────────
  {
    const X = await makePeer(net, domain, ARMED, 3);
    const Y = await makePeer(net, domain, {});
    const f = [craft(X, FAR, 0.9), craft(X, FAR | 1n, 0.3), craft(X, FAR | 2n, 0.6)];
    check('setup: at cap with three far entries; Y is closer than every held entry', X.node.synaptome.size === 3 && (X.big ^ Y.big) < (X.big ^ f[0]));
    X.peer.findKClosest = async () => [X.big, Y.big];       // the search's answer, driven
    const rep1 = await tick(X);
    const last = X.peer._fillLast;
    check('1 the at-cap tick ran the improvement step: searched, one candidate, one dial', last?.state === 'at-cap' && last?.improve?.searched === true && last?.improve?.candidates === 1 && last?.improve?.dialed === 1, J(last?.improve));
    check('1 the dial bound (outcome bound) and the tick returned 1 attempt', last?.improve?.outcome === 'bound' && rep1 === 1, J({ outcome: last?.improve?.outcome, rep1 }));
    check('1 Y is admitted by near displacement; a crafted entry is the victim; still at cap',
      X.node.synaptome.has(Y.big) && !X.node.synaptome.has(f[1]) && X.node.synaptome.size === 3, J({ size: X.node.synaptome.size, hasY: X.node.synaptome.has(Y.big) }));
    check('1 near-improve-dial and near-improve logged; counters moved',
      logCount(X.logs, 'near-improve-dial') === 1 && logCount(X.logs, 'near-improve') === 1 && X.peer._perimeterStats.improveDials === 1 && X.peer._perimeterStats.nearImproves === 1, J(X.peer._perimeterStats));
    check('1 no deficit deferral was logged for the covered dial', logCount(X.logs, 'dial-deferred') === 0);
    const rep2 = await tick(X);
    const last2 = X.peer._fillLast;
    check('2 a second tick finds nothing closer: searched, no candidate, no dial', rep2 === 0 && last2?.improve?.searched === true && last2?.improve?.candidates === 0 && last2?.improve?.dialed === 0, J(last2?.improve));
    await X.peer.stop(); await X.sim.stop(); await Y.peer.stop(); await Y.sim.stop();
  }

  // ── 3: not closer → never dialed at cap ─────────────────────────────────
  {
    const X = await makePeer(net, domain, ARMED, 3);
    const Z = await makePeer(net, domain, {});
    craft(X, 2n); craft(X, FAR, 0.9); craft(X, FAR | 1n, 0.3);   // nearest held at distance 2: nothing real can be closer
    X.peer.findKClosest = async () => [X.big, Z.big];
    const rep = await tick(X);
    const last = X.peer._fillLast;
    check('3 a candidate not closer than the kNear-th held is not a candidate and not dialed', rep === 0 && last?.improve?.candidates === 0 && last?.improve?.dialed === 0 && !X.node.synaptome.has(Z.big), J(last?.improve));
    await X.peer.stop(); await X.sim.stop(); await Z.peer.stop(); await Z.sim.stop();
  }

  // ── 4: the guard's schedule ─────────────────────────────────────────────
  {
    const X = await makePeer(net, domain, ARMED, 3);
    const Y = await makePeer(net, domain, {});
    craft(X, FAR, 0.9); craft(X, FAR | 1n, 0.3); craft(X, FAR | 2n, 0.6);
    X.peer.findKClosest = async () => [X.big, Y.big];
    X.peer._attemptGuard.allow = () => false;                // on backoff
    const rep = await tick(X);
    const last = X.peer._fillLast;
    check('4 a candidate on the guard\'s backoff is a candidate but is not dialed', rep === 0 && last?.improve?.candidates === 1 && last?.improve?.dialed === 0 && X.peer._perimeterStats.improveDials === 0, J(last?.improve));
    await X.peer.stop(); await X.sim.stop(); await Y.peer.stop(); await Y.sim.stop();
  }

  // ── 5: unarmed → inert ─────────────────────────────────────────────────
  {
    const X = await makePeer(net, domain, { synaptomeMaintain: { kNear: 1, maxPerTick: 3 } }, 3);
    craft(X, FAR, 0.9); craft(X, FAR | 1n, 0.3); craft(X, FAR | 2n, 0.6);
    const out = await X.peer._nearImproveStep(X.big);
    check('5 without the gate the step is inert', out.searched === false && out.dialed === 0, J(out));
    await X.peer.stop(); await X.sim.stop();
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
