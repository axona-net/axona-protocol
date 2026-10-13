// =====================================================================
// fence_retention_policy.mjs — Perimeter v0.1 §2 / v0.2 §B,E / v0.3 §2
// (Aster 388fda2d, 541a178f, fd1ff07b; council-accepted at design level
// 2026-10-12): ONE retention policy, re-derived at every ask.
//
// The defect it closes: the kernel protected peers through TWO readers of
// one contract — the admission gate held kNear nearest + sparse bands (+ the
// duty gate), while the mesh's degree-cap and make-room sites read the DUTY
// set alone. A bridge could retire one of its nearest peers that the gate
// would have held (bot 6cb69b2c). And the gate's K and the fill's K were two
// knobs (relay.js:91 vs :97).
//
// On real AxonaPeers over the sim transport (crafted synaptome entries at
// chosen XOR distances; the transport wrapped to expose the web surfaces):
//   1  NEAR: the kNear nearest non-stale admitted peers carry `near`
//   2  BAND: a peer alone in its distance band (≤ sparseFloor) carries `band`
//   3  SERVICE: the transport's bridge node carries `service`
//   4  DUTY: a peer AxonaManager.obligationsOf names carries `duty`
//   5  SUSPECT: a stale near peer loses `near` only — the next nearest
//      gains it; the stale peer stays in the table; nothing closes it
//   6  protectedPeers() is the union, as lowercase hex, and it is what the
//      transport's setObligedPeers reader returns (the mesh's degree-cap and
//      make-room sites read it): the near peer is in it, not only duties
//   7  retentionOf an unheld id is all-false; a transport without isStale
//      reads no SUSPECT; a throwing obligations reader reads DUTY (held)
//   8  K coupling: gate kNear 10 against maintain kNear 5 is refused at
//      construction; equal values construct; either alone constructs
//   9  health().perimeter carries kNear, held, near, stale, band, service, duty
//
// Mutants (run by hand 2026-10-13): make protectedPeers() return duties only
// → 6 fails; delete the K assertion → 8 fails; drop the stale exclusion in
// _retentionMap → 5 fails.
//
// Run: node test/fence_retention_policy.mjs
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

/** A real peer; the transport Proxy exposes setObligedPeers, isStale, bridgeNodeIdBig and records closes. */
async function makePeer(net, domain, opts, ctl) {
  const id = await createNodeIdentity({ lat: 40 + Math.random(), lng: -74 });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const transport = new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'setObligedPeers') return (fn) => { ctl.obliged = fn; };
      if (prop === 'isStale') return ctl.isStale ?? undefined;
      if (prop === 'bridgeNodeIdBig') return ctl.bridge ?? undefined;
      if (prop === 'closeConnection') return async (x) => { ctl.closed.push(x); return target.closeConnection(x); };
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  return { peer, node, transport, sim, big: fromHex(id.id), hex: id.id };
}
/** Craft an admitted entry at XOR distance `dist` from self. */
function craft(rec, dist, weight = 0.5) {
  const id = rec.big ^ dist;
  const syn = new Synapse({ peerId: id, latencyMs: 20, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'fence';
  rec.node.synaptome.set(id, syn);
  return id;
}

(async () => {
  console.log('fence_retention_policy: one retention policy — NEAR, BAND, SERVICE, DUTY — re-derived at every ask');
  const domain = new AxonaDomain();
  const net = new SimNetwork();
  const ARMED = { synaptomeMaintain: { kNear: 2, maxPerTick: 3 }, attemptGuard: {}, admissionGate: { kNear: 2, sparseFloor: 1 } };

  // ── 1–7: the reasons ───────────────────────────────────────────────────
  {
    const ctl = { closed: [] };
    const X = await makePeer(net, domain, ARMED, ctl);
    // Bands are clz264 >>> 2, capped at STRATA_GROUPS-1, so every near
    // distance lands in the closest group and only HIGH bits separate bands:
    // two near (distances 2 and 4, closest group), three far in ONE dense
    // band (bit 260: clz 3, group 0), one alone in another band (bit 258:
    // clz 5, group 1; STRATA_GROUPS is 4, so clz ≥ 12 saturates into group 3).
    const n1 = craft(X, 2n), n2 = craft(X, 4n);
    const f1 = craft(X, 1n << 260n), f2 = craft(X, (1n << 260n) | 1n), f3 = craft(X, (1n << 260n) | 2n);
    const lone = craft(X, 1n << 258n);
    const r = (id) => X.peer.retentionOf(id);
    check('1 the two nearest carry near', r(n1).near && r(n2).near && !r(f1).near && !r(lone).near, J({ n1: r(n1), f1: r(f1) }));
    check('2 the lone peer in its band carries band; the dense band carries none', r(lone).band && !r(f1).band && !r(f2).band, J({ lone: r(lone), f1: r(f1) }));
    ctl.bridge = f2;
    check('3 the bridge node carries service', r(f2).service && !r(f1).service, J(r(f2)));
    const am = X.peer._axonaManager;
    const dutyHex = toHex(f3).toLowerCase();
    const orig = am.obligationsOf.bind(am);
    am.obligationsOf = (hex) => (hex === dutyHex ? ['upstream'] : orig(hex));
    check('4 a peer the manager names carries duty; nothing else does', r(f3).duty && !r(f1).duty && !r(n1).duty, J(r(f3)));
    // 5. SUSPECT
    ctl.isStale = (big) => big === n1;
    const before = ctl.closed.length;
    const rn1 = r(n1), rn2 = r(n2), rl = r(lone);
    check('5 a stale near peer loses near; the next nearest (the lone far peer) gains it', !rn1.near && rn2.near && rl.near, J({ n1: rn1, n2: rn2, lone: rl }));
    check('5 the stale peer stays in the table and nothing closed it', X.node.synaptome.has(n1) && ctl.closed.length === before);
    ctl.isStale = undefined;
    // 6. the union and the mesh reader
    const set = X.peer.protectedPeers();
    const want = [n1, n2, lone, f2, f3].map((b) => toHex(b).toLowerCase());
    check('6 protectedPeers() is the union near ∪ band ∪ service ∪ duty, as lowercase hex', set instanceof Set && want.every((h) => set.has(h)) && !set.has(toHex(f1).toLowerCase()), J([...set].map((h) => h.slice(-6))));
    check('6 the transport\'s obliged-peers reader IS that union (the mesh\'s degree-cap and make-room read it)', typeof ctl.obliged === 'function' && (() => { const s = ctl.obliged(); return s instanceof Set && s.has(toHex(n1).toLowerCase()) && s.has(dutyHex) && !s.has(toHex(f1).toLowerCase()); })());
    // 7. edges
    const none = r(X.big ^ (1n << 77n));
    check('7 an unheld id reads all-false', !none.any && !none.near && !none.band && !none.service && !none.duty);
    am.obligationsOf = () => { throw new Error('reader down'); };
    check('7 a throwing obligations reader reads DUTY (held, fail closed)', r(f1).duty === true);
    am.obligationsOf = orig;
    // 9. health
    const h = X.peer.health?.()?.perimeter;
    check('9 health().perimeter carries kNear, held, near, band, service, duty and the counters', h && h.kNear === 2 && h.held === 6 && h.near === 2 && h.band >= 1 && h.service === 1 && typeof h.nearImproves === 'number', J(h));
    await X.peer.stop(); await X.sim.stop();
  }

  // ── 8: K coupling ──────────────────────────────────────────────────────
  {
    const mk = async (opts) => {
      const id = await createNodeIdentity({ lat: 41, lng: -74 });
      const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
      const node = new NeuronNode({ id: fromHex(id.id), lat: 41, lng: -74 }); node.transport = sim;
      return new AxonaPeer({ domain, node, nodeIdentity: id, transport: sim, ...opts });
    };
    const throws = async (opts) => { try { await mk(opts); return false; } catch (e) { return /kNear/.test(String(e?.message)); } };
    check('8 gate kNear 10 against maintain kNear 5 is refused at construction, naming kNear', await throws({ admissionGate: { kNear: 10 }, synaptomeMaintain: { kNear: 5 }, attemptGuard: {} }));
    check('8 equal values construct', !(await throws({ admissionGate: { kNear: 7 }, synaptomeMaintain: { kNear: 7 }, attemptGuard: {} })));
    check('8 the gate alone constructs; the maintenance alone constructs', !(await throws({ admissionGate: { kNear: 10 } })) && !(await throws({ synaptomeMaintain: { kNear: 10 } })));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
