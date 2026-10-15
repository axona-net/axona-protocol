#!/usr/bin/env node
// =====================================================================
// fence_pool_union_selector.mjs — G3 of the two-pool packet (axona-docs
// ca52384; Aster v1.1 bc4fe433 §5, v1.2 b395fe8f G3): THE ROUTING UNION
// AND ONE DEGREE.
//
// One iterator (_usablePeers) over the deduplicated M ∪ I set — admitted
// (M) or I-bound, not dead, transport-connected, not SUSPECT — read by
// greedy, the two-hop first-hop set, the fallback hop and health().degree.
//
//   1. A SUSPECT I peer is neither selected by greedy nor counted in
//      degree.usable (Vega 675c730b's green-badge counterexample).
//   2. An I peer strictly closer than every M peer is the greedy hop.
//   3. A dead I peer is skipped.
//   4. A peer present in both classes is counted once (as M).
//   5. The two-hop free pass answers from the I set before any probe.
//   6. The route_msg handler's greedy reads the same union (an I hop is
//      taken; a path-excluded I hop is not).
//   7. UNARMED: no I set exists, SUSPECT M peers route as 4.113.0 did,
//      and health().degree says armed:false.
//
// Mutant: count I in degree.usable without the SUSPECT filter → 1 fails.
// Run: node test/fence_pool_union_selector.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { DeadPeers }                from '../src/dht/DeadPeers.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';
import { makePoolProfile }          from '../src/transport/web/pool_profile.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 10) : x));
const asBig = (x) => (typeof x === 'bigint' ? x : fromHex(String(x)));

async function makePeer(net, domain, ctl) {
  const id = await createNodeIdentity({ lat: 40 + Math.random(), lng: -74 });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const transport = new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'isStale')     return (x) => ctl.stale.has(asBig(x));
      if (prop === 'poolProfile') return () => ctl.profile;
      if (prop === 'classOf')     return (x) => ctl.cls.get(asBig(x)) ?? null;
      if (prop === 'boundPeers')  return () => [...target.boundPeers(), ...ctl.bound];
      if (prop === 'isConnected') return (x) => (ctl.bound.has(asBig(x)) || ctl.conn.has(asBig(x))) && !ctl.disconnected.has(asBig(x)) || target.isConnected(x);
      if (prop === 'send')        return async (peerId, type, payload) => { ctl.sent.push({ peerId: asBig(peerId), type, payload }); return ctl.reply ? ctl.reply(asBig(peerId), type, payload) : { terminal: true }; };
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, admissionGate: { kNear: 2, sparseFloor: 1 } });
  peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  return { peer, node, transport, big: fromHex(id.id) };
}
const mkCtl = (profile) => ({ profile, cls: new Map(), bound: new Set(), conn: new Set(), stale: new Set(), disconnected: new Set(), sent: [], reply: null });
function craft(rec, dist) {
  const id = rec.big ^ dist;
  const syn = new Synapse({ peerId: id, latencyMs: 20, stratum: clz264(rec.big ^ id) });
  syn.weight = 0.5; syn.inertia = 0; syn._addedBy = 'fence';
  rec.node.synaptome.set(id, syn);
  return id;
}

(async () => {
  console.log('fence_pool_union_selector: G3 — one usable set for every selector and for the degree');
  const net = new SimNetwork();
  const domain = new AxonaDomain({ MAX_SYNAPTOME: 6, STRATA_GROUPS: 4, LOOKAHEAD_MS: 50 });

  // ── armed ────────────────────────────────────────────────────────────
  {
    const ctl = mkCtl(makePoolProfile({ M: 6, I: 6, H: 12 }));
    const X = await makePeer(net, domain, ctl);
    // M peers at bits 258..260 (far); I peers at bits 200 (near), 201 (stale), 202 (dead)
    const mFar = [craft(X, 1n << 258n), craft(X, 1n << 259n), craft(X, 1n << 260n)];
    for (const m of mFar) ctl.conn.add(m);
    const iNear = X.big ^ (1n << 200n), iStale = X.big ^ (1n << 201n), iDead = X.big ^ (1n << 202n);
    for (const i of [iNear, iStale, iDead]) { ctl.bound.add(i); ctl.cls.set(i, 'I'); }
    ctl.stale.add(iStale);
    (X.node._deadPeers ??= new DeadPeers()).fail?.(iDead, 'fence') ?? X.node._deadPeers.add(iDead);

    const usable = X.peer._usablePeers();
    const ids = new Set(usable.map((u) => u.id));
    check('1 a SUSPECT I peer is not in the usable set and not counted in degree.usable', !ids.has(iStale) && X.peer.health().degree.usable === 4 && X.peer.health().degree.I === 3, J({ usable: usable.length, degree: X.peer.health().degree }));
    check('3 a dead I peer is skipped', !ids.has(iDead));
    check('1b the usable set is M(3) + I(1): the three far M peers and the near I peer', usable.filter((u) => u.cls === 'M').length === 3 && usable.filter((u) => u.cls === 'I').length === 1 && ids.has(iNear));

    // 2. greedy toward a target the near I peer is closest to
    const target = iNear ^ 1n;
    check('2 greedy picks the I peer strictly closer than every M peer', X.peer._greedyNextHopToward(target) === iNear);
    const targetStale = iStale ^ 1n;
    const hopS = X.peer._greedyNextHopToward(targetStale);
    check('1c toward a target the STALE I peer alone is nearer to than self, greedy answers null (a terminal), never the suspect', hopS === null, J({ hopS }));
    check('2b the fallback hop reads the same union', X.peer._capableFallbackHop(target, null) === iNear);

    // 4. a peer in both classes is counted once, as M
    const both = craft(X, 1n << 257n); ctl.conn.add(both); ctl.bound.add(both); ctl.cls.set(both, 'I');
    const u2 = X.peer._usablePeers();
    check('4 a peer present as M and as I-bound is counted ONCE, as M', u2.filter((u) => u.id === both).length === 1 && u2.find((u) => u.id === both).cls === 'M' && X.peer.health().degree.I === 3);

    // 5. the two-hop free pass answers from the I set, no probe sent
    ctl.sent.length = 0;
    const closer = await X.peer._findCloserInTwoHops(target, { exclude: null });
    check('5 the two-hop free pass answers with the I peer and emits no probe', closer?.first === iNear && closer.via === null && ctl.sent.length === 0, J({ closer, sent: ctl.sent.length }));
    const LS = X.peer.lookaheadStats?.() ?? X.peer._lookaheadStats;
    check('5b the census records it as answered by incoming', (LS?.answeredByIncoming ?? 0) >= 1);

    // 5c. with the near I peer excluded (on the path) the probes go to the FULL usable union:
    //     the usable M set plus every usable I peer that is not itself closer (Aster b583c31c P1),
    //     never the suspect or dead I peers
    const iFar = X.big ^ (1n << 261n);                     // an I peer farther than self from the target: not an answer, still a probe
    ctl.bound.add(iFar); ctl.cls.set(iFar, 'I');
    ctl.sent.length = 0;
    await X.peer._findCloserInTwoHops(target, { exclude: new Set([iNear]) });
    const probed = new Set(ctl.sent.filter((s) => s.type === 'lookahead_probe').map((s) => s.peerId));
    check('5c with the near I peer on the path, the probe set is the usable union: 4 M + the far I peer, never the suspect or dead I peers', probed.size === 5 && probed.has(iFar) && !probed.has(iStale) && !probed.has(iDead) && !probed.has(iNear), J({ probed: probed.size, hasFar: probed.has(iFar) }));

    // 6. the route_msg handler's greedy is the same iterator: read in source
    {
      const { readFileSync } = await import('node:fs');
      const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
      const h = src.indexOf("registerFrame(transport, 'route_msg'"); const hb = src.slice(h, src.indexOf('_findCloserInTwoHops(targetBig, { exclude })', h));
      check('6 the route_msg handler\'s inline greedy iterates _usablePeers({ exclude }) and no longer walks node.synaptome itself', /for \(const \{ id \} of this\._usablePeers\(\{ exclude \}\)\)/.test(hb) && !/for \(const syn of node\.synaptome\.values\(\)\)/.test(hb));
    }
    await X.peer.stop().catch(() => {});
  }
  // ── unarmed ──────────────────────────────────────────────────────────
  {
    const ctl = mkCtl(null);
    const X = await makePeer(net, domain, ctl);
    const mNear = craft(X, 1n << 200n); ctl.conn.add(mNear); ctl.stale.add(mNear);
    const mFar = craft(X, 1n << 258n); ctl.conn.add(mFar);
    const stray = X.big ^ (1n << 201n); ctl.bound.add(stray);   // bound, unclassified — no I set exists
    const u = X.peer._usablePeers();
    check('7 unarmed: a SUSPECT M peer still routes (byte-identical to 4.113.0), no I entry exists for a bound unclassified peer', u.some((x) => x.id === mNear) && !u.some((x) => x.id === stray) && X.peer._greedyNextHopToward(mNear ^ 1n) === mNear);
    const d = X.peer.health().degree;
    check('7b health().degree reports armed:false, I 0, usable = the connected table', d.armed === false && d.I === 0 && d.usable === 2 && d.profile === null, J(d));
    await X.peer.stop().catch(() => {});
  }

  console.log(`\nfence_pool_union_selector: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
