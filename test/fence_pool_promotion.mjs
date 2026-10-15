#!/usr/bin/env node
// =====================================================================
// fence_pool_promotion.mjs — G6 of the two-pool packet (axona-docs
// ca52384; Aster v1.1 bc4fe433 §3–§4, v1.2 b395fe8f G6): PROMOTION,
// CHURN AND RETENTION.
//
//   A. PROMOTION IS A TRANSACTION. An INCOMING bound peer offered to the
//      table is promoted: the gate decides (room, or a lawful victim), the
//      ledger moves the class, the table takes the entry. Counted.
//   B. DEFICIT, NOT CLOSE. At M full and DUTY-saturated the promotion ends
//      in a deficit: the table and the victim untouched, the I channel KEPT
//      (nothing closes — an I peer is lawfully I), counted.
//   C. THE RECLASS IS ASKED FIRST. A ledger that refuses the reclass leaves
//      the table untouched (deficit).
//   D. BOUNDED IMMUNITY. While the transaction runs the identity is
//      PROMOTION_PENDING and listed by protectedPeers(); after it ends it
//      is not; an entry past promoteMs is dropped as expired.
//   E. THE FILL MAY NOMINATE A BOUND I PEER (promotion is its "dial"); a
//      bound M peer is never nominated.
//   F. ELECTIVE EVICTION ORDER. selectMeshRetire releases an I channel
//      before an M channel when classes are carried; with no class the
//      order is the pre-pool one.
//   G. DUTY STILL HOLDS AN I PEER: mayRetire refuses by duty whatever the
//      class.
//   H. UNARMED: an unclassified bound peer refused by the gate takes the
//      4.113.0 grace path (closed after grace), byte-identical.
//
// Mutants: let the immunity outlive the transaction (never delete the
// pending entry) → D2 fails; close the I channel on a refused promotion →
// B fails.
// Run: node test/fence_pool_promotion.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';
import { makePoolProfile }          from '../src/transport/web/pool_profile.js';
import { selectMeshRetire }         from '../src/transport/web/mesh_degree.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 10) : x));
const asBig = (x) => (typeof x === 'bigint' ? x : fromHex(String(x)));

/** A peer over the sim transport, with the pool surfaces faked through `ctl`. */
async function makePeer(net, domain, ctl, opts = {}) {
  const id = await createNodeIdentity({ lat: 40 + Math.random(), lng: -74 });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const transport = new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'isStale')     return (x) => ctl.stale.has(asBig(x));
      if (prop === 'poolProfile') return () => ctl.profile;
      if (prop === 'classOf')     return (x) => ctl.cls.get(asBig(x)) ?? null;
      if (prop === 'reclass')     return (x, cls) => { ctl.reclassCalls.push([asBig(x), cls]); if (ctl.reclassRefuse) return { ok: false, why: 'pool-M', prev: 'I' }; const prev = ctl.cls.get(asBig(x)) ?? null; ctl.cls.set(asBig(x), cls); return { ok: true, why: null, prev }; };
      if (prop === 'boundPeers')  return () => [...target.boundPeers(), ...ctl.bound];
      if (prop === 'isConnected') return (x) => ctl.bound.has(asBig(x)) || ctl.conn.has(asBig(x)) || target.isConnected(x);
      if (prop === 'closeConnection') return async (x) => { ctl.closed.push(asBig(x)); return target.closeConnection(x); };
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, admissionGate: { kNear: 2, sparseFloor: 1, closeGraceMs: 20 }, ...opts });
  peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  const logs = []; const ol = peer._emitLog.bind(peer); peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
  return { peer, node, transport, big: fromHex(id.id), logs };
}
const mkCtl = (profile) => ({ profile, cls: new Map(), bound: new Set(), conn: new Set(), stale: new Set(), closed: [], reclassCalls: [], reclassRefuse: false });
function craft(rec, dist, weight = 0.5) {
  const id = rec.big ^ dist;
  const syn = new Synapse({ peerId: id, latencyMs: 20, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'fence';
  rec.node.synaptome.set(id, syn);
  return id;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  console.log('fence_pool_promotion: G6 — promotion is a bounded transaction; refusal keeps the incoming channel');
  const net = new SimNetwork();
  const domain = new AxonaDomain({ MAX_SYNAPTOME: 3, STRATA_GROUPS: 4 });

  // ── A. promotion under room ──────────────────────────────────────────
  {
    const ctl = mkCtl(makePoolProfile({ M: 3, I: 3, H: 6, pPending: 2, cInbound: 2 }));
    const X = await makePeer(net, domain, ctl);
    const inc = X.big ^ (1n << 200n);
    ctl.bound.add(inc); ctl.cls.set(inc, 'I');
    check('A setup: one bound peer of class I, not in the table, M cap 3 with room', !X.node.synaptome.has(inc) && X.peer._classOf(inc) === 'I' && X.peer._capM() === 3);
    X.peer._seedSynaptomeWithSponsor(inc);
    check('A1 offered to the table it is PROMOTED: entry in the table, the ledger asked to reclass I→M, counted', X.node.synaptome.has(inc) && ctl.reclassCalls.length === 1 && ctl.reclassCalls[0][1] === 'M' && ctl.cls.get(inc) === 'M' && X.peer._poolStats.promoted === 1, J({ calls: ctl.reclassCalls, stats: X.peer._poolStats }));
    check('A2 the transaction is closed: nothing pending, health().degree reports it', X.peer._promotionPending.size === 0 && X.peer.health().degree.promoted === 1 && X.peer.health().degree.armed === true);
    await X.peer.stop().catch(() => {});
  }
  // ── B. deficit at M full, duty-saturated ─────────────────────────────
  {
    const ctl = mkCtl(makePoolProfile({ M: 3, I: 3, H: 6, pPending: 2, cInbound: 2 }));
    const X = await makePeer(net, domain, ctl);
    const held = [craft(X, 1n << 258n), craft(X, 1n << 259n), craft(X, 1n << 260n)];
    for (const h of held) ctl.conn.add(h);
    const am = X.peer._axonaManager; const orig = am.obligationsOf.bind(am);
    am.obligationsOf = (hex) => (held.some((h) => toHex(h).toLowerCase() === hex) ? ['principal'] : orig(hex));
    const inc = X.big ^ (1n << 200n);                    // nearer than every held peer: a near displacement candidate
    ctl.bound.add(inc); ctl.cls.set(inc, 'I');
    X.peer._seedSynaptomeWithSponsor(inc);
    check('B at M full and duty-saturated the promotion ends in DEFICIT: table unchanged, no victim, counted', !X.node.synaptome.has(inc) && X.node.synaptome.size === 3 && held.every((h) => X.node.synaptome.has(h)) && X.peer._poolStats.promotionDeficit === 1 && X.logs.some(([m]) => m === 'promotion-deficit'), J(X.peer._poolStats));
    await sleep(40);
    check('B2 the I channel is KEPT: no closeConnection, no grace timer, incomingKept counted', ctl.closed.length === 0 && !X.peer._gracePending.has(inc) && X.peer._poolStats.incomingKept === 1, J(ctl.closed));
    check('B3 the ledger was not asked to reclass (the gate refused first)', ctl.reclassCalls.length === 0);
    am.obligationsOf = orig;
    // G. duty holds an I peer too
    am.obligationsOf = (hex) => (hex === toHex(inc).toLowerCase() ? ['replica'] : orig(hex));
    check('G mayRetire refuses an I peer that holds a duty, whatever its class', X.peer.mayRetire(inc).ok === false && X.peer.mayRetire(inc).duty === 'replica');
    am.obligationsOf = orig;
    await X.peer.stop().catch(() => {});
  }
  // ── C. the reclass is asked before the table commits ─────────────────
  {
    const ctl = mkCtl(makePoolProfile({ M: 3, I: 3, H: 6, pPending: 2, cInbound: 2 }));
    ctl.reclassRefuse = true;
    const X = await makePeer(net, domain, ctl);
    const inc = X.big ^ (1n << 200n);
    ctl.bound.add(inc); ctl.cls.set(inc, 'I');
    X.peer._seedSynaptomeWithSponsor(inc);
    check('C a refused reclass leaves the table untouched and counts a deficit; the channel stays I', !X.node.synaptome.has(inc) && ctl.reclassCalls.length === 1 && ctl.cls.get(inc) === 'I' && X.peer._poolStats.promotionDeficit === 1 && ctl.closed.length === 0);
    await X.peer.stop().catch(() => {});
  }
  // ── D. bounded immunity ──────────────────────────────────────────────
  {
    const ctl = mkCtl(makePoolProfile({ M: 3, I: 3, H: 6, pPending: 2, cInbound: 2, promoteMs: 50 }));
    const X = await makePeer(net, domain, ctl);
    const inc = X.big ^ (1n << 200n);
    ctl.bound.add(inc); ctl.cls.set(inc, 'I');
    let duringProtected = null, duringPending = null;
    const origDecision = X.peer._gateDecision.bind(X.peer);
    X.peer._gateDecision = (s) => { duringPending = X.peer._promotionPending.has(s); duringProtected = X.peer.protectedPeers()?.has(toHex(s).toLowerCase()); return origDecision(s); };
    X.peer._promote(inc);
    check('D1 WHILE the transaction runs the identity is PROMOTION_PENDING and protected from elective eviction', duringPending === true && duringProtected === true);
    check('D2 AFTER it ends the entry is gone and the identity is no longer listed as pending', X.peer._promotionPending.size === 0 && X.peer.health().degree.promotionPending === 0);
    // an entry left past its bound (a transaction that never ended) is dropped as expired
    const stray = X.big ^ (1n << 201n);
    X.peer._promotionPending.set(stray, Date.now() - 1);
    const ids = X.peer._promotionPendingIds();
    check('D3 an entry past promoteMs is dropped on the next read and counted expired: the immunity cannot pin a peer for ever', !ids.has(stray) && X.peer._promotionPending.size === 0 && X.peer._poolStats.promotionExpired === 1);
    await X.peer.stop().catch(() => {});
  }
  // ── E. the fill nominates a bound I peer, never a bound M peer ───────
  {
    const ctl = mkCtl(makePoolProfile({ M: 3, I: 3, H: 6, pPending: 2, cInbound: 2 }));
    const X = await makePeer(net, domain, ctl, { synaptomeMaintain: { kNear: 2, kCache: 8, maxPerTick: 2, pPending: 2 }, attemptGuard: {} });
    const bi = X.big ^ (1n << 200n), bm = X.big ^ (1n << 201n), free = X.big ^ (1n << 202n);
    ctl.bound.add(bi); ctl.cls.set(bi, 'I');
    ctl.bound.add(bm); ctl.cls.set(bm, 'M');
    check('E setup: the fill is armed; two bound peers (one I, one M) and one unbound candidate', X.peer._fillArmed() && X.peer._isBoundPeer(bi) && X.peer._isBoundPeer(bm));
    const nI = X.peer._nominateCandidate(bi, 'near'), nM = X.peer._nominateCandidate(bm, 'near'), nF = X.peer._nominateCandidate(free, 'near');
    check('E a bound I peer IS nominated (its dial is a promotion); a bound M peer is NOT; an unbound candidate is', nI === true && nM === false && nF === true && X.peer._fillCache.has(bi) && !X.peer._fillCache.has(bm), J({ nI, nM, nF }));
    const out = await X.peer._considerCandidate(bi, 'fill');
    check('E2 dialing the bound I candidate promotes it through the bound path: outcome bound, table holds it as M', out === 'bound' && X.node.synaptome.has(bi) && ctl.cls.get(bi) === 'M' && X.peer._poolStats.promoted === 1, J({ out }));
    await X.peer.stop().catch(() => {});
  }
  // ── F. eviction order by class ───────────────────────────────────────
  {
    const now = 1_000_000;
    const cands = [
      { id: 'm-old', region: '89', openedAt: now - 500_000, rttMs: 10, inCooldown: false, isProtected: false, cls: 'M' },
      { id: 'i-new', region: '89', openedAt: now - 100_000, rttMs: 10, inCooldown: false, isProtected: false, cls: 'I' },
      { id: 'm-2',   region: '89', openedAt: now - 400_000, rttMs: 10, inCooldown: false, isProtected: false, cls: 'M' },
    ];
    const pick = selectMeshRetire(cands, { now, minUptimeMs: 30_000 });
    check('F with classes carried the INCOMING channel is released first, though an M channel is older', pick?.id === 'i-new' && pick.cls === 'I', J(pick));
    const bare = cands.map(({ cls, ...c }) => c);
    const pick2 = selectMeshRetire(bare, { now, minUptimeMs: 30_000 });
    check('F2 with no class on any candidate the order is the pre-pool one: oldest open channel in the densest region', pick2?.id === 'm-old', J(pick2));
    const protectedI = cands.map((c) => (c.id === 'i-new' ? { ...c, isProtected: true } : c));
    check('F3 a protected (PROMOTION_PENDING or duty) I channel is never chosen', selectMeshRetire(protectedI, { now, minUptimeMs: 30_000 })?.id !== 'i-new');
  }
  // ── H. unarmed: byte-identical grace path ────────────────────────────
  {
    const ctl = mkCtl(null);
    const X = await makePeer(net, domain, ctl);
    const held = [craft(X, 1n << 258n), craft(X, 1n << 259n), craft(X, 1n << 260n)];
    for (const h of held) ctl.conn.add(h);
    const am = X.peer._axonaManager; const orig = am.obligationsOf.bind(am);
    am.obligationsOf = (hex) => (held.some((h) => toHex(h).toLowerCase() === hex) ? ['principal'] : orig(hex));
    const cand = X.big ^ (1n << 200n);
    ctl.bound.add(cand);                                  // bound, UNCLASSIFIED (no pools)
    X.peer._seedSynaptomeWithSponsor(cand);
    check('H unarmed: a bound unclassified peer refused at a duty-saturated cap takes the GRACE path (timer armed), not a promotion', !X.node.synaptome.has(cand) && X.peer._gracePending.has(cand) && X.peer._poolStats.promotionDeficit === 0 && X.peer.health().degree.armed === false);
    await sleep(60);
    check('H2 and is closed after grace, as 4.113.0 did (the duty gate is asked for the candidate itself, which holds none)', ctl.closed.includes(cand));
    am.obligationsOf = orig;
    await X.peer.stop().catch(() => {});
  }

  console.log(`\nfence_pool_promotion: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
