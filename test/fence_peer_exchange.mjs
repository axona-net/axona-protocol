// =====================================================================
// fence_peer_exchange.mjs — P2 of Mesh Partition Self-Heal v0.2 (axona-docs
// 87a608a; David 2026-10-30 "Build P3 and P2, default off"): PEER EXCHANGE
// over the mesh, flag PEER_EXCHANGE (default OFF).
//
// The partition it answers: 20 Windows relays restarted together on 2026-10-28
// and from then on held links only to each other. Nothing after graduation
// asked a bound peer "who do you hold?", so a browser tab bound to both
// islands carried no ids across. P2: every T_pex an ARMED node asks ONE bound
// channel — unasked first, then least recently asked — for a sample of that
// peer's USABLE set (M ∪ I, uniform without replacement, ≤ S_pex ids); the ids
// enter the candidate cache as 'pex' under an aggregate cap of K_cache/2
// (oldest pex evicted; near and band never displaced). A responder answers one
// asker per channel per C_pex, and answers NOTHING with the flag off.
//
// Real AxonaPeers over the sim transport.
//   D  defaults: OFF; env PEER_EXCHANGE=1 arms; an option wins; =0 is a control arm.
//   R  the RESPONDER: a sample of M ∪ I minus the asker and self, ≤ n, ≤ S_pex,
//      as hex; flag off → { ids: [], off }; a second ask inside C_pex → refused;
//      after the window → served; the sample covers the pool over many asks.
//   A  the ASKER: the fill tick asks when T_pex is due and not before; the ids
//      are nominated as 'pex'; unasked channels first, then least recently
//      asked (rotation); a failed ask is counted and the window still advances.
//   C  the CAP: pex ≤ K_cache/2, the oldest pex evicted, near kept, a near past
//      K_cache refused as before (pex never displaces it).
//   V  VALIDATION: non-ids, out-of-range ids and the responder's own id are
//      dropped and counted; a reply longer than S_pex is truncated.
//   O  OFF: an asker with the flag off, or armed without the fill, sends no
//      peer_exchange; health() carries the counters.
//
// Mutants: drop the aggregate cap → C fails; sample the synaptome instead of
// _usablePeers → R1 still passes on the sim (M only) — the I-class case is
// R1c with a proxied boundPeers/classOf; drop the cooldown → R2; drop the
// rotation → A3; ask with the flag off → O1; default ON → D.
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
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 8) : x));
const EXPECTED = 34;
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECTED) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECTED} checks`); process.exitCode = 1; } });

const ARMED = (extra = {}) => ({
  synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 2, directoryMs: 600000, pexMs: 300, pexSample: 4, pexCooldownMs: 50, ...extra },
  attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2, closeGraceMs: 60000 },
});

/** A real AxonaPeer on the sim. `wrap` adds a Proxy that records and can override the ASKER's sends (never used on a responder: a Proxy receiver is not the object the sim's handlers are deposited on). */
async function makePeer(net, domain, lat, lng, opts = {}, wrap = false) {
  const id = await createNodeIdentity({ lat, lng });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 });
  await sim.start(id.id);
  const ctl = { sendOverride: null, sends: [] };
  // Crafted table entries are not sim channels; _usablePeers requires transport-connected, so the
  // fence tells the sim (on the INSTANCE, read at call time) that crafted ids are connected.
  const fake = new Set();
  const simConn = sim.isConnected.bind(sim);
  sim.isConnected = (x) => fake.has(x) || simConn(x);
  const transport = !wrap ? sim : new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'send') return (p, type, body) => { ctl.sends.push(type); return ctl.sendOverride ? ctl.sendOverride(p, type, body) : target.send(p, type, body); };
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  if (opts.admissionGate) peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }   // the fence drives the tick
  const logs = []; peer.onLog('info', (ev, ctx) => logs.push({ ev, ctx }));
  return { peer, id, transport, sim, node, big: fromHex(id.id), hex: id.id, ctl, logs, fake };
}
function craft(rec, xorSeed) {
  const id = rec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(rec.big ^ id) });
  syn.weight = 0.5; syn.inertia = 0; syn._addedBy = 'crafted';
  rec.node.synaptome.set(id, syn);
  rec.fake.add(id);
  return id;
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
/** Open a sim channel a↔b and hold each other in the table (bound AND usable). */
async function link(a, b) { await a.sim.openConnection(b.hex); await wait(10); synapse(a, b); synapse(b, a); }
const stranger = (self, seed) => self ^ (1n << BigInt(seed));
const pexLogs = (p) => p.logs.filter((l) => l.ev === 'peer-exchange').map((l) => l.ctx);
const quiet = (rec) => { rec.peer.findKClosest = async () => []; rec.peer._bandDiscoveryStep = async () => ({ band: null, probed: false, nominated: 0 }); };
const tick = async (rec) => { rec.peer._deficitBackoff?.reset(); return rec.peer._maintainSynaptome(); };
const stopAll = async (...xs) => { for (const x of xs) { try { await x.peer.stop(); } catch { /* */ } try { await x.sim.stop(); } catch { /* */ } } };

(async () => {
  console.log('fence_peer_exchange: P2 — one bound channel asked per T_pex for a sample of its usable set');

  // ── D. defaults ──────────────────────────────────────────────────────
  {
    const had = Object.prototype.hasOwnProperty.call(process.env, 'PEER_EXCHANGE'), prev = process.env.PEER_EXCHANGE;
    delete process.env.PEER_EXCHANGE;
    const d0 = new AxonaDomain().PEER_EXCHANGE;
    process.env.PEER_EXCHANGE = '1';
    const d1 = new AxonaDomain().PEER_EXCHANGE;
    const d2 = new AxonaDomain({ PEER_EXCHANGE: false }).PEER_EXCHANGE;
    process.env.PEER_EXCHANGE = '0';
    const d3 = new AxonaDomain().PEER_EXCHANGE;
    if (had) process.env.PEER_EXCHANGE = prev; else delete process.env.PEER_EXCHANGE;
    check('D the default is OFF', d0 === false, String(d0));
    check('D env PEER_EXCHANGE=1 arms it; an explicit option wins; PEER_EXCHANGE=0 is a control arm', d1 === true && d2 === false && d3 === false, `${d1} ${d2} ${d3}`);
  }

  const ON = new AxonaDomain({ PEER_EXCHANGE: true });
  const OFF = new AxonaDomain({ PEER_EXCHANGE: false });

  // ── R. the responder ─────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 1, 1);
    const B = await makePeer(net, ON, 1, 2, { synaptomeMaintain: { pexCooldownMs: 50, pexSample: 4 } });
    const Boff = await makePeer(net, OFF, 1, 3);
    await link(A, B); await link(A, Boff);
    const C = craft(B, 3n), D = craft(B, 5n);
    const r1 = await A.sim.send(B.big, 'peer_exchange', { n: 16 });
    const ids1 = (r1?.ids ?? []).map((h) => fromHex(h));
    check('R1 the responder answers a sample of its usable set as hex ids: C and D, never the asker, never itself',
      ids1.length === 2 && ids1.includes(C) && ids1.includes(D) && !ids1.includes(A.big) && !ids1.includes(B.big) && r1.ids.every((h) => typeof h === 'string' && h.length === 66), J(r1));
    await wait(60);
    const r1b = await A.sim.send(B.big, 'peer_exchange', { n: 1 });
    check('R1b n bounds the sample (n=1 → one id)', r1b?.ids?.length === 1 && [C, D].includes(fromHex(r1b.ids[0])), J(r1b));
    await wait(60);
    const r1c = await A.sim.send(B.big, 'peer_exchange', { n: 99 });
    check('R1c n is clamped to S_pex (4): a pool of 2 still answers 2; a malformed n answers S_pex at most', r1c?.ids?.length === 2, J(r1c));
    // I-class: an identity the transport holds BOUND in class I, not in the table, is in the usable set and in the sample.
    // The sim has no classes; stand the web transport's I-class surfaces on the instance (read at call time by _usablePeers).
    const I1 = stranger(B.big, 120);
    const simBound = B.sim.boundPeers.bind(B.sim), simConn = B.sim.isConnected.bind(B.sim);
    B.sim.boundPeers = () => [...simBound(), I1]; B.sim.classOf = (x) => (x === I1 ? 'I' : 'M'); B.sim.isConnected = (x) => x === I1 || simConn(x);
    await wait(60);
    const r1d = await A.sim.send(B.big, 'peer_exchange', { n: 16 });
    check('R1d an I-class bound peer (not in the table) IS in the sample — the responder reads M ∪ I, not the synaptome alone',
      r1d?.ids?.length === 3 && r1d.ids.map((h) => fromHex(h)).includes(I1), J(r1d));
    B.sim.boundPeers = simBound; delete B.sim.classOf; B.sim.isConnected = simConn;
    const roff = await A.sim.send(Boff.big, 'peer_exchange', { n: 16 });
    check('R1e a responder with the flag OFF answers { ids: [], off: true } and counts it', Array.isArray(roff?.ids) && roff.ids.length === 0 && roff.off === true && Boff.peer._pexStats.servedOff === 1, J(roff));
    // R2 cooldown
    await wait(60);
    const s1 = await A.sim.send(B.big, 'peer_exchange', { n: 16 });
    const s2 = await A.sim.send(B.big, 'peer_exchange', { n: 16 });
    check('R2 a second ask inside C_pex is refused (ids empty, refused: cooldown) and counted', s1.ids.length === 2 && s2.ids.length === 0 && s2.refused === 'cooldown' && B.peer._pexStats.servedRefused === 1, J({ s1, s2, st: B.peer._pexStats }));
    await wait(60);
    const s3 = await A.sim.send(B.big, 'peer_exchange', { n: 16 });
    check('R2b after the window the same asker is served again; the served-at map holds only live windows', s3.ids.length === 2 && B.peer._pexServedAt.size === 1, J({ s3, size: B.peer._pexServedAt.size }));
    // R3 coverage: with a pool of 4 and n = 2, forty asks reach every member.
    const E = craft(B, 7n), F = craft(B, 9n);
    B.peer._maintainCfg.pexCooldownMs = 0;
    const seen = new Set();
    for (let i = 0; i < 40; i++) { const r = await A.sim.send(B.big, 'peer_exchange', { n: 2 }); for (const h of r.ids) seen.add(fromHex(h)); }
    check('R3 the sample is drawn across the pool: forty asks of 2 from {C,D,E,F} name all four', [C, D, E, F].every((x) => seen.has(x)) && seen.size === 4, J([...seen]));
    await stopAll(A, B, Boff);
  }

  // ── A. the asker ─────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 2, 1, ARMED(), true);
    const B = await makePeer(net, ON, 2, 2);
    await link(A, B);
    A.node._maxSynaptome = 20;
    quiet(A);
    const C = craft(B, 3n), D = craft(B, 5n);
    check('A0 precondition: the fill is armed, the exchange is on, B is usable to A, nothing asked yet', A.peer._fillArmed() && A.peer._pexOn() && A.peer._usablePeers().some((e) => e.id === B.big) && A.peer._pexStats.asks === 0);
    await tick(A);
    const l1 = pexLogs(A);
    check('A1 the first tick asks B (unasked), B answers C and D, both nominated as pex',
      A.peer._pexStats.asks === 1 && l1.length === 1 && l1[0].peer === B.hex.slice(0, 12) && l1[0].state === 'answered' && l1[0].offered === 2 && l1[0].nominated === 2 && A.peer._pexStats.nominated === 2, J({ st: A.peer._pexStats, l1 }));
    check('A1b the nominations carry src pex (read from the cache before the dial phase took them, via the tick report)', A.peer._fillLast?.pex?.nominated === 2 && A.peer._fillLast?.pex?.state === 'answered', J(A.peer._fillLast?.pex));
    await tick(A);
    check('A2 a second tick inside T_pex does NOT ask (held); the ask count stays 1', A.peer._pexStats.asks === 1 && A.peer._fillLast?.pex?.state === 'held', J(A.peer._fillLast?.pex));
    await wait(320);
    await tick(A);
    check('A2b after T_pex the tick asks again', A.peer._pexStats.asks === 2, String(A.peer._pexStats.asks));
    // A3 rotation: a second channel B2; the next ask goes to the UNASKED one, then back to the least recently asked.
    const B2 = await makePeer(net, ON, 2, 3);
    await link(A, B2);
    await wait(320); await tick(A);
    await wait(320); await tick(A);
    const order = pexLogs(A).map((l) => l.peer);
    check('A3 rotation: the unasked channel B2 is asked next, then the least recently asked (B) again',
      order.length === 4 && order[2] === B2.hex.slice(0, 12) && order[3] === B.hex.slice(0, 12), J(order));
    check('A3b the asked-at map is bounded by the usable set (two channels → two entries)', A.peer._pexAskedAt.size === 2, String(A.peer._pexAskedAt.size));
    // A4 a failed ask: counted, the window still advances (no retry storm).
    A.ctl.sendOverride = async (p, type, body) => { if (type === 'peer_exchange') throw new Error('channel dead'); return A.sim.send(p, type, body); };
    await wait(320); await tick(A);
    const lf = pexLogs(A).at(-1);
    check('A4 a throwing ask is counted askFailed, logged ok:false, and the next ask waits a full T_pex', A.peer._pexStats.askFailed === 1 && lf.ok === false && A.peer._fillLast?.pex?.state === 'failed' && A.peer._pexNextAt > Date.now() + 200, J({ lf, st: A.peer._pexStats }));
    A.ctl.sendOverride = null;
    // A4b 4.127.0: a channel that answers "no handler" (a peer before the frame) is marked incapable and the SAME tick asks another.
    const B3 = await makePeer(net, ON, 2, 5);
    await link(A, B3);
    const noHandlerFor = new Set([B.big, B2.big]);
    A.ctl.sendOverride = async (p, type, body) => { if (type === 'peer_exchange' && noHandlerFor.has(p)) throw new Error(`remote handler error: no handler for 'peer_exchange'`); return A.sim.send(p, type, body); };
    const asksBefore = A.peer._pexStats.asks, logsBefore = pexLogs(A).length;
    A.peer._pexAskedAt.set(B3.big, Date.now());   // B3 is the most recently asked, so B and B2 (older) are picked first
    await wait(320); await tick(A);
    const tail = pexLogs(A).slice(logsBefore);
    check('A4b two channels answering "no handler" are marked incapable and the tick goes on to the third, which answers — all in ONE tick',
      A.peer._pexStats.incapable === 2 && A.peer._fillLast?.pex?.state === 'answered' && A.peer._fillLast?.pex?.peer === B3.hex.slice(0, 12) && A.peer._pexStats.asks - asksBefore === 3 && tail.filter((l) => l.incapable === true).length === 2, J({ st: A.peer._pexStats, last: A.peer._fillLast?.pex, tail }));
    await wait(320); await tick(A);
    check('A4c the incapable channels are not asked again while the window holds; the capable one is', A.peer._pexStats.incapable === 2 && A.peer._fillLast?.pex?.peer === B3.hex.slice(0, 12) && A.peer._pexIncapable.size === 2, J({ st: A.peer._pexStats, last: A.peer._fillLast?.pex }));
    A.ctl.sendOverride = null;
    await A.sim.closeConnection(B3.hex); A.node.synaptome.delete(B3.big);
    // A5 the peer-off answer is observed as such.
    const Boff = await makePeer(net, OFF, 2, 4);
    await A.sim.closeConnection(B.hex); await A.sim.closeConnection(B2.hex); A.node.synaptome.delete(B.big); A.node.synaptome.delete(B2.big);
    await link(A, Boff);
    await wait(320); await tick(A);
    check('A5 an answer from a flag-off peer is recorded as peer-off with nothing nominated', A.peer._fillLast?.pex?.state === 'peer-off' && A.peer._fillLast?.pex?.nominated === 0, J(A.peer._fillLast?.pex));
    await stopAll(A, B, B2, Boff);
  }

  // ── C. the aggregate cap ─────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 3, 1, ARMED());
    A.node._maxSynaptome = 20;
    for (let i = 0; i < 4; i++) A.peer._nominateCandidate(stranger(A.big, 100 + i), 'near');
    const pex = [];
    for (let i = 0; i < 4; i++) { const id = stranger(A.big, 110 + i); pex.push(id); check(`C1 pex nomination ${i + 1} of 4 accepted (cap K_cache/2 = 4)`, A.peer._nominateCandidate(id, 'pex') === true); await wait(2); }
    const fifth = stranger(A.big, 120);
    const ok5 = A.peer._nominateCandidate(fifth, 'pex');
    const pexCount = [...A.peer._fillCache.values()].filter((r) => r.src === 'pex').length;
    check('C2 a fifth pex evicts the OLDEST pex and enters: pex count stays 4, the first pex id is gone, evicted counted',
      ok5 === true && pexCount === 4 && !A.peer._fillCache.has(pex[0]) && A.peer._fillCache.has(fifth) && A.peer._pexStats.evicted === 1, J({ ok5, pexCount, st: A.peer._pexStats }));
    check('C3 the near entries are untouched (4 of 4 still cached; cache 8 = K_cache)', [...A.peer._fillCache.values()].filter((r) => r.src === 'near').length === 4 && A.peer._fillCache.size === 8);
    const okNear = A.peer._nominateCandidate(stranger(A.big, 130), 'near');
    check('C4 a near past K_cache is refused as before — pex never displaces a non-pex entry', okNear === false && A.peer._fillStats.nominateRefusedFull === 1);
    await stopAll(A);
  }

  // ── V. validation ────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, ON, 4, 1, ARMED(), true);
    const B = await makePeer(net, ON, 4, 2);
    await link(A, B);
    A.node._maxSynaptome = 20; quiet(A);
    const good = [stranger(A.big, 140), stranger(A.big, 141)].map((x) => toHex(x));
    const many = Array.from({ length: 12 }, (_, i) => toHex(stranger(A.big, 150 + i)));
    A.ctl.sendOverride = async () => ({ ids: ['zz', 'not-hex', 'ff'.repeat(40), toHex(B.big), ...good, ...many] });
    await tick(A);
    check('V1 the reply is truncated to S_pex (4) BEFORE validation; two non-ids, an out-of-range id and the responder\'s own id are all dropped and counted',
      A.peer._pexStats.offered === 4 && A.peer._pexStats.invalid === 4 && A.peer._pexStats.nominated === 0, J(A.peer._pexStats));
    A.ctl.sendOverride = async () => ({ ids: good });
    await wait(320); await tick(A);
    check('V2 a clean reply nominates every id', A.peer._pexStats.nominated === 2 && A.peer._pexStats.invalid === 4, J(A.peer._pexStats));
    A.ctl.sendOverride = async () => 'garbage';
    await wait(320); await tick(A);
    check('V3 a reply that is not { ids: [] } nominates nothing and does not throw', A.peer._pexStats.nominated === 2 && A.peer._fillLast?.pex?.offered === 0, J(A.peer._fillLast?.pex));
    await stopAll(A, B);
  }

  // ── O. off ───────────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, OFF, 5, 1, ARMED(), true);
    const B = await makePeer(net, ON, 5, 2);
    await link(A, B); quiet(A); A.node._maxSynaptome = 20;
    await tick(A);
    check('O1 flag OFF: an armed fill sends no peer_exchange and reports the step off', !A.ctl.sends.includes('peer_exchange') && A.peer._pexStats.asks === 0 && A.peer._fillLast?.pex?.state === 'off', J({ sends: A.ctl.sends, pex: A.peer._fillLast?.pex }));
    const U = await makePeer(net, ON, 5, 3, { synaptomeMaintain: true }, true);   // maintenance without the guard and gate: not armed
    const B3 = await makePeer(net, ON, 5, 4);
    await link(U, B3); U.peer.findKClosest = async () => [U.big];
    await tick(U);
    check('O2 flag ON without the fill armed: no ask (the exchange rides the fill)', !U.ctl.sends.includes('peer_exchange') && U.peer._pexOn() === false);
    const h = A.peer.health();
    check('O3 health() carries pex and noPath counters', h.pex && h.pex.on === false && typeof h.pex.asks === 'number' && h.noPath && typeof h.noPath.deferred === 'number', J({ pex: h.pex, noPath: h.noPath }));
    await stopAll(A, B, U, B3);
  }

  if (passed + failed !== EXPECTED) { console.log(`  ✗ ran ${passed + failed} of ${EXPECTED} checks`); failed++; }
  console.log(`\nfence_peer_exchange: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
