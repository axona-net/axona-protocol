#!/usr/bin/env node
// fence_fill_tick — Hold-and-Fill v0.15 (axona-docs e4809d2) row 12: the FILL.
//
// Rule 2: while peer(ADMITTED) < cap, discover and dial; stop at cap or when the
// fill stalls, and report which. Tick order after the reconcile and the search-
// backoff gate: NEIGHBOURS → DIRECTORY → DIAL, each dial reserving a pending
// slot (P_pending) and a channel token before the first frame; a dial that
// cannot reserve is deferred in place and counted dial-deferred (case 45).
//
//   A. ARMING: maintenance without the guard or the gate is the legacy near
//      refill (fill-disarmed logged once); with both, the fill is armed; with
//      the flag off nothing is nominated.
//   B. NOMINATE: self, held (in table or bound), duplicates, ineligible and
//      K_cache refusals; nothing consumed.
//   C. SOURCES: find_closest_set responses (a real sim lookup), lookahead
//      responses, the bridge peer-list through the transport's onPeerList.
//   D. THE TICK on the sim: nearest-first out of the cache, maxPerTick per
//      tick, each dial binds and admits; empty cache below cap → fill-stalled:
//      supply, logged once; at cap → at-cap, zero opens, zero deletes, zero
//      inserts (case 36's observable in the fill).
//   E. RESERVATION (case 45): at P_pending the dial is deferred in place —
//      attempts and token unchanged, dial-deferred counted and logged; a freed
//      slot lets the next tick dial; the channel-token half defers too.
//   F. LIVENESS: no directory answer → fill-stalled: rendezvous and nothing
//      inferred (case 7, no overlap); the re-contact timer draws in
//      [T/2, 3T/2]; an answered re-contact nominates the sample and the same
//      tick dials and binds it (case 7, overlap); every candidate on its
//      backoff → fill-stalled: fair-retry, candidates kept.
//   G. STATIC: tick order, reservation before allow and dial, nominate gated
//      on the arm, the transport's fan-out before the mesh's own dialing, the
//      ledger's pure predicate counts nothing.
//
// Admission is observed on the real table (sim binds admit through the gate).
import { readFileSync } from 'node:fs';
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
const ARMED = { synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 2, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2, closeGraceMs: 60000 } };

/**
 * A real AxonaPeer on a sim transport. `wrap` adds a Proxy that can stand in
 * for the web transport's surfaces: connectViaRelay (held tokens), an
 * openConnection override, requestPeerIntroductions, onPeerList, mayDial, send.
 */
async function makePeer(net, domain, lat, lng, opts = {}, wrap = false) {
  const id = await createNodeIdentity({ lat, lng });
  const sim = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await sim.start(id.id);
  const ctl = { boundCb: null, negCb: null, connected: null, relay: [], relayReturn: null, openOverride: null, opens: [], introductions: null, peerListCb: null, wantPeerList: false, mayDial: null, sendOverride: null };
  const transport = !wrap ? sim : new Proxy(sim, {
    get(target, prop, recv) {
      if (prop === 'onPeerBound') return (h) => { ctl.boundCb = h; return target.onPeerBound(h); };
      if (prop === 'onNegotiationFailed') return (h) => { ctl.negCb = h; return () => { ctl.negCb = null; }; };
      if (prop === 'isConnected') return (x) => (ctl.connected == null ? target.isConnected(x) : ctl.connected);
      if (prop === 'connectViaRelay') return ctl.relayReturn == null ? undefined : (hex) => { ctl.relay.push(hex); return typeof ctl.relayReturn === 'function' ? ctl.relayReturn(hex) : ctl.relayReturn; };
      if (prop === 'openConnection') return async (x) => { ctl.opens.push(x); return ctl.openOverride != null ? ctl.openOverride(x) : target.openConnection(x); };
      if (prop === 'requestPeerIntroductions') return ctl.introductions == null ? undefined : () => ctl.introductions();
      if (prop === 'onPeerList') return !ctl.wantPeerList ? undefined : (h) => { ctl.peerListCb = h; return () => { ctl.peerListCb = null; }; };
      if (prop === 'mayDial') return ctl.mayDial == null ? undefined : () => ctl.mayDial;
      if (prop === 'send' && ctl.sendOverride) return ctl.sendOverride;
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
  const logs = []; const ol = peer._emitLog.bind(peer); peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
  return { peer, id, transport, sim, node, big: fromHex(id.id), hex: id.id, ctl, logs };
}
function craft(rec, xorSeed, weight = 0.5) {
  const id = rec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'crafted';
  rec.node.synaptome.set(id, syn);
  return id;
}
const stranger = (self, seed) => self ^ (1n << BigInt(seed));
const logCount = (logs, m) => logs.filter(([x]) => x === m).length;
const tick = async (rec) => { rec.peer._deficitBackoff?.reset(); return rec.peer._maintainSynaptome(); };

(async () => {
  console.log('fence_fill_tick: row 12 — the fill: cap target, candidate cache, reservation, liveness report');
  const domain = new AxonaDomain();

  // ── A. arming ──────────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 1, 1, { synaptomeMaintain: true });
    a.peer.findKClosest = async () => [a.big];
    check('A1 maintenance without the guard: the fill is NOT armed', a.peer._fillArmed() === false && !!a.peer._maintainCfg);
    await tick(a); await tick(a);
    check('A1 the tick is the legacy near refill: no cache, no fill report, fill-disarmed logged ONCE', a.peer._fillCache.size === 0 && a.peer._fillLast == null && logCount(a.logs, 'fill-disarmed') === 1, J({ cache: a.peer._fillCache.size, logs: logCount(a.logs, 'fill-disarmed') }));
    const b = await makePeer(net, domain, 1, 2, { synaptomeMaintain: true, attemptGuard: {} });
    check('A2 guard without the gate: not armed', b.peer._fillArmed() === false);
    const c = await makePeer(net, domain, 1, 3, ARMED);
    check('A2 guard AND gate: armed; the option carries K_cache, P_pending and the re-contact period', c.peer._fillArmed() === true && c.peer._maintainCfg.kCache === 8 && c.peer._maintainCfg.pPending === 2 && c.peer._maintainCfg.directoryMs === 1000);
    const d = await makePeer(net, domain, 1, 4, { attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2 } });
    check('A3 flag off: nominate is inert, the cache stays empty', d.peer._nominateCandidate(stranger(d.big, 100), 'x') === false && d.peer._fillCache.size === 0);
    for (const x of [a, b, c, d]) await x.peer.stop().catch(() => {});
  }

  // ── B. nominate ────────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 2, 2, ARMED);
    a.node._maxSynaptome = 20;
    const inTable = craft(a, 3n);
    const p = await makePeer(net, domain, 2, 3, {}); await p.transport.openConnection(a.hex); await wait(30); a.node.synaptome.delete(p.big);
    check('B setup: armed, one crafted entry in the table, one real peer bound-not-in-table', a.peer._fillArmed() && a.node.synaptome.has(inTable) && a.transport.isConnected(p.hex) && !a.node.synaptome.has(p.big));
    check('B1 self: refused', a.peer._nominateCandidate(a.big, 'x') === false);
    check('B2 an identity in the table: refused', a.peer._nominateCandidate(inTable, 'x') === false);
    check('B3 a BOUND identity (held): refused', a.peer._nominateCandidate(p.big, 'x') === false);
    const S = stranger(a.big, 100);
    check('B4 a stranger: nominated (cache 1, counted)', a.peer._nominateCandidate(S, 'near') === true && a.peer._fillCache.size === 1 && a.peer._fillStats.nominated === 1 && a.peer._fillCache.get(S)?.src === 'near');
    check('B5 the same identity again: no-op', a.peer._nominateCandidate(S, 'near') === false && a.peer._fillCache.size === 1);
    const M = stranger(a.big, 101); a.node._deadPeers.fail(M, 'prior-loss');
    check('B6 a marked identity whose schedule has not come due: refused, counted ineligible, nothing consumed', a.peer._isEligibleCandidate(M) === false && a.peer._nominateCandidate(M, 'near') === false && a.peer._fillStats.nominateIneligible === 1 && a.node._deadPeers.get(M)?.attempts === 1);
    for (let i = 0; i < 8; i++) a.peer._nominateCandidate(stranger(a.big, 110 + i), 'near');
    check('B7 K_cache (8): the cache holds 8, the overflow is refused and counted', a.peer._fillCache.size === 8 && a.peer._fillStats.nominateRefusedFull === 1, J({ size: a.peer._fillCache.size, full: a.peer._fillStats.nominateRefusedFull }));
    for (const x of [a, p]) await x.peer.stop().catch(() => {});
  }

  // ── C. sources ─────────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 3, 3, ARMED, true);
    a.node._maxSynaptome = 20;
    const b = await makePeer(net, domain, 3, 4, {});
    const c = await makePeer(net, domain, 3, 5, {});
    await a.sim.openConnection(b.hex); await b.transport.openConnection(c.hex); await wait(40);
    check('C1 setup: a holds b (admitted), b holds c, a does not hold c', a.node.synaptome.has(b.big) && b.node.synaptome.has(c.big) && !a.node.synaptome.has(c.big) && !a.transport.isConnected(c.hex));
    await a.peer.findKClosest(c.big ^ 1n, 3);
    check('C1 a real lookup\'s find_closest_set responses NOMINATE: c is in a\'s cache as closest-set', a.peer._fillCache.get(c.big)?.src === 'closest-set', J([...a.peer._fillCache.entries()]));
    // C2 lookahead responses
    const X = stranger(a.big, 120), Y = stranger(a.big, 121);
    a.ctl.sendOverride = async (_to, type) => (type === 'lookahead_probe' ? { peerId: Y, latency: 5, terminal: false } : null);
    await a.peer._bestByTwoHopAP([{ peerId: X, latency: 10 }], 7n, a.big ^ 7n);
    check('C2 a lookahead response\'s forward peer is NOMINATED as lookahead', a.peer._fillCache.get(Y)?.src === 'lookahead', J([...a.peer._fillCache.keys()]));
    a.ctl.sendOverride = null;
    // C3 the directory sample through the transport's onPeerList (the handler is
    // installed at start, so this peer is built by hand on a transport that offers it)
    const idD = await createNodeIdentity({ lat: 3, lng: 8 }); const simD = simTransport({ network: net, identity: idD, heartbeatMs: 0 }); await simD.start(idD.id);
    const ctlD = { peerListCb: null };
    const tD = new Proxy(simD, { get(t, prop, r) { if (prop === 'onPeerList') return (h) => { ctlD.peerListCb = h; return () => {}; }; const v = Reflect.get(t, prop, r); return typeof v === 'function' ? v.bind(t) : v; } });
    const nodeD = new NeuronNode({ id: fromHex(idD.id), lat: 3, lng: 8 }); nodeD.transport = tD;
    const peerD = new AxonaPeer({ domain, node: nodeD, nodeIdentity: idD, transport: tD, ...ARMED }); peerD._requireAxonaManager('fence'); await peerD.start();
    if (peerD._maintainTimer) { clearInterval(peerD._maintainTimer); peerD._maintainTimer = null; }
    const logsD = []; const olD = peerD._emitLog.bind(peerD); peerD._emitLog = (l, m, c2) => { logsD.push([m, c2]); return olD(l, m, c2); };
    const bigD = fromHex(idD.id);
    check('C3 setup: the kernel installed an onPeerList handler on a transport that offers one', typeof ctlD.peerListCb === 'function');
    ctlD.peerListCb([toHex(stranger(bigD, 130)), toHex(stranger(bigD, 131)), idD.id, 'not-a-node-id']);
    check('C3 a peer-list is nominated as directory (two strangers; self and a bad id skipped), logged', peerD._fillCache.size === 2 && [...peerD._fillCache.values()].every(v => v.src === 'directory') && logsD.some(([m, c2]) => m === 'fill-directory-sample' && c2.offered === 4 && c2.nominated === 2), J({ size: peerD._fillCache.size, logs: logsD.filter(([m]) => m === 'fill-directory-sample') }));
    await peerD.stop().catch(() => {});
    for (const x of [a, b, c]) await x.peer.stop().catch(() => {});
    // C4 the REAL web transport factory: a bridge peer-list frame on its socket reaches
    // composite.onPeerList handlers (hex ids, a copy per handler) before the mesh's own
    // bootstrap dialing of it; composite.mayDial is a function and answers true with the
    // ledger not enforcing.
    {
      const { webTransport } = await import('../src/transport/web/index.js');
      class FakeWebSocket {
        constructor(url) { this.url = url; this.sent = []; this._l = new Map(); this.readyState = 0; queueMicrotask(() => { this.readyState = 1; this._fire('open'); }); }
        addEventListener(type, h) { if (!this._l.has(type)) this._l.set(type, new Set()); this._l.get(type).add(h); }
        send(data) { if (this.readyState !== 1) throw new Error('socket not open'); this.sent.push(data); }
        close() { this.readyState = 3; this._fire('close'); }
        _fire(type, ev = {}) { const s = this._l.get(type); if (s) for (const h of s) try { h(ev); } catch { /* */ } }
        _deliver(data) { this._fire('message', { data }); }
      }
      const alice = await createNodeIdentity({ lat: 3, lng: 9 });
      const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWebSocket, handshakeTimeoutMs: 1000 });
      check('C4 the real factory exposes onPeerList and mayDial', typeof t.onPeerList === 'function' && typeof t.mayDial === 'function' && t.mayDial() === true);
      const got = [];
      const unsub = t.onPeerList((peers) => got.push(peers));
      // The mesh's own bootstrap dialing of the list needs RTCPeerConnection, which Node
      // has not; its throw surfaces as a rejection AFTER the kernel handlers ran. Catch it
      // here so the fence observes the order and does not die on the mesh's dial.
      const rejections = []; const onRej = (e) => rejections.push(String(e?.message ?? e)); process.on('unhandledRejection', onRej);
      const sp = t.start().catch(() => {});
      await new Promise(r => queueMicrotask(r)); await wait(0);
      const h1 = toHex(stranger(fromHex(alice.id), 132)), h2 = toHex(stranger(fromHex(alice.id), 133));
      t.socket._deliver(JSON.stringify({ type: 'peer-list', peers: [h1, h2] }));
      await wait(0);
      check('C4 a peer-list frame on the bridge socket reached the kernel-facing handler with the hex ids (before the mesh dialed it)', got.length === 1 && Array.isArray(got[0]) && got[0].length === 2 && got[0][0] === h1 && got[0][1] === h2, J({ got, rejections }));
      unsub();
      t.socket._deliver(JSON.stringify({ type: 'peer-list', peers: [h1] }));
      await wait(0);
      check('C4 after unsubscribe the handler no longer fires', got.length === 1);
      await t.stop().catch(() => {}); await sp; await wait(0);
      process.off('unhandledRejection', onRej);
    }
  }

  // ── D. the tick on the sim ─────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 4, 4, ARMED, true);
    a.node._maxSynaptome = 11;
    a.peer.findKClosest = async () => [a.big];   // isolate: the near search finds nothing new
    const ss = [];
    for (let i = 0; i < 5; i++) { const s = await makePeer(net, domain, 40 + i, 40 + i, {}); ss.push(s); a.peer._nominateCandidate(s.big, 'near'); }
    check('D setup: five real strangers nominated, none held, table empty, cap 11', a.peer._fillCache.size === 5 && ss.every(s => !a.transport.isConnected(s.hex)) && a.node.synaptome.size === 0);
    const byDist = [...ss].sort((x, y) => ((x.big ^ a.big) < (y.big ^ a.big) ? -1 : 1)).map(s => s.big);
    const r1 = await tick(a); await wait(40);
    const rep1 = a.peer._fillLast;
    check('D1 the tick dialed maxPerTick (3), NEAREST-FIRST, each bound and admitted; return counts dials', r1 === 3 && rep1.dialed === 3 && rep1.state === 'filling' && a.ctl.opens.length === 3 && byDist.slice(0, 3).every(id => a.node.synaptome.has(id)) && a.peer._fillCache.size === 2, J({ r1, rep1, opens: a.ctl.opens.length, table: a.node.synaptome.size }));
    check('D1 synaptome-fill logged with the report', a.logs.some(([m, c]) => m === 'synaptome-fill' && c.dialed === 3 && c.cap === 11), J(a.logs.filter(([m]) => m === 'synaptome-fill')));
    const r2 = await tick(a); await wait(40);
    check('D2 the next tick dialed the remaining two; table 5; cache empty', r2 === 2 && a.node.synaptome.size === 5 && a.peer._fillCache.size === 0, J({ r2, table: a.node.synaptome.size, cache: a.peer._fillCache.size }));
    const r3 = await tick(a);
    check('D3 empty cache below cap, no directory held: fill-stalled: supply, logged with the condition; nothing dialed', r3 === 0 && a.peer._fillLast.state === 'fill-stalled:supply' && a.logs.some(([m, c]) => m === 'fill-stalled' && c.condition === 'supply') && a.ctl.opens.length === 5, J(a.peer._fillLast));
    await tick(a);
    check('D4 the same state next tick is NOT logged again (on change only)', logCount(a.logs, 'fill-stalled') === 1);
    // D5 at cap (case 36's observable in the fill): nothing opens, nothing leaves, nothing enters
    a.node._maxSynaptome = a.node.synaptome.size;
    const extra = await makePeer(net, domain, 50, 50, {}); a.peer._nominateCandidate(extra.big, 'near');
    const keys = new Set(a.node.synaptome.keys()); const opensBefore = a.ctl.opens.length;
    const r5 = await tick(a);
    check('D5 AT CAP: state at-cap, zero openConnection, zero deletes, zero inserts, the cache is left as it was', r5 === 0 && a.peer._fillLast.state === 'at-cap' && a.ctl.opens.length === opensBefore && a.node.synaptome.size === keys.size && [...keys].every(k => a.node.synaptome.has(k)) && a.peer._fillCache.has(extra.big), J(a.peer._fillLast));
    for (const x of [a, extra, ...ss]) await x.peer.stop().catch(() => {});
  }

  // ── E. reservation (case 45) ───────────────────────────────────────────
  {
    const net = new SimNetwork();
    const w = await makePeer(net, domain, 5, 5, ARMED, true);
    w.node._maxSynaptome = 20;
    w.peer.findKClosest = async () => [w.big];
    const g = w.peer._attemptGuard;
    let incN = 0; w.ctl.relayReturn = () => `inc-${++incN}`;   // every relay dial is HELD (web-shaped transport)
    w.ctl.openOverride = async () => false;                     // bound-only open: no binding
    const cands = [0, 1, 2, 3].map(i => stranger(w.big, 140 + i));
    for (const c of cands) w.peer._nominateCandidate(c, 'near');
    const r1 = await tick(w);
    const rep1 = w.peer._fillLast;
    const held = cands.filter(c => g.inflightOf(c)); const kept = cands.filter(c => w.peer._fillCache.has(c));
    check('E1 P_pending 2: two dials went out and are HELD; the third is DEFERRED in place (state filling, deferred 1)', r1 === 2 && rep1.dialed === 2 && rep1.deferred === 1 && rep1.state === 'filling' && held.length === 2 && kept.length === 2 && g.inflightCount() === 2, J({ r1, rep1, held: held.length, kept: kept.length }));
    check('E1 the deferred candidates: still NOMINATED, attempts 0, no token, no mark', kept.every(c => g.attemptsOf(c) === 0 && !g.inflightOf(c) && !w.node._deadPeers.has(c)));
    check('E1 dial-deferred logged with the pending count at the bound', w.logs.some(([m, c]) => m === 'dial-deferred' && c.pending === 2 && c.pPending === 2) && w.peer._fillStats.dialDeferred === 1);
    // a slot frees: the first held attempt's deadline
    w.ctl.connected = false; w.ctl.negCb(held[0], 'negotiation-timeout', 'inc-1'); w.ctl.connected = null;
    check('E2 setup: one slot freed (pending 1)', g.inflightCount() === 1 && !g.inflightOf(held[0]));
    const r2 = await tick(w);
    check('E2 after a slot frees, the next tick issues ONE more dial (held again at the bound), one still deferred', r2 === 1 && g.inflightCount() === 2 && w.peer._fillLast.dialed === 1 && w.peer._fillLast.deferred === 1 && w.peer._fillCache.size === 1, J(w.peer._fillLast));
    // the channel-token half
    w.ctl.negCb(held[1], 'negotiation-timeout', 'inc-2'); w.ctl.connected = null;
    w.ctl.mayDial = false;
    const r3 = await tick(w);
    check('E3 the ledger refuses an outbound channel: deferred without a dial, logged channel:false', r3 === 0 && w.peer._fillLast.deferred === 1 && w.peer._fillLast.dialed === 0 && w.logs.some(([m, c]) => m === 'dial-deferred' && c.channel === false), J(w.peer._fillLast));
    w.ctl.mayDial = null;
    await w.peer.stop().catch(() => {});
  }

  // ── F. liveness ────────────────────────────────────────────────────────
  {
    const net = new SimNetwork();
    // F1 case 7, no overlap: the directory is held and does not answer
    const r = await makePeer(net, domain, 6, 6, ARMED, true);
    r.node._maxSynaptome = 20;
    r.peer.findKClosest = async () => [r.big];
    let asked = 0; r.ctl.introductions = () => { asked++; return false; };   // socket closed: nothing sent
    const t0 = Date.now();
    const r1 = await tick(r);
    const rep = r.peer._fillLast;
    check('F1 no answer from the directory it holds, empty cache: fill-stalled: rendezvous, logged with the condition', r1 === 0 && asked === 1 && rep.directory.held && rep.directory.asked && rep.directory.ok === false && rep.state === 'fill-stalled:rendezvous' && r.logs.some(([m, c]) => m === 'fill-stalled' && c.condition === 'rendezvous'), J(rep));
    check('F1 nothing inferred from it: no guard attempt, no mark', r.peer._attemptGuard._state.size === 0 && r.node._deadPeers.size === 0 && r.peer._fillStats.directoryUnavailable === 1);
    const dt = r.peer._fillDirectoryNextAt - t0;
    check('F1 the re-contact timer drew in [T/2, 3T/2] (T 1000 ms)', dt >= 500 && dt <= 1500 + 50, String(dt));
    await tick(r);
    check('F1 before the timer is due a second tick does not re-ask; the state is unchanged and not re-logged', asked === 1 && logCount(r.logs, 'fill-stalled') === 1);
    // F2 case 7, overlap: the re-contact is answered with a sample; the same tick dials and binds it
    const q = await makePeer(net, domain, 60, 60, {});
    // this transport exposed no onPeerList at start; the bridge's answer is emulated at the
    // kernel's directory handler shape — the sample nominated as the transport would deliver it
    r.ctl.introductions = () => { asked++; r.peer._nominateCandidate(q.big, 'directory'); return true; };
    r.peer._fillDirectoryNextAt = 0;   // the timer is due
    const r2 = await tick(r); await wait(40);
    check('F2 the answered re-contact nominates the sample and the SAME tick dials it: one introduction, one bind, admitted', asked === 2 && r2 === 1 && r.peer._fillLast.state === 'filling' && r.node.synaptome.has(q.big) && r.transport.isConnected(q.hex), J({ asked, r2, rep: r.peer._fillLast, has: r.node.synaptome.has(q.big) }));
    // F3 fair retry: every candidate on its backoff schedule
    const f = await makePeer(net, domain, 7, 7, ARMED, true);
    f.node._maxSynaptome = 20;
    f.peer.findKClosest = async () => [f.big];
    const gf = f.peer._attemptGuard;
    const cs = [stranger(f.big, 150), stranger(f.big, 151)];
    for (const c of cs) { f.peer._nominateCandidate(c, 'near'); const k = gf.begin(c); gf.end(c, false, Date.now(), k); }   // one failed attempt each → backoff
    check('F3 setup: two nominated, both refused by the guard\'s backoff', f.peer._fillCache.size === 2 && cs.every(c => gf.allow(c) === false));
    const r3 = await tick(f);
    check('F3 all candidates on their schedule: fill-stalled: fair-retry, nothing dialed, both KEPT nominated', r3 === 0 && f.peer._fillLast.state === 'fill-stalled:fair-retry' && f.peer._fillLast.refused === 2 && f.peer._fillCache.size === 2 && f.ctl.opens.length === 0, J(f.peer._fillLast));
    for (const x of [r, q, f]) await x.peer.stop().catch(() => {});
  }

  // ── G. static ──────────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const s = src.indexOf('  async _fillTick('); const e = src.indexOf('\n  }\n', s); const body = src.slice(s, e);
    const i = (needle) => body.indexOf(needle);
    check('G _fillTick order: NEIGHBOURS (findKClosest) → DIRECTORY (_fillDirectoryStep) → DIAL', i('findKClosest') > 0 && i('findKClosest') < i('_fillDirectoryStep') && i('_fillDirectoryStep') < i('_considerCandidate'));
    check('G the reservation (inflightCount / _transportMayDial) sits BEFORE guard.allow and the dial', i('inflightCount()') < i('guard.allow(') && i('_transportMayDial()') < i('guard.allow(') && i('guard.allow(') < i('_considerCandidate'));
    check('G at cap the tick returns before the search', i("'at-cap'") < i('findKClosest'));
    const n = src.indexOf('  _nominateCandidate('); const nb = src.slice(n, src.indexOf('\n  }\n', n));
    check('G nominate is gated on the arm and consumes nothing', /^\s*_nominateCandidate\(id, src\) \{\n\s*if \(!this\._fillArmed\(\)\) return false;/.test(nb) && !/consume\(/.test(nb));
    check('G the arm requires the guard AND the gate', /_fillArmed\(\) \{\n\s*return !!\(this\._maintainCfg && this\._attemptGuard && this\._gateCfg\);/.test(src));
    check('G the tick branches to the fill only when armed, after the reconcile and the backoff gate', (() => { const t = src.indexOf('  async _maintainSynaptome('); const tb = src.slice(t, src.indexOf('\n  }\n', t)); return tb.indexOf('_reconcileBound()') < tb.indexOf('_deficitBackoff.allow()') && tb.indexOf('_deficitBackoff.allow()') < tb.indexOf('if (this._fillArmed()) return await this._fillTick(self);'); })());
    const isrc = readFileSync(new URL('../src/transport/web/index.js', import.meta.url), 'utf8');
    const pl = isrc.indexOf("case 'peer-list':"); const plb = isrc.slice(pl, isrc.indexOf("case 'peer-joined':", pl));
    check('G the web transport hands the peer-list to the kernel handlers BEFORE the mesh dials it', plb.indexOf('peerListHandlers') > 0 && plb.indexOf('peerListHandlers') < plb.indexOf('mesh.onPeerList(peers)'));
    check('G composite.mayDial is the ledger\'s pure predicate for an outbound channel', /composite\.mayDial = \(\) => \{[\s\S]*?canAllocate\('out'\)/.test(isrc));
    const lsrc = readFileSync(new URL('../src/transport/web/channel_ledger.js', import.meta.url), 'utf8');
    const c0 = lsrc.indexOf('  canAllocate(dir) {'); const cb = lsrc.slice(c0, lsrc.indexOf('\n  }\n', c0));
    check('G ledger.canAllocate counts nothing and logs nothing', c0 > 0 && !/_stats|_log\(/.test(cb));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
