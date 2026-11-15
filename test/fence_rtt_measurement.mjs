#!/usr/bin/env node
// =====================================================================
// fence_rtt_measurement.mjs — 4.148.0 (David 2026-11-15: "Start step 1, the
// round-trip measurement"). Hop Receipt and Removal v0.3 (axona-docs b5dac77),
// build step 1.
//
// WHAT WAS THERE. A receipt deadline must be derived from a round trip
// measured on the channel. The bridge and WebSocket transports answered
// getLatency() with the constant 50. The mesh answered with one unsmoothed
// sample, −1 until the first pong.
//
// WHAT THIS BUILD ADDS. A smoothed estimate per channel instance
// (transport/rtt.js) and a read-only getRtt on every transport.
//
// WHAT IT MUST NOT DO. Change a decision. getLatency answers exactly as it
// did, and nothing in the kernel reads an estimate to choose a hop, time a
// request or remove a peer. Section S pins that in the source.
//
//   E  the estimator: first sample, the smoothing arithmetic, refusals,
//      sources, reset, the cold answer
//   M  the mesh: the pinger measures, the ponger is told; cold before a
//      sample; an unknown peer; getLatency unchanged
//   B  the bridge socket: fed by its ping loop, cold on a new socket,
//      getLatency still 50
//   W  the node WebSocket transport: fed by its host, per peer, forgotten on
//      demand, bounded, getLatency still 50
//   C  the composite: the current route's answer; null when no route can say
//   P  the peer's snapshot and the contract default
//   S  static: no decision reads the measurement
//   T  the client's bridge ping carries its measured round trip, additively
// =====================================================================
import { readFileSync } from 'node:fs';
import { RttEstimator, RTT_SAMPLE_MAX_MS } from '../src/transport/rtt.js';
import { MeshManager }        from '../src/transport/web/mesh.js';
import { BridgeTransport }    from '../src/transport/web/bridge.js';
import { CompositeTransport } from '../src/transport/web/composite.js';
import { WebSocketTransport } from '../src/transport/node/wstransport.js';
import { Transport }          from '../src/contracts/Transport.js';
import { AxonaPeer }          from '../src/dht/AxonaPeer.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const J = (v) => JSON.stringify(v);
const near = (a, b) => Math.abs(a - b) < 1e-9;

(async () => {
  console.log('fence_rtt_measurement: a measured, smoothed round trip per channel, read by nothing');

  // ── E. the estimator ───────────────────────────────────────────────────
  {
    const e = new RttEstimator();
    const c = e.snapshot(1000);
    check('E1 a new estimator is cold: no figures, no age', e.cold === true && c.cold === true && c.srtt === null && c.rttvar === null && c.last === null && c.samples === 0 && c.ageMs === null, J(c));
    check('E2 the first sample sets srtt = R and rttvar = R/2', e.sample(100, { now: 1000 }) === true && e.srtt === 100 && e.rttvar === 50 && e.cold === false, J(e.snapshot(1000)));
    e.sample(200, { now: 1500 });
    // rttvar = 0.75·50 + 0.25·|100 − 200| = 62.5 ; srtt = 0.875·100 + 0.125·200 = 112.5
    check('E3 the second sample: rttvar = 3/4·rttvar + 1/4·|srtt − R|, computed from the OLD srtt', near(e.rttvar, 62.5), String(e.rttvar));
    check('E3b and srtt = 7/8·srtt + 1/8·R', near(e.srtt, 112.5), String(e.srtt));
    const s = e.snapshot(2000);
    check('E4 last, min, max, count and age are kept', s.last === 200 && s.min === 100 && s.max === 200 && s.samples === 2 && s.ageMs === 500, J(s));
    const before = J(e.snapshot(2000));
    const refused = [NaN, Infinity, -1, RTT_SAMPLE_MAX_MS + 1, '100', null, undefined].map((v) => e.sample(v, { now: 2000 }));
    const after = e.snapshot(2000);
    check('E5 a non-number, a negative and an over-cap sample are refused and counted', refused.every((r) => r === false) && after.rejected === 7, J({ refused, rejected: after.rejected }));
    check('E5b and a refused sample changes no figure', J({ ...after, rejected: 0 }) === before, J(after));
    check('E5c a sample exactly at the cap, and zero, are accepted', new RttEstimator().sample(RTT_SAMPLE_MAX_MS) === true && new RttEstimator().sample(0) === true);
    const o = new RttEstimator();
    o.sample(10, { source: 'own' }); o.sample(12, { source: 'reported' }); o.sample(11);
    check('E6 own and reported samples are counted apart; the default source is own', o.own === 2 && o.reported === 1 && o.samples === 3, J(o.snapshot()));
    o.reset();
    check('E7 reset returns it to cold', o.cold === true && o.srtt === null && o.samples === 0 && o.own === 0 && o.reported === 0 && o.rejected === 0 && o.lastAt === null, J(o.snapshot()));
    check('E8 the age is never negative when the reader\'s clock is behind', (() => { const x = new RttEstimator(); x.sample(5, { now: 5000 }); return x.snapshot(4000).ageMs === 0; })());
  }

  // ── M. the mesh ────────────────────────────────────────────────────────
  {
    const HB = { pingIntervalMs: 100, takeoverMs: 500, tiebreakMs: 100, staleMs: 2000, deadMs: 4000, tickMs: 25, reapMs: 50 };
    const DELAY = 20;
    const mk = () => ({ readyState: 'open', sent: [], onmessage: null, onopen: null, onclose: null, onerror: null, peer: null,
      send(data) { this.sent.push(JSON.parse(data)); const to = this.peer; setTimeout(() => { if (to.readyState === 'open' && to.onmessage) to.onmessage({ data }); }, DELAY); },
      close() { this.readyState = 'closed'; } });
    const a = mk(), b = mk(); a.peer = b; b.peer = a;
    const side = (role, peerId, dc) => {
      const mesh = new MeshManager({ sendSignal() {}, log() {}, heartbeat: HB });
      const st = mesh._newPeerState(peerId, role);
      mesh._peers.set(peerId, st);
      mesh._wireDataChannel(st, dc);
      st.state = 'open'; st.openedAt = Date.now(); st.dc = dc;
      return { mesh, st };
    };
    const A = side('offerer', 'peer-b', a), B = side('responder', 'peer-a', b);
    const cold = A.mesh.getRtt('peer-b');
    check('M1 a channel with no sample answers cold, with null figures', cold && cold.cold === true && cold.srtt === null && cold.samples === 0, J(cold));
    check('M1b and getLatency is −1, as before', A.mesh.getLatency('peer-b') === -1);
    check('M2 a peer the mesh does not hold answers null', A.mesh.getRtt('nobody') === null && A.mesh.getLatency('nobody') === -1);
    A.mesh._openHeartbeat(A.st); B.mesh._openHeartbeat(B.st);
    await wait(700);
    for (const s of [A, B]) { if (s.st.pingTimer) clearInterval(s.st.pingTimer); if (s.st.reaperTimer) clearInterval(s.st.reaperTimer); }
    const ra = A.mesh.getRtt('peer-b'), rb = B.mesh.getRtt('peer-a');
    check('M3 the PINGER measures: own samples, none reported', ra && ra.cold === false && ra.own >= 3 && ra.reported === 0, J(ra));
    check('M4 the PONGER is told: reported samples, none of its own', rb && rb.cold === false && rb.reported >= 2 && rb.own === 0, J(rb));
    check('M5 the smoothed figure is a round trip of the fake channel (two legs of 20 ms, timer slack allowed)', ra.srtt >= 2 * DELAY - 5 && ra.srtt < 2 * DELAY + 120 && ra.min >= 2 * DELAY - 5, J(ra));
    check('M6 getLatency still answers the LAST raw sample, not the smoothed one', A.mesh.getLatency('peer-b') === A.st.rttBuffer.at(-1) && A.mesh.getLatency('peer-b') === ra.last, J({ lat: A.mesh.getLatency('peer-b'), last: ra.last, srtt: ra.srtt }));
    check('M7 the estimate belongs to the channel instance: a new peer state starts cold', A.mesh._newPeerState('peer-b', 'offerer').rttEst.cold === true);
    const snap = A.mesh.getRtt('peer-b'); snap.srtt = -1;
    check('M8 a reader cannot change the estimate through the snapshot', A.mesh.getRtt('peer-b').srtt !== -1);
  }

  // ── B. the bridge socket ───────────────────────────────────────────────
  {
    const bt = new BridgeTransport({ localNodeId: 1n, sendToBridge() {}, isBridgeOpen: () => true, log() {} });
    const c = bt.getRtt(2n);
    check('B1 the bridge socket is cold until its ping loop feeds it', c && c.cold === true && c.srtt === null, J(c));
    check('B2 a fed sample is kept', bt.recordRtt(40, 1000) === true && bt.getRtt(2n).srtt === 40 && bt.getRtt(2n).samples === 1, J(bt.getRtt(2n)));
    check('B3 a bad sample is refused', bt.recordRtt(-3) === false && bt.getRtt(2n).samples === 1 && bt.getRtt(2n).rejected === 1);
    check('B4 getLatency still answers the constant 50', bt.getLatency(2n) === 50);
    bt.resetRtt();
    check('B5 a new socket starts cold', bt.getRtt(2n).cold === true && bt.getRtt(2n).samples === 0);
  }

  // ── W. the node WebSocket transport ────────────────────────────────────
  {
    const wt = new WebSocketTransport({ sendToConn: () => true, isConnOpen: () => true });
    check('W1 a peer the host never fed answers null', wt.getRtt(7n) === null);
    check('W2 the host feeds a sample for one peer', wt.recordRtt(7n, 30, 1000) === true && wt.getRtt(7n).srtt === 30 && wt.getRtt(8n) === null, J(wt.getRtt(7n)));
    wt.recordRtt(8n, 90);
    check('W3 estimates are per peer', wt.getRtt(7n).srtt === 30 && wt.getRtt(8n).srtt === 90);
    check('W4 getLatency still answers the constant 50', wt.getLatency(7n) === 50);
    check('W5 a forgotten peer answers null again; forgetting twice is harmless', wt.forgetRtt(7n) === true && wt.getRtt(7n) === null && wt.forgetRtt(7n) === false);
    const big = new WebSocketTransport({ sendToConn: () => true, isConnOpen: () => true });
    for (let i = 0; i < 4096; i++) big.recordRtt(BigInt(i), 1);
    check('W6 the table is bounded: peer 4097 is refused, a held peer still samples', big.recordRtt(99999n, 1) === false && big.getRtt(99999n) === null && big.recordRtt(5n, 2) === true && big._rttByNodeId.size === 4096);
    const none = new WebSocketTransport({ sendToConn: () => true, isConnOpen: () => true });
    check('W7 forgetting on a transport that never sampled is harmless', none.forgetRtt(1n) === false && none.getRtt(1n) === null);
  }

  // ── C. the composite ───────────────────────────────────────────────────
  {
    const answer = { cold: false, srtt: 12 };
    const call = (route) => CompositeTransport.prototype.getRtt.call({ _routeFor: () => route }, 5n);
    check('C1 the composite answers with the CURRENT route\'s estimate', call({ getRtt: (id) => (id === 5n ? answer : null) }) === answer);
    check('C2 no route: null', call(null) === null);
    check('C3 a route that cannot measure: null', call({ getLatency: () => 50 }) === null);
    check('C4 a route whose getRtt throws: null', call({ getRtt() { throw new Error('boom'); } }) === null);
    check('C5 a route that answers null stays null: no figure is invented', call({ getRtt: () => null }) === null);
  }

  // ── P. the peer's snapshot and the contract default ────────────────────
  {
    check('P1 the contract default is null: a transport that does not measure says so', new Transport().getRtt(1n) === null);
    const stub = (transport, peers) => ({ _node: { transport }, _usablePeers: () => peers });
    const snap = AxonaPeer.prototype.rttSnapshot.call(stub({ getRtt: (id) => (id === 2n ? { cold: false, srtt: 9 } : null) }, [{ id: 2n, cls: 'M' }, { id: 3n, cls: 'I' }]));
    check('P2 the peer lists each usable peer with its transport\'s answer', snap.length === 2 && snap[0].rtt?.srtt === 9 && snap[0].cls === 'M' && snap[1].rtt === null && typeof snap[0].id === 'string' && snap[0].id.length === 12, J(snap));
    check('P3 a transport with no getRtt gives an empty list', AxonaPeer.prototype.rttSnapshot.call(stub({}, [{ id: 2n }])).length === 0);
    check('P4 the list is bounded', AxonaPeer.prototype.rttSnapshot.call(stub({ getRtt: () => null }, Array.from({ length: 100 }, (_, i) => ({ id: BigInt(i + 1) }))), 10).length === 10);
    check('P5 a getRtt that throws reads as null, a _usablePeers that throws as no peers', AxonaPeer.prototype.rttSnapshot.call(stub({ getRtt() { throw new Error('x'); } }, [{ id: 2n }]))[0].rtt === null
      && AxonaPeer.prototype.rttSnapshot.call({ _node: { transport: { getRtt: () => null } }, _usablePeers() { throw new Error('y'); } }).length === 0);
  }

  // ── S. static: no decision reads the measurement ───────────────────────
  {
    const src = (f) => readFileSync(new URL('../src/' + f, import.meta.url), 'utf8');
    const peer = src('dht/AxonaPeer.js');
    const uses = (peer.match(/\.getRtt\(/g) || []).length;
    check('S1 AxonaPeer calls getRtt in ONE place, the snapshot', uses === 1 && /rttSnapshot\(max = 64\) \{[\s\S]{0,700}t\.getRtt\(p\.id\)/.test(peer), String(uses));
    check('S2 nothing in the kernel calls rttSnapshot', !/rttSnapshot\(/.test(peer.replace('rttSnapshot(max = 64) {', '')) && ['pubsub/AxonaManager.js', 'pubsub/repairPlane.js', 'pubsub/wireHandlers.js', 'transport/web/index.js', 'transport/node/index.js'].every((f) => !/rttSnapshot|\.getRtt\(/.test(src(f))));
    check('S3 the bridge and node WebSocket transports still answer getLatency with 50', /getLatency\(_nodeId\) \{ return 50; \}/.test(src('transport/web/bridge.js')) && /getLatency\(_nodeId\) \{ return 50; \}/.test(src('transport/node/wstransport.js')));
    const mesh = src('transport/web/mesh.js');
    check('S4 the mesh getLatency still reads the raw buffer', /getLatency\(peerId\) \{\s+const state = this\._peers\.get\(peerId\);\s+if \(!state\) return -1;\s+const last = state\.rttBuffer\.at\(-1\);/.test(mesh));
    check('S5 the mesh reads rttEst only to sample and to snapshot', (mesh.match(/rttEst/g) || []).length === 5 && (mesh.match(/rttEst\?\.sample\(/g) || []).length === 2, String((mesh.match(/rttEst/g) || []).length));
    check('S6 the estimator file computes no timeout', !/rto|timeout|deadline\s*=/i.test(src('transport/rtt.js').replace(/\/\/.*$/gm, '')));
    check('S7 the bridge ping loop feeds the bridge transport and a welcome resets it', /bridge\.recordRtt\(lastRtt\)/.test(src('transport/web/index.js')) && /bridge\.resetRtt\(\)/.test(src('transport/web/index.js')));
  }

  // ── T. the client tells the bridge ─────────────────────────────────────
  {
    const idx = readFileSync(new URL('../src/transport/web/index.js', import.meta.url), 'utf8');
    check('T1 the bridge ping carries the round trip measured on THIS socket, only once there is one', /bridgeBound: bridgeChannelBound\(bridgeNodeIdBig\), \.\.\.\(typeof socketRtt === 'number' \? \{ rtt: socketRtt \} : \{\}\) \}\)\);/.test(idx));
    check('T2 socketRtt starts null and is set in exactly two places: from a pong, and back to null on a welcome', /let socketRtt\s+= null;/.test(idx) && /socketRtt = lastRtt;\s+try \{ bridge\.recordRtt\(lastRtt\)/.test(idx) && /socketRtt = null;\s+try \{ bridge\.resetRtt\(\)/.test(idx) && (idx.match(/socketRtt = /g) || []).length === 2, String((idx.match(/socketRtt = /g) || []).length));
    check('T3 the older lastRtt, which a reconnect does not clear, is not what the ping carries', !/rtt: lastRtt/.test(idx));
  }

  console.log(`\nfence_rtt_measurement: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(2); });
