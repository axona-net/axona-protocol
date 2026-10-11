// =====================================================================
// fence_route_path_memory.mjs — R3 (and the R2 via commitment) of
// Routed-Walk-Terminals v0.5 (axona-docs 10f43fc), on REAL AxonaPeers over
// the sim transport.
//
// The defect: the lookahead's first hop need not be closer than the node
// holding the message, and the frame carried no memory of where it had been.
// Two nodes each "closer" by the other's lookahead traded the frame to
// MAX_HOPS (40): 28 of 50 walks toward axona.chat on 2026-10-11, 1.8 s each.
//
// R3: route_msg carries `path`, the last PATH_MEMORY node ids; greedy, the
// lookahead and the via commitment all choose from OUTSIDE the path.
// R2 via: the lookahead reply names the two-hop node; the forward carries it;
// the receiving hop forwards there first when it is connected and off-path.
//
// TOPOLOGY (ids are minted, so roles are assigned by XOR distance to T):
//   D = closest to T (T = D ^ 1), then B, A, C by distance.
//   channels: A–B, A–C (and A–D only in the via-honoured case).
//   A's synaptome lists D whether or not A holds a channel to D.
//
// Case 1 (loop, origin C):  C → A (greedy) → B (greedy). At B nothing
//   connected is closer. Pre-R3, B's lookahead probes A, A names D (closer),
//   B forwards to A with via D, D is unconnected at A, A's greedy picks B
//   again — and so on to the cap. With R3, A is on B's path: B probes nothing
//   and is terminal at hop 2.
// Case 2 (via unavailable, origin B): B's lookahead probes A, A names D,
//   B forwards to A via D; D unconnected → viaUnavailable; A's greedy excludes
//   B (on path), nothing closer; A's lookahead probes C only → terminal at A.
// Case 3 (via honoured, origin B, A–D connected): same start; A honours via
//   and forwards to D; D is terminal at hop 2; A counts viaHonoured.
//
// Mutant run by hand on 2026-10-11: delete the three `exclude.has(...)` lines
// in the route_msg handler → case 1 runs to hops 39 `exhausted`.
//
// Run: node test/fence_route_path_memory.mjs
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
const wait  = (ms) => new Promise((r) => setTimeout(r, ms));
const short = (b) => (b == null ? 'null' : b.toString(16).padStart(66, '0').slice(0, 8));

async function makePeer(net, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
async function build({ connectAD }) {
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 300 });
  const raw = [];
  for (let i = 0; i < 4; i++) raw.push(await makePeer(net, domain, 40 + i * 0.01, -74));
  const D = raw[0];
  const T = D.big ^ 1n;
  const rest = raw.slice(1).sort((x, y) => ((x.big ^ T) < (y.big ^ T) ? -1 : 1));
  const [B, A, C] = rest;
  // channels
  await A.transport.openConnection(B.hex);
  await A.transport.openConnection(C.hex);
  if (connectAD) await A.transport.openConnection(D.hex);
  await wait(20);
  // synaptomes (the crafted routing picture)
  synapse(A, B); synapse(A, C); synapse(A, D);
  synapse(B, A);
  synapse(C, A);
  return { net, domain, T, A, B, C, D, all: raw };
}
const LS = (p) => p.peer.lookaheadStats();
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }

(async () => {
  console.log('fence_route_path_memory: R3 path memory + R2 via commitment on real peers');

  // ── Case 1: the loop ────────────────────────────────────────────────────
  {
    const w = await build({ connectAD: false });
    const { T, A, B, C } = w;
    check('1 setup: D is closest, then B, then A, then C',
      (w.D.big ^ T) < (B.big ^ T) && (B.big ^ T) < (A.big ^ T) && (A.big ^ T) < (C.big ^ T));
    check('1 setup: A holds channels to B and C, none to D',
      A.transport.isConnected(B.hex) && A.transport.isConnected(C.hex) && !A.transport.isConnected(w.D.hex));
    const t0 = Date.now();
    const r = await C.peer.routeMessage(T, 'fence:noop', {});
    const dt = Date.now() - t0;
    check('1 the walk ends terminal at B in 2 hops (C→A→B), not at the cap',
      r && r.terminal === true && r.atNode === B.big && r.hops === 2, `r=${JSON.stringify({ ...r, atNode: short(r?.atNode) })} dt=${dt}ms`);
    check('1 the walk took well under one request timeout', dt < 2000, `${dt}ms`);
    const lb = LS(B);
    check('1 at B the on-path node A was skipped by the lookahead, nothing probed', lb.probesSkippedOnPath >= 1 && lb.probesEmitted === 0,
      `skippedOnPath=${lb.probesSkippedOnPath} emitted=${lb.probesEmitted}`);
    await stopAll(w);
  }

  // ── Case 2: via unavailable ─────────────────────────────────────────────
  {
    const w = await build({ connectAD: false });
    const { T, A, B, C } = w;
    const r = await B.peer.routeMessage(T, 'fence:noop', {});
    check('2 origin B: lookahead names D through A; A cannot reach D; walk ends terminal at A in 1 hop',
      r && r.terminal === true && r.atNode === A.big && r.hops === 1, JSON.stringify({ ...r, atNode: short(r?.atNode) }));
    const la = LS(A), lb = LS(B);
    check('2 B answered by probe with a closer two-hop node', lb.answeredByProbe === 1 && lb.probesCloserThanMe === 1, `byProbe=${lb.answeredByProbe}`);
    check('2 A counted the via it could not honour', la.viaUnavailable === 1 && la.viaHonoured === 0, `unavail=${la.viaUnavailable} honoured=${la.viaHonoured}`);
    check('2 A\'s own lookahead skipped B (on path) and probed only C', la.probesSkippedOnPath >= 1 && la.probesEmitted === 1, `skipped=${la.probesSkippedOnPath} emitted=${la.probesEmitted}`);
    await stopAll(w);
  }

  // ── Case 3: via honoured ────────────────────────────────────────────────
  {
    const w = await build({ connectAD: true });
    const { T, A, B, D } = w;
    const r = await B.peer.routeMessage(T, 'fence:noop', {});
    check('3 origin B: A honours via and the walk ends terminal at D (the closest node) in 2 hops',
      r && r.terminal === true && r.atNode === D.big && r.hops === 2, JSON.stringify({ ...r, atNode: short(r?.atNode) }));
    const la = LS(A);
    check('3 A counted the honoured via and ran no selection of its own', la.viaHonoured === 1 && la.viaUnavailable === 0 && la.probesEmitted === 0,
      `honoured=${la.viaHonoured} unavail=${la.viaUnavailable} emitted=${la.probesEmitted}`);
    await stopAll(w);
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
