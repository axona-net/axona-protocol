// =====================================================================
// fence_lookup_walk.mjs — 4.123.0 (David 2026-10-23, "Make the lookup iterative,
// then roll it"). 4.122.0 showed the recursive lookup_step walk failing on its
// first hop with "timeout awaiting lookup_step" (234 of 303 failed sends, median
// 5,090 ms): every hop waited for the whole rest of the walk, and the
// originator's single request timeout had to cover all of it.
//
// The acked walk: each hop takes the walk, ACKs its caller at once, and forwards
// on its own; the result returns as lookup_done along the path; the originator
// gives the whole walk LOOKUP_WALK_DEADLINE_MS. Real AxonaPeers over the sim
// transport, bound-only (no dial, no send without a channel), as the web
// transport behaves. Chain O → H1 → H2 → H3 → H4 → T, each hop slowed by
// SLOW_MS, request timeout TIMEOUT_MS < 4 × SLOW_MS.
//
//   W0 control: LOOKUP_WALK false (the recursive walk) FAILS on this chain with
//      a send timeout at hop 1 — the 4.122.0 failure, reproduced.
//   W1 LOOKUP_WALK true: the same chain is FOUND, the full path comes back.
//   W2 a hop whose next hop has NO lookup_walk handler (an older kernel) falls
//      back to the recursive lookup_step for that hop, and the walk is found.
//   W3 a hop that ACKs and never finishes: the originator ends at the walk
//      deadline with reason walk-deadline, not at the first request timeout.
//   W4 a send failure deep in the walk returns to the originator with the
//      failing hop's facts (reason send-failed, at = that hop).
//
// Mutants: set LOOKUP_WALK's default false → W1; drop the legacy fallback → W2;
// drop the originator's deadline settle → W3 (hangs to the alarm); stop
// lookup_done forwarding to the predecessor → W1.
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
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const TIMEOUT_MS = 300, SLOW_MS = 120;

function boundOnly(t) {
  t.openConnection = async (id) => t.isConnected(id);
  const send = t.send.bind(t);
  t.send = async (id, type, body) => {
    if (!t.isConnected(id)) throw new Error(`Transport.send: peer ${String(id)} not connected`);
    return send(id, type, body);
  };
  return t;
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
function slow(p, ms) {
  for (const type of ['lookup_step', 'lookup_walk']) {
    const h = p.transport._reqHandlers.get(type);
    if (h) p.transport._reqHandlers.set(type, async (from, payload) => { await sleep(ms); return h(from, payload); });
  }
}
async function build({ walk, deadlineMs = 3000, slowMs = SLOW_MS }) {
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK: walk, LOOKUP_WALK_DEADLINE_MS: deadlineMs });
  const raw = [];
  for (let i = 0; i < 6; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: TIMEOUT_MS }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    raw.push({ peer, transport, node, big: fromHex(id.id), hex: id.id });
  }
  const T = raw[0];
  const chain = raw.slice(1).sort((x, y) => ((x.big ^ T.big) > (y.big ^ T.big) ? -1 : 1));   // farthest first
  const nodes = [...chain, T];                                                                  // O … H4, T
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (let i = 0; i + 1 < nodes.length; i++) { await openRaw(nodes[i], nodes[i + 1]); synapse(nodes[i], nodes[i + 1]); synapse(nodes[i + 1], nodes[i]); }
  await sleep(20);
  if (slowMs) for (const p of nodes) slow(p, slowMs);
  return { O: nodes[0], T, nodes, all: raw };
}
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
const at = (p) => p.hex.slice(0, 12);

// An early exit with the loop drained must not read as a pass.
process.on('exit', (code) => { if (code === 0 && passed + failed < 9) { console.log(`  ✗ exited after ${passed + failed} of 9 checks`); process.exitCode = 1; } });

(async () => {
  console.log('fence_lookup_walk: the acked walk survives a walk longer than one request timeout');

  // W0 control: recursive
  {
    const w = await build({ walk: false });
    const r = await w.O.peer.lookup(w.T.big);
    check('W0 control: the RECURSIVE walk fails this chain with a send timeout at hop 1 (the 4.122.0 failure)',
      r?.found === false && r.reason === 'send-failed' && r.fail?.hop === 1 && /time/i.test(String(r.fail?.err)), JSON.stringify({ f: r?.found, r: r?.reason, hop: r?.fail?.hop, err: r?.fail?.err }));
    await stopAll(w);
  }

  // W1 acked walk
  {
    const w = await build({ walk: true });
    const t0 = Date.now();
    const r = await w.O.peer.lookup(w.T.big);
    const dt = Date.now() - t0;
    check('W1 the ACKED walk finds the target on the same chain', r?.found === true && r.reason === 'found', JSON.stringify({ f: r?.found, r: r?.reason, fail: r?.fail }));
    check('W1 … and took longer than one request timeout (it could not have under the recursive walk)', dt > TIMEOUT_MS, `${dt}ms`);
    check('W1 … with the full path O → … → T returned to the originator', Array.isArray(r?.path) && r.path.length === 6, `path=${r?.path?.length}`);
    await stopAll(w);
  }

  // W2 legacy next hop
  {
    const w = await build({ walk: true, slowMs: 0 });
    const legacy = w.nodes[3];
    legacy.transport._reqHandlers.delete('lookup_walk');
    const r = await w.O.peer.lookup(w.T.big);
    check('W2 a next hop with no lookup_walk handler (older kernel) → recursive lookup_step for that hop, walk found', r?.found === true, JSON.stringify({ f: r?.found, r: r?.reason, fail: r?.fail }));
    await stopAll(w);
  }

  // W3 a hop that ACKs and never finishes
  {
    const w = await build({ walk: true, deadlineMs: 500, slowMs: 0 });
    w.nodes[2].transport._reqHandlers.set('lookup_walk', async () => ({ ack: true }));
    const t0 = Date.now();
    const r = await w.O.peer.lookup(w.T.big);
    const dt = Date.now() - t0;
    check('W3 a hop that takes the walk and drops it → reason walk-deadline at the originator', r?.found === false && r.reason === 'walk-deadline', JSON.stringify({ f: r?.found, r: r?.reason }));
    check('W3 … after the walk deadline, not the first request timeout', dt >= 450 && dt < 2000, `${dt}ms`);
    await stopAll(w);
  }

  // W4 send failure deep in the walk comes back
  {
    const w = await build({ walk: true, slowMs: 0 });
    const h3 = w.nodes[3], h4 = w.nodes[4];
    h3.node.synaptome.delete(h4.big);                 // H3 keeps a stale entry instead: the target itself, no channel
    synapse(h3, w.T);
    const r = await w.O.peer.lookup(w.T.big);
    check('W4 a send failure at H3 returns to the originator: send-failed, at = H3, next = T, walk = true',
      r?.found === false && r.reason === 'send-failed' && r.fail?.at === at(h3) && r.fail?.next === at(w.T) && r.fail?.walk === true, JSON.stringify({ r: r?.reason, fail: r?.fail }));
    await stopAll(w);
  }

  // W5 the default
  { const d = new AxonaDomain(); check('W5 the acked walk is the DEFAULT (LOOKUP_WALK true, deadline 12 000 ms)', d.LOOKUP_WALK === true && d.LOOKUP_WALK_DEADLINE_MS === 12000, `${d.LOOKUP_WALK} ${d.LOOKUP_WALK_DEADLINE_MS}`); }

  const EXPECTED = 9;   // every check above must have RUN; an early exit is a failure
  if (passed + failed !== EXPECTED) { console.log(`  ✗ ran ${passed + failed} of ${EXPECTED} checks`); failed++; }
  console.log(`\nfence_lookup_walk: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
