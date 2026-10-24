// =====================================================================
// fence_lookup_reselect.mjs — B1 of Mesh Signaling Route First v0.1 (axona-docs
// 7cb47a7; David 2026-10-24 "Ship B1 first"). A chosen next hop is TENTATIVE
// until the send issues.
//
// 4.124.0 window, 34 lookups on our relays: the step chose a hop from the
// table, awaited the two-hop lookahead (up to 1.5 s), the hop died and was
// evicted during the await, the step read isConnected false and SENT ANYWAY,
// and the whole lookup ended send-failed with a second candidate still in the
// set. The same shape sits in the route_msg forwarder and routeMessage's
// origin send.
//
// Real AxonaPeers over the sim transport, bound-only (no dial, no send without
// a channel), as the web transport behaves. The death is FORCED, not timed: a
// wrapper around the awaited stage kills the chosen hop's channel the moment
// the stage returns, so the step resumes with its choice already evicted.
//
//   L0 OFF control: the lookup ends send-failed at hop 1, connected:false, next
//      = the dead hop, and the dead hop is already charged into the path (the
//      4.124.0 record, reproduced).
//   L1 ON: the same race is survived. Found, path O → survivor → T, the dead
//      hop in neither path nor hops, and ZERO sends to the dead hop (the
//      re-check, not the catch, saved the send).
//   L2 ON, validation-then-loss: the channel dies between the liveness read
//      and the send; the send throws an edge loss; the step re-selects and the
//      target is found. Run on the acked walk and on the recursive step.
//   L3 OFF control of L2: send-failed.
//   R1 the route_msg FORWARDER: its chosen hop dies inside _deliverRouted;
//      ON forwards to the other live hop and the target consumes, with zero
//      sends to the dead hop; OFF ends exhausted.
//   R2 the routeMessage ORIGIN: same, at hop 0.
//   D  the default is OFF; env LOOKUP_RESELECT=1 arms it; an explicit option
//      wins over the env; LOOKUP_RESELECT=0 is a control arm.
//
// Mutants: drop the post-await re-check (sends to the dead hop → L1, R1, R2);
// drop the catch re-selection → L2; charge at selection → L1's path; flip the
// default → D.
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
const J = (o) => JSON.stringify(o, (_k, v) => (typeof v === 'bigint' ? toHex(v).slice(0, 12) : v));
const EXPECTED = 14;
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECTED) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECTED} checks`); process.exitCode = 1; } });

function boundOnly(t) {
  t.openConnection = async (id) => t.isConnected(id);
  const send = t.send.bind(t);
  t.sentTo = new Map();                                   // recipient hex → count of lookup_*/route_msg sends
  t.send = async (id, type, body) => {
    // Only the walk's and the route's own sends count; the lookahead probes go out before any kill.
    if (/^lookup_(walk|step)$|^route_msg$/.test(type)) { const k = typeof id === 'bigint' ? toHex(id) : String(id); t.sentTo.set(k, (t.sentTo.get(k) ?? 0) + 1); }
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
// Peers sorted by XOR distance to the target: [T, m1, m2, …, O]. `links` are
// pairs of indexes into that order.
async function build({ reselect, n, links, walk = true }) {
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0, LOOKUP_WALK: walk, LOOKUP_RESELECT: reselect });
  const raw = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }));
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    peer._routedHandlers.set('fence:route', (_payload, meta) => (meta.targetId === node.id ? 'consumed' : null));
    raw.push({ peer, transport, node, big: fromHex(id.id), hex: id.id });
  }
  const T = raw[0];
  const nodes = [T, ...raw.slice(1).sort((x, y) => ((x.big ^ T.big) < (y.big ^ T.big) ? -1 : 1))];
  const openRaw = async (x, y) => { await Object.getPrototypeOf(x.transport).openConnection.call(x.transport, y.hex); };
  for (const [i, j] of links) { await openRaw(nodes[i], nodes[j]); synapse(nodes[i], nodes[j]); synapse(nodes[j], nodes[i]); }
  await sleep(20);
  return { T, nodes, all: raw };
}
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
const sends = (from, to) => from.transport.sentTo.get(to.hex) ?? 0;
const hasId = (path, p) => Array.isArray(path) && path.some((x) => (typeof x === 'bigint' ? x : fromHex(String(x))) === p.big);
// The hop a greedy chooser at `from` takes toward T among `cands`: the XOR-closest.
const closestTo = (T, cands) => cands.reduce((a, b) => (((a.big ^ T.big) < (b.big ^ T.big)) ? a : b));

// Star: O links to A and B, both link to T. O's candidates for T are A and B.
const STAR = { n: 4, links: [[3, 1], [3, 2], [1, 0], [2, 0]] };
// Line-then-star: O → M → {A, B} → T. M is the forwarder with two live hops.
const VIA_M = { n: 5, links: [[4, 3], [3, 1], [3, 2], [1, 0], [2, 0]] };

// Kill `victim`'s channel to `at` from the victim's side: `at` sees onPeerDied,
// evicts the entry, and its transport answers isConnected(victim) false.
const killEdge = async (victim, at) => { await victim.transport.closeConnection(at.hex); await sleep(5); };

(async () => {
  console.log('fence_lookup_reselect: a chosen hop is tentative until the send issues');

  // L0 / L1: the lookahead returns, the chosen hop dies, the step resumes.
  for (const reselect of [false, true]) {
    const w = await build({ reselect, ...STAR });
    const [T, A, B, O] = w.nodes;
    let victim = null;
    const orig = O.peer._bestByTwoHopAP.bind(O.peer);
    O.peer._bestByTwoHopAP = async (...args) => { const r = await orig(...args); victim = r.peerId === A.big ? A : B; await killEdge(victim, O); return r; };
    const r = await O.peer.lookup(T.big);
    const survivor = victim === A ? B : A;
    if (!reselect) {
      check('L0 OFF: the step sends to the hop that died in the await — send-failed at hop 1, connected:false, next = the dead hop',
        r?.found === false && r.reason === 'send-failed' && r.fail?.hop === 1 && r.fail?.connected === false && r.fail?.next === victim.hex.slice(0, 12),
        J({ f: r?.found, r: r?.reason, fail: r?.fail }));
      check('L0 OFF: … and the dead hop was already charged into the path (charge at selection)',
        hasId(r?.path, victim) && sends(O, victim) === 1, J({ path: r?.path?.length, sentToVictim: sends(O, victim) }));
    } else {
      check('L1 ON: the same race is survived — found through the other candidate',
        r?.found === true && r.reason === 'found', J({ f: r?.found, r: r?.reason, fail: r?.fail }));
      check('L1 ON: the dead hop is in neither path nor hops; path is O → survivor → T (charge at issue)',
        !hasId(r?.path, victim) && hasId(r?.path, survivor) && r?.path?.length === 3 && r?.hops === 2, J({ path: r?.path?.length, hops: r?.hops, victimInPath: hasId(r?.path, victim) }));
      check('L1 ON: ZERO sends to the dead hop — the post-await re-check saved the send, not the catch',
        sends(O, victim) === 0, `sentToVictim=${sends(O, victim)}`);
    }
    await stopAll(w);
  }

  // L2 / L3: the channel dies between the liveness read and the send; L2 on both the acked walk and the recursive step.
  for (const [reselect, walk] of [[true, true], [true, false], [false, true]]) {
    const w = await build({ reselect, walk, ...STAR });
    const [T, A, B, O] = w.nodes;
    let victim = null;
    const send = O.transport.send;
    O.transport.send = async (id, type, body) => {
      if (victim === null && /^lookup_(walk|step)$/.test(type)) { victim = (typeof id === 'bigint' ? toHex(id) : String(id)) === A.hex ? A : B; await killEdge(victim, O); }
      return send(id, type, body);
    };
    const r = await O.peer.lookup(T.big);
    if (reselect) {
      check(`L2 ON (${walk ? 'acked walk' : 'recursive step'}): a channel that dies between the liveness read and the send throws an edge loss; the step re-selects and the target is found`,
        r?.found === true && victim !== null && !hasId(r?.path, victim), J({ f: r?.found, r: r?.reason, fail: r?.fail, victimInPath: hasId(r?.path, victim) }));
    } else {
      check('L3 OFF control of L2: send-failed at hop 1 with the edge-loss error',
        r?.found === false && r.reason === 'send-failed' && r.fail?.connected === true && /Transport\.send: peer|not connected|no open channel/i.test(String(r.fail?.err)), J({ f: r?.found, r: r?.reason, connected: r?.fail?.connected, err: r?.fail?.err }));
    }
    await stopAll(w);
  }

  // R1: the route_msg FORWARDER at M. Its chosen hop dies inside _deliverRouted.
  for (const reselect of [false, true]) {
    const w = await build({ reselect, ...VIA_M });
    const [T, A, B, M, O] = w.nodes;
    const victim = closestTo(T, [A, B]), survivor = victim === A ? B : A;
    const orig = M.peer._deliverRouted.bind(M.peer);
    M.peer._deliverRouted = async (type, payload, meta) => { const r = await orig(type, payload, meta); if (type === 'fence:route') await killEdge(victim, M); return r; };
    const r = await O.peer.routeMessage(T.big, 'fence:route', { k: 1 });
    if (!reselect) {
      check('R1 OFF: the forwarder sends to the hop that died in _deliverRouted — exhausted',
        r?.consumed === false && r?.exhausted === true && sends(M, victim) === 1, J({ r, sentToVictim: sends(M, victim) }));
    } else {
      check('R1 ON: the forwarder takes the other live hop; the target consumes; ZERO sends to the dead hop',
        r?.consumed === true && sends(M, survivor) === 1 && sends(M, victim) === 0, J({ r, toSurvivor: sends(M, survivor), toVictim: sends(M, victim) }));
    }
    await stopAll(w);
  }

  // R2: the routeMessage ORIGIN at O. Its chosen hop dies inside _deliverRouted (hop 0).
  for (const reselect of [false, true]) {
    const w = await build({ reselect, ...STAR });
    const [T, A, B, O] = w.nodes;
    const victim = closestTo(T, [A, B]), survivor = victim === A ? B : A;
    const orig = O.peer._deliverRouted.bind(O.peer);
    O.peer._deliverRouted = async (type, payload, meta) => { const r = await orig(type, payload, meta); if (type === 'fence:route' && meta.hopCount === 0) await killEdge(victim, O); return r; };
    const r = await O.peer.routeMessage(T.big, 'fence:route', { k: 2 });
    if (!reselect) {
      check('R2 OFF: the origin sends to the hop that died — exhausted',
        r?.consumed === false && r?.exhausted === true && sends(O, victim) === 1, J({ r, sentToVictim: sends(O, victim) }));
    } else {
      check('R2 ON: the origin takes the other live hop; the target consumes; ZERO sends to the dead hop',
        r?.consumed === true && sends(O, survivor) === 1 && sends(O, victim) === 0, J({ r, toSurvivor: sends(O, survivor), toVictim: sends(O, victim) }));
    }
    await stopAll(w);
  }

  // D: default OFF; env arms; an explicit option wins.
  {
    const had = Object.prototype.hasOwnProperty.call(process.env, 'LOOKUP_RESELECT'), prev = process.env.LOOKUP_RESELECT;
    delete process.env.LOOKUP_RESELECT;
    const d0 = new AxonaDomain().LOOKUP_RESELECT;
    process.env.LOOKUP_RESELECT = '1';
    const d1 = new AxonaDomain().LOOKUP_RESELECT;
    const d2 = new AxonaDomain({ LOOKUP_RESELECT: false }).LOOKUP_RESELECT;
    process.env.LOOKUP_RESELECT = '0';
    const d3 = new AxonaDomain().LOOKUP_RESELECT;
    if (had) process.env.LOOKUP_RESELECT = prev; else delete process.env.LOOKUP_RESELECT;
    check('D the default is OFF', d0 === false, String(d0));
    check('D env LOOKUP_RESELECT=1 arms it; an explicit option wins over the env; LOOKUP_RESELECT=0 is a control arm', d1 === true && d2 === false && d3 === false, `${d1} ${d2} ${d3}`);
  }

  if (passed + failed !== EXPECTED) { console.log(`  ✗ ran ${passed + failed} of ${EXPECTED} checks`); failed++; }
  console.log(`\nfence_lookup_reselect: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
