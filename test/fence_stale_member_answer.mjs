// =====================================================================
// fence_stale_member_answer.mjs — 4.144.0 (David 2026-11-14: "add tests to the
// system as necessary to see why it is failing - if the residue is causing it
// or some other factor ... a perfectly running protocol interacting with a
// similar state in the future may fail in the same way.")
//
// THE FIELD STATE (testnet, run RI1, 2026-11-14 18:26:45Z). Three live hosts
// of one region joined through one long-lived node on an old kernel. Host A
// received host B's sdp-offer through the bridge and never sent its answer:
//   relay-signal-origin { reach: lookup-no, result: not-sent,
//     lookupWhy: send-failed, failAt: <old node>, failNext: <a host dead 21 h>,
//     failConnected: false }
// A looks B up before it answers. The walk reaches the old node M. M holds a
// DEAD member D that is XOR-closer to B than M is, and does not hold B as a
// member (B dialed M; it sits in M's incoming pool). M forwards to D, the send
// fails, the walk ends. The answer is dropped and the pair never meshes.
//
// THIS FENCE BUILDS THAT STATE ON PURPOSE, on real AxonaPeers over the sim
// transport, and asks which of the mechanisms the kernel already carries is
// sufficient to get the answer out:
//   S0  defaults                       → THE FAULT: not sent, naming M and D
//   S1  LOOKUP_RESELECT at the holder  → NOT sufficient: the dead member is
//       skipped and M has no other candidate (B is bound, not a member)
//   S2  the no-channel sweep at holder → SUFFICIENT once D has left the table
//   S3  SIGNAL_RETURN_ROUTE at A       → SUFFICIENT at once: the answer goes
//       back along the offer's own hops and no lookup is asked
// Each arm changes ONE switch on ONE node. The fence is the record of which
// switch covers this state, and it fails when that answer changes.
//
// WHAT S0 AND S2 TOGETHER SAY: M holds an OPEN CHANNEL to the target B the
// whole time. With the dead member in its table M forwards to the member and
// stops; without it M delivers on the channel. The forwarder prefers a table
// member that is closer to the target over the target itself when the target
// is bound and not a member. That preference is the fault this state hits.
//
// TOPOLOGY. Channels A–M and M–B. No channel A–B. M's table: A (member),
// D (member, no channel). B is bound to M and is not a member of M's table.
// D is chosen XOR-closer to B than M is, so M's greedy step prefers it.
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
const wait  = (ms) => new Promise((r) => setTimeout(r, ms));
const INTERVAL = 15000;

async function makePeer(net, domainOpts, lat, lng, peerOpts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 300, ...domainOpts });   // one domain per node: a switch is armed on ONE node
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...peerOpts });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }   // the fence drives the tick
  const rec = { peer, id, transport, node, domain, big: fromHex(id.id), hex: id.id, logs: [], clock: 1_000_000 };
  peer._noChannelClock = () => rec.clock;
  for (const lv of ['info', 'warn']) peer.onLog(lv, (ev, ctx) => rec.logs.push({ ev, ctx }));
  return rec;
}
function member(a, bBig) {
  const syn = new Synapse({ peerId: bBig, latencyMs: 10, stratum: clz264(a.big ^ bBig) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(bBig, syn);
}
/**
 * Build the field state. `arm` names the one switch and the node it is on.
 * Returns the three live nodes and the dead id.
 */
async function build({ holderDomain = {}, holderPeer = {}, answererDomain = {}, offererDomain = {} } = {}) {
  const net = new SimNetwork();
  const A = await makePeer(net, answererDomain, 40.00, -74.00);
  const M = await makePeer(net, holderDomain,   40.01, -74.00, holderPeer);
  const B = await makePeer(net, offererDomain,  40.02, -74.00);
  await A.transport.openConnection(M.hex);
  await B.transport.openConnection(M.hex);           // B dialed M: bound, and not a member of M's table
  await wait(20);
  member(A, M.big); member(M, A.big); member(B, M.big);
  M.node.synaptome.delete(B.big);                    // however the bind landed, B is not a table member of M
  // D: a dead id closer to B than M is. Flip one low bit of B: distance 2 to B.
  const D = B.big ^ 2n;
  member(M, D);                                      // no channel to D was ever opened: the stale row
  return { net, A, M, B, D, all: [A, M, B] };
}
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
const originRows = (p, attempt) => p.logs.filter((l) => l.ev === 'relay-signal-origin' && l.ctx?.attempt === attempt).map((l) => l.ctx);
const answer = (w, attempt) => w.A.peer._relayMeshSignal(w.B.big, w.B.hex, { kind: 'sdp-answer', sdp: 'v=0', attempt });
/** B's offer, routed B → M → A, as a relayed offer travels. */
const offer = (w, attempt) => w.B.peer.routeMessage(w.A.big, 'mesh:signal', { from: w.B.hex, signal: { kind: 'sdp-offer', sdp: 'v=0', attempt } });
const short = (big) => toHex(big).slice(0, 12);

(async () => {
  console.log('fence_stale_member_answer: an answer gated on a lookup that meets a dead table member');

  // ── P. preconditions: the state is the field state ────────────────────
  {
    const w = await build();
    const { A, M, B, D } = w;
    check('P1 M holds the dead id as a member and no channel to it', M.node.synaptome.has(D) && M.transport.isConnected(D) !== true);
    check('P2 M holds a channel to B and B is not a member of M\'s table', M.transport.isConnected(B.big) === true && !M.node.synaptome.has(B.big));
    check('P3 the dead id is closer to B than M is', (D ^ B.big) < (M.big ^ B.big));
    check('P4 A holds no channel to B', A.transport.isConnected(B.big) !== true);
    const r = await offer(w, 'p-offer');
    check('P5 B\'s offer reaches A through M: the path the answer could return on exists', r?.consumed === true && r.atNode === A.big, JSON.stringify({ consumed: r?.consumed, hops: r?.hops, terminal: r?.terminal }));
    await stopAll(w);
  }

  // ── S0. defaults: the fault ────────────────────────────────────────────
  {
    const w = await build();
    await offer(w, 's0-offer');
    await answer(w, 's0');
    const row = originRows(w.A, 's0')[0];
    check('S0 with every switch at its default the answer is NOT SENT', row?.result === 'not-sent' && row.reach === 'lookup-no', JSON.stringify(row));
    check('S0b and the row names the holder and the dead member, as the field row did', row?.lookupWhy === 'send-failed' && row.failAt === short(w.M.big) && row.failNext === short(w.D) && row.failConnected === false, JSON.stringify(row));
    await stopAll(w);
  }

  // ── S1. LOOKUP_RESELECT on the holder only ─────────────────────────────
  let s1 = null;
  {
    const w = await build({ holderDomain: { LOOKUP_RESELECT: true } });
    await offer(w, 's1-offer');
    await answer(w, 's1');
    s1 = originRows(w.A, 's1')[0] ?? null;
    console.log(`    S1 measured: ${JSON.stringify(s1)}`);
    await stopAll(w);
  }

  // ── S2. the no-channel sweep on the holder only ────────────────────────
  let s2 = null, s2swept = null;
  {
    const w = await build({ holderPeer: { synaptomeMaintain: { noChannelTicks: 2 } } });
    await offer(w, 's2-offer');
    for (let i = 0; i < 4; i++) { w.M.clock += INTERVAL; await w.M.peer._maintainSynaptome(); }
    s2swept = !w.M.node.synaptome.has(w.D);
    await answer(w, 's2');
    s2 = originRows(w.A, 's2')[0] ?? null;
    console.log(`    S2 measured: swept=${s2swept} ${JSON.stringify(s2)}`);
    await stopAll(w);
  }

  // ── S3. SIGNAL_RETURN_ROUTE on the answerer only ───────────────────────
  let s3 = null;
  {
    const w = await build({ answererDomain: { SIGNAL_RETURN_ROUTE: true } });
    await offer(w, 's3-offer');
    await answer(w, 's3');
    s3 = originRows(w.A, 's3')[0] ?? null;
    console.log(`    S3 measured: ${JSON.stringify(s3)}`);
    await stopAll(w);
  }

  // ── The record: which switch covers this state ─────────────────────────
  check('S1 LOOKUP_RESELECT at the holder is NOT sufficient: it skips the dead member and has no other candidate, because B is bound to M and is not a member of its table', s1?.result === 'not-sent' && s1.failAt != null && /no live candidate after re-selection/.test(String(s1.failErr)) && s1.failReselects === 1, JSON.stringify(s1));
  check('S2 the no-channel sweep removes the dead member after its ticks', s2swept === true);
  check('S2b and then the answer is DELIVERED by the answerer\'s own lookup: with no dead member in the way, M reaches B on the channel it holds', s2?.result === 'delivered' && s2.reach === 'lookup-ok', JSON.stringify(s2));
  check('S3 SIGNAL_RETURN_ROUTE at the answerer: the answer is DELIVERED along the offer\'s own path', s3?.result === 'delivered' && s3.reach === 'return-route', JSON.stringify(s3));

  console.log(`\nfence_stale_member_answer: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
