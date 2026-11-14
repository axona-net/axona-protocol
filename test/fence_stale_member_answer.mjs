// =====================================================================
// fence_stale_member_answer.mjs — 4.144.0 fence, 4.145.0 rule (David 2026-11-14: "add tests to the
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
//   S2  the no-channel sweep at holder → removes D; NOT sufficient while B
//       stays a non-member (M's table at its cap): the walk stalls at M
//   S2r the sweep, table with room     → sufficient, and only because the
//       maintain pass's reconcile promotes B into the table (Orion 0e8f044b)
//   S3  SIGNAL_RETURN_ROUTE at A       → SUFFICIENT at once: the answer goes
//       back along the offer's own hops and no lookup is asked
//   S4  BOUND_TARGET_DELIVER at M      → SUFFICIENT at once (4.145.0): M holds
//       an open channel to B and sends there, dead member or not
// Each arm changes ONE switch on ONE node. The fence is the record of which
// switch covers this state, and it fails when that answer changes.
//
// THE FAULT UNDER S0, S1 AND S2: a peer that dialed in is bound and is not
// always a table member. With pools unarmed it is in neither the lookup
// step's candidates nor the forwarder's usable set. M holds an open channel
// to the target throughout and cannot see it.
//
//   G. the bound-target rule's guards and reach: default OFF; a stale channel
//      is not used; a target already on the path is not revisited; a routed
//      frame and the origin take the same rule; a walk toward an id nobody
//      is bound to (a topic) is unchanged.
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
async function build({ holderDomain = {}, holderPeer = {}, answererDomain = {}, offererDomain = {}, atCap = true } = {}) {
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
  // A table at its cap admits nobody: the maintain pass cannot promote a bound
  // peer into it. Modelled by switching off M's reconcile (the test table holds
  // 2 rows, far below the cap of 48, so the real reconcile would promote B).
  if (atCap) M.peer._reconcileBound = () => {};
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

  // ── S2. the no-channel sweep on the holder only, table at its cap ──────
  let s2 = null, s2swept = null, s2member = null;
  {
    const w = await build({ holderPeer: { synaptomeMaintain: { noChannelTicks: 2 } } });
    await offer(w, 's2-offer');
    for (let i = 0; i < 4; i++) { w.M.clock += INTERVAL; await w.M.peer._maintainSynaptome(); }
    s2swept = !w.M.node.synaptome.has(w.D);
    s2member = w.M.node.synaptome.has(w.B.big);
    await answer(w, 's2');
    s2 = originRows(w.A, 's2')[0] ?? null;
    await stopAll(w);
  }
  // ── S2r. the same sweep, table with room: the reconcile promotes B ─────
  let s2r = null, s2rMember = null;
  {
    const w = await build({ holderPeer: { synaptomeMaintain: { noChannelTicks: 2 } }, atCap: false });
    await offer(w, 's2r-offer');
    for (let i = 0; i < 4; i++) { w.M.clock += INTERVAL; await w.M.peer._maintainSynaptome(); }
    s2rMember = w.M.node.synaptome.has(w.B.big);
    await answer(w, 's2r');
    s2r = originRows(w.A, 's2r')[0] ?? null;
    await stopAll(w);
  }

  // ── S3. SIGNAL_RETURN_ROUTE on the answerer only ───────────────────────
  let s3 = null;
  {
    const w = await build({ answererDomain: { SIGNAL_RETURN_ROUTE: true } });
    await offer(w, 's3-offer');
    await answer(w, 's3');
    s3 = originRows(w.A, 's3')[0] ?? null;
    await stopAll(w);
  }

  // ── S4. BOUND_TARGET_DELIVER on the holder only ────────────────────────
  let s4 = null, s4stats = null, s4dead = null;
  {
    const w = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    await offer(w, 's4-offer');
    await answer(w, 's4');
    s4 = originRows(w.A, 's4')[0] ?? null;
    s4stats = w.M.peer._boundTargetStats ?? null;
    s4dead = w.M.node.synaptome.has(w.D);
    await stopAll(w);
  }

  check('S1 LOOKUP_RESELECT at the holder is NOT sufficient: it skips the dead member and has no other candidate, because B is bound to M and is not a member of its table', s1?.result === 'not-sent' && s1.failAt != null && /no live candidate after re-selection/.test(String(s1.failErr)) && s1.failReselects === 1, JSON.stringify(s1));
  check('S2 the no-channel sweep removes the dead member after its ticks', s2swept === true);
  check('S2b with M\'s table at its cap B is still not a member, and the answer is STILL NOT SENT: the sweep alone is not sufficient', s2member === false && s2?.result === 'not-sent' && s2.reach === 'lookup-no', JSON.stringify({ s2member, s2 }));
  check('S2r with room in the table the maintain pass promotes B, and only then is the answer delivered by A\'s own lookup', s2rMember === true && s2r?.result === 'delivered' && s2r.reach === 'lookup-ok', JSON.stringify({ s2rMember, s2r }));
  check('S3 SIGNAL_RETURN_ROUTE at the answerer: the answer is DELIVERED along the offer\'s own path', s3?.result === 'delivered' && s3.reach === 'return-route', JSON.stringify(s3));
  check('S4 BOUND_TARGET_DELIVER at the holder: the answer is DELIVERED by A\'s own lookup, with the dead member still in M\'s table', s4?.result === 'delivered' && s4.reach === 'lookup-ok' && s4dead === true, JSON.stringify({ s4, s4dead }));
  check('S4b and M counted the rule in its lookup step', (s4stats?.lookup ?? 0) >= 1, JSON.stringify(s4stats));

  // ── G. the rule's guards and reach ─────────────────────────────────────
  {
    const w = await build();
    check('G1 the switch is OFF by default on every node', w.all.every((p) => p.domain.BOUND_TARGET_DELIVER === false));
    await stopAll(w);
  }
  {
    // A routed frame (not a lookup): A → M → B. Without the rule M cannot see B and the frame ends at M.
    const off = await build();
    const r0 = await off.A.peer.routeMessage(off.B.big, 'fence:noop', {});
    await stopAll(off);
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    let got = 0;
    on.B.peer.onRoutedMessage?.('fence:probe', () => { got++; return 'consumed'; });
    const r1 = await on.A.peer.routeMessage(on.B.big, 'fence:probe', {});
    const st = on.M.peer._boundTargetStats ?? null;
    check('G2 a routed frame: without the rule it does not reach B; with it the holder sends it to B', !(r0?.atNode === off.B.big) && r1?.atNode === on.B.big && (st?.route ?? 0) >= 1, JSON.stringify({ off: { consumed: r0?.consumed, at: r0?.atNode === off.B.big }, on: { consumed: r1?.consumed, at: r1?.atNode === on.B.big }, st }));
    await stopAll(on);
  }
  {
    // The origin itself holds the channel: M → B directly.
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    const r = await on.M.peer.routeMessage(on.B.big, 'fence:noop', {});
    check('G3 the origin takes the same rule: M sends straight to the peer it is bound to', r?.atNode === on.B.big && r.hops === 1 && (on.M.peer._boundTargetStats?.origin ?? 0) === 1, JSON.stringify({ at: r?.atNode === on.B.big, hops: r?.hops, st: on.M.peer._boundTargetStats }));
    await stopAll(on);
  }
  {
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    on.M.peer._isStalePeer = (id) => id === on.B.big;            // the transport calls the channel to B stale
    await offer(on, 'g4-offer');
    await answer(on, 'g4');
    const row = originRows(on.A, 'g4')[0];
    check('G4 a channel the transport calls STALE is not used: the rule stands aside and the old outcome returns', row?.result === 'not-sent' && on.M.peer._boundTargetOk(on.B.big) === false, JSON.stringify(row));
    await stopAll(on);
  }
  {
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    const M = on.M.peer;
    check('G5 a target already on the path is not revisited', M._boundTargetOk(on.B.big, new Set([on.B.big])) === false && M._boundTargetOk(on.B.big, new Set([on.A.big])) === true);
    check('G6 self, a dead-marked target, an id nobody is bound to and a non-id are never bound targets', M._boundTargetOk(on.M.big) === false && M._boundTargetOk(on.D) === false && M._boundTargetOk(on.B.big ^ (1n << 200n)) === false && M._boundTargetOk('x') === false);
    on.M.node._deadPeers?.mark?.(on.B.big);
    const marked = on.M.node._deadPeers?.has?.(on.B.big) === true;
    check('G6b a target marked dead is not used even while its channel reads open', !marked || M._boundTargetOk(on.B.big) === false, `marked ${marked}`);
    await stopAll(on);
  }
  {
    // No dead member at all: M simply has nobody closer to B than itself. Unarmed, B is still invisible.
    const off = await build();
    off.M.node.synaptome.delete(off.D);
    await offer(off, 'g8-offer'); await answer(off, 'g8');
    const r0 = originRows(off.A, 'g8')[0];
    await stopAll(off);
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    on.M.node.synaptome.delete(on.D);
    await offer(on, 'g8-offer'); await answer(on, 'g8');
    const r1 = originRows(on.A, 'g8')[0];
    check('G8 with NO dead member the default still fails (the walk stalls at M, which cannot see B); with the rule the bound target is a candidate and the answer is delivered', r0?.result === 'not-sent' && r1?.result === 'delivered', JSON.stringify({ off: r0, on: r1 }));
    await stopAll(on);
  }
  {
    // A ranker that never prefers the bound target (the lookahead's float ranking can do this at scale, Orion 0e8f044b).
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    on.M.peer._bestByTwoHopAP = async (cands) => cands.find((c) => c._addedBy !== 'bound-target') ?? cands[0];
    await offer(on, 'g9-offer'); await answer(on, 'g9');
    const row = originRows(on.A, 'g9')[0];
    check('G9 the bound target is CHOSEN, not merely ranked: a ranker that prefers any other candidate does not get a say', row?.result === 'delivered', JSON.stringify(row));
    await stopAll(on);
  }
  {
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true } });
    const t = on.M.node.transport; const ic = t.isConnected.bind(t);
    t.isConnected = (id) => (id === on.M.big ? true : ic(id));       // a transport that claims a channel to itself
    check('G10 a node is never its own bound target, whatever the transport says', on.M.peer._boundTargetOk(on.M.big) === false);
    await stopAll(on);
  }
  {
    // A walk toward an id that is nobody's node id (as a topic id is): the rule adds no candidate.
    const on = await build({ holderDomain: { BOUND_TARGET_DELIVER: true }, answererDomain: { BOUND_TARGET_DELIVER: true }, offererDomain: { BOUND_TARGET_DELIVER: true } });
    const topicLike = on.B.big ^ (1n << 180n);
    await on.A.peer.lookup(topicLike, { noCache: true }).catch(() => null);
    await on.A.peer.routeMessage(topicLike, 'fence:noop', {}).catch(() => null);
    const sum = on.all.reduce((n, p) => n + (p.peer._boundTargetStats ? p.peer._boundTargetStats.lookup + p.peer._boundTargetStats.route + p.peer._boundTargetStats.origin : 0), 0);
    check('G7 a walk toward an id no node holds (a topic id) never takes the rule, on any node', sum === 0, `count ${sum}`);
    await stopAll(on);
  }

  console.log(`\nfence_stale_member_answer: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
