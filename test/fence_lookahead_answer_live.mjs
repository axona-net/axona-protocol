// =====================================================================
// fence_lookahead_answer_live.mjs — 4.147.0 (David 2026-11-15: "Build the
// lookahead-answer liveness check first").
//
// THE FIELD STATE (testnet, runs RJ1, RL3, RM1, 2026-11-14/15). A host with no
// closer peer of its own ran the two-hop check and asked its peers for THEIR
// best forward toward a topic. Some peer's table still held a host that had
// been hard-stopped, XOR-closer to the topic than the asker. The
// lookahead_probe handler named it. The asker forwarded toward the answering
// peer, and the walk left the true closest host: the topics that missed their
// closest host were exactly those XOR-closer to one dead host id (RM1: 132 of
// 263, all 263 classified by that one id).
//
// THE ASYMMETRY. The ASKER filters the peers it probes: dead-marked, not
// connected, on the path. The ANSWERER filtered nothing.
//
// THE RULE (LOOKAHEAD_ANSWER_LIVE, default OFF). A node names only a table
// entry it could send to now: not itself, not dead-marked, a channel that
// reads open, not suspect.
//
// TOPOLOGY. H asks S. S's table holds:
//   D  a dead member, no channel, distance 2 from the target T
//   L  a live member with an open channel, farther from T than D, closer than H
// H holds a channel to S only.
//
//   P  preconditions: the state is the field state
//   A  the answer: OFF names the dead member (the fault); ON names the live
//      one, or says terminal when no nameable entry is closer
//   G  guards: a dead-marked entry with an open channel is not named; a
//      suspect entry is not named; a live entry is still named; a transport
//      that cannot say leaves the answer as before; the flag is OFF by
//      default and env-armed; the counters count
//   E  end to end: H's own two-hop check stops being pulled toward the dead id
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
const short = (big) => (typeof big === 'bigint' ? toHex(big).slice(0, 12) : String(big));

async function makePeer(net, domainOpts, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 300, ...domainOpts });   // one domain per node: the switch is armed on ONE node
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  return { peer, id, transport, node, domain, big: fromHex(id.id), hex: id.id };
}
function member(a, bBig) {
  const syn = new Synapse({ peerId: bBig, latencyMs: 10, stratum: clz264(a.big ^ bBig) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(bBig, syn);
}
/** Build the state. `answerer` is S's domain options; `withLive` gives S the live member L. */
async function build({ answerer = {}, withLive = true } = {}) {
  const net = new SimNetwork();
  const H = await makePeer(net, {},       40.00, -74.00);
  const S = await makePeer(net, answerer, 40.01, -74.00);
  const L = await makePeer(net, {},       40.02, -74.00);
  await H.transport.openConnection(S.hex);
  await L.transport.openConnection(S.hex);
  await wait(20);
  // T: a target that is no node. L sits 2^40 from it; D sits 2 from it.
  const T = L.big ^ (1n << 40n);
  const D = T ^ 2n;
  S.node.synaptome.clear(); H.node.synaptome.clear();
  member(H, S.big);
  member(S, D);                                   // the stale row: no channel to D was ever opened
  if (withLive) member(S, L.big);
  return { net, H, S, L, T, D, all: [H, S, L] };
}
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
/** H asks S, over the wire, as the two-hop check does. */
const ask = (w) => w.H.transport.send(w.S.big, 'lookahead_probe', { target: w.T, fromDist: w.H.big ^ w.T });
const named = (r) => (r && r.peerId != null ? (typeof r.peerId === 'bigint' ? r.peerId : fromHex(String(r.peerId))) : null);

(async () => {
  console.log('fence_lookahead_answer_live: a lookahead answer names only a peer the answerer could send to');

  // ── P. preconditions ───────────────────────────────────────────────────
  {
    const w = await build();
    const { H, S, L, T, D } = w;
    check('P1 S holds the dead id as a member and no channel to it', S.node.synaptome.has(D) && S.transport.isConnected(D) !== true);
    check('P2 S holds the live peer as a member with an open channel', S.node.synaptome.has(L.big) && S.transport.isConnected(L.big) === true);
    check('P3 the dead id is closer to the target than the live peer, and both are closer than the asker', (D ^ T) < (L.big ^ T) && (L.big ^ T) < (H.big ^ T));
    check('P4 the asker holds a channel to S and to nobody closer', H.transport.isConnected(S.big) === true && H.transport.isConnected(L.big) !== true && H.transport.isConnected(D) !== true);
    check('P5 the switch is OFF by default', new AxonaDomain({}).LOOKAHEAD_ANSWER_LIVE === false);
    await stopAll(w);
  }

  // ── A. the answer ──────────────────────────────────────────────────────
  {
    const w = await build();
    const r = await ask(w);
    check('A0 OFF: the answer NAMES THE DEAD MEMBER (the fault, as 4.146.0)', named(r) === w.D && r.terminal === false, JSON.stringify({ named: short(named(r)), terminal: r?.terminal }));
    check('A0b OFF: no counter is kept', w.S.peer._lookaheadAnswerStats === undefined);
    await stopAll(w);
  }
  {
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true } });
    const r = await ask(w);
    check('A1 ON: the answer names the LIVE member, not the dead one', named(r) === w.L.big && r.terminal === false, JSON.stringify({ named: short(named(r)), terminal: r?.terminal }));
    const st = w.S.peer._lookaheadAnswerStats;
    check('A1b ON: one answer counted, one entry skipped, not terminal', st?.answers === 1 && st.skipped === 1 && st.terminalAfterSkip === 0, JSON.stringify(st));
    await stopAll(w);
  }
  {
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true }, withLive: false });
    const r = await ask(w);
    check('A2 ON, no nameable entry closer: the answer is TERMINAL and names the answerer itself', r?.terminal === true && named(r) === w.S.big, JSON.stringify({ named: short(named(r)), terminal: r?.terminal }));
    check('A2b ON: the terminal-after-skip counter moved', w.S.peer._lookaheadAnswerStats?.terminalAfterSkip === 1, JSON.stringify(w.S.peer._lookaheadAnswerStats));
    await stopAll(w);
  }
  {
    const w = await build({ withLive: false });
    const r = await ask(w);
    check('A3 OFF, same table: the dead member is still named (control for A2)', named(r) === w.D && r.terminal === false, JSON.stringify({ named: short(named(r)), terminal: r?.terminal }));
    await stopAll(w);
  }

  // ── G. guards ──────────────────────────────────────────────────────────
  {
    // G1: an entry with an OPEN channel that is dead-marked is not named.
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true } });
    w.S.node.synaptome.delete(w.D);                                  // only L is closer now
    const before = named(await ask(w));
    const marks = (w.S.node._deadPeers ??= new Set());
    if (typeof marks.fail === 'function') marks.fail(w.L.big, 'fence'); else marks.add(w.L.big);
    const r = await ask(w);
    check('G1 a dead-marked entry is not named though its channel reads open', before === w.L.big && r?.terminal === true && w.S.transport.isConnected(w.L.big) === true, JSON.stringify({ before: short(before), named: short(named(r)), terminal: r?.terminal }));
    await stopAll(w);
  }
  {
    // G2: a suspect (stale) entry is not named.
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true } });
    w.S.node.synaptome.delete(w.D);
    const realStale = w.S.transport.isStale;
    w.S.transport.isStale = (id) => id === w.L.big;
    const r = await ask(w);
    w.S.transport.isStale = realStale;
    const r2 = await ask(w);
    check('G2 a suspect entry is not named', r?.terminal === true, JSON.stringify({ named: short(named(r)), terminal: r?.terminal }));
    check('G2b and it is named again once it is no longer suspect', named(r2) === w.L.big && r2.terminal === false, JSON.stringify({ named: short(named(r2)), terminal: r2?.terminal }));
    await stopAll(w);
  }
  {
    // G3: nothing live is lost. Only the live member in the table: named with the switch on.
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true } });
    w.S.node.synaptome.delete(w.D);
    const r = await ask(w);
    const st = w.S.peer._lookaheadAnswerStats;
    check('G3 a live entry is named and nothing is skipped', named(r) === w.L.big && st?.skipped === 0, JSON.stringify({ named: short(named(r)), st }));
    await stopAll(w);
  }
  {
    // G4: the predicate itself, case by case.
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true } });
    const p = w.S.peer;
    check('G4 nameable: the live member', p._lookaheadNameable(w.L.big) === true);
    check('G4b not nameable: the member with no channel', p._lookaheadNameable(w.D) === false);
    check('G4c not nameable: this node itself', p._lookaheadNameable(w.S.big) === false);
    check('G4d not nameable: a value that is no id', p._lookaheadNameable('x') === false && p._lookaheadNameable(null) === false);
    // A transport that cannot say: the entry is named as before.
    const realConn = w.S.transport.isConnected;
    w.S.transport.isConnected = undefined;
    const cannotSay = p._lookaheadNameable(w.D);
    w.S.transport.isConnected = realConn;
    check('G4e a transport with no isConnected cannot say: the entry is named as before', cannotSay === true);
    // A transport whose isConnected throws: not nameable.
    w.S.transport.isConnected = () => { throw new Error('boom'); };
    const threw = p._lookaheadNameable(w.L.big);
    w.S.transport.isConnected = realConn;
    check('G4f an isConnected that throws reads as not open', threw === false);
    // With a transport that says "open" to everything, self and a non-id are still refused by their own tests.
    w.S.transport.isConnected = () => true;
    const selfOpen = p._lookaheadNameable(w.S.big), strOpen = p._lookaheadNameable('x'), liveOpen = p._lookaheadNameable(w.L.big);
    w.S.transport.isConnected = realConn;
    check('G4g with every channel reading open: self and a non-id are still not nameable, a peer is', selfOpen === false && strOpen === false && liveOpen === true, JSON.stringify({ selfOpen, strOpen, liveOpen }));
    await stopAll(w);
  }
  {
    // G5: env arming, and an explicit option beats the env.
    const prev = process.env.LOOKAHEAD_ANSWER_LIVE;
    process.env.LOOKAHEAD_ANSWER_LIVE = '1';
    const armed = new AxonaDomain({}).LOOKAHEAD_ANSWER_LIVE;
    const opt   = new AxonaDomain({ LOOKAHEAD_ANSWER_LIVE: false }).LOOKAHEAD_ANSWER_LIVE;
    process.env.LOOKAHEAD_ANSWER_LIVE = '0';
    const zero = new AxonaDomain({}).LOOKAHEAD_ANSWER_LIVE;
    if (prev === undefined) delete process.env.LOOKAHEAD_ANSWER_LIVE; else process.env.LOOKAHEAD_ANSWER_LIVE = prev;
    check('G5 env LOOKAHEAD_ANSWER_LIVE=1 arms it; =0 and an explicit false leave it off', armed === true && opt === false && zero === false, JSON.stringify({ armed, opt, zero }));
  }

  // ── E. end to end: the asker's own two-hop check ───────────────────────
  {
    const w = await build({ withLive: false });
    const c = await w.H.peer._findCloserInTwoHops(w.T, { exclude: new Set([w.H.big]) });
    check('E1 OFF: the asker\'s two-hop check is PULLED toward the dead id through S', c != null && c.first === w.S.big && c.via === w.D, JSON.stringify(c && { first: short(c.first), via: short(c.via) }));
    await stopAll(w);
  }
  {
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true }, withLive: false });
    const c = await w.H.peer._findCloserInTwoHops(w.T, { exclude: new Set([w.H.big]) });
    check('E2 ON at S: the two-hop check finds nobody closer; the asker is the terminal', c == null || c.first === w.H.big, JSON.stringify(c && { first: short(c.first), via: short(c.via) }));
    await stopAll(w);
  }
  {
    const w = await build({ answerer: { LOOKAHEAD_ANSWER_LIVE: true } });
    const c = await w.H.peer._findCloserInTwoHops(w.T, { exclude: new Set([w.H.big]) });
    check('E3 ON at S with a live closer member: the two-hop check goes through S to the LIVE peer', c != null && c.first === w.S.big && c.via === w.L.big, JSON.stringify(c && { first: short(c.first), via: short(c.via) }));
    await stopAll(w);
  }

  console.log(`\nfence_lookahead_answer_live: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(2); });
