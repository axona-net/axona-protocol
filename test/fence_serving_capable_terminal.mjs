// =====================================================================
// fence_serving_capable_terminal.mjs — R4 of Routed-Walk-Terminals v0.6
// (axona-docs 88d6871), on REAL AxonaPeers over the sim transport.
//
// The defect: any node could be a routing terminal, and any terminal could
// root. A browser tab admitted at the door, whose node id landed beside
// axona.chat, became that topic's greedy destination and may have rooted it;
// on 2026-10-11 the acceptance probe still found one four-minute-old relay
// declaring itself terminal at hop 0 with nothing behind it.
//
// R4: a node is a terminal, and births a root, only if transport.servingCapable()
// says it can serve. One that cannot forwards to the bridge's node when it
// holds it, else to its XOR-closest connected peer off the walk's path, else
// stops with `no-capable-terminal`. The gate sits on _becomeRoot itself, so
// every birth (sub, pub, kill, metricson, handoff heir) and promote() pass
// through it; the capability test runs BEFORE the terminal verify; and a
// transport with no predicate is the named EXCEPTION, treated capable, logged
// once, counted.
//
// The sim transport has no servingCapable() — the fence installs one per peer,
// which is also how it exercises the exception branch (by not installing it).
//
// Cases:
//   1  routing: the closest node cannot serve → the walk forwards to its
//      closest connected off-path peer, which is terminal (1 hop further)
//   2  routing: an incapable node with no off-path peer → stopped no-capable-terminal
//   3  births: sub/pub/kill/metricson/handoff-heir on an incapable node → null;
//      the same on a capable node → a role
//   4  promote(): an incapable terminal stays non-root; a capable one takes the seat
//   5  verify order: with the verify armed, an incapable node's SUB never calls
//      _verifyTerminalOwnership and births nothing
//   6  exception: a transport with no predicate is treated capable, one log
//      line, counted as the exception and never as capable
//   7  health surfaces the counters
//
// Mutants run by hand on 2026-10-11: delete the `isTerminal && !capable`
// branch in route_msg → case 1 ends terminal at the incapable node; delete the
// gate in _becomeRoot → case 3 births roots on the incapable node.
//
// Run: node test/fence_serving_capable_terminal.mjs
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
const hex66 = (b) => b.toString(16).padStart(66, '0');

async function makePeer(net, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const logs = [];
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  peer.onLog?.('warn', (msg, ctx) => logs.push({ msg, ctx }));
  await peer.start();
  peer._requireAxonaManager('fence');            // the default manager a first pub/sub call would build
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id, logs, am: peer._axonaManager };
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
const capable = (p, v) => { p.transport.servingCapable = () => v; };
async function stopAll(ps) { for (const p of ps) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }

(async () => {
  console.log('fence_serving_capable_terminal: R4 — a terminal must be able to serve');
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 300 });

  // ── Cases 1 and 2: routing ──────────────────────────────────────────────
  {
    const net = new SimNetwork();
    const raw = []; for (let i = 0; i < 3; i++) raw.push(await makePeer(net, domain, 40 + i * 0.01, -74));
    const A = raw[0]; const T = A.big ^ 1n;                       // A is closest to T
    const [B, C] = raw.slice(1).sort((x, y) => ((x.big ^ T) < (y.big ^ T) ? -1 : 1));
    await A.transport.openConnection(B.hex); await A.transport.openConnection(C.hex); await wait(20);
    synapse(A, B); synapse(A, C); synapse(B, A); synapse(C, A);
    capable(A, false); capable(B, true); capable(C, true);
    const r = await C.peer.routeMessage(T, 'fence:noop', {});
    check('1 the closest node cannot serve: the walk forwards to its closest connected off-path peer and ends terminal there',
      r && r.terminal === true && r.atNode === B.big && r.hops === 2, JSON.stringify({ ...r, atNode: short(r?.atNode) }));
    const st = A.peer.health?.()?.servingCapable ?? A.peer._servingCapableStats;
    check('1 the incapable node counted the forward', (st?.forwardedIncapable | 0) >= 1 && (st?.incapable | 0) >= 1, JSON.stringify(st));
    // 2: strip A of every off-path peer: close A–B, leave only C (on the path)
    await A.transport.closeConnection(B.hex); await wait(20);
    const r2 = await C.peer.routeMessage(T, 'fence:noop', {});
    check('2 an incapable node with no off-path peer stops the walk: no-capable-terminal',
      r2 && r2.stopped === 'no-capable-terminal' && r2.atNode === A.big && r2.terminal !== true, JSON.stringify({ ...r2, atNode: short(r2?.atNode) }));
    await stopAll(raw);
  }

  // ── Cases 3, 4, 5: births, promote, verify order ───────────────────────
  {
    const net = new SimNetwork();
    const A = await makePeer(net, domain, 41, -74); const B = await makePeer(net, domain, 41.01, -74);
    capable(A, false); capable(B, true);
    const topic = A.big ^ 0x1234n;
    for (const why of ['sub-terminal', 'pub-terminal', 'kill-terminal', 'metricson-terminal', 'handoff-heir']) {
      const role = A.am._becomeRoot(topic ^ BigInt(why.length), why);
      check(`3 ${why} on an incapable node births nothing`, role === null, String(role));
    }
    check('3 the refusals are counted', (A.am._rootBirthsRefused | 0) === 5, String(A.am._rootBirthsRefused));
    const roleB = B.am._becomeRoot(topic, 'sub-terminal');
    check('3 sub-terminal on a capable node births a root', !!roleB && roleB.isRoot === true, String(roleB && roleB.isRoot));
    // 4: promote
    capable(A, true);
    const roleA = A.am._becomeRoot(topic ^ 7n, 'sub-terminal');
    roleA.isRoot = false;
    capable(A, false);
    A.am._rootClaim.promote(roleA, { via: [] }, { isTerminal: true });
    check('4 promote() on an incapable terminal leaves the role non-root', roleA.isRoot === false);
    capable(A, true);
    A.am._rootClaim.promote(roleA, { via: [] }, { isTerminal: true });
    check('4 promote() on a capable terminal takes the seat', roleA.isRoot === true);
    // 5: verify order
    capable(A, false);
    let verifyCalled = 0;
    A.am._subTerminalVerify = true;
    A.am._verifyTerminalOwnership = async () => { verifyCalled++; return { kind: 'self' }; };
    const t5 = topic ^ 0x99n;
    const out = await A.am._onSub({ topicId: hex66(t5), subscriberId: B.hex, since: 0 }, { fromId: B.hex, isTerminal: true });
    check('5 an incapable node\'s terminal SUB never reaches the verify and births nothing',
      verifyCalled === 0 && !A.am.axonRoles.has(t5), `verifyCalled=${verifyCalled} role=${A.am.axonRoles.has(t5)} out=${out}`);
    capable(A, true);
    await A.am._onSub({ topicId: hex66(t5), subscriberId: B.hex, since: 0 }, { fromId: B.hex, isTerminal: true });
    check('5 the same SUB on a capable node runs the verify and births the root', verifyCalled === 1 && A.am.axonRoles.has(t5), `verifyCalled=${verifyCalled}`);
    await stopAll([A, B]);
  }

  // ── Cases 6 and 7: the exception branch and the health surface ─────────
  {
    const net = new SimNetwork();
    const E = await makePeer(net, domain, 42, -74);           // no servingCapable installed
    const v1 = E.peer._servingCapable(); const v2 = E.peer._servingCapable();
    const st = E.peer._servingCapableStats;
    check('6 a transport with no predicate is treated capable', v1 === true && v2 === true);
    check('6 ...counted as the exception, never as capable', st.exception === 2 && st.capable === 0 && st.incapable === 0, JSON.stringify(st));
    const lines = E.logs.filter((l) => l.msg === 'serving-capable-unknown');
    check('6 ...and logged exactly once', lines.length === 1 && lines[0].ctx?.treatedAs === 'capable', JSON.stringify(lines));
    const h = E.peer.health?.();
    check('7 health surfaces servingCapable counters and the live answer', h && h.servingCapable && h.servingCapable.exception >= 2 && h.servingCapable.now === true, JSON.stringify(h?.servingCapable));
    await stopAll([E]);
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
