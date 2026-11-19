// =====================================================================
// fence_term_verify_unreachable.mjs — the terminal-ownership check must not
// defer a subscribe to a node this node cannot reach (4.157.0).
//
// THE BUG (sim, 2026-11-18, GH #75 follow-up). With SUB_TERMINAL_VERIFY on, a
// node cut off from the mesh subscribes. Its SUB ends at itself (nobody closer
// is reachable). _verifyTerminalOwnership asks the iterative lookup who is
// closest and gets X: strictly closer, but with no channel from here. The
// handler defers the SUB "via X". The route toward X ends back at this node,
// the waypoint reads as dead, _reroute pops it, the bare SUB ends here again,
// and the check answers X again (fresh hint):
//
//   _onSub -> verify 'closer' X -> _deferToRoot(via X) -> route ends at self
//          -> _onSub 'reroute' -> bare SUB ends at self -> _onSub -> verify ...
//
// No timer between steps. The process ran to the 4 GB heap limit in ~2 min in
// the sim (one topic was enough). Same shape as the 2026-07-27 bridge wedge
// (smoke_reroute_termination.mjs): a terminal node re-sending to itself.
//
// THE FIX. The check's own contract (AxonaManager, "a strictly-closer REACHABLE
// node → attach there") was not enforced: neither the fast path nor the lookup
// tested reachability. Now 'closer' requires a node this node can reach (the
// same neighbour test the repair plane uses, _isReachableId). A closer node
// that is not reachable is 'inconclusive': FAIL CLOSED, hold the seat, the
// subscription renewal retries once the mesh has grown.
//
// What this pins:
//   1. Precondition: with the check on, a cut-off node whose lookup names an
//      unreachable closer node does not loop: the SUB handler is entered a
//      bounded number of times and returns.
//   2. It does not self-root (fail closed holds the seat).
//   3. A REACHABLE closer node is still deferred to (the check still works).
//   4. Check off: unchanged (self-roots, no lookup).
//   5. MULTI-HOP (Orion 2165944e): the closer node has no direct channel, but a
//      live peer is closer to it than this node is. That is reachable: the SUB
//      is deferred via the closer node. b14fecb called it 'inconclusive' and
//      stranded the subscriber.
//
// Offline, fake DHT. The fake routes any message aimed at a node this node is
// not connected to back to this node as terminal, which is what greedy routing
// does when no neighbour is closer. A runaway is cut at 500 deliveries and
// reported, so the pre-fix code fails the test instead of hanging it.
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { idHex } from '../src/pubsub/ids.js';
import { sealTestDht } from './lib/testCapability.mjs';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => { if (cond) { pass++; console.log(`  ✓ ${name}`); } else { fail++; console.log(`  ✗ ${name} ${extra}`); } };

const TOPIC = 0x80c0000000000000000000000000000000000000000000000000000000000000c0n;
// Distances are built in one 4-bit window so every relation is explicit
// (topic-relative offsets 12, 11, 13 at bit 200; the region byte is untouched):
//   CLOSER is strictly closer to TOPIC than SELF (11 < 12);
//   RELAY is FARTHER from TOPIC than SELF (13 > 12), so SELF is the greedy
//   terminal for the topic, yet closer to CLOSER than SELF is (13^11=6 < 12^11=7).
const SELF   = TOPIC ^ (12n << 200n);
const CLOSER = TOPIC ^ (11n << 200n);
const RELAY  = TOPIC ^ (13n << 200n);
const CAP = 500;

function mkManager({ verify, reachable, listedNotLive = false, viaRelay = false }) {
  const handlers = new Map();
  let deliveries = 0, runaway = false; const sent = [];
  const neighbors = viaRelay ? [idHex(RELAY)] : (reachable || listedNotLive) ? [idHex(CLOSER)] : [];
  const dht = {
    getSelfId: () => SELF,
    verdictsSupported: false,
    onRoutedMessage: (type, h) => { handlers.set(type, h); },
    neighbors: () => neighbors,
    isLive: (id) => (reachable && id === CLOSER) || (viaRelay && id === RELAY),   // open, healthy channel now
    findKClosest: async () => [CLOSER],
    lookup: async () => ({ path: viaRelay ? [SELF, RELAY, CLOSER] : [SELF, CLOSER] }),
    // Greedy: a reachable target is delivered there (recorded as sent); anything
    // else ends at this node as terminal. With a live relay the closer node is
    // reached in two hops.
    routeMessage: (target, type, payload) => {
      sent.push({ target, type, payload: { ...payload } });
      if (reachable && target === CLOSER) return { consumed: true, atNode: CLOSER, hops: 1 };
      if (viaRelay && target === CLOSER) return { consumed: true, atNode: CLOSER, hops: 2 };
      if (++deliveries > CAP) { runaway = true; return { consumed: false, exhausted: true }; }
      const h = handlers.get(type);
      return Promise.resolve(h ? h(payload, { targetId: SELF, originId: idHex(SELF), isTerminal: true, hopCount: 0 }) : undefined)
        .then((r) => (r === 'consumed' ? { consumed: true, atNode: SELF, hops: 0 } : { consumed: false, atNode: SELF, hops: 0, terminal: true }));
    },
  };
  const prev = process.env.SUB_TERMINAL_VERIFY;
  if (verify) process.env.SUB_TERMINAL_VERIFY = '1'; else delete process.env.SUB_TERMINAL_VERIFY;
  const m = new AxonaManager({ dht: sealTestDht(dht), now: () => Date.now() });
  if (prev === undefined) delete process.env.SUB_TERMINAL_VERIFY; else process.env.SUB_TERMINAL_VERIFY = prev;
  m._log = () => {};
  m._rootClaim.meshBare = () => false;          // not alone in the dark: the guard under test is the verify path
  return { m, sent, stats: () => ({ deliveries, runaway }) };
}

async function subscribeOnce(m) {
  m.pubsubSubscribe(TOPIC);                      // seats mySubscriptions and sends the bare SUB
  for (let i = 0; i < 50; i++) await new Promise((r) => setImmediate(r));
  await new Promise((r) => setTimeout(r, 50));
}

console.log('\n[1] check on, closer node UNREACHABLE: no loop');
{
  const { m, stats } = mkManager({ verify: true, reachable: false });
  ok(m._subTerminalVerify === true, 'precondition: the check is on');
  await subscribeOnce(m);
  const s = stats();
  ok(!s.runaway, 'the SUB is not re-delivered without bound', JSON.stringify(s));
  ok(s.deliveries <= 4, 'the SUB handler is entered a few times at most', JSON.stringify(s));
  const role = m.axonRoles.get(TOPIC);
  ok(!(role && role.isRoot), 'and the node does not self-root (fail closed)');
  ok(m.mySubscriptions.has(TOPIC), 'the subscription is held for the renewal to retry');
}

console.log('\n[1b] check on, closer node LISTED as a neighbour but no live channel: no loop');
{
  // The sim trace (2026-11-18): a cut-off node's neighbour list falls back to its
  // synaptome and still names the closer node; the route to it ends back here.
  const { m, stats } = mkManager({ verify: true, reachable: false, listedNotLive: true });
  ok(m._isReachableId(idHex(CLOSER)) === true, 'precondition: the neighbour test calls it reachable');
  await subscribeOnce(m);
  const s = stats();
  ok(!s.runaway, 'the SUB is not re-delivered without bound', JSON.stringify(s));
  ok(s.deliveries <= 4, 'the SUB handler is entered a few times at most', JSON.stringify(s));
  const role = m.axonRoles.get(TOPIC);
  ok(!(role && role.isRoot), 'and the node does not self-root');
}

console.log('\n[2] check on, closer node REACHABLE: deferred to it');
{
  const { m, sent, stats } = mkManager({ verify: true, reachable: true });
  await subscribeOnce(m);
  ok(!stats().runaway, 'no loop', JSON.stringify(stats()));
  ok(sent.some((s) => s.type === T.SUB && s.target === CLOSER), 'the SUB is sent via the reachable closer node');
  const role = m.axonRoles.get(TOPIC);
  ok(!(role && role.isRoot), 'and this node does not root it');
}

console.log('\n[3] check off: unchanged, self-roots without a lookup');
{
  const { m, stats } = mkManager({ verify: false, reachable: false });
  ok(m._subTerminalVerify === false, 'precondition: the check is off');
  await subscribeOnce(m);
  ok(!stats().runaway, 'no loop');
  const role = m.axonRoles.get(TOPIC);
  ok(!!(role && role.isRoot), 'the terminal node roots the topic, as before');
}

console.log('\n[5] check on, closer node 2 hops away through a live relay: deferred to it (Orion 2165944e)');
{
  const { m, sent, stats } = mkManager({ verify: true, reachable: false, viaRelay: true });
  ok((RELAY ^ CLOSER) < (SELF ^ CLOSER), 'precondition: the relay is closer to the closer node than this node is');
  ok((RELAY ^ TOPIC) > (SELF ^ TOPIC), 'precondition: the relay is not closer to the topic, so this node is the greedy terminal');
  const v = await m._verifyTerminalOwnership(TOPIC);
  ok(v.kind === 'closer' && v.rootHex === idHex(CLOSER), "the check answers 'closer' with the 2-hop node (b14fecb: 'inconclusive')", JSON.stringify(v));
  await subscribeOnce(m);
  ok(!stats().runaway, 'no loop', JSON.stringify(stats()));
  ok(sent.some((s) => s.type === T.SUB && s.target === CLOSER), 'the SUB is sent via the closer node');
  const role = m.axonRoles.get(TOPIC);
  ok(!(role && role.isRoot), 'and this node does not root it');
}

console.log(`\n${fail ? '✗' : '✓'} fence_term_verify_unreachable: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
