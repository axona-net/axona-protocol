// =====================================================================
// fence_root_forward_return.mjs — a send aimed at a closer root that comes
// back to the sender is a failure for that root, not a new message (4.157.0).
//
// THE BUG (Orion 2165944e, reproduced offline on b14fecb). With
// SUB_TERMINAL_VERIFY OFF, a node with no live channel that still holds a fresh
// beacon record naming a strictly-closer root sends SUB / PUB / KILL /
// METRICSON "via" that root. The route ends back here (nobody is closer to the
// root), the waypoint reads dead, _reroute pops it and sends the copy bare, the
// bare copy ends here, the closer-root gate names the same root, and the copy
// is sent via it again:
//
//   handler -> _liveCloserRoot -> _deferToRoot/_forwardToRoot (via R)
//           -> route ends at self -> 'reroute' -> bare -> handler -> ...
//
// No timer between steps. _forwardToRoot never invalidates the record: a
// delivery back to self is 'consumed', and 'consumed' not attributed to R
// returns early. The beacon record keeps the gate open for up to 1.5×BEACON_MS
// (verified) or its TTL (synaptome-listed neighbour).
//
// THE FIX. Every via-send to a closer root carries a nonce (rootFwd). A copy
// that comes back to its sender carrying that sender's nonce, for the same
// topic and verb, is the forward returning: the matching beacon record and
// root hint are dropped (same generation only), the copy is logged
// undeliverable, and it is not sent again. The sender's retry or renewal
// carries the message. Same shape as Aster's holdFwd bound (_holdIntercept).
//
// What this pins:
//   1. Precondition: the check is off and the gate names the closer root.
//   2. For each of SUB, PUB, KILL, METRICSON: the handler is entered a bounded
//      number of times (no runaway; the old code hit the 500 cap).
//   3. The stale beacon record is dropped after the return.
//   4. A forward that does NOT come back (the root consumed it) keeps the
//      record (no false invalidation).
// Offline, fake DHT: a message to a target this node has no channel to ends
// here as terminal; a runaway is cut at 500 deliveries and reported.
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { idHex } from '../src/pubsub/ids.js';
import { sealTestDht } from './lib/testCapability.mjs';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => { if (cond) { pass++; console.log(`  ✓ ${name}`); } else { fail++; console.log(`  ✗ ${name} ${extra}`); } };

const SELF   = 0x80aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaan;
const TOPIC  = 0x80c0000000000000000000000000000000000000000000000000000000000000c0n;
const CLOSER = 0x80c0000000000000000000000000000000000000000000000000000000000000c1n;
const OTHER  = 0x80bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbn;
const CAP = 500;

function mkManager({ rootConsumes = false } = {}) {
  const handlers = new Map();
  let deliveries = 0, runaway = false; const sent = [];
  const dht = {
    getSelfId: () => SELF,
    verdictsSupported: false,
    onRoutedMessage: (type, h) => { handlers.set(type, h); },
    neighbors: () => [],                // cut off: no bound peers
    isLive: () => false,
    findKClosest: async () => [SELF],
    lookup: async () => ({ path: [SELF] }),
    routeMessage: (target, type, payload) => {
      sent.push({ target, type, payload: { ...payload } });
      if (rootConsumes && target === CLOSER) return { consumed: true, atNode: CLOSER, hops: 2 };
      if (++deliveries > CAP) { runaway = true; return { consumed: false, exhausted: true }; }
      const h = handlers.get(type);
      return Promise.resolve(h ? h(payload, { targetId: SELF, originId: idHex(SELF), isTerminal: true, hopCount: 0 }) : undefined)
        .then((r) => (r === 'consumed' ? { consumed: true, atNode: SELF, hops: 0 } : { consumed: false, atNode: SELF, hops: 0, terminal: true }));
    },
  };
  const prev = process.env.SUB_TERMINAL_VERIFY;
  delete process.env.SUB_TERMINAL_VERIFY;
  const m = new AxonaManager({ dht: sealTestDht(dht), now: () => Date.now() });
  if (prev !== undefined) process.env.SUB_TERMINAL_VERIFY = prev;
  m._log = () => {};
  m._rootClaim.meshBare = () => false;
  const now = Date.now();
  m._rootBeacons.set(TOPIC, { root: idHex(CLOSER), at: now, exp: now + 90_000, verified: true });
  return { m, handlers, sent, stats: () => ({ deliveries, runaway }) };
}

const settle = async () => {
  for (let i = 0; i < 50; i++) await new Promise((r) => setImmediate(r));
  await new Promise((r) => setTimeout(r, 50));
};

const payloadFor = (type) => {
  const base = { topicId: idHex(TOPIC) };
  if (type === T.SUB) return { ...base, subscriberId: idHex(OTHER) };
  if (type === T.PUB) return { ...base, json: JSON.stringify({ k: 1 }) };
  if (type === T.KILL) return { ...base, msgId: 'ab'.repeat(32) };
  return { ...base, subscriberId: idHex(OTHER) };
};

for (const [name, type] of [['SUB', T.SUB], ['PUB', T.PUB], ['KILL', T.KILL], ['METRICSON', T.METRICSON]]) {
  console.log(`\n[${name}] check off, no live channel, fresh beacon names a closer root`);
  const { m, handlers, stats } = mkManager();
  ok(m._subTerminalVerify === false, 'precondition: the check is off');
  ok(m._liveCloserRoot(TOPIC, { requireReachable: type !== T.PUB && type !== T.KILL }) === idHex(CLOSER), 'precondition: the gate names the closer root');
  const h = handlers.get(type);
  ok(typeof h === 'function', 'precondition: handler registered');
  try { await h(payloadFor(type), { targetId: SELF, originId: idHex(OTHER), isTerminal: true, hopCount: 1 }); } catch (e) { ok(false, 'handler threw', String(e)); }
  await settle();
  const s = stats();
  ok(!s.runaway, 'the message is not re-delivered without bound', JSON.stringify(s));
  ok(s.deliveries <= 4, 'the handler is entered a few times at most', JSON.stringify(s));
  ok(!m._rootBeacons.has(TOPIC), 'the beacon record for the unreachable root is dropped');
}

console.log('\n[control] the closer root consumes the forward: nothing comes back');
{
  const { m, handlers, sent, stats } = mkManager({ rootConsumes: true });
  await handlers.get(T.PUB)(payloadFor(T.PUB), { targetId: SELF, originId: idHex(OTHER), isTerminal: true, hopCount: 1 });
  await settle();
  ok(sent.some((x) => x.target === CLOSER && x.type === T.PUB), 'the PUB is sent via the closer root');
  ok(!stats().runaway && stats().deliveries === 0, 'no copy comes back', JSON.stringify(stats()));
  ok(m._rootBeacons.has(TOPIC), 'the beacon record is kept (no false invalidation)');
}

// RF-2 (Orion 82dad175): a copy carrying a FOREIGN holdFwd (it left a held
// node, or a peer set the field) at a node that holds nothing must still be
// tagged and bounded. 6c91a83 skipped the rootFwd tag on the wire field and
// ran to the cap on PUB and KILL.
for (const [name, type] of [['PUB', T.PUB], ['KILL', T.KILL]]) {
  console.log(`\n[RF-2 ${name}] foreign holdFwd on the wire, this node holds nothing`);
  const { m, handlers, stats } = mkManager();
  ok(!m._rootClaim.holdFor(TOPIC), 'precondition: this node holds no step-down for the topic');
  await handlers.get(type)({ ...payloadFor(type), holdFwd: 'f0f0f0f0f0f0f0f0' }, { targetId: SELF, originId: idHex(OTHER), isTerminal: true, hopCount: 1 });
  await settle();
  const s = stats();
  ok(!s.runaway && s.deliveries <= 4, 'bounded despite the foreign holdFwd', JSON.stringify(s));
}

// RF-1 (Orion 82dad175): a write flight opened on a forward that arrived
// carrying ANOTHER node's rootFwd must not store that nonce. 6c91a83 kept it,
// so a promotion back to that node inside ROOT_FWD_MS read there as its own
// forward returning and was dropped.
console.log('\n[RF-1] a flight never stores an upstream rootFwd');
{
  const { m } = mkManager({ rootConsumes: true });
  const fwdIn = { topicId: idHex(TOPIC), json: JSON.stringify({ msgId: 'cd'.repeat(32), k: 1 }), rootFwd: 'a1a1a1a1a1a1a1a1' };   // writeMsgId reads json.msgId
  m._forwardToRoot(TOPIC, T.PUB, fwdIn, idHex(CLOSER));
  await settle();
  let stored = 0, withNonce = 0;
  for (const f of (m._writeFlights?.values?.() ?? [])) for (const e of f.entries.values()) { stored++; if (e.payload && 'rootFwd' in e.payload) withNonce++; }
  ok(stored >= 1, 'precondition: a write flight was opened', `stored=${stored}`);
  ok(withNonce === 0, 'the stored payload carries no rootFwd', `withNonce=${withNonce}`);
}

console.log(`\n${fail ? '✗' : '✓'} fence_root_forward_return: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
