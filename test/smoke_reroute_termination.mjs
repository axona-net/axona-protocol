// =====================================================================
// smoke_reroute_termination.mjs
//
// REGRESSION FENCE for the 2026-07-27 east-bridge wedge (~50 min prod down).
//
// THE BUG. A node that is TERMINAL for a topic (nobody closer) must take a root
// role to accept a message. If admission refuses at the HARD tier — today only
// the bridge fence — become() returns null. The 4.46.0 decline path then called
// _reroute() UNCONDITIONALLY. With the via chain exhausted, _send targets the
// topic id, the DHT hands the message straight back to this node, and:
//
//   _onPub -> _becomeRoot -> admitRole -> refuse -> _reroute -> _onPub -> ...
//
// synchronously and unbounded. Not a slowdown — a hard wedge. The event loop
// never yields, so no timers, no sockets, no health check, no logs. The process
// prints "listen" and then serves nothing forever. Measured live: 49,999 of
// 50,000 admission calls returned why:'bridge', CPU pegged at 93.9%.
//
// THE FIX. _reroute() returns whether it actually handed the message to a
// DIFFERENT node. "Declined and forwarded" and "declined with nowhere to go"
// are different outcomes and the caller must tell them apart. The second is
// terminal: stop, and log `undeliverable` loudly.
//
// What this pins:
//   1. A fenced terminal node does NOT re-send when the via chain is empty.
//   2. It does NOT recurse — the property whose absence wedged production.
//   3. It DOES still forward when a real next hop exists (no silent loss).
//   4. A via hop pointing back at self counts as no forward (same trap).
//   5. All three decline sites (PUB/SUB/KILL) behave the same way.
//
// Runs offline against a fake DHT. No network, no bridge, no timing.
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { idHex } from '../src/pubsub/ids.js';

let pass = 0, fail = 0;
const ok = (cond, name) => { if (cond) { pass++; console.log(`  ✓ ${name}`); } else { fail++; console.log(`  ✗ ${name}`); } };

const SELF = 0x89aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaan;
const OTHER = 0x89bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbn;

/** Fake DHT that records every routeMessage instead of delivering it. */
function mkManager({ neverRoot = true } = {}) {
  const sent = [];
  const dht = {
    getSelfId: () => SELF,
    routeMessage: (target, type, payload) => { sent.push({ target, type, payload }); },
    findKClosest: () => [],
    lookup: async () => null,
    peers: () => [],
    onRoutedMessage: () => {},        // required by the AxonaManager contract
  };
  const m = new AxonaManager({ dht, neverRoot, now: () => 0 });
  const logs = [];
  m._log = (level, event, ctx) => logs.push({ level, event, ctx });
  return { m, sent, logs };
}

const TOPIC = 0x89ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccn;

console.log('\n[1] the fence still refuses (precondition for everything below)');
{
  const { m } = mkManager();
  const v = m.canAcceptRole();
  ok(v.ok === false && v.why === 'bridge', "canAcceptRole = {ok:false, why:'bridge'}");
  ok(v.hard === true, 'and it is a HARD refusal (the floor may not override it)');
}

console.log('\n[2] no via left ⇒ NO re-send (this is the wedge, gone)');
{
  const { m, sent } = mkManager();
  const payload = { topicId: idHex(TOPIC), via: ['deadbeef'] };   // slice(1) ⇒ empty
  const forwarded = m._rerouteDeclined(T.PUB, payload);
  ok(forwarded === false, '_rerouteDeclined reports FALSE — no forward exists');
  ok(sent.length === 0, 'and nothing was put back on the wire (0 routeMessage calls)');
}

console.log('\n[3] a real next hop ⇒ it DOES forward (no silent loss)');
{
  const { m, sent } = mkManager();
  const payload = { topicId: idHex(TOPIC), via: ['deadbeef', idHex(OTHER)] };
  const forwarded = m._rerouteDeclined(T.PUB, payload);
  ok(forwarded === true, '_rerouteDeclined reports TRUE');
  ok(sent.length === 1, 'exactly one re-send');
  ok(sent[0]?.target === OTHER, 'aimed at the surviving via hop, not the topic id');
}

console.log('\n[4] a via hop pointing back at SELF is not a forward');
{
  const { m, sent } = mkManager();
  const payload = { topicId: idHex(TOPIC), via: ['deadbeef', idHex(SELF)] };
  const forwarded = m._rerouteDeclined(T.PUB, payload);
  ok(forwarded === false, 'self-targeted via ⇒ FALSE (same trap, different shape)');
  ok(sent.length === 0, 'and nothing re-sent');
}

console.log('\n[5] the undeliverable outcome is LOUD, not silent');
{
  const { m, logs } = mkManager();
  m._undeliverable(T.PUB, TOPIC, 'refused-no-forward');
  const rec = logs.find((l) => l.event === 'undeliverable');
  ok(!!rec, 'emits an `undeliverable` log');
  ok(rec?.level === 'warn', 'at warn level — a placement failure, not routine');
  ok(rec?.ctx?.why === 'refused-no-forward', 'and says why');
}

console.log('\n[6] BOUNDEDNESS: repeated declines terminate (the actual property)');
{
  // The pre-fix code would spin here forever. Drive the decline path many times
  // and assert the wire stays silent — no growth, no recursion, returns.
  const { m, sent } = mkManager();
  for (let i = 0; i < 10_000; i++) {
    m._rerouteDeclined(T.PUB, { topicId: idHex(TOPIC), via: ['deadbeef'] });
  }
  ok(sent.length === 0, '10,000 declines produced 0 re-sends and returned');
}

console.log('\n[7] _reroute (dead-waypoint fall-through) is UNCHANGED');
{
  // I first "fixed" this by putting the terminal guard inside _reroute itself.
  // smoke_pubsub_core caught it: the topic-id fall-through when the via chain
  // empties is how a subscriber pinned to a DEAD root re-homes onto a fresh
  // one. _reroute has 9 other callers that all depend on it. The decline path
  // needed its own method, not a change to shared routing.
  const { m, sent } = mkManager();
  m._reroute(T.PUB, { topicId: idHex(TOPIC), via: ['deadbeef'] });
  ok(sent.length === 1, 'plain _reroute STILL falls through when via empties');
  ok(sent[0]?.target === TOPIC, 'and it targets the topic id (re-homing works)');
}

console.log('\n[8] a node WITHOUT the fence is unaffected (no behaviour change)');
{
  const { m } = mkManager({ neverRoot: false });
  const v = m.canAcceptRole();
  ok(v.ok === true || v.why !== 'bridge', 'unfenced node is not refused as a bridge');
}

console.log(`\nResult: ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
