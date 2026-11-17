#!/usr/bin/env node
// fence_pub_hint_live.mjs — 4.154.0, two options, both default OFF.
//   PUB_HINT_LIVE  a publish is pinned to its root hint only when the hint is a
//                  fresh beacon's root or a peer with an open, healthy channel,
//                  and never to a node on hold. A pinned publish this node is
//                  subscribed to and does not see come back puts the hint on
//                  hold and drops the cached guess.
//   STEPPED_HOLD   a root evicted for no receipt is tombstoned 2 minutes, 10 on
//                  a repeat, where it was 10 always.
// Hold lengths are David's (2026-11-16): two minutes, ten if it happens again
// with the same node.
//
// Seen on production 2026-11-16: a seat attached to a topic pinned every
// publish for thirteen hours to a relay that two fleet rolls had replaced.
//
// The decision is made in the manager from the adapter's liveness reader, so
// the manager seam is the right place to test it. The reader itself is tested
// at the end on REAL AxonaPeers.
//
// Run: node test/fence_pub_hint_live.mjs
import assert from 'node:assert/strict';
import { AxonaManager }             from '../src/pubsub/AxonaManager.js';
import { T, HOLD_FIRST_MS, HOLD_REPEAT_MS, BEACON_TTL_MS, BEACON_MS, PENDING_PUB_MAX_TRIES } from '../src/pubsub/constants.js';
import { ROOT_TOMBSTONE_TTL_MS }    from '../src/pubsub/writeFlight.js';
import { idHex, idBig }             from '../src/pubsub/ids.js';
import { sealTestDht }              from './lib/testCapability.mjs';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';

let n = 0;
const check = (name, ok, extra = '') => { assert(ok, `${name} ${extra}`); n++; console.log('PASS ' + name); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const id = (x) => (0x89n << 256n) | BigInt(x);
const SELF = id(0x1000), DEAD = id(0x2000), LIVE = id(0x3000), ROOT = id(0x4000), TOPIC = id(0x5000);
const hx = (b) => idHex(b).toLowerCase();

function mk({ pubHintLive = false, steppedHold = false, live = [], withReader = true } = {}) {
  const clock = { t: 1_800_000_000_000 };
  const sent = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => { sent.push({ target, type, via: [...(payload.via || [])] }); return { consumed: true, hops: 1 }; },
    neighbors: () => [],
    bridgeId: () => null,
    findKClosest: async () => [],
  };
  if (withReader) dht.isLive = (b) => live.includes(b);
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, pubHintLive, steppedHold });
  am.nodeId = SELF;
  const logs = []; am.setLogSink((l, ev, ctx) => logs.push({ ev, ctx }));
  // An ATTACHED node with a cached guess: the state the production seat was in.
  const hint = (b) => { am._upstream.set(TOPIC, [hx(b)]); am._rootHint.set(TOPIC, { via: hx(b), at: clock.t }); };
  const subscribe = () => am.mySubscriptions.set(TOPIC, { createdAt: clock.t, since: 0, lastRenewSent: clock.t, interval: 1000 });
  const pub = (msgId) => am.pubsubPublish(TOPIC, JSON.stringify({ msgId, ts: clock.t }));
  const last = () => sent[sent.length - 1];
  const ev = (name) => logs.filter((l) => l.ev === name || l.ev === 'pubsub:' + name);
  return { am, clock, sent, logs, hint, subscribe, pub, last, ev };
}
const beacon = (s, rootBig) => s.am._rootBeacons.set(TOPIC, { root: hx(rootBig), at: s.clock.t, exp: s.clock.t + BEACON_TTL_MS, epoch: 1 });

check('the holds are two minutes, then ten', HOLD_FIRST_MS === 120_000 && HOLD_REPEAT_MS === 600_000);
check('both options are off unless asked for', (() => { const s = mk(); return s.am._pubHintLive === false && s.am._steppedHold === false; })());

// ── off: nothing changes ────────────────────────────────────────────────────
{
  const s = mk(); s.hint(DEAD); s.subscribe();
  s.pub('m1');
  check('off: a publish is pinned to the cached guess, alive or not (the defect)', s.last().via.join() === hx(DEAD) && s.last().target === DEAD);
  for (let i = 0; i < 3; i++) { s.clock.t += 5000; s.am.refreshTick?.(); }
  const retries = s.sent.filter((x) => x.type === T.PUB);
  check('off: every retry goes to the same node', retries.length >= 2 && retries.every((x) => x.via.join() === hx(DEAD)), JSON.stringify(retries.map((x) => x.via)));
  check('off: no hold is ever recorded', s.am._holds.size === 0 && s.ev('pub-hint-held').length === 0);
}

// ── on: the address ─────────────────────────────────────────────────────────
{
  const s = mk({ pubHintLive: true, live: [LIVE] }); s.hint(DEAD);
  s.pub('m2');
  check('on: a cached guess with no open channel is not pinned; the publish goes to the bare topic id', s.last().via.length === 0 && s.last().target === TOPIC);
  check('on: skipping costs no strike; nothing was tried', s.am._holds.size === 0 && s.am._pubHintStats['not-live'] === 1);
}
{
  const s = mk({ pubHintLive: true, live: [LIVE] }); s.hint(LIVE);
  s.pub('m3');
  check('on: a cached guess with an open, healthy channel is pinned, as before', s.last().via.join() === hx(LIVE));
}
{
  const s = mk({ pubHintLive: true, live: [] }); s.hint(DEAD); beacon(s, ROOT);
  s.pub('m4');
  check('on: a fresh beacon\'s root is pinned though it is no direct neighbour', s.last().via.join() === hx(ROOT));
  s.clock.t += BEACON_TTL_MS + 1; s.hint(DEAD);
  s.pub('m5');
  check('on: once the beacon has lapsed the stale guess is not pinned', s.last().via.length === 0);
}
{
  const s = mk({ pubHintLive: true, withReader: false }); s.hint(DEAD);
  s.pub('m6');
  check('on: an adapter with no liveness reader cannot say, so its hint is kept', s.last().via.join() === hx(DEAD));
}

// ── on: a pinned publish that never comes back ──────────────────────────────
{
  const s = mk({ pubHintLive: true, live: [LIVE] }); s.hint(LIVE); s.subscribe();
  s.pub('m7');
  check('pinned to a live peer', s.last().via.join() === hx(LIVE));
  s.clock.t += 5000; s.am.refreshTick();
  const held = s.ev('pub-hint-held');
  check('on: a tick with no sight of the message puts the hint on hold for two minutes', held.length === 1 && held[0].ctx.holdS === 120 && held[0].ctx.strikes === 1, JSON.stringify(held));
  check('on: the cached guess is dropped', !s.am._rootHint.has(TOPIC) || s.am._rootHint.get(TOPIC).via !== hx(LIVE));
  check('on: the retry in the same tick goes to the bare topic id', s.last().type === T.PUB && s.last().via.length === 0 && s.last().target === TOPIC);
  s.hint(LIVE);                                         // the background refresh picks the same node again
  s.pub('m8');
  check('on: a node on hold is not pinned even if the refresh names it again', s.last().via.length === 0 && s.am._pubHintStats.held >= 1);
  s.clock.t += 5000; s.am.refreshTick();
  check('on: one failure, one strike: later ticks of the same failure add none', s.ev('pub-hint-held').length === 1);

  s.clock.t += HOLD_FIRST_MS;                           // the two minutes pass
  s.am._pendingPub.clear();
  s.hint(LIVE);
  s.pub('m9');
  check('on: after two minutes the node is used again', s.last().via.join() === hx(LIVE));
  s.clock.t += 5000; s.am.refreshTick();
  const again = s.ev('pub-hint-held');
  check('on: a second failure of the same node holds it for ten minutes', again.length === 2 && again[1].ctx.holdS === 600 && again[1].ctx.strikes === 2, JSON.stringify(again));
  s.clock.t += HOLD_FIRST_MS + 1000; s.am._pendingPub.clear(); s.hint(LIVE);
  s.pub('m10');
  check('on: it is still set aside after two minutes of the ten', s.last().via.length === 0);
  s.clock.t += HOLD_REPEAT_MS; s.am._pendingPub.clear(); s.hint(LIVE);
  s.pub('m11');
  check('on: and used again after the ten', s.last().via.join() === hx(LIVE));
}
{
  // forgotten: a failure long after the last one starts again at two minutes
  const s = mk({ pubHintLive: true, live: [LIVE] }); s.hint(LIVE); s.subscribe();
  s.pub('f1'); s.clock.t += 5000; s.am.refreshTick();
  s.clock.t += HOLD_REPEAT_MS + 60_000; s.am._pendingPub.clear(); s.hint(LIVE);
  s.pub('f2'); s.clock.t += 5000; s.am.refreshTick();
  const h = s.ev('pub-hint-held');
  check('on: a failure more than ten minutes after the last is a first failure again', h.length === 2 && h[1].ctx.holdS === 120 && h[1].ctx.strikes === 1, JSON.stringify(h));
}
{
  const s = mk({ pubHintLive: true, live: [LIVE] }); s.hint(LIVE);   // NOT subscribed
  s.pub('u1');
  for (let i = 0; i < PENDING_PUB_MAX_TRIES + 1; i++) { s.clock.t += 5000; s.am.refreshTick(); }
  check('on: a publisher that is not subscribed never sees its own message; silence there holds nobody', s.ev('pub-hint-held').length === 0 && s.am._holds.size === 0);
}
{
  const s = mk({ pubHintLive: true, live: [LIVE] }); s.hint(LIVE); s.subscribe();
  s.pub('c1');
  s.am._confirmPending(TOPIC, 'c1');                    // the message came back
  s.clock.t += 5000; s.am.refreshTick();
  check('on: a publish that comes back holds nobody', s.ev('pub-hint-held').length === 0);
}
{
  const s = mk({ pubHintLive: true, live: [] }); s.hint(DEAD); s.subscribe();
  s.am.pubsubKill(TOPIC, { msgId: 'k1' });
  check('on: a kill takes the same address rule', s.last().type === T.KILL && s.last().via.length === 0);
}

// ── STEPPED_HOLD: the tombstone of a root evicted for no receipt ────────────
{
  const flight = (s) => ({ topicBig: TOPIC, rootHex: hx(ROOT), epoch: 1, entries: new Map(), stage: 'probing', rounds: 2, deadline: 0, ackTo: hx(SELF), flightNonce: 'x' });
  const off = mk(); off.am._writeFlights = new Map();
  off.am._flightEvict('k', flight(off));
  check('off: an evicted root is tombstoned for ten minutes, as before', off.am._rootTombstones.get(TOPIC).exp - off.clock.t === ROOT_TOMBSTONE_TTL_MS && ROOT_TOMBSTONE_TTL_MS === 600_000);

  const on = mk({ steppedHold: true }); on.am._writeFlights = new Map();
  on.am._flightEvict('k', flight(on));
  check('on: the first eviction tombstones for two minutes', on.am._rootTombstones.get(TOPIC).exp - on.clock.t === HOLD_FIRST_MS);
  check('on: two minutes still outlives a beacon record', HOLD_FIRST_MS > BEACON_TTL_MS + BEACON_MS);
  on.clock.t += HOLD_FIRST_MS + 1000;
  check('on: after two minutes the root is no longer tombstoned', on.am._rootTombstoned(TOPIC, hx(ROOT), 1) === false);
  on.am._flightEvict('k', flight(on));
  check('on: evicted again, the same root on the same topic is tombstoned for ten', on.am._rootTombstones.get(TOPIC).exp - on.clock.t === HOLD_REPEAT_MS);
  const other = { ...flight(on), rootHex: hx(LIVE) };
  on.clock.t += 1000; on.am._flightEvict('k2', other);
  check('on: a different root starts at two minutes', on.am._rootTombstones.get(TOPIC).exp - on.clock.t === HOLD_FIRST_MS);
}

// ── the liveness reader, on real peers ──────────────────────────────────────
{
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 50, EPSILON: 0 });
  const mkPeer = async (i) => {
    const ident = await createNodeIdentity({ lat: 40 + i * 0.01, lng: -74 });
    const transport = simTransport({ network: net, identity: ident, heartbeatMs: 0, sendTimeoutMs: 400 });
    await transport.start(ident.id);
    const node = new NeuronNode({ id: fromHex(ident.id), lat: 40 + i * 0.01, lng: -74 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: ident, transport });
    await peer.start();
    return { peer, transport, am: peer._requireAxonaManager('fence'), big: fromHex(ident.id), hex: ident.id };
  };
  const A = await mkPeer(0), B = await mkPeer(1), C = await mkPeer(2);
  await A.transport.openConnection(B.hex);
  await sleep(20);
  check('real peers: the adapter has a liveness reader', typeof A.am.dht.isLive === 'function');
  check('real peers: a peer with an open channel reads live', A.am.dht.isLive(B.big) === true);
  check('real peers: a peer never connected reads not live', A.am.dht.isLive(C.big) === false);
  check('real peers: self reads not live', A.am.dht.isLive(A.big) === false);
  try { await B.peer.stop?.(); await B.transport.stop?.(); } catch { /* */ }
  await sleep(60);
  check('real peers: after the peer has gone it reads not live', A.am.dht.isLive(B.big) === false);
  try { await A.peer.stop?.(); await C.peer.stop?.(); } catch { /* */ }
}
console.log(`\n${n} checks passed`);
process.exit(0);
