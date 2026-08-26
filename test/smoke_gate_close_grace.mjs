// smoke_gate_close_grace.mjs — v4.68.0 deferred refusal-close (closeGraceMs).
// The gate's refusal-time channel close, deferred by an opt-in grace window:
//   1. graceMs=0 (default): close fires immediately — v4.67.1 behavior.
//   2. graceMs>0: close DEFERRED; fires after the window when not admitted.
//   3. RESCUE: admitted-meanwhile peer's close is skipped at fire time.
//   4. BOUND: pending closes capped at graceMaxPending; overflow closes OLDEST
//      immediately (state and channel budget stay bounded under churn).
//
// Run: node test/smoke_gate_close_grace.mjs
// v4.68.1 additions (Aster review 1c11a94e):
//   5. CONFIG: invalid closeGraceMs/graceMaxPending normalize before use —
//      zero/negative/fractional/non-finite/non-numeric fail safe to grace
//      OFF; fractional closeGraceMs floors. The dormant path stays correct.
//   6. HEADROOM: deferral capacity derives from live channel headroom
//      against node.maxConnections — no headroom, immediate close.
//   7. LIFECYCLE: stop() and leave() clear every grace timer; nothing fires
//      after teardown.
//   8. BILATERAL (real sim transport, two peers): both-armed rescue keeps
//      the channel; one-armed counterpart degrades to immediate close with
//      idempotent late expiry; simultaneous expiry closes both ends clean.
import { AxonaPeer, AxonaDomain, NeuronNode } from '../src/index.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { fromHex } from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (l, c) => { console.log(`  ${c ? '✓' : '✗'} ${l}`); c ? passed++ : failed++; };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

function makePeer({ graceMs = 0, maxPending = 64, cap = 2 } = {}) {
  const closed = [];
  const transport = {
    closeConnection: async (id) => { closed.push(id); },
    // minimal surface the ctor path touches
    boundPeers: () => [],
  };
  const node = new NeuronNode({ id: 0x89_0000_0001n, lat: 0, lng: 0 });
  node.transport = transport;
  node._maxSynaptome = cap;
  const peer = new AxonaPeer({
    domain: new AxonaDomain({ k: 20 }),
    node,
    transport,
    admissionGate: { kNear: 1, sparseFloor: 1, closeGraceMs: graceMs, graceMaxPending: maxPending },
  });
  return { peer, node, closed };
}

// Fill the table to cap so further sponsors are REFUSED at the gate.
function fillToCap(node, n) {
  for (let i = 1; i <= n; i++) node.synaptome.set(0x89_1000_0000n + BigInt(i), { peerId: 0x89_1000_0000n + BigInt(i), weight: 0.9, stratum: 1 });
}

console.log('gate close-grace smoke\n');

// ── 1. default: immediate close ──────────────────────────────────────
{
  const { peer, node, closed } = makePeer({ graceMs: 0 });
  fillToCap(node, 8);
  await peer._seedSynaptomeWithSponsor(0x89_2000_0001n);
  await wait(30);
  check('1 graceMs=0: refusal closes immediately', closed.length === 1);
  check('1 no pending state at graceMs=0', peer._gracePending.size === 0);
}

// ── 2. deferred: fires after the window ──────────────────────────────
{
  const { peer, node, closed } = makePeer({ graceMs: 150 });
  fillToCap(node, 8);
  await peer._seedSynaptomeWithSponsor(0x89_2000_0002n);
  check('2 refusal does NOT close inside the window', closed.length === 0);
  check('2 pending entry exists', peer._gracePending.size === 1);
  await wait(260);
  check('2 close fires after the window', closed.length === 1);
  check('2 pending drained', peer._gracePending.size === 0);
}

// ── 3. rescue: admitted-meanwhile close is skipped ───────────────────
{
  const { peer, node, closed } = makePeer({ graceMs: 150 });
  fillToCap(node, 8);
  const s = 0x89_2000_0003n;
  await peer._seedSynaptomeWithSponsor(s);
  check('3 deferred (pending=1)', peer._gracePending.size === 1);
  node.synaptome.set(s, { peerId: s, weight: 0.5, stratum: 1 });   // admitted meanwhile
  await wait(260);
  check('3 RESCUE: admitted peer never closed', closed.length === 0);
  check('3 pending drained after rescue', peer._gracePending.size === 0);
}

// ── 4. bound: overflow closes oldest immediately ─────────────────────
{
  const { peer, node, closed } = makePeer({ graceMs: 5000, maxPending: 3 });
  fillToCap(node, 8);
  for (let i = 1; i <= 5; i++) await peer._seedSynaptomeWithSponsor(0x89_3000_0000n + BigInt(i));
  check('4 pending bounded at graceMaxPending', peer._gracePending.size <= 3);
  check('4 overflow closed OLDEST immediately (2 of 5)', closed.length === 2
    && closed[0] === 0x89_3000_0001n && closed[1] === 0x89_3000_0002n);
  for (const h of peer._gracePending.values()) clearTimeout(h);
}

// ── 5. config normalization (v4.68.1) ────────────────────────────────
{
  // graceMaxPending 0 = zero deferral capacity = grace OFF: immediate
  // close, empty map, and the overflow loop is never entered.
  const a = makePeer({ graceMs: 500, maxPending: 0 });
  fillToCap(a.node, 8);
  await a.peer._seedSynaptomeWithSponsor(0x89_4000_0001n);
  check('5 graceMaxPending=0: grace off, immediate close', a.closed.length === 1 && a.peer._gracePending.size === 0);

  for (const [label, graceMs] of [['negative', -5], ['NaN', NaN], ['string', 'x'], ['Infinity', Infinity]]) {
    const b = makePeer({ graceMs });
    fillToCap(b.node, 8);
    await b.peer._seedSynaptomeWithSponsor(0x89_4000_0002n);
    check(`5 closeGraceMs ${label}: normalized off, immediate close`, b.closed.length === 1 && b.peer._gracePending.size === 0);
  }

  const c = makePeer({ graceMs: 150.9 });
  check('5 fractional closeGraceMs floors', c.peer._gateCfg.closeGraceMs === 150);

  for (const [label, maxPending] of [['negative', -3], ['fractional', 2.5], ['NaN', NaN]]) {
    const d = makePeer({ graceMs: 500, maxPending });
    fillToCap(d.node, 8);
    await d.peer._seedSynaptomeWithSponsor(0x89_4000_0003n);
    check(`5 graceMaxPending ${label}: fails safe to grace off`, d.closed.length === 1 && d.peer._gracePending.size === 0);
  }
}

// ── 6. headroom: deferral bounded by node.maxConnections ─────────────
{
  const { peer, node, closed } = makePeer({ graceMs: 5000 });
  fillToCap(node, 8);
  node.maxConnections = 9;               // kept 8 + pending 0 + 1 = 9 ≤ 9 → defer ok
  await peer._seedSynaptomeWithSponsor(0x89_5000_0001n);
  check('6 within headroom: refusal defers', peer._gracePending.size === 1 && closed.length === 0);
  // v4.68.2 regression (Aster …-08): at EXACT headroom, a duplicate refusal
  // for the already-graced sponsor holds zero incremental capacity — it must
  // neither close the graced channel nor grow the map, and the ORIGINAL
  // timer is retained (no refresh).
  const firstHandle = peer._gracePending.get(0x89_5000_0001n);
  await peer._seedSynaptomeWithSponsor(0x89_5000_0001n);
  check('6 duplicate refusal at bound: graced channel NOT closed', closed.length === 0);
  check('6 duplicate refusal: map unchanged, original timer retained',
    peer._gracePending.size === 1 && peer._gracePending.get(0x89_5000_0001n) === firstHandle);
  await peer._seedSynaptomeWithSponsor(0x89_5000_0002n);   // 8 + 1 + 1 = 10 > 9 → immediate
  check('6 no headroom: DIFFERENT sponsor closes immediately', closed.length === 1 && closed[0] === 0x89_5000_0002n);
  check('6 pending unchanged past the cap', peer._gracePending.size === 1);
  for (const h of peer._gracePending.values()) clearTimeout(h);
  peer._gracePending.clear();
}

// ── real-transport helper for 7 & 8 ──────────────────────────────────
async function makeRealPeer(net, domain, lat, lng, { graceMs = 0 } = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  node._maxSynaptome = 2;
  const peer = new AxonaPeer({
    domain, node, nodeIdentity: id, transport,
    admissionGate: { kNear: 1, sparseFloor: 1, closeGraceMs: graceMs, graceMaxPending: 64 },
  });
  await peer.start();
  return { peer, node, transport, big: fromHex(id.id), hex: id.id };
}
const fillReal = (node) => fillToCap(node, 8);
// Deterministic refusal on the REAL bind path: the gate's swap margin
// compares band occupancy, and a nearby-geo real peer lands in an emptier
// band than synthetic group-0 fill — it would be legitimately swap-ADMITTED
// (measured). Filling with ids in the COUNTERPART'S band makes the
// candidate's own band the densest (candCount 8, margin needs 10) → refuse.
const fillNear = (node, otherBig) => {
  for (let i = 1; i <= 8; i++) {
    const e = otherBig ^ BigInt(i * 2 + 1);
    node.synaptome.set(e, { peerId: e, weight: 0.9, stratum: 1 });
  }
};

// ── 7. lifecycle: stop()/leave() clear grace timers ──────────────────
{
  const net = new SimNetwork();
  const domain = new AxonaDomain({ k: 20 });
  const a = await makeRealPeer(net, domain, 10, 10, { graceMs: 60000 });
  fillReal(a.node);
  await a.peer._seedSynaptomeWithSponsor(0x89_6000_0001n);
  check('7 pending before stop()', a.peer._gracePending.size === 1);
  await a.peer.stop();
  check('7 stop() drains grace map', a.peer._gracePending.size === 0);

  const b = await makeRealPeer(net, domain, 20, 20, { graceMs: 60000 });
  fillReal(b.node);
  await b.peer._seedSynaptomeWithSponsor(0x89_6000_0002n);
  check('7 pending before leave()', b.peer._gracePending.size === 1);
  await b.peer.leave({ timeoutMs: 100 });
  check('7 leave() drains grace map', b.peer._gracePending.size === 0);
}

// ── 8. bilateral, two peers on the real sim transport ────────────────
{
  const net = new SimNetwork();
  const domain = new AxonaDomain({ k: 20 });

  // (a) both armed + rescued: channel survives the window on both ends.
  const A = await makeRealPeer(net, domain, 30, 30, { graceMs: 400 });
  const B = await makeRealPeer(net, domain, 31, 31, { graceMs: 400 });
  fillNear(A.node, B.big); fillNear(B.node, A.big);
  await A.transport.openConnection(B.hex);
  await wait(50);   // let both _fireBound admissions run
  const aDeferred = A.peer._gracePending.has(B.big);
  const bDeferred = B.peer._gracePending.has(A.big);
  check('8a both ends refused and deferred', aDeferred && bDeferred);
  // rescue on BOTH ends inside the window
  A.node.synaptome.set(B.big, { peerId: B.big, weight: 0.5, stratum: 1 });
  B.node.synaptome.set(A.big, { peerId: A.big, weight: 0.5, stratum: 1 });
  await wait(600);
  const stillBound = (A.transport.boundPeers?.() ?? []).some((p) => String(p).includes(B.hex.slice(-16)))
                  || A.transport.isConnected?.(B.hex) === true;
  check('8a both-armed rescue: channel survives expiry', stillBound === true);
  check('8a pending drained after rescue', A.peer._gracePending.size === 0 && B.peer._gracePending.size === 0);
  await A.peer.stop(); await B.peer.stop();

  // (b) one-armed degradation: legacy end closes at refusal; armed end's
  //     late expiry fires against the closed channel without error.
  const C = await makeRealPeer(net, domain, 40, 40, { graceMs: 300 });
  const D = await makeRealPeer(net, domain, 41, 41, { graceMs: 0 });
  fillNear(C.node, D.big); fillNear(D.node, C.big);
  await C.transport.openConnection(D.hex);
  await wait(50);
  check('8b armed end deferred', C.peer._gracePending.has(D.big));
  check('8b legacy end closed immediately (no pending)', D.peer._gracePending.size === 0);
  await wait(500);  // C's expiry fires on the already-closed channel — idempotent
  check('8b idempotent late expiry: no error, pending drained', C.peer._gracePending.size === 0);
  await C.peer.stop(); await D.peer.stop();

  // (c) simultaneous expiry, no rescue: both close clean.
  const E = await makeRealPeer(net, domain, 50, 50, { graceMs: 200 });
  const F = await makeRealPeer(net, domain, 51, 51, { graceMs: 200 });
  fillNear(E.node, F.big); fillNear(F.node, E.big);
  await E.transport.openConnection(F.hex);
  await wait(50);
  await wait(450);  // both windows expire together
  check('8c simultaneous expiry drains both ends clean', E.peer._gracePending.size === 0 && F.peer._gracePending.size === 0);
  const gone = !(E.transport.isConnected?.(F.hex) === true);
  check('8c channel closed after unrescued expiry', gone === true);
  await E.peer.stop(); await F.peer.stop();
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
