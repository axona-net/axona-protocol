// smoke_gate_close_grace.mjs — v4.68.0 deferred refusal-close (closeGraceMs).
// The gate's refusal-time channel close, deferred by an opt-in grace window:
//   1. graceMs=0 (default): close fires immediately — v4.67.1 behavior.
//   2. graceMs>0: close DEFERRED; fires after the window when not admitted.
//   3. RESCUE: admitted-meanwhile peer's close is skipped at fire time.
//   4. BOUND: pending closes capped at graceMaxPending; overflow closes OLDEST
//      immediately (state and channel budget stay bounded under churn).
//
// Run: node test/smoke_gate_close_grace.mjs
import { AxonaPeer, AxonaDomain, NeuronNode } from '../src/index.js';

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

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
