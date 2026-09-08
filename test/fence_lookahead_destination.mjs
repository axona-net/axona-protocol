// =====================================================================
// fence_lookahead_destination.mjs — 4.78.0
//
// _findCloserInTwoHops fired at the DESTINATION of every routed message and
// fanned out `lookahead_probe` over the entire synaptome before the local
// handler was reached. At the destination that fan-out is arithmetically
// incapable of returning anything: both scoring tests are `d < bestDist`,
// bestDist starts at `node.id ^ target` = 0, and XOR distance is unsigned, so
// `d < 0` is unsatisfiable. It was ~70 network round-trips whose result was
// fixed before the first packet left — and Promise.allSettled waits for the
// slowest, so one connected-but-silent peer cost the full 5s request timeout,
// which recursive-await then charged to every upstream node.
//
// Measured before the fix (ops/lookahead-control.mjs, same peer, same payload,
// one hop, n=40 per arm): transport.send p50 1.0ms vs routeMessage p50 536ms /
// p90 5,001ms. 531x, with 15 of 40 samples sitting on the timer.
//
// THE FENCE IS ABOUT SILENCE, NOT THE RETURN VALUE. Returning null was always
// correct; the defect was doing network I/O to discover it. So these pin the
// PROBE COUNT. A future refactor that restores correctness while restoring the
// fan-out would pass a null-only assertion and fail these.
//
// Run: node test/fence_lookahead_destination.mjs
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

// A node stub carrying only what _findCloserInTwoHops touches. `send` records
// every probe so the fence can assert on I/O rather than on the answer.
function mkNode(selfId, peerIds, { reply = null, incoming = [] } = {}) {
  const sent = [];
  return {
    sent,
    node: {
      id: selfId,
      synaptome: new Map(peerIds.map((p, i) => [String(i), { peerId: p }])),
      incomingSynapses: new Map(incoming.map((p, i) => [String(i), { peerId: p }])),
      transport: {
        send: async (peerId, type, payload) => {
          sent.push({ peerId, type, payload });
          return typeof reply === 'function' ? reply(peerId) : reply;
        },
      },
    },
  };
}
const call = (stub, target) =>
  AxonaPeer.prototype._findCloserInTwoHops.call({ _node: stub.node }, target);

const SELF = (0x89n << 248n) | 0xabcdn;
const PEERS = [
  (0x89n << 248n) | 0x1111n,
  (0x89n << 248n) | 0x2222n,
  (0x89n << 248n) | 0x3333n,
];

console.log('4.78.0 — the destination does not ask who is closer than zero\n');

// ── the fix ─────────────────────────────────────────────────────────────
console.log('— at the destination (target === self) —');
{
  const stub = mkNode(SELF, PEERS);
  const got = await call(stub, SELF);
  check('1. returns null', got === null, `got ${got}`);
  check('2. sends ZERO probes — the whole point', stub.sent.length === 0,
    `sent ${stub.sent.length}`);
}
{
  // Same, with a populated incomingSynapses set: that loop uses the identical
  // `d < bestDist` test and is equally unsatisfiable at distance 0.
  const stub = mkNode(SELF, PEERS, { incoming: [(0x89n << 248n) | 0x4444n] });
  check('3. incoming synapses cannot rescue it either', await call(stub, SELF) === null);
  check('3b. …and still no probes', stub.sent.length === 0, `sent ${stub.sent.length}`);
}
{
  // The arithmetic the fix rests on, asserted rather than assumed: nothing can
  // score below distance 0. If this ever fails, the bypass is unsound.
  const target = SELF;
  const worst = PEERS.concat([(0x89n << 248n) | 0x4444n, 0n, (1n << 263n)]);
  check('4. no id anywhere scores below XOR distance 0',
    worst.every((p) => !((p ^ target) < (target ^ target))));
}

// ── what must NOT change ────────────────────────────────────────────────
console.log('\n— the genuine local minimum still probes (this is what lookahead is FOR) —');
{
  const FAR = (0x89n << 248n) | 0xf000n;   // not self
  const stub = mkNode(SELF, PEERS, { reply: null });
  await call(stub, FAR);
  check('5. a non-destination hop STILL fans out', stub.sent.length === PEERS.length,
    `sent ${stub.sent.length} want ${PEERS.length}`);
  check('5b. …as lookahead_probe', stub.sent.every((s) => s.type === 'lookahead_probe'));
  check('5c. …carrying target and fromDist',
    stub.sent.every((s) => s.payload?.target === FAR && typeof s.payload?.fromDist === 'bigint'));
}
{
  // And it still finds a closer first hop when a probe reports one.
  const FAR = (0x89n << 248n) | 0xf000n;
  const NEAR2HOP = (0x89n << 248n) | 0xf001n;      // 2-hop node very close to FAR
  const stub = mkNode(SELF, PEERS, { reply: (p) => (p === PEERS[1] ? { peerId: NEAR2HOP } : null) });
  const got = await call(stub, FAR);
  check('6. returns the FIRST HOP, not the 2-hop node', got === PEERS[1], `got ${got}`);
}
{
  // A terminal answer must not be scored.
  const FAR = (0x89n << 248n) | 0xf000n;
  const stub = mkNode(SELF, PEERS, { reply: () => ({ peerId: (0x89n << 248n) | 0xf001n, terminal: true }) });
  check('7. terminal probe answers are ignored', await call(stub, FAR) === null);
}
{
  // Reverse channels are a valid next hop when they are genuinely closer.
  const FAR  = (0x89n << 248n) | 0xf000n;
  const BACK = (0x89n << 248n) | 0xf00fn;   // closer to FAR than SELF is
  const stub = mkNode(SELF, [], { incoming: [BACK] });
  check('8. incomingSynapses still supply a next hop when closer',
    await call(stub, FAR) === BACK);
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
