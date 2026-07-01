// smoke_cold_burst.mjs — cold-publish burst (v4.11.0).
//
// A publish from a freshly-joined (not-yet-integrated) node is the worst case for
// the one-shot greedy PUB: it strands and never re-homes. Waiting is harmful —
// OUTBOUND traffic is what integrates a newcomer. So while COLD (few neighbours),
// pubsubPublish re-sends the SAME envelope a few times over the first ~second
// (idempotent; root dedups by msgId). It must:
//   1. COLD publisher (neighbours < threshold) → several PUB sends (initial + burst)
//   2. WARM publisher (neighbours ≥ threshold) → exactly one PUB send (no burst)
//   3. burst stops early once the publish is confirmed (_confirmPending)
//   4. stop() cancels any in-flight burst timers
//
// Run: node test/smoke_cold_burst.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const T_PUB = 'pubsub:pub';
const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const delay = (ms) => new Promise(r => setTimeout(r, ms));

const SELF = REG | 0x11n, TOPIC = REG | 0xabcn;

function mk({ neighbours }) {
  const pubs = [];
  const nbrs = Array.from({ length: neighbours }, (_, i) => idHex(REG | BigInt(0x100 + i)));
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (_target, type, _payload) => { if (type === T_PUB) pubs.push(1); },
    neighbors: () => nbrs,
    bridgeId: () => null,
    findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht, now: () => Date.now() });
  am.nodeId = SELF;
  return { am, pubs };
}

function publish(am, msgId) {
  const json = JSON.stringify({ msgId, message: 'hi', topic: { name: 't' } });
  am.pubsubPublish(TOPIC, json, { postHash: msgId });
}

// 1. COLD → burst (initial send + up to 5 re-sends)
{
  const { am, pubs } = mk({ neighbours: 2 });     // < COLD_PEER_THRESHOLD (8)
  publish(am, 'a'.repeat(64));
  ok('cold: initial send fires immediately', pubs.length === 1, `(${pubs.length})`);
  await delay(1300);                              // 5 × 200ms + margin
  ok('cold: burst re-sends the envelope multiple times', pubs.length >= 5, `(total sends=${pubs.length})`);
  ok('cold: burst is bounded (≤ initial + COLD_BURST_TRIES)', pubs.length <= 6, `(total sends=${pubs.length})`);
  am.stop();
}

// 2. WARM → exactly one send, no burst
{
  const { am, pubs } = mk({ neighbours: 20 });    // ≥ threshold
  publish(am, 'b'.repeat(64));
  await delay(1300);
  ok('warm: exactly one send (no burst)', pubs.length === 1, `(total sends=${pubs.length})`);
  am.stop();
}

// 3. COLD but confirmed mid-burst → stops early
{
  const { am, pubs } = mk({ neighbours: 2 });
  const msgId = 'c'.repeat(64);
  publish(am, msgId);
  await delay(350);                               // ~1 burst tick in
  am._confirmPending(TOPIC, msgId);               // publisher observed its own msgId → stop
  const atConfirm = pubs.length;
  await delay(900);
  ok('confirmed mid-burst → no further re-sends', pubs.length === atConfirm, `(at confirm=${atConfirm}, final=${pubs.length})`);
  ok('confirmed early → fewer than a full burst', pubs.length < 6, `(total sends=${pubs.length})`);
  am.stop();
}

// 4. stop() cancels in-flight burst timers
{
  const { am, pubs } = mk({ neighbours: 2 });
  publish(am, 'd'.repeat(64));
  await delay(250);
  am.stop();
  const atStop = pubs.length;
  await delay(900);
  ok('stop() cancels the remaining burst', pubs.length === atStop, `(at stop=${atStop}, final=${pubs.length})`);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_cold_burst: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
