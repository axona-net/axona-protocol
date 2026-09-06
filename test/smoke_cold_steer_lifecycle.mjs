// Cold-steer retry lifecycle (GH #418 follow-up, kernel 4.76.1) — the two council
// bounds Aster/Vega named on 4a275a3, now enforced:
//   • ONE BUDGET PER COLD CYCLE: a repairPlane renewal re-enters _sendSubscribe every
//     renewFastMs (~5s) while a burst still has retries left; it must NOT stack a
//     second steer on a live cycle.
//   • GENERATION TOKEN: unsubscribe releases the cycle, and a same-topic resubscribe
//     starts a FRESH generation, so a late timer from the prior cycle cannot act for
//     the new one.
// White-box: inspects AxonaManager._coldSteerGen (topic -> generation; presence = live).
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c) => { n++; if (!c) { fail++; console.log(`  ✗ ${m}`); } else console.log(`  ok ${n} - ${m}`); };

// A resolver that never names a node (path:[]) keeps the cold cycle rescheduling —
// so the cycle stays LIVE and observable in _coldSteerGen across the assertions.
const dht = {
  verdictsSupported: true,
  getSelfId: () => 1n,
  onRoutedMessage: () => {},
  neighbors: () => [],
  bridgeId: () => null,
  routeMessage: async () => ({ consumed: false, terminal: true }),
  lookup: async () => ({ path: [] }),
  findKClosest: async () => [],
};

const am = new AxonaManager({ dht: sealTestDht(dht), now: () => 0, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
am.nodeId = 1n;
am.setLogSink(() => {});

const T = 0xABCn;
am.mySubscriptions.set(T, { since: 0 });   // wants() → true

// 1. a cold steer registers exactly one live cycle
am._steerColdSubscribe(T);
ok('cold steer registers one live cycle', am._coldSteerGen?.size === 1 && am._coldSteerGen.has(T));
const gen1 = am._coldSteerGen.get(T);

// 2. ONE BUDGET PER CYCLE — a renewal re-entry does not stack a second cycle
am._steerColdSubscribe(T);
ok('renewal re-entry does not stack a second cycle (gen unchanged, size 1)',
   am._coldSteerGen.size === 1 && am._coldSteerGen.get(T) === gen1);

// 3. unsubscribe RELEASES the cycle
am.pubsubUnsubscribe(T);
ok('unsubscribe releases the cold-steer cycle', !am._coldSteerGen.has(T));

// 4. GENERATION TOKEN — a resubscribe starts a fresh cycle with a new generation
am.mySubscriptions.set(T, { since: 0 });
am._steerColdSubscribe(T);
ok('resubscribe starts a fresh cycle with a new generation',
   am._coldSteerGen.has(T) && am._coldSteerGen.get(T) !== gen1);

console.log(`\n${fail ? '✗' : '✓'} smoke_cold_steer_lifecycle: ${n} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
