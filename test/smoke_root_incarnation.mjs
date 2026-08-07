// smoke_root_incarnation.mjs — root INCARNATION plumbing (Dead-Root Eviction
// v0.3, phase E1). Every entry to the ROOT nature mints epoch = knownEpoch+1;
// beacons carry the epoch, receivers store it as a high-water mark, and a
// beacon without the field reads as epoch 0 (wire-compatible both ways).
// The epoch ORDERS claims for a seat; it changes no placement behavior here —
// comparison semantics land with reconciliation (E4).
//
// Run: node test/smoke_root_incarnation.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG   = 0x89n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc    = (s) => String(s).toLowerCase();

const TOPIC  = REG | 0x1000n;
const CLOSER = REG | 0x1001n;   // strictly closer to TOPIC than SELF
const SELF   = REG | 0x1010n;
const NB     = REG | 0x8000n;

function mk() {
  const clock = { t: 1_000_000 };
  const sends = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      sends.push({ type, payload });
      return { consumed: true, hops: 1, atNode: lc(idHex(NB)) };
    },
    neighbors: () => [NB],
    bridgeId: () => null,
    findKClosest: async () => [],
    lookup: async () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, sends };
}

// ── 1. Cold seat: become() mints epoch 1 and the beacon carries it ──────────
{
  const { am, sends } = mk();
  const role = am._rootClaim.become(TOPIC, 'test');
  ok('1a cold become() mints epoch 1', role.epoch === 1, `got ${role.epoch}`);
  const b = sends.find((s) => s.type === T.ROOTBEACON);
  ok('1b announce beacon carries epochs aligned with topics',
    !!b && Array.isArray(b.payload.epochs) && b.payload.topics[b.payload.topics.indexOf(idHex(TOPIC))] === idHex(TOPIC)
      && b.payload.epochs[b.payload.topics.indexOf(idHex(TOPIC))] === 1,
    b ? JSON.stringify({ topics: b.payload.topics, epochs: b.payload.epochs }) : 'no beacon sent');
}

// ── 2. Receipt stores the epoch; re-become mints past it ────────────────────
{
  const { am } = mk();
  am._onRootBeacon(
    { root: lc(idHex(CLOSER)), topics: [idHex(TOPIC)], epochs: [5], beaconId: 'b-e1-a', layer: 1 },
    { fromId: idHex(NB) },
  );
  const rec = am._rootBeacons.get(TOPIC);
  ok('2a received beacon epoch stored on the record', rec && rec.epoch === 5, `got ${rec && rec.epoch}`);
  ok('2b _knownEpoch reads it', am._knownEpoch(TOPIC) === 5, `got ${am._knownEpoch(TOPIC)}`);
  // The closer root's beacon gates become(); the epoch must survive to the
  // mint anyway. Simulate the seat opening by clearing the pointer while the
  // epoch high-water mark is what a real tombstone would carry (E3) — here we
  // re-deliver the record with the corpse gone by making SELF closest again.
  am._rootBeacons.set(TOPIC, { ...rec, root: lc(idHex(SELF)) });
  const role = am._rootClaim.become(TOPIC, 'test');
  ok('2c re-become mints knownEpoch+1', !!role && role.epoch === 6, `got ${role && role.epoch}`);
}

// ── 3. Wire compat: beacon WITHOUT epochs field reads as epoch 0 ────────────
{
  const { am } = mk();
  am._onRootBeacon(
    { root: lc(idHex(CLOSER)), topics: [idHex(TOPIC)], beaconId: 'b-e1-b', layer: 1 },
    { fromId: idHex(NB) },
  );
  const rec = am._rootBeacons.get(TOPIC);
  ok('3a pre-epoch beacon stores epoch 0', rec && rec.epoch === 0, `got ${rec && rec.epoch}`);
  ok('3b garbage epoch reads as 0 (no throw)', (() => {
    am._onRootBeacon({ root: lc(idHex(CLOSER)), topics: [idHex(TOPIC)], epochs: ['x'], beaconId: 'b-e1-c', layer: 1 }, { fromId: idHex(NB) });
    const r2 = am._rootBeacons.get(TOPIC);
    return r2 && r2.epoch === 0;
  })());
}

// ── 4. High-water mark: a later beacon with a LOWER epoch never regresses ───
{
  const { am } = mk();
  am._onRootBeacon({ root: lc(idHex(CLOSER)), topics: [idHex(TOPIC)], epochs: [7], beaconId: 'b-e1-d', layer: 1 }, { fromId: idHex(NB) });
  am._onRootBeacon({ root: lc(idHex(CLOSER)), topics: [idHex(TOPIC)], epochs: [3], beaconId: 'b-e1-e', layer: 1 }, { fromId: idHex(NB) });
  const rec = am._rootBeacons.get(TOPIC);
  ok('4a record keeps the highest epoch seen for the seat', rec && rec.epoch === 7, `got ${rec && rec.epoch}`);
}

console.log(fail === 0 ? `\nsmoke_root_incarnation: ${n}/${n} ok` : `\nsmoke_root_incarnation: ${fail} FAILED of ${n}`);
process.exit(fail === 0 ? 0 : 1);
