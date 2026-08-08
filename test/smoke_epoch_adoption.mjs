// smoke_epoch_adoption.mjs — adopt-on-rejoin + epoch-ordered reconciliation
// (Dead-Root Eviction v0.3, phase E4). Root claims for one seat now converge
// by a TOTAL order: higher incarnation wins, regardless of closeness. Age
// never beats epoch; neither does distance. Whenever either side is
// unversioned (epoch 0 — a pre-epoch node), the standing closeness rule is
// the whole rule, so mixed-version meshes behave exactly as 4.61.x did.
//
// Run: node test/smoke_epoch_adoption.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG    = 0x89n << 248n;
const idHex  = (b) => b.toString(16).padStart(66, '0');
const lc     = (s) => String(s).toLowerCase();

// SELF is CLOSEST to TOPIC — the returning-corpse position. PROMOTED is the
// farther node the flight promoted while SELF was away.
const TOPIC    = REG | 0x1000n;
const SELF     = REG | 0x1001n;
const PROMOTED = REG | 0x1004n;
const NB       = REG | 0x8000n;

function mk(selfId = SELF) {
  const clock = { t: 1_000_000 };
  const dht = {
    verdictsSupported: true,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async (target) => ({ consumed: true, hops: 1, atNode: lc(idHex(typeof target === 'bigint' ? target : BigInt(`0x${target}`))) }),
    neighbors: () => [NB, PROMOTED],
    bridgeId: () => null,
    findKClosest: async () => [],
    lookup: async () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 0 });
  am.nodeId = selfId;
  const logs = [];
  am.setLogSink((lvl, evt, data) => logs.push({ lvl, evt, data }));
  return { am, clock, logs };
}

const beacon = (am, root, epoch, seq = 0) => am._onRootBeacon(
  { root: lc(idHex(root)), topics: [idHex(TOPIC)], epochs: [epoch], beaconId: `b-e4-${epoch}-${seq}-${Math.floor(am._now())}`, layer: 1 },
  { fromId: idHex(NB) },
);

// ── 1. ADOPT-ON-REJOIN: the returning corpse yields to a higher incarnation ─
{
  const { am, logs } = mk();
  const role = am._rootClaim.become(TOPIC, 'test');       // I root at epoch 1 (the pre-partition claim)
  role.epoch = 4;                                         // ...convicted at 4 while partitioned
  beacon(am, PROMOTED, 5);                                // the promoted (FARTHER) root beacons epoch 5
  ok('1a a farther beacon with a HIGHER epoch passes the closeness gate',
    am._rootBeacons.get(TOPIC)?.epoch === 5);
  ok('1b the returning root ADOPTS — demoted despite being closest',
    !am.axonRoles.get(TOPIC)?.isRoot);
  ok('1c the yield is attributed: epoch-superseded',
    logs.some((l) => l.evt === 'pubsub:root-transition' && l.data?.why === 'epoch-superseded'));
}

// ── 2. A stale (lower-epoch) claim never takes the seat, even from closer ───
{
  const FAR_SELF = REG | 0x1010n;                         // I hold the seat from farther out
  const CLOSER   = REG | 0x1001n;                         // the ghost is closer
  const { am } = mk(FAR_SELF);
  const role = am._rootClaim.become(TOPIC, 'test');
  role.epoch = 7;                                         // my live claim
  // The ghost is CONVICTED: its incarnation is tombstoned (the E3 flight did
  // this). A mere epoch lag never blocks succession — only a conviction does.
  am._rootTombstones = new Map([[TOPIC, { root: lc(idHex(CLOSER)), epoch: 3, exp: am._now() + 600_000 }]]);
  beacon(am, CLOSER, 3);                                  // the convicted ghost re-beacons, closer but tombstoned
  ok('2a the seat is KEPT — a convicted incarnation loses regardless of closeness',
    am.axonRoles.get(TOPIC)?.isRoot === true);
}

// ── 3. Normal succession: closer AND higher epoch wins as before ────────────
{
  const FAR_SELF = REG | 0x1010n;
  const CLOSER   = REG | 0x1001n;
  const { am } = mk(FAR_SELF);
  const role = am._rootClaim.become(TOPIC, 'test');
  role.epoch = 2;
  beacon(am, CLOSER, 3);
  ok('3a a closer, higher-epoch claimant takes the seat (the ordinary rule)',
    !am.axonRoles.get(TOPIC)?.isRoot);
}

// ── 4. Wire compat: an UNVERSIONED (epoch 0) claimant gets 4.61.x behavior ──
{
  const FAR_SELF = REG | 0x1010n;
  const CLOSER   = REG | 0x1001n;
  const { am } = mk(FAR_SELF);
  const role = am._rootClaim.become(TOPIC, 'test');
  role.epoch = 6;
  beacon(am, CLOSER, 0);                                  // pre-epoch node
  ok('4a closer unversioned claimant still wins — the old closeness rule is the whole rule',
    !am.axonRoles.get(TOPIC)?.isRoot);
}
{
  const { am } = mk();
  // I am an unversioned root (epoch 0 — role minted by a pre-epoch build is
  // modeled by forcing 0): a farther versioned beacon must NOT supersede me,
  // because supersession requires BOTH sides versioned.
  const role = am._rootClaim.become(TOPIC, 'test');
  role.epoch = 0;
  beacon(am, PROMOTED, 9);
  ok('4b a farther versioned beacon does not bulldoze an unversioned root — closeness still protects it',
    am.axonRoles.get(TOPIC)?.isRoot === true);
}

console.log(fail === 0 ? `\nsmoke_epoch_adoption: ${n}/${n} ok` : `\nsmoke_epoch_adoption: ${fail} FAILED of ${n}`);
process.exit(fail === 0 ? 0 : 1);
