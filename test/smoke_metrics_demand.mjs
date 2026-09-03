// smoke_metrics_demand.mjs — demand-driven metrics (METRICSON), v4.12.0.
//
// Metrics are NOT a relay feature: ANY node that roots a topic publishes its
// snapshots WHILE a metrics lease is active, and stops when it lapses.
//   1. requesting metrics sends METRICSON toward the data topic + renews it
//   2. the ROOT arms a lease and publishes the FIRST snapshot immediately
//      (v4.16.1 — at routing latency, not the next tick), throttled so renewals
//      don't storm; subsequent snapshots follow on the tick each METRICS_PUB_MS
//   3. a path (non-terminal) node forwards the first METRICSON, short-circuits a
//      quick duplicate, and an inheriting root picks the lease up on promotion
//   4. the lease self-expires → the root stops publishing (no orphan load)
//
// Injected clock so the ~20s cadence + 70s lease are exercised in milliseconds.
// Run: node test/smoke_metrics_demand.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const T_METRICSON = 'pubsub:metricson';
const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x11n, TOPIC = REG | 0xabcn, REQ = REG | 0x99n;

let clock = 1_000_000;
const now = () => clock;
function mk() {
  const sent = [], pubs = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: false,   // audited: returns a push-count / undefined, never a verdict
    routeMessage: (_t, type, payload) => sent.push({ type, payload }),
    neighbors: () => [], bridgeId: () => null, findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now });
  am.nodeId = SELF;
  am.setMetricsPublisher((dataIdHex, snap) => { pubs.push({ dataIdHex, snap }); });
  return { am, sent, pubs };
}

// 1. requester emits + renews METRICSON toward the data topic
{
  const { am, sent } = mk();
  am.pubsubMetricsOn(TOPIC);
  const m = sent.filter(s => s.type === T_METRICSON);
  ok('pubsubMetricsOn routes a METRICSON toward the data topic', m.length === 1 && m[0].payload.topicId === idHex(TOPIC), `(${m.length})`);
  // renewal on the tick after the cadence elapses
  clock += 21_000;
  await am.refreshTick();
  ok('the request is renewed on the refresh tick', sent.filter(s => s.type === T_METRICSON).length >= 2);
  am.stop();
}

// 2. the ROOT arms a lease and answers the demand IMMEDIATELY (v4.16.1),
//    then continues on the tick cadence
{
  const { am, pubs } = mk();
  am._onMetricsOn({ topicId: idHex(TOPIC), via: [], requesterId: idHex(REQ) }, { isTerminal: true });
  const role = am.axonRoles.get(TOPIC);
  ok('root arms a metrics lease', !!role && role.isRoot && role.metricsOn > now(), `(metricsOn=${role?.metricsOn})`);
  ok('root publishes the FIRST snapshot immediately (no tick needed)',
    pubs.length === 1 && pubs[0].dataIdHex === idHex(TOPIC), `(${pubs.length})`);
  ok('snapshot carries the metric fields', pubs.length === 1 && pubs[0].snap && 'current_count' in pubs[0].snap && 'seq' in pubs[0].snap && 'subscribers' in pubs[0].snap);
  // a prompt renewal (second METRICSON inside METRICS_PUB_MS) must NOT re-publish
  clock += 3_000;
  am._onMetricsOn({ topicId: idHex(TOPIC), via: [], requesterId: idHex(REQ) }, { isTerminal: true });
  ok('a renewal inside the cadence is throttled (no publish storm)', pubs.length === 1, `(${pubs.length})`);
  clock += 21_000;                       // past METRICS_PUB_MS
  await am.refreshTick();
  ok('root publishes the next snapshot on the tick cadence', pubs.length === 2, `(${pubs.length})`);
  // 3b. lease self-expires → publishing stops
  clock += 80_000;                       // past METRICS_LEASE_MS (70s)
  const before = pubs.length;
  await am.refreshTick();
  ok('root stops publishing once the lease lapses', pubs.length === before, `(before=${before}, after=${pubs.length})`);
  am.stop();
}

// 3. a path (non-terminal) node forwards then short-circuits a quick duplicate
{
  const { am } = mk();
  const r1 = am._onMetricsOn({ topicId: idHex(TOPIC), via: [], requesterId: idHex(REQ) }, { isTerminal: false });
  ok('non-terminal node forwards the first METRICSON', r1 === undefined && (am._metricsWanted.get(TOPIC) || 0) > now());
  const r2 = am._onMetricsOn({ topicId: idHex(TOPIC), via: [], requesterId: idHex(REQ) }, { isTerminal: false });
  ok('a quick duplicate is short-circuited (already informed the root)', r2 === 'consumed');
  am.stop();
}

// 4. an inheriting root picks up the lease on promotion (flag passed through en route)
{
  const { am, pubs } = mk();
  const role = am._becomeRoot(TOPIC); role.isRoot = false; // a non-root relay role exists here
  am._metricsWanted.set(TOPIC, now() + 70_000);            // a METRICSON passed through earlier
  am._maybePromoteRoot(role, { via: [] }, { isTerminal: true }); // …then routing promotes us to root
  ok('inheriting root adopts the active lease', role.isRoot && role.metricsOn > now(), `(metricsOn=${role.metricsOn})`);
  clock += 21_000;
  await am.refreshTick();
  ok('inheriting root publishes a snapshot', pubs.length === 1);
  am.stop();
}

// 5. #47 — a REPLICATED BACKUP that NEVER saw the METRICSON inherits the lease
//    across a root transition. This is the specimen the path-node seed (§4) misses:
//    the promoted node received the role via REPLICATE, not on the METRICSON path,
//    so `_metricsWanted` is empty and the lease must ride the replication payload.
const PRINCIPAL = REG | 0x77n;
{
  const { am, pubs } = mk();
  am._rootReplicas = 2;                                    // backup duty on
  const leaseUntil = now() + 70_000;
  // the live root's REPLICATE arrives carrying its active lease (Edit 1 → Edit 2)
  await am._syncIngest(
    { topicId: idHex(TOPIC), from: idHex(PRINCIPAL), msgs: [], dels: [], metricsOn: leaseUntil },
    { fromId: idHex(PRINCIPAL), isTerminal: false },
    'REPLICATE',
  );
  const role = am.axonRoles.get(TOPIC);
  ok('backup ingested the REPLICATE (never on the METRICSON path)',
    !!role && role.isRoot === false && (am._metricsWanted.get(TOPIC) || 0) === 0);
  ok('backup stashed the inherited lease, but does NOT publish while a backup',
    role._inheritedMetricsOn === leaseUntil && pubs.length === 0, `(inh=${role?._inheritedMetricsOn}, pubs=${pubs.length})`);
  // …the root dies and routing promotes this backup
  am._maybePromoteRoot(role, { via: [] }, { isTerminal: true });
  ok('promoted backup adopts the replicated lease (no METRICSON needed)',
    role.isRoot && role.metricsOn === leaseUntil, `(metricsOn=${role.metricsOn})`);
  clock += 21_000;
  await am.refreshTick();
  ok('promoted backup publishes a snapshot — metrics survive the transition (#47)', pubs.length >= 1, `(${pubs.length})`);
  am.stop();
}

// 5b. CONTROL — a backup with NO inherited lease (and never on the path) stays
//     metrics-dark on promotion. Proves the lease is what arms it, not promotion.
{
  const { am, pubs } = mk();
  am._rootReplicas = 2;
  await am._syncIngest(
    { topicId: idHex(TOPIC), from: idHex(PRINCIPAL), msgs: [], dels: [] },   // no metricsOn
    { fromId: idHex(PRINCIPAL), isTerminal: false },
    'REPLICATE',
  );
  const role = am.axonRoles.get(TOPIC);
  am._maybePromoteRoot(role, { via: [] }, { isTerminal: true });
  ok('control: no inherited lease → promoted root stays metrics-dark', role.isRoot && !(role.metricsOn > now()), `(metricsOn=${role.metricsOn})`);
  clock += 21_000;
  await am.refreshTick();
  ok('control: and publishes no snapshot', pubs.length === 0, `(${pubs.length})`);
  am.stop();
}

console.log(`\n${fail ? '✗' : '✓'} smoke_metrics_demand: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
