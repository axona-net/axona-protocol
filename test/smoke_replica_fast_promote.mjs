// smoke_replica_fast_promote.mjs — departure-triggered backup promotion (v4.17.0).
//
// A warm backup holding a topic's cache promotes to root when its root goes away.
// Two speeds:
//   • root DEPARTED the mesh (no longer a reachable neighbour) → promote after the
//     SHORT grace (REPLICA_GONE_MS ≈ 15s), so history is served within a churn window
//   • root still a NEIGHBOUR but quiet (transient replicate loss) → keep waiting the
//     full REPLICA_STALE_MS (~65s), never split a live root
//
// Injected clock so both windows are exercised in milliseconds.
// Run: node test/smoke_replica_fast_promote.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const idHex = (b) => b.toString(16).padStart(66, '0');
const REG = 0x87n << 256n;         // 264-bit space: region byte is the TOP byte
const SELF  = REG | 0xa0n;         // our node id
const TOPIC = REG | 0xa1n;         // topic — SELF is XOR-closest (^=0x01) among {SELF, ROOT}
const ROOT  = REG | 0xf00n;        // the (departing) root that replicated to us (^TOPIC=0xfa1, far)

let clock = 1_000_000;
const now = () => clock;
function mk(neighborsFn) {
  const sent = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (_t, type, payload) => sent.push({ type, payload }),
    neighbors: neighborsFn,
    bridgeId: () => null,
    findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht, now });
  am.nodeId = SELF;
  return { am, sent };
}

// Seed a cache-bearing BACKUP role directly (bypassing the signature-verifying
// replicate ingest — we're testing the promotion timing, not ingestion). Mirrors
// the post-condition of a REPLICATE from ROOT: isRoot=false, backupOf=ROOT, one
// cached message, lastReplicaAt=now.
function seedBackup(am) {
  const role = am._becomeRoot(TOPIC);
  role.isRoot = false;
  role.backupOf = idHex(ROOT);
  role.lastReplicaAt = now();
  role.cache.push({ msgId: 'm1', publishTs: now(), json: '{"message":"hi"}', bytes: 17 });
  role.cacheIds.add('m1');
  role.cacheBytes = 17;
  role.seq = 1; role.lastTs = now();
  return role;
}

// 1. Root DEPARTED (not a neighbour) → promotes after the short grace, not 65s.
{
  const { am } = mk(() => []);                 // empty synaptome ⇒ ROOT is gone, SELF is closest
  const role = seedBackup(am);
  ok('seeded as a backup of the root', !!role && role.backupOf != null && !role.isRoot && role.cache.length === 1,
     `(backupOf=${role?.backupOf?.slice(0,6)})`);

  clock += 60_000;                             // 60s < REPLICA_GONE_MS (65s) — too soon
  await am.refreshTick();
  ok('does NOT promote before the short grace', !am.axonRoles.get(TOPIC).isRoot);

  clock += 10_000;                             // now 70s total > REPLICA_GONE_MS (65s)
  await am.refreshTick();
  const r = am.axonRoles.get(TOPIC);
  ok('promotes to root shortly after the root departs', r.isRoot && r.backupOf == null && r.cache.length === 1,
     `(isRoot=${r.isRoot})`);
  am.stop();
}

// 2. Root still a NEIGHBOUR but quiet → does NOT promote in the short window
//    (must wait the full stale window; a live-but-lossy root is never split).
{
  const { am } = mk(() => [idHex(ROOT)]);      // ROOT still reachable
  seedBackup(am);
  clock += 10_000;                             // > REPLICA_GONE_MS but « REPLICA_STALE_MS
  await am.refreshTick();
  ok('a quiet-but-reachable root is NOT prematurely promoted over', !am.axonRoles.get(TOPIC).isRoot);

  clock += 60_000;                             // now 70s > REPLICA_STALE_MS (65s)
  await am.refreshTick();
  ok('promotes only after the full stale window when the root stayed reachable',
     am.axonRoles.get(TOPIC).isRoot);
  am.stop();
}

console.log(`\n${fail ? '✗' : '✓'} smoke_replica_fast_promote: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
