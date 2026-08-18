// fence_replica_ledger.mjs — C4 (partial). A root must not record a backup it
// never managed to send to.
//
// STATUS: THIS FENCE IS INSUFFICIENT AND ITS SUBJECT IS NOT FIXED.
// Aster (council, 2026-07-31) rejected c1c435d as the C4 ledger fix, and I
// reproduced it: `_syncPush` calls `_route` without awaiting or returning it
// (syncEngine.js), and `_route` discards the Promise from `dht.routeMessage`
// (AxonaManager.js). Production routing reports failure by RESOLVING an
// exhausted result, which no try/catch can see. Measured on c1c435d with a real
// _syncPush and `routeMessage: async () => ({exhausted:true})`: 13 route calls,
// all failed, and still `replicas: 2` with `lastFullAt` advanced.
//
// Worse, the fence below stubs `_syncPush` out entirely and throws
// synchronously — so it exercises the catch block I wrote rather than the
// production dispatch chain. It certified its own premise. A real fence must
// drive `dht.routeMessage` failure, and the repair must thread an observed
// dispatch outcome through `_route`/`_syncPush` (or adopt a consciously weaker,
// honestly named enqueue-only contract).
//
// I originally wrote here that the false credit also "suppressed its own
// repair" and the member "is never pushed to again". That was retracted in the
// v4.54.0 commit message and in repairPlane.js but NOT here — the third of
// three sites, missed. Case 2a passes both ways: pushes DO continue, because
// the signature check and the ROOT_REPLICATE_FULL_MS backstop re-arm
// independently.
//
// What the fence below still legitimately pins: a SYNCHRONOUS throw out of the
// push path must not credit a replica, and a working transport must.
//
// This fence pins the ledger, NOT the completion contract. What discharges the
// ROOT obligation — local dispatch, an ACK, or observed possession via the
// high-water a cohort member already advertises — is the open C4 decision, and
// `sync.lastFullAt` is deliberately not asserted here.
//
// Run: node test/fence_replica_ledger.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const NB1 = idHex(REG | 0xaa0n), NB2 = idHex(REG | 0xaafn);
const TICK = 5_000;

function mk({ pushThrows }) {
  const clock = { t: 1_000_000 };
  const pushed = [];
  const dht = {
    // v4.58.0: capability is DECLARED. This fence stubs _syncPush out entirely, so
    // it never reaches _route — but the ledger now demands a verdict, and a stub
    // that returns nothing is a contract violation, not a silent success.
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: () => {},
    neighbors: () => [NB1, NB2],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  // The transport is the thing that fails. A synchronous throw out of _syncPush
  // is exactly what the swallowing catch was hiding.
  am._syncPush = (target, t, role, policy, opts) => {
    pushed.push(String(policy));
    if (pushThrows) throw new Error('transport down');
    return { consumed: true };            // v4.58.0: a working push REPORTS
  };
  return { am, clock, pushed };
}

async function rootWithState(am, clock, T) {
  am.pubsubSubscribe(T);
  am._becomeRoot(T);
  am.axonRoles.get(T).cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });
  for (let i = 0; i < 4; i++) { clock.t += TICK; await am.refreshTick(); }
  return am.axonRoles.get(T);
}

console.log('replica ledger — a root must not record a backup it never reached\n');

// ── 1. THE DEFECT: every push throws, yet the cohort is recorded ───────────
{
  const { am, clock, pushed } = mk({ pushThrows: true });
  const role = await rootWithState(am, clock, REG | 0x1001n);

  ok('1a. replication was attempted (the push path really ran)', pushed.length > 0,
    `pushes=${pushed.length}`);
  ok('1b. EVERY push threw, so NO cohort member is recorded as holding a replica',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}

// ── 2. THE CONSEQUENCE: the retry must stay armed ──────────────────────────
// This is the half that makes it a durability bug rather than a cosmetic one.
{
  const { am, clock, pushed } = mk({ pushThrows: true });
  const role = await rootWithState(am, clock, REG | 0x2001n);
  const after = pushed.length;
  for (let i = 0; i < 4; i++) { clock.t += TICK; await am.refreshTick(); }
  ok('2a. a root that could not reach its cohort KEEPS TRYING on later ticks',
    pushed.length > after, `before=${after} after=${pushed.length}`);
  ok('2b. …and still records no replicas while the transport is down',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}

// ── 3. THE CONTROL: a working transport DOES populate the ledger ───────────
// Without this, "never record anything" would pass sections 1 and 2 and be a
// regression wearing a fix's clothing.
{
  const { am, clock } = mk({ pushThrows: false });
  const role = await rootWithState(am, clock, REG | 0x3001n);
  ok('3a. CONTROL — with a working transport the cohort IS recorded',
    role.replicas.size > 0, `replicas=${role.replicas.size}`);
  ok('3b. …and the recorded members are the wanted cohort, not arbitrary ids',
    [...role.replicas.keys()].every(h => h === NB1 || h === NB2),
    JSON.stringify([...role.replicas.keys()]));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
