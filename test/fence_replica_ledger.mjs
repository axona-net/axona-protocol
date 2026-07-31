// fence_replica_ledger.mjs — C4 (partial). A root must not record a backup it
// never managed to send to.
//
// WHY THIS IS A FENCE AND NOT A TIDY-UP. _replicateRole pushed to each cohort
// member inside a try/catch that swallowed the error, then recorded the member
// in `role.replicas` unconditionally. That is not merely a wrong number:
//
//     const full = sig !== role.sync.sig
//       || want.some((hex) => !role.replicas.has(hex))     // <-- the retry
//       || (now - role.sync.lastFullAt) >= ROOT_REPLICATE_FULL_MS;
//
// the full-push re-arms precisely when a wanted member is MISSING from the
// ledger. A falsely credited member therefore looks seeded forever and is never
// pushed to again. The false record suppressed its own repair, and the root sat
// believing it had a durable backup that had received nothing.
//
// This fence pins the ledger, NOT the completion contract. What discharges the
// ROOT obligation — local dispatch, an ACK, or observed possession via the
// high-water a cohort member already advertises — is the open C4 decision, and
// `sync.lastFullAt` is deliberately not asserted here.
//
// Run: node test/fence_replica_ledger.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

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
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: () => {},
    neighbors: () => [NB1, NB2],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  // The transport is the thing that fails. A synchronous throw out of _syncPush
  // is exactly what the swallowing catch was hiding.
  am._syncPush = (target, t, role, policy, opts) => {
    pushed.push(String(policy));
    if (pushThrows) throw new Error('transport down');
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
