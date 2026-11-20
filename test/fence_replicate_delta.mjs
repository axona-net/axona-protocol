// =====================================================================
// fence_replicate_delta.mjs — the eager cohort replicate sends a credited
// replica only what it does not hold yet (4.158.0, REPLICATE_DELTA, default off).
//
// THE COST (David 2026-11-20; council 0a2320e8; Howard #77). Every publish a
// root ingests runs the eager replicate (_replicateRole from the publish and
// kill paths). The cache signature changes on every publish, so each run pushed
// the WHOLE cache to every replica: P publishes to one topic = P snapshots of
// growing size per replica, P² bytes. Testnet TQ3 (THROTTLE=1, region 80): one
// host took 10,030 ingest entries in 29 s; its JS heap reached 584 MB.
//
// THE CHANGE (armed only). On the eager path a member gets a DELTA — messages
// newer than the stamp recorded at its last credited, content-bearing push —
// when (i) it is credited (role.replicas, 'consumed' only), (ii) a stamp was
// recorded, and (iii) the last full push is inside ROOT_REPLICATE_FULL_MS.
// Otherwise the old rule applies. A delta round does not advance role.sync.sig,
// so the next periodic sweep still sends one full snapshot (it covers a message
// merged in below the high-water, which a stamp-based delta can miss).
//
// What this pins:
//   1. Unarmed: unchanged — every eager push carries the whole cache.
//   2. Armed: first push full; later eager pushes carry only the new message.
//   3. Armed: a delta round still counts as durability evidence (snapshot true).
//   4. Armed: the periodic sweep still sends the full cache after delta rounds.
//   5. Armed: past ROOT_REPLICATE_FULL_MS the eager push is full again.
//   6. Armed: a new cohort member gets the full cache; the old one a delta.
//   7. Armed: a member whose last push was not consumed gets the full cache.
// Offline: _route is stubbed to capture each REPLICATE payload.
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { ROOT_REPLICATE_FULL_MS } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => { if (cond) { pass++; console.log(`  ✓ ${name}`); } else { fail++; console.log(`  ✗ ${name} ${extra}`); } };

const REG = 0x87n << 248n;
const hex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const NB1 = hex(REG | 0xaa0n), NB2 = hex(REG | 0xaafn), NB3 = hex(REG | 0xab1n);
const T = REG | 0x1001n;

function mk({ armed }) {
  const clock = { t: 1_000_000 };
  let neighbors = [NB1, NB2];
  const refuse = new Set();
  const pushes = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: () => ({ consumed: true }),
    neighbors: () => neighbors,
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2, replicateDelta: armed });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  am._route = (target, type, payload) => {
    const to = hex(target);
    pushes.push({ to, n: Array.isArray(payload.msgs) ? payload.msgs.length : 0, ids: (payload.msgs || []).map((m) => m.msgId) });
    if (refuse.has(to)) { refuse.delete(to); return { consumed: false }; }
    return { consumed: true };
  };
  am.pubsubSubscribe(T);
  const role = am._becomeRoot(T);
  let seq = 0;
  const publish = () => { seq++; clock.t += 10; role.cache.push({ msgId: `m${seq}`, publishTs: clock.t, json: `{"k":${seq}}`, seq }); };
  const eager = () => am._replicateRole(T, role, null, clock.t, null, -1, { eager: true });
  const sweep = () => am._replicateRole(T, role, null, clock.t);
  return { am, role, clock, pushes, publish, eager, sweep, setNeighbors: (a) => { neighbors = a; }, refuse };
}

const last = (pushes, k) => pushes.slice(-k);

console.log('\n[1] unarmed: every eager push carries the whole cache (unchanged)');
{
  const { role, pushes, publish, eager, am } = mk({ armed: false });
  ok(am._replicateDelta === false, 'precondition: REPLICATE_DELTA is off by default');
  for (let i = 0; i < 5; i++) { publish(); await eager(); }
  ok(role.cache.length === 5, 'precondition: 5 messages cached');
  const lastTwo = last(pushes, 2);
  ok(lastTwo.length === 2 && lastTwo.every((p) => p.n === 5), 'the fifth publish pushed all 5 messages to each replica', JSON.stringify(lastTwo));
  ok(pushes.reduce((a, p) => a + p.n, 0) === 2 * (1 + 2 + 3 + 4 + 5), 'total messages sent grows as the square (2 × 15)', String(pushes.reduce((a, p) => a + p.n, 0)));
}

console.log('\n[2]–[3] armed: first push full, then only the new message');
{
  const { role, pushes, publish, eager, am } = mk({ armed: true });
  ok(am._replicateDelta === true, 'precondition: REPLICATE_DELTA armed');
  publish(); const r1 = await eager();
  ok(last(pushes, 2).every((p) => p.n === 1), 'first push: the whole (1-message) cache to each replica');
  let snapshots = r1.snapshot === true;
  for (let i = 0; i < 9; i++) { publish(); const r = await eager(); snapshots = snapshots && r.snapshot === true && r.verified === 2; }
  ok(role.cache.length === 10, 'precondition: 10 messages cached');
  const tail = last(pushes, 18);
  ok(tail.every((p) => p.n === 1), 'each later eager push carried exactly one message', JSON.stringify(tail.map((p) => p.n)));
  const lastTwo = last(pushes, 2);
  ok(lastTwo.every((p) => p.ids[0] === 'm10'), 'and it was the new one (m10)', JSON.stringify(lastTwo));
  ok(pushes.reduce((a, p) => a + p.n, 0) === 20, 'total messages sent is linear (2 × 10)', String(pushes.reduce((a, p) => a + p.n, 0)));
  ok(snapshots, 'every round reported content-bearing, credited pushes (durability evidence)');
}

console.log('\n[4] armed: the periodic sweep still sends the full cache after delta rounds');
{
  const { pushes, publish, eager, sweep } = mk({ armed: true });
  for (let i = 0; i < 6; i++) { publish(); await eager(); }
  await sweep();
  ok(last(pushes, 2).every((p) => p.n === 6), 'sweep pushed all 6 messages to each replica', JSON.stringify(last(pushes, 2)));
}

console.log('\n[5] armed: past ROOT_REPLICATE_FULL_MS the eager push is full again');
{
  const { pushes, publish, eager, clock } = mk({ armed: true });
  for (let i = 0; i < 4; i++) { publish(); await eager(); }
  clock.t += ROOT_REPLICATE_FULL_MS + 1;
  publish(); await eager();
  ok(last(pushes, 2).every((p) => p.n === 5), 'the backstop push carried all 5', JSON.stringify(last(pushes, 2)));
}

console.log('\n[6] armed: a new cohort member gets the full cache, the old one a delta');
{
  const { pushes, publish, eager, setNeighbors } = mk({ armed: true });
  for (let i = 0; i < 4; i++) { publish(); await eager(); }
  setNeighbors([NB1, NB3]);
  publish(); await eager();
  const byTo = Object.fromEntries(last(pushes, 2).map((p) => [p.to, p.n]));
  ok(byTo[NB3] === 5, 'new member NB3 got all 5', JSON.stringify(byTo));
  ok(byTo[NB1] === 1, 'old member NB1 got only the new one', JSON.stringify(byTo));
}

console.log('\n[7] armed: a member whose last push was not consumed gets the full cache');
{
  const { pushes, publish, eager, refuse } = mk({ armed: true });
  for (let i = 0; i < 3; i++) { publish(); await eager(); }
  refuse.add(NB2);
  publish(); await eager();                                  // NB2 refuses: uncredited
  publish(); await eager();
  const byTo = Object.fromEntries(last(pushes, 2).map((p) => [p.to, p.n]));
  ok(byTo[NB2] === 5, 'NB2 (last push not consumed) got all 5', JSON.stringify(byTo));
  ok(byTo[NB1] === 1, 'NB1 got only the new one', JSON.stringify(byTo));
}

console.log(`\n${fail ? '✗' : '✓'} fence_replicate_delta: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
