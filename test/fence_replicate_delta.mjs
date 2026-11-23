// =====================================================================
// fence_replicate_delta.mjs — the eager cohort replicate sends a credited
// replica only what it does not hold yet (4.160.0, REPLICATE_DELTA, default off).
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
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { idHex, lc } from '../src/pubsub/ids.js';

let pass = 0, fail = 0;
const ok = (cond, name, extra = '') => { if (cond) { pass++; console.log(`  ✓ ${name}`); } else { fail++; console.log(`  ✗ ${name} ${extra}`); } };

const REG = 0x87n << 248n;
const hex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const NB1 = hex(REG | 0xaa0n), NB2 = hex(REG | 0xaafn), NB3 = hex(REG | 0xab1n);
const T_DEFAULT = REG | 0x1001n;
const DESC = { region: 'useast', owner: null, name: 'fence-replicate-delta', write: 'open' };
const T_REAL = await deriveTopicIdBig(DESC);
const author = await createAuthorIdentity();

function mk({ armed, T = T_DEFAULT }) {
  const clock = { t: 1_000_000 };
  let neighbors = [NB1, NB2];
  const refuse = new Set();
  const hold = new Map();   // to -> resolver list: a push whose verdict the test releases later
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
    if (hold.has(to)) { const q = hold.get(to); hold.delete(to); return new Promise((res) => q.push(res)); }
    return { consumed: true };
  };
  am.pubsubSubscribe(T);
  const role = am._becomeRoot(T);
  let seq = 0;
  const publish = () => { seq++; clock.t += 10; role.cache.push({ msgId: `m${seq}`, publishTs: clock.t, json: `{"k":${seq}}`, seq }); role.lastTs = clock.t; };
  // A stamped body arriving from elsewhere (merge import, replay-up, handoff):
  // a SIGNED envelope through the real _ingestStamped, at a stamp the test
  // chooses. Returns the msgId when the body was held.
  const importAt = async (tag, ts) => {
    const e = await buildEnvelope({ topic: DESC, message: `{"imp":"${tag}"}`, identity: author, ts, seq: 0 });
    const r = await am._ingestStamped(role, { msgId: e.msgId, json: JSON.stringify(e), publishTs: ts, seq: 0 }, am._cutCapture(role));
    return r === 'held' && role.cacheIds.has(e.msgId) ? e.msgId : null;
  };
  const eager = () => am._replicateRole(T, role, null, clock.t, null, -1, { eager: true });
  const sweep = () => am._replicateRole(T, role, null, clock.t);
    // A body arriving on the DELIVER path (unverified there; cached in arrival order).
  const deliverAt = (id, ts) => am._onDeliver({ topicId: idHex(T), from: lc(idHex(REG | 0xbb0n)), msgs: [{ msgId: id, publishTs: ts, json: `{"d":"${id}"}`, seq: 0 }] }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  // A REAL publish: a signed envelope through _ingestPublish, which stamps it and runs the eager replicate itself.
  const realPublish = async (tag) => { const e = await buildEnvelope({ topic: DESC, message: `{"p":"${tag}"}`, identity: author, ts: clock.t, seq: 1 }); const r = await am._ingestPublish(role, JSON.stringify(e)); return { id: e.msgId, r }; };
  return { am, role, clock, pushes, publish, eager, sweep, deliverAt, realPublish, setNeighbors: (a) => { neighbors = a; }, refuse, hold, importAt };
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

// ── Orion eed879b0 (2026-11-23): RD-0 … RD-3 ─────────────────────────────
console.log('\n[8] RD-0: a body that enters the cache at or below a replica\'s stamp ends that replica\'s deltas');
{
  const { pushes, publish, eager, importAt, role } = mk({ armed: true, T: T_REAL });
  publish(); await eager(); publish(); await eager();
  const hw = role.lastTs;
  const old = await importAt('old', hw - 500);
  ok(!!old, 'precondition: a signed body stamped BELOW the high-water was held through _ingestStamped');
  publish(); await eager();
  const got = last(pushes, 2);
  ok(got.every((p) => p.n === 4 && p.ids.includes(old)), 'the next eager push carried the whole cache, the old body included, to each replica', JSON.stringify(got));
  publish(); await eager();
  ok(last(pushes, 2).every((p) => p.n === 1), 'and the round after that is a delta again', JSON.stringify(last(pushes, 2).map((p) => p.n)));
}
{
  const { pushes, publish, eager, importAt, role } = mk({ armed: true, T: T_REAL });
  publish(); await eager(); publish(); await eager();
  const tie = await importAt('tie', role.lastTs);             // SAME millisecond as the stamp
  publish(); await eager();
  ok(!!tie && last(pushes, 2).every((p) => p.ids.includes(tie)), 'a body stamped EQUAL to the stamp is not skipped either', JSON.stringify(last(pushes, 2)));
}

console.log('\n[9] RD-1: a round that sent one member a delta does not count as the full push');
{
  const { pushes, publish, eager, sweep, setNeighbors, role } = mk({ armed: true });
  for (let i = 0; i < 3; i++) { publish(); await eager(); }
  const sigBefore = role.sync.sig, fullBefore = role.sync.lastFullAt;
  setNeighbors([NB1, NB3]);
  publish(); await eager();                                    // NB1 delta, NB3 full
  const byTo = Object.fromEntries(last(pushes, 2).map((p) => [p.to, p.n]));
  ok(byTo[NB1] === 1 && byTo[NB3] === 4, 'precondition: a mixed round (NB1 delta, NB3 full)', JSON.stringify(byTo));
  ok(role.sync.sig === sigBefore && role.sync.lastFullAt === fullBefore, 'the mixed round did not advance the full-push record', JSON.stringify(role.sync));
  await sweep();
  const sw = Object.fromEntries(last(pushes, 2).map((p) => [p.to, p.n]));
  ok(sw[NB1] === 4, 'so the next sweep sent NB1 the whole cache, not a keepalive', JSON.stringify(sw));
}

console.log('\n[10] RD-2: a role that leaves ROOT and returns starts with no replica stamps');
{
  const { pushes, publish, eager, role, am } = mk({ armed: true });
  publish(); await eager(); publish(); await eager();
  ok(role.replicaHw?.size === 2, 'precondition: both replicas carry a stamp');
  role.isRoot = false;
  const back = am._rootClaim._set(role, true, 'fence-return');
  ok(back?.committed !== false && role.isRoot === true, 'precondition: the role is ROOT again through RootClaim._set', JSON.stringify(back));
  ok(!(role.replicaHw?.size), 'the stamps of the earlier incarnation are gone', String(role.replicaHw?.size));
  publish(); await eager();
  ok(last(pushes, 2).every((p) => p.n === 3), 'and its first eager push is the whole cache', JSON.stringify(last(pushes, 2).map((p) => p.n)));
}

console.log('\n[11] RD-3: a push that was in flight across an invalidation records no stamp');
{
  const { pushes, publish, eager, importAt, role, hold } = mk({ armed: true, T: T_REAL });
  publish(); await eager(); publish();
  const q = []; hold.set(NB1, q);
  const flying = eager();                                      // NB1's verdict is held
  for (let i = 0; i < 20 && !q.length; i++) await Promise.resolve();
  ok(q.length === 1, 'precondition: one push to NB1 is in flight');
  const old = await importAt('old', role.lastTs - 500);        // invalidation while it flies
  q[0]({ consumed: true }); await flying;
  ok(!role.replicaHw?.has(NB1), 'the late verdict did not put NB1\'s stamp back', String(role.replicaHw?.get(NB1)));
  publish(); await eager();
  const byTo = Object.fromEntries(last(pushes, 2).map((p) => [p.to, p.ids.includes(old)]));
  ok(!!old && byTo[NB1] === true, 'and NB1\'s next eager push carried the old body', JSON.stringify(byTo));
}
{
  const { publish, eager, role, hold } = mk({ armed: true });
  publish(); await eager(); publish();
  const q = []; hold.set(NB1, q);
  const first = eager();                                       // stamp A, held
  for (let i = 0; i < 20 && !q.length; i++) await Promise.resolve();
  publish(); await eager();                                    // stamp B > A, lands first
  const b = role.replicaHw.get(NB1);
  q[0]({ consumed: true }); await first;                       // A lands second
  ok(role.replicaHw.get(NB1) === b, 'two verdicts arriving out of order never move a stamp backwards', `${role.replicaHw.get(NB1)} vs ${b}`);
}
{
  const { publish, eager, role, hold } = mk({ armed: true });
  publish(); await eager(); publish();
  const q = []; hold.set(NB1, q);
  const flying = eager();
  for (let i = 0; i < 20 && !q.length; i++) await Promise.resolve();
  role.isRoot = false;                                         // the role stopped being ROOT while the push flew
  q[0]({ consumed: true }); await flying;
  ok(!role.replicaHw?.has(NB1) || role.replicaHw.get(NB1) < role.lastTs, 'a verdict landing on a role that is no longer ROOT records no new stamp', String(role.replicaHw?.get(NB1)));
}

console.log('\n[12] RD-4 (Orion 4b1f95f0): a body cached on the DELIVER path, and a stamp above the next publish');
{
  const { pushes, publish, eager, deliverAt, role } = mk({ armed: true });
  publish(); await eager(); publish(); await eager();
  await deliverAt('dold', role.lastTs - 500);                 // RD-4a: a late DELIVER below the stamps, at a ROOT
  ok(role.cacheIds.has('dold'), 'precondition: the root cached a body that arrived by DELIVER below its high-water');
  publish(); await eager();
  ok(last(pushes, 2).every((p) => p.ids.includes('dold')), 'RD-4a: the next eager push carried that body to each replica', JSON.stringify(last(pushes, 2).map((p) => p.ids)));
}
for (const armed of [false, true]) {
  const { pushes, sweep, deliverAt, realPublish, role, am, clock } = mk({ armed, T: T_REAL });
  role.isRoot = false;                                        // a relay under a root whose clock runs 1.5 s ahead
  await deliverAt('skew', clock.t + 1500);
  ok(role.cacheIds.has('skew') && (role.lastTs || 0) < clock.t + 1500, `precondition (${armed ? 'armed' : 'unarmed'}): the relay cached the skewed body and its own last stamp did not move`);
  am._rootClaim._set(role, true, 'fence-promote');
  clock.t += 100; await sweep();
  clock.t += 100; const before = pushes.length;
  const { id, r } = await realPublish('new');
  const sent = pushes.slice(before);
  ok(r?.ok === true && role.cache.find((c) => c.msgId === id)?.publishTs < clock.t + 1300, `precondition (${armed ? 'armed' : 'unarmed'}): a real publish was stamped BELOW the skewed body`, JSON.stringify(r));
  ok(sent.length === 2 && sent.every((p) => p.ids.includes(id)), `RD-4b (${armed ? 'armed' : 'unarmed'}): the publish itself reached both replicas`, JSON.stringify(sent.map((p) => p.n)));
}

console.log('\n[13] an eager round with nothing new: armed, a member with no stamp gets the whole cache; unarmed, unchanged');
for (const armed of [true, false]) {
  const { pushes, publish, eager, sweep, role, am } = mk({ armed });
  for (let i = 0; i < 3; i++) { publish(); await eager(); }
  await sweep();                                              // every member full: the full-push record now matches the cache
  role.isRoot = false; am._rootClaim._set(role, true, 'fence-return');   // stamps dropped; cache, ledger and record unchanged
  await eager();                                              // nothing new (a kill claim runs the eager replicate like this)
  const ns = last(pushes, 2).map((p) => p.n);
  if (armed) ok(ns.every((n) => n === 3), 'armed: both members, having no stamp, got all 3', JSON.stringify(ns));
  else ok(ns.every((n) => n === 0), 'unarmed: both got a keepalive, as before this release', JSON.stringify(ns));
}

console.log(`\n${fail ? '✗' : '✓'} fence_replicate_delta: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
