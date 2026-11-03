#!/usr/bin/env node
// =====================================================================
// fence_cbr_3c_replica_inherit.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 3c, replicas inherit (David 2026-11-03 "go ahead with 3c"), manager
// option replicaInherit / env REPLICA_INHERIT, default off.
//
// The root's backup cohort came from discovery alone (findKClosest, two
// channel-hops; neighbours in the fallback). Armed, it is the K closest of
// discovery + the nodes seated on the topic with this root (any kind) + the
// nodes this root's last verify visited. A closer candidate displaces the
// farthest replica, which is pushed to no more and lapses on its lease.
// Drives the real _replicateRole through refreshTick, with _syncPush stubbed
// to record targets and report consumed. K = 2. Distances to T: SELF 1,
// D1 16, N 32, D2 64, F 128.
//   R1  on, neighbours fallback: seated N is picked over D2.
//   R2  off (control): the cohort stays D1 + D2.
//   R3  on, findKClosest discovery: seated N displaces D2.
//   R4  on, N reached only by a fresh verify: N displaces D2.
//   R5  on, the verify set is stale: the cohort stays D1 + D2.
//   R6  on, N is the bridge: never a replica.
//   R7  on, seated F farther than every replica: displaces nobody.
//   R8  on, N seated after D1 + D2 were seeded: a replica-inherit row names N:seated
//       in and D2 out, and D2 receives no further push.
//   R9  on, the root's own verify (lookup visits D2, N) records them for the pick.
//   R10 off (control): the verify records nothing.
// Run: node test/fence_cbr_3c_replica_inherit.mjs
// =====================================================================
import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { sealTestDht }    from './lib/testCapability.mjs';
import { ROOT_VERIFY_MS } from '../src/pubsub/constants.js';

const EXPECT = 10;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

const REG = 0x87n << 248n;   // topics differ in the low bits; peers in the high bits
const hex = (b) => b.toString(16).padStart(66, '0');
const TICK = 5_000;
const H = (x) => x << 224n;   // offsets in the high bits, so logged 10-hex prefixes differ

function mk({ arm, T, discovery = 'neighbors', bridge = null }) {
  const SELF = T ^ H(0x1n);
  const D1 = hex(T ^ H(0x10n)), N = hex(T ^ H(0x20n)), D2 = hex(T ^ H(0x40n)), F = hex(T ^ H(0x80n));
  const clock = { t: 1_000_000 };
  const pushed = [];
  const logs = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: () => {},
    neighbors: () => [D1, D2],
    bridgeId: () => null,
    ...(bridge ? { bridgeNodeId: () => bridge } : {}),
    ...(discovery === 'findk' ? { findKClosest: async () => [D2, D1] } : {}),
    lookup: async () => ({ path: [hex(SELF), D2, N] }),
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2, replicaInherit: arm });
  am.nodeId = SELF;
  am.setLogSink((l, ev, ctx) => logs.push({ ev, ctx }));
  am._syncPush = (target) => { pushed.push(hex(target)); return { consumed: true }; };
  return { am, clock, pushed, logs, ids: { D1, N, D2, F } };
}
async function rootWith(am, clock, T, { seat = [], walked = null, walkedAge = 0 } = {}) {
  am.pubsubSubscribe(T);
  am._becomeRoot(T);
  const role = am.axonRoles.get(T);
  role.cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });
  for (const h of seat) role.subscribers.set(h, { since: clock.t, lastRenewed: clock.t });
  if (walked) role.verifySeen = { at: clock.t - walkedAge, ids: walked };
  for (let i = 0; i < 3; i++) {
    clock.t += TICK;
    for (const h of seat) { const s = role.subscribers.get(h); if (s) s.lastRenewed = clock.t; }
    if (walked && walkedAge === 0) role.verifySeen.at = clock.t;
    await am.refreshTick();
  }
  return role;
}
const cohort = (role) => [...role.replicas.keys()].sort();
const same = (a, b) => JSON.stringify([...a].sort()) === JSON.stringify([...b].sort());

console.log('fence_cbr_3c_replica_inherit: a node closer than the farthest replica takes the slot');

{ const T = REG | 0x1000n; const s = mk({ arm: true, T });
  const role = await rootWith(s.am, s.clock, T, { seat: [s.ids.N] });
  check('R1 on, neighbours: seated N is picked over D2', same(cohort(role), [s.ids.D1, s.ids.N]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x2000n; const s = mk({ arm: false, T });
  const role = await rootWith(s.am, s.clock, T, { seat: [s.ids.N] });
  check('R2 off (control): the cohort stays D1 + D2', same(cohort(role), [s.ids.D1, s.ids.D2]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x3000n; const s = mk({ arm: true, T, discovery: 'findk' });
  const role = await rootWith(s.am, s.clock, T, { seat: [s.ids.N] });
  check('R3 on, findKClosest: seated N displaces D2', same(cohort(role), [s.ids.D1, s.ids.N]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x4000n; const s = mk({ arm: true, T });
  const role = await rootWith(s.am, s.clock, T, { walked: [hex(T ^ H(0x20n))] });
  check('R4 on, N seen by a fresh verify: N displaces D2', same(cohort(role), [s.ids.D1, s.ids.N]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x5000n; const s = mk({ arm: true, T });
  const role = await rootWith(s.am, s.clock, T, { walked: [hex(T ^ H(0x20n))], walkedAge: 2 * ROOT_VERIFY_MS + 1 });
  check('R5 on, the verify set is stale: the cohort stays D1 + D2', same(cohort(role), [s.ids.D1, s.ids.D2]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x6000n; const s = mk({ arm: true, T, bridge: hex(T ^ H(0x20n)) });
  const role = await rootWith(s.am, s.clock, T, { seat: [s.ids.N] });
  check('R6 on, N is the bridge: never a replica', same(cohort(role), [s.ids.D1, s.ids.D2]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x7000n; const s = mk({ arm: true, T });
  const role = await rootWith(s.am, s.clock, T, { seat: [s.ids.F] });
  check('R7 on, seated F farther than every replica: displaces nobody', same(cohort(role), [s.ids.D1, s.ids.D2]), JSON.stringify(cohort(role).map((h) => h.slice(-3)))); }
{ const T = REG | 0x8000n; const s = mk({ arm: true, T });
  const role = await rootWith(s.am, s.clock, T);                       // D1 + D2 seeded first
  const hadD2 = s.pushed.includes(s.ids.D2);
  role.subscribers.set(s.ids.N, { since: s.clock.t, lastRenewed: s.clock.t });
  role.cache.push({ msgId: 'm2', ts: s.clock.t, json: '{}' });
  s.clock.t += TICK; await s.am.refreshTick();
  const mark = s.pushed.length;
  for (let i = 0; i < 3; i++) { s.clock.t += TICK; role.subscribers.get(s.ids.N).lastRenewed = s.clock.t; await s.am.refreshTick(); }
  const later = s.pushed.slice(mark);
  const row = s.logs.find((l) => /replica-inherit/.test(l.ev));
  check('R8 on, N seated after D1 + D2: row names N:seated in and D2 out; D2 gets no further push', hadD2 && later.length > 0 && !later.includes(s.ids.D2) && later.includes(s.ids.N) && same(cohort(role), [s.ids.D1, s.ids.N])
    && !!row && row.ctx.in.includes(`${s.ids.N.slice(0, 10)}:seated`) && row.ctx.out.includes(s.ids.D2.slice(0, 10)),
    JSON.stringify({ hadD2, later: later.map((h) => h.slice(-3)), cohort: cohort(role).map((h) => h.slice(-3)), row: row?.ctx ?? null })); }

for (const arm of [true, false]) {
  const T = arm ? (REG | 0x9000n) : (REG | 0xa000n); const s = mk({ arm, T });
  s.am.pubsubSubscribe(T); s.am._becomeRoot(T);
  const role = s.am.axonRoles.get(T); role.formedAt = 0; role.lastVerify = 0;
  s.am._verifyRoots(s.clock.t);
  for (let i = 0; i < 20 && s.am._verifyInflight?.has(T); i++) await new Promise((r) => setTimeout(r, 10));
  const ids = role.verifySeen?.ids ?? null;
  if (arm) check('R9 on, the root\'s verify records the nodes it visited (D2, N), not itself', Array.isArray(ids) && ids.includes(s.ids.D2) && ids.includes(s.ids.N) && ids.length === 2, JSON.stringify(ids));
  else check('R10 off (control): the verify records nothing', ids === null, JSON.stringify(ids));
}
console.log(`\nfence_cbr_3c_replica_inherit: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
