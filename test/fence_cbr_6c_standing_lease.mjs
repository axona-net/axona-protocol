#!/usr/bin/env node
// =====================================================================
// fence_cbr_6c_standing_lease.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 6c, a lease for standing topics (David 2026-11-03 "go ahead with 6c");
// manager option standingLease / env STANDING_LEASE, default off.
//
// An empty root with no demand is dismantled DROP_MS after its last demand
// (Topic Lifetime 4.5), which ends its readers' subscriptions. A standing
// lease keeps it, held where the metrics lease is held. Real AxonaManager,
// refreshTick, _onSub, _syncIngest; stub dht; Topic Lifetime on (default).
//   S1 a standing SUB keeps an empty root past DROP_MS: no termination.
//   S2 control, no standing SUB: the empty root is retired (reaped or terminated).
//   S3 past the lease the root terminates as any other.
//   S4 a lease asked beyond the MAX is held only to the MAX.
//   S5 a standing subscription declares the lease on its SUB; unarmed, none.
//   S6 a full REPLICATE carries the lease; the backup promoted to root holds it.
// Run: node test/fence_cbr_6c_standing_lease.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { DROP_MS, STANDING_LEASE_MS, STANDING_LEASE_MAX_MS, T as WT } from '../src/pubsub/constants.js';

const EXPECT = 7;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });
const REG = 0x87n << 248n, hex = (b) => b.toString(16).padStart(66, '0'), H = (x) => x << 224n;
const TICK = 5_000;

function mk(T, { armed = true, replicas = 0 } = {}) {
  const SELF = T ^ H(0x1n); const clock = { t: 10_000_000 }; const logs = []; const sent = [];
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => ({ consumed: true }), neighbors: () => [], bridgeId: () => null };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: replicas, standingLease: armed });
  am.nodeId = SELF; am.setLogSink((l, ev, ctx) => logs.push({ ev, ctx }));
  return { am, clock, logs, sent, SELF };
}
const rootOf = (s, T) => { s.am._becomeRoot(T); return s.am.axonRoles.get(T); };
const subFrom = async (s, T, from, standing) => s.am._onSub({ topicId: hex(T), via: [], subscriberId: hex(from), since: 0, hw: 0, lw: 0, ...(standing ? { standing } : {}) }, { isTerminal: true, fromId: hex(from), targetId: s.SELF });
const idleFor = async (s, ms) => { const end = s.clock.t + ms; while (s.clock.t < end) { s.clock.t += TICK; await s.am.refreshTick(); } };
const terminated = (s) => s.logs.some((l) => /topic-terminating|role-reaped/.test(l.ev));   // either retirement path: Topic Lifetime termination or the empty-role reaper

console.log('fence_cbr_6c_standing_lease: a standing lease keeps an empty root alive');
{ const T = REG | 0x1000n; const s = mk(T); const role = rootOf(s, T);
  await subFrom(s, T, T ^ H(0x50n), STANDING_LEASE_MS);
  role.subscribers.clear();                                   // the reader goes; no demand from here
  await idleFor(s, DROP_MS + 6 * TICK);
  check('S1 a standing SUB keeps an empty root past DROP_MS', !terminated(s) && !!s.am.axonRoles.get(T)?.isRoot, JSON.stringify({ terminated: terminated(s), until: role.standingUntil - s.clock.t }));
  s.clock.t = role.standingUntil + 1; await idleFor(s, DROP_MS + 6 * TICK);
  check('S3 past the lease the root terminates as any other', terminated(s), JSON.stringify({ terminated: terminated(s) })); }
{ const T = REG | 0x2000n; const s = mk(T); const role = rootOf(s, T);
  await subFrom(s, T, T ^ H(0x50n), 0); role.subscribers.clear();
  await idleFor(s, DROP_MS + 6 * TICK);
  check('S2 control, no standing SUB: the empty root is retired (reaped or terminated)', terminated(s) && !s.am.axonRoles.get(T)?.isRoot); }
{ const T = REG | 0x3000n; const s = mk(T); const role = rootOf(s, T);
  await subFrom(s, T, T ^ H(0x50n), 10 * STANDING_LEASE_MAX_MS);
  check('S4 a lease asked beyond the MAX is held only to the MAX', role.standingUntil <= s.clock.t + STANDING_LEASE_MAX_MS && role.standingUntil > s.clock.t + STANDING_LEASE_MAX_MS - 1000, JSON.stringify({ heldMs: role.standingUntil - s.clock.t })); }
for (const armed of [true, false]) {
  const T = armed ? (REG | 0x4000n) : (REG | 0x4100n); const s = mk(T, { armed });
  const seen = []; s.am._send = (type, payload) => { seen.push({ type, payload }); return { consumed: true }; };
  s.am.pubsubSubscribe(T, { standing: true });
  const subs = seen.filter((x) => x.type === WT.SUB);
  if (armed) check('S5 a standing subscription declares its lease on the SUB', subs.length > 0 && subs.every((x) => x.payload.standing === STANDING_LEASE_MS), JSON.stringify(subs.map((x) => x.payload.standing ?? null)));
  else check('S5b unarmed: no standing field', subs.length > 0 && subs.every((x) => !('standing' in x.payload)), JSON.stringify(subs.map((x) => x.payload.standing ?? null)));
}
{ const T = REG | 0x5000n; const root = mk(T); const rRole = rootOf(root, T);
  rRole.cache.push({ msgId: 'm1', publishTs: root.clock.t, json: '{}' });
  await subFrom(root, T, T ^ H(0x50n), STANDING_LEASE_MS);
  let captured = null; root.am._route = (target, type, payload) => { if (type === WT.REPLICATE) captured = payload; return Promise.resolve({ consumed: true }); };
  root.am._syncPush(T ^ H(0x60n), T, rRole, 'COHORT_REPLICATE', { full: true });
  const backup = mk(T, { replicas: 2 }); backup.am.nodeId = T ^ H(0x60n);
  if (captured) await backup.am._syncIngest({ ...captured, topicId: hex(T), from: hex(root.SELF) }, { fromId: hex(root.SELF), targetId: backup.am.nodeId }, 'REPLICATE');
  const bRole = backup.am.axonRoles.get(T);
  if (bRole) backup.am._rootClaim._set(bRole, true, 'test-promote');
  check('S6 a full REPLICATE carries the lease and the promoted backup holds it', Number.isFinite(captured?.standingRemainingMs) && captured.standingRemainingMs > 0 && bRole?.standingUntil > backup.clock.t,
    JSON.stringify({ rem: captured?.standingRemainingMs ?? null, held: bRole ? bRole.standingUntil - backup.clock.t : null })); }

console.log(`\nfence_cbr_6c_standing_lease: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
