#!/usr/bin/env node
// =====================================================================
// fence_cbr_6b_successor_gate.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 6b, hold the root only with a current successor (David 2026-11-03
// "go ahead with 6b"), manager option successorGate / env SUCCESSOR_GATE,
// default off; processKind 'service' (default) | 'app'; hostTag.
//
// An 'app' node holding a root reports degraded: no-successor while no
// successor is CURRENT (its SUB within RENEW_MS, hw at or above the root's
// high-water as of RENEW_MS ago, failure domain not this host), and re-seats
// (the next replicate tick is FULL). It keeps the root. Successors are
// replicas, standby seats, and the loser of a staged handover it won.
// Real AxonaManager, real _onSub and refreshTick; the dht is stubbed.
//   G1  app, armed, no successor: degraded no-successor, one warn row, still root.
//   G1b the re-seat: a FULL push on entering, then at most one per RENEW_MS
//       (a service root in the same state sends keepalives).
//   G2  service (control): never degraded.
//   G3  app, gate off (control): never degraded.
//   G4  a standby seat's SUB, other host, hw current: current; degraded clears, successor-current row.
//   G5  that SUB names this node's own host: does not count.
//   G6  the successor's last SUB is older than RENEW_MS: does not count.
//   G7  one renewal of lag counts (hw = the high-water RENEW_MS ago); more lag does not.
//   G8  a plain reader's SUB is not a successor.
//   G9  _stagePromote under a staged handover seeds the loser as a current successor.
//   G10 an armed node's SUB carries fd { kind, host }; unarmed carries none.
// Run: node test/fence_cbr_6b_successor_gate.mjs
// =====================================================================
import { AxonaManager }       from '../src/pubsub/AxonaManager.js';
import { sealTestDht }        from './lib/testCapability.mjs';
import { RENEW_MS, T as WT }  from '../src/pubsub/constants.js';
import { makeRole }           from '../src/pubsub/rootClaim.js';

const EXPECT = 12;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

const REG = 0x87n << 248n;
const hex = (b) => b.toString(16).padStart(66, '0');
const H = (x) => x << 224n;
const TICK = 5_000;

function mk({ T, gate = true, kind = 'app', host = 'host-a', cohort = [] }) {
  const SELF = T ^ H(0x1n);
  const clock = { t: 10_000_000 };
  const logs = [], sent = [];
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => cohort, bridgeId: () => null };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2, successorGate: gate, processKind: kind, hostTag: host });
  am.nodeId = SELF;
  am.setLogSink((l, ev, ctx) => logs.push({ l, ev, ctx }));
  am._syncPush = (target, t, role, policy, opts) => { sent.push({ at: clock.t, full: opts?.full !== false }); return { consumed: true }; };
  return { am, clock, logs, sent, SELF };
}
async function root(s, T, { msgsAgo = [] } = {}) {
  s.am.pubsubSubscribe(T); s.am._becomeRoot(T);
  const role = s.am.axonRoles.get(T);
  for (const [i, ago] of msgsAgo.entries()) role.cache.push({ msgId: `m${i}`, publishTs: s.clock.t - ago, json: '{}' });
  role.cache.sort((a, b) => a.publishTs - b.publishTs);
  return role;
}
const TERMINAL = (from) => ({ isTerminal: true, fromId: from, targetId: null });
const sub = (T, from, extra) => ({ topicId: hex(T), via: [], subscriberId: from, since: 0, lw: 0, ...extra });
const tick = async (s, n = 1) => { for (let i = 0; i < n; i++) { s.clock.t += TICK; await s.am.refreshTick(); } };
const rows = (s, ev) => s.logs.filter((l) => l.ev === ev || String(l.ev).endsWith(':' + ev));

console.log('fence_cbr_6b_successor_gate: an app-held root states whether a successor is current');

{ const T = REG | 0x1000n; const s = mk({ T }); const role = await root(s, T, { msgsAgo: [1000] });
  await tick(s, 2);
  const st = s.am.successorStatus(T);
  check('G1 app, armed, no successor: degraded no-successor, one warn row, full push re-armed, still root',
    st.degraded === 'no-successor' && rows(s, 'degraded').length === 1 && rows(s, 'degraded')[0].l === 'warn' && role.isRoot,
    JSON.stringify({ st, rows: rows(s, 'degraded').length })); }
{ // G1b: 30 ticks (150 s) with one cohort member D and an unchanged cache.
  const fulls = async (kind, T) => {
    const s = mk({ T, kind, cohort: [hex(T ^ H(0x10n))] }); await root(s, T, { msgsAgo: [1000] });
    await tick(s, 30);
    return s.sent.filter((x) => x.full).length;
  };
  const app = await fulls('app', REG | 0x1100n), svc = await fulls('service', REG | 0x1200n);
  // service: the first push + the ROOT_REPLICATE_FULL_MS backstop (60 s) = 3 in 150 s.
  // app: those, plus the re-seat on entering and each RENEW_MS; never every tick.
  check('G1b re-seat: more full pushes than a service root, but never one per tick', app > svc && app <= 2 * svc + 1 && app < 30, JSON.stringify({ app, svc, ticks: 30 })); }
{ const T = REG | 0x2000n; const s = mk({ T, kind: 'service' }); await root(s, T, { msgsAgo: [1000] });
  await tick(s, 2);
  check('G2 service (control): never degraded', s.am.successorStatus(T).degraded === null && rows(s, 'degraded').length === 0, JSON.stringify(s.am.successorStatus(T))); }
{ const T = REG | 0x3000n; const s = mk({ T, gate: false }); await root(s, T, { msgsAgo: [1000] });
  await tick(s, 2);
  check('G3 app, gate off (control): never degraded', s.am.successorStatus(T).degraded === null && rows(s, 'degraded').length === 0, JSON.stringify(s.am.successorStatus(T))); }

async function withSuccessor(T, { host = 'host-b', hwAgo = 0, subAge = 0, standby = true, msgsAgo = [1000] }) {
  const s = mk({ T }); const role = await root(s, T, { msgsAgo });
  await tick(s, 1);                                     // degraded first
  const B = hex(T ^ H(0x20n));
  const myHw = s.am._highWater(role);
  await s.am._onSub(sub(T, B, { hw: myHw - hwAgo, ...(standby ? { standby: true } : {}), fd: { kind: 'service', host } }), TERMINAL(B));
  if (subAge) s.clock.t += subAge;
  await tick(s, 1);
  return { s, role, B };
}
{ const { s } = await withSuccessor(REG | 0x4000n, {});
  const st = s.am.successorStatus(REG | 0x4000n);
  check('G4 standby seat, other host, hw current: current; degraded clears with a successor-current row',
    st.degraded === null && st.current === 1 && rows(s, 'successor-current').length === 1, JSON.stringify({ st, rows: rows(s, 'successor-current').length })); }
{ const { s } = await withSuccessor(REG | 0x5000n, { host: 'host-a' });
  const st = s.am.successorStatus(REG | 0x5000n);
  check('G5 the successor names this node\'s own host: does not count', st.degraded === 'no-successor' && st.current === 0, JSON.stringify(st)); }
{ const { s } = await withSuccessor(REG | 0x6000n, { subAge: RENEW_MS + 1 });
  const st = s.am.successorStatus(REG | 0x6000n);
  check('G6 the successor\'s last SUB is older than RENEW_MS: does not count', st.degraded === 'no-successor' && st.current === 0, JSON.stringify(st)); }
{ // cache: one message RENEW_MS + 2 s old, one 1 s old. hw-then = the older one.
  const T7 = REG | 0x7000n, T7b = REG | 0x7100n;
  const mkLag = async (T, lagBeyond) => {
    const s = mk({ T }); const role = await root(s, T, { msgsAgo: [RENEW_MS + 2000, 1000] });
    await tick(s, 1);
    const then = role.cache[0].publishTs;
    const B = hex(T ^ H(0x20n));
    await s.am._onSub(sub(T, B, { hw: lagBeyond ? then - 1 : then, standby: true, fd: { kind: 'service', host: 'host-b' } }), TERMINAL(B));
    await tick(s, 1);
    return s.am.successorStatus(T);
  };
  const ok1 = await mkLag(T7, false), ok2 = await mkLag(T7b, true);
  check('G7 one renewal of lag counts; more lag does not', ok1.current === 1 && ok1.degraded === null && ok2.current === 0 && ok2.degraded === 'no-successor', JSON.stringify({ oneRenewal: ok1, beyond: ok2 })); }
{ const { s } = await withSuccessor(REG | 0x8000n, { standby: false });
  const st = s.am.successorStatus(REG | 0x8000n);
  check('G8 a plain reader\'s SUB is not a successor', st.degraded === 'no-successor' && st.current === 0 && st.known === 0, JSON.stringify(st)); }
{ const T = REG | 0x9000n; const s = mk({ T });
  const role = makeRole(T, false, s.clock.t);   // a stager: no role for T until the handover
  s.am.axonRoles.set(T, role);
  role.cache.push({ msgId: 'mx', publishTs: s.clock.t - 1000, json: '{}' });
  const L = hex(T ^ H(0x40n));
  role.staging = { corr: 'corr-6b-9', created: false, at: s.clock.t };
  s.am._stagePromote(role, { loser: L, loserEpoch: 3 }, 'corr-6b-9');
  await tick(s, 1);
  const st = s.am.successorStatus(T);
  check('G9 a won staged handover seeds the loser as a current successor', role.isRoot && st.current === 1 && role.successors?.get(L)?.via === 'loser' && st.degraded === null, JSON.stringify({ isRoot: role.isRoot, st, l: role.successors?.get(L) ?? null })); }
for (const gate of [true, false]) {
  const T = gate ? (REG | 0xa000n) : (REG | 0xa100n); const s = mk({ T, gate });
  const seen = [];
  s.am._send = (type, payload) => { seen.push({ type, payload }); return { consumed: true }; };
  s.am.pubsubSubscribe(T);
  const subs = seen.filter((x) => x.type === WT.SUB);
  if (gate) check('G10a armed: the SUB carries fd { kind: app, host }', subs.length > 0 && subs.every((x) => x.payload.fd && x.payload.fd.kind === 'app' && x.payload.fd.host === 'host-a'), JSON.stringify(subs.map((x) => x.payload.fd ?? null)));
  else check('G10b unarmed (control): the SUB carries no fd', subs.length > 0 && subs.every((x) => !('fd' in x.payload)), JSON.stringify(subs.map((x) => x.payload.fd ?? null)));
}

console.log(`\nfence_cbr_6b_successor_gate: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
