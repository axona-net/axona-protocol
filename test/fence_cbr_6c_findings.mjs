#!/usr/bin/env node
// =====================================================================
// fence_cbr_6c_findings.mjs — Orion d8fe2183 on 6c (94891d2), the standing
// lease (David 2026-11-03 "go ahead with 6c"), option standingLease, default off.
//   C1 6C-1: peer.sub(topic, h, { standing: true }) reaches the manager and the
//      wire SUB; when the standing subscription stops and a plain one remains,
//      the lease is no longer declared.
//   C2 6C-2: a leased root that has carried nothing for ROLE_IDLE_TTL_MS is not
//      taken by the 24 h idle reaper; control, unleased, is.
//   C3 6C-3: a stager (staging.created, not root) is not leased by a SUB, and
//      the registry types both new fields as numbers.
//   C4 the root's own standing renewal, which self-loops, still leases it.
// Run: node test/fence_cbr_6c_findings.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { rowDefs }   from '../src/pubsub/boundary1Registry.js';
import { STANDING_LEASE_MS, ROLE_IDLE_TTL_MS, T as WT } from '../src/pubsub/constants.js';

const EXPECT = 7;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });
const REG = 0x87n << 248n, hex = (b) => b.toString(16).padStart(66, '0'), H = (x) => x << 224n;
const TICK = 5_000;

function mk(T) {
  const SELF = T ^ H(0x1n); const clock = { t: 10_000_000 }; const logs = [];
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => ({ consumed: true }), neighbors: () => [], bridgeId: () => null };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 0, standingLease: true });
  am.nodeId = SELF; am.setLogSink((l, ev, ctx) => logs.push({ ev, ctx }));
  return { am, clock, logs, SELF };
}
const subFrom = async (s, T, from, standing) => s.am._onSub({ topicId: hex(T), via: [], subscriberId: hex(from), since: 0, hw: 0, lw: 0, ...(standing ? { standing } : {}) }, { isTerminal: true, fromId: hex(from), targetId: s.SELF });

console.log('fence_cbr_6c_findings: Orion d8fe2183 on the standing lease');
{ // C1 — the public API
  const net = new SimNetwork(); const domain = new AxonaDomain({});
  const id = await createNodeIdentity({ lat: 40, lng: -74 });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0, sendTimeoutMs: 400 }); await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat: 40, lng: -74 }); node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport }); await peer.start();
  const am = peer._requireAxonaManager('fence'); am._standingLease = true;
  const seen = []; const send = am._send.bind(am); am._send = (type, payload, ...r) => { if (type === WT.SUB) seen.push(payload); return send(type, payload, ...r); };
  const desc = { region: 'eagle', name: 'cbr-6c-findings' };
  const plain = await peer.sub(desc, () => {});
  const standing = await peer.sub(desc, () => {}, { standing: true });
  const T = [...am.mySubscriptions.keys()][0];
  const wire = seen.at(-1)?.standing ?? null;
  check('C1a peer.sub { standing: true } reaches mySubscriptions and the wire SUB', am.mySubscriptions.get(T)?.standing === true && wire === STANDING_LEASE_MS, JSON.stringify({ ms: am.mySubscriptions.get(T)?.standing, wire }));
  await standing.stop();
  check('C1b the standing subscription stops, a plain one remains: no lease declared', am.mySubscriptions.get(T)?.standing === false && am.mySubscriptions.has(T), JSON.stringify({ ms: am.mySubscriptions.get(T)?.standing }));
  await plain.stop();
  try { am.stop?.(); } catch { /* */ } try { await peer.stop?.(); } catch { /* */ } try { await transport.stop(); } catch { /* */ }
}
for (const leased of [true, false]) { // C2 — the 24 h idle reaper
  const T = leased ? (REG | 0x1000n) : (REG | 0x1100n); const s = mk(T);
  s.clock.t = 2 * ROLE_IDLE_TTL_MS;   // createdAt below must stay positive: _roleIdleMs reads 0 for anchor <= 0
  s.am._becomeRoot(T); const role = s.am.axonRoles.get(T);
  role.createdAt = s.clock.t - (ROLE_IDLE_TTL_MS - 60_000); role.lastTs = 0;
  await subFrom(s, T, T ^ H(0x50n), leased ? STANDING_LEASE_MS : 0);
  for (let i = 0; i < 24; i++) { s.clock.t += TICK; await s.am.refreshTick(); }
  const alive = !!s.am.axonRoles.get(T)?.isRoot;
  if (leased) check('C2a a leased root idle past ROLE_IDLE_TTL_MS is not reaped', alive && role.standingUntil > s.clock.t, JSON.stringify({ alive, left: role.standingUntil - s.clock.t }));
  else check('C2b control, unleased: the idle reaper takes it', !alive, JSON.stringify({ alive }));
}
{ // C3 — the stager, and the registry types
  const T = REG | 0x2000n; const s = mk(T);
  s.am._becomeRoot(T); const role = s.am.axonRoles.get(T);
  const L = T ^ H(0x50n);
  role.isRoot = false; role.staging = { created: true, corr: 'ab'.repeat(32), loser: hex(L) };
  // The loser's own SUB with a merge block that does not complete is the one
  // that passes the staging intercept and reaches the staging return.
  await s.am._onSub({ topicId: hex(T), via: [], subscriberId: hex(L), since: 0, hw: 0, lw: 0, standing: STANDING_LEASE_MS, merge: { corr: 'cd'.repeat(32) } }, { isTerminal: true, fromId: hex(L), targetId: s.SELF });
  check('C3a a stager is not leased by an uncompleted SUB', !(role.standingUntil > s.clock.t), JSON.stringify({ until: role.standingUntil ?? null }));
  const reg = (wire) => rowDefs().find((d) => d?.wire === wire);
  check('C3b registry: SUB.standing and REPLICATE.standingRemainingMs are typed number', reg(WT.SUB)?.schema?.types?.standing === 'number' && reg(WT.REPLICATE)?.schema?.types?.standingRemainingMs === 'number');
}
{ // C4 — the root's own renewal
  const T = REG | 0x3000n; const s = mk(T);
  s.am._becomeRoot(T); const role = s.am.axonRoles.get(T);
  await subFrom(s, T, s.SELF, STANDING_LEASE_MS);
  check('C4 the root\'s own standing SUB, self-looped, leases it', role.standingUntil > s.clock.t, JSON.stringify({ until: role.standingUntil ?? null }));
}
console.log(`\nfence_cbr_6c_findings: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
