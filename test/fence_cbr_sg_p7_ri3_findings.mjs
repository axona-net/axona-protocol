#!/usr/bin/env node
// =====================================================================
// fence_cbr_sg_p7_ri3_findings.mjs — Orion 7af4822f on 6b (322a09e), the
// Part 7 gate (0db60c0) and the RI-2 memory (27e6767).
//   SG-1  the loser's completing SUB records its host on the winner:
//     S1 W and L share host 'host-a': after the handover L does not count,
//        and W reports degraded no-successor.
//     S2 control, L on 'host-b': L is current, W not degraded.
//   P7-1  a refused staged-without-handoff round leaves no stuck candidate:
//     P1 after the refusal, L's candidate is clear and the corr closed, and
//        L's next verify offers again.
//   RI-3  role.inheritRefused is swept:
//     R1 an entry older than RENEW_MS for a departed peer is gone after a tick.
// Run: node test/fence_cbr_sg_p7_ri3_findings.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, RENEW_MS } from '../src/pubsub/constants.js';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

const EXPECT = 4;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

async function scene(name, { wOpts = {}, lOpts = {} } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, lId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId, { cutover: true, ...wOpts }); const L = fab.addNode(lId, { cutover: true, ...lOpts });
  W.am._announceRoot = () => {}; L.am._announceRoot = () => {};
  L.am.dht.lookup = async () => ({ path: [idHex(lId), idHex(wId)], hops: 1, time: 1, found: false });
  const lRole = L.rc.become(Lt.topicId, 'sub-terminal');
  const meta = (n) => ({ targetId: n.id, isTerminal: true, hopCount: 2, fromId: idHex(Lt.publisher) });
  const stamp = async (n, k) => {
    fab.clock += 10;
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: fab.clock });
    await n.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, meta(n));
    await fab.settle();
  };
  const trigger = async () => {
    lRole.formedAt = fab.clock - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
    L.am._verifyRoots(fab.clock);
    for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
    await fab.settle();
  };
  return { Lt, fab, W, L, lRole, stamp, trigger, wr: () => W.am.axonRoles.get(Lt.topicId) };
}

console.log('fence_cbr_sg_p7_ri3_findings: Orion 7af4822f SG-1, P7-1, RI-3');

for (const lHost of ['host-a', 'host-b']) {
  const gate = { successorGate: true, processKind: 'app' };
  const S = await scene(`cbr-sg1-${lHost}`, { wOpts: { ...gate, hostTag: 'host-a' }, lOpts: { ...gate, hostTag: lHost } });
  await S.stamp(S.L, 1); await S.stamp(S.L, 2);
  // The 6b gate (fence_cbr_6b_gate): an app winner takes the root only once a
  // second holder of L's is current. A backup X on a third host renews first.
  const X = idHex(S.L.id ^ 0x55n);
  await S.L.handlers.get('pubsub:sub')({ topicId: idHex(S.Lt.topicId), via: [], subscriberId: X, since: 0, hw: S.L.am._highWater(S.lRole), lw: 0, standby: true, fd: { kind: 'service', host: 'host-x' } }, { targetId: S.L.id, isTerminal: true, hopCount: 1, fromId: X });
  await S.fab.settle();
  await S.trigger();
  const w = S.wr();
  S.W.am._successorTick();
  const st = S.W.am.successorStatus(S.Lt.topicId);
  const rec = w?.successors?.get(idHex(S.L.id).toLowerCase()) ?? [...(w?.successors?.values?.() ?? [])][0] ?? null;
  if (lHost === 'host-a') check('S1 SG-1 W and L share a host: after the handover L is recorded with its host and does not count; W degraded',
    !!w?.isRoot && rec?.host === 'host-a' && st.current === 0 && st.degraded === 'no-successor', JSON.stringify({ root: !!w?.isRoot, rec, st }));
  else check('S2 control, L on another host: L is current, W not degraded',
    !!w?.isRoot && rec?.host === 'host-b' && st.current === 1 && st.degraded === null, JSON.stringify({ root: !!w?.isRoot, rec, st }));
}
{ const S = await scene('cbr-p71');
  await S.stamp(S.L, 1); await S.stamp(S.L, 2);
  S.fab.tap = (j) => { if (j.type === 'pubsub:pullup' && j.payload?.staged) { const p = { ...j.payload }; delete p.handoff; return { ...j, payload: p }; } return j; };
  await S.trigger();
  const offers1 = S.fab.sent.filter((j) => j.type === 'pubsub:mergeoffer').length;
  const cand = S.lRole.merge?.candidate ?? null;
  const closed = S.lRole.merge?.closedCorrs?.size ?? 0;
  S.fab.clock += ROOT_VERIFY_MS + 1;
  await S.trigger();
  const offers2 = S.fab.sent.filter((j) => j.type === 'pubsub:mergeoffer').length;
  check('P1 P7-1 after a staged-without-handoff refusal: candidate clear, corr closed, the next verify offers again',
    cand === null && closed >= 1 && offers2 > offers1 && S.lRole.isRoot, JSON.stringify({ cand, closed, offers1, offers2, l: S.lRole.isRoot })); }
{ const REG = 0x87n << 248n, hex = (b) => b.toString(16).padStart(66, '0'), H = (x) => x << 224n;
  const T = REG | 0x1000n, SELF = T ^ H(0x1n), GONE = hex(T ^ H(0x30n));
  const clock = { t: 1_000_000 };
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [hex(T ^ H(0x10n))], bridgeId: () => null };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2, replicaInherit: true });
  am.nodeId = SELF; am.setLogSink(() => {}); am._syncPush = () => ({ consumed: true });
  am.pubsubSubscribe(T); am._becomeRoot(T);
  const role = am.axonRoles.get(T);
  role.cache.push({ msgId: 'm1', publishTs: clock.t, json: '{}' });
  role.inheritRefused = new Map([[GONE, clock.t - RENEW_MS - 1]]);
  clock.t += 5_000; await am.refreshTick();
  check('R1 RI-3 an expired refusal for a departed peer is swept', !role.inheritRefused.has(GONE), JSON.stringify([...role.inheritRefused.keys()].map((h) => h.slice(0, 10)))); }

console.log(`\nfence_cbr_sg_p7_ri3_findings: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
