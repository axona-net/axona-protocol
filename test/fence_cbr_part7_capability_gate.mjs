#!/usr/bin/env node
// =====================================================================
// fence_cbr_part7_capability_gate.mjs — Closer-Born Relay Inherits the Root
// v0.6 Part 7, the capability gate (David 2026-11-03 "go ahead with Part 7").
//
// A loser that runs the staged handover (3e, `cutover`) says so on its
// MERGE_OFFER (`handoff: 1`); a candidate with no root role stages only from
// such a loser, and its staged PULLUP says so back. An old loser forwards
// writes to a winner that cannot stamp, so its offer takes the existing path:
// the candidate refuses not-root and holds nothing. The loser marks its gate
// staged only when the pull carries both `staged` and `handoff`.
//   P1 armed W, legacy L (no handoff on the offer): W refuses not-root, holds no
//      role, logs merge-stage-legacy; L stays root.
//   P2 control, both armed: W stages and ends the one root.
//   P3 legacy W, armed L: W answers not-root as before; L stays root.
//   P4 the armed L's offer carries handoff: 1; a legacy L's carries none.
//   P5 the staged PULLUP carries staged and handoff. With handoff stripped in
//      flight, L refuses the pull before any cut (merge-stale
//      staged-without-handoff): no gate, L stays root, W never root. Intact,
//      L cuts and marks its gate staged.
// Run: node test/fence_cbr_part7_capability_gate.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';

const EXPECT = 6;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

async function scene(name, { wArmed = true, lArmed = true } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, lId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId, wArmed ? { cutover: true } : {}); const L = fab.addNode(lId, lArmed ? { cutover: true } : {});
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
  const ev = (n) => n.logs.map((l) => l.ev.replace('pubsub:', ''));
  const sent = (type) => (fab.sent ?? []).filter((j) => j.type === type);
  return { Lt, fab, W, L, lRole, stamp, trigger, ev, sent, wr: () => W.am.axonRoles.get(Lt.topicId) };
}

console.log('fence_cbr_part7_capability_gate: stage only toward a loser that runs the staged handover');
{ const S = await scene('cbr-p7-legacy-loser', { lArmed: false });
  await S.stamp(S.L, 1); await S.stamp(S.L, 2);
  await S.trigger();
  const e = S.ev(S.W), acks = S.sent('pubsub:replayack').map((j) => j.payload?.disposition);
  check('P1 armed W, legacy L: W refuses not-root, holds no role, logs merge-stage-legacy; L stays root',
    !S.wr() && e.includes('merge-stage-legacy') && !e.includes('merge-stage-start') && acks.includes('not-root') && S.lRole.isRoot,
    JSON.stringify({ wRole: !!S.wr(), ev: e.filter((x) => /merge|stage/.test(x)), acks, l: S.lRole.isRoot })); }
{ const S = await scene('cbr-p7-both-armed');
  await S.stamp(S.L, 1); await S.stamp(S.L, 2);
  await S.trigger();
  const e = S.ev(S.W);
  check('P2 control, both armed: W stages and ends the one root', !!S.wr()?.isRoot && !S.lRole.isRoot && e.includes('merge-stage-start') && !e.includes('merge-stage-legacy'),
    JSON.stringify({ w: !!S.wr()?.isRoot, l: S.lRole.isRoot, ev: e.filter((x) => /merge|stage/.test(x)) })); }
{ const S = await scene('cbr-p7-legacy-winner', { wArmed: false });
  await S.stamp(S.L, 1);
  await S.trigger();
  const acks = S.sent('pubsub:replayack').map((j) => j.payload?.disposition);
  check('P3 legacy W, armed L: W answers not-root as before; L stays root', !S.wr() && acks.includes('not-root') && S.lRole.isRoot,
    JSON.stringify({ wRole: !!S.wr(), acks, l: S.lRole.isRoot })); }
{ const A = await scene('cbr-p7-offer-armed'); await A.stamp(A.L, 1); await A.trigger();
  const B = await scene('cbr-p7-offer-legacy', { lArmed: false }); await B.stamp(B.L, 1); await B.trigger();
  const oa = A.sent('pubsub:mergeoffer').map((j) => j.payload?.handoff), ob = B.sent('pubsub:mergeoffer').map((j) => j.payload?.handoff);
  check('P4 the armed L\'s offer carries handoff: 1; a legacy L\'s carries none', oa.length > 0 && oa.every((h) => h === 1) && ob.length > 0 && ob.every((h) => h === undefined),
    JSON.stringify({ armed: oa, legacy: ob })); }
for (const strip of [true, false]) {
  const S = await scene(strip ? 'cbr-p7-pull-stripped' : 'cbr-p7-pull-intact');
  await S.stamp(S.L, 1); await S.stamp(S.L, 2);
  let pulls = [];
  S.fab.tap = (j) => {
    if (j.type === 'pubsub:pullup' && j.payload?.staged) { pulls.push({ staged: j.payload.staged, handoff: j.payload.handoff }); if (strip) { const p = { ...j.payload }; delete p.handoff; return { ...j, payload: p }; } }
    if (j.type === 'pubsub:replayack') return null;   // hold the round open so the gate is observable
    return j;
  };
  await S.trigger();
  const gate = S.lRole.merge?.gate;
  const refused = S.L.logs.some((l) => /merge-stale/.test(l.ev) && l.ctx?.why === 'staged-without-handoff');
  if (strip) check('P5a the staged PULLUP carries staged and handoff; stripped in flight, L refuses before any cut and stays root',
    pulls.length > 0 && pulls.every((p) => p.staged === true && p.handoff === 1) && !gate && refused && S.lRole.isRoot && !S.wr()?.isRoot, JSON.stringify({ pulls, gate: gate ?? null, refused, l: S.lRole.isRoot, w: !!S.wr()?.isRoot }));
  else check('P5b intact: L marks its gate staged', !!gate && gate.staged === true, JSON.stringify({ gate: gate ? { staged: gate.staged ?? null } : null }));
}
console.log(`\nfence_cbr_part7_capability_gate: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
