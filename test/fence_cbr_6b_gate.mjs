#!/usr/bin/env node
// =====================================================================
// fence_cbr_6b_gate.mjs — Closer-Born Relay Inherits the Root v0.6 Part 6b
// as the GATE (David 2026-11-03 "go ahead with 6b as the gate");
// successorGate, cutover.
//
// "A closer ephemeral node offered the win stays a replica until a second
// node has pulled the cache, then takes the root; the loser's cache is
// released only after that pull's terminal ack." Built at the loser, which is
// the service: an app-process winner says so on its staged pull (ephemeral,
// fdHost); the loser accepts its terminal ack but HOLDS STEP 5 (demote,
// release, promotion) until a second holder is current: a backup of the loser
// other than the winner, not on the winner's host, renewed within RENEW_MS at
// or above the loser's high-water. Past the round's lease, step 5 aborts and
// the loser keeps the root.
//   G1 ephemeral W, no second holder: step 5 deferred; L still root, W staged.
//   G1b the winner itself renewing as a standby (any host) never counts.
//   G2 a second holder S (other host) renews: step 5 runs; W ends the root.
//   G3 S on W's own host never counts: past the lease the round aborts; L root.
//   G4 control, W a service: no deferral.
//   G5 control, gate off at L: no deferral even for an ephemeral W.
// Run: node test/fence_cbr_6b_gate.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS, MERGE_TIMEOUT_MS } from '../src/pubsub/constants.js';

const EXPECT = 6;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });
const settleMore = async (fab) => { for (let i = 0; i < 8; i++) { await fab.settle(); await new Promise((r) => setImmediate(r)); } };

async function scene(name, { wApp = true, lGate = true } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const ids = [await freshId(), await freshId(), await freshId()].sort((a, b) => ((a ^ Lt.topicId) < (b ^ Lt.topicId) ? -1 : 1));
  const [wId, lId, sId] = ids;   // W closest, then L; S is a farther third node
  const W = fab.addNode(wId, { cutover: true, successorGate: true, processKind: wApp ? 'app' : 'service', hostTag: 'host-w' });
  const L = fab.addNode(lId, { cutover: true, successorGate: lGate, processKind: 'service', hostTag: 'host-l' });
  for (const n of [W, L]) { n.am._announceRoot = () => {}; n.am._mono = () => fab.clock; }
  L.am.dht.lookup = async () => ({ path: [idHex(lId), idHex(wId)], hops: 1, time: 1, found: false });
  const lRole = L.rc.become(Lt.topicId, 'sub-terminal');
  for (let k = 1; k <= 2; k++) {
    fab.clock += 10;
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: fab.clock });
    await L.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, { targetId: L.id, isTerminal: true, hopCount: 2, fromId: idHex(Lt.publisher) });
    await fab.settle();
  }
  const trigger = async () => {
    lRole.formedAt = fab.clock - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
    L.am._verifyRoots(fab.clock);
    for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
    await settleMore(fab);
  };
  // S, a backup of L, renews to L with a standby SUB carrying its hw and host.
  const sRenews = async (host) => {
    await L.handlers.get('pubsub:sub')({ topicId: idHex(Lt.topicId), via: [], subscriberId: idHex(sId), since: 0, hw: L.am._highWater(lRole), lw: 0, standby: true, fd: { kind: 'service', host } }, { targetId: L.id, isTerminal: true, hopCount: 1, fromId: idHex(sId) });
    await settleMore(fab);
  };
  const ev = (n) => n.logs.map((l) => l.ev.replace('pubsub:', ''));
  return { Lt, fab, W, L, lRole, trigger, sRenews, ev, wr: () => W.am.axonRoles.get(Lt.topicId) };
}

console.log('fence_cbr_6b_gate: an ephemeral winner takes the root only once a second holder is current');
{ const S = await scene('cbr-6bg-defer');
  await S.trigger();
  check('G1 ephemeral W, no second holder: step 5 deferred, L still root, W not root', S.ev(S.L).includes('merge-step5-deferred') && S.lRole.isRoot && !S.wr()?.isRoot && !S.ev(S.L).includes('merge-step5'),
    JSON.stringify({ l: S.lRole.isRoot, w: !!S.wr()?.isRoot, ev: S.ev(S.L).filter((x) => /step5|merge-ack/.test(x)) }));
  await S.L.handlers.get('pubsub:sub')({ topicId: idHex(S.Lt.topicId), via: [], subscriberId: idHex(S.W.id), since: 0, hw: S.L.am._highWater(S.lRole), lw: 0, standby: true, fd: { kind: 'service', host: 'host-z' } }, { targetId: S.L.id, isTerminal: true, hopCount: 1, fromId: idHex(S.W.id) });
  await S.L.am.refreshTick(); await settleMore(S.fab);
  check('G1b the winner itself renewing as a standby never counts: still deferred', S.lRole.isRoot && !S.wr()?.isRoot && !S.ev(S.L).includes('merge-step5-released'), JSON.stringify({ l: S.lRole.isRoot, w: !!S.wr()?.isRoot }));
  S.fab.clock += 1000; await S.sRenews('host-s');
  await S.L.am.refreshTick(); await settleMore(S.fab);
  check('G2 a second holder on another host renews: step 5 runs, W ends the one root', S.ev(S.L).includes('merge-step5-released') && S.ev(S.L).includes('merge-step5') && !S.lRole.isRoot && !!S.wr()?.isRoot,
    JSON.stringify({ l: S.lRole.isRoot, w: !!S.wr()?.isRoot, ev: S.ev(S.L).filter((x) => /step5|merge-ack/.test(x)) })); }
{ const S = await scene('cbr-6bg-samehost');
  await S.trigger();
  S.fab.clock += 1000; await S.sRenews('host-w');
  await S.L.am.refreshTick(); await settleMore(S.fab);
  const stillDeferred = S.lRole.isRoot && !S.wr()?.isRoot;
  S.fab.clock += MERGE_TIMEOUT_MS + 1;
  await S.L.am.refreshTick(); await settleMore(S.fab);
  check('G3 a holder on the winner\'s host never counts: past the lease the round aborts and L keeps the root',
    stillDeferred && S.lRole.isRoot && !S.wr()?.isRoot && (S.ev(S.L).includes('merge-ack-expired') || S.ev(S.L).includes('merge-abort')), JSON.stringify({ stillDeferred, l: S.lRole.isRoot, w: !!S.wr()?.isRoot, ev: S.ev(S.L).filter((x) => /step5|merge-ack|abort/.test(x)) })); }
{ const S = await scene('cbr-6bg-service', { wApp: false });
  await S.trigger();
  check('G4 control, W a service: no deferral; W ends the root', !S.ev(S.L).includes('merge-step5-deferred') && !!S.wr()?.isRoot && !S.lRole.isRoot, JSON.stringify({ ev: S.ev(S.L).filter((x) => /step5/.test(x)) })); }
{ const S = await scene('cbr-6bg-off', { lGate: false });
  await S.trigger();
  check('G5 control, gate off at L: no deferral even for an ephemeral W', !S.ev(S.L).includes('merge-step5-deferred') && !!S.wr()?.isRoot && !S.lRole.isRoot, JSON.stringify({ ev: S.ev(S.L).filter((x) => /step5/.test(x)) })); }

console.log(`\nfence_cbr_6b_gate: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
