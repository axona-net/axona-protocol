#!/usr/bin/env node
// =====================================================================
// fence_cbr_6bg_findings.mjs — Orion 12a4f349 on the 6b gate (8f4dc8b).
//   6BG-1  a second holder must hold this root's CURRENT high-water: with a
//          cache younger than RENEW_MS an empty standby (hw 0) passed.
//     F1 an hw-0 standby on another host does not release step 5; one at
//        L's high-water does.
//   6BG-2  (a) the export is pinned while step 5 waits, and unpinned when the
//          round aborts; (b) a deferred step 5 whose round went stale (the
//          loser's epoch moved) does nothing.
//     F2 pinned during deferral; F3 unpinned after the abort.
//     F4 L's epoch moves while deferred: a holder renews, step 5 does NOT
//        run ('merge-step5-stale'); L stays root, W is not root.
// Run: node test/fence_cbr_6bg_findings.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS, MERGE_TIMEOUT_MS } from '../src/pubsub/constants.js';

const EXPECT = 4;
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


console.log('fence_cbr_6bg_findings: Orion 12a4f349 6BG-1, 6BG-2');
const renew = async (S, hw) => { await S.L.handlers.get('pubsub:sub')({ topicId: idHex(S.Lt.topicId), via: [], subscriberId: idHex(S.L.id ^ 0x77n), since: 0, hw, lw: 0, standby: true, fd: { kind: 'service', host: 'host-s' } }, { targetId: S.L.id, isTerminal: true, hopCount: 1, fromId: idHex(S.L.id ^ 0x77n) }); await settleMore(S.fab); };
{ const S = await scene('cbr-6bgf-1');
  await S.trigger();
  S.fab.clock += 1000; await renew(S, 0); await S.L.am.refreshTick(); await settleMore(S.fab);
  const heldEmpty = S.lRole.isRoot && !S.wr()?.isRoot;
  await renew(S, S.L.am._highWater(S.lRole)); await S.L.am.refreshTick(); await settleMore(S.fab);
  check('F1 6BG-1 an empty (hw 0) standby does not release step 5; one at L\'s high-water does', heldEmpty && !S.lRole.isRoot && !!S.wr()?.isRoot, JSON.stringify({ heldEmpty, l: S.lRole.isRoot, w: !!S.wr()?.isRoot })); }
{ const S = await scene('cbr-6bgf-2');
  await S.trigger();
  const m = [...S.lRole.sync.openMerges.values()][0];
  const pinned = !!m && m.deferPinned === true && m.manifest.bodies.every((id) => S.lRole.merge.pins.has(id));
  check('F2 6BG-2a the export is pinned while step 5 waits', pinned, JSON.stringify({ deferPinned: m?.deferPinned, bodies: m?.manifest?.bodies?.length, pins: S.lRole.merge.pins.size }));
  const ids = m ? [...m.manifest.bodies] : [];
  S.fab.clock += MERGE_TIMEOUT_MS + 1; await S.L.am.refreshTick(); await settleMore(S.fab);
  check('F3 6BG-2a the round aborts past its lease and the deferral\'s pins are released; L keeps the root', S.lRole.isRoot && ids.length > 0 && ids.every((id) => !S.lRole.merge.pins.has(id)), JSON.stringify({ l: S.lRole.isRoot, left: ids.filter((id) => S.lRole.merge.pins.has(id)).length })); }
{ const S = await scene('cbr-6bgf-4');
  await S.trigger();
  S.lRole.epoch = (S.lRole.epoch | 0) + 1;   // the loser re-incarnated while the old round waited
  S.fab.clock += 1000; await renew(S, S.L.am._highWater(S.lRole)); await S.L.am.refreshTick(); await settleMore(S.fab);
  check('F4 6BG-2b a stale deferral does nothing: no step 5, L stays root, W not root', S.lRole.isRoot && !S.wr()?.isRoot && S.ev(S.L).includes('merge-step5-stale') && !S.ev(S.L).includes('merge-step5'), JSON.stringify({ l: S.lRole.isRoot, w: !!S.wr()?.isRoot, ev: S.ev(S.L).filter((x) => /step5/.test(x)) })); }
console.log(`\nfence_cbr_6bg_findings: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
