#!/usr/bin/env node
// =====================================================================
// fence_pin_proof.mjs — "Not attached until something has come down" v0.2,
// parts 1 and 2. Manager option pinProof (env PIN_PROOF), default off.
//
// On 2026-11-24 a browser tab's SUBSCRIBEs went up and were consumed for nine
// hours while every answer to it was lost on the way down. The kernel judged
// the pin by the upward send alone, so the tab stayed pinned and deaf.
//
//   A  the deaf subscriber: answers dropped on the way down. With the option
//      on the pin is dropped once two renewals have gone unanswered, and not
//      before; the next SUBSCRIBE goes out unpinned. Off: the pin stays.
//   B  a fed subscriber on a QUIET topic keeps its pin through ten renewals.
//   C  AP-1: a root beacon is not proof. Beacons arriving all the while do not
//      save the deaf subscriber's pin.
//   D  AP-2: the count is read before the send. After two renewals sent and
//      unanswered the pin still stands; the third tick drops it.
//   E  AP-3: the answer does not lean on attachEcho or topicLifetime (both off
//      here), and an empty answer to one seat is floored.
//   F  a root WITHOUT the option is never left for being quiet.
//   G  off is today's shape: no proof field on the wire, no answer to a
//      caught-up renewal.
// Run: node test/fence_pin_proof.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { PIN_PROOF_SILENT, PIN_PROOF_FLOOR_MS, RENEW_MS } from '../src/pubsub/constants.js';

const EXPECT = 19;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

// A root W holding one message and a subscriber S attached to it and caught up.
async function scene(name, { root = true, sub = true } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, sId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId, { pinProof: root }); const S = fab.addNode(sId, { pinProof: sub });
  W.am._announceRoot = () => {};
  S.am._rootClaim.selfClosestReachable = () => false;      // S is a reader here, never a claimant
  S.am._readRepair = async () => {};
  const wRole = W.rc.become(Lt.topicId, 'sub-terminal');
  fab.clock += 10;
  const env = await buildEnvelope({ topic: Lt.desc, message: { k: 1 }, seq: 1, identity: author, ts: fab.clock });
  await W.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) },
    { targetId: W.id, isTerminal: true, hopCount: 1, fromId: idHex(Lt.publisher) });
  await fab.settle();
  S.am.pubsubSubscribe(Lt.topicId, {});
  await S.am._sendSubscribe(Lt.topicId);
  await fab.settle();
  const w = idHex(wId).toLowerCase();
  const pin = () => ((S.am._upstream.get(Lt.topicId) || [])[0] || null);
  const sub0 = S.am.mySubscriptions.get(Lt.topicId);
  // one renewal: move the clock a whole settled gap, run the tick, drain the wire
  const renew = async () => { fab.clock += RENEW_MS + 1; await S.am.refreshTick(); await fab.settle(); };
  const deaf = () => { fab.tap = (j) => (j.type === 'pubsub:deliver' && j.dest === sId ? null : j); };
  const subsOut = () => S.routed.filter((r) => r.type === 'pubsub:sub');
  const delivers = () => W.routed.filter((r) => r.type === 'pubsub:deliver' && r.target === sId);
  const unproven = () => S.logs.filter((l) => l.ev === 'pubsub:upstream-unproven');
  return { Lt, fab, W, S, wRole, w, wId, sId, pin, sub0, renew, deaf, subsOut, delivers, unproven };
}

console.log('fence_pin_proof: a pin needs proof from below');

console.log(' A. the deaf subscriber');
{
  const X = await scene('pin-proof-A');
  check('A0 attached, and the root said it answers renewals', X.pin() === X.w && X.sub0.proofFrom === X.w, JSON.stringify({ pin: X.pin(), pf: X.sub0.proofFrom }));
  X.deaf();
  await X.renew(); await X.renew();
  check('A1 two renewals unanswered: the pin still stands', X.pin() === X.w && X.unproven().length === 0, JSON.stringify({ pin: X.pin(), n: X.sub0.unanswered }));
  const before = X.subsOut().length;
  await X.renew();
  const last = X.subsOut().pop()?.payload;
  check('A2 the third tick drops the pin and says why', X.unproven().length === 1 && X.unproven()[0].ctx.silent === PIN_PROOF_SILENT, JSON.stringify(X.unproven()));
  check('A3 and the SUBSCRIBE it then sends is the unpinned one', X.subsOut().length > before && X.pin() === null && X.sub0.interval === X.S.am.renewFastMs, JSON.stringify({ pin: X.pin(), iv: X.sub0.interval, last: last?.via }));
  const off = await scene('pin-proof-A-off', { root: false, sub: false });
  off.deaf();
  for (let i = 0; i < 6; i++) await off.renew();
  check('A4 off (control): six unanswered renewals and the pin stays, as on 2026-11-24', off.pin() === off.w && off.unproven().length === 0, String(off.pin()));
}

console.log(' B. a fed subscriber on a quiet topic');
{
  const X = await scene('pin-proof-B');
  const d0 = X.delivers().length;
  for (let i = 0; i < 10; i++) await X.renew();
  check('B1 ten renewals, nothing published: the pin is kept', X.pin() === X.w && X.unproven().length === 0, JSON.stringify({ pin: X.pin(), n: X.sub0.unanswered }));
  check('B2 each renewal was answered, with an empty frame', X.delivers().length - d0 === 10 && X.delivers().slice(d0).every((r) => Array.isArray(r.payload.msgs) && r.payload.msgs.length === 0 && r.payload.proof === 1), String(X.delivers().length - d0));
  check('B3 the count is back at zero after each answer', (X.sub0.unanswered | 0) === 0, String(X.sub0.unanswered));
}

console.log(' C. AP-1: a beacon is not proof');
{
  const X = await scene('pin-proof-C');
  X.deaf();
  for (let i = 0; i < 3; i++) {
    X.S.am._onRootBeacon({ root: idHex(X.wId), topics: [idHex(X.Lt.topicId)], epochs: [1], beaconId: `b-${i}-${X.fab.clock}`, layer: 0 }, { targetId: X.S.id, isTerminal: false, hopCount: 1 });
    await X.renew();
  }
  check('C1 beacons from the root arrive all the while; the pin is dropped all the same', X.pin() === null && X.unproven().length === 1, JSON.stringify({ pin: X.pin(), n: X.unproven().length }));
}

console.log(' D. AP-2: the count is read before the send');
{
  const X = await scene('pin-proof-D');
  X.deaf();
  await X.renew();
  check('D1 one sent, unanswered: count 1, pinned', X.sub0.unanswered === 1 && X.pin() === X.w, String(X.sub0.unanswered));
  await X.renew();
  check('D2 two sent, unanswered: count 2, STILL pinned', X.sub0.unanswered === PIN_PROOF_SILENT && X.pin() === X.w, String(X.sub0.unanswered));
  X.fab.tap = null;                                           // the way down comes back
  await X.renew();
  check('D3 unpinned at the third tick, and the answer to that SUBSCRIBE re-pins it', X.unproven().length === 1 && X.pin() === X.w && (X.sub0.unanswered | 0) === 0, JSON.stringify({ pin: X.pin(), n: X.sub0.unanswered }));
}

console.log(' E. AP-3: self-contained, and floored');
{
  const X = await scene('pin-proof-E');
  check('E1 attachEcho and topicLifetime are both off in this scene', X.W.am._attachEcho === false && X.W.am._topicLifetime === false);
  const newest = X.wRole.cache[X.wRole.cache.length - 1].publishTs;
  const meta = { targetId: X.W.id, isTerminal: true, hopCount: 1, fromId: idHex(X.sId) };
  const sub = { topicId: idHex(X.Lt.topicId), subscriberId: idHex(X.sId), via: [idHex(X.wId)], since: newest };
  X.fab.clock += PIN_PROOF_FLOOR_MS + 1;
  const d0 = X.delivers().length;
  for (let i = 0; i < 50; i++) { await X.W.handlers.get('pubsub:sub')(sub, meta); }
  await X.fab.settle();
  check('E2 fifty renewals in one instant draw ONE empty answer', X.delivers().length - d0 === 1, String(X.delivers().length - d0));
  check('E3 and the root counted the rest as floored', X.W.am._pinProofStats.floored === 49, JSON.stringify(X.W.am._pinProofStats));
  X.fab.clock += PIN_PROOF_FLOOR_MS + 1;
  await X.W.handlers.get('pubsub:sub')(sub, meta); await X.fab.settle();
  check('E4 past the floor the next renewal is answered again', X.delivers().length - d0 === 2, String(X.delivers().length - d0));
}

console.log(' F. a root without the option is never left for being quiet');
{
  const X = await scene('pin-proof-F', { root: false, sub: true });
  check('F0 the root made no promise', X.pin() === X.w && !X.sub0.proofFrom, String(X.sub0.proofFrom));
  for (let i = 0; i < 6; i++) await X.renew();
  check('F1 six quiet renewals, no answer owed: the pin is kept', X.pin() === X.w && X.unproven().length === 0 && (X.sub0.unanswered | 0) === 0, JSON.stringify({ pin: X.pin(), n: X.sub0.unanswered }));
}

console.log(' G. off is today\'s shape');
{
  const X = await scene('pin-proof-G', { root: false, sub: false });
  const d0 = X.delivers().length;
  await X.renew();
  check('G1 no proof field on any DELIVER, and a caught-up renewal draws none', X.delivers().every((r) => !('proof' in r.payload)) && X.delivers().length === d0, JSON.stringify({ n: X.delivers().length, d0 }));
}

console.log(`\nfence_pin_proof: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
