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
// Aster 8aff1137:
//   H  the proof is a reply to THIS reader's own renewal: fan-out from the
//      parent does not zero the count, nor does a token the reader never sent.
//   I  a thin reader that left its pin on silence does NOT claim the root; an
//      ordinary unattached reader still does; a reader with peers still does.
//   J  the budget: after PIN_PROOF_BUDGET drops inside the period the pin is
//      kept and the reader reads degraded; a reply to its renewal clears it.
//   K  one budget for every empty answer: with the option on, attach-echo's
//      empties are floored too; with it off they are as before.
//   L  mixed versions: an old reader at a new root keeps its pin and ignores
//      the new fields.
// Run: node test/fence_pin_proof.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { PIN_PROOF_SILENT, PIN_PROOF_FLOOR_MS, PIN_PROOF_BUDGET, PIN_PROOF_BUDGET_MS, ROOT_CLAIM_MS, RENEW_MS } from '../src/pubsub/constants.js';

const EXPECT = 38;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

// A root W holding one message and a subscriber S attached to it and caught up.
async function scene(name, { root = true, sub = true, rootOpts = {} } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, sId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId, { pinProof: root, ...rootOpts }); const S = fab.addNode(sId, { pinProof: sub });
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

console.log(' H. the proof is a reply to this reader\'s own renewal');
{
  const X = await scene('pin-proof-H');
  // deaf to REPLIES only: every DELIVER that echoes a token is dropped; fan-out passes.
  X.fab.tap = (j) => (j.type === 'pubsub:deliver' && j.dest === X.sId && typeof j.payload?.pp === 'string' ? null : j);
  const pub = async (k) => {
    X.fab.clock += 10;
    const env = await buildEnvelope({ topic: X.Lt.desc, message: { k }, seq: k, identity: author, ts: X.fab.clock });
    await X.W.handlers.get('pubsub:pub')({ topicId: idHex(X.Lt.topicId), json: JSON.stringify(env) }, { targetId: X.W.id, isTerminal: true, hopCount: 1, fromId: idHex(X.Lt.publisher) });
    await X.fab.settle();
  };
  await X.renew(); await pub(2); await X.renew(); await pub(3);
  check('H1 fan-out from the parent arrives and does NOT zero the count', X.sub0.unanswered === PIN_PROOF_SILENT && X.pin() === X.w, String(X.sub0.unanswered));
  await X.renew();
  check('H2 so the pin is still dropped on silence to its renewals', X.unproven().length === 1, String(X.unproven().length));
  const Y = await scene('pin-proof-H3');
  Y.deaf(); await Y.renew(); await Y.renew();
  await Y.S.handlers.get('pubsub:deliver')({ topicId: idHex(Y.Lt.topicId), from: idHex(Y.wId), msgs: [], proof: 1, pp: '00'.repeat(8) }, { targetId: Y.S.id, isTerminal: true, hopCount: 1, fromId: idHex(Y.wId) });
  check('H3 a frame echoing a token the reader never sent does not zero it', Y.sub0.unanswered === PIN_PROOF_SILENT, String(Y.sub0.unanswered));
  const tok = Y.sub0.ppSent[Y.sub0.ppSent.length - 1];
  await Y.S.handlers.get('pubsub:deliver')({ topicId: idHex(Y.Lt.topicId), from: idHex(Y.wId), msgs: [], proof: 1, pp: tok }, { targetId: Y.S.id, isTerminal: true, hopCount: 1, fromId: idHex(Y.wId) });
  check('H4 a frame echoing its own latest token does', (Y.sub0.unanswered | 0) === 0, String(Y.sub0.unanswered));
}

console.log(' I. a thin reader that left on silence does not claim the root');
{
  const run = async (name, { thin, viaProof }) => {
    const X = await scene(name);
    let claims = 0;
    X.S.am._rootClaim.selfClosestReachable = () => true;
    X.S.am._rootClaim.claimReachable = () => { claims++; return null; };
    X.S.am._proofThin = () => thin;
    X.deaf();
    if (viaProof) { await X.renew(); await X.renew(); await X.renew(); }
    else { X.S.am._upstream.delete(X.Lt.topicId); }
    for (let i = 0; i < 3; i++) { X.fab.clock += ROOT_CLAIM_MS + 1; await X.S.am.refreshTick(); await X.fab.settle(); }
    return { X, claims };
  };
  const a = await run('pin-proof-I1', { thin: true, viaProof: true });
  check('I1 thin, left on silence: unattached past ROOT_CLAIM_MS, sees itself closest, and does NOT claim', a.claims === 0 && a.X.pin() === null && a.X.sub0.unprovenPin === a.X.w, JSON.stringify({ claims: a.claims, ctx: a.X.sub0.unprovenPin?.slice(0, 8) }));
  check('I2 and it says so once', a.X.S.logs.filter((l) => l.ev === 'pubsub:self-claim-held').length === 1 && a.X.S.am.pubsubPinProofState(a.X.Lt.topicId).recovering === true);
  const b = await run('pin-proof-I3', { thin: true, viaProof: false });
  check('I3 control: a thin reader unattached for any OTHER reason still claims, as today', b.claims >= 1, String(b.claims));
  const c = await run('pin-proof-I4', { thin: false, viaProof: true });
  check('I4 a reader with peers that left on silence still claims', c.claims >= 1, String(c.claims));
  const d = await run('pin-proof-I5', { thin: true, viaProof: true });
  d.X.fab.tap = null; await d.X.renew();
  check('I5 a reply to its renewal ends the hold: re-pinned, context cleared', d.X.pin() === d.X.w && d.X.sub0.unprovenPin === null && d.X.S.am.pubsubPinProofState(d.X.Lt.topicId).recovering === false, JSON.stringify({ pin: d.X.pin()?.slice(0, 8), ctx: d.X.sub0.unprovenPin }));
}

console.log(' J. the budget');
{
  const X = await scene('pin-proof-J');
  // The reader's OWN cadence: after a drop it renews at the fast floor and backs off, so
  // three drops fit well inside the period. (Stepping a whole minute each time would
  // let the first drop age out before the fourth silence.)
  const stepOf = (Z) => async () => { Z.fab.clock += (Z.sub0.interval || Z.S.am.renewFastMs) + 1; await Z.S.am.refreshTick(); await Z.fab.settle(); };
  const step = stepOf(X);
  // The root answers the FIRST frame after each drop (so the reader re-pins) and is then silent again.
  let letOne = false;
  X.fab.tap = (j) => { if (j.type !== 'pubsub:deliver' || j.dest !== X.sId) return j; if (letOne) { letOne = false; return j; } return null; };
  const cycle = async () => { await step(); await step(); letOne = true; await step(); };
  for (let i = 0; i < PIN_PROOF_BUDGET; i++) await cycle();
  check(`J1 ${PIN_PROOF_BUDGET} drops inside the period, each re-pinned by the next answer`, X.unproven().length === PIN_PROOF_BUDGET && X.pin() === X.w, JSON.stringify({ n: X.unproven().length, pin: X.pin()?.slice(0, 8) }));
  await step(); await step(); await step();
  const st = X.S.am.pubsubPinProofState(X.Lt.topicId);
  check('J2 the next silence does NOT drop the pin: the reader is degraded and says so once', X.unproven().length === PIN_PROOF_BUDGET && X.pin() === X.w && st.degraded === true && X.S.logs.filter((l) => l.ev === 'pubsub:upstream-unproven-degraded').length === 1, JSON.stringify(st));
  await step(); await step(); await step(); await step();
  check('J3 it stays degraded, with no further drop, while the silence lasts', X.unproven().length === PIN_PROOF_BUDGET && X.S.am.pubsubPinProofState(X.Lt.topicId).degraded === true);
  X.fab.tap = null; await step();
  check('J4 a reply to its renewal clears the degraded mark', X.S.am.pubsubPinProofState(X.Lt.topicId).degraded === false && (X.sub0.unanswered | 0) === 0);
  const Y = await scene('pin-proof-J5');
  let one = false;
  Y.fab.tap = (j) => { if (j.type !== 'pubsub:deliver' || j.dest !== Y.sId) return j; if (one) { one = false; return j; } return null; };
  const stepY = stepOf(Y);
  for (let i = 0; i < PIN_PROOF_BUDGET; i++) { await stepY(); await stepY(); one = true; await stepY(); }
  await stepY(); await stepY(); await stepY();
  const heldAt = Y.unproven().length;
  Y.fab.clock += PIN_PROOF_BUDGET_MS + 1;
  await stepY(); await stepY(); await stepY();
  check('J5 held at the budget, and whole again once the period has passed', heldAt === PIN_PROOF_BUDGET && Y.unproven().length === PIN_PROOF_BUDGET + 1, JSON.stringify({ heldAt, after: Y.unproven().length }));
}

console.log(' K. one budget for every empty answer');
{
  const fire = async (X, n, extra) => {
    const newest = X.wRole.cache[X.wRole.cache.length - 1].publishTs;
    const meta = { targetId: X.W.id, isTerminal: true, hopCount: 1, fromId: idHex(X.sId) };
    X.fab.clock += PIN_PROOF_FLOOR_MS + 1;
    const d0 = X.delivers().length;
    for (let i = 0; i < n; i++) await X.W.handlers.get('pubsub:sub')({ topicId: idHex(X.Lt.topicId), subscriberId: idHex(X.sId), via: [idHex(X.wId)], since: newest, ...extra(i) }, meta);
    await X.fab.settle();
    return X.delivers().length - d0;
  };
  const on = await scene('pin-proof-K1', { rootOpts: { attachEcho: true } });
  check('K1 option on: fifty attach-bearing renewals in one instant draw ONE empty answer', (await fire(on, 50, (i) => ({ attach: i.toString(16).padStart(16, '0') }))) === 1);
  const off = await scene('pin-proof-K2', { root: false, sub: false, rootOpts: { attachEcho: true } });
  check('K2 option off (control): the same fifty draw fifty, as today', (await fire(off, 50, (i) => ({ attach: i.toString(16).padStart(16, '0') }))) === 50);
}

console.log(' L. mixed versions');
{
  const X = await scene('pin-proof-L', { root: true, sub: false });
  const d0 = X.delivers().length;
  for (let i = 0; i < 6; i++) await X.renew();
  check('L1 an old reader at a new root: answered each renewal, pin kept, nothing counted', X.pin() === X.w && X.delivers().length - d0 === 6 && X.sub0.unanswered === undefined && X.S.am.pubsubPinProofState(X.Lt.topicId) === null, JSON.stringify({ d: X.delivers().length - d0, n: X.sub0.unanswered }));
  check('L2 its renewals carry no token, and the answers echo none', X.subsOut().every((r) => !('pp' in r.payload)) && X.delivers().slice(d0).every((r) => !('pp' in r.payload) && r.payload.proof === 1));
  const Y = await scene('pin-proof-L3', { root: false, sub: true });
  check('L3 a new reader at an old root sends its token and is never counted (F, restated)', Y.subsOut().some((r) => typeof r.payload.pp === 'string') && !Y.sub0.proofFrom);
}

console.log(`\nfence_pin_proof: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
