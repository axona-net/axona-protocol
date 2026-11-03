#!/usr/bin/env node
// =====================================================================
// fence_cbr_handoff_binding.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 7, binding `handoff` to the loser's identity (David 2026-11-03 "go
// ahead with the handoff identity binding"); manager option handoffBinding /
// env HANDOFF_BINDING, default off.
//
// A cutover loser with a node identity signs its offer's handoff claim; an
// armed candidate stages only when the signature verifies and the key hashes
// into the loser's node id. Real identities (Ed25519) on the merge Fabric.
//   B1 both armed, real round: W stages and ends the one root.
//   B2 an OLD loser's offer with handoff: 1 added in flight: an armed W
//      refuses not-root and holds no role (merge-stage-unbound);
//   B2c control, binding off: the same forged offer makes W stage (the hazard).
//   B3 a valid signature by a key that is not L's: refused.
//   B4 L's own signature, corr changed after signing: refused.
//   B5 the correct signature on a hand-built offer: staged (control for B3/B4).
//   B6 an armed loser with no identity sends no signature; an armed W refuses.
// Run: node test/fence_cbr_handoff_binding.mjs
// =====================================================================
import { Fabric, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { handoffTranscript } from '../src/pubsub/handoffBinding.js';
import { bytesToHex } from '../src/pubsub/ackProof.js';

const EXPECT = 7;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });
const LOC = { lat: 40.7, lng: -74 };
const settleMore = async (fab) => { for (let i = 0; i < 8; i++) { await fab.settle(); await new Promise((r) => setImmediate(r)); } };

async function scene(name, { wOpts = {}, lOpts = {}, lIdentity = true } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await createNodeIdentity(LOC), b = await createNodeIdentity(LOC);
  const [wI, lI] = ((BigInt('0x' + a.id) ^ Lt.topicId) < (BigInt('0x' + b.id) ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(BigInt('0x' + wI.id), { identity: wI, ...wOpts });
  const L = fab.addNode(BigInt('0x' + lI.id), { ...(lIdentity ? { identity: lI } : {}), ...lOpts });
  W.am._announceRoot = () => {}; L.am._announceRoot = () => {};
  L.am.dht.lookup = async () => ({ path: [idHex(L.id), idHex(W.id)], hops: 1, time: 1, found: false });
  const lRole = L.rc.become(Lt.topicId, 'sub-terminal');
  const meta = (n) => ({ targetId: n.id, isTerminal: true, hopCount: 2, fromId: idHex(Lt.publisher) });
  for (let k = 1; k <= 2; k++) {
    fab.clock += 10;
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: fab.clock });
    await L.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, meta(L));
    await fab.settle();
  }
  const trigger = async () => {
    lRole.formedAt = fab.clock - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
    L.am._verifyRoots(fab.clock);
    for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
    await settleMore(fab);
  };
  const ev = (n) => n.logs.map((l) => l.ev.replace('pubsub:', ''));
  const acks = () => (fab.sent ?? []).filter((j) => j.type === 'pubsub:replayack').map((j) => j.payload?.disposition);
  // A hand-built offer from L to W, signed by `signer` over `signedCorr`.
  const offerTo = async (signer, { corr = 'c'.repeat(32), signedCorr = corr } = {}) => {
    const offer = { topicId: idHex(Lt.topicId), corr, loser: idHex(L.id), loserEpoch: lRole.epoch | 0, hw: L.am._highWater(lRole), lw: L.am._lowWater(lRole), count: lRole.cache.length, timing: L.am._mergeTiming(), handoff: 1 };
    const sig = await signer.sign(handoffTranscript({ topicId: offer.topicId, corr: signedCorr, loser: offer.loser, winner: idHex(W.id), loserEpoch: offer.loserEpoch }));
    await W.handlers.get('pubsub:mergeoffer')({ ...offer, handoffSig: bytesToHex(sig), handoffPub: bytesToHex(signer.pubkey) }, { targetId: W.id, isTerminal: true, hopCount: 1, fromId: idHex(L.id) });
    await settleMore(fab);
  };
  return { Lt, fab, W, L, lI, lRole, trigger, ev, acks, offerTo, wr: () => W.am.axonRoles.get(Lt.topicId) };
}
const armed = { cutover: true, handoffBinding: true };

console.log('fence_cbr_handoff_binding: a candidate stages only on a handoff the loser signed');
{ const S = await scene('cbr-hb-real', { wOpts: armed, lOpts: armed });
  await S.fab.settle();
  await S.trigger();
  const offer = (S.fab.sent ?? []).find((j) => j.type === 'pubsub:mergeoffer')?.payload;
  check('B1 both armed, real round: the offer is signed; W stages and ends the one root', !!S.wr()?.isRoot && !S.lRole.isRoot && typeof offer?.handoffSig === 'string' && S.ev(S.W).includes('merge-stage-start'),
    JSON.stringify({ w: !!S.wr()?.isRoot, l: S.lRole.isRoot, signed: typeof offer?.handoffSig, ev: S.ev(S.W).filter((x) => /stage|merge/.test(x)) })); }
for (const bind of [true, false]) {
  const S = await scene(bind ? 'cbr-hb-forged' : 'cbr-hb-forged-off', { wOpts: bind ? armed : { cutover: true }, lOpts: {} });   // L is OLD: no cutover, no handoff
  await S.fab.settle();
  S.fab.tap = (j) => (j.type === 'pubsub:mergeoffer' ? { ...j, payload: { ...j.payload, handoff: 1 } } : j);
  await S.trigger();
  if (bind) check('B2 an old loser\'s offer with handoff added in flight: armed W refuses not-root, holds no role (merge-stage-unbound)',
    !S.wr() && S.ev(S.W).includes('merge-stage-unbound') && !S.ev(S.W).includes('merge-stage-start') && S.acks().includes('not-root'), JSON.stringify({ wRole: !!S.wr(), ev: S.ev(S.W).filter((x) => /stage|merge/.test(x)), acks: S.acks() }));
  else check('B2c control, binding off: the same forged offer makes W stage from the old loser (the hazard)',
    S.ev(S.W).includes('merge-stage-start'), JSON.stringify({ ev: S.ev(S.W).filter((x) => /stage|merge/.test(x)) }));
}
{ const S = await scene('cbr-hb-wrongkey', { wOpts: armed, lOpts: armed });
  const X = await createNodeIdentity(LOC);
  await S.offerTo(X);
  check('B3 a valid signature by a key that is not L\'s: refused, no stage', !S.wr() && S.ev(S.W).includes('merge-stage-unbound'), JSON.stringify({ ev: S.ev(S.W).filter((x) => /stage|merge/.test(x)) })); }
{ const S = await scene('cbr-hb-corr', { wOpts: armed, lOpts: armed });
  await S.offerTo(S.lI, { corr: 'd'.repeat(32), signedCorr: 'e'.repeat(32) });
  check('B4 L\'s own signature, corr changed after signing: refused, no stage', !S.wr() && S.ev(S.W).includes('merge-stage-unbound'), JSON.stringify({ ev: S.ev(S.W).filter((x) => /stage|merge/.test(x)) })); }
{ const S = await scene('cbr-hb-good', { wOpts: armed, lOpts: armed });
  await S.offerTo(S.lI);
  check('B5 control: L\'s correct signature on a hand-built offer stages', S.ev(S.W).includes('merge-stage-start') && !S.ev(S.W).includes('merge-stage-unbound'), JSON.stringify({ ev: S.ev(S.W).filter((x) => /stage|merge/.test(x)) })); }
{ const S = await scene('cbr-hb-noid', { wOpts: armed, lOpts: armed, lIdentity: false });
  await S.fab.settle(); await S.trigger();
  const offer = (S.fab.sent ?? []).find((j) => j.type === 'pubsub:mergeoffer')?.payload;
  const row = S.W.logs.find((l) => /merge-stage-unbound/.test(l.ev));
  check('B6 an armed loser with no identity sends no signature; an armed W refuses (signed: false)',
    offer?.handoff === 1 && offer?.handoffSig === undefined && !S.wr() && row?.ctx?.signed === false, JSON.stringify({ handoff: offer?.handoff, sig: offer?.handoffSig, row: row?.ctx ?? null })); }

console.log(`\nfence_cbr_handoff_binding: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
