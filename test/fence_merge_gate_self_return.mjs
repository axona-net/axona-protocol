#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_self_return.mjs — dual-root packet 4(d) "Re-entry"
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A GATED WRITE FORWARDED TO W THAT COMES BACK TO L IS NOT FORWARDED AGAIN
// AND NOT STAMPED. The forward carries a `gateFwd` nonce in a bounded ledger
// at L (the shape of holdFwd). A bare write carrying L's own nonce that
// returns is undeliverable `merge-gate-return`, whatever the state of the
// gate; the publisher's flight recovers it. A ledger at capacity refuses
// with `merge-gate-saturated`.
//
// Checks: (1) the forward carries a nonce held in the ledger; (2) the same
// write returning bare with that nonce: undeliverable merge-gate-return,
// not forwarded again, not stamped, nonce consumed; (3) a bare write with
// an unknown nonce while gated: merge-gate-return-unknown, not forwarded;
// (4) the ledger at MERGE_GATE_FWD_MAX refuses the next forward with
// merge-gate-saturated; (5) a return after the gate cleared is still
// undeliverable and still not stamped; (6) an unknown nonce with no gate is
// an ordinary bare write and is stamped.
//
// Mutant (red): delete the gateFwd branch in _mergeGateIntercept → 2 fails
// (forwarded twice).
// Run: node test/fence_merge_gate_self_return.mjs
// =====================================================================
import { MERGE_GATE_FWD_MAX } from '../src/pubsub/constants.js';
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, bareMeta, pubsTo, log, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const undeliv = (ctx, why) => log(ctx, 'undeliverable').filter((l) => l.ctx.why === why);

console.log('fence_merge_gate_self_return: 4(d) — a returned gated forward is never forwarded again, never stamped');

const L = await realTopic('gate-self-return');
{ // 1–3, 5
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 2);
  gateRoot(ctx, L.topicId, L.near, 3);
  const w = await signedPub(ctx, L.desc, 90);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  const fwd = pubsTo(ctx, idHex(L.near))[0];
  const nonce = fwd?.payload?.gateFwd;
  check('1 the forward carries a gateFwd nonce held in L\'s ledger', typeof nonce === 'string' && role.merge.gateFwd.has(nonce));
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json, gateFwd: nonce }, bareMeta(ctx, L.near));   // the forward came back bare
  check('2 the return: undeliverable merge-gate-return, not forwarded again, not stamped, nonce consumed', undeliv(ctx, 'merge-gate-return').length === 1 && pubsTo(ctx, idHex(L.near)).length === 1 && !role.cacheIds.has(w.msgId) && !role.merge.gateFwd.has(nonce));
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json, gateFwd: 'deadbeefdeadbeef' }, bareMeta(ctx, L.near));
  check('3 an unknown nonce while gated: merge-gate-return-unknown, not forwarded, not stamped', undeliv(ctx, 'merge-gate-return-unknown').length === 1 && pubsTo(ctx, idHex(L.near)).length === 1 && !role.cacheIds.has(w.msgId));
  // a second forward, then the gate clears, then it returns
  const w2 = await signedPub(ctx, L.desc, 91);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w2.json }, bareMeta(ctx, L.publisher));
  const n2 = pubsTo(ctx, idHex(L.near))[1]?.payload?.gateFwd;
  ctx.am._mergeGateClear(role, 'test');
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w2.json, gateFwd: n2 }, bareMeta(ctx, L.near));
  check('5 a return after the gate cleared is still undeliverable and not stamped', typeof n2 === 'string' && undeliv(ctx, 'merge-gate-return').length === 2 && !role.cacheIds.has(w2.msgId) && pubsTo(ctx, idHex(L.near)).length === 2);
  const w3 = await signedPub(ctx, L.desc, 92);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w3.json, gateFwd: 'ffffffffffffffff' }, bareMeta(ctx, L.publisher));
  check('6 an unknown nonce with no gate is an ordinary bare write: stamped', role.cacheIds.has(w3.msgId));
}
{ // 4 saturation
  const ctx = makeManagerAt(L.self);
  const { role } = await rootWithBodies(ctx, L, 1);
  gateRoot(ctx, L.topicId, L.near, 3);
  for (let i = 0; i < MERGE_GATE_FWD_MAX; i++) role.merge.gateFwd.set(`n${i}`, { at: ctx.clock.t, type: 'pubsub:pub', corr: 'c0000001' });
  const w = await signedPub(ctx, L.desc, 93);
  await ctx.am._onPub({ topicId: idHex(L.topicId), json: w.json }, bareMeta(ctx, L.publisher));
  check('4 a ledger at MERGE_GATE_FWD_MAX refuses with merge-gate-saturated: not forwarded, not stamped', undeliv(ctx, 'merge-gate-saturated').length === 1 && pubsTo(ctx, idHex(L.near)).length === 0 && !role.cacheIds.has(w.msgId));
}

console.log(`\nfence_merge_gate_self_return: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
