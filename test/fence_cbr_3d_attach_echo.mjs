#!/usr/bin/env node
// =====================================================================
// fence_cbr_3d_attach_echo.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 3d (David 2026-11-03 "go ahead with 3d"), manager option attachEcho
// (env ATTACH_ECHO), default off.
//
// A caught-up renewal used to produce no DELIVER (_accept -> _replayTo with
// ping false), so a subscriber that lost its pin, or whose pin named a node
// that has since handed off, never heard the echo that pins it; and the root
// cannot tell an unpinned SUB from a pinned one, since an unpinned SUB with a
// warm hint names that hint in via exactly as a pinned SUB names its pin.
//   D1 on: a caught-up renewal carrying an attach nonce gets a DELIVER
//      (empty) back.
//   D2 on: a caught-up renewal marked unpinned gets a DELIVER back.
//   D3 off (control): the same attach renewal gets nothing, today's shape.
//   D4 on: a pinned renewal with no nonce stays silent (cost bounded).
//   D5 sender: armed and unpinned, the SUB says unpinned:true; pinned, it
//      does not.
//   D6 end to end: a subscriber that lost its pin re-pins to the root on its
//      next renewal (on); with the option off it stays unpinned.
// Run: node test/fence_cbr_3d_attach_echo.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < 7) { console.log(`  ✗ exited after ${passed + failed} of 7 checks`); process.exitCode = 1; } });

async function scene(name, attachEcho) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, sId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId, { attachEcho }); const S = fab.addNode(sId, { attachEcho });
  W.am._announceRoot = () => {};
  const wRole = W.rc.become(Lt.topicId, 'sub-terminal');
  const meta = (n) => ({ targetId: n.id, isTerminal: true, hopCount: 1, fromId: idHex(sId) });
  fab.clock += 10;
  const env = await buildEnvelope({ topic: Lt.desc, message: { k: 1 }, seq: 1, identity: author, ts: fab.clock });
  await W.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, { ...meta(W), fromId: idHex(Lt.publisher) });
  await fab.settle();
  const newest = wRole.cache[wRole.cache.length - 1].publishTs;
  const sub = (extra = {}) => ({ topicId: idHex(Lt.topicId), subscriberId: idHex(sId), via: [idHex(wId)], since: newest, ...extra });
  await W.handlers.get('pubsub:sub')(sub(), meta(W));          // seat S
  await fab.settle();
  const deliversToS = () => W.routed.filter((r) => r.type === 'pubsub:deliver' && r.target === sId).length;
  const renew = async (extra) => { const before = deliversToS(); await W.handlers.get('pubsub:sub')(sub(extra), meta(W)); await fab.settle(); return deliversToS() - before; };
  return { Lt, fab, W, S, wRole, renew, wId, sId };
}

console.log('fence_cbr_3d_attach_echo: an attach-bearing or unpinned renewal is always answered');
{
  const on = await scene('cbr-3d-on', true);
  check('D1 on: a caught-up renewal carrying an attach nonce gets a DELIVER back', (await on.renew({ attach: 'a1b2c3d4e5f60718' })) === 1);
  check('D2 on: a caught-up renewal marked unpinned gets a DELIVER back', (await on.renew({ unpinned: true })) === 1);
  check('D4 on: a pinned renewal with no nonce stays silent', (await on.renew({})) === 0);
  const off = await scene('cbr-3d-off', false);
  check('D3 off (control): the same attach renewal gets nothing', (await off.renew({ attach: 'a1b2c3d4e5f60718' })) === 0);
}
{
  const S = await scene('cbr-3d-sender', true);
  const subsOut = () => S.S.routed.filter((r) => r.type === 'pubsub:sub');
  S.S.am._upstream.delete(S.Lt.topicId);
  S.S.am._emitSubscribe(S.Lt.topicId, [idHex(S.wId)]);
  const unpinnedSub = subsOut().pop()?.payload;
  S.S.am._upstream.set(S.Lt.topicId, [idHex(S.wId).toLowerCase()]);
  S.S.am._emitSubscribe(S.Lt.topicId, [idHex(S.wId)]);
  const pinnedSub = subsOut().pop()?.payload;
  check('D5 sender: unpinned, the SUB says unpinned:true; pinned, it does not', unpinnedSub?.unpinned === true && pinnedSub && !('unpinned' in pinnedSub), JSON.stringify({ u: unpinnedSub?.unpinned, p: pinnedSub?.unpinned }));
}
for (const attachEcho of [true, false]) {
  const X = await scene(`cbr-3d-e2e-${attachEcho}`, attachEcho);
  X.S.am.pubsubSubscribe(X.Lt.topicId, {});
  await X.S.am._sendSubscribe(X.Lt.topicId);
  await X.fab.settle();
  const firstPin = (X.S.am._upstream.get(X.Lt.topicId) || [])[0] || null;
  X.S.am._upstream.delete(X.Lt.topicId);                     // the pin is lost
  X.S.am._lastSeenTsByTopic?.set?.(X.Lt.topicId, X.wRole.cache[X.wRole.cache.length - 1].publishTs);
  await X.S.am._sendSubscribe(X.Lt.topicId);                  // a caught-up renewal
  await X.fab.settle();
  const after = (X.S.am._upstream.get(X.Lt.topicId) || [])[0] || null;
  const w = idHex(X.wId).toLowerCase();
  if (attachEcho) check('D6 on: a subscriber that lost its pin re-pins to the root on its next renewal', firstPin?.toLowerCase() === w && after?.toLowerCase() === w, JSON.stringify({ firstPin: firstPin?.slice(0, 8), after: after?.slice(0, 8) }));
  else check('D6 off (control): the subscriber that lost its pin stays unpinned after a caught-up renewal', firstPin?.toLowerCase() === w && after === null, JSON.stringify({ firstPin: firstPin?.slice(0, 8), after: after?.slice(0, 8) }));
}
console.log(`\nfence_cbr_3d_attach_echo: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
