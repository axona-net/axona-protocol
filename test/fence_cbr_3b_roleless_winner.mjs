#!/usr/bin/env node
// =====================================================================
// fence_cbr_3b_roleless_winner.mjs — Closer-Born Relay Inherits the Root
// v0.6 Part 3b (David 2026-11-03 "go ahead with 3b"), under `cutover`.
//
// A node closer to the topic than the root, holding no root role, is offered
// the win. Today it answers not-root (mergeMachine _onMergeOffer), the 143
// refusal; an epoch-0 claim is no claim and XOR decides, so the offer is made
// (RootClaim.claimWins). Under cutover it
// STAGES: a role that serves nothing pulls the root's whole cache, and takes
// ROOT only when the loser's merge-bearing SUB completes the round, at one
// above the larger of its own known epoch and the loser's (CB-2).
//   B1 role-less winner: W ends the one root with L's three bodies, epoch
//      above L's, L demoted; W's promotion follows merge-complete.
//   B2 while staged (terminal ack held): W is not root, and a PUB reaching W
//      is forwarded to L by name and stamped there, never at W.
//   B3 off (control): W answers not-root, holds no role, L stays root.
//   B4 a CHILD winner (non-root role) stages the same way and keeps its role.
//   B5 every pull answer lost: after the pull timeout the role created only
//      to stage is discarded, and L is still root.
//   B6 a second contender while staging is refused 'busy'.
//   B7 CB-1: the verify never nominates the bridge.
//   B8 a write L stamps while W is staged reaches W after completion (the
//      SUB high-water pull), and W ends root holding it.
// Orion 06b0739f (3B-1..4):
//   B9  a CHILD stager handed a bare PUB forwards it to L (no self-reroute spin).
//   B10 a KILL reaching a stager is applied at L, never at the stager.
//   B11 the loser dies mid-stage: a PUB at the stager ends once, no bounce.
//   B12 a merge SUB that does not complete never seats L on a created stager.
//   B13 3B-4a: nor does a SUB naming L with a malformed (non-object) merge.
// Run: node test/fence_cbr_3b_roleless_winner.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { ROOT_VERIFY_FIRST_MS, MERGE_TIMEOUT_MS } from '../src/pubsub/constants.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { buildKill } from '../src/pubsub/kill.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < 14) { console.log(`  ✗ exited after ${passed + failed} of 14 checks`); process.exitCode = 1; } });

async function scene(name, { cutover = true, wRole = 'none' } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, lId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const opts = cutover ? { cutover: true } : {};
  const W = fab.addNode(wId, opts); const L = fab.addNode(lId, opts);
  W.am._announceRoot = () => {}; L.am._announceRoot = () => {};
  L.am.dht.lookup = async () => ({ path: [idHex(lId), idHex(wId)], hops: 1, time: 1, found: false });
  const lRole = L.rc.become(Lt.topicId, 'sub-terminal');
  if (wRole === 'child') { const r = makeRole(Lt.topicId, false, fab.clock); W.am.axonRoles.set(Lt.topicId, r); W.am._upstream.set(Lt.topicId, [idHex(lId)]); }
  const meta = (n) => ({ targetId: n.id, isTerminal: true, hopCount: 2, fromId: idHex(Lt.publisher) });
  const stamp = async (n, k) => {
    fab.clock += 10;
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: fab.clock });
    await n.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, meta(n));
    await fab.settle();
    return env.msgId;
  };
  const trigger = async () => {
    lRole.formedAt = fab.clock - ROOT_VERIFY_FIRST_MS - 1; lRole.lastVerify = 0;
    L.am._verifyRoots(fab.clock);
    for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
    await fab.settle();
  };
  const ev = (n) => n.logs.map((l) => l.ev.replace('pubsub:', ''));
  const wr = () => W.am.axonRoles.get(Lt.topicId);
  return { Lt, fab, W, L, lRole, stamp, trigger, ev, wr, meta };
}

console.log('fence_cbr_3b_roleless_winner: a closer node with no root role stages, then takes the root');
{
  const S = await scene('cbr-3b-roleless');
  const ids = [await S.stamp(S.L, 1), await S.stamp(S.L, 2), await S.stamp(S.L, 3)];
  const lEpoch = S.lRole.epoch | 0;
  await S.trigger();
  const w = S.wr(), e = S.ev(S.W);
  check('B1 role-less winner: W is the one root with L\'s three bodies, L demoted',
    !!w?.isRoot && ids.every((id) => w.cacheIds.has(id)) && !S.lRole.isRoot, JSON.stringify({ w: !!w?.isRoot, n: w?.cache?.length, l: S.lRole.isRoot, ev: e.filter((x) => /merge|stage|root/.test(x)) }));
  check('B1 … W staged first, promoted only after merge-complete, at an epoch above L\'s (CB-2)',
    e.includes('merge-stage-start') && e.indexOf('merge-stage-promoted') > e.indexOf('merge-complete') && e.indexOf('merge-complete') >= 0 && (w?.epoch | 0) > lEpoch, JSON.stringify({ wEpoch: w?.epoch, lEpoch, ev: e.filter((x) => /merge|stage/.test(x)) }));
}
{
  const S = await scene('cbr-3b-staged-serves-nothing');
  await S.stamp(S.L, 1); await S.stamp(S.L, 2);
  S.fab.tap = (j) => (j.type === 'pubsub:replayack' ? null : j);   // hold the round open: W stays staged
  await S.trigger();
  const staged = S.wr();
  const before = S.lRole.cache.length;
  const env = await buildEnvelope({ topic: S.Lt.desc, message: { k: 9 }, seq: 9, identity: author, ts: S.fab.clock + 50 });
  await S.W.handlers.get('pubsub:pub')({ topicId: idHex(S.Lt.topicId), json: JSON.stringify(env) }, S.meta(S.W));
  await S.fab.settle();
  check('B2 while staged W is not root, and a PUB reaching W is stamped at L, never at W',
    staged && !staged.isRoot && !!staged.staging && S.lRole.cacheIds.has(env.msgId) && S.lRole.cache.length === before + 1 && !S.ev(S.W).includes('root-formed'),
    JSON.stringify({ staged: !!staged, root: staged?.isRoot, staging: !!staged?.staging, lHas: S.lRole.cacheIds.has(env.msgId) }));
}
{
  const S = await scene('cbr-3b-off', { cutover: false });
  await S.stamp(S.L, 1);
  await S.trigger();
  check('B3 off (control): W answers not-root, holds no role, L stays root',
    !S.W.am.axonRoles.get(S.Lt.topicId) && S.lRole.isRoot && !S.ev(S.W).includes('merge-stage-start') && S.ev(S.W).includes('merge-offer-not-root'),
    JSON.stringify({ wRole: !!S.W.am.axonRoles.get(S.Lt.topicId), l: S.lRole.isRoot, lev: S.ev(S.L).filter((x) => /merge/.test(x)) }));
}
{
  const S = await scene('cbr-3b-child', { wRole: 'child' });
  const ids = [await S.stamp(S.L, 1), await S.stamp(S.L, 2)];
  await S.trigger();
  const w = S.wr();
  check('B4 a CHILD winner stages the same way and becomes root with L\'s bodies',
    !!w?.isRoot && ids.every((id) => w.cacheIds.has(id)) && !S.lRole.isRoot && S.W.logs.some((l) => l.ev === 'pubsub:merge-stage-start' && l.ctx.created === false),
    JSON.stringify({ w: !!w?.isRoot, ev: S.ev(S.W).filter((x) => /merge|stage/.test(x)) }));
}
{
  const S = await scene('cbr-3b-timeout');
  await S.stamp(S.L, 1);
  S.fab.tap = (j) => (j.type === 'pubsub:replayup' ? null : j);   // every pull answer lost
  await S.trigger();
  const during = !!S.wr();
  S.fab.clock += MERGE_TIMEOUT_MS + 1;
  S.W.am._mergeSweep(S.fab.clock); S.L.am._mergeSweep(S.fab.clock);
  await S.fab.settle();
  check('B5 pull timeout: the role created only to stage is discarded and L is still root',
    during && !S.wr() && S.lRole.isRoot && S.W.logs.some((l) => l.ev === 'pubsub:merge-stage-abandoned' && l.ctx.discarded === true),
    JSON.stringify({ during, after: !!S.wr(), l: S.lRole.isRoot, ev: S.ev(S.W).filter((x) => /stage|pull/.test(x)) }));
}
{
  const S = await scene('cbr-3b-busy');
  await S.stamp(S.L, 1);
  let offer = null;
  S.fab.tap = (j) => { if (j.type === 'pubsub:mergeoffer') offer = j.payload; return j.type === 'pubsub:replayup' ? null : j; };
  await S.trigger();
  const L2 = S.Lt.topicId ^ (0x7fn << 256n);   // top byte flipped: certainly farther from the topic than W
  const sent = [];
  const route0 = S.W.am.dht.routeMessage;
  S.W.am._route = (t, type, payload) => { sent.push({ type, payload }); return Promise.resolve({ consumed: true }); };
  await S.W.handlers.get('pubsub:mergeoffer')({ ...offer, corr: 'b2b2b2b2b2b2b2b2', loser: idHex(L2) }, { targetId: S.W.id, isTerminal: true, hopCount: 1, fromId: idHex(L2) });
  void route0;
  const ack = sent.find((x) => x.type === 'pubsub:replayack');
  check('B6 a second contender while W is staging is refused busy',
    !!offer && ack?.payload?.disposition === 'busy' && S.W.logs.some((l) => l.ev === 'pubsub:merge-stage-busy'),
    JSON.stringify({ offer: !!offer, ack: ack?.payload?.disposition }));
}
{
  const S = await scene('cbr-3b-bridge');
  await S.stamp(S.L, 1);
  S.L.am.dht.bridgeId = () => S.W.id;
  await S.trigger();
  check('B7 CB-1: the verify never nominates the bridge (no candidate, no offer, L stays root)',
    !S.L.logs.some((l) => /merge-candidate|merge-offer/.test(l.ev)) && S.lRole.isRoot && !S.W.am.axonRoles.get(S.Lt.topicId),
    JSON.stringify({ lev: S.ev(S.L).filter((x) => /merge/.test(x)) }));
}
{
  const S = await scene('cbr-3b-late-write');
  const ids = [await S.stamp(S.L, 1), await S.stamp(S.L, 2)];
  let held = null;
  S.fab.tap = (j) => { if (j.type === 'pubsub:replayack' && !held) { held = j; return null; } return j; };
  await S.trigger();
  const late = await S.stamp(S.L, 3);                 // stamped at L during the staged round
  const atL = S.lRole.cacheIds.has(late);
  S.fab.tap = null;
  S.fab.queue.push(held);                             // release W's terminal ack
  await S.fab.settle();
  await S.fab.settle();
  const w = S.wr();
  check('B8 a write stamped at L during the staged round reaches W after completion; W is root with all three',
    !!held && atL && !!w?.isRoot && [...ids, late].every((id) => w.cacheIds.has(id)) && !S.lRole.isRoot,
    JSON.stringify({ held: !!held, atL, w: !!w?.isRoot, has: [...ids, late].map((id) => !!w?.cacheIds?.has(id)), l: S.lRole.isRoot }));
}
const held = async (name, wRole) => {
  const S = await scene(name, { wRole });
  const ids = [await S.stamp(S.L, 1), await S.stamp(S.L, 2)];
  S.fab.tap = (j) => (j.type === 'pubsub:replayack' ? null : j);
  await S.trigger();
  return { S, ids };
};
const pubAt = async (S, n, k) => {
  const env = await buildEnvelope({ topic: S.Lt.desc, message: { k }, seq: k, identity: author, ts: S.fab.clock + 50 + k });
  let threw = null;
  try { await n.handlers.get('pubsub:pub')({ topicId: idHex(S.Lt.topicId), json: JSON.stringify(env) }, S.meta(n)); await S.fab.settle(); } catch (e) { threw = String(e?.message || e); }
  return { env, threw };
};
{
  const { S } = await held('cbr-3b-child-pub', 'child');
  const staged = S.wr();
  const { env, threw } = await pubAt(S, S.W, 21);
  check('B9 3B-1: a CHILD stager handed a bare PUB forwards it to L, which stamps it; no spin',
    staged && !staged.isRoot && !!staged.staging && !threw && S.lRole.cacheIds.has(env.msgId) && !staged.cacheIds.has(env.msgId),
    JSON.stringify({ staging: !!staged?.staging, threw, atL: S.lRole.cacheIds.has(env.msgId) }));
}
{
  const { S, ids } = await held('cbr-3b-kill', 'none');
  const k = await buildKill({ topicId: idHex(S.Lt.topicId), msgId: ids[0], ts: S.fab.clock + 60, seq: 1, identity: author });
  let threw = null;
  try { await S.W.handlers.get('pubsub:kill')({ topicId: idHex(S.Lt.topicId), kill: k }, S.meta(S.W)); await S.fab.settle(); } catch (e) { threw = String(e?.message || e); }
  const w = S.wr();
  check('B10 3B-2: a KILL reaching a stager is applied at L, never at the stager',
    !threw && !!S.lRole.tombstones?.get(ids[0]) && !(w?.tombstones?.get(ids[0])),
    JSON.stringify({ threw, atL: !!S.lRole.tombstones?.get(ids[0]), atW: !!w?.tombstones?.get(ids[0]) }));
}
{
  const { S } = await held('cbr-3b-loser-gone', 'none');
  S.L.alive = false;
  const { env, threw } = await pubAt(S, S.W, 22);
  const w = S.wr();
  check('B11 3B-3: the loser gone mid-stage, a PUB at the stager ends once (merge-stage-return), no bounce, never stamped at W',
    !threw && S.W.logs.filter((l) => l.ev === 'pubsub:merge-stage-return').length === 1 && !(w?.cacheIds?.has(env.msgId)),
    JSON.stringify({ threw, returns: S.W.logs.filter((l) => l.ev === 'pubsub:merge-stage-return').length }));
}
{
  const { S } = await held('cbr-3b-bad-merge-sub', 'none');
  const sub = { topicId: idHex(S.Lt.topicId), subscriberId: idHex(S.L.id), via: [idHex(S.W.id)], since: 0, merge: { corr: 'deadbeefdeadbeef', winner: idHex(S.W.id), winnerEpoch: 0, loserEpoch: S.lRole.epoch | 0, hash: 'x' } };
  await S.W.handlers.get('pubsub:sub')(sub, S.meta(S.W));
  await S.fab.settle();
  const w = S.wr();
  check('B12 3B-4: a merge SUB that does not complete never seats L on a created stager',
    !!w && !w.isRoot && !w.subscribers.has(idHex(S.L.id).toLowerCase()),
    JSON.stringify({ w: !!w, root: w?.isRoot, subs: w ? [...w.subscribers.keys()].map((k) => k.slice(0, 8)) : null }));
}
{
  const { S } = await held('cbr-3b-bad-merge-shape', 'none');
  const sub = { topicId: idHex(S.Lt.topicId), subscriberId: idHex(S.L.id), via: [idHex(S.W.id)], since: 0, merge: true };
  await S.W.handlers.get('pubsub:sub')(sub, S.meta(S.W));
  await S.fab.settle();
  const w = S.wr();
  check('B13 3B-4a: a SUB naming L with merge:true never seats L on a created stager',
    !!w && !w.isRoot && !w.subscribers.has(idHex(S.L.id).toLowerCase()),
    JSON.stringify({ w: !!w, subs: w ? [...w.subscribers.keys()].map((k) => k.slice(0, 8)) : null }));
}
console.log(`\nfence_cbr_3b_roleless_winner: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
