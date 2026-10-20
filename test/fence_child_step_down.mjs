#!/usr/bin/env node
// =====================================================================
// fence_child_step_down.mjs — THE THIN BRANCH (Topic Lifetime 4.7; axona-docs
// architecture/Axona-Topic-Lifetime-v0.1.md v0.9 @ 11288d7; David 2026-10-20:
// "Let's use 2 subscribers and 90 seconds.").
//
// A child whose declared demand has read below CHILD_MIN_READERS (2) for
// CHILD_STEP_DOWN_MS (90 s) steps down: it sends REHOME, naming its upstream,
// to each reader seat; tells its parent with an UNSUB marked steppedDown (the
// parent arms a cooldown and does not re-promote it); writes a LEFT record;
// retires. Hysteresis: a new child is not judged before CHILD_MIN_STAY_MS
// (90 s); demand at or above the floor clears the clock. REHOME is
// kernel-internal: the reader's kernel keeps the app's subscription, re-pins
// to the named upstream and walks to the topic on its fast cadence; a REHOME
// from a node that is not the receiver's upstream is ignored. Not applied
// to a backup, a root, or a read holder; not while the upstream is
// unreachable (that is the election path).
//
// Mutants (red): make REHOME end the app's subscription → H1 (reader gone);
// drop the min-stay check → C2; drop the cooldown filter in _promoteChild →
// P1; count a child's seats as readers regardless of its declaration → C1.
// Run: TOPIC_LIFETIME=1 node test/fence_child_step_down.mjs
// =====================================================================
import { manager, seed, sub, rehome, idHex, lc, TP, NODE, SELF, T0, MIN, T } from './lib/lifetimeFixture.mjs';
import { CHILD_MIN_READERS, CHILD_STEP_DOWN_MS, CHILD_MIN_STAY_MS, CHILD_COOLDOWN_MS, DROP_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };
const ROOT = NODE(3), A = NODE(10), B = NODE(11), C = NODE(12), STRANGER = NODE(13);
const TICK = 5_000;

console.log('fence_child_step_down: a thin branch steps down and the app never knows');
check(`David's numbers: ${CHILD_MIN_READERS} readers, ${CHILD_STEP_DOWN_MS / 1000} s; stay ${CHILD_MIN_STAY_MS / 1000} s, cooldown ${CHILD_COOLDOWN_MS / 1000} s`, CHILD_MIN_READERS === 2 && CHILD_STEP_DOWN_MS === 90_000 && CHILD_MIN_STAY_MS === 90_000 && CHILD_COOLDOWN_MS === 300_000);

/** A child relay at SELF under ROOT with `n` reader seats renewing via SELF. */
async function child(ctx, t, readers) {
  const role = seed(ctx, t, { isRoot: false });
  ctx.am._upstream.set(t, [lc(idHex(ROOT))]);
  for (const r of readers) await sub(ctx, t, r, {}, { viaSelf: true });
  return role;
}
const renewAll = async (ctx, t, readers) => { for (const r of readers) await sub(ctx, t, r, {}, { viaSelf: true }); };

console.log('\n[C1] one reader for 90 s after a 90 s stay: REHOME to the reader, UNSUB steppedDown to the parent, LEFT record, role gone');
{
  const ctx = manager({ neighbors: [ROOT] });
  const role = await child(ctx, TP(1), [A]);
  let at = T0;
  for (; at < T0 + CHILD_MIN_STAY_MS; at += TICK) { await renewAll(ctx, TP(1), [A]); await ctx.tick(at); }
  check('inside the minimum stay nothing happens though demand reads 1 (thinSince stamped on the first tick)', ctx.held(TP(1)) && role.thinSince === T0);
  for (; at <= T0 + CHILD_STEP_DOWN_MS; at += TICK) { await renewAll(ctx, TP(1), [A]); await ctx.tick(at); }
  check('at exactly 90 s thin it still stands', ctx.held(TP(1)));
  await renewAll(ctx, TP(1), [A]); await ctx.tick(at);
  check('one tick later it steps down: role gone, logged with demand 1', !ctx.held(TP(1)) && ctx.log('child-stepped-down').length === 1 && ctx.log('child-stepped-down')[0].ctx.demand === 1);
  const rh = ctx.sent(T.REHOME);
  check('one REHOME, to the reader, from this node, naming the upstream', rh.length === 1 && rh[0].target === A && lc(rh[0].payload.from) === lc(idHex(SELF)) && lc(rh[0].payload.upstream) === lc(idHex(ROOT)));
  const un = ctx.sent(T.UNSUB, (r) => r.payload.steppedDown === true);
  check('one UNSUB marked steppedDown, via the parent', un.length === 1 && lc(un[0].payload.via[0]) === lc(idHex(ROOT)));
  check('a LEFT record (stepped-down) names the upstream and no root', ctx.am._leftRecords.get(TP(1))?.why === 'stepped-down' && ctx.am._leftRecords.get(TP(1)).upstream === lc(idHex(ROOT)) && ctx.am._leftRecords.get(TP(1)).rootId === null);
  check('counted', ctx.am.inspectAdmission().lifetime.steppedDown === 1);
}

console.log('\n[C2] hysteresis: demand at the floor clears the clock; a new child is not judged before its stay; a backup and a read holder are never stepped down');
{
  const ctx = manager({ neighbors: [ROOT] });
  const role = await child(ctx, TP(2), [A, B]);
  let at = T0;
  for (; at <= T0 + 2 * CHILD_STEP_DOWN_MS; at += TICK) { await renewAll(ctx, TP(2), [A, B]); await ctx.tick(at); }
  check('two readers: never thin, thinSince null, still a child', ctx.held(TP(2)) && role.thinSince === null);
  const bStops = at;
  for (; at <= bStops + DROP_MS + TICK; at += TICK) { await renewAll(ctx, TP(2), [A]); await ctx.tick(at); }   // B stops renewing; its SEAT lasts DROP_MS
  const thinAt = role.thinSince;
  check('B gone: its seat lapses at DROP_MS, and the child is thin from the first tick that read demand 1', thinAt !== null && thinAt > bStops + DROP_MS - TICK && ctx.held(TP(2)), thinAt);
  await renewAll(ctx, TP(2), [A, B]); await ctx.tick(at);                              // B comes back
  check('B back: the clock clears', role.thinSince === null && ctx.held(TP(2)));
  // a fresh child, thin from birth, judged only after its stay
  const young = seed(ctx, TP(3), { isRoot: false, createdAt: ctx.clock.t });
  ctx.am._upstream.set(TP(3), [lc(idHex(ROOT))]);
  for (let i = 0; i <= (CHILD_STEP_DOWN_MS / TICK) + 1; i++) { await sub(ctx, TP(3), C, {}, { viaSelf: true }); await ctx.tick(ctx.clock.t + TICK); }
  check('a child thin from birth goes once past both its thin window and its stay (equal here)', !ctx.held(TP(3)));
  // the stay has teeth when it is LONGER than the thin window: 150 s of stay, 90 s thin → not before 150 s
  ctx.am._childMinStayMs = 150_000;
  const young2 = seed(ctx, TP(30), { isRoot: false, createdAt: ctx.clock.t });
  ctx.am._upstream.set(TP(30), [lc(idHex(ROOT))]);
  const born = ctx.clock.t;
  while (ctx.clock.t - born < 150_000) { await sub(ctx, TP(30), C, {}, { viaSelf: true }); await ctx.tick(ctx.clock.t + TICK); }
  check('thin for 150 s but inside a 150 s stay: still a child', ctx.held(TP(30)) && (ctx.clock.t - young2.createdAt) <= 150_000, ctx.clock.t - born);
  await sub(ctx, TP(30), C, {}, { viaSelf: true }); await ctx.tick(ctx.clock.t + TICK);
  check('one tick past the stay it steps down', !ctx.held(TP(30)));
  ctx.am._childMinStayMs = CHILD_MIN_STAY_MS;
  // a backup with a seat
  const bk = seed(ctx, TP(4), { isRoot: false }); bk.backupOf = lc(idHex(ROOT)); ctx.am._backupTopics.add(TP(4)); bk.lastReplicaAt = ctx.clock.t;
  ctx.am._upstream.set(TP(4), [lc(idHex(ROOT))]);
  await sub(ctx, TP(4), A, {}, { viaSelf: true });
  for (let i = 0; i < 2 * (CHILD_STEP_DOWN_MS / TICK); i++) { bk.lastReplicaAt = ctx.clock.t; await sub(ctx, TP(4), A, {}, { viaSelf: true }); await ctx.tick(ctx.clock.t + TICK); }
  check('a backup seating one reader is never stepped down', ctx.held(TP(4)) && bk.backupOf !== null && !ctx.log('child-stepped-down').some((r) => r.ctx.topic === idHex(TP(4)).slice(0, 12)));
}

console.log('\n[C3] an unreachable upstream is not a step-down: the election path keeps the branch');
{
  const ctx = manager({ neighbors: [] });                 // ROOT is not a neighbour → _isReachableId false
  await child(ctx, TP(5), [A]);
  for (let at = T0; at <= T0 + 3 * CHILD_STEP_DOWN_MS; at += TICK) { await renewAll(ctx, TP(5), [A]); await ctx.tick(at); }
  check('thin for three windows with the root unreachable: still a child', ctx.held(TP(5)) && ctx.sent(T.REHOME).length === 0);
}

console.log('\n[B02] BODY CUSTODY (Aster c5adedec): a thin child holding a body its upstream did not send does not step down until the upstream has it');
{
  const ctx = manager({ neighbors: [ROOT] });
  const role = await child(ctx, TP(10), [A]);
  // a body that arrived from BELOW (a reader's replay-up), not from the upstream
  role.cache.push({ msgId: 'below', publishTs: T0, json: '{}', bytes: 80 }); role.cacheIds.add('below');
  let at = T0;
  for (; at <= T0 + 3 * CHILD_STEP_DOWN_MS; at += TICK) { await renewAll(ctx, TP(10), [A]); await ctx.tick(at); }
  check('thin for three windows with an untransferred body: still a child, the body still held, logged once', ctx.held(TP(10)) && role.cache.length === 1 && ctx.log('step-down-held-body').length === 1 && ctx.sent(T.REHOME).length === 0);
  // the root pulled it and fanned it back: a DELIVER from the upstream carrying that msgId marks it upstream-held
  await ctx.am._onDeliver({ topicId: idHex(TP(10)), from: idHex(ROOT), msgs: [{ msgId: 'below', publishTs: T0, json: '{}', seq: 1 }], gen: { root: lc(idHex(ROOT)), epoch: 1 } }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  await renewAll(ctx, TP(10), [A]); await ctx.tick(at);
  check('once the upstream has sent that body back, the child steps down', !ctx.held(TP(10)) && ctx.sent(T.REHOME).length === 1);
  // a body the upstream SENT is no bar: the ordinary relay case
  const r2 = await child(ctx, TP(11), [A]);
  await ctx.am._onDeliver({ topicId: idHex(TP(11)), from: idHex(ROOT), msgs: [{ msgId: 'fromup', publishTs: ctx.clock.t, json: '{}', seq: 1 }], gen: { root: lc(idHex(ROOT)), epoch: 1 } }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('the relayed body is cached and marked upstream-held', r2.cache.length === 1 && r2.upstreamHeld.has('fromup'));
  const born = ctx.clock.t;
  while (ctx.clock.t - born <= CHILD_STEP_DOWN_MS + TICK) { await renewAll(ctx, TP(11), [A]); await ctx.tick(ctx.clock.t + TICK); }
  check('a thin child whose only body came from its upstream steps down', !ctx.held(TP(11)));
}

console.log('\n[H1] REHOME received: the reader keeps the app\'s subscription, re-pins to the named upstream, renews fast; a REHOME from a non-upstream is ignored');
{
  const ctx = manager();
  ctx.am.pubsubSubscribe(TP(6));
  ctx.am._upstream.set(TP(6), [lc(idHex(C))]);           // pinned to the child C
  const s = ctx.am.mySubscriptions.get(TP(6)); s.interval = 60_000; s.lastRenewSent = T0;
  const subsBefore = ctx.sent(T.SUB).length;
  await rehome(ctx, TP(6), STRANGER, ROOT);
  check('from a stranger: ignored, pin unchanged', ctx.log('rehome-ignored').length === 1 && ctx.am._upstream.get(TP(6))[0] === lc(idHex(C)) && ctx.sent(T.SUB).length === subsBefore);
  await ctx.am._onDeliver({ topicId: idHex(TP(6)), from: idHex(C), msgs: [], gen: { root: lc(idHex(ROOT)), epoch: 2 } }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  await ctx.am._onRehome({ topicId: idHex(TP(6)), from: idHex(C), upstream: idHex(ROOT), gen: { root: lc(idHex(ROOT)), epoch: 1 } }, { targetId: SELF, isTerminal: true, hopCount: 1, fromId: idHex(C) });
  check('from my upstream but naming an OLD generation (epoch 1 while attached to 2): ignored (TL-B01)', ctx.log('rehome-ignored').some((r) => r.ctx.why === 'foreign-generation') && ctx.am._upstream.get(TP(6))[0] === lc(idHex(C)));
  await rehome(ctx, TP(6), C, ROOT);
  check('from my upstream C: the app\'s subscription persists', ctx.am.mySubscriptions.has(TP(6)));
  check('re-pinned to the named upstream (the root), interval snapped to the floor', ctx.am._upstream.get(TP(6))[0] === lc(idHex(ROOT)) && s.interval === ctx.am.renewFastMs);
  const subs = ctx.sent(T.SUB).slice(subsBefore);
  check('one SUB sent at once, via the root', subs.length === 1 && lc(subs[0].payload.via[0]) === lc(idHex(ROOT)));
  check('counted: rehomed = 1', ctx.am.inspectAdmission().lifetime.rehomed === 1);
}

console.log('\n[P1] the parent: an UNSUB marked steppedDown drops the seat and arms a cooldown; the cooled node is not re-promoted; the cooldown expires');
{
  const ctx = manager({ maxDirect: 3 });
  const root = seed(ctx, TP(7));
  await sub(ctx, TP(7), A); await sub(ctx, TP(7), B); await sub(ctx, TP(7), C);
  await ctx.am._onUnsub({ topicId: idHex(TP(7)), via: [idHex(SELF)], subscriberId: idHex(A), steppedDown: true }, { targetId: SELF, isTerminal: true, hopCount: 1 });
  check('A\'s seat is dropped and A is on cooldown', !root.subscribers.has(lc(idHex(A))) && ctx.am._childCooldown.get(lc(idHex(A))) === T0 + CHILD_COOLDOWN_MS && ctx.log('child-cooldown').length === 1);
  // a fresh root where the cooled node is FIRST in line: without the filter it would be the one promoted
  const root2 = seed(ctx, TP(71));
  await sub(ctx, TP(71), A); await sub(ctx, TP(71), B); await sub(ctx, TP(71), C);
  await sub(ctx, TP(71), STRANGER);                         // the fourth seat: over MAX_DIRECT → promote a leaf
  const adopts = ctx.sent(T.ADOPT);
  check('over capacity the root promotes the first leaf that is NOT the cooled node (B, not A)', adopts.length === 1 && adopts[0].target === B && !root2.children.has(lc(idHex(A))) && root2.children.has(lc(idHex(B))), adopts.map((a) => idHex(a.target).slice(-4)).join(','));
  void root;
  ctx.clock.t = T0 + CHILD_COOLDOWN_MS + 1;
  await ctx.tick(ctx.clock.t);
  check('the cooldown expires with time', !ctx.am._childCooldown.has(lc(idHex(A))));
}

console.log('\n[U1] unarmed: no step-down, no cooldown, REHOME consumed without effect');
{
  const ctx = manager({ armed: false, neighbors: [ROOT] });
  await child(ctx, TP(8), [A]);
  for (let at = T0; at <= T0 + 3 * CHILD_STEP_DOWN_MS; at += TICK) { await renewAll(ctx, TP(8), [A]); await ctx.tick(at); }
  check('unarmed: a one-reader child stands for ever', ctx.held(TP(8)) && ctx.sent(T.REHOME).length === 0);
  ctx.am.pubsubSubscribe(TP(9)); ctx.am._upstream.set(TP(9), [lc(idHex(C))]);
  await rehome(ctx, TP(9), C, ROOT);
  check('unarmed: REHOME changes nothing', ctx.am._upstream.get(TP(9))[0] === lc(idHex(C)) && ctx.log('rehomed').length === 0);
}

console.log(`\nfence_child_step_down: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
