#!/usr/bin/env node
// =====================================================================
// fence_role_retired_cut.mjs — a RETIRED role refuses every commit that
// resumes on it (Aster edfdd869, 2026-10-19, on the empty-role grace).
//
// THE HAZARD. A handler captures its cut, then awaits a signature check. If
// refreshTick retires the role during that await — the grace reap, the
// on-sight reap, the idle TTL, the plain teardown — the handler still holds
// the detached object. 4.115.0's _cutHolds refused a commit only when a
// REPLACEMENT role sat in the map; with no replacement it let the write
// commit into the detached object, and the kill path then ACKed a tombstone
// nobody could ever serve. The reapers always did this; the grace just makes
// the window common for empty keyspace roots.
//
// THE RULE. axonRoles has ONE detach site, `_retireRole`, and it MARKS the
// object (`retired` = why, generation moved) before deleting it. `_cutHolds`
// refuses a marked role, replacement or not. A role that was never registered
// carries no mark and is still allowed (construction paths push before they
// register). A refused write is forwarded where a path exists, otherwise left
// unacked so its sender's own retry re-walks to the live terminus; it is never
// acked without custody. The duplicate-kill path commits under the same cut
// and keeps the same refusal/forward/no-ack semantics as a fresh kill.
//
// Mutants (red). The defence is layered: `_retireRole` moves the generation
// (an entry-time cut then fails on gen0) AND marks the object (a null-cut
// commit then fails on the mark). Drop the mark alone → R4 red (the
// generation still catches R1/R3); drop both → R1, R3, R4 red; drop the
// `=== false` branch of the duplicate path → R5 red.
// Run: node test/fence_role_retired_cut.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, gateRoot, signedPub, signedKill, bareMeta, log, idHex } from './lib/mergeFixture.mjs';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { ROLE_EMPTY_GRACE_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const acks = (ctx) => ctx.routed.filter((r) => r.type === 'pubsub:ingestack').length;
const settle = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setImmediate(r)); };

console.log('fence_role_retired_cut: a retired role refuses every commit that resumes on it');

const L = await realTopic('retired-cut');

/** A root with one body and a signed kill for it; the kill is parked in verifyKill when `during` runs. */
async function parkedKill(during) {
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  const acksBefore = acks(ctx);
  const p = ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));   // parked in `await verifyKill`
  during(ctx, role);                                                                             // synchronous: the kill has not resumed
  await p; await settle();
  return { ctx, role, ids, acksBefore };
}

{ // R1 — parked first kill, the role retired with NO replacement
  const { ctx, role, ids, acksBefore } = await parkedKill((c, r) => { c.am._retireRole(L.topicId, r, 'empty'); });
  check('R1 the role is marked retired and detached, generation moved', role.retired === 'empty' && !ctx.am.axonRoles.has(L.topicId) && role.gen === 1);
  check('R1 the kill is refused at applyKill on the detached object (cut-refused row)', log(ctx, 'cut-refused').some((l) => l.ctx.at === 'applyKill'), JSON.stringify(log(ctx, 'cut-refused').map((l) => l.ctx)));
  check('R1 no tombstone on the detached object, its body untouched', role.tombstones.size === 0 && role.cacheIds.has(ids[0]));
  check('R1 NOT acked: the killer\'s own retry re-walks to the live terminus', acks(ctx) === acksBefore, `${acks(ctx)}/${acksBefore}`);
  check('R1 no forward path remains (upstream cleared with the role) → undeliverable, not silently dropped', log(ctx, 'undeliverable').length === 1 || ctx.logs.some((l) => /undeliverable/.test(l.ev)), JSON.stringify(ctx.logs.map((l) => l.ev).slice(-6)));
}

{ // R2 — parked first kill, the role retired and REPLACED before it resumes
  const { ctx, role, ids, acksBefore } = await parkedKill((c, r) => { c.am._retireRole(L.topicId, r, 'idle'); c.rc.become(L.topicId, 'sub-terminal'); });
  const live = ctx.am.axonRoles.get(L.topicId);
  check('R2 refused: neither the retired object nor the replacement gains a tombstone; not acked', live !== role && role.tombstones.size === 0 && live.tombstones.size === 0 && acks(ctx) === acksBefore);
}

{ // R3 — a parked BODY (REPLAY_UP transfer, parked in verifyEnvelope) across a retirement with no replacement
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role } = await rootWithBodies(ctx, L, 1);
  const body = await signedPub(ctx, L.desc, 30);
  const p = ctx.am._syncIngest({ topicId: idHex(L.topicId), msgs: [{ msgId: body.msgId, json: body.json, publishTs: ctx.clock.t, seq: 50 }], dels: [] }, { targetId: L.self }, 'REPLAY_UP');
  ctx.am._retireRole(L.topicId, role, 'dead');
  await p; await settle();
  check('R3 the body is refused at cachePush on the retired object', !role.cacheIds.has(body.msgId) && log(ctx, 'cut-refused').some((l) => l.ctx.at === 'cachePush'), JSON.stringify(log(ctx, 'cut-refused').map((l) => l.ctx)));
}

{ // R4 — the mark is what refuses, not map absence: a NEVER-REGISTERED role still accepts a commit (construction paths)
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const fresh = makeRole(L.topicId, true, ctx.clock.t);             // built, pushed into, registered later — smoke_backup_handoff's shape
  check('R4 an unregistered, unretired role holds under a null cut', ctx.am._cutHolds(fresh, null) === true);
  check('R4 …and under its own entry-time cut', ctx.am._cutHolds(fresh, ctx.am._cutCapture(fresh)) === true);
  ctx.am._retireRole(L.topicId, fresh, 'teardown');
  check('R4 the same object refuses once retired, with and without a cut', ctx.am._cutHolds(fresh, null) === false && ctx.am._cutHolds(fresh, ctx.am._cutCapture(fresh)) === false);
}

{ // R5 — the DUPLICATE kill commits under the same cut: a gate set during its verify refuses it, it is forwarded, not acked
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  await ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher)); await settle();
  const acksAfterFirst = acks(ctx), killsBefore = ctx.routed.filter((r) => r.type === 'pubsub:kill').length;
  check('R5 precondition: the first kill stands and was acked', role.tombstones.size === 1 && acksAfterFirst >= 1);
  const p = ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));   // the duplicate, parked in verifyKill
  gateRoot(ctx, L.topicId, L.near, 3);                                                          // the role is gated while it waits
  await p; await settle();
  check('R5 the duplicate is refused at applyKill (gate during the await), the tombstone keeps its stamp', log(ctx, 'cut-refused').some((l) => l.ctx.at === 'applyKill' && l.ctx.gated === true) && role.tombstones.size === 1);
  check('R5 NOT acked, forwarded toward the gate\'s winner instead', acks(ctx) === acksAfterFirst && ctx.routed.filter((r) => r.type === 'pubsub:kill').length === killsBefore + 1, `${acks(ctx)}/${acksAfterFirst}`);
}

{ // R6 — every reap path marks the role: the grace reap, the on-sight reap, the idle TTL
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  ctx.am._hostKeyspace = true;
  const r = ctx.rc.become(L.topicId, 'sub-terminal');              // empty keyspace root
  await ctx.am.refreshTick();
  ctx.clock.t += ROLE_EMPTY_GRACE_MS + 1;
  await ctx.am.refreshTick();
  check('R6 the grace reap marks the role retired=empty', r.retired === 'empty' && !ctx.am.axonRoles.has(L.topicId), r.retired);
  ctx.am._hostKeyspace = false;
  const r2 = ctx.rc.become(L.topicId, 'sub-terminal');
  await ctx.am.refreshTick();
  check('R6 the on-sight reap marks retired=dead', r2.retired === 'dead', r2.retired);
  const r3 = ctx.rc.become(L.topicId, 'sub-terminal'); r3.subscribers.set(idHex(L.far), { since: 0, lastRenewed: ctx.clock.t }); r3.lastTs = ctx.clock.t;
  ctx.clock.t += 24 * 3600 * 1000 + 1; for (const s of r3.subscribers.values()) s.lastRenewed = ctx.clock.t;
  await ctx.am.refreshTick();
  check('R6 the idle TTL marks retired=idle', r3.retired === 'idle', r3.retired);
}

{ // R7 — the bulk reset is not a second, unmarked detach site (Aster 87632cbb): every role leaves marked
  const ctx = makeManagerAt(L.self, { neighbors: [L.near] });
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  const acksBefore = acks(ctx);
  const p = ctx.am._onKill({ topicId: idHex(L.topicId), kill }, bareMeta(ctx, L.publisher));   // parked in verifyKill
  ctx.am.resetState();                                                                           // the bulk reset lands while it waits
  await p; await settle();
  check('R7 resetState marks the role retired=reset and empties the map', role.retired === 'reset' && ctx.am.axonRoles.size === 0, role.retired);
  check('R7 the parked kill resuming after the reset is refused, no tombstone, not acked', role.tombstones.size === 0 && acks(ctx) === acksBefore);
}

console.log(`\nfence_role_retired_cut: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
