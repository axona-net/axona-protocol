// smoke_reorder_convergence.mjs — REORDER CONVERGENCE (REF-0.2 golden trace #1,
// Aster seq-598). The stamped-set materialization must converge: the SAME set of
// signed, ALREADY-STAMPED events — the replicate / replay-up / handoff bodies that
// carry their own (publishTs, seq) — ingested in ANY arrival order must land the
// holder in the same observable state. Networks reorder; a root that adopts the
// pre-transition half after the post half, or a backup that receives a REPLICATE
// batch shuffled, must not diverge from one that saw them in stamp order.
//
// This is a CHARACTERIZATION trace, not a change: it drives the real
// _ingestStamped + _applyKill (the TopicStore seam) and locks in what the current
// kernel actually does across permutations. Two independent findings the run
// pins:
//   CONVERGES (order-independent):  the held msgId SET, the high-water cursor
//     (role.lastTs → _highWater, the `since` a renewal advertises), the dense seq
//     counter, and the tombstone set — a kill suppresses its target whether it
//     arrives BEFORE or AFTER the body. The DURABLE state a late joiner would
//     replay is the same regardless of arrival order.
//   ARRIVAL-ORDERED (does NOT converge, by design):
//     • low-water. The cache is stored in arrival order, so _lowWater
//       (cache[0].publishTs) tracks the FIRST-ARRIVED stamp, not the minimum
//       held. This is the seam requirement REF-0.2 hands the refactor: TopicStore
//       must keep a stamp-ordered structure so low-water is well-defined
//       independent of arrival order. Asserting the current behavior here makes a
//       future stamp-ordered store announce itself by breaking this one check.
//     • the app-delivery TIMELINE. The four survivors are always delivered; the
//       killed body is delivered-then-retracted iff it arrived before its kill,
//       and suppressed outright otherwise. The set the app ends up believing is
//       correct converges (survivors live, target retracted or never shown) — but
//       whether a "deleted" callback ever fired is an arrival-order fact, not a
//       divergence in durable state.
//
// Run: node test/smoke_reorder_convergence.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

// A holder manager with an empty role for `topicBig`. isRoot is set, but
// _rootReplicas is left unset so _applyKill's eager-replicate branch is skipped —
// this keeps the trace hermetic (no wire), exercising only the store transition.
// Subscribed to the topic so exactly-once app delivery is observable.
function mkHolder(topicBig, nowRef) {
  const dht = {
    verdictsSupported: false,
    getSelfId: () => topicBig ^ (1n << 12n),
    onRoutedMessage: () => {},
    routeMessage: async () => ({ consumed: false }),
    neighbors: () => [],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => nowRef.t });
  am.nodeId = dht.getSelfId();
  am.setLogSink(() => {});
  const role = makeRole(topicBig, true, nowRef.t);
  am.axonRoles.set(topicBig, role);
  am.mySubscriptions.set(topicBig, { since: 0, lastRenewed: 0 });
  const delivered = [];
  am.onPubsubDelivery((_t, _j, msgId) => delivered.push(msgId));
  return { am, role, delivered };
}

// A stable, arrival-order-independent signature of the observable state.
function signature(h) {
  const r = h.role;
  return {
    heldSet:   [...r.cacheIds].sort().join(','),
    cacheLen:  r.cache.length,
    hw:        h.am._highWater(r),
    lw:        h.am._lowWater(r),
    seq:       r.seq,
    lastTs:    r.lastTs,
    tombSet:   [...r.tombstones.keys()].sort().join(','),
    delivered: [...new Set(h.delivered)].sort().join(','),
  };
}

// ── the fixed event set ─────────────────────────────────────────────────
// Five signed publishes, each ALREADY carrying a monotone (publishTs, seq) as if
// stamped by an origin root, plus one authorized kill of the MIDDLE message. All
// stamps sit below `now` so the future-stamp guard (§5) never fires.
const nowRef = { t: 1_700_000_000_000 };
const author = await createAuthorIdentity();
const topicDesc = { region: 'useast', name: 'reorder-convergence-smoke', write: 'open' };
const T = await deriveTopicIdBig(topicDesc);

const BASE = nowRef.t - 10_000;
const events = [];
for (let i = 0; i < 5; i++) {
  const env = await buildEnvelope({ topic: topicDesc, message: `m-${i}`, identity: author });
  events.push({ kind: 'pub', msgId: env.msgId, publishTs: BASE + i * 20, seq: i + 1, json: JSON.stringify(env) });
}
const killTarget = events[2].msgId;                 // the middle message
const killObj = await buildKill({ topicId: T.toString(16).padStart(66, '0'), msgId: killTarget, identity: author });
const killEvent = {
  kind: 'kill', msgId: killTarget, killTs: BASE + 200, seq: 6,
  signer: (killObj.signerPubkey || '').toLowerCase(),
};

// Feed one permutation of [pubs…, kill] into a fresh holder.
async function run(order) {
  const h = mkHolder(T, nowRef);
  for (const e of order) {
    if (e.kind === 'pub') {
      await h.am._ingestStamped(h.role, { msgId: e.msgId, publishTs: e.publishTs, json: e.json, seq: e.seq });
    } else {
      h.am._applyKill(h.role, T, { msgId: e.msgId, killTs: e.killTs, signer: e.signer, seq: e.seq });
    }
  }
  return h;
}

const all = [...events, killEvent];
const inOrder   = all.slice();                                   // pubs 0..4 then kill
const reversed  = all.slice().reverse();                         // kill first, pubs 4..0
const killEarly = [events[0], killEvent, events[1], events[2], events[3], events[4]]; // kill BEFORE its target body
const shuffled  = [events[4], events[1], killEvent, events[3], events[0], events[2]];

console.log('— ingest the same stamped set in four arrival orders —');
const A = await run(inOrder);
const B = await run(reversed);
const C = await run(killEarly);
const D = await run(shuffled);
const sA = signature(A), sB = signature(B), sC = signature(C), sD = signature(D);

// ── order-INDEPENDENT: the properties that must converge ─────────────────
console.log('— convergence: held set, delivery, high-water, seq, tombstone —');
const wantHeld = events.filter(e => e.msgId !== killTarget).map(e => e.msgId).sort().join(',');
check('held msgId SET is the four survivors, identical across all orders',
  sA.heldSet === wantHeld && sB.heldSet === wantHeld && sC.heldSet === wantHeld && sD.heldSet === wantHeld,
  JSON.stringify({ A: sA.heldSet, B: sB.heldSet, C: sC.heldSet, D: sD.heldSet }));
check('cache length is 4 in every order (killed body never resident)',
  [sA, sB, sC, sD].every(s => s.cacheLen === 4), JSON.stringify([sA, sB, sC, sD].map(s => s.cacheLen)));
check('high-water cursor converges (= newest stamp held, order-independent)',
  sA.hw === sB.hw && sB.hw === sC.hw && sC.hw === sD.hw && sA.hw === BASE + 4 * 20,
  JSON.stringify({ A: sA.hw, B: sB.hw, C: sC.hw, D: sD.hw, want: BASE + 80 }));
check('dense seq counter converges (= max slot seen, incl. the kill\'s)',
  [sA, sB, sC, sD].every(s => s.seq === 6), JSON.stringify([sA, sB, sC, sD].map(s => s.seq)));
check('lastTs converges (= max publish stamp)',
  [sA, sB, sC, sD].every(s => s.lastTs === BASE + 4 * 20), JSON.stringify([sA, sB, sC, sD].map(s => s.lastTs)));
check('tombstone set is exactly the killed msgId, every order',
  [sA, sB, sC, sD].every(s => s.tombSet === killTarget), JSON.stringify([sA, sB, sC, sD].map(s => s.tombSet)));
check('kill suppresses its target whether it arrived AFTER (A) or BEFORE (C) the body',
  !A.role.cacheIds.has(killTarget) && !C.role.cacheIds.has(killTarget) &&
  A.role.tombstones.has(killTarget) && C.role.tombstones.has(killTarget));
check('every order delivered ALL four survivors to the app (exactly once)',
  [sA, sB, sC, sD].every(s => wantHeld.split(',').every(id => s.delivered.includes(id))),
  JSON.stringify([sA, sB, sC, sD].map(s => s.delivered)));
// The killed body is delivered-then-retracted iff it arrived before its kill
// (order A, kill last), and suppressed outright otherwise (B/C/D). This is the
// app-delivery TIMELINE asymmetry — durable state still converges above; only
// whether a "deleted" callback fired depends on arrival order.
check('killed body reached the app ONLY when it arrived before its kill (order A)',
  A.delivered.includes(killTarget) &&
  !B.delivered.includes(killTarget) && !C.delivered.includes(killTarget) && !D.delivered.includes(killTarget),
  JSON.stringify({ A: A.delivered.includes(killTarget), B: B.delivered.includes(killTarget), C: C.delivered.includes(killTarget), D: D.delivered.includes(killTarget) }));

// ── dedup: re-ingesting the whole set is a no-op ─────────────────────────
console.log('— dedup: replaying the full set again changes nothing —');
const before = signature(A);
for (const e of events) {
  if (e.msgId !== killTarget) {
    await A.am._ingestStamped(A.role, { msgId: e.msgId, publishTs: e.publishTs, json: e.json, seq: e.seq });
  }
}
const after = signature(A);
check('re-ingest leaves cache length unchanged (msgId dedup)', before.cacheLen === after.cacheLen, `${before.cacheLen}→${after.cacheLen}`);
check('re-ingest does NOT resurrect the killed body', !A.role.cacheIds.has(killTarget));
check('re-ingested killed body stays suppressed (tombstone honored on replay)', after.tombSet === killTarget);

// ── CHARACTERIZATION: low-water is arrival-ordered, NOT converged ────────
// The seam finding REF-0.2 hands the refactor. cache[0] is the first-arrived
// entry, so _lowWater reports that stamp — different per order. Locked in so a
// future stamp-ordered TopicStore breaks THIS check and nothing else.
console.log('— characterization: low-water tracks first-arrival (seam requirement) —');
check('low-water differs across arrival orders (cache is arrival-ordered today)',
  new Set([sA.lw, sB.lw, sC.lw, sD.lw]).size > 1,
  `lw A=${sA.lw} B=${sB.lw} C=${sC.lw} D=${sD.lw}`);
check('each low-water equals ITS order\'s first-cached entry\'s stamp',
  sA.lw === A.role.cache[0].publishTs && sB.lw === B.role.cache[0].publishTs &&
  sC.lw === C.role.cache[0].publishTs && sD.lw === D.role.cache[0].publishTs);

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
