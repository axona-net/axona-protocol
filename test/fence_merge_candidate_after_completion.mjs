#!/usr/bin/env node
// =====================================================================
// fence_merge_candidate_after_completion.mjs — a COMPLETED merge leaves no
// candidate record behind, and a later trigger naming the same winner
// reaches the offer (4.159.0).
//
// THE BUG (production, 2026-11-19 to 11-21, topic #council). role.merge.
// candidate was cleared by every abort, refusal and timeout and by nothing
// on the path of a merge that COMPLETED. The loser kept { id: W } on its
// role. When its step-down hold expired it promoted again on the same role
// object, and from then on every trigger naming W reached
//
//   if (prev && !RootClaim.claimWins(topicBig, claim, prev)) return false;
//
// in _mergeCandidate. The same id does not win over itself, so the call
// returned with no log and no offer. The relay named the closer root 2,666
// times in 33 hours, logging `root-verify-demote` each time, and stayed a
// second root. A fresh subscribe then landed on W, publishes followed W, and
// the relay's subscribers heard nothing.
//
// The scene: W (closer) and L (farther), both root, both holding one body.
//   A  L merges into W and the round closes                      (works today)
//   B  the hold expires and L promotes again on the same role
//   C  L's self-verify names W again                             (the bug)
//
// Checks:
//   1  after the closed merge L holds no candidate record
//   2  after hold expiry and re-promotion L is root, with no candidate
//   3  the second verify sends a second MERGE_OFFER to W
//   4  the row it logs says outcome 'offered'
//   5  the two converge: exactly ONE of them is root afterwards and it holds
//      all three bodies. WHICH one is the election's business (epoch first:
//      L re-promoted at a higher epoch, so here W merges into L). On the old
//      code there is no second offer and two roots stand for ever.
//   6  a verify while an offer to W stands logs outcome 'standing' and sends
//      no further offer (the same-winner path does not storm)
//
//   7  MC-1 (Orion 49bffa7b): MERGE_CLOSED is one unretried frame. With it
//      LOST on the first merge, L still holds `completing`, the round record
//      and its pins when the hold expires. Re-promoted, L must carry none of
//      them: it offers again and the two converge to one root holding every
//      body. Before the fix `completing` refused every later offer and every
//      inbound pull on the new root, and two roots stood for ever.
//   8  The same lost MERGE_CLOSED, L staying a child: past MERGE_RETENTION_MS
//      the sweep closes the ROUND (record, `completing`, pins) and not before.
//   9  CUSTODY (Aster 9ba1d128; Dual-Root v0.1, "What holds custody when
//      completion is lost"): nobody confirmed closure, so the child may be
//      the only holder. After the sweep its export is still in the cache
//      with the cache driven past its cap, and the role is custody-alive.
//  10  An expiry pass before the entry's own TTL removes nothing.
//  11  Past this node's own TTL the entry expires, custody is empty and the
//      role is no longer custody-alive. Round cleanup is bounded by R;
//      custody is bounded by TTL.
//
// Mutants (each red): drop the same-winner branch in _mergeCandidate → 3,4,5;
// drop the clear in _mergeStep5 AND _onMergeClosed AND the hold expiry AND
// _set → 1 (any one of the four left in keeps 1 green on this path, so 1 is
// pinned by removing them together; the branch is the load-bearing fix).
//  12  ROUND OVERLAP (Aster 054520b2): one role can be W for one round and L
//      for another. C (farthest) merges into L and its completion SUB is
//      lost, so L holds an acked pull. L then merges into W and MERGE_CLOSED
//      is lost. When the sweep ends L's PULL at its retention bound, L's own
//      round is still completing: its export must stay pinned and survive a
//      full cache. Until 4.159.0 that release cleared every pin on the role.
//  13  When L's own round then reaches ITS bound, the export moves to
//      custody and no pin is left.
//  14  The other direction: MERGE_CLOSED arrives for L's round while the
//      pull from C is still open. The pull's baseline and manifest stay
//      pinned and the write reserve stands.
//
//  15  Orion's chain (71d4a536): C stalls MID-PULL, so L's pull is still
//      `pulling` with its baseline pinned; L merges into W, MERGE_CLOSED lost.
//      The pull TIMES OUT while L is completing: the export stays pinned and
//      held under cache pressure, moves to custody at L's own bound, and is
//      gone, with custody empty, after this node's TTL.
//  16  Two W-side pulls open on one root: ending the first leaves the
//      second's baseline and manifest pinned and the write reserve standing.
//
// Overlap mutants: _mergeReleasePins clears the set (the old body) -> 12;
// _onMergeClosed clears the set (the old body) -> 14.
// Custody mutants: release the pins in the sweep without moving the export to
// `custody` → 9; exempt custody from _expireCache as pins are → 11; drop the
// custody test from the cap loop in _cachePush → 9.
// Run: node test/fence_merge_candidate_after_completion.mjs
// =====================================================================
import { splitScene, idHex, freshId } from './lib/mergeFixture.mjs';
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';
import { MERGE_RETENTION_MS, MERGE_TIMEOUT_MS, TTL_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_candidate_after_completion: a completed merge leaves no candidate; the same winner can be offered again');

const S = await splitScene('candidate-after-completion');
await S.stamp(S.W, 1); await S.stamp(S.Ln, 2);
const offers = () => S.logs(S.Ln, 'merge-offer').length;

// A: the first merge, to its close.
await S.trigger();
const closed1 = S.logs(S.Ln, 'merge-closed').length;
if (S.lRole.isRoot || closed1 !== 1) { console.log(`setup failed: first merge did not close (isRoot=${S.lRole.isRoot}, closed=${closed1})`); process.exit(1); }
check('1 after the closed merge L holds no candidate record', (S.lRole.merge?.candidate ?? null) === null, JSON.stringify(S.lRole.merge?.candidate));
const offers1 = offers();

// B: the hold expires; a bare terminal request promotes L on the SAME role object.
S.fab.clock += (S.Ln.am._stepDownHoldMs | 0) + 1000;
S.Ln.am._announceRoot = () => {};     // W does not hear L's new claim either: the merge is the only way the two meet
const roleBefore = S.lRole;
S.Ln.rc.promote(S.lRole, { topicId: idHex(S.Lt.topicId), via: [] }, { targetId: S.lId, isTerminal: true, hopCount: 1 });
await S.fab.settle();
const lRole = S.Ln.am.axonRoles.get(S.Lt.topicId);
check('2 after hold expiry L is root again on the same role, with no candidate', lRole === roleBefore && lRole.isRoot === true && (lRole.merge?.candidate ?? null) === null,
  `same=${lRole === roleBefore} isRoot=${lRole?.isRoot} cand=${JSON.stringify(lRole?.merge?.candidate)}`);

// C: L takes a body W lacks, then its self-verify names W again.
await S.stamp(S.Ln, 3);
// Hold W's reply so the offer STANDS while a second verify fires (check 6).
let held = [];
S.fab.tap = (j) => (j.type === 'pubsub:mergeoffer') ? (held.push(j), null) : j;
await S.trigger();
check('3 the second verify sends a second MERGE_OFFER to W', offers() === offers1 + 1, `offers ${offers1} -> ${offers()}`);
const rows = S.logs(S.Ln, 'root-verify-demote');
check("4 the verify row says outcome 'offered'", rows.length >= 2 && rows[rows.length - 1].ctx?.outcome === 'offered', JSON.stringify(rows[rows.length - 1]?.ctx));
const offersHeld = offers();
await S.trigger();
const rows2 = S.logs(S.Ln, 'root-verify-demote');
check("6 a verify while the offer stands logs 'standing' and sends no further offer", offers() === offersHeld && rows2[rows2.length - 1].ctx?.outcome === 'standing', `offers ${offersHeld} -> ${offers()} ${JSON.stringify(rows2[rows2.length - 1]?.ctx)}`);

// Release the held offer and let the second merge run.
S.fab.tap = null;
for (const j of held) S.fab.queue.push(j);
await S.fab.settle();
for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
await S.fab.settle();
const wRole = S.W.am.axonRoles.get(S.Lt.topicId);
const roots = [lRole, wRole].filter((r) => r && r.isRoot);
check('5 the two converge: exactly one root afterwards, holding all three bodies', roots.length === 1 && roots[0].cache.length === 3,
  `roots=${roots.length} L=${lRole.isRoot}/${lRole.cache.length} W=${wRole?.isRoot}/${wRole?.cache.length}`);

// ── MC-1: the same story with MERGE_CLOSED lost ──────────────────────────
{
  const S2 = await splitScene('candidate-mergeclosed-lost');
  await S2.stamp(S2.W, 1); await S2.stamp(S2.Ln, 2);
  S2.fab.tap = (j) => (j.type === 'pubsub:mergeclosed') ? null : j;      // the single frame, lost
  await S2.trigger();
  const L = S2.lRole;
  if (L.isRoot || !L.merge?.completing || !L.sync.openMerges.size) { console.log(`setup failed (MC-1): isRoot=${L.isRoot} completing=${!!L.merge?.completing} open=${L.sync.openMerges.size}`); process.exit(1); }
  S2.fab.tap = null;
  S2.fab.clock += (S2.Ln.am._stepDownHoldMs | 0) + 1000;
  S2.Ln.am._announceRoot = () => {};
  S2.Ln.rc.promote(L, { topicId: idHex(S2.Lt.topicId), via: [] }, { targetId: S2.lId, isTerminal: true, hopCount: 1 });
  await S2.fab.settle();
  const clean = L.isRoot === true && L.merge.completing === null && L.merge.candidate === null && L.sync.openMerges.size === 0 && L.merge.pins.size === 0 && L.merge.custody.size === 0;
  const offersBefore = S2.logs(S2.Ln, 'merge-offer').length;
  await S2.stamp(S2.Ln, 3);
  await S2.trigger();
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  await S2.fab.settle();
  const w2 = S2.W.am.axonRoles.get(S2.Lt.topicId);
  const roots2 = [L, w2].filter((r) => r && r.isRoot);
  check('7 MERGE_CLOSED lost: the re-promoted root carries no round residue, offers again, and the two converge',
    clean && S2.logs(S2.Ln, 'merge-offer').length === offersBefore + 1 && roots2.length === 1 && roots2[0].cache.length === 3,
    `clean=${clean} completing=${JSON.stringify(L.merge.completing)} open=${L.sync.openMerges.size} pins=${L.merge.pins.size} offers ${offersBefore} -> ${S2.logs(S2.Ln, 'merge-offer').length} roots=${roots2.length} L=${L.isRoot}/${L.cache.length} W=${w2?.isRoot}/${w2?.cache.length}`);
}
{
  const S3 = await splitScene('candidate-mergeclosed-lost-sweep');
  await S3.stamp(S3.W, 1); const exportId = await S3.stamp(S3.Ln, 2);
  S3.fab.tap = (j) => (j.type === 'pubsub:mergeclosed') ? null : j;
  await S3.trigger();
  const L = S3.lRole; const am = S3.Ln.am;
  if (L.isRoot || !L.merge?.completing || !L.merge.pins.has(exportId)) { console.log(`setup failed (sweep): isRoot=${L.isRoot} completing=${!!L.merge?.completing} pinned=${L.merge?.pins?.has(exportId)}`); process.exit(1); }
  am._mergeSweep(S3.fab.clock + MERGE_RETENTION_MS - 1);
  const early = L.merge.completing !== null && L.sync.openMerges.size === 1 && L.merge.pins.has(exportId);
  const tSweep = S3.fab.clock + MERGE_RETENTION_MS + 1;
  am._mergeSweep(tSweep);
  check('8 MERGE_CLOSED lost, L stays a child: the sweep ends the ROUND at MERGE_RETENTION_MS and not before',
    early && L.isRoot === false && L.merge.completing === null && L.sync.openMerges.size === 0 && L.merge.pins.size === 0 && S3.logs(S3.Ln, 'merge-completion-unclosed').length === 1,
    `early=${early} completing=${JSON.stringify(L.merge.completing)} open=${L.sync.openMerges.size} pins=${L.merge.pins.size} log=${S3.logs(S3.Ln, 'merge-completion-unclosed').length}`);
  // Cache pressure: the cap is set to two entries and five newer bodies arrive.
  const capWas = am._cacheMax; am._cacheMax = 2;
  for (let i = 0; i < 5; i++) am._cachePush(L, { msgId: `pressure-${i}`, publishTs: tSweep + 1 + i, json: '{}' });
  am._cacheMax = capWas;
  check('9 custody: with the cache driven past its cap the unclosed export is still held and the role is custody-alive',
    L.cacheIds.has(exportId) && L.merge.custody.has(exportId) && L.cache.length <= 3 && am._custodyAlive(L, tSweep + 10) === true,
    `held=${L.cacheIds.has(exportId)} custody=${L.merge.custody?.has(exportId)} cache=${L.cache.length} alive=${am._custodyAlive(L, tSweep + 10)}`);
  const body = L.cache.find((c) => c.msgId === exportId);
  const tEarly = body.publishTs + TTL_MS - 1;
  am._expireTombstones(L, tEarly); am._expireCache(L, tEarly);
  check('10 an expiry pass before the entry\'s own TTL removes nothing', L.cacheIds.has(exportId) && L.merge.custody.has(exportId),
    `held=${L.cacheIds.has(exportId)} custody=${L.merge.custody.has(exportId)}`);
  const tLate = body.publishTs + TTL_MS + 1;
  am._expireTombstones(L, tLate); am._expireCache(L, tLate);
  check('11 past this node\'s own TTL the entry expires, custody is empty, the role is not custody-alive',
    !L.cacheIds.has(exportId) && L.merge.custody.size === 0 && am._custodyAlive(L, tLate) === false,
    `held=${L.cacheIds.has(exportId)} custody=${L.merge.custody.size} alive=${am._custodyAlive(L, tLate)}`);
}

// ── round overlap: L is W for C's round and L for its own ────────────────
async function overlapScene(name, { dropClosed, stall = false, second = false, toW = true }) {
  const S = await splitScene(name);
  const topic = S.Lt.topicId, lHex = idHex(S.lId);
  let cId; do { cId = await freshId(); } while ((cId ^ topic) <= (S.lId ^ topic));   // C is farther than L
  const C = S.fab.addNode(cId); const cHex = idHex(cId);
  C.am._announceRoot = () => {}; S.Ln.am._announceRoot = () => {};
  C.am.dht.lookup = async () => ({ path: [cHex, lHex], hops: 1, time: 1, found: false });
  const cRole = C.rc.become(topic, 'sub-terminal');
  await S.stamp(S.W, 1); await S.stamp(S.Ln, 2); await S.stamp(C, 3);
  // C merges into L; C's merge-bearing SUB is lost, so L's pull stays acked.
  let pullups = 0;
  const lost = (j) => {
    if (j.type === 'pubsub:sub' && j.payload?.merge && j.meta?.fromId !== lHex) return true;         // a farther root's completion SUB
    if (stall && j.type === 'pubsub:pullup' && j.dest === cId && ++pullups > 1) return true;       // C never sees the pull after the header
    return false;
  };
  S.fab.tap = (j) => lost(j) ? null : j;
  await S.fab.settle();
  cRole.formedAt = S.fab.clock - ROOT_VERIFY_FIRST_MS - 1; cRole.lastVerify = 0;
  C.am._verifyRoots(S.fab.clock);
  for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
  await S.fab.settle();
  const L = S.lRole;
  const pull = [...L.sync.openPulls.values()][0];
  const want = stall ? 'pulling' : 'acked';
  if (!L.isRoot || (!stall && cRole.isRoot) || L.sync.openPulls.size !== 1 || pull?.state !== want || !pull.header) { console.log(`setup failed (${name}, C->L): L.isRoot=${L.isRoot} C.isRoot=${cRole.isRoot} pulls=${L.sync.openPulls.size} state=${pull?.state} header=${!!pull?.header}`); process.exit(1); }
  let pull2 = null;
  if (second) {
    let c2; do { c2 = await freshId(); } while ((c2 ^ topic) <= (S.lId ^ topic));
    const C2 = S.fab.addNode(c2); const c2Hex = idHex(c2);
    C2.am._announceRoot = () => {};
    C2.am.dht.lookup = async () => ({ path: [c2Hex, lHex], hops: 1, time: 1, found: false });
    const c2Role = C2.rc.become(topic, 'sub-terminal');
    await S.stamp(C2, 4);
    S.fab.clock += 1000;
    c2Role.formedAt = S.fab.clock - ROOT_VERIFY_FIRST_MS - 1; c2Role.lastVerify = 0;
    C2.am._verifyRoots(S.fab.clock);
    for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
    await S.fab.settle();
    pull2 = [...L.sync.openPulls.values()].find((x) => x !== pull) ?? null;
    if (L.sync.openPulls.size !== 2 || pull2?.state !== 'acked' || !(pull.tS < pull2.tS)) { console.log(`setup failed (${name}, C2->L): pulls=${L.sync.openPulls.size} state2=${pull2?.state} tS=${pull.tS}/${pull2?.tS}`); process.exit(1); }
  }
  if (!toW) return { S, L, C, pull, pull2 };
  // L merges into W; MERGE_CLOSED lost or delivered.
  S.fab.clock += 1000;
  S.fab.tap = (j) => {
    if (lost(j)) return null;
    if (dropClosed && j.type === 'pubsub:mergeclosed' && j.dest === S.lId) return null;
    return j;
  };
  await S.trigger();
  return { S, L, C, pull, pull2 };
}
{
  const { S, L, pull } = await overlapScene('overlap-pull-ends-first', { dropClosed: true });
  const am = S.Ln.am; const m = [...L.sync.openMerges.values()][0];
  if (L.isRoot || !L.merge?.completing || m?.state !== 'completed' || L.sync.openPulls.size !== 1 || !(pull.tS < m.completedAt)) { console.log(`setup failed (overlap 12): isRoot=${L.isRoot} completing=${!!L.merge?.completing} m=${m?.state} pulls=${L.sync.openPulls.size} tS=${pull.tS} completedAt=${m?.completedAt}`); process.exit(1); }
  const exportIds = [...m.manifest.bodies];
  am._mergeSweep(pull.tS + MERGE_RETENTION_MS + 1);                      // the PULL's bound; L's own round is not yet at its own
  const pinned = exportIds.length >= 2 && exportIds.every((id) => L.merge.pins.has(id));
  const capWas = am._cacheMax; am._cacheMax = 2;
  for (let i = 0; i < 5; i++) am._cachePush(L, { msgId: `overlap-pressure-${i}`, publishTs: S.fab.clock + 1 + i, json: '{}' });
  am._cacheMax = capWas;
  check('12 round overlap: ending the pull leaves the still-completing round\'s export pinned and held under cache pressure',
    L.sync.openPulls.size === 0 && L.merge.completing !== null && L.sync.openMerges.size === 1 && pinned && exportIds.every((id) => L.cacheIds.has(id)),
    `pulls=${L.sync.openPulls.size} completing=${!!L.merge.completing} open=${L.sync.openMerges.size} pinned=${pinned} pins=${L.merge.pins.size} held=${exportIds.map((id) => L.cacheIds.has(id))}`);
  am._mergeSweep(m.completedAt + MERGE_RETENTION_MS + 1);
  check('13 at its own bound the round closes: export in custody, no pin left',
    L.merge.completing === null && L.sync.openMerges.size === 0 && L.merge.pins.size === 0 && exportIds.every((id) => L.merge.custody.has(id) && L.cacheIds.has(id)),
    `completing=${JSON.stringify(L.merge.completing)} open=${L.sync.openMerges.size} pins=${L.merge.pins.size} custody=${exportIds.map((id) => L.merge.custody.has(id))}`);
}
{
  const { S, L, pull } = await overlapScene('overlap-closed-arrives-first', { dropClosed: false });
  if (L.isRoot || S.logs(S.Ln, 'merge-closed').length !== 1 || L.sync.openPulls.size !== 1) { console.log(`setup failed (overlap 14): isRoot=${L.isRoot} closed=${S.logs(S.Ln, 'merge-closed').length} pulls=${L.sync.openPulls.size}`); process.exit(1); }
  const own = [...(pull.baseline || []), ...pull.manifest.bodies, ...pull.manifest.dels];
  check('14 round overlap: MERGE_CLOSED for L\'s round leaves the open pull\'s baseline and manifest pinned, reserve standing',
    L.merge.completing === null && L.sync.openMerges.size === 0 && own.length >= 2 && own.every((id) => L.merge.pins.has(id)) && L.merge.reserve !== null,
    `completing=${JSON.stringify(L.merge.completing)} open=${L.sync.openMerges.size} own=${own.length} pinned=${own.map((id) => L.merge.pins.has(id))} reserve=${JSON.stringify(L.merge.reserve)}`);
}

{
  const { S, L, pull } = await overlapScene('overlap-pull-times-out', { dropClosed: true, stall: true });
  const am = S.Ln.am; const m = [...L.sync.openMerges.values()].find((x) => x.state === 'completed');
  if (L.isRoot || !L.merge?.completing || !m || L.sync.openPulls.size !== 1 || pull.state !== 'pulling') { console.log(`setup failed (overlap 15): isRoot=${L.isRoot} completing=${!!L.merge?.completing} m=${m?.state} pulls=${L.sync.openPulls.size} state=${pull.state}`); process.exit(1); }
  const exportIds = [...m.manifest.bodies];
  const tOut = pull.startedAt + MERGE_TIMEOUT_MS + 1;
  const before = tOut < m.completedAt + MERGE_RETENTION_MS;                 // the pull ends first
  am._mergeSweep(tOut);
  const timedOut = L.sync.openPulls.size === 0 && S.logs(S.Ln, 'merge-pull-timeout').length === 1 && L.merge.completing !== null;
  const pinned = exportIds.length >= 1 && exportIds.every((id) => L.merge.pins.has(id));
  const capWas = am._cacheMax; am._cacheMax = 2;
  for (let i = 0; i < 5; i++) am._cachePush(L, { msgId: `timeout-pressure-${i}`, publishTs: S.fab.clock + 1 + i, json: '{}' });
  am._cacheMax = capWas;
  const held = exportIds.every((id) => L.cacheIds.has(id));
  am._mergeSweep(m.completedAt + MERGE_RETENTION_MS + 1);
  const inCustody = L.merge.completing === null && L.merge.pins.size === 0 && exportIds.every((id) => L.merge.custody.has(id));
  const newest = Math.max(...L.cache.filter((c) => exportIds.includes(c.msgId)).map((c) => c.publishTs));
  am._expireTombstones(L, newest + TTL_MS + 1); am._expireCache(L, newest + TTL_MS + 1);
  const gone = exportIds.every((id) => !L.cacheIds.has(id)) && L.merge.custody.size === 0;
  check('15 a pull that times out while the role is completing: export stays pinned and held, then custody at the bound, then gone at TTL',
    before && timedOut && pinned && held && inCustody && gone,
    `before=${before} timedOut=${timedOut} pinned=${pinned} held=${held} inCustody=${inCustody} gone=${gone} custody=${L.merge.custody.size}`);
}
{
  const { S, L, pull, pull2 } = await overlapScene('overlap-two-pulls', { dropClosed: false, second: true, toW: false });
  const am = S.Ln.am;
  am._mergeSweep(pull.tS + MERGE_RETENTION_MS + 1);                         // the first pull's bound only
  const own2 = [...(pull2.baseline || []), ...pull2.manifest.bodies, ...pull2.manifest.dels];
  check('16 two W-side pulls: ending the first leaves the second\'s baseline and manifest pinned, reserve standing',
    L.sync.openPulls.size === 1 && [...L.sync.openPulls.values()][0] === pull2 && own2.length >= 3 && own2.every((id) => L.merge.pins.has(id)) && L.merge.reserve !== null,
    `pulls=${L.sync.openPulls.size} own2=${own2.length} pinned=${own2.map((id) => L.merge.pins.has(id))} reserve=${JSON.stringify(L.merge.reserve)}`);
}

console.log(`\nfence_merge_candidate_after_completion: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
