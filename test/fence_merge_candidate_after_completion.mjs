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
// Mutants (each red): drop the same-winner branch in _mergeCandidate → 3,4,5;
// drop the clear in _mergeStep5 AND _onMergeClosed AND the hold expiry AND
// _set → 1 (any one of the four left in keeps 1 green on this path, so 1 is
// pinned by removing them together; the branch is the load-bearing fix).
// Run: node test/fence_merge_candidate_after_completion.mjs
// =====================================================================
import { splitScene, idHex } from './lib/mergeFixture.mjs';

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

console.log(`\nfence_merge_candidate_after_completion: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
