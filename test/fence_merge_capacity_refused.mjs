#!/usr/bin/env node
// =====================================================================
// fence_merge_capacity_refused.mjs — dual-root packet 4(d)
// capacity_refused + growth_offer_to_cut + full_w_unique_baseline
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// ADMISSION IS SIZED AT THE CUT, FAIL CLOSED, BASELINE INTACT. W admits a
// round only if its baseline (every entry it holds now) + the header's
// bodies + MERGE_WRITE_RESERVE_ENTRIES fit under its cap; a refusal
// answers REPLAYACK refused-capacity on the header, before any data, L
// clears its gate and keeps custody, and W evicts nothing of its own to
// admit. The OFFER's count is an estimate: L keeps stamping until the
// bind, and the authoritative size is the header's.
//
// W's cap here: replayCacheSize = 70 (reserve is 64), so W admits only
// when baseline + bodies ≤ 6. Checks: (A) two individually valid caches
// (W 4, L 3) whose union exceeds the cap: refused on the header, L root
// with custody, gate cleared, W's four intact; (B) growth from OFFER to
// cut: W 2, L 3 at the offer (fits: 5), L stamps two more before the bind
// (7 > 6): refused on the header although the offer fit; (C) W full with
// unique bodies (6) refuses a one-body export with its baseline intact;
// (D) a tombstone-only export is refused the same way when the baseline
// alone fails (dels count against nothing here; the rule is bodies and
// bytes); (E) control: W 2, L 3 → admitted, L demotes.
//
// Mutant (red): admit on the OFFER's count (skip the header check) → A, B,
// C fail; evict baseline to admit → C fails.
// Run: node test/fence_merge_capacity_refused.mjs
// =====================================================================
import { MERGE_WRITE_RESERVE_ENTRIES } from '../src/pubsub/constants.js';
import { splitScene } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const CAP = MERGE_WRITE_RESERVE_ENTRIES + 6;

console.log('fence_merge_capacity_refused: 4(d) — admission sized at the cut, fail closed, baseline intact');

async function scene(name, wN, lN, { grow = 0 } = {}) {
  const S = await splitScene(name, { wOpts: { replayCacheSize: CAP } });
  const w = []; for (let k = 0; k < wN; k++) w.push(await S.stamp(S.W, 100 + k));
  const l = []; for (let k = 0; k < lN; k++) l.push(await S.stamp(S.Ln, 200 + k));
  if (grow) {
    // stamp at L between the OFFER and the bind: intercept the first PULLUP under corr and stamp before it is delivered
    let done = false;
    S.fab.tap = (j) => { if (!done && j.type === 'pubsub:pullup' && typeof j.payload.corr === 'string') { done = true; S.fab.queue.push({ ...j, meta: j.meta }); for (let k = 0; k < grow; k++) S.lRole.cache.push({ msgId: `grown-${k}`, publishTs: S.fab.clock + k + 1, json: '{"grown":true}', seq: 900 + k, bytes: 100 }); for (let k = 0; k < grow; k++) S.lRole.cacheIds.add(`grown-${k}`); return null; } return j; };
  }
  await S.trigger();
  return { S, w, l, refused: S.logs(S.W, 'refused-capacity')[0]?.ctx, admitted: S.logs(S.W, 'merge-admitted')[0]?.ctx, lRefused: S.logs(S.Ln, 'merge-refused')[0]?.ctx };
}
{ // A
  const { S, w, refused, lRefused } = await scene('capacity-A', 4, 3);
  check('A union exceeds the cap: refused-capacity on the header, L root with custody, gate cleared, W\'s four intact', !!refused && refused.baselineN === 4 && refused.bodies === 3 && !!lRefused && lRefused.why === 'refused-capacity' && S.lRole.isRoot === true && S.lRole.cache.length === 3 && S.lRole.merge.gate === null && w.every((id) => S.wRole.cacheIds.has(id)) && S.wRole.cache.length === 4, JSON.stringify({ refused, lRefused, lRoot: S.lRole.isRoot }));
}
{ // B growth
  const { S, refused, admitted } = await scene('capacity-B', 2, 3, { grow: 2 });
  check('B the export grew from 3 at the offer to 5 at the cut: refused on the header (2 + 5 > 6) although the offer fit', !!refused && refused.bodies === 5 && refused.baselineN === 2 && !admitted && S.lRole.isRoot === true, JSON.stringify({ refused, admitted }));
}
{ // C full W unique baseline
  const { S, w, refused } = await scene('capacity-C', 6, 1);
  check('C W full with six unique bodies refuses a one-body export with its baseline intact', !!refused && refused.baselineN === 6 && refused.bodies === 1 && w.every((id) => S.wRole.cacheIds.has(id)) && S.wRole.cache.length === 6 && S.lRole.isRoot === true);
}
{ // D tombstone-only export at a full W
  const S = await splitScene('capacity-D', { wOpts: { replayCacheSize: CAP } });
  for (let k = 0; k < 7; k++) await S.stamp(S.W, 300 + k);        // 7 > 6: the baseline alone fails
  const y = await S.stamp(S.Ln, 400); await S.kill(S.Ln, y);
  await S.trigger();
  const refused = S.logs(S.W, 'refused-capacity')[0]?.ctx;
  check('D a tombstone-only export is refused when the baseline alone exceeds the cap; L stays root with its tombstone', !!refused && refused.bodies === 0 && S.lRole.isRoot === true && S.lRole.tombstones.has(y), JSON.stringify(refused));
}
{ // E control
  const { S, l, admitted, refused } = await scene('capacity-E', 2, 3);
  check('E control: 2 + 3 fits → admitted, L demotes, W holds five', !!admitted && !refused && S.lRole.isRoot === false && l.every((id) => S.wRole.cacheIds.has(id)) && S.wRole.cache.length === 5, JSON.stringify({ admitted, refused }));
}

console.log(`\nfence_merge_capacity_refused: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
