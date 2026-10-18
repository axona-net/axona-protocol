#!/usr/bin/env node
// =====================================================================
// fence_merge_lease_cancellation.mjs — dual-root packet 4(d) "Request
// timeout and ack validity, at L, anchored at the bind" (axona-docs
// 14a6bd8; David "build it" 2026-10-17).
//
// L records tB on its monotonic clock at the bind. A terminal ack is
// discarded as merge-ack-expired if L's monotonic clock reads past tB + A
// when L would act on it; a wall-clock jump against the monotonic clock by
// more than E discards it too. L keeps custody in both. W releases at its
// own tS + R with the record failed.
//
// Checks: (A) the ack delivered after tB + A (monotonic): merge-ack-expired,
// L root with custody, the gate cleared, no step 5; W's record fails at
// tS + R (retention-expired) with pins released; (B) a wall-clock jump
// past E between the bind and the ack, monotonic inside A: discarded the
// same way; (C) control: inside both → step 5.
//
// Mutant (red): act on a late ack (drop the lease check) → A and B fail.
// Run: node test/fence_merge_lease_cancellation.mjs
// =====================================================================
import { MERGE_TIMEOUT_MS, MERGE_RETENTION_MS, FUTURE_TOLERANCE_MS } from '../src/pubsub/constants.js';
import { splitScene } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_lease_cancellation: 4(d) — a late ack is discarded; custody stays at L; W fails at tS + R');

async function scene(name, delay) {
  const S = await splitScene(name);
  await S.stamp(S.W, 1); await S.stamp(S.Ln, 2);
  const mono = { t: 1_000_000 }; S.Ln.am._mono = () => mono.t;
  let held = [];
  S.fab.tap = (j) => { if (j.type === 'pubsub:replayack') { held.push(j); return null; } return j; };
  await S.trigger();
  delay(S, mono);
  S.fab.tap = null; for (const j of held) S.fab.queue.push(j); held = [];
  await S.fab.settle();
  return { S, mono };
}
{ // A
  const { S } = await scene('lease-A', (S, mono) => { mono.t += MERGE_TIMEOUT_MS + 1; S.fab.clock += MERGE_TIMEOUT_MS + 1; });
  check('A an ack after tB + A (monotonic): merge-ack-expired, L root with custody, gate cleared, no step 5', S.logs(S.Ln, 'merge-ack-expired').length === 1 && S.lRole.isRoot === true && S.lRole.cache.length === 1 && S.lRole.merge.gate === null && S.logs(S.Ln, 'merge-step5').length === 0, JSON.stringify(S.Ln.logs.map((l) => l.ev).slice(-4)));
  // W: the record fails at tS + R
  S.fab.clock += MERGE_RETENTION_MS + 1; S.W.am._mergeSweep(S.W.am._now());
  check('A W releases its pins at tS + R with the record failed (retention-expired)', S.logs(S.W, 'merge-retention-expired').length === 1 && S.wRole.merge.pins.size === 0 && S.wRole.sync.openPulls.size === 0 && S.wRole.isRoot === true);
}
{ // B
  const { S } = await scene('lease-B', (S, mono) => { mono.t += 1_000; S.fab.clock += FUTURE_TOLERANCE_MS + 2_000; });   // the wall clock jumped
  check('B a wall-clock jump past E inside A: discarded as merge-ack-expired, L root with custody', S.logs(S.Ln, 'merge-ack-expired').length === 1 && S.lRole.isRoot === true && S.logs(S.Ln, 'merge-step5').length === 0);
}
{ // C
  const { S } = await scene('lease-C', (S, mono) => { mono.t += 1_000; S.fab.clock += 1_000; });
  check('C control: inside the lease → step 5, L demoted', S.logs(S.Ln, 'merge-step5').length === 1 && S.lRole.isRoot === false);
}

console.log(`\nfence_merge_lease_cancellation: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
