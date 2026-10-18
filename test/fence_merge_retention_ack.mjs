#!/usr/bin/env node
// =====================================================================
// fence_merge_retention_ack.mjs — dual-root packet 4(d) retention_ack +
// manifest_dels (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// COUNTS OF INGESTS DO NOT AUTHORIZE CUSTODY RELEASE; RETENTION AT ACK
// TIME DOES. W computes retained and delsActive against the MANIFEST from
// its cacheIds and active tombstones at the moment it acks. A manifest body
// W no longer holds, or a manifest tombstone W failed to keep, makes the
// ack short, and L keeps custody.
//
// Scene: L holds b1, b2 and the tombstone of y (manifest: 2 bodies, 1 del).
// Checks: (A) W ingests every frame and loses one manifest body before the
// ack (deleted from its cache between the terminal frame and the ack):
// the ack is short (retained = bodies − 1), L logs merge-ack-short, aborts
// the round and stays root with its cache intact; (B) W loses a manifest
// tombstone before the ack: short (delsActive 0), L stays root; (C)
// control: nothing lost → full, L demotes.
//
// Mutant (red): ack full on held === sent (the ingest tally) instead of
// the retention count → A and B fail.
// Run: node test/fence_merge_retention_ack.mjs
// =====================================================================
import { splitScene } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_retention_ack: 4(d) — the ack counts retention at ack time, not ingests');

async function scene(name, lose) {
  const S = await splitScene(name);
  await S.stamp(S.W, 1);
  const b1 = await S.stamp(S.Ln, 2), b2 = await S.stamp(S.Ln, 3), y = await S.stamp(S.Ln, 4); await S.kill(S.Ln, y);
  // between the terminal frame's ingest and the ack: W loses something
  const orig = S.W.am._mergeManifestHash.bind(S.W.am);
  S.W.am._mergeManifestHash = async (bodies, dels) => { lose(S, { b1, b2, y }); return orig(bodies, dels); };
  await S.trigger();
  return { S, b1, b2, y, acked: S.logs(S.W, 'merge-acked')[0]?.ctx, short: S.logs(S.Ln, 'merge-ack-short')[0]?.ctx };
}
{ // A — a manifest body lost
  const { S, b1, acked, short } = await scene('retention-ack-body', (S, { b1 }) => {
    const i = S.wRole.cache.findIndex((c) => c.msgId === b1); if (i >= 0) { S.wRole.cacheBytes -= S.wRole.cache[i].bytes; S.wRole.cache.splice(i, 1); S.wRole.cacheIds.delete(b1); }
  });
  check('A the ack is short (retained = bodies − 1); L logs merge-ack-short, aborts, stays root with its cache intact', !!acked && acked.full === false && acked.retained === 1 && acked.bodies === 2 && !!short && S.lRole.isRoot === true && S.lRole.cache.length === 2 && S.lRole.merge.gate === null && S.logs(S.Ln, 'merge-step5').length === 0, JSON.stringify({ acked, short, lRoot: S.lRole.isRoot }));
}
{ // B — a manifest tombstone lost
  const { S, acked, short } = await scene('retention-ack-del', (S, { y }) => { S.wRole.tombstones.delete(y); });
  check('B a manifest tombstone W failed to keep: short (delsActive 0 of 1), L stays root', !!acked && acked.full === false && acked.delsActive === 0 && acked.dels === 1 && !!short && S.lRole.isRoot === true && S.logs(S.Ln, 'merge-step5').length === 0, JSON.stringify({ acked, short }));
}
{ // C — control
  const { S, acked } = await scene('retention-ack-control', () => {});
  check('C control: nothing lost → full, L demotes', !!acked && acked.full === true && acked.retained === 2 && acked.delsActive === 1 && S.lRole.isRoot === false, JSON.stringify(acked));
}

console.log(`\nfence_merge_retention_ack: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
