#!/usr/bin/env node
// =====================================================================
// fence_return_route_life.mjs — "Not attached until something has come down"
// v0.2, part 3. Manager option returnRouteTtlMs (env RETURN_ROUTE_TTL_MS).
//
// A breadcrumb lives 60 s and a settled subscriber renews every 60 s. Between
// the expiry and the next renewal, and for as long as that renewal runs late,
// the root has no way back. This is NOT what made the tab of 2026-11-24 deaf
// (its renewals were answered in the step that recorded the breadcrumb). It
// is a hole of its own: a message published late in the gap.
//
//   L1 default: the life is 60 s, equal to RENEW_MS (the hole, pinned).
//   L2 default: a breadcrumb is there at 59.9 s and gone at 60 s.
//   L3 set to 3 x RENEW_MS: there at 60 s, at one late renewal (61 s) and at
//      179.9 s; gone at 180 s.
//   L4 a renewal resets the life.
//   L5 a refused value (0, NaN, negative, 10 hours) leaves the default.
//   L6 the health census says which life is in force.
// Run: node test/fence_return_route_life.mjs
// =====================================================================
import { Fabric, freshId, idHex } from './lib/mergeFixture.mjs';
import { RENEW_MS } from '../src/pubsub/constants.js';

const EXPECT = 11;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

async function scene(opts) {
  const fab = new Fabric();
  const R = fab.addNode(await freshId(), { returnRoute: true, ...opts });
  const sBig = await freshId(); const s = idHex(sBig).toLowerCase();
  const note = () => R.am._noteReturn({ path: [s] }, [s]);
  const has = () => Array.isArray(R.am._returnFor(sBig));
  return { fab, R, note, has };
}

console.log('fence_return_route_life: a breadcrumb that outlives the renewal gap');
{
  const X = await scene({});
  check('L1 default: the life equals the settled renewal gap', X.R.am._returnRouteTtlMs === 60_000 && RENEW_MS === 60_000, String(X.R.am._returnRouteTtlMs));
  X.note(); X.fab.clock += 59_900;
  check('L2a default: there at 59.9 s', X.has());
  X.fab.clock += 100;
  check('L2b default: gone at 60 s, the moment the next renewal is due', !X.has());
}
{
  const X = await scene({ returnRouteTtlMs: 3 * RENEW_MS });
  X.note(); X.fab.clock += 60_000;
  check('L3a set: there at 60 s', X.has());
  X.fab.clock += 1_000;
  check('L3b set: there when the renewal runs a second late', X.has());
  X.fab.clock += 118_900;
  check('L3c set: there at 179.9 s', X.has());
  X.fab.clock += 100;
  check('L3d set: gone at 180 s', !X.has());
  X.note(); X.fab.clock += 100_000; X.note(); X.fab.clock += 179_000;
  check('L4 a renewal resets the life', X.has());
}
{
  const bad = [0, NaN, -5, 36_000_000, 'soon'];
  const got = [];
  for (const v of bad) got.push((await scene({ returnRouteTtlMs: v })).R.am._returnRouteTtlMs);
  check('L5 a refused value leaves the default', got.every((g) => g === 60_000), JSON.stringify(got));
}
{
  const { AxonaPeer } = await import('../src/dht/AxonaPeer.js');
  const fake = (ttl) => AxonaPeer.prototype.returnRouteStats.call({ _axonaManager: { _returnRoute: true, _retRoutes: new Map(), _returnRouteTtlMs: ttl, _returnRouteStats: {} }, _returnRouteStats: {} });
  check('L6a the census carries the life in force', fake(180_000).pubsub.ttlMs === 180_000, JSON.stringify(fake(180_000).pubsub));
  check('L6b and null where the manager has none', fake(undefined).pubsub.ttlMs === null);
}
console.log(`\nfence_return_route_life: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
