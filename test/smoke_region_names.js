// =====================================================================
// smoke_region_names.js — the 192 canonical region names are well-formed,
// unique, a bijection with the codes, and resolve as prefixes.
//
// Run: node test/smoke_region_names.js
// =====================================================================

import {
  REGION_NAMES, regionName, regionCode, resolveRegion, regionNameForLatLng,
} from '../src/utils/region-names.js';
import { S2_CELL_COUNT, geoCellId } from '../src/utils/s2.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

console.log('Axona region-names smoke');

console.log('\n── shape: 192 names, well-formed, unique ──');
check(`exactly ${S2_CELL_COUNT} names`, REGION_NAMES.length === S2_CELL_COUNT);
const fmt = /^[a-z0-9]{1,8}$/;
const badFmt = REGION_NAMES.filter(n => !fmt.test(n));
check('all names match /^[a-z0-9]{1,8}$/ (lowercase, no spaces, ≤8)', badFmt.length === 0);
if (badFmt.length) console.log('     offenders:', badFmt.slice(0, 10));
check('all 192 names unique', new Set(REGION_NAMES).size === S2_CELL_COUNT);
check('list is frozen', Object.isFrozen(REGION_NAMES));

console.log('\n── bijection: name ↔ code round-trips for every code ──');
let rtOk = true;
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const name = regionName(code);
  if (name !== REGION_NAMES[code] || regionCode(name) !== code) { rtOk = false; break; }
}
check('regionCode(regionName(code)) === code  ∀ code', rtOk);

console.log('\n── invalid / reserved codes ──');
check('regionName(192) === null (reserved)', regionName(192) === null);
check('regionName(255) === null (reserved)', regionName(255) === null);
check('regionName(-1) === null', regionName(-1) === null);
check('regionCode("nope") === null', regionCode('nope') === null);
check('regionCode(123) === null (non-string)', regionCode(123) === null);

console.log('\n── case-insensitivity ──');
check('regionCode("USEAST") === regionCode("useast")',
  regionCode('USEAST') === regionCode('useast') && regionCode('useast') !== null);

console.log('\n── resolveRegion: name OR numeric code ──');
const useast = regionCode('useast');
check('resolveRegion("useast") === code', resolveRegion('useast') === useast);
check('resolveRegion("0x89") === 0x89', resolveRegion('0x89') === 0x89);
check('resolveRegion("137") === 137', resolveRegion('137') === 137);
check('resolveRegion(137) === 137', resolveRegion(137) === 137);
check('resolveRegion(192) === null (reserved)', resolveRegion(192) === null);
check('resolveRegion("garbage") === null', resolveRegion('garbage') === null);

console.log('\n── known anchors map to expected names ──');
const known = [
  ['useast', 38.0, -77.0],   // Virginia
  ['uswest', 37.0, -122.0],  // California
  ['britain', 51.5, -0.1],   // London
  ['japan', 35.7, 139.7],    // Tokyo
  ['auseast', -33.9, 151.2], // Sydney
  ['saopau', -23.5, -46.6],  // São Paulo
];
for (const [expected, lat, lng] of known) {
  const got = regionNameForLatLng(lat, lng);
  check(`(${lat},${lng}) → "${expected}" (got "${got}")`, got === expected);
}
check('regionNameForLatLng === REGION_NAMES[geoCellId]',
  regionNameForLatLng(38, -77) === REGION_NAMES[geoCellId(38, -77, 8)]);

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
