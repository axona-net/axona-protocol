// =====================================================================
// smoke_region_names.js — every region has two well-formed names (its two
// halves), names map unambiguously to codes, and the half-aware lookups work.
//
// Run: node test/smoke_region_names.js
// =====================================================================

import {
  REGION_NAMES, regionNames, regionName, regionCode, resolveRegion, regionNameForLatLng,
} from '../src/utils/region-names.js';
import { S2_CELL_COUNT, geoCellId, geoCellHalf } from '../src/utils/s2.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

console.log('Axona region-names smoke (two names per region)');

console.log('\n── shape: 192 pairs, well-formed ──');
check(`exactly ${S2_CELL_COUNT} regions`, REGION_NAMES.length === S2_CELL_COUNT);
check('every entry is a [a, b] pair of strings',
  REGION_NAMES.every(p => Array.isArray(p) && p.length === 2 && p.every(s => typeof s === 'string')));
const fmt = /^[a-z0-9_]{1,8}$/;
const flat = REGION_NAMES.flat();
const badFmt = flat.filter(n => !fmt.test(n));
check('all names match /^[a-z0-9_]{1,8}$/', badFmt.length === 0);
if (badFmt.length) console.log('     offenders:', badFmt.slice(0, 12));
check('list + pairs frozen', Object.isFrozen(REGION_NAMES) && Object.isFrozen(REGION_NAMES[0]));

console.log('\n── name → code is unambiguous (no name in two different codes) ──');
const owners = new Map();
REGION_NAMES.forEach((pair, code) => { for (const n of pair) {
  if (!owners.has(n)) owners.set(n, new Set());
  owners.get(n).add(code);
}});
const ambiguous = [...owners].filter(([, codes]) => codes.size > 1);
check('no name maps to multiple codes', ambiguous.length === 0);
if (ambiguous.length) console.log('     ambiguous:', ambiguous.slice(0, 8).map(([n]) => n));

console.log('\n── regionNames / regionCode round-trip for every code ──');
let rt = true;
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const pair = regionNames(code);
  if (!pair || pair !== REGION_NAMES[code]) { rt = false; break; }
  for (const n of pair) if (regionCode(n) !== code) { rt = false; break; }
}
check('regionCode(eitherName) === code  ∀ code', rt);

console.log('\n── regionName: default half vs half-aware ──');
check('regionName(code) === half 0', regionName(0x89) === REGION_NAMES[0x89][0]);
check('regionName(code,lat,lng) picks the point\'s half', (() => {
  // a coordinate in Virginia → its half of 0x89
  const code = geoCellId(38, -77, 8), half = geoCellHalf(38, -77);
  return regionName(code, 38, -77) === REGION_NAMES[code][half];
})());

console.log('\n── invalid / reserved ──');
check('regionNames(192) === null', regionNames(192) === null);
check('regionName(255) === null', regionName(255) === null);
check('regionCode("nope") === null', regionCode('nope') === null);

console.log('\n── resolveRegion: name OR numeric ──');
check('resolveRegion("uswest") → a code whose pair has uswest',
  REGION_NAMES[resolveRegion('uswest')]?.includes('uswest'));
check('resolveRegion("USEAST") case-insensitive', resolveRegion('USEAST') === resolveRegion('useast'));
check('resolveRegion("0x68") === 0x68', resolveRegion('0x68') === 0x68);
check('resolveRegion("137") === 137', resolveRegion('137') === 137);
check('resolveRegion(137) === 137', resolveRegion(137) === 137);
check('resolveRegion("pac_68") === 0x68', resolveRegion('pac_68') === 0x68);
check('resolveRegion(192) === null', resolveRegion(192) === null);

console.log('\n── known locations resolve to the expected half-name ──');
const known = [
  ['useast', 38.0, -77.0], ['uswest', 37.0, -122.0], ['uk', 51.5, -0.1],
  ['japan', 35.7, 139.7], ['ozeast', -33.9, 151.2],
];
for (const [expected, lat, lng] of known) {
  const got = regionNameForLatLng(lat, lng);   // may carry a compass suffix
  check(`(${lat},${lng}) → "${expected}*" (got "${got}")`, got.startsWith(expected));
}
check('regionNameForLatLng === REGION_NAMES[id][half]', (() => {
  const id = geoCellId(19, 73, 8);
  return regionNameForLatLng(19, 73) === REGION_NAMES[id][geoCellHalf(19, 73)];
})());

console.log('\n── ocean naming: open water uses <oce3>_<hex> ──');
check('0x68 is pac_68', REGION_NAMES[0x68][0] === 'pac_68');
check('ocean names embed their own code', resolveRegion('atl_01') === 0x01);

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
