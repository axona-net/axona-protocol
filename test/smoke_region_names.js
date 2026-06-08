// =====================================================================
// smoke_region_names.js — every region has ONE well-formed name, names map
// to codes (canonical code for a multi-cell area), and lookups round-trip.
//
// Run: node test/smoke_region_names.js
// =====================================================================

import {
  REGION_NAMES, regionNames, regionName, regionCode, resolveRegion, regionNameForLatLng,
} from '../src/utils/region-names.js';
import { S2_CELL_COUNT, geoCellId } from '../src/utils/s2.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

console.log('Axona region-names smoke (one name per region)');

console.log('\n── shape: 192 single names, well-formed ──');
check(`exactly ${S2_CELL_COUNT} regions`, REGION_NAMES.length === S2_CELL_COUNT);
check('every entry is a string', REGION_NAMES.every(s => typeof s === 'string'));
const fmt = /^[a-z0-9_]{1,8}$/;
const badFmt = REGION_NAMES.filter(n => !fmt.test(n));
check('all names match /^[a-z0-9_]{1,8}$/', badFmt.length === 0);
if (badFmt.length) console.log('     offenders:', badFmt.slice(0, 12));
check('list frozen', Object.isFrozen(REGION_NAMES));

console.log('\n── regionName / regionNames ──');
check('regionName(code) === REGION_NAMES[code]', regionName(0x89) === REGION_NAMES[0x89] && regionName(0x89) === 'useast');
check('regionNames(code) shim → one-element array', (() => {
  const a = regionNames(0x89); return Array.isArray(a) && a.length === 1 && a[0] === 'useast';
})());

console.log('\n── name → canonical code, and round-trip ──');
let rt = true;
for (let code = 0; code < S2_CELL_COUNT; code++) {
  // the code reported for a region's own name has that same name
  if (regionName(regionCode(REGION_NAMES[code])) !== REGION_NAMES[code]) { rt = false; break; }
}
check('regionName(regionCode(name)) === name  ∀ code', rt);
check('regionCode returns the canonical (lowest) code for a multi-cell name',
  regionCode('centrlam') === Math.min(...REGION_NAMES.flatMap((n, c) => n === 'centrlam' ? [c] : [])));

console.log('\n── one-name fix: N. America (the reported confusions) ──');
const expectOne = { 0x89: 'useast', 0x80: 'uswest', 0x81: 'mexico', 0x87: 'uscentlw', 0x88: 'uscentle', 0x84: 'centrlam', 0x8f: 'centrlam' };
for (const [code, name] of Object.entries(expectOne))
  check(`0x${(+code).toString(16)} → "${name}"`, REGION_NAMES[+code] === name);

console.log('\n── invalid / reserved ──');
check('regionName(192) === null', regionName(192) === null);
check('regionName(255) === null', regionName(255) === null);
check('regionCode("nope") === null', regionCode('nope') === null);

console.log('\n── resolveRegion: name OR numeric ──');
check('resolveRegion("uswest") === regionCode("uswest")', resolveRegion('uswest') === regionCode('uswest'));
check('resolveRegion("USEAST") case-insensitive', resolveRegion('USEAST') === 0x89);
check('resolveRegion("0x68") === 0x68', resolveRegion('0x68') === 0x68);
check('resolveRegion("137") === 137', resolveRegion('137') === 137);
check('resolveRegion(137) === 137', resolveRegion(137) === 137);
check('resolveRegion("pac_68") === 0x68', resolveRegion('pac_68') === 0x68);
check('resolveRegion(192) === null', resolveRegion(192) === null);

console.log('\n── regionNameForLatLng matches the code\'s name ──');
check('Virginia (38,-77) → useast', regionNameForLatLng(38, -77) === 'useast');
let llOk = true;
for (const [lat, lng] of [[37, -122], [51.5, -0.1], [35.7, 139.7], [-33.9, 151.2], [19, 73]]) {
  if (regionNameForLatLng(lat, lng) !== REGION_NAMES[geoCellId(lat, lng, 8)]) { llOk = false; break; }
}
check('regionNameForLatLng(lat,lng) === REGION_NAMES[geoCellId(...)]', llOk);

console.log('\n── ocean naming: open water uses <oce3>_<hex> ──');
check('0x68 is pac_68', REGION_NAMES[0x68] === 'pac_68');
check('ocean names embed their own code', resolveRegion('atl_01') === 0x01);

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
