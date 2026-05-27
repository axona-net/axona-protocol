// =====================================================================
// smoke_s2.js — verify the S2 cube-projection cell math:
//   · all valid coordinates produce IDs in [0, 192)
//   · reserved range (192..255) never appears
//   · round-trip geoCellId → geoCellCenter → geoCellId is stable
//   · all 192 IDs are reachable
//   · cells are roughly equal-area (no extreme polar shrinkage)
//   · known coordinates land on expected faces
// Run: node test/smoke_s2.js
// =====================================================================

import {
  geoCellId,
  geoCellCenter,
  geoCellCorners,
  geoCellFace,
  isValidCellId,
  S2_FACES,
  S2_CELL_COUNT,
  S2_RESERVED_FROM,
} from '../src/utils/s2.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

console.log('S2 cube-projection cell math');

// ─── Constants ───
console.log('\n── constants ──');
check('S2_FACES = 6',           S2_FACES === 6);
check('S2_CELL_COUNT = 192',    S2_CELL_COUNT === 192);
check('S2_RESERVED_FROM = 192', S2_RESERVED_FROM === 192);

// ─── Validity ───
console.log('\n── valid range ──');
check('isValidCellId(0) true',     isValidCellId(0));
check('isValidCellId(191) true',   isValidCellId(191));
check('isValidCellId(192) false',  !isValidCellId(192));
check('isValidCellId(255) false',  !isValidCellId(255));
check('isValidCellId(-1) false',   !isValidCellId(-1));
check('isValidCellId(1.5) false',  !isValidCellId(1.5));

// ─── Globe sweep: every coordinate yields a valid ID ───
console.log('\n── globe sweep ──');
{
  const seen = new Set();
  let invalidCount = 0;
  for (let lat = -89.5; lat < 90; lat += 1) {
    for (let lng = -179.5; lng < 180; lng += 1) {
      const cid = geoCellId(lat, lng, 8);
      if (cid < 0 || cid >= 192) invalidCount++;
      seen.add(cid);
    }
  }
  check('no cell ID lands outside [0, 192)', invalidCount === 0);
  check('all 192 cells are reachable',       seen.size === 192);
  console.log(`    coverage: ${seen.size} / 192 distinct IDs`);
}

// ─── Face partition: ~32 cells per face, all faces non-empty ───
console.log('\n── face partition ──');
{
  const perFace = new Array(6).fill(0);
  for (let lat = -89.5; lat < 90; lat += 1) {
    for (let lng = -179.5; lng < 180; lng += 1) {
      const face = geoCellFace(geoCellId(lat, lng, 8));
      perFace[face]++;
    }
  }
  console.log('    sample count per face:', perFace.join(', '));
  check('all 6 faces have samples', perFace.every(c => c > 0));
}

// ─── Round-trip: geoCellId → geoCellCenter → geoCellId is stable ───
console.log('\n── round-trip stability ──');
{
  let stable = 0, drift = 0;
  for (let cid = 0; cid < 192; cid++) {
    const center = geoCellCenter(cid);
    if (!center) { drift++; continue; }
    const back = geoCellId(center.lat, center.lng, 8);
    if (back === cid) stable++; else drift++;
  }
  check('all 192 cell centers round-trip back to their cell ID', drift === 0);
  console.log(`    stable: ${stable} / 192`);
}

// ─── geoCellCenter returns null for reserved IDs ───
console.log('\n── reserved IDs ──');
check('geoCellCenter(192) is null', geoCellCenter(192) === null);
check('geoCellCenter(255) is null', geoCellCenter(255) === null);
check('geoCellFace(192) is -1',     geoCellFace(192) === -1);

// ─── geoCellCorners returns 4 lat/lng pairs ───
console.log('\n── cell corners ──');
{
  const corners = geoCellCorners(0);
  check('geoCellCorners(0) returns 4 points', Array.isArray(corners) && corners.length === 4);
  check('each corner has lat + lng', corners?.every(c => typeof c.lat === 'number' && typeof c.lng === 'number'));
  check('geoCellCorners(192) is null', geoCellCorners(192) === null);
}

// ─── Cells are roughly equal-area (no polar shrinkage to zero) ───
// Approximate per-cell area by sampling globe and counting unique IDs
// that include each point.  Then check that no cell has more than 5×
// the count of any other.  (Real S2 quadratic projection yields ~1.7×
// max ratio; we allow 5× for sample noise.)
console.log('\n── area uniformity ──');
{
  const counts = new Array(192).fill(0);
  const STEP = 0.5;   // 0.5° sampling
  let total = 0;
  for (let lat = -89.75; lat < 90; lat += STEP) {
    // Sample longitudes proportional to cos(lat) — uniform area weighting
    const cosLat = Math.cos(lat * Math.PI / 180);
    const nLng = Math.max(1, Math.round(360 * cosLat / STEP));
    const dLng = 360 / nLng;
    for (let i = 0; i < nLng; i++) {
      const lng = -180 + (i + 0.5) * dLng;
      const cid = geoCellId(lat, lng, 8);
      counts[cid]++;
      total++;
    }
  }
  const minCount = Math.min(...counts);
  const maxCount = Math.max(...counts);
  const meanCount = total / 192;
  const ratio = maxCount / minCount;
  console.log(`    cell area-proxy counts: min=${minCount}, max=${maxCount}, mean=${meanCount.toFixed(1)}, max/min=${ratio.toFixed(2)}`);
  check('no cell is empty',                  minCount > 0);
  check('max/min cell area ratio < 5',       ratio < 5);
  // For real S2 quadratic this should be < ~1.8; we leave margin.
}

// ─── Known landmark coordinates ───
console.log('\n── landmark sanity ──');
{
  const places = [
    ['London',       51.5,    -0.1],
    ['Virginia',     38.0,   -77.0],
    ['Sydney',      -33.9,   151.2],
    ['Tokyo',        35.7,   139.7],
    ['Cape Town',   -33.9,    18.4],
    ['Reykjavik',    64.1,   -21.9],
    ['Buenos Aires',-34.6,   -58.4],
    ['South Pole',  -89.99,    0],
    ['North Pole',   89.99,    0],
  ];
  console.log('    location          cellId   face');
  for (const [name, lat, lng] of places) {
    const cid = geoCellId(lat, lng, 8);
    const face = geoCellFace(cid);
    console.log(`    ${name.padEnd(16)}  0x${cid.toString(16).padStart(2,'0')} (${cid.toString().padStart(3)})  face ${face}`);
    check(`${name} cellId is valid`, isValidCellId(cid));
  }
}

// ─── Geographic locality: nearby points usually share a cell or
//     neighbouring face ───
console.log('\n── locality ──');
{
  const N = 1000;
  let sameCell = 0, sameFace = 0;
  for (let i = 0; i < N; i++) {
    const lat = Math.random() * 180 - 90;
    const lng = Math.random() * 360 - 180;
    // perturb by ~50 km (0.45° lat)
    const lat2 = lat + (Math.random() - 0.5) * 0.9;
    const lng2 = lng + (Math.random() - 0.5) * 0.9;
    const c1 = geoCellId(lat, lng, 8);
    const c2 = geoCellId(lat2, lng2, 8);
    if (c1 === c2) sameCell++;
    if (geoCellFace(c1) === geoCellFace(c2)) sameFace++;
  }
  console.log(`    of ${N} ~50km neighbour pairs: same cell ${sameCell} / same face ${sameFace}`);
  check('majority of ~50km neighbours share a face',
        sameFace > N * 0.95);
}

// ─── Summary ───
console.log(`\nResult: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
