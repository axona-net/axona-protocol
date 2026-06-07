// gen-region-names.mjs — generate TWO curated names per region (the cell's two
// halves), per the naming rules:
//
//   • Each 8-bit cell = two S2 level-3 sub-cells (geoCellSubCenters) — its two
//     "halves". We name each half from the nearest geographic anchor, so a user
//     sees the name closest to their actual location. Both names resolve to the
//     same region code.
//   • Full names where they fit (colombia, europe, uk, scandinavia, blacksea).
//   • Open ocean (no land/island within LAND_DIST/ISLAND_DIST of the half) →
//     "<oce3>_<hex>"  e.g. pac_68, atl_0a, ind_22 — both halves identical.
//   • An island in a half uses the island's name.
//   • If a land base repeats across cells, disambiguate with a COMPASS octant
//     (never a number); last-resort tiebreak is "_<hex>".
//   • A half-pair that lands on the same base collapses to one name (france/
//     france) so a single-country cell reads as that country.
//
// Output: a JS literal array of [a,b] pairs (stdout) + a face-grouped review
// table (stderr). The protocol ships the RESULT (src/utils/region-names.js).

import { geoCellSubCenters, geoCellFace, S2_CELL_COUNT } from '../src/utils/s2.js';

const LAND_DIST   = 13;   // deg — within this of a land anchor ⇒ land name
const ISLAND_DIST = 6;    // deg — within this of an island ⇒ island name
const COMPASS_EPS = 2;    // deg — offset from anchor before a compass letter

// type: 'land' | 'island'.  Oceans are NOT anchors — open water is detected by
// distance to the nearest land/island, then named by basin() below.
const ANCHORS = [
  // ── North America ──
  ['useast', 38, -78, 'land'], ['uswest', 38, -121, 'land'], ['uscentral', 40, -100, 'land'],
  ['canada', 56, -100, 'land'], ['quebec', 53, -72, 'land'], ['alaska', 64, -150, 'land'],
  ['greenland', 72, -42, 'land'], ['mexico', 23, -103, 'land'], ['guatemala', 15, -90, 'land'],
  ['cuba', 21, -78, 'island'], ['bahamas', 24, -76, 'island'],
  // ── South America ──
  ['colombia', 4, -73, 'land'], ['venezuela', 8, -65, 'land'], ['amazon', -3, -62, 'land'],
  ['brazil', -9, -45, 'land'], ['saopaulo', -23, -47, 'land'], ['argentina', -34, -64, 'land'],
  ['chile', -33, -71, 'land'], ['peru', -12, -75, 'land'], ['patagonia', -47, -70, 'land'],
  // ── Europe ──
  ['iceland', 65, -18, 'island'], ['uk', 54, -2, 'land'], ['iberia', 40, -4, 'land'],
  ['france', 47, 2, 'land'], ['europe', 51, 11, 'land'], ['italy', 42, 13, 'land'],
  ['scandinavia', 63, 16, 'land'], ['baltic', 57, 24, 'land'], ['balkans', 43, 22, 'land'],
  ['ukraine', 49, 32, 'land'], ['greece', 38, 23, 'land'],
  // ── Russia / North Asia ──
  ['russia', 56, 42, 'land'], ['urals', 60, 64, 'land'], ['siberia', 62, 90, 'land'],
  ['eastsib', 66, 130, 'land'], ['mongolia', 47, 104, 'land'], ['kamchatka', 56, 159, 'land'],
  // ── Middle East / Caucasus ──
  ['turkey', 39, 35, 'land'], ['levant', 33, 40, 'land'], ['arabia', 23, 45, 'land'],
  ['iran', 32, 53, 'land'], ['caucasus', 42, 45, 'land'],
  // ── Africa ──
  ['sahara', 25, 13, 'land'], ['egypt', 26, 30, 'land'], ['westafrica', 10, -3, 'land'],
  ['sahel', 15, 8, 'land'], ['ethiopia', 8, 40, 'land'], ['eastafrica', -2, 37, 'land'],
  ['congo', -2, 22, 'land'], ['angola', -12, 18, 'land'], ['southafrica', -29, 24, 'land'],
  ['madagascar', -19, 47, 'island'], ['somalia', 8, 48, 'land'],
  // ── South / SE Asia ──
  ['india', 22, 78, 'land'], ['southindia', 12, 78, 'land'], ['pakistan', 30, 68, 'land'],
  ['bengal', 24, 90, 'land'], ['china', 34, 104, 'land'], ['southchina', 24, 110, 'land'],
  ['tibet', 31, 86, 'land'], ['korea', 37, 127, 'land'], ['japan', 36, 138, 'island'],
  ['taiwan', 24, 121, 'island'], ['vietnam', 14, 107, 'land'], ['thailand', 15, 101, 'land'],
  ['malaysia', 3, 102, 'land'], ['indonesia', -2, 117, 'island'], ['borneo', 1, 114, 'island'],
  ['philippines', 13, 122, 'island'], ['png', -6, 144, 'island'],
  // ── Oceania ──
  ['ozwest', -26, 120, 'land'], ['ozcentral', -25, 134, 'land'], ['ozeast', -31, 147, 'land'],
  ['oznorth', -14, 133, 'land'], ['nzealand', -42, 172, 'island'],
  // ── Poles ──
  ['arctic', 87, 0, 'land'], ['antarctica', -85, 0, 'land'], ['antpenin', -68, -63, 'land'],
  ['eastant', -74, 90, 'land'], ['rossant', -82, 175, 'land'],
  // ── Islands in open ocean (override ocean within ISLAND_DIST) ──
  ['hawaii', 21, -157, 'island'], ['galapagos', 0, -91, 'island'], ['easter', -27, -109, 'island'],
  ['tahiti', -17, -149, 'island'], ['samoa', -14, -172, 'island'], ['fiji', -17, 178, 'island'],
  ['tonga', -21, -175, 'island'], ['newcaled', -21, 165, 'island'], ['solomon', -9, 160, 'island'],
  ['guam', 13, 145, 'island'], ['marshall', 9, 168, 'island'], ['kiribati', 2, -157, 'island'],
  ['azores', 38, -28, 'island'], ['canary', 28, -16, 'island'], ['capeverde', 16, -24, 'island'],
  ['bermuda', 32, -65, 'island'], ['falklands', -52, -59, 'island'], ['sthelena', -16, -6, 'island'],
  ['maldives', 3, 73, 'island'], ['mauritius', -20, 57, 'island'], ['seychelles', -5, 55, 'island'],
  ['kerguelen', -49, 69, 'island'], ['svalbard', 78, 18, 'island'], ['faroe', 62, -7, 'island'],
  ['srilanka', 7, 81, 'island'], ['caribbean', 15, -73, 'island'],
];

const DEG = Math.PI / 180;
const unit = (lat, lng) => { const a = lat*DEG, b = lng*DEG, c = Math.cos(a); return [c*Math.cos(b), c*Math.sin(b), Math.sin(a)]; };
const angDeg = (u, v) => Math.acos(Math.max(-1, Math.min(1, u[0]*v[0]+u[1]*v[1]+u[2]*v[2]))) / DEG;
const A = ANCHORS.map(([name, lat, lng, type]) => ({ name, lat, lng, type, v: unit(lat, lng) }));
const LAND   = A.filter(a => a.type === 'land' || a.type === 'island');
const ISLAND = A.filter(a => a.type === 'island');

const hex2 = (c) => c.toString(16).padStart(2, '0');

// Rough ocean basin for an open-water point.
function basin(lat, lng) {
  let L = lng; while (L > 180) L -= 360; while (L < -180) L += 360;
  if (lat >= 66) return 'arc';
  if (lat <= -55) return 'sou';
  if (L >= -70 && L <= 20) return 'atl';
  if (L > 20 && L <= 110 && lat < 31) return 'ind';
  return 'pac';
}

function nearest(list, u) {
  let best = null, bd = Infinity;
  for (const a of list) { const d = angDeg(u, a.v); if (d < bd) { bd = d; best = a; } }
  return { anchor: best, dist: bd };
}

function octant(lat, lng, a) {
  let dLng = lng - a.lng; while (dLng > 180) dLng -= 360; while (dLng < -180) dLng += 360;
  const ns = (lat - a.lat) > COMPASS_EPS ? 'n' : (lat - a.lat) < -COMPASS_EPS ? 's' : '';
  const ew = dLng > COMPASS_EPS ? 'e' : dLng < -COMPASS_EPS ? 'w' : '';
  return ns + ew;
}

// Raw name + the anchor used (for compass disambiguation), for one half.
function rawName(center, code) {
  const u = unit(center.lat, center.lng);
  const isl = nearest(ISLAND, u);
  if (isl.anchor && isl.dist <= ISLAND_DIST) return { base: isl.anchor.name, anchor: isl.anchor, ocean: false };
  const land = nearest(LAND, u);
  if (land.anchor && land.dist <= LAND_DIST) return { base: land.anchor.name, anchor: land.anchor, ocean: false };
  return { base: `${basin(center.lat, center.lng)}_${hex2(code)}`, anchor: null, ocean: true };
}

const NAMES = Array.from({ length: S2_CELL_COUNT }, () => [null, null]);
const usedNames = new Set();

// Pass 1: classify every half; assign ocean names immediately (unique via hex).
// Collect land "entries" — a cell whose two halves share a base is ONE entry
// (so it reads as one place); otherwise each land half is its own entry.
const landEntries = [];
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const [c0, c1] = geoCellSubCenters(code);
  const r0 = rawName(c0, code), r1 = rawName(c1, code);
  const oceanHalf = (r, half) => { NAMES[code][half] = r.base; usedNames.add(r.base); };

  if (r0.ocean) oceanHalf(r0, 0);
  if (r1.ocean) oceanHalf(r1, 1);

  if (!r0.ocean && !r1.ocean && r0.base === r1.base) {
    landEntries.push({ slots: [[code, 0], [code, 1]], base: r0.base, anchor: r0.anchor, code,
      center: { lat: (c0.lat + c1.lat) / 2, lng: (c0.lng + c1.lng) / 2 } });
  } else {
    if (!r0.ocean) landEntries.push({ slots: [[code, 0]], base: r0.base, anchor: r0.anchor, code, center: c0 });
    if (!r1.ocean) landEntries.push({ slots: [[code, 1]], base: r1.base, anchor: r1.anchor, code, center: c1 });
  }
}

// Pass 2: within each base group, the entry CLOSEST to the anchor keeps the
// bare name; the rest get a compass octant; last-resort tiebreak is "_<hex>".
const byBase = new Map();
for (const e of landEntries) { if (!byBase.has(e.base)) byBase.set(e.base, []); byBase.get(e.base).push(e); }
for (const [base, group] of byBase) {
  group.forEach(e => { e.dist = angDeg(unit(e.center.lat, e.center.lng), e.anchor.v); });
  group.sort((a, b) => a.dist - b.dist);
  group.forEach((e, i) => {
    const cands = [];
    if (i === 0) cands.push(base);
    const oc = octant(e.center.lat, e.center.lng, e.anchor);
    if (oc) cands.push(base + oc);
    // forced single-letter compass if the octant came back empty
    if (!oc) {
      let dLng = e.center.lng - e.anchor.lng; while (dLng > 180) dLng -= 360; while (dLng < -180) dLng += 360;
      const f = Math.abs(e.center.lat - e.anchor.lat) >= Math.abs(dLng)
        ? (e.center.lat >= e.anchor.lat ? 'n' : 's')
        : (dLng >= 0 ? 'e' : 'w');
      cands.push(base + f);
    }
    cands.push(`${base}_${hex2(e.code)}`);
    const name = cands.find(c => !usedNames.has(c)) ?? `${base}_${hex2(e.code)}`;
    usedNames.add(name);
    for (const [code, half] of e.slots) NAMES[code][half] = name;
  });
}

// ── Review table → stderr ──
const pad = (s, n) => String(s).padEnd(n);
for (let f = 0; f < 6; f++) {
  process.stderr.write(`\n── face ${f} ──\n`);
  for (let code = f * 32; code < (f + 1) * 32; code++) {
    const [c0, c1] = geoCellSubCenters(code);
    process.stderr.write(`0x${hex2(code)}  ${pad(NAMES[code][0], 12)} ${pad(NAMES[code][1], 12)}` +
      `  (${c0.lat.toFixed(0)},${c0.lng.toFixed(0)} | ${c1.lat.toFixed(0)},${c1.lng.toFixed(0)})\n`);
  }
}
const flat = NAMES.flat();
process.stderr.write(`\nnames=${flat.length}  unique=${new Set(flat).size}  ` +
  `maxlen=${Math.max(...flat.map(n => n.length))}\n`);

// ── Literal → stdout ──
let out = '';
for (let code = 0; code < S2_CELL_COUNT; code++) {
  out += `  ['${NAMES[code][0]}', '${NAMES[code][1]}'],  // 0x${hex2(code)}\n`;
}
process.stdout.write(out);
