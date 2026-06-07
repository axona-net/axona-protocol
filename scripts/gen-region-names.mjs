// gen-region-names.mjs — generate TWO names per region (the cell's two halves).
//
// Rules:
//   • Each 8-bit cell = two S2 level-3 sub-cells (its halves). Name each half
//     from the nearest anchor so a user sees the closest name; both names
//     resolve to one code. Homogeneous cells share one name.
//   • LAND anchors (continents + big archipelagos) may span cells and take a
//     COMPASS suffix when repeated (russiae, japanne) — never a number.
//   • ISLAND anchors are CLAIM-ONCE: a small island names only the single
//     closest cell (1 half, or both halves if it straddles them). No compass
//     spread (no "canarye", no five "easter" cells).
//   • Open water (no land within LAND_DIST, no island within ISLAND_DIST of a
//     half) → "<oce3>_<hex>" (pac_68, atl_0a, ind_22, sou_a3).
//   • Last-resort land tiebreak is "_<hex>".
//
// Output: literal [a,b] pairs (stdout) + face-grouped review (stderr).

import { geoCellSubCenters, geoCellFace, S2_CELL_COUNT } from '../src/utils/s2.js';

const LAND_DIST   = 10;   // deg — within this of a land anchor ⇒ land name
const ISLAND_DIST = 8;    // deg — within this of an island ⇒ that island
const COMPASS_EPS = 2;    // deg — offset before a compass letter

// [name, lat, lng, 'land' | 'island'].
const ANCHORS = [
  // ── North America (land) ──
  ['uswest', 40, -121, 'land'], ['ussw', 34, -108, 'land'], ['uscentral', 41, -98, 'land'],
  ['useast', 41, -78, 'land'], ['usse', 32, -82, 'land'], ['canada', 56, -100, 'land'],
  ['quebec', 52, -72, 'land'], ['alaska', 64, -150, 'land'], ['greenland', 72, -42, 'land'],
  ['mexico', 24, -103, 'land'], ['guatemala', 15, -90, 'land'],
  // ── South America (land) ──
  ['colombia', 4, -73, 'land'], ['venezuela', 8, -64, 'land'], ['amazon', -4, -62, 'land'],
  ['brazil', -10, -45, 'land'], ['saopaulo', -23, -47, 'land'], ['bolivia', -17, -64, 'land'],
  ['argentina', -34, -64, 'land'], ['chile', -33, -71, 'land'], ['peru', -10, -76, 'land'],
  ['patagonia', -45, -69, 'land'],
  // ── Europe (land) ──
  ['uk', 54, -2, 'land'], ['iberia', 40, -4, 'land'], ['france', 47, 2, 'land'],
  ['europe', 51, 11, 'land'], ['italy', 42, 13, 'land'], ['scandinavia', 63, 16, 'land'],
  ['baltic', 57, 24, 'land'], ['poland', 52, 19, 'land'], ['balkans', 43, 21, 'land'],
  ['ukraine', 49, 32, 'land'], ['greece', 39, 22, 'land'], ['turkey', 39, 35, 'land'],
  // ── Russia / North & Central Asia (land) ──
  ['russia', 56, 42, 'land'], ['urals', 60, 64, 'land'], ['siberia', 62, 95, 'land'],
  ['eastsib', 66, 130, 'land'], ['mongolia', 47, 104, 'land'], ['kamchatka', 56, 159, 'land'],
  ['kazakhstan', 48, 67, 'land'],
  // ── Middle East (land) ──
  ['levant', 33, 38, 'land'], ['arabia', 23, 45, 'land'], ['iran', 32, 53, 'land'],
  ['caucasus', 42, 45, 'land'],
  // ── Africa (land) ──
  ['morocco', 31, -7, 'land'], ['algeria', 28, 2, 'land'], ['sahara', 24, 9, 'land'],
  ['libya', 27, 17, 'land'], ['egypt', 27, 29, 'land'], ['mali', 18, -3, 'land'],
  ['niger', 17, 9, 'land'], ['chad', 15, 18, 'land'], ['sudan', 15, 30, 'land'],
  ['westafrica', 9, -6, 'land'], ['nigeria', 9, 8, 'land'], ['ethiopia', 9, 40, 'land'],
  ['somalia', 6, 46, 'land'], ['eastafrica', -3, 36, 'land'], ['congo', -2, 22, 'land'],
  ['angola', -12, 18, 'land'], ['namibia', -22, 17, 'land'], ['southafrica', -29, 24, 'land'],
  ['tanzania', -6, 35, 'land'],
  // ── South / SE Asia (land + big archipelagos) ──
  ['pakistan', 30, 68, 'land'], ['india', 22, 78, 'land'], ['southindia', 12, 78, 'land'],
  ['bengal', 24, 89, 'land'], ['tibet', 32, 86, 'land'], ['china', 34, 104, 'land'],
  ['southchina', 24, 110, 'land'], ['korea', 37, 127, 'land'], ['japan', 37, 138, 'land'],
  ['vietnam', 16, 107, 'land'], ['thailand', 15, 101, 'land'], ['myanmar', 21, 96, 'land'],
  ['malaysia', 4, 102, 'land'], ['indonesia', -2, 118, 'land'], ['philippines', 13, 122, 'land'],
  // ── Oceania (land) ──
  ['ozwest', -26, 119, 'land'], ['ozcentral', -25, 134, 'land'], ['oznorth', -15, 133, 'land'],
  ['ozeast', -32, 147, 'land'],
  // ── Poles (land) ──
  ['arctic', 86, 0, 'land'], ['antarctica', -84, 0, 'land'], ['antpenin', -68, -63, 'land'],
  ['eastant', -74, 90, 'land'], ['rossant', -82, 175, 'land'],
  // ── Islands (claim-once) ──
  ['iceland', 65, -18, 'island'], ['hawaii', 21, -157, 'island'], ['galapagos', 0, -91, 'island'],
  ['easter', -27, -109, 'island'], ['tahiti', -17, -149, 'island'], ['samoa', -14, -172, 'island'],
  ['fiji', -17, 178, 'island'], ['tonga', -21, -175, 'island'], ['newcaled', -21, 165, 'island'],
  ['solomon', -9, 160, 'island'], ['guam', 13, 145, 'island'], ['marshall', 7, 168, 'island'],
  ['kiribati', 2, -157, 'island'], ['azores', 38, -28, 'island'], ['canary', 28, -16, 'island'],
  ['capeverde', 16, -24, 'island'], ['bermuda', 32, -65, 'island'], ['falklands', -52, -59, 'island'],
  ['sthelena', -16, -6, 'island'], ['ascension', -8, -14, 'island'], ['maldives', 3, 73, 'island'],
  ['mauritius', -20, 57, 'island'], ['seychelles', -5, 55, 'island'], ['kerguelen', -49, 69, 'island'],
  ['svalbard', 78, 18, 'island'], ['faroe', 62, -7, 'island'], ['cuba', 21, -78, 'island'],
  ['bahamas', 24, -76, 'island'], ['caribbean', 15, -71, 'island'], ['srilanka', 7, 81, 'island'],
  ['taiwan', 24, 121, 'island'], ['madagascar', -19, 47, 'island'], ['nzealand', -42, 172, 'island'],
  ['borneo', 1, 114, 'island'], ['png', -6, 144, 'island'], ['sumatra', 0, 101, 'island'],
];

const DEG = Math.PI / 180;
const unit = (lat, lng) => { const a = lat*DEG, b = lng*DEG, c = Math.cos(a); return [c*Math.cos(b), c*Math.sin(b), Math.sin(a)]; };
const angDeg = (u, v) => Math.acos(Math.max(-1, Math.min(1, u[0]*v[0]+u[1]*v[1]+u[2]*v[2]))) / DEG;
const A = ANCHORS.map(([name, lat, lng, type]) => ({ name, lat, lng, type, v: unit(lat, lng) }));
const LAND = A.filter(a => a.type === 'land');
const ISLAND = A.filter(a => a.type === 'island');
const hex2 = (c) => c.toString(16).padStart(2, '0');

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

// All 384 halves.
const halves = [];
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const [c0, c1] = geoCellSubCenters(code);
  halves.push({ code, idx: 0, c: c0, u: unit(c0.lat, c0.lng) });
  halves.push({ code, idx: 1, c: c1, u: unit(c1.lat, c1.lng) });
}
const key = (code, idx) => code * 2 + idx;

const NAMES = Array.from({ length: S2_CELL_COUNT }, () => [null, null]);
const usedNames = new Set();
const islandClaimed = new Set();

// ── Pass 1: island claim-once. Tightest island first; each names only the
// single closest cell (its 1–2 halves within ISLAND_DIST). ──
const islandCand = ISLAND.map(isl => {
  const cs = halves.map(h => ({ h, d: angDeg(h.u, isl.v) }))
                   .filter(x => x.d <= ISLAND_DIST).sort((a, b) => a.d - b.d);
  return { isl, cs, best: cs.length ? cs[0].d : Infinity };
}).filter(x => x.cs.length).sort((a, b) => a.best - b.best);

for (const { isl, cs } of islandCand) {
  const first = cs.find(x => !islandClaimed.has(key(x.h.code, x.h.idx)));
  if (!first) continue;
  const winner = first.h.code;
  for (const x of cs) {
    if (x.h.code === winner && !islandClaimed.has(key(x.h.code, x.h.idx))) {
      NAMES[x.h.code][x.h.idx] = isl.name;
      islandClaimed.add(key(x.h.code, x.h.idx));
    }
  }
  usedNames.add(isl.name);
}

// ── Pass 2: classify remaining halves; assign ocean now, collect land. ──
const landEntries = [];
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const [c0, c1] = geoCellSubCenters(code);
  const cls = [{ idx: 0, c: c0 }, { idx: 1, c: c1 }].map(h => {
    if (NAMES[code][h.idx] !== null) return { idx: h.idx, kind: 'island' };
    const land = nearest(LAND, unit(h.c.lat, h.c.lng));
    if (land.anchor && land.dist <= LAND_DIST) return { idx: h.idx, kind: 'land', base: land.anchor.name, anchor: land.anchor, c: h.c };
    return { idx: h.idx, kind: 'ocean', c: h.c };
  });
  for (const c of cls) if (c.kind === 'ocean') {
    const nm = `${basin(c.c.lat, c.c.lng)}_${hex2(code)}`;
    NAMES[code][c.idx] = nm; usedNames.add(nm);
  }
  const lands = cls.filter(c => c.kind === 'land');
  if (lands.length === 2 && lands[0].base === lands[1].base) {
    landEntries.push({ slots: [[code, 0], [code, 1]], base: lands[0].base, anchor: lands[0].anchor, code,
      center: { lat: (c0.lat + c1.lat) / 2, lng: (c0.lng + c1.lng) / 2 } });
  } else {
    for (const c of lands) landEntries.push({ slots: [[code, c.idx]], base: c.base, anchor: c.anchor, code, center: c.c });
  }
}

// ── Pass 3: land naming. Closest entry to an anchor keeps the bare name; the
// rest get a compass octant; "_<hex>" as last resort. ──
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
    else {
      let dLng = e.center.lng - e.anchor.lng; while (dLng > 180) dLng -= 360; while (dLng < -180) dLng += 360;
      cands.push(base + (Math.abs(e.center.lat - e.anchor.lat) >= Math.abs(dLng)
        ? (e.center.lat >= e.anchor.lat ? 'n' : 's') : (dLng >= 0 ? 'e' : 'w')));
    }
    cands.push(`${base}_${hex2(e.code)}`);
    const name = cands.find(c => !usedNames.has(c)) ?? `${base}_${hex2(e.code)}`;
    usedNames.add(name);
    for (const [code, half] of e.slots) NAMES[code][half] = name;
  });
}

// ── Review → stderr ──
const pad = (s, n) => String(s).padEnd(n);
for (let f = 0; f < 6; f++) {
  process.stderr.write(`\n── face ${f} ──\n`);
  for (let code = f * 32; code < (f + 1) * 32; code++) {
    const [c0, c1] = geoCellSubCenters(code);
    process.stderr.write(`0x${hex2(code)}  ${pad(NAMES[code][0], 13)} ${pad(NAMES[code][1], 13)}` +
      `  (${c0.lat.toFixed(0)},${c0.lng.toFixed(0)} | ${c1.lat.toFixed(0)},${c1.lng.toFixed(0)})\n`);
  }
}
const flat = NAMES.flat();
process.stderr.write(`\nnames=${flat.length}  unique=${new Set(flat).size}  maxlen=${Math.max(...flat.map(n => n.length))}\n`);

// ── Literal → stdout ──
let out = '';
for (let code = 0; code < S2_CELL_COUNT; code++) out += `  ['${NAMES[code][0]}', '${NAMES[code][1]}'],  // 0x${hex2(code)}\n`;
process.stdout.write(out);
