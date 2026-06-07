// gen-region-names.mjs — generate the 192 curated region names from cell centers.
//
// Strategy: each 8-bit S2 cell has a deterministic center (geoCellCenter).
// We assign every cell the name of its NEAREST geographic anchor (great-circle
// distance on the unit sphere) from the hand-built ANCHORS table below — land
// regions and ocean basins, chosen to blanket the globe. Within an anchor's
// group, the closest cell keeps the bare name; the others get a compass-octant
// suffix (n/s/e/w/ne…); any residual collision falls back to a trailing digit.
// All names are lowercased [a-z0-9], ≤ 8 chars, no spaces, globally unique.
//
// Output (stdout): a JS literal array suitable for pasting into the protocol
// module, plus a face-grouped review table on stderr.
//
//   node scripts/gen-region-names.mjs            # review table -> stderr, array -> stdout
//   node scripts/gen-region-names.mjs > /tmp/names.js
//
// The generator is kept in-repo for provenance, but the protocol ships the
// RESULT as a frozen literal (src/utils/region-names.js) — not this script.

import { geoCellCenter, geoCellFace, S2_CELL_COUNT } from '../src/utils/s2.js';

// [name, lat, lng] — keep names ≤ 6 chars where possible to leave room for a
// 1–2 char disambiguation suffix.
const ANCHORS = [
  // ── North America ──
  ['useast', 37, -78], ['uswest', 38, -121], ['uscen', 41, -100],
  ['canada', 55, -100], ['cannef', 53, -68], ['alaska', 63, -152],
  ['grnland', 72, -42], ['mexico', 23, -103], ['caribb', 19, -77],
  ['camer', 14, -88],
  // ── South America ──
  ['colomb', 4, -73], ['venez', 8, -65], ['amazon', -3, -62],
  ['brazil', -9, -45], ['saopau', -23, -47], ['argent', -34, -64],
  ['chile', -33, -71], ['peru', -12, -75], ['patago', -47, -70],
  // ── Europe ──
  ['iceland', 65, -18], ['britain', 54, -2], ['iberia', 40, -4],
  ['france', 47, 2], ['germany', 51, 10], ['italy', 42, 13],
  ['nordic', 63, 16], ['baltic', 57, 24], ['balkan', 43, 22],
  ['ukrain', 49, 32], ['greece', 38, 23],
  // ── Russia / North Asia ──
  ['russw', 56, 40], ['ural', 58, 62], ['sibwest', 60, 82],
  ['sibest', 65, 120], ['mongol', 47, 104], ['kamchk', 56, 159],
  ['yakutia', 67, 130],
  // ── Middle East / Caucasus ──
  ['turkey', 39, 35], ['mideast', 33, 40], ['arabia', 23, 45],
  ['iran', 32, 53], ['caucas', 42, 45],
  // ── Africa ──
  ['sahara', 25, 13], ['egypt', 26, 30], ['wafric', 10, -3],
  ['sahel', 15, 8], ['ethiop', 8, 40], ['eafric', -2, 37],
  ['congo', -2, 22], ['angola', -12, 18], ['safric', -29, 24],
  ['madag', -19, 47], ['horn', 9, 48],
  // ── South / SE Asia ──
  ['india', 22, 78], ['sindia', 12, 78], ['pakist', 30, 68],
  ['bengal', 24, 90], ['china', 34, 104], ['schina', 24, 110],
  ['tibet', 32, 86], ['korea', 37, 127], ['japan', 36, 138],
  ['taiwan', 24, 121], ['seasia', 14, 105], ['malay', 3, 102],
  ['indones', -2, 117], ['borneo', 1, 114], ['philip', 13, 122],
  ['png', -6, 144],
  // ── Oceania ──
  ['auswest', -26, 120], ['auscen', -25, 134], ['auseast', -31, 147],
  ['ausnth', -14, 133], ['nzeal', -42, 172],
  // ── Poles ──
  ['arctic', 87, 0], ['antarc', -83, 0], ['antpen', -68, -63],
  ['anteast', -74, 90], ['antross', -82, 175],
  // ── North Atlantic ──
  ['natlan', 33, -45], ['natle', 45, -28], ['natlw', 28, -62],
  ['labrad', 56, -48],
  // ── South Atlantic ──
  ['satlan', -22, -12], ['satlw', -28, -35],
  // ── North Pacific ──
  ['npacif', 30, -160], ['npacw', 35, 172], ['npace', 23, -132],
  ['bering', 57, -178], ['hawaii', 21, -157],
  // ── South Pacific ──
  ['spacif', -22, -132], ['space', -32, -98], ['spacw', -18, -162],
  ['coral', -15, 155], ['tasman', -44, 162], ['polyn', -16, -148],
  ['fiji', -17, 178],
  // ── Indian Ocean ──
  ['indian', -28, 80], ['nindia', 5, 72], ['arabsea', 14, 63],
  ['bengbay', 13, 88], ['sindoc', -40, 88],
  // ── Marginal seas ──
  ['medsea', 36, 17], ['blacks', 43, 34], ['caspia', 42, 50],
  ['redsea', 20, 38], ['gulf', 27, 52],
  // ── Southern Ocean ──
  ['sthn', -57, 10], ['sthne', -55, 85], ['sthnw', -55, -42],
  ['sthnp', -58, 150],
];

const DEG = Math.PI / 180;
function unit(lat, lng) {
  const a = lat * DEG, b = lng * DEG, c = Math.cos(a);
  return [c * Math.cos(b), c * Math.sin(b), Math.sin(a)];
}
function angDist(u, v) { // angular distance via dot product
  const d = Math.max(-1, Math.min(1, u[0]*v[0] + u[1]*v[1] + u[2]*v[2]));
  return Math.acos(d);
}
const anchorVecs = ANCHORS.map(([n, lat, lng]) => ({ name: n, lat, lng, v: unit(lat, lng) }));

function octant(cellLat, cellLng, aLat, aLng) {
  const dLat = cellLat - aLat;
  let dLng = cellLng - aLng;
  while (dLng > 180) dLng -= 360;
  while (dLng < -180) dLng += 360;
  const ns = dLat > 8 ? 'n' : dLat < -8 ? 's' : '';
  const ew = dLng > 8 ? 'e' : dLng < -8 ? 'w' : '';
  return ns + ew;
}

// Pass 1: nearest anchor per cell.
const cells = [];
for (let id = 0; id < S2_CELL_COUNT; id++) {
  const c = geoCellCenter(id);
  const u = unit(c.lat, c.lng);
  let best = null, bestD = Infinity;
  for (const a of anchorVecs) {
    const d = angDist(u, a.v);
    if (d < bestD) { bestD = d; best = a; }
  }
  cells.push({ id, lat: c.lat, lng: c.lng, face: geoCellFace(id), anchor: best, dist: bestD });
}

// Pass 2: within each anchor group, closest cell keeps the bare name; others
// get a compass suffix; residual collisions get a trailing digit.
const byAnchor = new Map();
for (const c of cells) {
  if (!byAnchor.has(c.anchor.name)) byAnchor.set(c.anchor.name, []);
  byAnchor.get(c.anchor.name).push(c);
}
const used = new Set();
const clamp8 = (s) => s.slice(0, 8);
function uniquify(base) {
  let name = clamp8(base);
  if (!used.has(name)) { used.add(name); return name; }
  // mutate: append digits, trimming base to stay ≤ 8
  for (let d = 2; d < 100; d++) {
    const suf = String(d);
    const cand = clamp8(base.slice(0, 8 - suf.length) + suf);
    if (!used.has(cand)) { used.add(cand); return cand; }
  }
  throw new Error('could not uniquify ' + base);
}

const names = new Array(S2_CELL_COUNT);
for (const [, group] of byAnchor) {
  group.sort((a, b) => a.dist - b.dist);
  group.forEach((c, i) => {
    let base = c.anchor.name;
    if (i > 0) {
      const oct = octant(c.lat, c.lng, c.anchor.lat, c.anchor.lng);
      base = clamp8(c.anchor.name + (oct || String(i + 1)));
    }
    names[c.id] = uniquify(base);
  });
}

// ── Review table → stderr ──
const pad = (s, n) => String(s).padEnd(n);
for (let f = 0; f < 6; f++) {
  process.stderr.write(`\n── face ${f} ──\n`);
  for (let id = f * 32; id < (f + 1) * 32; id++) {
    const c = cells[id];
    process.stderr.write(
      `0x${id.toString(16).padStart(2, '0')} ${pad(names[id], 9)} ` +
      `(${c.lat.toFixed(0)},${c.lng.toFixed(0)})\n`);
  }
}
process.stderr.write(`\nunique=${new Set(names).size}/${S2_CELL_COUNT}  ` +
  `maxlen=${Math.max(...names.map(n => n.length))}\n`);

// ── Literal array → stdout ──
let out = '';
for (let r = 0; r < S2_CELL_COUNT; r += 6) {
  out += '  ' + names.slice(r, r + 6).map(n => `'${n}'`).join(', ') + ',\n';
}
process.stdout.write(out);
