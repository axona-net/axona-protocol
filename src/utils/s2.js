/**
 * s2.js — Geographic cell ID via Google's S2 cube projection.
 *
 * Maps (lat, lng) to an 8-bit cell ID in [0, 192) using the standard
 * S2 unit-sphere-on-cube geometry.  Unlike a flat lat/lng partition,
 * S2 cells have near-equal area across the globe (within ~2× of each
 * other) including at the poles — which matters both for routing
 * locality and for the privacy property that no peer should be
 * uniquely identifiable by its cell prefix.
 *
 * Implementation summary:
 *
 *   1. (lat, lng)               →  unit-sphere (x, y, z)
 *   2. xyz                      →  face (0..5)  +  face-local (u, v) ∈ [-1,+1]²
 *   3. quadratic UV-to-ST       →  (s, t)       ∈ [0, 1]²            (equal-area)
 *   4. bin (s, t) into 4×8      →  (sBin, tBin)
 *   5. cellId = face·32 + sBin·8 + tBin                              ∈ [0, 192)
 *
 * The 4×8 partition is "S2 level 2.5" — one bit short of S2 level 3
 * (8×8 squares per face).  Cells are rectangular on each face but
 * 192 fits within 8 bits with 64 byte values left over (192..255)
 * reserved for system topics, future address-space extensions, etc.
 *
 * Face axes follow the Google S2 convention.  ST quadratic transform
 * follows S2 default ("quadratic" projection — equal-area within
 * ~1.7× across the globe, vs. ~3× for the linear projection).
 *
 * No external dependencies.  Reverses cleanly: cellId → lat/lng-center.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/** Number of cube faces (S2's hierarchical foundation). */
export const S2_FACES = 6;

/** Cell subdivisions per face axis. */
const S_BINS = 4;
const T_BINS = 8;

/** Cells per face. */
const CELLS_PER_FACE = S_BINS * T_BINS;   // 32

/** Total valid 8-bit cell IDs. */
export const S2_CELL_COUNT = S2_FACES * CELLS_PER_FACE;   // 192

/** First reserved (invalid) 8-bit cell ID. */
export const S2_RESERVED_FROM = S2_CELL_COUNT;            // 192

/**
 * Face metadata.  For each face k, defines the three sphere-axis
 * roles: which axis is the face normal, which is the u-axis, which
 * is the v-axis, each with a sign.  Matches Google S2 conventions.
 *
 *   Format: [ [normalAxis, normalSign], [uAxis, uSign], [vAxis, vSign] ]
 *   where axis is 0=X, 1=Y, 2=Z and sign is ±1.
 */
const FACE_AXES = [
  // Face 0: normal=+X, u=+Y, v=+Z
  [[0, +1], [1, +1], [2, +1]],
  // Face 1: normal=+Y, u=-X, v=+Z
  [[1, +1], [0, -1], [2, +1]],
  // Face 2: normal=+Z, u=-X, v=-Y
  [[2, +1], [0, -1], [1, -1]],
  // Face 3: normal=-X, u=-Z, v=-Y
  [[0, -1], [2, -1], [1, -1]],
  // Face 4: normal=-Y, u=-Z, v=+X
  [[1, -1], [2, -1], [0, +1]],
  // Face 5: normal=-Z, u=+Y, v=+X
  [[2, -1], [1, +1], [0, +1]],
];

// ─────────────────────────────────────────────────────────────────────────────
// Coordinate conversions
// ─────────────────────────────────────────────────────────────────────────────

const DEG = Math.PI / 180;
const RAD = 180 / Math.PI;

function latLngToXYZ(lat, lng) {
  const latR = lat * DEG;
  const lngR = lng * DEG;
  const c = Math.cos(latR);
  return { x: c * Math.cos(lngR), y: c * Math.sin(lngR), z: Math.sin(latR) };
}

function xyzToLatLng(x, y, z) {
  return {
    lat: Math.asin(Math.max(-1, Math.min(1, z))) * RAD,
    lng: Math.atan2(y, x) * RAD,
  };
}

/** Which of the 6 cube faces does this unit-sphere point sit on? */
function xyzToFace(x, y, z) {
  const ax = Math.abs(x), ay = Math.abs(y), az = Math.abs(z);
  if (ax >= ay && ax >= az) return x >= 0 ? 0 : 3;
  if (ay >= az)             return y >= 0 ? 1 : 4;
  return                          z >= 0 ? 2 : 5;
}

/** Project xyz onto the chosen face's (u, v) plane.  u, v ∈ [-1, +1]. */
function xyzToFaceUV(face, x, y, z) {
  const xyz = [x, y, z];
  const [[nAx, nSign], [uAx, uSign], [vAx, vSign]] = FACE_AXES[face];
  const n = nSign * xyz[nAx];        // > 0 by face-selection
  return { u: (uSign * xyz[uAx]) / n, v: (vSign * xyz[vAx]) / n };
}

/** Inverse of xyzToFaceUV: unproject from face-(u,v) back onto unit sphere. */
function faceUVToXYZ(face, u, v) {
  const xyz = [0, 0, 0];
  const [[nAx, nSign], [uAx, uSign], [vAx, vSign]] = FACE_AXES[face];
  xyz[nAx] = nSign;
  xyz[uAx] = uSign * u;
  xyz[vAx] = vSign * v;
  const len = Math.hypot(xyz[0], xyz[1], xyz[2]);
  return { x: xyz[0] / len, y: xyz[1] / len, z: xyz[2] / len };
}

// ─────────────────────────────────────────────────────────────────────────────
// Hilbert curve for cell numbering within a face
//
// Real S2 numbers cells inside each face along a Hilbert curve so
// consecutive cell IDs are guaranteed spatial neighbours.  Our per-
// face grid is 4×8 — not a Hilbert-native square — so we build the
// curve as two 4×4 Hilbert blocks stacked along the long (t) axis,
// with the second block reflected so its d=0 cell is spatially
// adjacent to the first block's d=15 cell.
//
// Standard 4×4 Hilbert with start (0,0) ends at (3,0).  We arrange
// block A at t ∈ [0, 3] (ends at sBin=3, tBin=0... wait, careful with
// orientation: we use s as the rows, t as the columns, so the curve
// visits within the (s, t) plane and its endpoint after block A is at
// (s=3, t=0).  No — actually depending on starting orientation it
// ends at one of the corners.  We pick orientations so the endpoint
// of block A and the start of block B share an edge.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Hilbert 4×4 xy → d.  (x, y) ∈ [0..3]², returns d ∈ [0..15].
 *
 * Wikipedia's xy2d uses `n-1 - x` in the rotation (the full grid size
 * stays constant across iterations); using `s-1 - x` here would push
 * intermediate coordinates negative and corrupt later iterations.
 *
 * Standard 4×4 Hilbert: starts at (0,0), ends at (3,0).
 */
function hilbert4x4_xy2d(x, y) {
  const N = 4, NM1 = N - 1;
  let d = 0;
  for (let s = N >> 1; s > 0; s >>= 1) {
    const rx = (x & s) > 0 ? 1 : 0;
    const ry = (y & s) > 0 ? 1 : 0;
    d += s * s * ((3 * rx) ^ ry);
    if (ry === 0) {
      if (rx === 1) { x = NM1 - x; y = NM1 - y; }
      const tmp = x; x = y; y = tmp;
    }
  }
  return d;
}

/**
 * Hilbert 4×4 d → xy.  d ∈ [0..15], returns {x, y} ∈ [0..3]².
 *
 * The inverse uses `s-1 - x` (Wikipedia d2xy convention — different
 * from xy2d because here (x, y) is built up progressively in [0, s)).
 */
function hilbert4x4_d2xy(d) {
  let x = 0, y = 0, t = d;
  for (let s = 1; s < 4; s <<= 1) {
    const rx = 1 & (t >> 1);
    const ry = 1 & (t ^ rx);
    if (ry === 0) {
      if (rx === 1) { x = s - 1 - x; y = s - 1 - y; }
      const tmp = x; x = y; y = tmp;
    }
    x += s * rx;
    y += s * ry;
    t >>= 2;
  }
  return { x, y };
}

/**
 * Hilbert numbering for the 4×8 (sBin × tBin) per-face grid.
 *
 * The 4×4 Hilbert curve starts at (0, 0) and ends at (3, 0).  Naive
 * stacking of two 4×4 blocks at t ∈ [0, 3] and [4, 7] would have
 * block A end at (s=3, t=0) — *not* adjacent to block B's start.
 *
 * Fix: rotate block A by swapping its (x, y) so it ends at (s=0, t=3)
 * along the t-axis boundary.  Block B then starts at (s=0, t=4),
 * directly adjacent — single step in t, no step in s.
 *
 *   Block A  (d ∈ [0, 15], t ∈ [0, 3]):
 *     d = hilbert4x4_xy2d(tBin, sBin)        ← swap to rotate
 *   Block B  (d ∈ [16, 31], t ∈ [4, 7]):
 *     d = 16 + hilbert4x4_xy2d(sBin, tBin-4) ← standard orientation
 *
 * Block A endpoint: cell d=15 at (s=0, t=3).
 * Block B start:    cell d=16 at (s=0, t=4) — neighbours. ✓
 *
 * @param {number} sBin – row index ∈ [0..3]
 * @param {number} tBin – col index ∈ [0..7]
 * @returns {number}      d ∈ [0..31]
 */
function faceCellIndex(sBin, tBin) {
  if (tBin < 4) return hilbert4x4_xy2d(tBin, sBin);
  return 16 + hilbert4x4_xy2d(sBin, tBin - 4);
}

/** Inverse of faceCellIndex.  d ∈ [0..31] → {sBin, tBin}. */
function faceCellIndexInverse(d) {
  if (d < 16) {
    const { x, y } = hilbert4x4_d2xy(d);
    return { sBin: y, tBin: x };       // un-swap (block A rotation)
  }
  const { x, y } = hilbert4x4_d2xy(d - 16);
  return { sBin: x, tBin: y + 4 };
}

// ─────────────────────────────────────────────────────────────────────────────
// Quadratic UV↔ST transform (Google S2 default — for equal-area cells)
// ─────────────────────────────────────────────────────────────────────────────

/** UV → ST.  u ∈ [-1, +1]  →  s ∈ [0, 1]. */
function uvToST(u) {
  return u >= 0
    ?         0.5 * Math.sqrt(1 + 3 * u)
    : 1.0   - 0.5 * Math.sqrt(1 - 3 * u);
}

/** ST → UV.  s ∈ [0, 1]  →  u ∈ [-1, +1]. */
function stToUV(s) {
  return s >= 0.5
    ?         (4 * s * s - 1) / 3
    : -((4 * (1 - s) * (1 - s) - 1) / 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Compute the 8-bit S2 cell ID for a (lat, lng) coordinate.
 *
 * @param {number} lat   – latitude in degrees, [-90, +90]
 * @param {number} lng   – longitude in degrees, [-180, +180]
 * @param {number} [bits] – ignored (kept for API back-compat); always returns 8 bits.
 * @returns {number}     – integer in [0, 192).  Never returns 192..255.
 */
export function geoCellId(lat, lng, bits) {
  // bits parameter ignored; kept in the signature so callers that
  // previously passed `8` don't need patching.  All Axona usage is
  // 8-bit at this level; deeper hierarchies are a future addition.
  void bits;

  const { x, y, z } = latLngToXYZ(lat, lng);
  const face = xyzToFace(x, y, z);
  const { u, v } = xyzToFaceUV(face, x, y, z);
  const s = uvToST(u);
  const t = uvToST(v);
  const sBin = clamp(Math.floor(s * S_BINS), 0, S_BINS - 1);
  const tBin = clamp(Math.floor(t * T_BINS), 0, T_BINS - 1);
  return face * CELLS_PER_FACE + faceCellIndex(sBin, tBin);
}

/**
 * Compute the (lat, lng) at the center of a given 8-bit S2 cell.
 *
 * Useful for visualization and for system topics that need a well-
 * defined coordinate per cell.  Returns null if the cell ID is in
 * the reserved 192..255 range.
 *
 * @param {number} cellId – 8-bit cell ID in [0, 192).
 * @returns {{lat: number, lng: number} | null}
 */
export function geoCellCenter(cellId) {
  if (!isValidCellId(cellId)) return null;
  const face = Math.floor(cellId / CELLS_PER_FACE);
  const inFace = cellId - face * CELLS_PER_FACE;
  const { sBin, tBin } = faceCellIndexInverse(inFace);
  const s = (sBin + 0.5) / S_BINS;     // center of bin in s
  const t = (tBin + 0.5) / T_BINS;     // center of bin in t
  const u = stToUV(s);
  const v = stToUV(t);
  const { x, y, z } = faceUVToXYZ(face, u, v);
  return xyzToLatLng(x, y, z);
}

/**
 * Compute the four (lat, lng) corners of an 8-bit S2 cell, in
 * face-local order.  The cell boundary on the sphere is the great-
 * circle arc between consecutive corners.  Useful for the visualizer
 * to draw curved cell outlines.
 *
 * Returns null for reserved cell IDs.
 *
 * @param {number} cellId – 8-bit cell ID in [0, 192).
 * @returns {Array<{lat: number, lng: number}> | null} 4-element array.
 */
export function geoCellCorners(cellId) {
  if (!isValidCellId(cellId)) return null;
  const face = Math.floor(cellId / CELLS_PER_FACE);
  const inFace = cellId - face * CELLS_PER_FACE;
  const { sBin, tBin } = faceCellIndexInverse(inFace);
  const s0 = sBin / S_BINS,       s1 = (sBin + 1) / S_BINS;
  const t0 = tBin / T_BINS,       t1 = (tBin + 1) / T_BINS;
  const ll = (s, t) => {
    const { x, y, z } = faceUVToXYZ(face, stToUV(s), stToUV(t));
    return xyzToLatLng(x, y, z);
  };
  // Go around: (s0,t0) → (s1,t0) → (s1,t1) → (s0,t1)
  return [ll(s0, t0), ll(s1, t0), ll(s1, t1), ll(s0, t1)];
}

/**
 * Compute the face (0..5) for an 8-bit S2 cell ID.
 * Reserved IDs (≥192) return -1.
 */
export function geoCellFace(cellId) {
  if (!isValidCellId(cellId)) return -1;
  return Math.floor(cellId / CELLS_PER_FACE);
}

/** True iff cellId is in the valid [0, 192) range. */
export function isValidCellId(cellId) {
  return Number.isInteger(cellId) && cellId >= 0 && cellId < S2_CELL_COUNT;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}
