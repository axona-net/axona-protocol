// file-transport.js — generic file ⇄ pub/sub chunking, reusable across Axona apps.
//
// A publish message is capped (kernel MAX_PUBLISH_BYTES = 256 KB), so any file
// bigger than one message must travel as a set of messages and be reassembled.
// This library does that, BYTE-EXACTLY, for ANY file (images now; documents and
// other files later — those must arrive identical to what was sent).
//
//   chunkBytes(bytes, {name, mime, meta})  → [msg, msg, …]   (each JSON-safe, < cap)
//   createReassembler(onComplete)          → { accept(msg) } → onComplete({bytes,…})
//
// Messages are self-describing (every chunk carries name/mime/total/size), so they
// reassemble in any order and late joiners replaying history (since:'all') work.
// App-level metadata (e.g. an image caption) rides in chunk 0 via `meta`.
//
// Lossy compression is OPTIONAL and image-only (compressImage, browser/canvas) —
// it runs in the app BEFORE chunking, so the transport itself stays byte-exact
// and format-agnostic. Documents skip it and arrive bit-for-bit.

// Raw bytes per chunk. base64 inflates 4/3 and JSON adds a little; 150 KB raw →
// ~205 KB message, safely under the 256 KB publish cap.
const DEFAULT_MAX_CHUNK = 150 * 1024;
const FT = 1;                                   // message marker + format version

// ── base64 (works in browser AND node, for tests) ───────────────────
const hasBuffer = typeof Buffer !== 'undefined';
function bytesToB64(u8) {
  if (hasBuffer) return Buffer.from(u8).toString('base64');
  let s = ''; const CH = 0x8000;
  for (let i = 0; i < u8.length; i += CH) s += String.fromCharCode.apply(null, u8.subarray(i, i + CH));
  return btoa(s);
}
function b64ToBytes(b64) {
  if (hasBuffer) return new Uint8Array(Buffer.from(b64, 'base64'));
  const s = atob(b64); const u8 = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) u8[i] = s.charCodeAt(i);
  return u8;
}
function randomId() {
  const c = (typeof crypto !== 'undefined') ? crypto : null;
  if (c?.randomUUID) return c.randomUUID();
  let s = ''; for (let i = 0; i < 32; i++) s += (Math.floor((c?.getRandomValues ? c.getRandomValues(new Uint8Array(1))[0] : (i * 7 + 13)) % 16)).toString(16);
  return s;
}

/**
 * Split a file's bytes into self-describing messages, each JSON-serialisable and
 * under the publish cap. `meta` (app data, e.g. {caption}) rides in chunk 0.
 * @param {Uint8Array} bytes
 * @returns {Array<object>} messages to publish (in any order)
 */
export function chunkBytes(bytes, { name = 'file', mime = 'application/octet-stream', meta = null, maxChunk = DEFAULT_MAX_CHUNK, fileId = null } = {}) {
  if (!(bytes instanceof Uint8Array)) bytes = new Uint8Array(bytes);
  const id = fileId || randomId();
  const total = Math.max(1, Math.ceil(bytes.length / maxChunk));
  const msgs = [];
  for (let i = 0; i < total; i++) {
    const slice = bytes.subarray(i * maxChunk, Math.min(bytes.length, (i + 1) * maxChunk));
    const msg = { ft: FT, id, i, n: total, name, mime, size: bytes.length, data: bytesToB64(slice) };
    if (i === 0 && meta != null) msg.meta = meta;
    msgs.push(msg);
  }
  return msgs;
}

/**
 * Collect chunk messages (any order, duplicates tolerated). When all chunks of a
 * file have arrived, fires onComplete({ id, name, mime, size, bytes, meta }) once.
 * onProgress({ id, name, have, total }) fires per accepted chunk if provided.
 */
export function createReassembler(onComplete, { onProgress = null } = {}) {
  const partial = new Map();           // id → { n, name, mime, size, meta, chunks:Map<i,Uint8Array> }
  const done = new Set();              // completed ids (ignore re-delivered chunks)
  return {
    accept(msg) {
      if (!msg || msg.ft !== FT || typeof msg.id !== 'string') return false;
      if (done.has(msg.id)) return false;
      let e = partial.get(msg.id);
      if (!e) { e = { n: msg.n, name: msg.name, mime: msg.mime, size: msg.size, meta: null, chunks: new Map() }; partial.set(msg.id, e); }
      if (msg.meta != null) e.meta = msg.meta;
      if (!e.chunks.has(msg.i)) e.chunks.set(msg.i, b64ToBytes(msg.data));
      if (onProgress) onProgress({ id: msg.id, name: e.name, have: e.chunks.size, total: e.n });
      if (e.chunks.size !== e.n) return false;
      const bytes = new Uint8Array(e.size); let off = 0;
      for (let i = 0; i < e.n; i++) { const c = e.chunks.get(i); bytes.set(c, off); off += c.length; }
      partial.delete(msg.id); done.add(msg.id);
      onComplete({ id: msg.id, name: e.name, mime: e.mime, size: e.size, bytes, meta: e.meta });
      return true;
    },
    seen(id) { return done.has(id); },
    pending() { return partial.size; },
  };
}

// ── optional, image-only, browser-only: lossy downsample + JPEG ─────
/**
 * Downsample + JPEG-compress an image File/Blob to <= maxBytes. Returns a Blob.
 * Lossy by design — only for images; never used for documents.
 */
export async function compressImage(fileOrBlob, { maxBytes = 1_000_000, maxDim = 2048, mime = 'image/jpeg' } = {}) {
  // Use an <img> element + a plain <canvas> + toBlob — the most broadly supported
  // path (works on iOS Safari, where OffscreenCanvas/createImageBitmap/convertToBlob
  // are inconsistent). All steps reject loudly so failures surface, not silently die.
  const img = await loadImageEl(fileOrBlob);
  const baseW = img.naturalWidth || img.width, baseH = img.naturalHeight || img.height;
  if (!baseW || !baseH) throw new Error('could not read image dimensions');
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  let scale = Math.min(1, maxDim / Math.max(baseW, baseH));
  let quality = 0.9;
  const render = () => new Promise((resolve, reject) => {
    canvas.width = Math.max(1, Math.round(baseW * scale));
    canvas.height = Math.max(1, Math.round(baseH * scale));
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    canvas.toBlob((b) => (b ? resolve(b) : reject(new Error('canvas.toBlob returned null'))), mime, quality);
  });
  let blob = await render();
  for (let attempt = 0; attempt < 9 && blob.size > maxBytes; attempt++) {
    if (quality > 0.45) quality -= 0.15; else scale *= 0.8;   // drop quality first, then dimensions
    blob = await render();
  }
  return blob;
}
function loadImageEl(blob) {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(blob);
    const img = new Image();
    img.onload = () => { resolve(img); setTimeout(() => URL.revokeObjectURL(url), 5000); };
    img.onerror = () => { URL.revokeObjectURL(url); reject(new Error('image failed to load (unsupported format?)')); };
    img.src = url;
  });
}

export const _internals = { bytesToB64, b64ToBytes, DEFAULT_MAX_CHUNK };
