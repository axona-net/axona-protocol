// =====================================================================
// @axona/protocol/std/publisher — manage publish IDs.
//
// The publishId is the pub/sub layer's dedup / exactly-once token. It is
// DECOUPLED from the transport id (which is always ephemeral — recomputed every
// restart). So if an app wants a logical publish stream to stay continuous
// across restarts (or across a rotated transport id, or multiple devices), it
// owns and persists its publishId here — not the kernel.
//
//   import { persistentPublisher } from '@axona/protocol/std';
//   const pub = persistentPublisher('sightings');      // survives reload (localStorage)
//   await peer.pub(topic, msg, { publishId: pub.next() });
//
// A Publisher = a stable base id + a monotonic counter; next() yields a unique
// per-event publishId (`<base>:<n>`) and (optionally) persists the counter so it
// never reuses a value across restarts. Run as many as you like — one per
// channel / file transfer / logical sender — each with its own storage key.
//
//   createPublisher()              — ephemeral (no persistence)
//   persistentPublisher(key)       — browser-localStorage-backed by default
//   createPublisher({ store, key })— bring your own { get, set } store
// =====================================================================

function randHex(n = 16) {
  const c = globalThis.crypto;
  if (c?.getRandomValues) {
    const b = c.getRandomValues(new Uint8Array(n));
    return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
  }
  let s = ''; for (let i = 0; i < n * 2; i++) s += Math.floor(Math.random() * 16).toString(16);
  return s;
}

/** Browser localStorage as a sync { get, set } store, or null if unavailable. */
export function defaultStore() {
  try {
    if (typeof localStorage !== 'undefined') {
      return { get: (k) => localStorage.getItem(k), set: (k, v) => localStorage.setItem(k, v) };
    }
  } catch { /* access denied (sandboxed iframe, etc.) */ }
  return null;
}

/**
 * Create a publisher (publish-ID stream).
 * @param {object}  [opts]
 * @param {string}  [opts.id]    fixed base id (else a random S2-free token is minted)
 * @param {number}  [opts.seq=0] starting sequence
 * @param {{get,set}} [opts.store] persistence store (sync get/set); if set, the
 *                    sequence is written on every next() so it never reuses a value
 * @param {string}  [opts.key]   storage key (default 'axona:publisher')
 * @returns {{ id:string, seq:number, next():string, toJSON():object }}
 */
export function createPublisher({ id = null, seq = 0, store = null, key = 'axona:publisher' } = {}) {
  const baseId = id || ('pub_' + randHex());      // S2-free, decoupled from the transport id
  let n = Number.isFinite(seq) ? seq : 0;
  const save = () => { if (store) { try { store.set(key, JSON.stringify({ id: baseId, seq: n })); } catch { /* best-effort */ } } };
  return {
    id: baseId,
    get seq() { return n; },
    /** A fresh, unique-per-event publishId; advances + persists the sequence. */
    next() { n += 1; save(); return `${baseId}:${n}`; },
    toJSON() { return { id: baseId, seq: n }; },
  };
}

/**
 * A publisher whose stream survives restarts. Restores { id, seq } from `store`
 * (browser localStorage by default) so it continues where it left off — no reuse,
 * no reset-to-zero collision — and creates + persists a fresh one if none saved.
 * Use a distinct `key` per logical stream to run several at once.
 * @param {string} [key='axona:publisher']
 * @param {{ store?: {get,set} }} [opts]
 */
export function persistentPublisher(key = 'axona:publisher', { store = defaultStore() } = {}) {
  let saved = null;
  if (store) { try { const s = store.get(key); if (s) saved = JSON.parse(s); } catch { /* corrupt → fresh */ } }
  const pub = createPublisher({ id: saved?.id, seq: saved?.seq ?? 0, store, key });
  if (store && !saved) { try { store.set(key, JSON.stringify(pub.toJSON())); } catch { /* best-effort */ } }
  return pub;
}
