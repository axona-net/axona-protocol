// registry/snapshotMint.js — the DECODER-PRIVATE snapshot capability (refactor
// Phase 1, REF-1.1; S1g per Aster's S1f disposition). The shadow layer may
// reflect only on a value whose provenance is certified here. Two properties the
// S1f WeakSet mint lacked:
//
//   1. UNFORGEABLE. `certify` is NOT re-exported by registry/index.js and this
//      module is blocked from the package `exports` map (see package.json:
//      "./pubsub/registry/snapshotMint.js": null). A public consumer cannot
//      import it, so it cannot mint membership for a hostile object. The mint
//      also never brands a caller-supplied object graph — it parses a serialized
//      frame itself, so a Proxy can never enter the certified set.
//
//   2. TRANSITIVE. `certify` brands EVERY reachable object and array node, and
//      the dispatcher checks membership before every reflective operation
//      (isCertified in shadowRegistry.js). A nested value that is not branded —
//      a nested Proxy, a Proxy inserted after minting, anything the mint did not
//      construct — is never touched. The membership check is a WeakSet identity
//      lookup, which fires no Proxy trap.
//
// The graph is not frozen: the handler receives its frame verbatim (shadow mode
// changes no behavior). Safety comes from construction (built from bytes, so
// Proxy-free) plus per-node membership (a post-mint insertion is unbranded and
// is skipped), not from immutability.

const _certified = new WeakSet();
const MAX_DEPTH = 8;
const MAX_NODES = 4096;

export function isCertified(x) { try { return _certified.has(x); } catch { return false; } }

// certify(serialized): parse a serialized frame (JSON text — the decoder's own
// output from wire bytes) into a fresh graph and brand every reachable node.
// Structurally cannot brand a caller-supplied Proxy: the input is text, and
// JSON.parse never yields a Proxy. Returns the parsed+branded graph, or null on
// malformed input. Internal callers only (the frame decoder).
export function certify(serialized) {
  if (typeof serialized !== 'string') return null;
  let g; try { g = JSON.parse(serialized); } catch { return null; }
  const budget = { n: 0 };
  brandWalk(g, 0, budget);
  return g;
}

function brandWalk(v, depth, budget) {
  if (v === null || typeof v !== 'object') return;
  if (depth > MAX_DEPTH || budget.n >= MAX_NODES) return;   // beyond bounds: leave unbranded → skipped at read
  budget.n++;
  _certified.add(v);
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) { if (budget.n >= MAX_NODES) break; brandWalk(v[i], depth + 1, budget); }
  } else {
    const ks = Object.keys(v);   // safe: JSON.parse output is a plain object, never a Proxy
    for (let i = 0; i < ks.length; i++) { if (budget.n >= MAX_NODES) break; brandWalk(v[ks[i]], depth + 1, budget); }
  }
}

export default certify;
