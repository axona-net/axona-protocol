// registry/snapshotMint.js — the DECODER-PRIVATE snapshot capability (refactor
// Phase 1, REF-1.1; S1h per Aster's S1g disposition). The shadow layer may
// reflect only on a value whose provenance is certified here, and the safety of
// that reflection must NOT depend on a mutable prototype chain or a
// realm-replaceable intrinsic.
//
// TRUST MODEL (explicit — Aster S1g #2): the security property holds under
// INTACT REALM INTRINSICS at kernel module-load time. This is the standard
// trust base for JavaScript security code; the kernel does not run inside a
// hardened realm. Two things follow:
//   * We capture a PRISTINE `JSON.parse` at module load (`_parse`). A consumer
//     that replaces the global `JSON.parse` AFTER load cannot make `certify`
//     brand a Proxy. A consumer that replaced it BEFORE the kernel imported this
//     module is outside the trust base (they could replace anything).
//   * The package `exports` map blocking this subpath is API ENCAPSULATION /
//     hygiene, NOT a security boundary. A consumer can still resolve the file by
//     URL and import `certify`. The security property is designed to hold even
//     then: `certify` takes serialized TEXT and builds a fresh graph with the
//     pristine parser, and the dispatcher classifies nodes WITHOUT touching any
//     prototype or constructor (see below), so a reachable `certify` cannot
//     produce a value whose observation fires a trap.
//
// PROVENANCE:
//   * certify brands EVERY reachable object/array node in a WeakSet (identity,
//     trap-free to check).
//   * Structural kind is recorded at CONSTRUCTION time in a decoder-private
//     WeakMap (`_kind`), read by `kindOf`. represent() uses that tag instead of
//     `instanceof` or `Array.isArray` on the live value — so a prototype swapped
//     after certification is never consulted and fires no trap (Aster S1g #1).

const _parse = JSON.parse;          // pristine parser captured at module load
const _certified = new WeakSet();
const _kind = new WeakMap();        // node -> frozen structural tag { k, len? }
const MAX_DEPTH = 8;
const MAX_NODES = 4096;

export function isCertified(x) { try { return _certified.has(x); } catch { return false; } }
// Construction-time structural tag (trap-free WeakMap read). null for a plain
// object (classified as 'obj' without prototype traversal) or an uncertified value.
export function kindOf(x) { try { return _kind.get(x) || null; } catch { return null; } }

// certify(serialized): parse a serialized frame (the decoder's own output from
// wire bytes) with the pristine parser and brand every reachable node, tagging
// its structural kind at construction. Input is text, so a Proxy can never enter
// the certified set. Returns the graph, or null on malformed input.
export function certify(serialized) {
  if (typeof serialized !== 'string') return null;
  let g; try { g = _parse(serialized); } catch { return null; }
  brandWalk(g, 0, { n: 0 });
  return g;
}

function brandWalk(v, depth, budget) {
  if (v === null || typeof v !== 'object') return;
  if (depth > MAX_DEPTH || budget.n >= MAX_NODES) return;   // beyond bounds: unbranded → skipped at read
  budget.n++;
  _certified.add(v);
  // v is fresh output of the pristine parser here (never a Proxy), so Array.isArray
  // and v.length are safe AT CONSTRUCTION. The result is frozen into a tag that
  // the dispatcher reads later without touching v.
  if (Array.isArray(v)) {
    _kind.set(v, Object.freeze({ k: 'arr', len: v.length }));
    for (let i = 0; i < v.length; i++) { if (budget.n >= MAX_NODES) break; brandWalk(v[i], depth + 1, budget); }
  } else {
    const ks = Object.keys(v);   // safe: pristine-parser output is a plain object
    for (let i = 0; i < ks.length; i++) { if (budget.n >= MAX_NODES) break; brandWalk(v[ks[i]], depth + 1, budget); }
  }
}

export default certify;
