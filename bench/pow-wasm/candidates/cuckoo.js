// Cuckoo-Cycle-style ASYMMETRIC PoW candidate — memory-(bandwidth-)hard to SOLVE,
// trivially cheap to VERIFY. Faithful reference implementation (J. Tromp, Cuckoo
// Cycle 2014): build a large random bipartite graph from siphash, find a cycle.
// Solve cost is dominated by holding + scanning the big edge arrays; verify just
// re-derives the cycle's edges and checks they close. Pure JS, no dependency.
//
// DIFFICULTY = EDGE-BITS (graph size) — the memory/phone-floor axis. The graph
// is 2^edgebits edges over 2^(edgebits-1) nodes/side (avg degree ~2 ⇒ cycles
// exist w.h.p.), so peak memory ≈ 2^edgebits · 8 bytes.
//
// NOTE: a benchmark-faithful variant — exact algorithm + cost profile + cheap
// verify, with its own parameterisation (any-length cycle, not Tromp's fixed
// L=42). Correct + measurable; an audited/optimised solver is a separate step
// before this is the *kernel* PoW.

const M = (1n << 64n) - 1n;
const rotl = (x, b) => ((x << b) | (x >> (64n - b))) & M;
function sipround(v) {
  let [a, b, c, d] = v;
  a = (a + b) & M; b = rotl(b, 13n); b ^= a; a = rotl(a, 32n);
  c = (c + d) & M; d = rotl(d, 16n); d ^= c;
  a = (a + d) & M; d = rotl(d, 21n); d ^= a;
  c = (c + b) & M; b = rotl(b, 17n); b ^= c; c = rotl(c, 32n);
  return [a, b, c, d];
}
// Tromp-style siphash-2-4 over a single u64 nonce.
function siphash(k, nonce) {
  let v = [k[0], k[1], k[2], k[3] ^ nonce];
  v = sipround(v); v = sipround(v);
  v[0] ^= nonce; v[2] ^= 0xffn;
  v = sipround(v); v = sipround(v); v = sipround(v); v = sipround(v);
  return (v[0] ^ v[1] ^ v[2] ^ v[3]) & M;
}
async function deriveKeys(pubkeyHex, nonce) {
  const bytes = new TextEncoder().encode(`cuckoo:${pubkeyHex}:${nonce}`);
  const h = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  const k = [];
  for (let i = 0; i < 4; i++) {
    let x = 0n;
    for (let j = 7; j >= 0; j--) x = (x << 8n) | BigInt(h[i * 8 + j]);   // little-endian u64
    k.push(x);
  }
  return k;
}

let _peak = 0;

async function solveGraph(pubkeyHex, nonce, edgebits) {
  const nEdges = 2 ** edgebits;
  const nps = 2 ** (edgebits - 1);                 // nodes per side
  const k = await deriveKeys(pubkeyHex, nonce);
  const U = new Uint32Array(nEdges);
  const V = new Uint32Array(nEdges);
  _peak = U.byteLength + V.byteLength + (2 * nps) * 4;
  const npsB = BigInt(nps);
  for (let i = 0; i < nEdges; i++) {
    U[i] = Number(siphash(k, BigInt(2 * i)) % npsB);
    V[i] = Number(siphash(k, BigInt(2 * i + 1)) % npsB);
  }
  // union-find over 2*nps nodes (U-side 0..nps-1, V-side nps..2nps-1) + a tree
  // adjacency of accepted edges, so a back-edge can be traced into a cycle.
  const parent = new Int32Array(2 * nps).fill(-1);
  const find = (x) => { while (parent[x] >= 0) { if (parent[parent[x]] >= 0) parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
  const adj = new Map();                           // node → [{to, edge}]
  const link = (a, b, e) => { (adj.get(a) || adj.set(a, []).get(a)).push({ to: b, edge: e }); (adj.get(b) || adj.set(b, []).get(b)).push({ to: a, edge: e }); };

  for (let i = 0; i < nEdges; i++) {
    const a = U[i], b = nps + V[i];
    const ra = find(a), rb = find(b);
    if (ra === rb) {
      const cyc = trace(adj, a, b);                // path of edge indices a..b in the forest
      if (cyc && cyc.length >= 3) return [...cyc, i];   // + this back-edge = cycle (len ≥ 4)
    } else {
      parent[ra] = rb;
      link(a, b, i);
    }
  }
  return null;
}

// BFS over the forest's accepted edges from a to b → list of edge indices.
function trace(adj, a, b) {
  const prev = new Map([[a, { node: -1, edge: -1 }]]);
  const q = [a];
  while (q.length) {
    const x = q.shift();
    if (x === b) break;
    for (const { to, edge } of (adj.get(x) || [])) {
      if (!prev.has(to)) { prev.set(to, { node: x, edge }); q.push(to); }
    }
  }
  if (!prev.has(b)) return null;
  const edges = [];
  for (let x = b; x !== a; ) { const p = prev.get(x); edges.push(p.edge); x = p.node; }
  return edges;
}

export const name = 'cuckoo-cycle (asymmetric, memory-bandwidth-hard)';
export const suiteDifficulties = [16, 18, 20];        // EDGE-BITS → mem ≈ 2^bits·8B (~0.8MB … ~12MB).
// NOTE: capped low because the BigInt siphash makes mint compute-bound — a
// 32-bit-limb / WASM siphash is needed to reach the OOM-relevant 128–512 MB
// graphs. Slow devices will timeout-skip the high end (fault tolerance).
export const difficultyLabel = 'edge-bits';

export async function mint(pubkeyHex, edgebits) {
  for (let nonce = 0; nonce < 200; nonce++) {
    const cyc = await solveGraph(pubkeyHex, nonce, edgebits);
    if (cyc) return JSON.stringify({ nonce, cycle: cyc });   // witness: nonce + cycle edge indices
  }
  throw new Error('no cycle found within nonce budget');
}

export async function verify(pubkeyHex, witness, edgebits) {
  let parsed; try { parsed = JSON.parse(witness); } catch { return false; }
  const { nonce, cycle } = parsed || {};
  if (!Array.isArray(cycle) || cycle.length < 4 || cycle.length % 2 !== 0) return false;
  const nps = 2 ** (edgebits - 1);
  const npsB = BigInt(nps);
  const k = await deriveKeys(pubkeyHex, nonce);
  // Re-derive each cycle edge's endpoints; check consecutive edges share exactly
  // one endpoint and the chain closes into a single loop (cheap — |cycle| sips).
  if (new Set(cycle).size !== cycle.length) return false;        // distinct edges
  const ep = cycle.map((e) => {
    if (!Number.isInteger(e) || e < 0 || e >= 2 ** edgebits) return null;
    return [Number(siphash(k, BigInt(2 * e)) % npsB), nps + Number(siphash(k, BigInt(2 * e + 1)) % npsB)];
  });
  if (ep.some((x) => x === null)) return false;
  // A valid cycle is a 2-regular single loop (order-independent): build
  // node→edges, require every node degree 2 and nodes == edges, then walk once
  // and confirm it returns to the start having used ALL edges (one cycle, not
  // several disjoint ones).
  const adj = new Map();
  ep.forEach(([u, v], i) => { (adj.get(u) || adj.set(u, []).get(u)).push(i); (adj.get(v) || adj.set(v, []).get(v)).push(i); });
  for (const lst of adj.values()) if (lst.length !== 2) return false;
  if (adj.size !== ep.length) return false;
  const used = new Array(ep.length).fill(false);
  const start = ep[0][0]; let node = start, steps = 0;
  do {
    const e = (adj.get(node) || []).find((x) => !used[x]);
    if (e === undefined) return false;
    used[e] = true; steps++;
    const [u, v] = ep[e];
    node = (u === node) ? v : u;
  } while (node !== start && steps <= ep.length);
  return steps === ep.length && node === start;
}

export function peakMemoryBytes() { return _peak; }
export function reset() { _peak = 0; }
