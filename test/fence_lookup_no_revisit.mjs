#!/usr/bin/env node
// =====================================================================
// fence_lookup_no_revisit.mjs — dual-root packet (c) (axona-docs 14a6bd8,
// Part 4(c); David "build it" 2026-10-17).
//
// THE LOOKUP WALK NEVER VISITS A NODE TWICE, AND A WALK THAT CANNOT ADVANCE
// SAYS SO. `ctx.queried` crosses hops, but until 2026-10-17 only the fallback
// branch read it: the main candidate loops admitted any synapse closer than
// the current node, so the closest node S answered a fresh neighbour Y by
// the fallback, and Y, whose only closer synapse is S, sent the query back.
// Three fresh #council lookups that day ran 41 hops each with S visited
// sixteen times, ended on an arbitrary far node, and the root self-verify
// read that node as the terminus.
//
// The ring here: A (origin) → Y1 → S (closest to the key, no closer
// neighbour) → S's fresh neighbours Y2..Y6, each knowing only S. With the
// fix: A, Y1, S, Y2, stalled — S once, hops 3, closest = S. Without it:
// S, Y2, S, Y3, S … S six times.
//
// Mutant (red): delete the three `ctx.queried.has(…)` exclusions in the main
// candidate loops → checks 1 and 2 fail (S repeats, hops climb).
// Run: node test/fence_lookup_no_revisit.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const hx = (b) => b.toString(16).slice(0, 8);

const net = new SimNetwork();
const domain = new AxonaDomain();

async function makePeer(lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  return { id, hex: id.id, big: fromHex(id.id), node, peer, transport };
}
const link = async (a, b) => {
  a.node.synaptome.set(b.big, new Synapse({ peerId: b.big, latencyMs: 50, stratum: clz264(a.big ^ b.big) }));
  b.node.synaptome.set(a.big, new Synapse({ peerId: a.big, latencyMs: 50, stratum: clz264(b.big ^ a.big) }));
  await a.transport.openConnection(b.hex);
  await wait(10);
};

console.log('fence_lookup_no_revisit: (c) — the walk never revisits and reports a stall');

// Build seven peers, then pick the KEY and the roles by XOR distance so the
// ring is what the comment says it is: S closest, Y1 and Y2..Y6 between S
// and A, A farthest. Identities are random, so sort by distance to a key.
const raw = [];
for (let i = 0; i < 7; i++) raw.push(await makePeer(40 + i * 0.01, -70 + i * 0.01));
const KEY = raw[0].big ^ (1n << 200n);
raw.sort((p, q) => ((p.big ^ KEY) < (q.big ^ KEY) ? -1 : 1));
const [S, Y1, Y2, Y3, Y4, Y5, A] = raw;        // S nearest the key, A farthest
// Edges: A—Y1, Y1—S, S—Y2..Y5. Y1 also knows A (ring back is possible only via S).
await link(A, Y1); await link(Y1, S);
for (const Y of [Y2, Y3, Y4, Y5]) await link(S, Y);

const r = await A.peer.lookup(KEY);
const path = r.path.map((b) => (typeof b === 'bigint' ? b : fromHex(String(b))));
const counts = new Map(); for (const b of path) counts.set(b, (counts.get(b) || 0) + 1);
const sTimes = counts.get(S.big) || 0;
const anyRepeat = [...counts.values()].some((n) => n > 1);
console.log(`  path: ${path.map(hx).join(' > ')}  hops=${r.hops} found=${r.found} stalled=${r.stalled} closest=${r.closest == null ? null : hx(r.closest)}`);

check('1 no node appears twice in the path', !anyRepeat, path.map(hx).join(' > '));
check('2 the closest node S is visited exactly once', sTimes === 1, `S×${sTimes}`);
check('3 the walk ends STALLED, not at the hop cap', r.stalled === true && r.found === false && r.hops < domain.MAX_HOPS, `stalled=${r.stalled} hops=${r.hops}`);
check('4 hops are bounded by the distinct nodes reachable (≤ 4 here)', r.hops <= 4, `hops=${r.hops}`);
check('5 the result names S as the closest visited node', r.closest === S.big, `closest=${r.closest == null ? null : hx(r.closest)} S=${hx(S.big)}`);
check('6 the path visits S (the walk still reaches the true terminus)', sTimes >= 1);

for (const p of raw) { try { await p.peer.stop(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } }
console.log(`\nfence_lookup_no_revisit: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
