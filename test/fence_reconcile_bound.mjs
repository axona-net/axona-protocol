// =====================================================================
// fence_reconcile_bound.mjs — Hold-and-Fill v0.15 (axona-docs e4809d2),
// repair row 7: the fill tick RECONCILES first — every BOUND identity not in
// the table is offered to admit(), with zero new dial (design case 2).
//
// What this fences (real AxonaPeer on the sim transport; the tick is driven
// directly; findKClosest is stubbed so the tick's dial phase has nothing to
// dial and every openConnection / _considerCandidate call is recorded):
//   A. GATED: with synaptomeMaintain unset the tick is inert and reconciles
//      nothing; three bound-not-in-table peers stay out of the table.
//   B. Flag on, gate off, below cap: three bound-not-in-table peers are
//      admitted by the tick (back in the table), with ZERO dials; the
//      counters and the `synaptome-reconcile` log line agree; a second tick
//      offers nothing (idempotent).
//   C. Case 2 — flag on, gate ARMED, table at cap−1 with three bound-not-in-
//      table peers: exactly one is admitted, two are refused and stay BOUND
//      (still connected, not in the table) with their grace timers armed;
//      zero dials. The next tick offers the two again and admits neither.
//   D. The tick's own dial phase still runs after the reconcile and still
//      skips bound peers (no dial for a refused one).
//   E. STATIC: _reconcileBound is called before findKClosest in
//      _maintainSynaptome and returns before anything when _maintainCfg is
//      unset.
//
// With the reconcile call removed from the tick, B and C fail.
//
// Run: node test/fence_reconcile_bound.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 8) : x));

async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }   // the fence drives the tick
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}
function craft(rec, xorSeed, weight = 0.5) {
  const id = rec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'crafted';
  rec.node.synaptome.set(id, syn);
  return id;
}
/** Bind three real peers to `a` and take them OUT of the table: bound-not-in-table. */
async function threeBoundOut(net, domain, a) {
  const ps = [];
  for (let i = 0; i < 3; i++) { const p = await makePeer(net, domain, 20 + i, 20 + i, {}); await p.transport.openConnection(a.hex); ps.push(p); }
  await wait(30);
  for (const p of ps) a.node.synaptome.delete(p.big);
  return ps;
}
const spy = (a) => { const calls = { open: [], consider: [] }; const oo = a.transport.openConnection.bind(a.transport); a.transport.openConnection = async (id) => { calls.open.push(id); return oo(id); }; const oc = a.peer._considerCandidate.bind(a.peer); a.peer._considerCandidate = async (id, s) => { calls.consider.push({ id, s }); return oc(id, s); }; return calls; };

(async () => {
  console.log('fence_reconcile_bound: row 7 — the tick reconciles bound-not-in-table peers first, dialing nothing');
  const domain = new AxonaDomain();

  // A. gated
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 1, 1, {});
    const ps = await threeBoundOut(net, domain, a);
    check('A setup: three peers bound to a and NOT in its table', ps.every(p => a.transport.isConnected(p.hex) && !a.node.synaptome.has(p.big)));
    a.peer.findKClosest = async () => [a.big];
    const n = await a.peer._maintainSynaptome();
    check('A flag OFF: the tick is inert — nothing reconciled, the three stay out', n === 0 && ps.every(p => !a.node.synaptome.has(p.big)) && (a.peer._reconcileLast == null || a.peer._reconcileLast.offered === 0));
    for (const x of [a, ...ps]) await x.peer.stop().catch(() => {});
  }

  // B. flag on, gate off, below cap
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 2, 2, { synaptomeMaintain: true });
    a.node._maxSynaptome = 20;
    const ps = await threeBoundOut(net, domain, a);
    check('B setup: bound-not-in-table ×3, flag on, gate off', ps.every(p => a.transport.isConnected(p.hex) && !a.node.synaptome.has(p.big)) && !!a.peer._maintainCfg && !a.peer._gateCfg);
    const calls = spy(a);
    const logs = []; const ol = a.peer._emitLog.bind(a.peer); a.peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
    a.peer.findKClosest = async () => [a.big];   // the dial phase finds nothing to dial
    await a.peer._maintainSynaptome();
    check('B the tick ADMITTED the three (back in the table)', ps.every(p => a.node.synaptome.has(p.big)), J(ps.map(p => a.node.synaptome.has(p.big))));
    check('B zero dials: no openConnection, no _considerCandidate', calls.open.length === 0 && calls.consider.length === 0, J(calls));
    check('B counters: offered 3, admitted 3, refused 0; log line present', a.peer._reconcileLast?.offered === 3 && a.peer._reconcileLast?.admitted === 3 && a.peer._reconcileLast?.refused === 0 && logs.some(([m, c]) => m === 'synaptome-reconcile' && c?.admitted === 3), J(a.peer._reconcileLast));
    await a.peer._maintainSynaptome();
    check('B a second tick offers nothing (idempotent)', a.peer._reconcileLast?.offered === 0);
    for (const x of [a, ...ps]) await x.peer.stop().catch(() => {});
  }

  // C. case 2: gate armed, cap−1, three bound-not-in-table → one admitted, two refused and charged, zero dials
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 3, 3, { synaptomeMaintain: true, admissionGate: { kNear: 5, sparseFloor: 2, closeGraceMs: 60000 } });
    a.peer._requireAxonaManager('fence');
    const ps = await threeBoundOut(net, domain, a);
    // fill to cap−1 with crafted entries (the gate's own sim shape: 5 near, 2 sparse, dense rest)
    a.node._maxSynaptome = 11;
    [1n, 2n, 3n, 4n, 5n].forEach(x => craft(a, x)); craft(a, 1n << 259n); craft(a, 1n << 258n);
    craft(a, (1n << 263n) + 7n, 0.9); craft(a, (1n << 262n) + 3n, 0.9); craft(a, (1n << 261n) + 9n, 0.9);
    check('C setup: cap 11, table 10 (cap−1), three bound-not-in-table, gate armed', a.node.synaptome.size === 10 && ps.every(p => a.transport.isConnected(p.hex) && !a.node.synaptome.has(p.big)) && !!a.peer._gateCfg, String(a.node.synaptome.size));
    const calls = spy(a);
    a.peer.findKClosest = async () => [a.big];
    await a.peer._maintainSynaptome();
    const inTable = ps.filter(p => a.node.synaptome.has(p.big)); const out = ps.filter(p => !a.node.synaptome.has(p.big));
    check('C exactly ONE admitted, table at cap', inTable.length === 1 && a.node.synaptome.size === 11, J({ inTable: inTable.length, size: a.node.synaptome.size }));
    check('C two REFUSED: still bound (connected), out of the table, grace timers armed', out.length === 2 && out.every(p => a.transport.isConnected(p.hex) && a.peer._gracePending.has(p.big)), J({ out: out.length, grace: out.map(p => a.peer._gracePending.has(p.big)) }));
    check('C zero dials', calls.open.length === 0 && calls.consider.length === 0, J(calls));
    check('C counters: offered 3, admitted 1, refused 2', a.peer._reconcileLast?.offered === 3 && a.peer._reconcileLast?.admitted === 1 && a.peer._reconcileLast?.refused === 2, J(a.peer._reconcileLast));
    await a.peer._maintainSynaptome();
    check('C next tick: the two are offered again and refused again; still no dial', a.peer._reconcileLast?.offered === 2 && a.peer._reconcileLast?.admitted === 0 && calls.open.length === 0, J(a.peer._reconcileLast));
    // D. the dial phase still runs and skips bound peers
    a.peer.findKClosest = async () => [a.big, out[0].big, a.big ^ (1n << 150n)];
    await a.peer._maintainSynaptome();
    check('D the dial phase skipped the refused BOUND peer and considered only the stranger', !calls.consider.some(c => c.id === out[0].big) && calls.consider.some(c => c.id === (a.big ^ (1n << 150n))), J(calls.consider));
    for (const t of a.peer._gracePending.values()) clearTimeout(t);
    for (const x of [a, ...ps]) await x.peer.stop().catch(() => {});
  }

  // E. static
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const s = src.indexOf('async _maintainSynaptome('); const e = src.indexOf('\n  }\n', s); const body = src.slice(s, e);
    check('E _maintainSynaptome reconciles BEFORE findKClosest', body.indexOf('this._reconcileBound()') > 0 && body.indexOf('this._reconcileBound()') < body.indexOf('this.findKClosest(self'));
    const r = src.indexOf('  _reconcileBound() {'); const rb = src.slice(r, src.indexOf('\n  }\n', r));
    check('E _reconcileBound returns before anything when the flag is unset', /if \(!this\._maintainCfg \|\| !node\?\.synaptome \|\| typeof t\?\.boundPeers !== 'function'\) return out;/.test(rb));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
