// =====================================================================
// fence_no_anneal_below_cap.mjs — Hold-and-Fill v0.5/v0.7 (axona-docs
// 4334504, 95c2ff4), repair row 6: anneal is gone; _addByVitality is skipped
// at cap before its open.
//
// What this fences:
//   1. STATIC: AxonaPeer.js defines no _tryAnneal and emits no 'anneal-fired';
//      the transit site cools the temperature and calls nothing.
//   2. _addByVitality BELOW CAP is an admit of a bound candidate: with a real
//      bound peer not yet in the table, it opens (bound-only) and inserts;
//      with an unbound candidate it opens nothing and inserts nothing.
//   3. _addByVitality AT CAP is SKIPPED BEFORE THE OPEN (design case 36): no
//      openConnection call, no synaptome delete, no insert, no closeConnection,
//      `vitality-swap-skipped` counted and logged; the lowest-vitality
//      incumbent (the old victim) stays; a bound candidate stays bound and
//      an unbound one never had a channel.
//   4. TABLE NEVER DECREASES BELOW CAP under the paths this row touches: a
//      burst of hop_cache / lateral_spread-shaped candidates at cap leaves
//      the table exactly as it was.
//
// With the row-6 change reverted (anneal call and at-cap swap restored),
// checks 1 and 3 fail.
//
// Run: node test/fence_no_anneal_below_cap.mjs
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

async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}
function craft(rec, xorSeed, weight = 0.5) {
  const id = rec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(rec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'crafted';
  rec.node.synaptome.set(id, syn);
  return id;
}
const candidateSyn = (rec, peerBig, source = 'hopCache') => {
  const syn = new Synapse({ peerId: peerBig, latencyMs: 0, stratum: clz264(rec.big ^ peerBig) });
  syn.weight = 0.5; syn.inertia = 0; syn._addedBy = source;
  return syn;
};

(async () => {
  console.log('fence_no_anneal_below_cap: row 6 — anneal gone; no swap at cap');

  // 1. static
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const code = src.split('\n').filter(l => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n');   // strip comment lines
    check('1 no _tryAnneal definition or call in code', !/_tryAnneal\s*\(/.test(code));
    check('1 no anneal-fired emitter in code', !/['"]anneal-fired['"]/.test(code));
    check('1 the transit site still cools the temperature', /node\.temperature = Math\.max\(domain\.T_MIN, node\.temperature \* domain\.ANNEAL_COOLING\)/.test(code));
    check('1 _addByVitality has no victim path in code', !/victimAny/.test(code) && !/closeConnection\(victim\.peerId\)/.test(code.slice(code.indexOf('async _addByVitality'), code.indexOf('async _addByVitality') + 2500)));
  }

  const net = new SimNetwork(); const domain = new AxonaDomain();
  const a = await makePeer(net, domain, 10, 10, {});
  a.node._maxSynaptome = 4;
  const logs = [];
  const origLog = a.peer._emitLog.bind(a.peer);
  a.peer._emitLog = (lvl, msg, ctx) => { logs.push([msg, ctx]); return origLog(lvl, msg, ctx); };

  // 2. below cap: a bound candidate is admitted; an unbound one is not
  {
    const b = await makePeer(net, domain, 11, 11, {});
    await b.transport.openConnection(a.hex); await wait(15);
    a.node.synaptome.delete(b.big);                        // bound, not in the table
    check('2 setup: b bound, not in table, below cap', a.transport.isConnected(b.hex) && !a.node.synaptome.has(b.big) && a.node.synaptome.size < 4);
    const ok = await a.peer._addByVitality(candidateSyn(a, b.big));
    check('2 below cap: bound candidate admitted', ok === true && a.node.synaptome.has(b.big));
    const strangerBig = a.big ^ (1n << 200n);
    const calls = []; const origOpen = a.transport.openConnection.bind(a.transport);
    a.transport.openConnection = async (id) => { calls.push(id); return origOpen(id); };
    const ok2 = await a.peer._addByVitality(candidateSyn(a, strangerBig));
    check('2 below cap: unbound candidate: open attempted (bound-only → false), nothing inserted', ok2 === false && calls.length === 1 && !a.node.synaptome.has(strangerBig));
    a.transport.openConnection = origOpen;
  }

  // 3. at cap: skipped before the open
  {
    while (a.node.synaptome.size < 4) craft(a, 1n << BigInt(100 + a.node.synaptome.size), 0.9);
    const weakest = craft(a, 1n << 90n, 0.01); a.node.synaptome.delete(weakest);   // make room, then re-add as the lowest vitality
    // ensure exactly at cap with the weakest present
    if (a.node.synaptome.size >= 4) { const k = [...a.node.synaptome.keys()].find(x => a.node.synaptome.get(x)._addedBy === 'crafted'); a.node.synaptome.delete(k); }
    a.node.synaptome.set(weakest, Object.assign(new Synapse({ peerId: weakest, latencyMs: 50, stratum: clz264(a.big ^ weakest) }), { weight: 0.01, inertia: 0, _addedBy: 'crafted' }));
    check('3 setup: exactly at cap with a low-vitality incumbent', a.node.synaptome.size === 4 && a.node.synaptome.has(weakest), String(a.node.synaptome.size));
    const before = new Set(a.node.synaptome.keys());
    const opens = [], closes = [];
    const origOpen = a.transport.openConnection.bind(a.transport); const origClose = a.transport.closeConnection.bind(a.transport);
    a.transport.openConnection = async (id) => { opens.push(id); return origOpen(id); };
    a.transport.closeConnection = async (id) => { closes.push(id); return origClose(id); };
    // a bound candidate at cap
    const c = await makePeer(net, domain, 12, 12, {});
    await c.transport.openConnection(a.hex); await wait(15);
    a.node.synaptome.delete(c.big);                        // (the seed admitted it over cap on the legacy path; take it back out)
    while (a.node.synaptome.size < 4) a.node.synaptome.set(weakest, a.node.synaptome.get(weakest) ?? Object.assign(new Synapse({ peerId: weakest, latencyMs: 50, stratum: clz264(a.big ^ weakest) }), { weight: 0.01, inertia: 0, _addedBy: 'crafted' }));
    const before2 = new Set(a.node.synaptome.keys());
    logs.length = 0;
    const r1 = await a.peer._addByVitality(candidateSyn(a, c.big, 'lateral'));
    check('3 at cap, bound candidate: returns false', r1 === false);
    check('3 at cap: NO openConnection call', opens.length === 0, String(opens.length));
    check('3 at cap: NO closeConnection call', closes.length === 0);
    check('3 at cap: table unchanged (no delete, no insert)', a.node.synaptome.size === 4 && [...before2].every(k => a.node.synaptome.has(k)) && !a.node.synaptome.has(c.big));
    check('3 at cap: the low-vitality incumbent stays', a.node.synaptome.has(weakest));
    check('3 at cap: bound candidate stays bound', a.transport.isConnected(c.hex));
    check('3 at cap: counted and logged vitality-swap-skipped', a.peer._vitalitySwapSkipped === 1 && logs.some(([m, x]) => m === 'vitality-swap-skipped' && x?.cap === 4 && x?.source === 'lateral'));
    // an unbound candidate at cap
    const strangerBig = a.big ^ (1n << 150n);
    const r2 = await a.peer._addByVitality(candidateSyn(a, strangerBig));
    check('3 at cap, unbound candidate: skipped, no open, counter 2', r2 === false && opens.length === 0 && a.peer._vitalitySwapSkipped === 2 && !a.transport.isConnected(toHex(strangerBig)));
    // 4. a burst of candidates at cap leaves the table exactly as it was
    for (let i = 0; i < 25; i++) await a.peer._addByVitality(candidateSyn(a, a.big ^ (1n << BigInt(20 + i)), i % 2 ? 'hopCache' : 'lateral'));
    check('4 burst at cap: table identical, nothing opened or closed', a.node.synaptome.size === 4 && [...before2].every(k => a.node.synaptome.has(k)) && opens.length === 0 && closes.length === 0 && a.peer._vitalitySwapSkipped === 27);
    a.transport.openConnection = origOpen; a.transport.closeConnection = origClose;
    void before;
    await c.peer.stop().catch(() => {}); await c.transport.stop().catch(() => {});
  }

  await a.peer.stop().catch(() => {}); await a.transport.stop().catch(() => {});
  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
