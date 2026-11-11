// =====================================================================
// fence_no_channel_sweep.mjs — kernel 4.132.0, the No-Channel Sweep
// (council eb305550; shape corrected by Vega 56751848).
//
// The table's invariant is "every synapse is a live channel". Until 4.132.0
// only a death EVENT enforced it. A member whose channel ended with no death
// reaching the kernel stayed in the table for the life of the process.
//
// What this fences (real AxonaPeer on the sim transport; the tick is driven
// directly; findKClosest is stubbed so the tick's dial phase has nothing to
// dial). A STRANDED member is a table row whose id the transport does not
// hold: no channel, no death event ever delivered for it.
//   A. OFF (default): synaptomeMaintain on, noChannelTicks unset. Five ticks.
//      The stranded member stays. No dead mark. No log row.
//   B. ARMED, N = 3: the member survives ticks 1 and 2 and is gone on tick 3.
//      It left through the DEATH BODY: dead-marked with reason no-channel,
//      pubsubPeerDied called with its id, a peer-died-evicted row with
//      reason no-channel, a no-channel-swept row naming ticks and addedBy.
//   C. A member with a LIVE channel is never swept (ten ticks).
//   D. IN FLIGHT: while the attempt guard reports an attempt to the id in
//      flight, no tick counts. The run resets: after the attempt ends it
//      takes N further ticks.
//   E. UNKNOWN: a tick on which isConnected THROWS counts nothing and resets
//      the run.
//   F. RECOVERY: a member that reads connected on one tick has its run
//      reset; N further not-connected ticks are needed.
//   G. The transport's own death event still runs the same body (one path,
//      two triggers): a real peer stopped → evicted, marked, purge called.
//   H. STATIC: the sweep is called in _maintainSynaptome after the reconcile
//      and above the deficit-backoff gate; the onPeerDied handler body is
//      the single call to _peerDeath.
//
// With the `this._sweepNoChannel()` call removed from the tick, B fails.
// With the in-flight read removed, D fails. With the catch on isConnected
// counting the tick, E fails. With _peerDeath replaced by a bare delete, B's
// mark and purge checks fail.
//
// Run: node test/fence_no_channel_sweep.mjs
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
const EXPECTED = 27;
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECTED) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECTED} checks`); process.exitCode = 1; } });

async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }   // the fence drives the tick
  const rec = { peer, id, transport, node, big: fromHex(id.id), hex: id.id, logs: [], purged: [], clock: 1_000_000 };
  peer._noChannelClock = () => rec.clock;                                     // the fence owns the sweep's clock
  peer.findKClosest = async () => [rec.big];                                  // nothing to dial
  const emit = peer._emitLog?.bind(peer);
  peer._emitLog = (lvl, ev, ctx) => { rec.logs.push({ ev, ctx }); return emit?.(lvl, ev, ctx); };
  peer._axonaManager = { pubsubPeerDied: (hex) => rec.purged.push(hex) };      // the pub/sub purge, recorded
  return rec;
}
/** A table row the transport does not hold: no channel, and no death was ever delivered for it. */
function strand(rec, xorSeed) {
  const id = rec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(rec.big ^ id) });
  syn.weight = 0.5; syn.inertia = 0; syn._addedBy = 'gate-admit';
  rec.node.synaptome.set(id, syn);
  return id;
}
const INTERVAL = 15000;                                                        // the maintain default the elapsed floor multiplies
const tick = (rec) => { rec.clock += INTERVAL; return rec.peer._maintainSynaptome(); };     // a pass one interval later
const pass = (rec) => rec.peer._maintainSynaptome();                           // a pass with NO time elapsed (a death-triggered pass)
const marked = (rec, id) => !!rec.node._deadPeers?.has?.(id);
const stopAll = async (...rs) => { for (const r of rs) await r.peer.stop().catch(() => {}); };

(async () => {
  console.log('fence_no_channel_sweep: a table member with no channel dies after N ticks, through the death body');
  const domain = new AxonaDomain();

  // A. OFF by default
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 1, 1, { synaptomeMaintain: true });
    const ghost = strand(a, 0x1111n);
    for (let i = 0; i < 5; i++) await tick(a);
    check('A off (default): the stranded member stays after five ticks', a.node.synaptome.has(ghost));
    check('A off: no dead mark, no sweep row', !marked(a, ghost) && !a.logs.some(l => l.ev === 'no-channel-swept'));
    await stopAll(a);
  }

  // B. ARMED, N = 3
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 2, 2, { synaptomeMaintain: { noChannelTicks: 3 } });
    const ghost = strand(a, 0x2222n);
    check('B precondition: the member is in the table and the transport does not hold it', a.node.synaptome.has(ghost) && a.transport.isConnected(ghost) === false);
    await tick(a); const after1 = a.node.synaptome.has(ghost);
    await tick(a); const after2 = a.node.synaptome.has(ghost);
    check('B armed N=3: present after ticks 1 and 2', after1 && after2, JSON.stringify({ after1, after2 }));
    await tick(a);
    check('B armed N=3: gone on tick 3', !a.node.synaptome.has(ghost));
    const mark = a.node._deadPeers?.get?.(ghost);
    check('B death body: dead-marked with reason no-channel', marked(a, ghost) && (mark?.cause ?? mark?.reason ?? null) === 'no-channel', JSON.stringify(mark ?? null));
    check('B death body: pubsubPeerDied called with the id', a.purged.includes(toHex(ghost)));
    const died = a.logs.find(l => l.ev === 'peer-died-evicted' && l.ctx?.peer === toHex(ghost));
    check('B death body: peer-died-evicted row, reason no-channel', died?.ctx?.reason === 'no-channel', JSON.stringify(died ?? null));
    const swept = a.logs.find(l => l.ev === 'no-channel-swept');
    check('B log: no-channel-swept names ticks 3 and addedBy gate-admit', swept?.ctx?.ticks === 3 && swept?.ctx?.addedBy === 'gate-admit' && swept?.ctx?.peer === toHex(ghost), JSON.stringify(swept ?? null));
    check('B counters: last tick swept 1; lifetime swept 1', a.peer._noChannelLast?.swept === 1 && a.peer._noChannelSwept === 1, JSON.stringify(a.peer._noChannelLast));
    await stopAll(a);
  }

  // C. a live channel is never swept
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 3, 3, { synaptomeMaintain: { noChannelTicks: 2 } });
    const b = await makePeer(net, domain, 3.1, 3.1, {});
    await b.transport.openConnection(a.hex); await wait(30);
    check('C precondition: b is bound to a and in a\'s table', a.transport.isConnected(b.big) === true && a.node.synaptome.has(b.big));
    for (let i = 0; i < 10; i++) await tick(a);
    check('C a member with a live channel survives ten ticks', a.node.synaptome.has(b.big) && !marked(a, b.big) && (a.peer._noChannelSwept ?? 0) === 0);
    await stopAll(a, b);
  }

  // D. in flight
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 4, 4, { synaptomeMaintain: { noChannelTicks: 2 } });
    const ghost = strand(a, 0x4444n);
    let inflight = true;
    a.peer._attemptGuard = { inflightOf: (id) => (id === ghost && inflight ? { token: 1 } : null) };
    for (let i = 0; i < 6; i++) await tick(a);
    check('D in flight: six ticks count nothing; the member stays', a.node.synaptome.has(ghost) && a.peer._noChannelLast?.inflight === 1 && a.peer._noChannelLast?.noChannel === 0, JSON.stringify(a.peer._noChannelLast));
    inflight = false;
    await tick(a);
    check('D attempt ended: one tick is not enough (the run restarted)', a.node.synaptome.has(ghost));
    await tick(a);
    check('D attempt ended: gone after N=2 further ticks', !a.node.synaptome.has(ghost));
    await stopAll(a);
  }

  // E. isConnected throws
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 5, 5, { synaptomeMaintain: { noChannelTicks: 2 } });
    const ghost = strand(a, 0x5555n);
    const real = a.transport.isConnected.bind(a.transport);
    let boom = false;
    a.transport.isConnected = (id) => { if (boom && id === ghost) throw new Error('transport read failed'); return real(id); };
    await tick(a);                       // run = 1
    boom = true; await tick(a);          // unknown: resets
    check('E unknown: a tick on which isConnected threw counts nothing', a.node.synaptome.has(ghost) && a.peer._noChannelLast?.unknown === 1 && a.peer._noChannelLast?.noChannel === 0, JSON.stringify(a.peer._noChannelLast));
    boom = false; await tick(a);         // run = 1 again
    check('E unknown: the run restarted; still present one tick later', a.node.synaptome.has(ghost));
    await tick(a);                       // run = 2
    check('E unknown: gone after N=2 consecutive not-connected ticks', !a.node.synaptome.has(ghost));
    await stopAll(a);
  }

  // F. recovery resets the run
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 6, 6, { synaptomeMaintain: { noChannelTicks: 2 } });
    const ghost = strand(a, 0x6666n);
    const real = a.transport.isConnected.bind(a.transport);
    let up = false;
    a.transport.isConnected = (id) => (id === ghost ? up : real(id));
    await tick(a);                       // run = 1
    up = true; await tick(a);            // connected: resets
    up = false; await tick(a);           // run = 1
    check('F recovery: connected on one tick resets the run', a.node.synaptome.has(ghost));
    await tick(a);                       // run = 2
    check('F recovery: gone only after N further not-connected ticks', !a.node.synaptome.has(ghost));
    await stopAll(a);
  }

  // G. the transport's own death still runs the same body
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 7, 7, { synaptomeMaintain: true });
    const b = await makePeer(net, domain, 7.1, 7.1, {});
    await b.transport.openConnection(a.hex); await wait(30);
    const had = a.node.synaptome.has(b.big);
    a.peer._peerDeath(b.big, 'socket-closed');
    check('G one body, two triggers: a reported death evicts, marks and purges', had && !a.node.synaptome.has(b.big) && marked(a, b.big) && a.purged.includes(toHex(b.big)) && a.logs.some(l => l.ev === 'peer-died-evicted' && l.ctx?.reason === 'socket-closed'));
    await stopAll(a, b);
  }

  // I. PASSES ARE NOT TIME (4.133.0). N = 3 passes with no time elapsed do not sweep;
  //    the member goes only once (N - 1) intervals have elapsed since the first.
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 8, 8, { synaptomeMaintain: { noChannelTicks: 3 } });
    const ghost = strand(a, 0x8888n);
    for (let i = 0; i < 6; i++) await pass(a);
    check('I six passes with no time elapsed sweep nothing', a.node.synaptome.has(ghost) && (a.peer._noChannelSwept ?? 0) === 0 && a.peer._noChannelLast?.noChannel === 1, JSON.stringify(a.peer._noChannelLast));
    a.clock += 2 * INTERVAL - 1; await pass(a);
    check('I one millisecond short of (N - 1) intervals: still present', a.node.synaptome.has(ghost));
    a.clock += 1; await pass(a);
    const swept = a.logs.find(l => l.ev === 'no-channel-swept');
    check('I at (N - 1) intervals the member dies; the row carries the elapsed ms', !a.node.synaptome.has(ghost) && swept?.ctx?.ms === 2 * INTERVAL && swept?.ctx?.ticks === 8, JSON.stringify(swept?.ctx));
    await stopAll(a);
  }

  // H. static
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const tickAt = src.indexOf('async _maintainSynaptome() {');
    const body = src.slice(tickAt, src.indexOf('\n  }\n', tickAt));
    const iRec = body.indexOf('this._reconcileBound();'), iSweep = body.indexOf('this._sweepNoChannel();'), iBack = body.indexOf('this._deficitBackoff && !this._deficitBackoff.allow()');
    check('H static: the sweep runs after the reconcile and above the search backoff', iRec >= 0 && iSweep > iRec && iBack > iSweep, JSON.stringify({ iRec, iSweep, iBack }));
    const h = src.slice(src.indexOf('this._onPeerDiedUnsub = transport.onPeerDied('), src.indexOf('AxonaPeer.onPeerDied: eviction failed'));
    check('H static: the onPeerDied handler calls _peerDeath and deletes nothing itself', h.includes('this._peerDeath(dead, reason)') && !h.includes('synaptome?.delete') && !h.includes('marks.fail'));
    check('H static: the sweep itself deletes no table row', (() => { const s = src.slice(src.indexOf('  _sweepNoChannel() {'), src.indexOf('  _scheduleMaintain() {')); return s.includes('this._peerDeath(key, \'no-channel\')') && !s.includes('synaptome.delete'); })());
  }

  console.log(`\nfence_no_channel_sweep: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
