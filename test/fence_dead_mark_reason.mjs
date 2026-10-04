// =====================================================================
// fence_dead_mark_reason.mjs — Hold-and-Fill v0.5 (axona-docs 4334504),
// repair row 1: a dead-peer mark carries a reason; the filter behaves as
// today.
//
// What this fences:
//   1. A peer whose channel dies is marked with { kind: 'loss', cause, at }.
//      The sim transport supplies no cause, so cause is 'unknown', which is
//      the kernel's existing log convention (4.76.3).
//   2. The readers see what they saw with the Set: has() is true for a dead
//      id, greedy routing skips it, and a re-bind deletes the mark.
//   3. The Set-compatible writer the bridge uses (add(id)) still works and
//      produces a mark with cause 'unknown'.
//   4. The table exists on a fresh NeuronNode before any death, so a foreign
//      died-handler wired first does not install a bare Set over it.
//   5. A known cause (the web transport's 4.76.3 close reason) is forwarded
//      into the mark, and a later no-information add() for the same death
//      keeps it (Aster fb79c09e).
//   6. A bare Set installed over the table by another owner keeps working:
//      membership is recorded, nothing throws, no mark exists.
//
// With the row-1 wiring removed (a bare Set), checks 1, 4 and 5 fail: a Set
// has no get(), no mark(), and a fresh node has no table. Checks 2 and 6 pass
// with either shape, which is the "no behaviour change" half of the fence.
//
// Run: node test/fence_dead_mark_reason.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { DeadPeers }                from '../src/dht/DeadPeers.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));

async function makePeer(net, domain, lat, lng, identity = null) {
  const id = identity ?? await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}

(async () => {
  console.log('fence_dead_mark_reason: row 1 — a dead-peer mark carries a reason');
  const net    = new SimNetwork();
  const domain = new AxonaDomain();

  // 4. The table exists before any death, as the right shape.
  const fresh = new NeuronNode({ id: 1n, lat: 0, lng: 0 });
  check('4 fresh node has a DeadPeers table', fresh._deadPeers instanceof DeadPeers, String(fresh._deadPeers));
  check('4 fresh table is empty', fresh._deadPeers?.size === 0);

  const a = await makePeer(net, domain, 40.0, -74.0);
  const b = await makePeer(net, domain, 40.1, -74.1);

  await a.transport.openConnection(b.hex); await wait(15);
  check('setup: a is connected to b', a.transport.isConnected(b.big));
  check('setup: b not marked dead at a', !a.node._deadPeers?.has(b.big));

  // 1. b's channel dies: a marks it with a reason.
  const before = Date.now();
  await b.transport.stop(); await wait(15);
  const m = a.node._deadPeers.get?.(b.big);      // a bare Set has no get(): the mark is then undefined and checks 1 fail
  check('1 b is marked dead at a', a.node._deadPeers.has(b.big));
  check('1 mark is an object', m !== null && typeof m === 'object', String(m));
  check('1 mark.kind is loss', m?.kind === 'loss', String(m?.kind));
  check('1 mark.cause is unknown on the sim transport', m?.cause === 'unknown', String(m?.cause));
  check('1 mark.at is a timestamp from this run', Number.isFinite(m?.at) && m.at >= before && m.at <= Date.now() + 1, String(m?.at));

  // 2. The filter behaves as today: greedy routing skips the dead id even
  //    when a synapse to it is present.
  const stratum = clz264(a.big ^ b.big);
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'crafted';
  a.node.synaptome.set(b.big, syn);
  check('2 greedy next hop toward b skips the dead b', a.peer._greedyNextHopToward(b.big) !== b.big);

  // 2. A re-bind deletes the mark (onPeerBound at AxonaPeer start).
  const b2 = await makePeer(net, domain, 40.1, -74.1, b.id);
  await a.transport.openConnection(b2.hex); await wait(15);
  check('2 re-bind deletes the mark', !a.node._deadPeers.has(b.big));
  check('2 after re-bind greedy next hop toward b is b', a.peer._greedyNextHopToward(b.big) === b.big);

  // 3. The Set-compatible writer, and how it composes with mark() in both
  //    orders (Aster fb79c09e): add() is membership only and never
  //    overwrites a known mark; mark() always records the latest cause.
  const t = new DeadPeers();
  t.add(7n);
  check('3 add(id) marks with cause unknown', t.get(7n)?.cause === 'unknown' && t.get(7n)?.kind === 'loss');
  check('3 add(id) counts as one entry', t.size === 1 && t.has(7n));
  t.mark(7n, { cause: 'pong-timeout' });
  check('3 add then mark: mark() replaces the entry, latest cause wins', t.get(7n)?.cause === 'pong-timeout' && t.size === 1);
  t.mark(8n, { kind: 'policy', cause: 'identity', at: 5 });
  check('3 mark() keeps an explicit kind and at', t.get(8n)?.kind === 'policy' && t.get(8n)?.at === 5);
  t.add(8n);
  check('3 mark then add: add() preserves the known cause', t.get(8n)?.cause === 'identity');
  check('3 mark then add: add() preserves kind and at', t.get(8n)?.kind === 'policy' && t.get(8n)?.at === 5);
  check('3 mark then add: still one entry for the id', t.size === 2);
  t.delete(7n);
  check('3 delete() behaves as the Set did', !t.has(7n) && t.size === 1);

  // 5. The kernel's onPeerDied forwards a KNOWN cause (the web transport's
  //    4.76.3 close reason) into the mark. The sim transport never supplies
  //    one, so invoke the registered died-handlers with a reason directly.
  const c = await makePeer(net, domain, 40.2, -74.2);
  await a.transport.openConnection(c.hex); await wait(15);
  check('5 setup: c bound at a, not marked', a.transport.isConnected(c.big) && !a.node._deadPeers.has(c.big));
  for (const h of a.transport._diedHandlers) h(c.big, 'pong-timeout');
  check('5 known cause forwarded into the mark', a.node._deadPeers.get?.(c.big)?.cause === 'pong-timeout', String(a.node._deadPeers.get?.(c.big)?.cause));
  check('5 kind is loss on the known-cause path', a.node._deadPeers.get?.(c.big)?.kind === 'loss');
  // The bridge's legacy add() for the SAME death, arriving after the kernel's
  // mark, does not erase the cause.
  a.node._deadPeers.add(c.big);
  check('5 a later no-information add() keeps the known cause', a.node._deadPeers.get?.(c.big)?.cause === 'pong-timeout');

  // 6. Foreign-Set fallback: a bare Set installed over the table by some
  //    other owner keeps working; the id is recorded, nothing throws, and
  //    the reason lives only in the peer-died-evicted log line.
  a.node._deadPeers = new Set();
  let threw = false;
  try { for (const h of a.transport._diedHandlers) h(c.big, 'send-fail'); } catch { threw = true; }
  check('6 foreign Set: handler does not throw', !threw);
  check('6 foreign Set: id recorded by membership', a.node._deadPeers.has(c.big));
  check('6 foreign Set: no mark (Set has no get)', typeof a.node._deadPeers.get !== 'function');
  a.node._deadPeers = new DeadPeers();   // restore for teardown
  await c.transport.stop().catch(() => {});

  await a.peer.stop().catch(() => {});
  await b2.peer.stop().catch(() => {});
  await a.transport.stop().catch(() => {});
  await b2.transport.stop().catch(() => {});

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
