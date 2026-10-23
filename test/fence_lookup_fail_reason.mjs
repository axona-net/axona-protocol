// =====================================================================
// fence_lookup_fail_reason.mjs — 4.122.0 (David 2026-10-23 03:xxZ, "Log why each
// lookup fails, then roll it"). The 4.121.0 hop trace found 42% of relayed
// offers withheld because the pre-send lookup said not-found, and 599 of 1,234
// withheld signals went to targets another relay reached within 60 s
// (axona.bot de2f5aad). A lookup now says WHY it failed, and the reason travels
// back over lookup_step replies to the originator and into the relay gate's
// relay-signal-origin line.
//
//   R1 found → reason 'found'.
//   R2 a lone peer with nothing to try → 'stalled'.
//   R3 a table entry for the target with NO channel behind it (bound-only open,
//      as the web transport does) → 'send-failed' at hop 1: next = the target,
//      connected=false, opened=false, direct=true, isTarget=true.
//   R4 the same failure one hop away, at B, travels back to A over the wire.
//   R5 the origin's not-sent line carries lookupWhy and the fail fields.
//
// Mutants: drop the send-failed reason → R3/R4; drop the stalled reason → R2;
// drop `reason` from lookup()'s return → R3; drop the why/fail stored by the gate
// → R5.
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise((r) => setTimeout(r, ms));

// Bound-only open + no send without a channel: the web transport's contract
// (composite.js openConnection never dials; webrtc.js send throws "not connected").
function boundOnly(t) {
  t.openConnection = async (id) => t.isConnected(id);
  const send = t.send.bind(t);
  t.send = async (id, type, body) => {
    if (!t.isConnected(id)) throw new Error(`Transport.send: peer ${String(id)} not connected`);
    return send(id, type, body);
  };
  return t;
}
async function makePeer(net, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = boundOnly(simTransport({ network: net, identity: id, heartbeatMs: 0 }));
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
async function stopAll(list) { for (const p of list) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }

(async () => {
  console.log('fence_lookup_fail_reason: a failed lookup says why, and the reason reaches the relay gate');
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 300, EPSILON: 0 });
  const raw = [];
  for (let i = 0; i < 4; i++) raw.push(await makePeer(net, domain, 40 + i * 0.01, -74));
  const T = raw[0];
  // A farthest from T, B nearer than A; L is a lone peer.
  const [B, A, L] = raw.slice(1).sort((x, y) => ((x.big ^ T.big) < (y.big ^ T.big) ? -1 : 1));
  // The ONE real channel, A–B: the sim's own open (from the prototype), bypassing
  // the bound-only wrapper the fence installs on each instance.
  const openRaw = async (x, y) => { const o = Object.getPrototypeOf(x.transport).openConnection; if (o) await o.call(x.transport, y.hex); };
  await openRaw(A, B);
  await wait(20);
  check('setup: A holds a channel to B and none to T; B holds none to T',
    A.transport.isConnected(B.hex) && !A.transport.isConnected(T.hex) && !B.transport.isConnected(T.hex),
    `A-B ${A.transport.isConnected(B.hex)} A-T ${A.transport.isConnected(T.hex)} B-T ${B.transport.isConnected(T.hex)}`);

  synapse(A, B); synapse(B, A);

  // R1 found
  const r1 = await A.peer.lookup(B.big);
  check('R1 a lookup that lands on its target says found', r1?.found === true && r1.reason === 'found', JSON.stringify({ f: r1?.found, r: r1?.reason }));

  // R2 stalled
  const r2 = await L.peer.lookup(T.big);
  check('R2 a lone peer with nothing to try says stalled', r2?.found === false && r2.reason === 'stalled', JSON.stringify({ f: r2?.found, r: r2?.reason }));

  // R3 a table entry for T with no channel, at the originator
  synapse(A, T);
  const r3 = await A.peer.lookup(T.big);
  const f3 = r3?.fail || {};
  check('R3 a table entry with no channel behind it → send-failed at hop 1, naming the target as next',
    r3?.found === false && r3.reason === 'send-failed' && f3.hop === 1 && f3.next === T.hex.slice(0, 12) && f3.at === A.hex.slice(0, 12), JSON.stringify({ r: r3?.reason, f3 }));
  check('R3 … the failing hop knew it had no channel, the open did not make one, and it took the direct-to-target shortcut',
    f3.connected === false && f3.opened === false && f3.direct === true && f3.isTarget === true && typeof f3.err === 'string', JSON.stringify(f3));
  A.node.synaptome.delete(T.big);

  // R4 the same failure at B, travelling back to A
  synapse(B, T);
  const r4 = await A.peer.lookup(T.big);
  const f4 = r4?.fail || {};
  check('R4 a failure one hop away comes back over the wire: send-failed at B, hop 2, next = T',
    r4?.found === false && r4.reason === 'send-failed' && f4.at === B.hex.slice(0, 12) && f4.hop === 2 && f4.next === T.hex.slice(0, 12), JSON.stringify({ r: r4?.reason, f4 }));
  await stopAll(raw);

  // R5 the origin's not-sent line names the reason
  {
    const logs = [];
    const self = {
      _relayReach: new Map(), _node: { id: 7n }, _domain: { _emit() {} },
      _emitLog: (lvl, ev, ctx) => logs.push({ ev, ctx }),
      lookup: async () => ({ found: false, reason: 'send-failed', fail: { hop: 1, at: 'aaaaaaaaaaaa', next: 'ffffffffffff', connected: false, opened: false, addedBy: 'hopCache', direct: true, isTarget: true, err: 'not connected' } }),
      routeMessage: async () => ({ consumed: true }),
    };
    self._relayReachable = AxonaPeer.prototype._relayReachable.bind(self);
    await AxonaPeer.prototype._relayMeshSignal.call(self, 255n, 'ff'.repeat(33), { kind: 'sdp-offer', attempt: 'r5-1' });
    const o = logs.find((l) => l.ev === 'relay-signal-origin')?.ctx || {};
    check('R5 the origin\'s not-sent line carries lookupWhy and the failing hop\'s facts',
      o.result === 'not-sent' && o.lookupWhy === 'send-failed' && o.failHop === 1 && o.failNext === 'ffffffffffff' && o.failConnected === false && o.failAddedBy === 'hopCache' && o.failDirect === true && o.failIsTarget === true,
      JSON.stringify(o));
  }

  console.log(`\nfence_lookup_fail_reason: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
