// =====================================================================
// fence_signal_hop_trace.mjs — 4.121.0 (David 2026-10-22, "Trace the relayed
// signal hop by hop first"; the 4.120.0 join found ~96% of failed dials to our
// relays lost IN TRANSIT on the mesh-relayed path, axona.bot 7ce27c7c).
//
//   H. EVERY HOP SAYS WHAT IT DID with a relayed offer or answer, on REAL
//      AxonaPeers over the sim transport: 'forwarded' (with the next hop and
//      whether it held a channel to it), 'delivered' at the target,
//      'terminal-not-target' where the walk ends short. Candidates are not
//      traced; other routed types are not traced.
//   O. THE ORIGIN SAYS WHAT HAPPENED: the reachability verdict that gates the
//      send (lookup-no / cached-no / lookup-ok / cached-ok) and the route's
//      result (not-sent / delivered / terminal-not-target / exhausted / threw),
//      with the wire attempt id. Before 4.121.0 these left only a domain event.
//
// TOPOLOGY (from fence_route_path_memory): D closest to T = D^1, then B, A, C.
// Channels A–B, A–C. C routes via A to B.
//
// Mutants: drop the 'forwarded' trace → H1; drop 'delivered' → H2; drop
// 'terminal-not-target' → H3; trace every routed type → H4; drop the origin's
// not-sent line → O1; drop the failed-route line → O3.
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

async function makePeer(net, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  const logs = [];
  peer.onLog('info', (ev, ctx) => logs.push({ ev, ctx }));
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id, logs };
}
function synapse(a, b) {
  const syn = new Synapse({ peerId: b.big, latencyMs: 10, stratum: clz264(a.big ^ b.big) });
  syn.weight = 0.9; syn.inertia = 0; syn._addedBy = 'fence';
  a.node.synaptome.set(b.big, syn);
}
async function build() {
  const net = new SimNetwork();
  const domain = new AxonaDomain({ LOOKAHEAD_MS: 300 });
  const raw = [];
  for (let i = 0; i < 4; i++) raw.push(await makePeer(net, domain, 40 + i * 0.01, -74));
  const D = raw[0];
  const T = D.big ^ 1n;
  const [B, A, C] = raw.slice(1).sort((x, y) => ((x.big ^ T) < (y.big ^ T) ? -1 : 1));
  await A.transport.openConnection(B.hex);
  await A.transport.openConnection(C.hex);
  await wait(20);
  synapse(A, B); synapse(A, C); synapse(A, D); synapse(B, A); synapse(C, A);
  return { T, A, B, C, D, all: raw };
}
async function stopAll(w) { for (const p of w.all) { try { await p.peer.stop?.(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } } }
const hopsOf = (p) => p.logs.filter((l) => l.ev === 'relay-signal-hop');
const offer = (attempt) => ({ from: '00'.repeat(33), signal: { kind: 'sdp-offer', sdp: 'v=0', attempt } });

(async () => {
  console.log('fence_signal_hop_trace: every hop and the origin say what happened to a relayed offer/answer');

  // ── H. hops ─────────────────────────────────────────────────────────
  {
    const w = await build();
    const { A, B, C } = w;
    const r = await C.peer.routeMessage(B.big, 'mesh:signal', offer('att-1'));
    check('H0 precondition: C → A → B, consumed at B', r && r.consumed === true && r.atNode === B.big, JSON.stringify({ c: r?.consumed, hops: r?.hops }));
    const ha = hopsOf(A).find((l) => l.ctx.attempt === 'att-1');
    check('H1 A logs forwarded, naming B as the next hop it holds a channel to', ha?.ctx.decision === 'forwarded' && ha.ctx.next === B.hex.slice(0, 12) && ha.ctx.nextConnected === true && ha.ctx.kind === 'sdp-offer', JSON.stringify(ha?.ctx));
    const hb = hopsOf(B).find((l) => l.ctx.attempt === 'att-1');
    check('H2 B logs delivered (it is the target)', hb?.ctx.decision === 'delivered' && hb.ctx.target === B.hex.slice(0, 12), JSON.stringify(hb?.ctx));

    const r2 = await C.peer.routeMessage(w.T, 'mesh:signal', offer('att-2'));
    const hb2 = hopsOf(B).find((l) => l.ctx.attempt === 'att-2');
    check('H3 a walk toward an id nobody holds ends at B, which logs terminal-not-target with its synaptome size', r2?.terminal === true && hb2?.ctx.decision === 'terminal-not-target' && Number.isFinite(hb2.ctx.synaptome), JSON.stringify(hb2?.ctx));

    const before = w.all.reduce((n, p) => n + hopsOf(p).length, 0);
    await C.peer.routeMessage(B.big, 'mesh:signal', { from: '00'.repeat(33), signal: { kind: 'ice', candidate: { c: 1 }, attempt: 'att-3' } });
    await C.peer.routeMessage(B.big, 'fence:noop', {});
    const after = w.all.reduce((n, p) => n + hopsOf(p).length, 0);
    check('H4 an ICE candidate and a non-signal routed type are NOT traced', after === before, `${before} → ${after}`);
    check('H5 no hop line carries an SDP body', !w.all.some((p) => hopsOf(p).some((l) => JSON.stringify(l.ctx).includes('v=0'))));
    await stopAll(w);
  }

  // ── O. origin ───────────────────────────────────────────────────────
  {
    const mk = ({ reach, route, cachedNo = false }) => {
      const logs = [];
      const self = {
        _relayReach: new Map(), _node: { id: 7n }, _domain: { _emit() {} },
        _emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }),
        _relayReachable: async () => reach,
        routeMessage: route,
      };
      if (cachedNo) self._relayReach.set((255n).toString(16), { ok: false, ts: Date.now() });
      return { self, logs };
    };
    const run = async (self, kind = 'sdp-offer') => AxonaPeer.prototype._relayMeshSignal.call(self, 255n, 'ff'.repeat(33), { kind, attempt: 'o-1' });
    const pick = (logs) => logs.filter((l) => l.ev === 'relay-signal-origin').map((l) => l.ctx);

    let m = mk({ reach: false, route: async () => ({ consumed: true }) });
    await run(m.self);
    check('O1 an unreachable verdict logs reach=lookup-no, result=not-sent, with the attempt', pick(m.logs)[0]?.reach === 'lookup-no' && pick(m.logs)[0]?.result === 'not-sent' && pick(m.logs)[0]?.attempt === 'o-1', JSON.stringify(pick(m.logs)));
    m = mk({ reach: false, route: async () => ({ consumed: true }), cachedNo: true });
    await run(m.self);
    check('O1b a CACHED unreachable verdict says cached-no', pick(m.logs)[0]?.reach === 'cached-no', JSON.stringify(pick(m.logs)));
    m = mk({ reach: true, route: async () => ({ consumed: true, hops: 3 }) });
    await run(m.self);
    check('O2 a delivered route logs result=delivered, hops and tries', pick(m.logs)[0]?.result === 'delivered' && pick(m.logs)[0]?.hops === 3 && pick(m.logs)[0]?.tries === 1, JSON.stringify(pick(m.logs)));
    m = mk({ reach: true, route: async () => ({ consumed: false, terminal: true, atNode: 0xabcdn, hops: 2 }) });
    await run(m.self);
    check('O3 a route that ends short logs terminal-not-target, where, after two tries', pick(m.logs)[0]?.result === 'terminal-not-target' && pick(m.logs)[0]?.atNode === toHex(0xabcdn).slice(0, 12) && pick(m.logs)[0]?.tries === 2, JSON.stringify(pick(m.logs)));
    m = mk({ reach: true, route: async () => { throw new Error('boom'); } });
    await run(m.self);
    check('O4 a route that throws both times logs result=threw, threw=2', pick(m.logs)[0]?.result === 'threw' && pick(m.logs)[0]?.threw === 2, JSON.stringify(pick(m.logs)));
    m = mk({ reach: true, route: async () => ({ consumed: true }) });
    await run(m.self, 'ice');
    check('O5 an ICE candidate is not traced at the origin', pick(m.logs).length === 0);
  }

  console.log(`\nfence_signal_hop_trace: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
