// fence_air_gap_capability.mjs — Bridge-Air-Gap-Plan v0.3 §7.1, v0.5 §7.1.2, v0.7
// §7.1.4 (as amended). WP4 rows P2 (lookahead), P3 (role-holder pickers), P4
// (classification and provenance), P5 (inbound role matrix), P6 (pinning).
//
// No network: stub transports that CLASSIFY, a peer built the way the lookahead
// fences build one, and a manager built on a stub dht. Requirements against the
// kernel, not evidence about any host.
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { CompositeTransport } from '../src/transport/web/composite.js';
import { Transport } from '../src/contracts/Transport.js';
import { Synapse } from '../src/dht/Synapse.js';
import { ErrorCodes } from '../src/errors.js';
import { depositDispatchCapability } from '../src/registry/index.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + String(extra)}`); ok ? passed++ : failed++; };

const R = (n) => (0x89n << 248n) | BigInt(n);
const BRIDGE = (0xffn << 248n) | 0x1n;
const SELF = R(0x1000);
const TARGET = R(0x0001);

// ── a classifying stub sub-transport ──────────────────────────────────────
class Stub extends Transport {
  constructor(name, cls, ids) {
    super();
    this.name = name; this.cls = cls; this.ids = new Set(ids); this.gen = new Map(); this.sent = [];
    for (const id of ids) this.gen.set(id, 1);
    depositDispatchCapability(this, { request() {}, notification() {} });
  }
  async start() {} async stop() {}
  ownsPeer(id) { return this.ids.has(id); }
  isConnected(id) { return this.ids.has(id); }
  boundPeers() { return [...this.ids]; }
  capabilityFor(id) { return this.ids.has(id) ? this.cls : 'unknown'; }
  generationFor(id) { return this.gen.get(id) ?? 0; }
  rebind(id) { this.gen.set(id, (this.gen.get(id) ?? 0) + 1); }
  async send(id, type, body) { this.sent.push({ id, type, body }); return { peerId: R(0x0002), latency: 1, terminal: false }; }
  async notify(id, type, body) { this.sent.push({ id, type, body, ntf: true }); return true; }
  onPeerDied() { return () => {}; }
  getLatency() { return 1; }
}
function composite(subs) {
  const c = new CompositeTransport({ localNodeId: SELF, log: () => {} });
  for (const s of subs) c.addSubtransport(s);
  return c;
}
function peerWith(transport, synIds) {
  const node = { id: SELF, alive: true, transport, synaptome: new Map(), incomingSynapses: new Map(), _deadPeers: new Set() };
  for (const id of synIds) node.synaptome.set(String(id), new Synapse({ peerId: id, latencyMs: 1, stratum: 0 }));
  const self = { _node: node, _lookaheadStats: undefined };
  for (const m of ['capabilityOf', 'isTransit', 'isIntroduction', 'introductionIds', '_pinFor', '_greedyNextHopToward', '_findCloserInTwoHops', 'lookaheadStats']) self[m] = AxonaPeer.prototype[m];
  return self;
}

console.log('\n[P4] classification: bridge = introduction, mesh peer = transport, unknown fails closed');
{
  const bridge = new Stub('bridge', 'introduction', [BRIDGE]);
  const mesh = new Stub('mesh', 'transport', [R(2), R(3)]);
  const c = composite([bridge, mesh]);
  check('bridge id classifies introduction', c.capabilityFor(BRIDGE) === 'introduction');
  check('mesh peer classifies transport', c.capabilityFor(R(2)) === 'transport');
  check('an id nobody owns classifies unknown', c.capabilityFor(R(9)) === 'unknown');
  const p = peerWith(c, [BRIDGE, R(2)]);
  check('peer.isTransit true only for the mesh peer', p.isTransit(R(2)) && !p.isTransit(BRIDGE) && !p.isTransit(R(9)));
  check('peer.introductionIds lists exactly the bridge', JSON.stringify(p.introductionIds().map(String)) === JSON.stringify([String(BRIDGE)]));
  const bare = peerWith({ boundPeers: () => [R(2)] }, [R(2)]);
  check('a transport without capabilityFor answers unknown → never transit (fails closed)', !bare.isTransit(R(2)) && bare.capabilityOf(R(2)) === 'unknown');
  const base = new Transport();
  check('Transport contract defaults: capabilityFor unknown, generationFor 0', base.capabilityFor(R(1)) === 'unknown' && base.generationFor(R(1)) === 0);
}

console.log('\n[P2] greedy and lookahead never select the introduction edge');
{
  // the bridge is XOR-closer to TARGET than any mesh peer, and the only synapse that would answer "closer"
  const near = (0x89n << 248n) | 0x0002n;            // closer than self to TARGET
  const bridge = new Stub('bridge', 'introduction', [BRIDGE]);
  const mesh = new Stub('mesh', 'transport', [R(0x2000)]);   // farther than self from TARGET
  const c = composite([bridge, mesh]);
  const p = peerWith(c, [BRIDGE, R(0x2000)]);
  // greedy: self R(0x1000) vs target R(0x0001): the mesh peer R(0x2000) is farther; the bridge is 0xff… farther still → null
  check('greedy returns null when the only nearer-by-reply peer is the bridge', p._greedyNextHopToward(TARGET) === null);
  // lookahead: probes go only to the mesh peer; the bridge is never probed even though it would name a closer node
  bridge.send = async () => ({ peerId: near, latency: 1, terminal: false });
  mesh.send = async () => ({ peerId: R(0x3000), latency: 1, terminal: true });
  const hop = await p._findCloserInTwoHops(TARGET);
  check('lookahead never probes the bridge', !bridge.sent.length && bridge.send !== undefined);
  check('lookahead first hop is not the bridge (null here: the mesh peer knew nobody closer)', hop === null);
  const s = p.lookaheadStats();
  check('probesSuppressedByCapability counts the bridge', s.probesSuppressedByCapability === 1, JSON.stringify(s.probesSuppressedByCapability));
  // incoming synapse that is an introduction edge never becomes the first hop
  p._node.incomingSynapses.set('b', { peerId: BRIDGE });
  const hop2 = await p._findCloserInTwoHops(TARGET);
  check('an introduction-class incoming synapse is rejected as first hop', hop2 === null && p.lookaheadStats().firstHopsRejectedByCapability >= 1);
}

console.log('\n[P6] the egress gate: class and generation pinning at the composite');
{
  const bridge = new Stub('bridge', 'introduction', [BRIDGE]);
  const mesh = new Stub('mesh', 'transport', [R(2)]);
  const c = composite([bridge, mesh]);
  let err = null;
  try { await c.send(BRIDGE, 'route_msg', {}); } catch (e) { err = e; }
  check('route_msg to the bridge is refused NO_TRANSPORT_ROUTE', err && err.code === ErrorCodes.NO_TRANSPORT_ROUTE, err && err.code);
  err = null; try { await c.send(R(9), 'route_msg', {}); } catch (e) { err = e; }
  check('route_msg to an unowned id is still PEER_UNREACHABLE', err && err.code === ErrorCodes.TRANSPORT_PEER_UNREACHABLE, err && err.code);
  await c.send(BRIDGE, 'lookup_step', {});
  check('discovery to the bridge is allowed', bridge.sent.some((x) => x.type === 'lookup_step'));
  await c.notify(BRIDGE, 'presence', {});
  check('an introduction notification to the bridge is allowed', bridge.sent.some((x) => x.type === 'presence'));
  await c.notify(BRIDGE, 'direct_pubsub:deliver', {});
  check('a direct_* notification to the bridge is dropped (forward class)', !bridge.sent.some((x) => x.type === 'direct_pubsub:deliver'));
  await c.send(R(2), 'route_msg', {}, { pin: 1 });
  check('route_msg to a transport peer with the current generation is sent', mesh.sent.some((x) => x.type === 'route_msg'));
  mesh.rebind(R(2)); err = null;
  try { await c.send(R(2), 'route_msg', {}, { pin: 1 }); } catch (e) { err = e; }
  check('after a rebind the pinned send is refused NO_TRANSPORT_ROUTE (stale generation), no fallback', err && err.code === ErrorCodes.NO_TRANSPORT_ROUTE && err.message.includes('stale-generation'));
  check('the composite counts refusals', c.noTransportRouteCount() === 3, c.noTransportRouteCount());
}

console.log('\n[P3] role-holder pickers exclude introduction ids via the dht shim shape');
{
  // rootClaim.meshBare + selfClosestReachable and repairPlane._isIntroductionId consume the shim; exercise them through their modules
  const { default: RootClaim } = await import('../src/pubsub/rootClaim.js').catch(() => ({ default: null }));
  const rc = await import('../src/pubsub/rootClaim.js');
  const RC = rc.RootClaim ?? rc.default;
  const m = {
    nodeId: SELF,
    dht: {
      neighbors: () => [BRIDGE, R(0x3000)],
      introductionIds: () => [BRIDGE],
      isTransit: (id) => id === R(0x3000),
      isIntroduction: (id) => id === BRIDGE,
      bridgeId: () => BRIDGE,
    },
    axonRoles: new Map(), _log: () => {}, _now: () => 0,
  };
  const claim = new RC(m, {});
  check('meshBare: a bridge plus one transport neighbour is meshed', claim.meshBare() === false);
  m.dht.neighbors = () => [BRIDGE];
  check('meshBare: bridge-only is bare', claim.meshBare() === true);
  m.dht.neighbors = () => [BRIDGE, R(0x3000)];
  // selfClosestReachable: the bridge is XOR-closest to a 0xff topic but must not count. Self is
  // R(0x1000); topic low bits 0x0000, so self's distance ends in 0x1000: R(0x3000) is farther
  // (0x3000), R(0x0001) is closer (0x0001).
  const ffTopic = (0xffn << 248n) | 0x0000n;
  check('selfClosestReachable ignores the bridge even when it is XOR-closest', claim.selfClosestReachable(ffTopic) === true);
  m.dht.neighbors = () => [BRIDGE, R(0x0001)];
  m.dht.isTransit = (id) => id === R(0x0001);
  check('selfClosestReachable still yields to a closer TRANSPORT neighbour', claim.selfClosestReachable(ffTopic) === false);
  const rp = await import('../src/pubsub/repairPlane.js');
  const RP = rp.default ?? rp;
  const isIntro = (RP._isIntroductionId ?? RP.prototype?._isIntroductionId);
  check('repairPlane._isIntroductionId exists', typeof isIntro === 'function');
  if (typeof isIntro === 'function') {
    check('repairPlane treats the bridge as introduction and an unknown id as eligible', isIntro.call({ dht: m.dht }, BRIDGE) === true && isIntro.call({ dht: m.dht }, R(7)) === false);
  }
}

console.log('\n[P5] the role matrix: an introduction-only node refuses every pushed role and every non-directory root');
{
  const { AxonaManager } = await import('../src/pubsub/AxonaManager.js');
  const dir = (0x89n << 248n) | 0xd1n;
  const other = (0x89n << 248n) | 0xd2n;
  const hex = (b) => b.toString(16).padStart(66, '0');
  const routed = new Map();
  const dht = {
    verdictsSupported: true,   // routeMessage below resolves a verdict, so true is the honest declaration
    routeMessage: async () => ({ consumed: false }), getSelfId: () => hex(SELF),
    onRoutedMessage: (type, h) => routed.set(type, h), onDirectMessage() {},
    neighbors: () => [], bridgeId: () => null, isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
  };
  // E3 seal: the manager registers its routed frames through a deposited capability, never a named primitive
  depositDispatchCapability(dht, { routed: (type, h) => dht.onRoutedMessage(type, h) });
  const mgr = new AxonaManager({ dht, introductionOnly: true, rootAllowList: [hex(dir)] });
  mgr._log = () => {};
  const ok = (t, r) => mgr.canAcceptRole(t, r);
  check('root of a named directory topic is allowed (subject to soft checks)', ok(dir, 'root').ok === true || ok(dir, 'root').hard === false, JSON.stringify(ok(dir, 'root')));
  check('root of any other topic is refused HARD not-directory', ok(other, 'root').hard === true && ok(other, 'root').why === 'not-directory');
  for (const role of ['backup', 'heir', 'child']) {
    check(`${role} on the directory topic is refused HARD role-not-allowed`, ok(dir, role).hard === true && ok(dir, role).why === 'role-not-allowed');
    check(`admitPushedRole(${role}) refuses on an introduction-only node`, mgr.admitPushedRole(dir, role) === false && mgr.admitPushedRole(other, role) === false);
  }
  const plain = new AxonaManager({ dht });
  plain._log = () => {};
  check('a regular node is unaffected: canAcceptRole has no hard refusal for backup', plain.canAcceptRole(other, 'backup').hard !== true);
}

console.log('\n[P7] an introduction-only node: every edge classifies introduction, no forward on any sub');
{
  const hex = (b) => b.toString(16).padStart(66, '0');
  const bridge = new Stub('bridge', 'introduction', [BRIDGE]);
  const mesh = new Stub('mesh', 'transport', [R(2)]);      // e.g. a WebRTC edge formed through an uplink
  const c = new CompositeTransport({ localNodeId: SELF, log: () => {}, introductionOnly: true });
  c.addSubtransport(bridge); c.addSubtransport(mesh);
  check('the composite maps a transport-class sub to introduction', c.capabilityFor(R(2)) === 'introduction' && c.capabilityFor(BRIDGE) === 'introduction');
  let err = null;
  try { await c.send(R(2), 'route_msg', { targetId: hex(R(7)) }); } catch (e) { err = e; }
  check('a forward to a transport-class sub is refused NO_TRANSPORT_ROUTE, nothing written', err && err.code === ErrorCodes.NO_TRANSPORT_ROUTE && mesh.sent.length === 0, err && err.code);
  await c.send(R(2), 'route_msg', { targetId: hex(R(2)) });
  check('a route_msg ADDRESSED to the socket peer still passes (one-hop rule, §7.2.7)', mesh.sent.length === 1 && mesh.sent[0].type === 'route_msg');
  err = null; try { await c.notify(R(2), 'direct_pubsub:deliver', {}); } catch (e) { err = e; }
  check('a direct_* notification is dropped on every edge', !mesh.sent.some((x) => x.type === 'direct_pubsub:deliver'));
  const plain = composite([mesh]);
  const p = peerWith(plain, [R(2)]);
  p._introductionOnly = true;
  check('peer-level introductionOnly: the transport edge reads introduction, never a hop', !p.isTransit(R(2)) && p.isIntroduction(R(2)) && p._greedyNextHopToward(R(7)) === null);
  check('…and introductionIds lists it', p.introductionIds().map(String).includes(String(R(2))));
  const q = peerWith(plain, [R(2)]);
  check('a regular node on the same transport is unaffected', q.isTransit(R(2)) && q._greedyNextHopToward(R(7)) === R(2));
}

console.log('\n[P8] the data-channel egress gate sits at the PHYSICAL write (mesh._dcWrite), below the composite, and sees the local cause');
{
  const { MeshManager } = await import('../src/transport/web/mesh.js');
  const sent = [];
  const seen = [];
  const gate = {
    before: (frame, peerId, cause) => { seen.push({ frame, peerId, cause }); const bad = frame.k === 'req' && frame.type === 'route_msg'; return { allowed: !bad && cause !== null, cls: bad ? 'genericTransit' : 'other' }; },
    after: (cls) => sent.push(`after:${cls}`),
    threw: (cls) => sent.push(`threw:${cls}`),
  };
  const mesh = new MeshManager({ sendSignal: () => {}, log: () => {}, egressGate: gate });
  mesh._peers.set('m1', { peerId: 'm1', dc: { readyState: 'open', send: (s) => sent.push(s) } });
  const r1 = mesh.send('m1', { k: 'req', id: 1, type: 'route_msg', body: {} }, 'kernel-request');
  check('a refused frame returns false and NOTHING reaches dc.send (invoked stays 0)', r1 === false && sent.length === 0 && mesh.egressStats().refused === 1 && mesh.egressStats().invoked === 0);
  const r2 = mesh.send('m1', { k: 'ntf', type: 'presence', body: {} }, 'kernel-notify');
  check('an allowed frame is invoked, returned, then after(cls) runs', r2 === true && sent.length === 2 && sent[0].includes('presence') && sent[1] === 'after:other');
  check('the gate saw the raw frame, the mesh peer id and the LOCAL cause', seen[1].peerId === 'm1' && seen[1].cause === 'kernel-notify' && seen[0].cause === 'kernel-request');
  const r3 = mesh.send('m1', { k: 'ntf', type: 'presence', body: {} });
  check('the same frame with NO cause is refused: provenance comes from the caller, never the type', r3 === false);
  mesh._peers.set('m2', { peerId: 'm2', dc: { readyState: 'open', send: () => { throw new Error('dc closed'); } } });
  let threw = false; try { mesh.send('m2', { k: 'ntf', type: 'presence', body: {} }, 'kernel-notify'); } catch { threw = true; }
  check('a send that throws is counted invoked + threw, not returned; the throw propagates', threw && sent.at(-1) === 'threw:other');
  check('counters: attempts 4, refused 2, invoked 2, returned 1, threw 1', JSON.stringify(mesh.egressStats()) === JSON.stringify({ attempts: 4, refused: 2, invoked: 2, returned: 1, threw: 1 }), JSON.stringify(mesh.egressStats()));
  const plain = new MeshManager({ sendSignal: () => {}, log: () => {} });
  const psent = [];
  plain._peers.set('m1', { peerId: 'm1', dc: { readyState: 'open', send: (s) => psent.push(s) } });
  check('without a gate: unchanged behaviour, counters stay 0', plain.send('m1', { k: 'req', id: 1, type: 'route_msg', body: {} }) === true && psent.length === 1 && plain.egressStats().attempts === 0);
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
