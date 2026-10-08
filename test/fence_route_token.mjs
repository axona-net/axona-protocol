#!/usr/bin/env node
// fence_route_token — Socket-is-bootstrap v0.5 (axona-docs 7a27d24, § Ownership):
// ONE IDENTITY, ONE ADMITTED ROUTE, ONE DIRECTION.
//
// A CompositeTransport over a BOOTSTRAP sub (a bridge door's WebSocket, a
// client's BridgeTransport: `isBootstrap === true`) and a mesh sub must not
// evict an identity when its bootstrap socket closes after a mesh channel to
// the same identity has bound (Vega 2b16970f, Aster BS-1), must not let a late
// first socket hello take the identity back (Vega 07c5ff8e, Aster dbbad807 §1),
// and must validate the route token before the attempt guard and before any
// kernel side effect (Aster 99c03319 BS1-R).
//
//   A. (a) ADMIT: a bind on a sub with no admitted route fires the kernel
//      handler once; the route is recorded.
//   B. (b) SWITCH, socket → mesh: the mesh bind fires NO kernel handler (route
//      change, no re-admission); routing moves to the mesh; the socket sub's
//      supersedePeer is called; the socket's death is SWALLOWED; the mesh's
//      death is forwarded and re-arms the bind dedup.
//   C. (b) REVERSE: mesh admitted, a socket binds → born superseded: no
//      handler, routing stays on the mesh, its death is swallowed.
//   D. STEP 0: a bind whose token is not the sub's current token is ignored;
//      a bind from a superseded route after the admitted route died is
//      ignored (the record outlives the admitted route); a fresh bind after
//      the superseded route closed admits again.
//   E. NO BOOTSTRAP SUB: two non-bootstrap subs behave as before this rule
//      (handler once by dedup; both deaths forwarded).
//   F. BIND POLICY: consulted for (a) only; a refusal records no route and
//      fires no handler; not consulted for a switch.
//   G. THROUGH A REAL AxonaPeer: the identity has ONE synaptome entry through
//      socket bind → mesh bind → socket close; it leaves only when the mesh
//      channel dies.
//   H. gatePreflight is pure and predicts _admitOrImprove.
//   I. STATICS: _routeFor skips a superseded sub; onPeerDied subscribes no sub.
//
// Mutants: drop the `_isSuperseded` skip in _routeFor → B, I; forward every
// death → B, C, G; make (b) undirected → C; drop step 0 → D; consult the
// policy on switch → F; make gatePreflight call _seedInsert → H.
import { readFileSync } from 'node:fs';
import { AxonaPeer }            from '../src/dht/AxonaPeer.js';
import { AxonaDomain }          from '../src/dht/AxonaDomain.js';
import { NeuronNode }           from '../src/dht/NeuronNode.js';
import { CompositeTransport }   from '../src/transport/web/composite.js';
import { Transport }            from '../src/contracts/Transport.js';
import { depositDispatchCapability } from '../src/registry/index.js';
import { createNodeIdentity }   from '../src/identity/index.js';
import { fromHex, toHex }       from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const ARMED = { synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 4, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2 } };

/** A sealed stub sub. `bootstrap` marks it a bootstrap route. Binds are
 *  driven by the test: bind(nodeId, token) / die(nodeId). */
class Sub extends Transport {
  constructor({ name, bootstrap = false }) {
    super();
    this.name = name; this._boot = bootstrap;
    this.tokens = new Map();        // nodeId → current token
    this.boundH = []; this.diedH = []; this.superseded = [];
    depositDispatchCapability(this, { request: () => {}, notification: () => {} });
  }
  get isBootstrap() { return this._boot; }
  async start() {} async stop() {} getLocalNodeId() { return 0n; }
  ownsPeer(id) { return this.tokens.has(id); }
  channelIdFor(id) { return this.tokens.get(id) ?? null; }
  boundPeers() { return [...this.tokens.keys()]; }
  onPeerBound(h) { this.boundH.push(h); return () => {}; }
  onPeerDied(h) { this.diedH.push(h); return () => {}; }
  supersedePeer(id, tok) { this.superseded.push({ id, tok }); }
  bind(id, tok) { this.tokens.set(id, tok); const rs = []; for (const h of this.boundH) rs.push(h(id, tok, null)); return rs; }
  /** fire a bind event with a token that is NOT the current one */
  bindStale(id, tok) { for (const h of this.boundH) h(id, tok, null); }
  die(id, reason = 'test') { this.tokens.delete(id); for (const h of this.diedH) h(id, reason); }
  async openConnection(id) { return this.tokens.has(id); }
  async closeConnection() {}
  isConnected(id) { return this.tokens.has(id); }
  async send() { throw new Error('stub'); } async notify() {}
  getLatency() { return 10; }
}

const mk = () => {
  const comp = new CompositeTransport({ localNodeId: 1n, log: () => {} });
  const door = new Sub({ name: 'door', bootstrap: true });
  const mesh = new Sub({ name: 'mesh' });
  comp.addSubtransport(door); comp.addSubtransport(mesh);
  const bound = []; const died = [];
  comp.onPeerBound((n, m, inc) => { bound.push({ n, m }); return true; });
  comp.onPeerDied((n, r) => died.push({ n, r }));
  return { comp, door, mesh, bound, died };
};
const X = 0xabc1n, Y = 0xabc2n;

(async () => {
  console.log('fence_route_token: socket-is-bootstrap v0.5 — one identity, one admitted route, one direction');

  // ── A ────────────────────────────────────────────────────────────────
  {
    const { comp, door, bound, died } = mk();
    door.bind(X, 'c7');
    check('A1 a bind on a sub with no admitted route fires the kernel handler once', bound.length === 1 && bound[0].n === X && bound[0].m === 'c7');
    check('A2 the route is recorded on the door with its token', comp.routeOf(X)?.sub === door && comp.routeOf(X)?.token === 'c7');
    check('A3 routing goes to the door', comp._routeFor(X) === door);
    door.die(X);
    check('A4 the admitted route\'s death is forwarded and the record is gone', died.length === 1 && comp.routeOf(X) === null);
  }

  // ── B ────────────────────────────────────────────────────────────────
  {
    const { comp, door, mesh, bound, died } = mk();
    door.bind(X, 'c7');
    mesh.bind(X, 'm-1');
    check('B1 the mesh bind after the socket bind fires NO kernel handler (route change, no re-admission)', bound.length === 1);
    check('B2 the admitted route is now the mesh', comp.routeOf(X)?.sub === mesh && comp.routeOf(X)?.token === 'm-1');
    check('B3 routing moves to the mesh while the socket is still open', comp._routeFor(X) === mesh && door.ownsPeer(X));
    check('B4 the door was told it is superseded (pending requests fail route-superseded there)', door.superseded.length === 1 && door.superseded[0].id === X);
    check('B5 routeStats counted one switch', comp.routeStats.switched === 1);
    door.die(X, 'socket-closed');
    check('B6 the socket\'s death is SWALLOWED: no peer-died reaches the kernel', died.length === 0 && comp.routeStats.deathSwallowed === 1);
    check('B7 the identity still routes to the mesh', comp._routeFor(X) === mesh && comp.routeOf(X)?.sub === mesh);
    mesh.die(X, 'pc-closed');
    check('B8 the mesh\'s death is forwarded: the identity dies', died.length === 1 && died[0].r === 'pc-closed' && comp.routeOf(X) === null);
    door.bind(X, 'c9');
    check('B9 the dedup was re-armed by the admitted route\'s death: a fresh bind fires the handler again', bound.length === 2);
  }

  // ── C ────────────────────────────────────────────────────────────────
  {
    const { comp, door, mesh, bound, died } = mk();
    mesh.bind(X, 'm-1');
    const rs = door.bind(X, 'c7');
    check('C1 a socket bind while the mesh route is admitted fires NO handler (born superseded)', bound.length === 1 && comp.routeStats.bornSuperseded === 1);
    check('C1b the sub\'s handler saw the event rejected (false)', rs[0] === false);
    check('C2 routing stays on the mesh; the door was told', comp._routeFor(X) === mesh && door.superseded.length === 1);
    door.die(X, 'socket-closed');
    check('C3 the born-superseded socket\'s death is swallowed', died.length === 0);
    mesh.die(X);
    check('C4 the mesh death kills the identity', died.length === 1 && comp.routeOf(X) === null);
  }

  // ── D ────────────────────────────────────────────────────────────────
  {
    const { comp, door, mesh, bound, died } = mk();
    door.bind(X, 'c7');
    door.bindStale(X, 'c3');
    check('D1 a bind whose token is not the sub\'s current token is ignored (step 0)', bound.length === 1 && comp.routeStats.staleToken === 1 && comp.routeOf(X)?.token === 'c7');
    mesh.bind(X, 'm-1');                       // switch
    mesh.die(X);                               // admitted route dies while the socket is still open
    check('D2 the admitted route died: forwarded; the record survives for the superseded socket', died.length === 1 && comp.routeOf(X) === null && comp._routes.has(X));
    const rs = door.bind(X, 'c7');             // a late hello on the superseded socket (same token)
    check('D3 a bind from the superseded route after identity death is ignored, not re-admitted', bound.length === 1 && rs[0] === false);
    check('D3b the identity has no routable owner (superseded is never re-promoted)', comp._routeFor(X) === null);
    door.die(X);
    check('D4 the superseded socket\'s close is swallowed and clears the record', died.length === 1 && !comp._routes.has(X));
    door.bind(X, 'c8');
    check('D5 a fresh socket bind afterwards admits again', bound.length === 2 && comp.routeOf(X)?.sub === door);
  }

  // ── E ────────────────────────────────────────────────────────────────
  {
    const comp = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const a = new Sub({ name: 'a' }), b = new Sub({ name: 'b' });
    comp.addSubtransport(a); comp.addSubtransport(b);
    const bound = [], died = [];
    comp.onPeerBound((n) => { bound.push(n); return true; }); comp.onPeerDied((n) => died.push(n));
    a.bind(X, 'a1'); b.bind(X, 'b1');
    check('E1 with no bootstrap sub, two subs binding one identity fire the handler once (dedup, as before)', bound.length === 1 && comp.routeStats.switched === 0 && comp.routeStats.bornSuperseded === 0);
    check('E2 routing is first-owner, as before', comp._routeFor(X) === a);
    b.die(X); a.die(X);
    check('E3 both deaths are forwarded, as before', died.length === 2);
  }

  // ── F ────────────────────────────────────────────────────────────────
  {
    const { comp, door, mesh, bound } = mk();
    const asked = [];
    comp.setBindPolicy((n, sub, tok) => { asked.push({ n, sub: sub.name, tok }); return n !== Y; });
    door.bind(Y, 'c1');
    check('F1 a refused bind records no route and fires no handler', bound.length === 0 && comp.routeOf(Y) === null && !comp._routes.has(Y) && comp.routeStats.policyRefused === 1);
    door.bind(X, 'c2');
    check('F2 a passed bind is admitted and fires', bound.length === 1 && comp.routeOf(X)?.sub === door && asked.length === 2);
    mesh.bind(X, 'm-1');
    check('F3 the policy is NOT consulted for a switch', asked.length === 2 && comp.routeOf(X)?.sub === mesh);
    comp.setBindPolicy(null);
    door.bind(Y, 'c1');
    check('F4 with the policy cleared the refused identity admits', comp.routeOf(Y)?.sub === door);
  }

  // ── G. through a real AxonaPeer ──────────────────────────────────────
  {
    const comp = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const door = new Sub({ name: 'door', bootstrap: true });
    const mesh = new Sub({ name: 'mesh' });
    comp.addSubtransport(door); comp.addSubtransport(mesh);
    const id = await createNodeIdentity({ lat: 0, lng: 0 });
    const node = new NeuronNode({ id: fromHex(id.id), lat: 0, lng: 0 });
    node.transport = comp;
    const peer = new AxonaPeer({ domain: new AxonaDomain(), node, nodeIdentity: id, transport: comp, ...ARMED });
    peer._requireAxonaManager('fence');
    await peer.start();
    if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
    const P = fromHex(id.id) ^ (1n << 200n);
    door.bind(P, 'c7');
    check('G1 the socket hello admits the identity into the synaptome', node.synaptome.has(P));
    const sizeAfterSocket = node.synaptome.size;
    mesh.bind(P, 'm-1');
    check('G2 the mesh bind leaves the synaptome unchanged (one entry, no re-admission)', node.synaptome.has(P) && node.synaptome.size === sizeAfterSocket);
    door.die(P, 'socket-closed');
    check('G3 the socket close does NOT evict the identity (its admitted route is the mesh)', node.synaptome.has(P) && !node._deadPeers?.has?.(P));
    mesh.die(P, 'pc-closed');
    check('G4 the mesh death evicts the identity', !node.synaptome.has(P));
    await peer.stop?.();
  }

  // ── H. gatePreflight ─────────────────────────────────────────────────
  {
    const comp = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    comp.addSubtransport(new Sub({ name: 'mesh' }));
    const id = await createNodeIdentity({ lat: 0, lng: 0 });
    const node = new NeuronNode({ id: fromHex(id.id), lat: 0, lng: 0 });
    node.transport = comp;
    const peer = new AxonaPeer({ domain: new AxonaDomain(), node, nodeIdentity: id, transport: comp, ...ARMED, admissionGate: { kNear: 5, sparseFloor: 2, kJoin: 2, laneCooldownMs: 5000, laneWindowMs: 300000 } });
    peer._requireAxonaManager('fence');
    await peer.start();
    if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
    node._maxSynaptome = 4;
    const self = fromHex(id.id);
    const ids = [1n, 2n, 3n, 4n, 5n, 6n].map((k) => self ^ (1n << (100n + k)));
    for (const q of ids.slice(0, 2)) peer._seedInsert(q, 'fence');   // 2 of 4; lane starts at cap−kJoin = 2
    const lane0 = peer._laneSeen.size, laneAt0 = peer._laneLastAt, size0 = node.synaptome.size;
    const d1 = peer.gatePreflight(ids[2]); const d2 = peer.gatePreflight(ids[2]);
    check('H1 gatePreflight is pure: repeated calls agree and change nothing', d1.admit === true && d2.admit === true && d1.how === d2.how && node.synaptome.size === size0 && peer._laneSeen.size === lane0 && peer._laneLastAt === laneAt0);
    const committed = peer._admitOrImprove(ids[2]);
    check('H2 the commit agrees with the preflight', committed === true && node.synaptome.has(ids[2]) && (d1.how !== 'gate-lane' || peer._laneSeen.size === lane0 + 1));
    const d3 = peer.gatePreflight(ids[3]);
    const size1 = node.synaptome.size;
    check('H3 a preflight that would be refused (lane cooldown) says so without mutating', d3.admit === false && typeof d3.why === 'string' && node.synaptome.size === size1, JSON.stringify(d3));
    check('H4 the commit of that decision refuses too', peer._admitOrImprove(ids[3]) === false && !node.synaptome.has(ids[3]));
    check('H5 an identity already in the table preflights as already', peer.gatePreflight(ids[2]).how === 'already');
    await peer.stop?.();
  }

  // ── I. statics ───────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/transport/web/composite.js', import.meta.url), 'utf8');
    const rf = src.slice(src.indexOf('_routeFor(nodeId) {'), src.indexOf('boundPeers() {'));
    check('I1 _routeFor skips a superseded sub', /_isSuperseded\(t, nodeId\)/.test(rf));
    const od = src.slice(src.indexOf('onPeerDied(handler) {'), src.indexOf('onNegotiationFailed(handler) {'));
    check('I2 onPeerDied subscribes no sub-transport itself (one subscription per sub lives in addSubtransport)', !/t\.onPeerDied\(/.test(od) && /this\._peerDiedHandlers\.push/.test(od));
    const ob = src.slice(src.indexOf('_routeBind(t, nodeId, token) {'), src.indexOf('_onSubBound(t, nodeIdBig, meshId, inc) {'));
    check('I3 step 0 (token validation) precedes the admitted-route logic in _routeBind', ob.indexOf('bind-stale-token') < ob.indexOf('rec.admitted'));
    const ap = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const gd = ap.slice(ap.indexOf('_gateDecision(sponsor) {'), ap.indexOf('mayRetire(id)'));
    check('I4 _gateDecision performs no insert, delete or close', !/_seedInsert|syn\.delete|closeConnection|_laneSeen\.set|_laneLastAt =/.test(gd));
  }

  // ── J. Aster 3d778257 / 8fb51cdb: replay admission, repeated and stale deaths, nested tokens ──
  {
    // RT-1: the existing-peer REPLAY in onPeerBound runs the same bind policy as live delivery.
    const { comp, door, bound } = mk();
    comp.setBindPolicy((n) => n !== Y);
    door.bind(Y, 'c1');                                   // refused live; the sub still reports Y bound (cleanup is the policy owner's)
    const late = [];
    comp.onPeerBound((n) => { late.push(n); return true; });
    check('J1 RT-1: a handler registered after a refused bind does not admit the refused identity on replay', !late.includes(Y) && comp.routeOf(Y) === null && bound.length === 0 && comp.routeStats.policyRefused === 2);
    comp.setBindPolicy(null);
    door.bind(X, 'c2');
    const late2 = []; comp.onPeerBound((n) => { late2.push(n); return true; });
    check('J1b with no policy the replay admits a bound identity (as before)', late2.includes(X) && comp.routeOf(X)?.sub === door);
  }
  {
    // RT-2: repeated death from a superseded sub; death from a non-owner; same-sub stale token.
    const { comp, door, mesh, died } = mk();
    door.bind(X, 'c7'); mesh.bind(X, 'm-1');              // switch: door superseded
    door.die(X); door.die(X); door.die(X);                // the socket's death arrives three times
    check('J2 RT-2: a REPEATED death from the superseded sub never reaches the kernel', died.length === 0 && comp.routeOf(X)?.sub === mesh && comp.routeStats.deathSwallowed >= 2);
    // a same-sub stale death: the mesh reports a death for an OLDER token than the admitted one
    for (const h of mesh.diedH) h(X, 'pc-closed', 'm-0');
    check('J3 a death the admitted sub reports for an older token is swallowed', died.length === 0 && comp.routeOf(X)?.sub === mesh && comp.routeStats.deathStaleToken === 1);
    for (const h of mesh.diedH) h(X, 'pc-closed', 'm-1');
    check('J3b the admitted token\'s death kills the identity', died.length === 1 && comp.routeOf(X) === null);
  }
  {
    // channelIdFor: the admitted route's token first, superseded subs skipped, nested composites recursed.
    const outer = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const door = new Sub({ name: 'door', bootstrap: true });
    const inner = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const innerBridge = new Sub({ name: 'inner-bridge', bootstrap: true });
    const innerMesh = new Sub({ name: 'inner-mesh' });
    inner.addSubtransport(innerBridge); inner.addSubtransport(innerMesh);
    outer.addSubtransport(door); outer.addSubtransport(inner);
    const outerBound = []; outer.onPeerBound((n) => { outerBound.push(n); return true; });
    door.bind(X, 'c7');
    innerBridge.bind(X, 'bridge'); innerMesh.bind(X, 'm-9');   // inside the nested composite: bridge sub then mesh → inner switch
    check('J4 nested: the inner composite switched to its mesh; the outer switched door → inner', inner.routeOf(X)?.sub === innerMesh && outer.routeOf(X)?.sub === inner && outerBound.length === 1);
    check('J5 channelIdFor names the admitted route\'s token through the nesting, not the superseded door\'s or the inner bridge\'s', outer.channelIdFor(X) === 'm-9' && inner.channelIdFor(X) === 'm-9');
    check('J6 both retained bootstrap bindings are superseded and routing reaches the inner mesh', door.ownsPeer(X) && innerBridge.ownsPeer(X) && outer._routeFor(X) === inner && inner._routeFor(X) === innerMesh);
    const late = []; outer.onPeerBound((n) => { late.push(n); return true; });
    check('J7 handler replay after the switch fires once for the identity and changes no route', late.length === 1 && late[0] === X && outer.routeOf(X)?.sub === inner && inner.routeOf(X)?.sub === innerMesh);
    door.die(X); innerBridge.die(X);
    check('J8 both bootstrap deaths are swallowed at their level', outer.routeOf(X)?.sub === inner && inner.routeOf(X)?.sub === innerMesh);
    // Aster 88f4c2f7: the inner switch fired no bind upward, so the outer's
    // admitted token for the child must FOLLOW the child's route change, and
    // the child's death must carry that token so the outer reads it as the
    // admitted route's death — not a stale one.
    check('J9 the outer\'s admitted token for the child follows the inner switch (inner mesh token, not the inner socket\'s)', outer.routeOf(X)?.token === 'm-9' && (outer.routeStats.tokenFollowed ?? 0) >= 1);
    const outerDied = []; outer.onPeerDied((n, r, tok) => outerDied.push({ n, r, tok }));
    for (const h of innerMesh.diedH) h(X, 'pc-closed', 'm-0');       // an inner STALE death (older token)
    check('J10 an inner stale-token death is swallowed at the inner level and never reaches the outer', outerDied.length === 0 && inner.routeOf(X)?.sub === innerMesh && outer.routeOf(X)?.sub === inner);
    innerMesh.die(X, 'pc-closed');                                     // the admitted inner route dies
    check('J11 the inner mesh death kills the identity at BOTH levels, carrying the inner token upward', outerDied.length === 1 && outerDied[0].tok === 'm-9' && inner.routeOf(X) === null && outer.routeOf(X) === null);
  }
  {
    // Three levels: a route change announced by the grandchild reaches the grandparent.
    const g = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const p = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const c = new CompositeTransport({ localNodeId: 1n, log: () => {} });
    const cb = new Sub({ name: 'c-bridge', bootstrap: true }); const cm = new Sub({ name: 'c-mesh' });
    c.addSubtransport(cb); c.addSubtransport(cm); p.addSubtransport(c); g.addSubtransport(p);
    const gDied = []; g.onPeerBound(() => true); g.onPeerDied((n, r, tok) => gDied.push(tok));
    cb.bind(X, 'bridge'); cm.bind(X, 'm-3');
    check('J12 three levels: the grandparent\'s admitted token follows the grandchild\'s switch', g.routeOf(X)?.token === 'm-3' && p.routeOf(X)?.token === 'm-3');
    cm.die(X, 'pc-closed');
    check('J12b and its death reaches the grandparent with that token', gDied.length === 1 && gDied[0] === 'm-3' && g.routeOf(X) === null);
  }

  console.log(`\nfence_route_token: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
