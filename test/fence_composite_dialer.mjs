#!/usr/bin/env node
// fence_composite_dialer — Bridge fill v0.8 (axona-docs 9b1ed08): the composite
// transport has ONE DIALER and a bound-only open.
//
// A node whose transport is a CompositeTransport over several sub-transports
// (the bridge: a WebSocket server and an uplink webTransport) must present the
// kernel's fill with exactly the surface a relay's web transport presents:
//
//   A. DIALER SELECTION. The sub-transport exposing connectViaRelay becomes the
//      composite's dialer when it is added; a second such sub refuses to be
//      added; a composite with none has NO connectViaRelay at all, so the
//      kernel's `openIsTheDial` reads true, exactly as before.
//   B. FORWARDING UNCHANGED. connectViaRelay / mayDial / canAllocate /
//      allocRefusedFor forward to the dialer and return its answers by
//      identity; a surface the dialer lacks is not installed.
//   C. THROUGH THE KERNEL CALLER. A real AxonaPeer on the composite:
//      an open to a peer the door owns returns true with zero dials ('bound',
//      dialer never called, nothing consumed); an unowned peer gets open=false
//      and ONE connectViaRelay, whose four answers end as the kernel's four
//      outcomes — 'held' with the incarnation attached to the token, 'held'
//      with none, 'failed' (token ended as a cancel), 'deferred' (token
//      released, nothing consumed) — with CONSUME exactly once and only on the
//      two issues.
//   D. OWNERSHIP MOVES during the awaited open: the dialer answers false for a
//      peer it now owns and the kernel ends the token as an unissued dial; a
//      peer whose socket closed between open and dial is dialled once.
//   E. onPeerList fans IN from every sub that emits it, including one added
//      after the kernel subscribed.
//   F. STATICS. openConnection's body is owner-or-false with no dial in it;
//      the second-dialer refusal is in addSubtransport.
//
// Mutants (each run by hand, each fails the named section): drop the
// second-dialer throw → A; coerce the dialer's answer (`!!r`) → B, C; make
// openConnection fall back to the dialer → C, D, F.
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
const stranger = (self, seed) => self ^ (1n << BigInt(seed));
const ARMED = { synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 4, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2 } };

/**
 * A sealed stub sub-transport. `door` owns the peers in `owned` (isConnected,
 * openConnection true, boundPeers lists `bound`), dials nothing. `dialer`
 * owns nothing, exposes connectViaRelay (programmable answer) and, when asked,
 * mayDial / canAllocate / allocRefusedFor.
 */
class StubSub extends Transport {
  constructor({ name, owned = new Set(), bound = new Set(), dial = null, ledger = false, peerList = false }) {
    super();
    this.name = name; this.owned = owned; this.bound = bound;
    this.calls = { open: [], relay: [], mayDial: 0, canAllocate: [], allocRefusedFor: [] };
    this.boundHandlers = []; this.peerListHandlers = [];
    depositDispatchCapability(this, { request: () => {}, notification: () => {} });
    if (dial) {
      this.answer = dial.answer;                               // value or (hex) => value
      this.connectViaRelay = (hex) => { this.calls.relay.push(hex); return typeof this.answer === 'function' ? this.answer(hex) : this.answer; };
    }
    if (ledger) {
      this.mayDialAnswer = true; this.canAllocateAnswer = { ok: true }; this.allocRefusedAnswer = false;
      this.mayDial         = ()     => { this.calls.mayDial++; return this.mayDialAnswer; };
      this.canAllocate     = (dir)  => { this.calls.canAllocate.push(dir); return this.canAllocateAnswer; };
      this.allocRefusedFor = (peer) => { this.calls.allocRefusedFor.push(peer); return this.allocRefusedAnswer; };
    }
    if (peerList) {
      this.onPeerList = (h) => { this.peerListHandlers.push(h); return () => { const i = this.peerListHandlers.indexOf(h); if (i >= 0) this.peerListHandlers.splice(i, 1); }; };
    }
  }
  async start() {} async stop() {} getLocalNodeId() { return 0n; }
  async openConnection(id) { this.calls.open.push(id); return this.owned.has(id); }
  async closeConnection() {}
  isConnected(id) { return this.owned.has(id); }
  async send() { throw new Error('stub send'); } async notify() {}
  onPeerDied() { return () => {}; }
  getLatency() { return 10; }
  boundPeers() { return [...this.bound]; }
  onPeerBound(h) { this.boundHandlers.push(h); return () => {}; }
}

async function makePeerOn(composite, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = composite;
  const domain = new AxonaDomain();
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport: composite, ...ARMED });
  peer._requireAxonaManager('fence');
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  // CONSUME spy on the real marks object.
  const consumed = []; const origConsume = node._deadPeers.consume.bind(node._deadPeers);
  node._deadPeers.consume = (x) => { consumed.push(x); return origConsume(x); };
  return { peer, node, big: fromHex(id.id), hex: id.id, consumed };
}

(async () => {
  console.log('fence_composite_dialer: bridge fill v0.8 — one dialer, bound-only open, answers forwarded by identity');
  const localBig = fromHex((await createNodeIdentity({ lat: 0, lng: 0 })).id);

  // ── A. dialer selection ────────────────────────────────────────────────
  {
    const none = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    none.addSubtransport(new StubSub({ name: 'door' }));
    check('A1 a composite with no dialing sub has NO connectViaRelay (openIsTheDial stays true)', typeof none.connectViaRelay === 'undefined' && none.dialer() === null);
    check('A1b nor mayDial / canAllocate / allocRefusedFor', typeof none.mayDial === 'undefined' && typeof none.canAllocate === 'undefined' && typeof none.allocRefusedFor === 'undefined');

    const one = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    const door = new StubSub({ name: 'door' }); const up = new StubSub({ name: 'uplink', dial: { answer: 'inc-a' }, ledger: true });
    one.addSubtransport(door); one.addSubtransport(up);
    check('A2 the sub exposing connectViaRelay is the dialer; the surface is installed', one.dialer() === up && typeof one.connectViaRelay === 'function' && typeof one.mayDial === 'function');

    const first = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    first.addSubtransport(new StubSub({ name: 'up1', dial: { answer: true } }));
    let threw = null; const second = new StubSub({ name: 'up2', dial: { answer: true } });
    try { first.addSubtransport(second); } catch (e) { threw = e; }
    check('A3 a SECOND dialing sub refuses to be added (TypeError naming one dialer) and is not in the list', threw instanceof TypeError && /one dialer/.test(threw.message) && !first._subs.includes(second) && first._subs.length === 1, String(threw && threw.message));

    const late = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    late.addSubtransport(new StubSub({ name: 'door' }));
    check('A4 before the uplink is added: no dialer', typeof late.connectViaRelay === 'undefined');
    const lateUp = new StubSub({ name: 'uplink', dial: { answer: null } });
    late.addSubtransport(lateUp);
    check('A4b the uplink added AFTER the door (the bridge\'s order) becomes the dialer', late.dialer() === lateUp && typeof late.connectViaRelay === 'function');
  }

  // ── B. forwarding by identity ──────────────────────────────────────────
  {
    const comp = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    const up = new StubSub({ name: 'uplink', dial: { answer: 'inc-1' }, ledger: true });
    comp.addSubtransport(new StubSub({ name: 'door' })); comp.addSubtransport(up);
    const answers = ['inc-1', true, false, null];
    let allSame = true;
    for (const a of answers) { up.answer = a; const r = comp.connectViaRelay('ab'.repeat(33)); if (r !== a) allSame = false; }
    check('B1 connectViaRelay returns the dialer\'s answer by identity for the incarnation string, true, false and null', allSame && up.calls.relay.length === 4);
    up.mayDialAnswer = false;
    check('B2 mayDial forwards (false stays false, call reaches the dialer)', comp.mayDial() === false && up.calls.mayDial === 1);
    const ca = { ok: false, why: 'pending' }; up.canAllocateAnswer = ca;
    check('B3 canAllocate forwards the direction and returns the dialer\'s object by identity', comp.canAllocate('out') === ca && up.calls.canAllocate[0] === 'out');
    up.allocRefusedAnswer = true; const pid = 7n;
    check('B4 allocRefusedFor forwards the peer and the answer', comp.allocRefusedFor(pid) === true && up.calls.allocRefusedFor[0] === pid);

    const bare = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    bare.addSubtransport(new StubSub({ name: 'uplink', dial: { answer: true } }));   // no ledger surfaces
    check('B5 a dialer without a ledger installs connectViaRelay only (mayDial absent → kernel: nothing to reserve against)', typeof bare.connectViaRelay === 'function' && typeof bare.mayDial === 'undefined' && typeof bare.canAllocate === 'undefined');
  }

  // ── C. through the kernel caller ──────────────────────────────────────
  {
    const comp = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    const door = new StubSub({ name: 'door' });
    const up = new StubSub({ name: 'uplink', dial: { answer: 'inc-x' }, ledger: true });
    comp.addSubtransport(door); comp.addSubtransport(up);
    const rec = await makePeerOn(comp, 3, 3);
    const g = rec.peer._attemptGuard;
    check('C0 setup: the peer is armed on a composite with a dialer; openIsTheDial is false', rec.peer._fillArmed() === true && typeof rec.node.transport.connectViaRelay === 'function');

    // C1 a peer the DOOR owns but has not yet reported bound: the kernel opens; the door answers true; no dial.
    const S1 = stranger(rec.big, 200); door.owned.add(S1);
    const r1 = await rec.peer._considerCandidate(S1, 'fill');
    check('C1 door-owned peer: open routed to the door and true → \'bound\'; dialer never called; nothing consumed',
      r1 === 'bound' && door.calls.open.includes(S1) && up.calls.relay.length === 0 && rec.consumed.length === 0 && !g.inflightOf(S1),
      `r=${r1} doorOpens=${door.calls.open.length} relay=${up.calls.relay.length} consumed=${rec.consumed.length}`);

    // C2 unowned peer, dialer answers an incarnation → 'held', token attached to inc, CONSUME once.
    const S2 = stranger(rec.big, 201); up.answer = 'inc-2';
    const c2 = rec.consumed.length;
    const r2 = await rec.peer._considerCandidate(S2, 'fill');
    const st2 = g._state.get((await import('../src/dht/attemptGuard.js')).identitySuffix(S2));
    check('C2 unowned peer, incarnation answer: open=false at the composite, ONE connectViaRelay with the hex, \'held\', token in flight with inc attached, CONSUME exactly once',
      r2 === 'held' && up.calls.relay.length === 1 && up.calls.relay[0] === toHex(S2) && g.inflightOf(S2) && st2?.inc === 'inc-2' && rec.consumed.length === c2 + 1 && rec.consumed[c2] === S2,
      `r=${r2} relay=${J(up.calls.relay)} inflight=${g.inflightOf(S2)} inc=${st2?.inc} consumed=${rec.consumed.length - c2}`);

    // C3 unowned, dialer answers true (issued, no incarnation) → 'held', inc null, CONSUME once.
    const S3 = stranger(rec.big, 202); up.answer = true; const c3 = rec.consumed.length; const n3 = up.calls.relay.length;
    const r3 = await rec.peer._considerCandidate(S3, 'fill');
    const st3 = g._state.get((await import('../src/dht/attemptGuard.js')).identitySuffix(S3));
    check('C3 answer true: \'held\', token in flight with NO incarnation, CONSUME once', r3 === 'held' && up.calls.relay.length === n3 + 1 && g.inflightOf(S3) && st3?.inc == null && rec.consumed.length === c3 + 1, `r=${r3} inc=${st3?.inc} consumed=${rec.consumed.length - c3}`);

    // C4 unowned, dialer answers false (not issued) → 'failed', token ended as a cancel, NOTHING consumed.
    const S4 = stranger(rec.big, 203); up.answer = false; const c4 = rec.consumed.length; const n4 = up.calls.relay.length;
    const r4 = await rec.peer._considerCandidate(S4, 'fill');
    check('C4 answer false: \'failed\', token ended (not in flight), attempt counted, nothing consumed', r4 === 'failed' && up.calls.relay.length === n4 + 1 && !g.inflightOf(S4) && g.attemptsOf(S4) === 1 && rec.consumed.length === c4, `r=${r4} inflight=${g.inflightOf(S4)} attempts=${g.attemptsOf(S4)} consumed=${rec.consumed.length - c4}`);

    // C5 unowned, dialer answers null (capacity refused) → 'deferred', token released, nothing counted or consumed.
    const S5 = stranger(rec.big, 204); up.answer = null; const c5 = rec.consumed.length; const n5 = up.calls.relay.length;
    const r5 = await rec.peer._considerCandidate(S5, 'fill');
    check('C5 answer null: \'deferred\', token released (not in flight), attempts 0, nothing consumed', r5 === 'deferred' && up.calls.relay.length === n5 + 1 && !g.inflightOf(S5) && g.attemptsOf(S5) === 0 && rec.consumed.length === c5, `r=${r5} inflight=${g.inflightOf(S5)} attempts=${g.attemptsOf(S5)} consumed=${rec.consumed.length - c5}`);

    check('C6 across C2–C5 the composite answered the open itself (owner-or-false): the door\'s open was never reached for an unowned peer, and the door has no dial', [S2, S3, S4, S5].every(s => door.calls.open.filter(x => x === s).length === 0) && typeof door.connectViaRelay === 'undefined', `doorOpens=${J(door.calls.open)}`);

    // C7 the ledger the kernel reads is the dialer's: mayDial false defers at preflight on the tick, no dial.
    up.mayDialAnswer = false; up.answer = 'inc-7';
    const S7 = stranger(rec.big, 205); rec.peer._nominateCandidate(S7, 'fence');
    const n7 = up.calls.relay.length; const md7 = up.calls.mayDial;
    await rec.peer._maintainSynaptome();
    check('C7 with the dialer\'s mayDial false the tick defers at preflight: mayDial was read on the dialer, no dial went out', up.calls.mayDial > md7 && up.calls.relay.length === n7, `mayDialCalls=${up.calls.mayDial - md7} relay=${up.calls.relay.length - n7}`);
    up.mayDialAnswer = true;

    // ── D. ownership moves during the awaited open ──────────────────────
    // D1 the peer becomes owned by the dialer's mesh while the door's open is awaited → dialer says false → 'failed'.
    const S8 = stranger(rec.big, 206);
    // The kernel awaits the COMPOSITE's open; the move happens inside that await
    // (an inbound channel to S8 lands on the dialer's mesh meanwhile).
    const origCompOpen = comp.openConnection.bind(comp);
    comp.openConnection = async (id) => { const r = await origCompOpen(id); if (id === S8) up.owned.add(S8); return r; };
    up.answer = (hex) => (hex === toHex(S8) && up.owned.has(S8) ? false : 'inc-8');   // web/index.js:1287: refuses a peer it owns
    const c8 = rec.consumed.length; const n8 = up.calls.relay.length;
    const r8 = await rec.peer._considerCandidate(S8, 'fill');
    check('D1 ownership moved to the dialer during the awaited open: the dialer refuses (false) → \'failed\', token ended, nothing consumed, exactly one dial attempt and no second allocator',
      r8 === 'failed' && !g.inflightOf(S8) && rec.consumed.length === c8 && up.calls.relay.length === n8 + 1, `r=${r8} relay=${up.calls.relay.length - n8} consumed=${rec.consumed.length - c8}`);
    comp.openConnection = origCompOpen;
    // D2 the socket closes between open and dial: the open was false anyway, dialled once.
    const S9 = stranger(rec.big, 207); up.answer = 'inc-9'; const n9 = up.calls.relay.length;
    const r9 = await rec.peer._considerCandidate(S9, 'fill');
    check('D2 a peer on no socket is dialled exactly once through the dialer', r9 === 'held' && up.calls.relay.length === n9 + 1);
    await rec.peer.stop?.();
  }

  // ── E. onPeerList fan-in ────────────────────────────────────────────────
  {
    const comp = new CompositeTransport({ localNodeId: localBig, log: () => {} });
    const door = new StubSub({ name: 'door' });                           // emits no peer-list
    const up = new StubSub({ name: 'uplink', dial: { answer: true }, peerList: true });
    comp.addSubtransport(door);
    const got = []; const unsub = comp.onPeerList((peers) => got.push(peers));
    check('E1 the composite exposes onPeerList before any emitting sub exists; nothing fires', typeof comp.onPeerList === 'function' && got.length === 0);
    comp.addSubtransport(up);
    check('E2 a sub added AFTER the subscription is wired', up.peerListHandlers.length === 1);
    up.peerListHandlers[0](['aa', 'bb']);
    check('E3 the frame reaches the kernel\'s handler unchanged', got.length === 1 && got[0][0] === 'aa' && got[0][1] === 'bb');
    unsub();
    check('E4 unsubscribe detaches from the emitting sub', up.peerListHandlers.length === 0);
  }

  // ── F. statics ──────────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/transport/web/composite.js', import.meta.url), 'utf8');
    const open = src.match(/async openConnection\(nodeId\) \{\s*const t = this\._routeFor\(nodeId\);\s*if \(!t\) return false;\s*return t\.openConnection\(nodeId\);\s*\}/);
    check('F1 openConnection is owner-or-false with no dial in its body', !!open);
    const openBody = open ? open[0] : '';
    check('F1b and names neither the dialer nor connectViaRelay', !/dialer|connectViaRelay/.test(openBody));
    check('F2 the second-dialer refusal lives in addSubtransport before the push', /addSubtransport\(t\) \{[\s\S]*?if \(this\._dialer\) \{\s*throw new TypeError\([\s\S]*?one dialer[\s\S]*?\}\s*this\._setDialer\(t\);\s*\}\s*this\._subs\.push\(t\);/.test(src));
    check('F3 forwarding returns the dialer\'s answer without coercion', /this\.connectViaRelay = \(toHex\) => t\.connectViaRelay\(toHex\);/.test(src) && !/!!t\.connectViaRelay|Boolean\(t\.connectViaRelay/.test(src));
  }

  console.log(`\nfence_composite_dialer: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(2); });

function J(v) { return JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 8) : x)); }
