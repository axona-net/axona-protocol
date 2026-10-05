// =====================================================================
// fence_self_integrate_bigint.mjs — Hold-and-Fill v0.15 (axona-docs e4809d2),
// repair row 11: _selfIntegrate passes the BigInt; connectViaRelay fallback
// on `false`, behind the guard.
//
// What this fences (Vega 79ccdf05's two-sided fence, both sides):
//   A. BOUND neighbour, NO guard: the transport receives a BIGINT and reports
//      the channel open → opened > 0; the relay fallback is never called.
//      (Before this row the id went out as hex, the BigInt-keyed map never
//      matched, and the count was 0 for every neighbour on the web transport.)
//   B. UNBOUND neighbour, NO guard: openConnection(bigint) is false and the
//      fallback is NOT taken → 0, nothing issued. The stranger dial is GATED.
//   C. UNBOUND neighbour, guard installed: allow → begin → open false → end
//      (bound=false) → CONSUME once → connectViaRelay(hex) once → relayed 1.
//   D. Guard refuses (`allow` false): nothing is opened, consumed or relayed.
//   E. Eligibility (row 10): a marked, not-yet-due neighbour is skipped before
//      any open; a due one is dialed.
//   F. Key type at the REAL web transport: WebRTCTransport.openConnection with
//      the BigInt of a bound identity resolves true; with that identity's hex
//      string it resolves false — the fact row 11 corrects for.
//   H, I. R11-1 (Aster d787d245): eligibility re-read at the dial and after the
//      awaited open; a loss during the open (H) or a loss marking the next
//      target (I) yields no open / CONSUME / relay for the newly ineligible id.
//   G. STATIC: `_selfIntegrate` no longer contains `openConnection(toHex(`.
//
// With `toHex(id)` restored in the open call, A fails (and F stands as the
// reason). With the guard condition removed from the fallback, B fails.
//
// Run: node test/fence_self_integrate_bigint.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { DeadPeers }                from '../src/dht/DeadPeers.js';
import { AttemptGuard }             from '../src/dht/attemptGuard.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';
import { MeshManager }              from '../src/transport/web/mesh.js';
import { WebRTCTransport }          from '../src/transport/web/webrtc.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16) : x));
const tick = () => new Promise(r => setTimeout(r, 0));

/** A recording transport: binding-capable shape, no network. */
function fakeTransport({ bound = new Set(), relay = true } = {}) {
  const calls = { open: [], relay: [], isConnected: [] };
  const t = {
    calls,
    isConnected(id) { calls.isConnected.push(id); return false; },
    boundPeers() { return [...bound]; },
    async openConnection(id) { calls.open.push(id); return typeof id === 'bigint' && bound.has(id); },
    onPeerBound() { return () => {}; },
    onPeerDied() { return () => {}; },
    start: async () => {}, stop: async () => {},
  };
  if (relay) t.connectViaRelay = (hex) => { calls.relay.push(hex); return true; };
  return t;
}

async function makePeer(transport, opts = {}) {
  const id = await createNodeIdentity({ lat: 10, lng: 10 });
  const node = new NeuronNode({ id: fromHex(id.id), lat: 10, lng: 10 });
  node.transport = transport;
  const domain = new AxonaDomain();
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  return { peer, node, id, big: fromHex(id.id) };
}
const neighbour = (self, seed) => self ^ (1n << BigInt(seed));

(async () => {
  console.log('fence_self_integrate_bigint: row 11 — the BigInt, and the gated stranger dial');

  // A. bound neighbour, no guard
  {
    const t = fakeTransport();
    const { peer, big } = await makePeer(t);
    const nb = neighbour(big, 200); t.boundPeers = () => [nb]; t.calls.bound = true;
    const boundSet = new Set([nb]); t.openConnection = async (id) => { t.calls.open.push(id); return typeof id === 'bigint' && boundSet.has(id); };
    peer.findKClosest = async () => [nb];
    const n = await peer._selfIntegrate();
    check('A bound neighbour, no guard: opened 1', n === 1, String(n));
    check('A the transport received a BIGINT, not a hex string', t.calls.open.length === 1 && typeof t.calls.open[0] === 'bigint', J(t.calls.open));
    check('A relay fallback never called', t.calls.relay.length === 0);
    check('A stats: opened 1, relayed 0, guarded false (carried in _selfIntegrateLast)', peer._selfIntegrateLast?.opened === 1 && peer._selfIntegrateLast?.relayed === 0 && peer._selfIntegrateLast?.guarded === false, J(peer._selfIntegrateLast));
  }

  // B. unbound neighbour, no guard: gated, nothing issued
  {
    const t = fakeTransport();
    const { peer, big } = await makePeer(t);
    const nb = neighbour(big, 201);
    peer.findKClosest = async () => [nb];
    const n = await peer._selfIntegrate();
    check('B unbound neighbour, no guard: opened 0', n === 0, String(n));
    check('B openConnection asked once with a BIGINT and answered false', t.calls.open.length === 1 && typeof t.calls.open[0] === 'bigint');
    check('B relay fallback NOT taken without the guard (GATED)', t.calls.relay.length === 0, J(t.calls.relay));
    check('B no mark consumed (no attempt issued)', !peer._node._deadPeers.has(nb));
  }

  // C. unbound neighbour, guard installed: allow → begin → open false → end → CONSUME → relay(hex)
  {
    const t = fakeTransport();
    const { peer, node, big } = await makePeer(t, { attemptGuard: { maxAttempts: 4, baseMs: 30000 } });
    const nb = neighbour(big, 202);
    peer.findKClosest = async () => [nb];
    const g = peer._attemptGuard;
    check('C setup: guard installed', g instanceof AttemptGuard);
    const consumed = []; const marks = node._deadPeers; const origConsume = marks.consume.bind(marks);
    marks.consume = (id) => { consumed.push(id); return origConsume(id); };
    const n = await peer._selfIntegrate();
    check('C opened 0 (unbound)', n === 0);
    check('C connectViaRelay called ONCE with the hex of the neighbour', t.calls.relay.length === 1 && t.calls.relay[0] === toHex(nb), J(t.calls.relay));
    check('C CONSUME ran once for the neighbour at issue', consumed.length === 1 && consumed[0] === nb);
    check('C guard: one attempt recorded, not in flight (begin+end once)', g.attemptsOf(nb) === 1 && g.allow(nb) === false, `attempts=${g.attemptsOf(nb)} allow=${g.allow(nb)}`);
    check('C stats: relayed 1, guarded true (carried in _selfIntegrateLast)', peer._selfIntegrateLast?.relayed === 1 && peer._selfIntegrateLast?.guardRefused === 0 && peer._selfIntegrateLast?.guarded === true, J(peer._selfIntegrateLast));
    // backoff: an immediate second pass is refused by the guard; nothing issued
    const n2 = await peer._selfIntegrate();
    check('C second pass inside the backoff: guard refused, no second relay dial, no second CONSUME', n2 === 0 && t.calls.relay.length === 1 && consumed.length === 1 && peer._selfIntegrateLast?.guardRefused === 1, J(peer._selfIntegrateLast));
  }

  // D. guard refuses outright
  {
    const t = fakeTransport();
    const { peer, big } = await makePeer(t, { attemptGuard: {} });
    const nb = neighbour(big, 203);
    peer.findKClosest = async () => [nb];
    peer._attemptGuard.allow = () => false;
    const n = await peer._selfIntegrate();
    check('D guard refuses: nothing opened, nothing relayed', n === 0 && t.calls.open.length === 0 && t.calls.relay.length === 0);
  }

  // E. eligibility (row 10): a marked, not-due neighbour is skipped; a due one is dialed
  {
    const t = fakeTransport();
    const { peer, node, big } = await makePeer(t, { attemptGuard: {} });
    const early = neighbour(big, 204), due = neighbour(big, 205);
    node._deadPeers = new DeadPeers({ B: 60000 });
    node._deadPeers.fail(early, 'test');                 // dueAt = now + 60 s → not eligible
    const d2 = new DeadPeers({ B: 0 }); d2.fail(due, 'test'); // B 0 → due at once
    const combined = node._deadPeers; combined.fail(due, 'test'); combined.get(due).dueAt = 0;
    peer.findKClosest = async () => [early, due];
    const n = await peer._selfIntegrate();
    check('E marked-not-due neighbour skipped before any open; due one dialed', n === 0 && t.calls.open.length === 1 && t.calls.open[0] === due && peer._selfIntegrateLast?.ineligible === 1, J({ open: t.calls.open, stats: peer._selfIntegrateLast }));
    check('E the due neighbour fell through to the relay dial', t.calls.relay.length === 1 && t.calls.relay[0] === toHex(due));
  }


  // H. R11-1 (Aster d787d245): a loss DURING the awaited open marks the target;
  //    the fallback must see the current eligibility: no CONSUME, no relay.
  {
    const t = fakeTransport();
    const { peer, node, big } = await makePeer(t, { attemptGuard: {} });
    const nb = neighbour(big, 206);
    node._deadPeers = new DeadPeers({ B: 60000 });
    const consumed = []; const oc = node._deadPeers.consume.bind(node._deadPeers); node._deadPeers.consume = (id) => { consumed.push(id); return oc(id); };
    t.openConnection = async (id) => { t.calls.open.push(id); await tick(); node._deadPeers.fail(nb, 'intervening-loss'); return false; };
    peer.findKClosest = async () => [nb];
    const n = await peer._selfIntegrate();
    check('H loss during the open: eligible at build, ineligible at the effect boundary → NO relay, NO consume', n === 0 && t.calls.open.length === 1 && t.calls.relay.length === 0 && consumed.length === 0 && peer._selfIntegrateLast?.ineligibleAfterOpen === 1, J({ relay: t.calls.relay, consumed: consumed.length, stats: peer._selfIntegrateLast }));
  }

  // I. R11-1: concurrency 1, two targets; the first open's loss callback marks
  //    the SECOND target before its dial starts → no open, no relay for it.
  {
    const t = fakeTransport();
    const { peer, node, big } = await makePeer(t, { attemptGuard: {} });
    const a = neighbour(big, 207), b = neighbour(big, 208);
    node._deadPeers = new DeadPeers({ B: 60000 });
    t.openConnection = async (id) => { t.calls.open.push(id); await tick(); if (id === a) node._deadPeers.fail(b, 'intervening-loss'); return false; };
    peer.findKClosest = async () => [a, b];
    const n = await peer._selfIntegrate({ concurrency: 1 });
    check('I the second target, marked by the first open\'s loss, is not opened and not relayed; the first is relayed once', n === 0 && t.calls.open.length === 1 && t.calls.open[0] === a && t.calls.relay.length === 1 && t.calls.relay[0] === toHex(a) && peer._selfIntegrateLast?.ineligible === 1, J({ open: t.calls.open, relay: t.calls.relay, stats: peer._selfIntegrateLast }));
  }

  // F. the real web transport's key type
  {
    class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() {} open() { this.readyState = 'open'; this.onopen?.(); } }
    class FakePC { constructor() { this.connectionState = 'new'; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; } createDataChannel() { return new FakeDC(); } async createOffer() { return { type: 'offer', sdp: 'v=0' }; } async createAnswer() { return { type: 'answer', sdp: 'v=0' }; } async setLocalDescription(d) { this.localDescription = d; } async setRemoteDescription(d) { this.remoteDescription = d; } async addIceCandidate() {} async getStats() { return new Map(); } close() { this.connectionState = 'closed'; } }
    globalThis.RTCPeerConnection = FakePC;
    const mesh = new MeshManager({ sendSignal: () => {}, log: () => {} });
    const w = new WebRTCTransport({ mesh, log: () => {} });
    await w.start();
    await mesh._initiateTo('c1'); await tick(); mesh._peers.get('c1').dc.open();
    const NID = BigInt('0x' + 'ab'.repeat(33));
    w.bindPeer(NID, 'c1');
    const asBig = await w.openConnection(NID);
    const asHex = await w.openConnection(toHex(NID));
    check('F real web transport: openConnection(BIGINT of a bound identity) → true', asBig === true);
    check('F real web transport: openConnection(HEX of the same identity) → false (the defect row 11 corrects for)', asHex === false);
    await w.stop(); mesh.dispose();
  }

  // G. static
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const s = src.indexOf('async _selfIntegrate('); const e = src.indexOf('\n  }\n', s);
    const body = src.slice(s, e);
    check('G _selfIntegrate no longer passes toHex(id) to openConnection', !/openConnection\(toHex\(/.test(body));
    check('G the fallback is conditioned on the guard in source', /guard && typeof t\.connectViaRelay === 'function'/.test(body));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
