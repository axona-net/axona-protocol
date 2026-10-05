// =====================================================================
// fence_hold_and_fill_integration.mjs — Hold-and-Fill Phase 1 (axona-docs
// v0.9 de10fd4), the COMBINED PIN hold-and-fill-phase1: rows 1, 2b, 3, 4,
// 5, 6, 10+13 merged. The three integration obligations Aster named for
// the combined review (f5237636, 2f945b42, d5b37071), each driven across
// the rows that meet there; none is a per-row claim.
//
//   A. GRACE / OVERFLOW × CLOSE × DUTY × MARKS (rows 4, 5, 6, 10 on the
//      kernel path, sim transport). At cap with the gate armed: a refused
//      candidate that this node owes a duty to is NOT closed at the grace
//      fire (row 4); once the duty ends the fire closes it, and the close is
//      VOLUNTARY: no onPeerDied at this node, so the mark table (row 10)
//      has NO loss mark for it and the identity stays ELIGIBLE. Overflow
//      (graceMaxPending reached) closes the OLDEST PERMITTED pending close
//      and no other, also mark-free. Anneal is gone and the at-cap
//      vitality path opens nothing (row 6), so the only closes below cap
//      on this path are the gate's. Case 46's shape, with the mark table
//      read after it.
//   B. PHYSICAL RETENTION UNDER REAL CLOSES (rows 3 + 5, web transport,
//      fake RTCPeerConnection). closeConnection unbinds first, then
//      disconnects; the ledger's channel record goes CLOSING and stays
//      CHARGED (chanAll unchanged) until the PC reports 'closed', when it
//      goes GONE and is released; the peer record is unbound at the
//      unbind, not at the close. With enforce on and C_phys at the count,
//      an allocation during CLOSING is refused and the first 'closed'
//      lets the next one through (design cases 40, 41). A voluntary close
//      fires no onPeerDied (row 5) so row 10 would write no mark; an
//      involuntary one still does.
//   C. THE CAP / RESERVATION SCHEDULE (row 15, NOT STARTED; design case
//      61, Aster d5b37071). Two _addByVitality calls below cap both pass
//      the size check, both await, both insert: table 5 at cap 4 on the
//      combined tree as on 270835d. This part RECORDS the pre-existing
//      outcome under that label; it is an observation, not a claim of
//      correctness, and it is rewritten when row 15 lands. The fixed half
//      (row 6) is also here: AT cap, no open and no insert.
//
// With row 4's mayRetire call removed, A fails; with row 5's disconnect
// removed, B fails (PC open after close, record never CLOSING); with row
// 3's hooks removed, B fails; with row 6 reverted, A's "no open at cap"
// and C's at-cap half fail.
//
// Run: node test/fence_hold_and_fill_integration.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { makeRole }                 from '../src/pubsub/rootClaim.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';
import { MeshManager }              from '../src/transport/web/mesh.js';
import { WebRTCTransport }          from '../src/transport/web/webrtc.js';
import { CHAN }                     from '../src/transport/web/channel_ledger.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const tick = () => new Promise(r => setTimeout(r, 0));
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16) : x));

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
const groupOf = (rec, big, domain) => Math.min(domain.STRATA_GROUPS - 1, clz264(rec.big ^ big) >>> 2);
async function mintInGroup(net, domain, rec, wantGroups, maxTries = 400) {
  for (let i = 0; i < maxTries; i++) {
    const p = await makePeer(net, domain, (i * 13) % 80 - 40, (i * 29) % 340 - 170, {});
    if (wantGroups.includes(groupOf(rec, p.big, domain))) return p;
    await p.peer.stop().catch(() => {}); await p.transport.stop?.();
  }
  return null;
}
const stopAll = async (...recs) => { for (const r of recs) { if (!r) continue; await r.peer.stop().catch(() => {}); await r.transport.stop().catch(() => {}); } };

// ── fake RTCPeerConnection for part B ─────────────────────────────────
class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } open() { this.readyState = 'open'; this.onopen?.(); } }
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.closeCalls = 0; }
  createDataChannel() { return new FakeDC(); }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.closeCalls++; this.connectionState = 'closed'; if (FakePC.fireClosedOnClose) queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
  fireClosed() { this.connectionState = 'closed'; this.onconnectionstatechange?.(); }
}
FakePC.instances = []; FakePC.fireClosedOnClose = true;
globalThis.RTCPeerConnection = FakePC;

(async () => {
  console.log('fence_hold_and_fill_integration: the combined pin — rows 1, 2b, 3, 4, 5, 6, 10+13 together');
  const net = new SimNetwork(); const domain = new AxonaDomain();

  // ── A. grace / overflow × close × duty × marks ─────────────────────
  console.log('\n  A. grace and overflow closes under the duty gate, read in the mark table');
  {
    const a = await makePeer(net, domain, 5, 5, { admissionGate: { kNear: 5, sparseFloor: 2, closeGraceMs: 60, graceMaxPending: 2 } });
    a.peer._requireAxonaManager('fence');
    a.node._maxSynaptome = 11;
    [1n, 2n, 3n, 4n, 5n].forEach(x => craft(a, x));
    craft(a, 1n << 259n); craft(a, 1n << 258n);
    craft(a, (1n << 263n) + 7n, 0.9); craft(a, (1n << 262n) + 3n, 0.9); craft(a, (1n << 261n) + 9n, 0.05); craft(a, (1n << 260n) + 5n, 0.9);
    check('A setup: at cap 11, anneal absent, mark table empty', a.node.synaptome.size === 11 && typeof a.peer._tryAnneal !== 'function' && a.node._deadPeers.size === 0);
    // the sim hands the died handler its hex peerId; never convert here (a throw inside a handler is swallowed and would hide the death)
    const died = []; a.transport.onPeerDied((id, reason) => died.push({ id: String(id), reason }));
    const logs = []; const origLog = a.peer._emitLog.bind(a.peer);
    a.peer._emitLog = (lvl, msg, ctx) => { logs.push([msg, ctx]); return origLog(lvl, msg, ctx); };
    const opens = []; const origOpen = a.transport.openConnection.bind(a.transport);
    a.transport.openConnection = async (id) => { opens.push(id); return origOpen(id); };

    // A1: a dense-band candidate with a duty → refused → grace → blocked → duty ends → closed, no mark
    const d0 = await mintInGroup(net, domain, a, [0]);
    check('A1 setup: dense-band candidate minted', d0 !== null);
    const am = a.peer._axonaManager;
    const rb = makeRole(0x5555n, false, Date.now()); rb.backupOf = d0.hex.toLowerCase(); am.axonRoles.set(0x5555n, rb);
    await d0.transport.openConnection(a.hex); await wait(20);
    check('A1 refused at cap, grace pending, no open issued by this node (row 6 path silent)', !a.node.synaptome.has(d0.big) && a.peer._gracePending.has(d0.big) && opens.length === 0, J({ opens }));
    await wait(90);
    check('A1 first fire BLOCKED by duty (row 4): channel open, logged, re-armed', logs.some(([m, c]) => m === 'refuse-grace-blocked' && c?.duty === 'principal') && a.transport.isConnected(d0.hex) && a.peer._gracePending.has(d0.big));
    check('A1 blocked fire wrote NO mark (row 10): table empty, identity eligible', a.node._deadPeers.size === 0 && a.node._deadPeers.eligible(d0.big) === true);
    am.axonRoles.delete(0x5555n);
    await wait(90);
    check('A1 duty ended: next fire CLOSES (row 5 semantics on sim): not connected, not pending', !a.transport.isConnected(d0.hex) && !a.peer._gracePending.has(d0.big));
    check('A1 the close was VOLUNTARY: no onPeerDied at this node', died.length === 0, J(died));
    check('A1 voluntary close wrote NO loss mark; identity still ELIGIBLE', !a.node._deadPeers.has(d0.big) && a.node._deadPeers.eligible(d0.big) === true, J(a.node._deadPeers.stats?.()));
    check('A1 table held at cap throughout (Rule 1)', a.node.synaptome.size === 11);

    // A2: overflow — graceMaxPending 2; the third refusal evicts the OLDEST PERMITTED pending close.
    // p1 (oldest) carries a duty, p2 does not, p3 arrives: p2 must be the one closed; p1 stays; nothing marked.
    const p1 = await mintInGroup(net, domain, a, [0]); const p2 = await mintInGroup(net, domain, a, [0]); const p3 = await mintInGroup(net, domain, a, [0]);
    check('A2 setup: three dense-band candidates minted', p1 && p2 && p3);
    const r1 = makeRole(0x7777n, false, Date.now()); r1.backupOf = p1.hex.toLowerCase(); am.axonRoles.set(0x7777n, r1);
    logs.length = 0; died.length = 0;
    await p1.transport.openConnection(a.hex); await wait(10);
    await p2.transport.openConnection(a.hex); await wait(10);
    check('A2 two pending (at graceMaxPending)', a.peer._gracePending.size === 2 && a.peer._gracePending.has(p1.big) && a.peer._gracePending.has(p2.big), String(a.peer._gracePending.size));
    await p3.transport.openConnection(a.hex); await wait(10);
    check('A2 overflow closed the oldest PERMITTED (p2), kept the duty (p1), queued p3', !a.transport.isConnected(p2.hex) && a.transport.isConnected(p1.hex) && a.peer._gracePending.has(p1.big) && a.peer._gracePending.has(p3.big) && !a.peer._gracePending.has(p2.big), J([...a.peer._gracePending.keys()].map(toHex)));
    check('A2 no grace-overflow-all-blocked (a permitted victim existed)', !logs.some(([m]) => m === 'grace-overflow-all-blocked'));
    check('A2 overflow close was voluntary: no onPeerDied, no mark on p2', died.length === 0 && !a.node._deadPeers.has(p2.big));
    // all-blocked: give p3 a duty too, then a fourth arrival finds no permitted victim
    const r3 = makeRole(0x8888n, false, Date.now()); r3.backupOf = p3.hex.toLowerCase(); am.axonRoles.set(0x8888n, r3);
    const p4 = await mintInGroup(net, domain, a, [0]);
    logs.length = 0;
    await p4.transport.openConnection(a.hex); await wait(10);
    check('A2 all pending are duties: grace-overflow-all-blocked, nothing closed, both kept', logs.some(([m]) => m === 'grace-overflow-all-blocked') && a.transport.isConnected(p1.hex) && a.transport.isConnected(p3.hex), J(logs.map(([m]) => m)));
    check('A2 nothing marked across the overflow', a.node._deadPeers.size === 0, J(a.node._deadPeers.stats?.()));
    check('A2 table held at cap throughout', a.node.synaptome.size === 11);
    am.axonRoles.delete(0x7777n); am.axonRoles.delete(0x8888n);
    // involuntary loss on this same node still marks (row 10 contract intact beside row 5)
    died.length = 0;
    check('A3 setup: p1 unmarked before the loss', !a.node._deadPeers.has(p1.big));
    await p1.transport.closeConnection(a.hex);   // the FAR end closes: involuntary at this node (sim notifies the target; stop() would not)
    await wait(20);
    check('A3 an involuntary loss still fires onPeerDied and writes a loss mark', died.some(d => d.id === p1.hex) && a.node._deadPeers.has(p1.big) && a.node._deadPeers.get(p1.big)?.kind === 'loss', J({ died, mark: a.node._deadPeers.get(p1.big) }));
    await stopAll(a, d0, p2, p3, p4); await p1.peer.stop().catch(() => {});
  }

  // ── B. physical retention under real closes (rows 3 + 5) ──────────
  console.log('\n  B. closeConnection through the ledger: CLOSING charged until the PC reports closed');
  {
    const NID = BigInt('0x' + 'ab'.repeat(33));
    const NID2 = BigInt('0x' + 'cd'.repeat(33));
    // B1: prompted close releases only at 'closed'
    FakePC.fireClosedOnClose = false;
    const mesh = new MeshManager({ sendSignal: () => {}, log: () => {}, ledger: { closeEscalateMs: 0 } });
    const t = new WebRTCTransport({ mesh, log: () => {} });
    await t.start();
    const died = []; t.onPeerDied((id, reason) => died.push({ id: toHex(id), reason }));
    await mesh._initiateTo('c1'); await tick();
    const st = mesh._peers.get('c1'); const pc = st.pc; const tok = st.inc;
    st.dc.open();
    t.bindPeer(NID, 'c1');
    const s0 = mesh.ledgerStats();
    check('B1 setup: OPEN, bound, chanAll 1, boundPeers 1', s0.byState.OPEN === 1 && s0.boundPeers === 1 && s0.all === 1 && t.ownsPeer(NID), J(s0));
    await t.closeConnection(NID);
    const s1 = mesh.ledgerStats();
    check('B1 after closeConnection: identity unbound (peer record gone) BEFORE the channel is', !t.ownsPeer(NID) && s1.boundPeers === 0);
    check('B1 channel CLOSING and still CHARGED: chanAll 1, pc.close() called once', s1.byState.CLOSING === 1 && s1.all === 1 && pc.closeCalls === 1, J(s1));
    // INT-1 (Aster 5a811599): read the reason itself, not only the state; a mutant that nulls the reason must fail here.
    check('B1 the ledger record carries the close reason', mesh._ledger.record(tok)?.state === CHAN.CLOSING && mesh._ledger.record(tok)?.reason === 'closeConnection', J(mesh._ledger.record(tok)));
    check('B1 voluntary: no onPeerDied', died.length === 0, J(died));
    pc.fireClosed(); await tick();
    const s2 = mesh.ledgerStats();
    check('B1 PC reports closed: GONE, released, chanAll 0, goneTotal 1', s2.all === 0 && s2.goneTotal === 1, J(s2));

    // B2: with enforce on and C_phys at the count, an allocation during CLOSING is refused; the first 'closed' lets it through
    const mesh2 = new MeshManager({ sendSignal: () => {}, log: () => {}, ledger: { closeEscalateMs: 0, cPhys: 1, enforce: true } });
    const t2 = new WebRTCTransport({ mesh: mesh2, log: () => {} });
    await t2.start();
    await mesh2._initiateTo('d1'); await tick();
    const st2 = mesh2._peers.get('d1'); st2.dc.open(); t2.bindPeer(NID2, 'd1');
    check('B2 setup: one channel at C_phys 1', mesh2.ledgerStats().all === 1);
    await t2.closeConnection(NID2);
    check('B2 CLOSING holds the slot', mesh2.ledgerStats().byState.CLOSING === 1 && mesh2.ledgerStats().all === 1);
    const before = FakePC.instances.length;
    const refusedBefore = mesh2.ledgerStats().refusedOut;
    await mesh2._initiateTo('d2'); await tick();
    const sR = mesh2.ledgerStats();
    // INT-2 (Aster 5a811599): the pinned stats field is refusedOut; assert its delta with no fallback that passes on absence.
    check('B2 allocation during CLOSING refused: no PC built, refusedOut +1', FakePC.instances.length === before && !mesh2._peers.has('d2') && typeof sR.refusedOut === 'number' && sR.refusedOut === refusedBefore + 1, J({ refusedBefore, stats: sR }));
    st2.pc.fireClosed(); await tick();
    check('B2 first closed releases: chanAll 0', mesh2.ledgerStats().all === 0);
    await mesh2._initiateTo('d3'); await tick();
    check('B2 the next allocation goes through', mesh2._peers.has('d3') && mesh2.ledgerStats().all === 1);

    // B3: an involuntary loss on the same transport still fires onPeerDied (the row-10 input survives row 5)
    died.length = 0;
    await mesh._initiateTo('c3'); await tick(); mesh._peers.get('c3').dc.open(); t.bindPeer(NID, 'c3');
    mesh._retire('c3', 'pong-timeout'); await tick();
    check('B3 involuntary loss: onPeerDied with its reason', died.length === 1 && died[0].reason === 'pong-timeout', J(died));
    await t.stop(); mesh.dispose(); await t2.stop(); mesh2.dispose();
    FakePC.fireClosedOnClose = true;
  }

  // ── C. the cap / reservation schedule (row 15 NOT STARTED; observation) ──
  console.log('\n  C. case 61 on the combined tree (row 15 not started): recorded, not claimed');
  {
    const a = await makePeer(net, domain, 10, 10, {});
    a.node._maxSynaptome = 4;
    const b = await makePeer(net, domain, 11, 11, {}); const c = await makePeer(net, domain, 12, 12, {});
    await b.transport.openConnection(a.hex); await c.transport.openConnection(a.hex); await wait(10);
    a.node.synaptome.delete(b.big); a.node.synaptome.delete(c.big);
    while (a.node.synaptome.size < 3) craft(a, 1n << BigInt(100 + a.node.synaptome.size), 0.9);
    while (a.node.synaptome.size > 3) { const k = [...a.node.synaptome.keys()].find(x => a.node.synaptome.get(x)._addedBy === 'crafted'); a.node.synaptome.delete(k); }
    check('C setup: size 3, cap 4, two bound candidates not in the table', a.node.synaptome.size === 3 && a.transport.isConnected(b.hex) && a.transport.isConnected(c.hex) && !a.node.synaptome.has(b.big) && !a.node.synaptome.has(c.big));
    // hold both opens across the same await so both pass the size check first
    const origOpen = a.transport.openConnection.bind(a.transport);
    let release; const gate = new Promise(r => { release = r; });
    a.transport.openConnection = async (id) => { await gate; return origOpen(id); };
    const pA = a.peer._addByVitality(candidateSyn(a, b.big)); const pB = a.peer._addByVitality(candidateSyn(a, c.big));
    await tick(); release(); const [rA, rB] = await Promise.all([pA, pB]);
    const size = a.node.synaptome.size;
    console.log(`     observation: both returned ${rA}/${rB}; table size ${size} at cap 4`);
    check('C OBSERVATION (pre-existing, row 15 not started): both insert, size 5 at cap 4 — the design\'s case 61 schedule reproduced on the combined tree', rA === true && rB === true && size === 5, `size ${size}`);
    // the fixed half (row 6): AT cap, nothing opens, nothing inserts
    a.transport.openConnection = async (id) => { throw new Error('open at cap: ' + toHex(id)); };
    const d = await makePeer(net, domain, 13, 13, {}); await d.transport.openConnection(a.hex); await wait(10); a.node.synaptome.delete(d.big);
    const before = a.node.synaptome.size;
    const r = await a.peer._addByVitality(candidateSyn(a, d.big, 'lateral'));
    check('C at cap (row 6): returns false, no open, no insert, vitality-swap-skipped counted', r === false && a.node.synaptome.size === before && !a.node.synaptome.has(d.big) && (a.peer._vitalitySwapSkipped || 0) >= 1);
    await stopAll(a, b, c, d);
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
