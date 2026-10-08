// =====================================================================
// fence_close_connection.mjs — Hold-and-Fill v0.5/v0.7 (axona-docs 4334504,
// 95c2ff4), repair row 5: closeConnection CLOSES.
//
// What this fences:
//   1. WebRTCTransport.closeConnection(nodeId) unbinds AND tears the channel
//      down: the mesh retires the peer (hasPeer false), the PC is closed, and
//      the identity is no longer bound. Before this row it only unbound and
//      the RTCPeerConnection stayed open.
//   2. A voluntary close is NOT a death: no onPeerDied fires, so the kernel
//      writes no loss mark. Order is unbind first, then disconnect.
//   3. Idempotent: unknown identity → nothing; a second call → nothing; a
//      channel already retired by the mesh → nothing thrown.
//   4. An involuntary loss still fires onPeerDied (the existing contract is
//      untouched).
//   5. THE CALLER LIST. Every closeConnection call site in src/dht/AxonaPeer.js
//      is one of the classified rows of the design's boundary table (v0.5
//      "The boundary"): peer-leaving, _clearGracePending, the gate's grace
//      close, the gate's overflow close, the gate-swap victim,
//      _evictAndReplace's dead synapse. A new caller fails this check until
//      it is classified. As read at 270835d the list also held
//      _addByVitality's victim and _tryAnneal's victim (eight sites); row 6
//      (0e335d5) removed the anneal and the at-cap swap, so on the combined
//      tree (hold-and-fill-phase1) there are six sites and those two callers
//      are REQUIRED ABSENT: their return fails this check too.
//
// With the row-5 change reverted (unbind only), checks 1 fail: the mesh
// still has the peer and the PC is open.
//
// Run: node test/fence_close_connection.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { MeshManager } from '../src/transport/web/mesh.js';
import { WebRTCTransport } from '../src/transport/web/webrtc.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = () => new Promise(r => setTimeout(r, 0));

class FakeDC { constructor() { this.readyState = 'connecting'; this.onopen = null; this.onclose = null; this.onmessage = null; this.onerror = null; } send() {} close() { this.readyState = 'closed'; } open() { this.readyState = 'open'; this.onopen?.(); } }
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16) : x));
class FakePC {
  constructor() { FakePC.instances.push(this); this.connectionState = 'new'; this.iceConnectionState = 'new'; this.remoteDescription = null; this.localDescription = null; this.onconnectionstatechange = null; this.oniceconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.closeCalls = 0; }
  createDataChannel() { return new FakeDC(); }
  async createOffer() { return { type: 'offer', sdp: 'v=0 offer' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0 answer' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.closeCalls++; this.connectionState = 'closed'; queueMicrotask(() => { try { this.onconnectionstatechange?.(); } catch {} }); }
}
FakePC.instances = [];
globalThis.RTCPeerConnection = FakePC;

(async () => {
  console.log('fence_close_connection: row 5 — closeConnection closes');
  const mkMesh = () => new MeshManager({ sendSignal: () => {}, log: () => {} });
  const NID = BigInt('0x' + 'ab'.repeat(33));

  // 1 + 2: a bound, open channel; closeConnection closes it; no death.
  {
    const mesh = mkMesh();
    const t = new WebRTCTransport({ mesh, log: () => {} });
    await t.start();
    const died = [];
    t.onPeerDied((id, reason) => died.push({ id, reason }));
    await mesh._initiateTo('c1'); await tick();
    const st = mesh._peers.get('c1'); const pc = st.pc;
    st.dc.open();
    t.bindPeer(NID, 'c1');
    check('1 setup: bound and open', t.ownsPeer(NID) && t.isConnected(NID) && mesh.hasPeer('c1'));
    await t.closeConnection(NID);
    await tick(); await tick();
    check('1 closeConnection: identity unbound', !t.ownsPeer(NID) && t.meshIdFor(NID) === null);
    check('1 closeConnection: mesh retired the peer (hasPeer false)', !mesh.hasPeer('c1'));
    check('1 closeConnection: the PC was closed', pc.closeCalls === 1 && pc.connectionState === 'closed');
    check('2 a voluntary close is not a death: no onPeerDied', died.length === 0, J(died));
    // 3 idempotent
    let threw = false;
    try { await t.closeConnection(NID); await t.closeConnection(BigInt('0x' + 'cd'.repeat(33))); } catch { threw = true; }
    check('3 second call and unknown identity: nothing thrown, nothing changed', !threw && !mesh.hasPeer('c1') && pc.closeCalls === 1);
    // a channel the mesh already retired: closeConnection must not throw
    await mesh._initiateTo('c2'); await tick(); mesh._peers.get('c2').dc.open(); t.bindPeer(NID, 'c2');
    mesh._retire('c2', 'pong-timeout'); await tick(); await tick();
    threw = false; try { await t.closeConnection(NID); } catch { threw = true; }
    check('3 channel already retired by the mesh: no throw', !threw);
    // 4 involuntary loss still fires onPeerDied
    died.length = 0;
    await mesh._initiateTo('c3'); await tick(); mesh._peers.get('c3').dc.open(); t.bindPeer(NID, 'c3');
    mesh._retire('c3', 'pong-timeout'); await tick();
    check('4 involuntary loss still fires onPeerDied with its reason', died.length === 1 && died[0].reason === 'pong-timeout', J(died));
    await t.stop(); mesh.dispose();
  }

  // 5 the caller list
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const lines = src.split('\n');
    const sites = [];
    lines.forEach((l, i) => { if (/closeConnection\?\?\.\(|closeConnection\(|closeConnection\?\.\(/.test(l) && !/^\s*(\/\/|\*)/.test(l)) sites.push(i + 1); });
    // classify each site by the nearest enclosing method name above it
    const methodOf = (ln) => { for (let i = ln - 1; i >= 0; i--) { const m = lines[i].match(/^  (?:async )?([A-Za-z_]\w*)\s*\(.*\)\s*\{\s*$/); if (m) return m[1]; } return '?'; };
    const found = sites.map(ln => methodOf(ln));
    // Socket-is-bootstrap v0.5: the gate's swap-victim close moved from
    // _admitOrImprove into _gateCommit (decision and commit split so a bridge
    // can preflight without side effects); same row, same close.
    const allowed = ['_clearGracePending', '_seedSynaptomeWithSponsor', '_gateCommit', '_evictAndReplace', '_installRoutingHandlers'];
    // `_installRoutingHandlers` holds the peer-leaving notification handler
    // (AxonaPeer.js ~:888): the departing peer announced itself; involuntary.
    const unknown = found.filter(m => !allowed.includes(m));
    check('5 every closeConnection caller is a classified row', unknown.length === 0, `unclassified: ${JSON.stringify(unknown)} all: ${JSON.stringify(found)}`);
    check('5 the classified callers are all still present (the table is the code)', ['_clearGracePending', '_seedSynaptomeWithSponsor', '_gateCommit', '_evictAndReplace', '_installRoutingHandlers'].every(m => found.includes(m)), JSON.stringify(found));
    check('5 row 6 callers are absent: no _addByVitality victim, no _tryAnneal', !found.includes('_addByVitality') && !found.includes('_tryAnneal') && !/_tryAnneal\s*\(/.test(src), JSON.stringify(found));
    check('5 site count on the combined tree: six (eight at 270835d minus row 6)', sites.length === 6, String(sites.length) + ' at lines ' + sites.join(','));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
