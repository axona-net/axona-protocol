#!/usr/bin/env node
// fence_retire_deferred_close — the mesh retires a channel's bookkeeping NOW
// and closes its native objects on the NEXT MACROTASK, never on the stack
// that retired it (4.108.0).
//
// WHY. 2026-10-09: two Windows relays (20 on one box) froze for 5 h and 16 h
// with the event loop dead, CPU time frozen, sockets held. Both logs end in
// the same second of the same sequence: a graduated relay re-dialled the
// bridge for a TURN credential, dialled an announced peer it already held, the
// new channel opened, the hello arrived ON THAT CHANNEL, bindPeer deduplicated
// it against the older channel and retired the new one — `dc.close()` and
// `pc.close()` called synchronously while that channel's own data-channel
// callback was still on the stack. libdatachannel documents that closing a
// PeerConnection from inside one of its callbacks can deadlock.
//
// THE FENCE drives the real MeshManager with a fake RTCPeerConnection whose
// close() records when it ran relative to the retire call:
//   A1 retire from inside the channel's own dc.onmessage: the peer is gone from
//      the map and `teardown` logged synchronously, the native close has NOT
//      run when the callback returns, and HAS run after one macrotask.
//   A2 the same through the public disconnect() path the dedup uses.
//   A3 dispose() closes synchronously (no callback can be on the stack there,
//      and the process may be about to exit).
//   A4 a retire issued twice closes once.
//   A5 static: the retire path has no synchronous pc.close() outside the
//      immediateClose branch.
// Mutant: replace `deferMacrotask(closeNative)` with `closeNative()` → A1, A2 fail.
import { readFileSync } from 'node:fs';
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const macrotask = () => new Promise((r) => setImmediate(r));

const closes = [];   // [peerId-ish, 'dc'|'pc', phase]
let phase = 'idle';
class FakeDC { constructor(tag) { this.tag = tag; this.readyState = 'open'; } close() { this.readyState = 'closed'; closes.push([this.tag, 'dc', phase]); } send() {} }
class FakePC {
  constructor() { this.tag = null; this.connectionState = 'new'; this.iceConnectionState = 'new'; this.onconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.localDescription = null; this.remoteDescription = null; this.closed = 0; }
  createDataChannel() { return new FakeDC(this.tag); }
  async createOffer() { return { type: 'offer', sdp: 'v=0' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.closed++; this.connectionState = 'closed'; closes.push([this.tag, 'pc', phase]); }
}
globalThis.RTCPeerConnection = FakePC;

function mkMesh() { const events = []; const mesh = new MeshManager({ sendSignal: () => {}, log: (e, c) => events.push({ e, c }), ledger: false }); return { mesh, events }; }
async function open(mesh, id) { await mesh._initiateTo(id); await new Promise((r) => setTimeout(r, 0)); const st = mesh._peers.get(id); st.pc.tag = id; st.dc.tag = id; st.dc.onopen(); return st; }

(async () => {
  console.log('fence_retire_deferred_close: the native close leaves the stack that retired the channel');

  // A1 — retire from inside the channel's own data-channel callback.
  {
    const { mesh, events } = mkMesh(); closes.length = 0;
    const st = await open(mesh, 'c1');
    let insideResult = null;
    phase = 'in-callback';
    // Simulate: a frame arrives on c1's data channel and the handler decides to retire c1.
    const handler = () => {
      mesh._retire('c1', 'duplicate-nodeId');
      insideResult = { gone: !mesh._peers.has('c1'), torn: events.some((x) => x.e === 'teardown' && x.c?.peerId === 'c1'), closedYet: st.pc.closed };
    };
    handler();   // the callback returns here; nothing native may have run yet
    phase = 'after-callback';
    check('A1a inside the callback: peer gone from the map and teardown logged', insideResult.gone && insideResult.torn, JSON.stringify(insideResult));
    check('A1b inside the callback: the native close has NOT run', insideResult.closedYet === 0 && closes.length === 0, JSON.stringify(closes));
    await macrotask();
    check('A1c one macrotask later: dc and pc closed, in that order', st.pc.closed === 1 && closes.map((c) => c[1]).join(',') === 'dc,pc' && closes.every((c) => c[2] === 'after-callback'), JSON.stringify(closes));
    mesh.dispose();
  }

  // A2 — the public disconnect() path (what webrtc.js bindPeer's dedup calls).
  {
    const { mesh } = mkMesh(); closes.length = 0;
    const st = await open(mesh, 'c2');
    phase = 'in-callback';
    mesh.disconnect('c2', 'duplicate-nodeId');
    const sync = { gone: !mesh._peers.has('c2'), closed: st.pc.closed };
    phase = 'after-callback';
    await macrotask();
    check('A2 disconnect(): gone synchronously, closed one macrotask later', sync.gone && sync.closed === 0 && st.pc.closed === 1, JSON.stringify({ sync, later: st.pc.closed }));
    mesh.dispose();
  }

  // A3 — dispose() closes synchronously.
  {
    const { mesh } = mkMesh(); closes.length = 0;
    const st = await open(mesh, 'c3');
    phase = 'dispose';
    mesh.dispose();
    check('A3 dispose() closes on the spot (no deferral at shutdown)', st.pc.closed === 1 && closes.length === 2 && closes.every((c) => c[2] === 'dispose'), JSON.stringify(closes));
  }

  // A4 — a double retire closes once.
  {
    const { mesh } = mkMesh(); closes.length = 0;
    const st = await open(mesh, 'c4');
    phase = 'x';
    mesh._retire('c4', 'first'); mesh._retire('c4', 'second');
    await macrotask();
    check('A4 retiring twice closes once', st.pc.closed === 1 && closes.filter((c) => c[1] === 'pc').length === 1, String(st.pc.closed));
    mesh.dispose();
  }

  // A5 — static.
  {
    const src = readFileSync(new URL('../src/transport/web/mesh.js', import.meta.url), 'utf8');
    const retire = src.slice(src.indexOf('  _retire(peerId, reason'), src.indexOf('this._peers.delete(peerId);', src.indexOf('  _retire(peerId, reason')));
    check('A5 _retire reaches pc.close() only through the deferred/immediate branch', /if \(immediateClose\) closeNative\(\); else deferMacrotask\(closeNative\);/.test(retire) && !/^\s*if \(state\.pc\) try \{ state\.pc\.close\(\); \} catch \{\}/m.test(retire));
  }

  console.log(`\nfence_retire_deferred_close: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
