#!/usr/bin/env node
// fence_ledger_browser_timers — the ChannelLedger calls the host timers as FREE
// FUNCTIONS, never as methods of itself.
//
// WHY. 2026-10-08, axona.chat 0.81.0 on kernel 4.107.0, David's console:
//   Uncaught TypeError: Illegal invocation
//       at Ce.closing (ChannelLedger.closing) ← Ae._retire ← onPeerLeft / _reapTick
// channel_ledger.js stored the host's native `setTimeout` on `this._setTimeout`
// and closing() called `this._setTimeout(fn, ms)`: in a browser that runs
// window.setTimeout with `this` = the ledger, and Chrome throws. The throw sits
// inside mesh._retire BEFORE `_peers.delete(peerId)`, so every retired channel
// stayed in the mesh's map and onPeerLost never fired: zombie channels in every
// browser client, since the ledger shipped. Node accepts any `this` for its
// timers, so the suite never saw it.
//
// THE FENCE emulates the browser: it injects timers that THROW the browser's
// TypeError when called with a `this` that is not undefined or globalThis, and
// drives the real MeshManager through a retire. Mutant: restore the direct
// property assignment → A1/A2 and B1 fail.
import { readFileSync } from 'node:fs';
import { ChannelLedger } from '../src/transport/web/channel_ledger.js';
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

// Browser-strict timers: a method-style call (this === some object) throws
// exactly as Chrome's WebIDL binding does.
function strict(fn, name) {
  return function (...args) {
    if (this !== undefined && this !== globalThis) throw new TypeError(`Illegal invocation (${name} called with a foreign this)`);
    return fn(...args);
  };
}
const strictSetTimeout = strict(setTimeout, 'setTimeout');
const strictClearTimeout = strict(clearTimeout, 'clearTimeout');

(async () => {
  console.log('fence_ledger_browser_timers: the ledger never calls a host timer as its own method');

  // ── A. the ledger alone ───────────────────────────────────────────────
  {
    const L = new ChannelLedger({ setTimeout: strictSetTimeout, clearTimeout: strictClearTimeout, closeEscalateMs: 50, log: () => {} });
    L.allocate('t1', 'm1', 'in'); L.negotiating('t1'); L.open('t1');
    let threw = null;
    try { L.closing('t1', 'test'); } catch (e) { threw = e; }
    check('A1 closing() under browser-strict timers does not throw', threw === null, String(threw?.message));
    let threw2 = null;
    try { L.gone('t1'); } catch (e) { threw2 = e; }
    check('A2 gone() (clearTimeout path) under browser-strict timers does not throw', threw2 === null, String(threw2?.message));
    L.dispose();
  }

  // ── B. through the real mesh: a retire with the escalate path armed ───
  {
    class FakeDC { constructor() { this.readyState = 'open'; } close() { this.readyState = 'closed'; } }
    class FakePC { constructor() { this.connectionState = 'new'; this.onconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.localDescription = null; this.remoteDescription = null; } createDataChannel() { return new FakeDC(); } async createOffer() { return { type: 'offer', sdp: 'v=0' }; } async setLocalDescription(d) { this.localDescription = d; } async setRemoteDescription(d) { this.remoteDescription = d; } async addIceCandidate() {} async getStats() { return new Map(); } close() { this.connectionState = 'closed'; } }
    globalThis.RTCPeerConnection = FakePC;
    const mesh = new MeshManager({ sendSignal: () => {}, log: () => {}, ledger: { setTimeout: strictSetTimeout, clearTimeout: strictClearTimeout, closeEscalateMs: 50 } });
    await mesh._initiateTo('c1'); await new Promise((r) => setTimeout(r, 0));
    const st = mesh._peers.get('c1'); st.dc.onopen();
    let threw = null;
    try { mesh._retire('c1', 'peer-left'); } catch (e) { threw = e; }
    check('B1 mesh._retire under browser-strict timers does not throw, and the peer leaves the map', threw === null && !mesh._peers.has('c1'), String(threw?.message));
    mesh.dispose();
  }

  // ── C. static: no host timer is stored as an instance property and called as a method ──
  {
    const src = readFileSync(new URL('../src/transport/web/channel_ledger.js', import.meta.url), 'utf8');
    check('C1 the ledger does not assign a host timer directly to an instance property', !/this\._setTimeout\s*=\s*typeof o\.setTimeout === 'function' \? o\.setTimeout : setTimeout;/.test(src) && !/this\._clearTimeout\s*=\s*typeof o\.clearTimeout === 'function' \? o\.clearTimeout : clearTimeout;/.test(src));
    check('C2 the timers are wrapped as free-function calls', /this\._setTimeout\s*=\s*\(fn, ms\) => st\(fn, ms\)/.test(src) && /this\._clearTimeout\s*=\s*\(h\) => ct\(h\)/.test(src));
  }

  console.log(`\nfence_ledger_browser_timers: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
