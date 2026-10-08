#!/usr/bin/env node
// fence_bind_deadline_all_channels — an open mesh channel whose peer never
// completes the handshake is retired at the bind deadline, on EVERY channel,
// not only a provisional one.
//
// WHY. 2026-10-08, east production door: sixteen sockets from one host ran a
// kernel-4.84.0 application that opened data channels and sent pings and never
// a mesh hello. A 4.107.0 newcomer dialled eight of them, opened eight
// channels, bound one, and kept the other seven as live for the whole run —
// the peer's legacy pings kept vitality up and nothing else looked at an open
// channel without an identity. Only a PROVISIONAL channel (degree policy,
// socket-is-bootstrap v0.5) had a bind deadline, and nothing is provisional
// with that flag off. Howard's suite read 84/92 topics mismatched through
// that door (ops/probe-bind.mjs, STATE.md 14:31Z).
//
// THE FENCE drives the real MeshManager with a fake RTCPeerConnection:
//   A1 open + never bound          → retired at the deadline, reason bind-timeout, counted
//   A2 open + bound (ledgerBind)   → survives the deadline
//   A3 bindDeadlineMs: 0           → never retired (off)
//   A4 the responder role          → same deadline (the open path is shared)
//   A5 a bind AFTER expiry is impossible: the peer is gone, ledgerBind is a no-op
// Mutant: delete the every-channel arming in dc.onopen → A1 and A4 fail.
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class FakeDC { constructor() { this.readyState = 'open'; } close() { this.readyState = 'closed'; } send() {} }
class FakePC {
  constructor() { this.connectionState = 'new'; this.iceConnectionState = 'new'; this.onconnectionstatechange = null; this.onicecandidate = null; this.ondatachannel = null; this.localDescription = null; this.remoteDescription = null; }
  createDataChannel() { return new FakeDC(); }
  async createOffer() { return { type: 'offer', sdp: 'v=0' }; }
  async createAnswer() { return { type: 'answer', sdp: 'v=0' }; }
  async setLocalDescription(d) { this.localDescription = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate() {}
  async getStats() { return new Map(); }
  close() { this.connectionState = 'closed'; }
}
globalThis.RTCPeerConnection = FakePC;

function mkMesh(opts = {}) {
  const events = [];
  const mesh = new MeshManager({ sendSignal: () => {}, log: (e, c) => events.push({ e, c }), ...opts });
  return { mesh, events };
}
async function openOutbound(mesh, id) {
  await mesh._initiateTo(id); await sleep(0);
  const st = mesh._peers.get(id); st.dc.onopen(); return st;
}

(async () => {
  console.log('fence_bind_deadline_all_channels: an open channel that never binds is retired on every channel');
  const DEADLINE = 60;

  // A1 — open, never bound → retired at the deadline.
  {
    const { mesh, events } = mkMesh({ bindDeadlineMs: DEADLINE });
    const st = await openOutbound(mesh, 'c1');
    check('A1a the channel is open and unbound before the deadline', mesh._peers.get('c1') === st && !st.boundAt);
    await sleep(DEADLINE * 2);
    const to = events.find((x) => x.e === 'bind-timeout' && x.c?.peerId === 'c1');
    const td = events.find((x) => x.e === 'teardown' && x.c?.peerId === 'c1');
    check('A1b retired at the deadline: bind-timeout logged, teardown reason bind-timeout, peer gone', !!to && td?.c?.reason === 'bind-timeout' && !mesh._peers.has('c1'), JSON.stringify({ to: !!to, reason: td?.c?.reason, has: mesh._peers.has('c1') }));
    check('A1c degreeStats().bindTimeouts counts it', mesh.degreeStats().bindTimeouts === 1, String(mesh.degreeStats().bindTimeouts));
    mesh.dispose();
  }

  // A2 — open, bound before the deadline → survives.
  {
    const { mesh, events } = mkMesh({ bindDeadlineMs: DEADLINE });
    const st = await openOutbound(mesh, 'c2');
    mesh.ledgerBind('c2', '89'.padEnd(66, '0'));
    check('A2a ledgerBind stamps boundAt and clears the timer', st.boundAt > 0 && st.bindTimer === null);
    await sleep(DEADLINE * 2);
    check('A2b a bound channel survives the deadline', mesh._peers.get('c2') === st && !events.some((x) => x.e === 'bind-timeout'));
    mesh.dispose();
  }

  // A3 — deadline 0 = off.
  {
    const { mesh, events } = mkMesh({ bindDeadlineMs: 0 });
    const st = await openOutbound(mesh, 'c3');
    await sleep(DEADLINE * 2);
    check('A3 bindDeadlineMs: 0 arms nothing and retires nothing', mesh._peers.get('c3') === st && st.bindTimer === null && !events.some((x) => x.e === 'bind-timeout'));
    mesh.dispose();
  }

  // A4 — responder role: an inbound offer opens a channel through the same path.
  {
    const { mesh, events } = mkMesh({ bindDeadlineMs: DEADLINE });
    await mesh.onSignal('c4', { kind: 'sdp-offer', sdp: 'v=0', attempt: 'att-4' }); await sleep(0);
    const st = mesh._peers.get('c4');
    const dc = new FakeDC();
    if (st?.pc?.ondatachannel) st.pc.ondatachannel({ channel: dc });
    const opened = st && mesh._peers.get('c4') === st && typeof st.dc?.onopen === 'function';
    if (opened) st.dc.onopen();
    await sleep(DEADLINE * 2);
    const td = events.find((x) => x.e === 'teardown' && x.c?.peerId === 'c4');
    check('A4 a responder-role channel that never binds is retired too', opened && td?.c?.reason === 'bind-timeout' && !mesh._peers.has('c4'), JSON.stringify({ opened, reason: td?.c?.reason, role: st?.role }));
    mesh.dispose();
  }

  // A5 — a late bind on a retired peer is a no-op (no resurrection, no throw).
  {
    const { mesh } = mkMesh({ bindDeadlineMs: DEADLINE });
    await openOutbound(mesh, 'c5');
    await sleep(DEADLINE * 2);
    let threw = null;
    try { mesh.ledgerBind('c5', '89'.padEnd(66, '0')); } catch (e) { threw = e; }
    check('A5 ledgerBind after expiry neither throws nor resurrects the peer', threw === null && !mesh._peers.has('c5'), String(threw?.message));
    mesh.dispose();
  }

  console.log(`\nfence_bind_deadline_all_channels: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
