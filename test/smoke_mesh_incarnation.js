// =====================================================================
// smoke_mesh_incarnation.js — every RTCPeerConnection carries its own
//   incarnation token, on every per-connection log event and on the PC.
//
// Why (4.101.0, council 6a46f038): a mesh peerId is the bridge's connection
// handle, and the retry path retires a failed PC and re-initiates to the SAME
// peerId inside the same process. Records keyed by peerId (or run + peerId)
// therefore conflate two connections. The token is `<run>.<n>`: a random
// per-MeshManager run tag plus a counter, set in _attachPc.
//
// Contract:
//   · two PCs to the same peerId get DIFFERENT tokens with the SAME run tag;
//   · two MeshManagers get DIFFERENT run tags;
//   · pc.axonaInc === state.inc (so a host observer can join on it);
//   · pc-state, dc-open, dc-close, retry and teardown all carry `inc`, and the
//     teardown of a retried PC carries the OLD token, not the new one.
//
// Run: node test/smoke_mesh_incarnation.js
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

class FakePC {
  constructor() { this.connectionState = 'new'; this.iceConnectionState = 'new'; }
  createDataChannel() { return { readyState: 'connecting', close() {}, send() {} }; }
  close() {}
  async getStats() { return new Map(); }
}
globalThis.RTCPeerConnection = FakePC;

const PEER = 'c2ab';

function main() {
  console.log('MeshManager per-connection incarnation token\n');
  const logs = [];
  const mesh = new MeshManager({ sendSignal: () => {}, log: (e, c) => logs.push({ e, c }) });

  const s1 = mesh._newPeerState(PEER, 'offerer');
  const pc1 = mesh._attachPc(s1);
  const s2 = mesh._newPeerState(PEER, 'offerer');
  const pc2 = mesh._attachPc(s2);

  check('state.inc set on attach', typeof s1.inc === 'string' && s1.inc.length > 0);
  check('same peerId, different PCs → different tokens', s1.inc !== s2.inc);
  check('same MeshManager → same run tag', s1.inc.split('.')[0] === s2.inc.split('.')[0]);
  check('pc.axonaInc mirrors state.inc', pc1.axonaInc === s1.inc && pc2.axonaInc === s2.inc);

  const other = new MeshManager({ sendSignal: () => {}, log: () => {} });
  const s3 = other._newPeerState(PEER, 'offerer'); other._attachPc(s3);
  check('different MeshManager → different run tag', s3.inc.split('.')[0] !== s1.inc.split('.')[0]);

  // pc-state carries the token of the PC it describes
  mesh._peers.set(PEER, s1);
  mesh._onConnState(s1, 'connecting');
  const ps = logs.find((l) => l.e === 'pc-state');
  check('pc-state carries inc', ps && ps.c.inc === s1.inc);

  // teardown of the FIRST PC carries the first token, even though a second
  // PC to the same peerId already exists
  s1.openedAt = Date.now();
  mesh._retire(PEER, 'retry', { keepDeadline: true, notifyLost: false });
  const td = logs.find((l) => l.e === 'teardown');
  check('teardown carries the retired PC\'s token', td && td.c.inc === s1.inc && td.c.inc !== s2.inc);

  // dc-open / dc-close carry the token (wire a channel through the real path)
  const s4 = mesh._newPeerState('c2cd', 'offerer');
  mesh._peers.set('c2cd', s4);
  mesh._attachPc(s4);
  const dc = { readyState: 'open', close() {}, send() {} };
  mesh._wireDataChannel(s4, dc);
  if (typeof dc.onopen === 'function') {
    dc.onopen(); dc.onclose?.();
    const open = logs.find((l) => l.e === 'dc-open' && l.c.peerId === 'c2cd');
    const close = logs.find((l) => l.e === 'dc-close' && l.c.peerId === 'c2cd');
    check('dc-open carries inc',  open && open.c.inc === s4.inc);
    check('dc-close carries inc', close && close.c.inc === s4.inc);
  } else {
    check("dc wiring helper found", false);
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main();
