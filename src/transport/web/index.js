// =====================================================================
// transport/web/index.js — browser-side Transport implementations.
//
// T1 part 1 (current commit): WebRTCTransport — the core typed
// request/response + notification + onPeerDied surface, wrapping a
// MeshLike abstraction that hides RTCPeerConnection details.
//
// T1 part 2 (next): MeshManager (the WebRTC + signaling driver),
// BridgeTransport (WebSocket fallback for unbound peers + bridge-
// originated traffic), CompositeTransport (fans send/notify between
// the two), and a clean `webTransport({...})` factory that wires the
// stack from configuration. Hello/hello-ack admission is also part 2.
//
// Until then, advanced consumers can construct a WebRTCTransport
// directly with a MeshManager-shaped dependency they own:
//
//     import { WebRTCTransport } from '@axona/protocol/transport/web';
//     const t = new WebRTCTransport({ mesh: myMesh, localNodeId, log });
//     await t.start();
//     t.bindPeer(nodeIdHex, meshId);
//     await t.openConnection(nodeIdHex);
//     const reply = await t.send(nodeIdHex, 'lookup_step', { ... });
// =====================================================================

export { WebRTCTransport } from './webrtc.js';
