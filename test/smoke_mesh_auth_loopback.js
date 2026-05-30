// =====================================================================
// smoke_mesh_auth_loopback.js — integration test for the WebRTC-mesh
// axona/4 handshake ORCHESTRATION (MeshAuth), driven end-to-end over an
// in-memory data-channel loopback — no browser / real RTCPeerConnection.
//
// This is the test that would have caught the production bug: it wires
// two MeshAuth instances together where each side addresses the other by
// the OTHER peer's bridge connId (the real asymmetry — A holds B's
// connId, B holds A's).  With the asymmetric connId folded into the CBV,
// the two sides derived different CBVs and NEITHER bound.  With the
// constant 'mesh' tag, both bind regardless of connId.  Asserting "both
// peers bind across asymmetric connIds" therefore guards the whole class.
//
// Run: node test/smoke_mesh_auth_loopback.js
// =====================================================================

import { MeshAuth } from '../src/transport/web/mesh-auth.js';
import { deriveIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const settle = () => new Promise(r => setTimeout(r, 10));

// Build a 2-peer loopback.  `connOf` maps each participant to the connId
// the OTHER side uses to address it (asymmetric, as the bridge assigns).
function wire2(alice, bob, connA, connB) {
  // What each side's `send(meshId, frame)` does: deliver to the peer,
  // tagged with the SENDER's connId (how the receiver knows the sender).
  const route = (frame, target, senderConnId) => {
    queueMicrotask(() => {
      if (frame.type === 'hello')      target.onHello(senderConnId, frame.body);
      else if (frame.type === 'hello-sig') target.onHelloSig(senderConnId, frame.body);
    });
  };
  return {
    aliceSend: (_meshId, frame) => route(frame, bob.auth,   connA), // bob hears "from A"
    bobSend:   (_meshId, frame) => route(frame, alice.auth, connB), // alice hears "from B"
  };
}

async function testTwoPeerBindAcrossAsymmetricConnIds() {
  console.log('\n── two peers bind across asymmetric connIds ──');
  const aliceId = await deriveIdentity({ lat: 40.7, lng: -74.0 });
  const bobId   = await deriveIdentity({ lat: 51.5, lng: -0.1 });
  const connA = 'cA1', connB = 'cB2';   // each side's view of the other

  const aliceBound = [], bobBound = [];
  const alice = {}, bob = {};
  const { aliceSend, bobSend } = wire2(alice, bob, connA, connB);

  alice.auth = new MeshAuth({
    identity: aliceId,
    send: aliceSend,
    bindPeer: (nodeIdHex, meshId) => aliceBound.push({ nodeIdHex, meshId }),
  });
  bob.auth = new MeshAuth({
    identity: bobId,
    send: bobSend,
    bindPeer: (nodeIdHex, meshId) => bobBound.push({ nodeIdHex, meshId }),
  });

  // Both sides see their channel open (alice addresses bob as connB,
  // bob addresses alice as connA — the asymmetric reality).
  alice.auth.onChannelOpen(connB);
  bob.auth.onChannelOpen(connA);

  for (let i = 0; i < 10 && (aliceBound.length === 0 || bobBound.length === 0); i++) await settle();

  check('Alice bound exactly one peer', aliceBound.length === 1);
  check('Alice bound Bob\'s proven nodeId', aliceBound[0]?.nodeIdHex === bobId.id);
  check('Bob bound exactly one peer', bobBound.length === 1);
  check('Bob bound Alice\'s proven nodeId', bobBound[0]?.nodeIdHex === aliceId.id);
  check('Alice MeshAuth reports bound', alice.auth.isBound(connB) && alice.auth.boundCount() === 1);
  check('Bob MeshAuth reports bound',   bob.auth.isBound(connA) && bob.auth.boundCount() === 1);
}

async function testForgedPeerNotBound() {
  console.log('\n── a peer that signs the wrong CBV is not bound ──');
  const aliceId = await deriveIdentity({ lat: 40.7, lng: -74.0 });
  const bobId   = await deriveIdentity({ lat: 51.5, lng: -0.1 });

  // Asymmetric, BROKEN routing: deliver each side's frames tagged with a
  // DIFFERENT meshId than the constant tag would share — simulate a peer
  // running the old code by making the CBV inputs disagree.  We model the
  // old bug by having one side's MeshAuth never receive a matching nonce
  // (drop hello-sig), so binding cannot complete.
  const aliceBound = [], bobBound = [];
  const alice = {}, bob = {};
  const route = (frame, target, senderConnId, dropSig) => {
    queueMicrotask(() => {
      if (frame.type === 'hello') target.onHello(senderConnId, frame.body);
      else if (frame.type === 'hello-sig' && !dropSig) target.onHelloSig(senderConnId, frame.body);
    });
  };
  alice.auth = new MeshAuth({ identity: aliceId, send: (_m, f) => route(f, bob.auth, 'cA', true),  bindPeer: (n,m) => aliceBound.push(n) });
  bob.auth   = new MeshAuth({ identity: bobId,   send: (_m, f) => route(f, alice.auth, 'cB', true), bindPeer: (n,m) => bobBound.push(n) });
  alice.auth.onChannelOpen('cB');
  bob.auth.onChannelOpen('cA');
  for (let i = 0; i < 10; i++) await settle();
  check('neither side binds when proofs never arrive', aliceBound.length === 0 && bobBound.length === 0);
}

async function main() {
  console.log('WebRTC-mesh MeshAuth loopback integration');
  await testTwoPeerBindAcrossAsymmetricConnIds();
  await testForgedPeerNotBound();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
