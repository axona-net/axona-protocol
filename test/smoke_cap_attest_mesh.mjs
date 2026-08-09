// smoke_cap_attest_mesh.mjs — the CAP_ATTEST exchange wired into MeshAuth,
// driven over the in-memory data-channel loopback (no browser). Proves the live
// oracle on the primary p2p path: after base auth binds, each side attests its
// write-flight-ack capability, the peer verifies it against the AUTHENTICATED
// pubkey + its OWN channel cbv (R15/R17), and MeshAuth.isCapable(meshId) flips
// true + fires onCapable(peerNodeId, meshId). Fail-closed: a peer that never
// attests is never marked capable; channel loss clears the flag.
//
// Run: node test/smoke_cap_attest_mesh.mjs
import { MeshAuth } from '../src/transport/web/mesh-auth.js';
import { createNodeIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };
const settle = () => new Promise(r => setTimeout(r, 10));

// Route hello / hello-sig / cap-attest between two MeshAuth instances. `dropCap`
// lets a test simulate a peer that never attests (the incapable-peer case).
function wire2(alice, bob, connA, connB, { dropCapFromAlice = false, dropCapFromBob = false } = {}) {
  const deliver = (frame, target, senderConnId, dropCap) => {
    queueMicrotask(() => {
      if (frame.type === 'hello')          target.onHello(senderConnId, frame.body);
      else if (frame.type === 'hello-sig') target.onHelloSig(senderConnId, frame.body);
      else if (frame.type === 'cap-attest') { if (!dropCap) target.onCapAttest(senderConnId, frame.body); }
    });
  };
  return {
    aliceSend: (_m, frame) => deliver(frame, bob.auth,   connA, dropCapFromAlice), // bob hears "from A"
    bobSend:   (_m, frame) => deliver(frame, alice.auth, connB, dropCapFromBob),   // alice hears "from B"
  };
}

async function run(label, { dropCapFromAlice = false, dropCapFromBob = false } = {}) {
  const aliceId = await createNodeIdentity({ lat: 40.7, lng: -74.0 });
  const bobId   = await createNodeIdentity({ lat: 51.5, lng: -0.1 });
  const connA = 'cA1', connB = 'cB2';
  const aliceCap = [], bobCap = [];
  const alice = {}, bob = {};
  const { aliceSend, bobSend } = wire2(alice, bob, connA, connB, { dropCapFromAlice, dropCapFromBob });

  alice.auth = new MeshAuth({ identity: aliceId, send: aliceSend, bindPeer: () => {},
    onCapable: (nodeIdHex, meshId) => aliceCap.push({ nodeIdHex, meshId }) });
  bob.auth = new MeshAuth({ identity: bobId, send: bobSend, bindPeer: () => {},
    onCapable: (nodeIdHex, meshId) => bobCap.push({ nodeIdHex, meshId }) });

  alice.auth.onChannelOpen(connB);
  bob.auth.onChannelOpen(connA);
  for (let i = 0; i < 20; i++) await settle();

  return { aliceId, bobId, alice, bob, connA, connB, aliceCap, bobCap };
}

// ── honest exchange → both sides capable ──────────────────────────────
{
  console.log('\n── honest CAP_ATTEST exchange ──');
  const r = await run('honest');
  check('alice bound the channel (precondition)', r.alice.auth.isBound(r.connB));
  check('bob bound the channel (precondition)', r.bob.auth.isBound(r.connA));
  check('alice sees bob as capable', r.alice.auth.isCapable(r.connB) === true);
  check('bob sees alice as capable', r.bob.auth.isCapable(r.connA) === true);
  check('alice onCapable fired with bob nodeId', r.aliceCap.length === 1 && r.aliceCap[0].nodeIdHex === r.bobId.id);
  check('bob onCapable fired with alice nodeId', r.bobCap.length === 1 && r.bobCap[0].nodeIdHex === r.aliceId.id);
  // channel loss clears capability (R17: per-channel, never persisted)
  r.alice.auth.onChannelLost(r.connB);
  check('alice capability cleared on channel loss', r.alice.auth.isCapable(r.connB) === false);
}

// ── a peer that never attests → the other stays incapable (fail-closed) ─
{
  console.log('\n── bob never attests → alice sees bob incapable ──');
  const r = await run('bob-silent', { dropCapFromBob: true });
  check('both still bound (auth unaffected by missing cap)', r.alice.auth.isBound(r.connB) && r.bob.auth.isBound(r.connA));
  check('alice does NOT see bob capable (fail-closed)', r.alice.auth.isCapable(r.connB) === false);
  check('alice onCapable never fired for bob', r.aliceCap.length === 0);
  // alice still attested, so bob DOES see alice capable — asymmetry is fine
  check('bob still sees alice capable (alice attested)', r.bob.auth.isCapable(r.connA) === true);
}

console.log(`\n${failed === 0 ? 'PASS' : 'FAIL'} — ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
