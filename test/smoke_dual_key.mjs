// =====================================================================
// smoke_dual_key.mjs — publish identity decoupled from transport identity.
//
// Verifies the dual-key model (kernel v2.50.0): a peer may sign publishes with
// a PUBLISH identity distinct from its (ephemeral) transport identity, and may
// run MANY publish identities through one peer via a per-call { signWith }.
//
//   1. default publishIdentity → envelope.signerPubkey = publish key (NOT transport)
//   2. per-call signWith overrides the default (multiple publish keys, one peer)
//   3. no publishIdentity + no signWith → signs with transport identity (back-compat)
//   4. dual-key envelope verifies (signature ok + msgId recomputes)
//   5. unlinkability: the envelope carries the publish key only — never the transport key
//   6. durable authorship across a transport-id rotation: a fresh transport identity,
//      the SAME publish identity → same signerPubkey (recognizable author)
//   7. a bad signWith (no private key) → PublishError, never an unsigned publish
//
//   node test/smoke_dual_key.mjs
// =====================================================================
import { AxonaPeer }       from '../src/dht/AxonaPeer.js';
import { deriveIdentity }  from '../src/identity/index.js';
import { verifyEnvelope, computeMsgId } from '../src/pubsub/envelope.js';
import { PublishError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

// Minimal AxonaManager mock: captures the serialized envelope per publish.
function mockManager(nodeId) {
  let n = 0;
  return {
    nodeId, published: [],
    pubsubPublish(topicId, json, meta) { const id = `${nodeId}:${++n}`; this.published.push({ topicId, json, meta, publishId: id }); return id; },
    pubsubSubscribe() {}, pubsubUnsubscribe() {}, onPubsubDelivery() {},
  };
}
function makePeer({ identity, publishIdentity = null }) {
  const node   = { id: identity.id, alive: true };
  const engine = { onEvent: () => () => {}, simEpoch: 0 };
  const am     = mockManager(identity.id);
  const peer   = new AxonaPeer({ engine, node, axonaManager: am, identity, publishIdentity });
  return { peer, am };
}
const lastEnv = (am) => JSON.parse(am.published[am.published.length - 1].json);

const SF = { lat: 37.77, lng: -122.42 };

async function main() {
  console.log('dual-key identity (publish vs transport)');

  const transport = await deriveIdentity(SF);
  const pubA      = await deriveIdentity(SF);     // a persistent publish identity
  const pubB      = await deriveIdentity(SF);     // a second publish identity, same peer

  // ── 1. default publishIdentity signs (not the transport key) ──
  {
    const { peer, am } = makePeer({ identity: transport, publishIdentity: pubA });
    await peer.pub('topic/x', { hello: 1 });
    const env = lastEnv(am);
    check('1. signerPubkey = publish key (default publishIdentity)', env.signerPubkey === pubA.pubkeyHex);
    check('1. signerPubkey ≠ transport key', env.signerPubkey !== transport.pubkeyHex);
  }

  // ── 2. per-call signWith overrides → multiple publish keys from one peer ──
  {
    const { peer, am } = makePeer({ identity: transport, publishIdentity: pubA });
    await peer.pub('topic/x', { a: 1 });                       // → pubA
    await peer.pub('topic/x', { b: 2 }, { signWith: pubB });   // → pubB
    const e1 = JSON.parse(am.published[0].json), e2 = JSON.parse(am.published[1].json);
    check('2. first publish signed by pubA', e1.signerPubkey === pubA.pubkeyHex);
    check('2. signWith publish signed by pubB', e2.signerPubkey === pubB.pubkeyHex);
    check('2. two distinct publish keys from one peer', e1.signerPubkey !== e2.signerPubkey);
  }

  // ── 3. no publishIdentity + no signWith → REFUSED (transport key must not sign) ──
  {
    const { peer } = makePeer({ identity: transport });           // transport only, no publish identity
    let err = null;
    try { await peer.pub('topic/x', { c: 3 }); } catch (e) { err = e; }
    check('3. signed publish without a publish identity → PublishError(PUBLISH_NO_PUBLISH_IDENTITY)',
      err instanceof PublishError && err.code === ErrorCodes.PUBLISH_NO_PUBLISH_IDENTITY);
  }

  // ── 3b. transport-key signing is possible ONLY when explicitly requested ──
  {
    const { peer, am } = makePeer({ identity: transport });
    await peer.pub('topic/x', { c: 3 }, { signWith: transport });   // intentional, discouraged escape hatch
    check('3b. explicit signWith: transport signs with the transport key (intentional override)',
      lastEnv(am).signerPubkey === transport.pubkeyHex);
  }

  // ── 3c. anonymous publish needs no publish identity ──
  {
    const { peer, am } = makePeer({ identity: transport });
    await peer.pub('topic/x', { c: 3 }, { sign: false });
    check('3c. sign:false → unsigned publish (no signerPubkey), no publish identity needed',
      lastEnv(am).signerPubkey == null);
  }

  // ── 4. the dual-key envelope verifies (signature + msgId) ──
  {
    const { peer, am } = makePeer({ identity: transport, publishIdentity: pubA });
    await peer.pub('topic/x', { d: 4 });
    const env = lastEnv(am);
    const res = await verifyEnvelope(env);
    check('4. verifyEnvelope ok for dual-key envelope', res?.ok === true);
    const recomputed = await computeMsgId({ publisher: env.signerPubkey, message: env.message });
    check('4. msgId recomputes from (signerPubkey, message)', recomputed === env.msgId);
  }

  // ── 5. unlinkability: the envelope contains the publish key only, never the transport key ──
  {
    const { peer, am } = makePeer({ identity: transport, publishIdentity: pubA });
    await peer.pub('topic/x', { e: 5 });
    const json = am.published[0].json;
    check('5. envelope carries the publish pubkey', json.includes(pubA.pubkeyHex));
    check('5. envelope contains NO transport-key material', !json.includes(transport.pubkeyHex) && !json.includes(transport.id));
  }

  // ── 6. durable authorship across a transport-id rotation ──
  {
    const newTransport = await deriveIdentity(SF);            // simulate a restart: fresh transport id
    check('6. transport id rotated', newTransport.id !== transport.id);
    const { peer, am } = makePeer({ identity: newTransport, publishIdentity: pubA });
    await peer.pub('topic/x', { f: 6 });
    const env = lastEnv(am);
    check('6. same publish identity ⇒ same signerPubkey after rotation', env.signerPubkey === pubA.pubkeyHex);
  }

  // ── 7. a bad signWith never silently downgrades to unsigned ──
  {
    const { peer } = makePeer({ identity: transport, publishIdentity: pubA });
    let err = null;
    try { await peer.pub('topic/x', { g: 7 }, { signWith: { pubkeyHex: 'deadbeef' } }); }  // no privateKey
    catch (e) { err = e; }
    check('7. invalid signWith → PublishError(PUBLISH_SIGN_FAILED)',
      err instanceof PublishError && err.code === ErrorCodes.PUBLISH_SIGN_FAILED);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
