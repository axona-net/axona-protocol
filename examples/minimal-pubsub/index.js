// =====================================================================
// minimal-pubsub — two Axona peers in one Node process, pub/sub roundtrip.
//
// What this demonstrates:
//   * deriveIdentity → 264-bit Ed25519 identity in a chosen S2 cell
//   * Two SimTransports on a shared SimNetwork (kernel's in-process router)
//   * Composing AxonaPeer + AxonManager from the kernel primitives
//   * Region-keyed topics via a synthetic publisher
//   * peer.pub / peer.sub roundtrip across two distinct peers
//
// Run:  node index.js
//
// For real-world browser/Node wiring (WebRTC + bridge fallback, etc.),
// see https://github.com/axona-net/axona-peer/blob/main/src/axona_node.js
// — this example is intentionally simpler to keep the moving parts visible.
// =====================================================================

import {
  AxonaPeer, AxonaDomain, NeuronNode, AxonManager, Synapse,
  SimNetwork, simTransport,
  deriveIdentity,
  geoCellId, clz264,
} from '@axona/protocol';

// ── 1. Region helpers ────────────────────────────────────────────────
// us-east (Virginia).  Both peers are in this cell.
const US_EAST = { lat: 38.0, lng: -77.0 };

function regionSynthPublisher({ lat, lng }) {
  const s2 = geoCellId(lat, lng, 8);
  // 2 hex chars (S2 prefix) + 64 zero hex chars = 66 char synthetic id.
  return s2.toString(16).padStart(2, '0') + '0'.repeat(64);
}

// ── 2. Build a peer ──────────────────────────────────────────────────
// One function, called twice — once for alice, once for bob — wires
// identity + transport + node + AxonaPeer + AxonManager together.

async function makePeer({ network, region }) {
  // 2a. Derive a 264-bit Ed25519 identity in this region's S2 cell.
  const identity = await deriveIdentity(region);

  // 2b. Open a SimTransport on the shared SimNetwork.
  const transport = simTransport({ network, identity, heartbeatMs: 0 });
  await transport.start(identity.id);

  // 2c. Build the local DHT node.  NeuronNode holds the synaptome and
  //     routing state; AxonaDomain holds parameters shared across peers.
  //     NeuronNode XORs ids as BigInts internally, so convert from
  //     identity.id (hex string) to BigInt at construction time.
  const node     = new NeuronNode({
    id:  BigInt('0x' + identity.id),
    lat: region.lat, lng: region.lng,
  });
  node.transport = transport;
  const domain   = new AxonaDomain({ k: 20 });

  // 2d. AxonaPeer is the per-node DHT contract implementation.
  const peer = new AxonaPeer({ domain, node, identity, transport });
  await peer.start();

  // 2e. AxonManager handles pub/sub.  It needs a `dht` adapter that
  //     forwards K-closest, sendDirect, routeMessage, and handler
  //     registration to our AxonaPeer.  Most of these are 1-line
  //     delegations; sendDirect special-cases self-target for local
  //     dispatch.
  const dht = {
    getSelfId:       () => peer.getNodeId(),
    findKClosest:    (...args) => peer.findKClosest(...args),
    routeMessage:    (...args) => peer.routeMessage(...args),
    sendDirect:      async (peerId, type, payload) => {
      if (peerId === peer.getNodeId()) {
        // Local-loopback: dispatch into our own direct handler table.
        const h = peer._directHandlers?.get(type);
        if (!h) return false;
        try { await h(payload, { fromId: peer.getNodeId(), type }); return true; }
        catch (err) { console.error('self-sendDirect threw:', err); return false; }
      }
      return peer.sendDirect(peerId, type, payload);
    },
    onRoutedMessage: (type, h) => peer.onRoutedMessage(type, h),
    onDirectMessage: (type, h) => peer.onDirectMessage(type, h),
  };
  const axonManager = new AxonManager({ dht });
  peer._axonManager = axonManager;       // hand the AM directly to the peer
  return { peer, identity };
}

// ── 3. Wire two peers + connect them ─────────────────────────────────

const network = new SimNetwork();

const { peer: alice, identity: aliceId } = await makePeer({ network, region: US_EAST });
const { peer: bob,   identity: bobId   } = await makePeer({ network, region: US_EAST });

// Open a SimNetwork channel between alice and bob so they're directly
// reachable, then admit each other to their synaptomes.  Real transports
// (WebRTC mesh, WebSocket bridge) do this admission via the axona:hello
// / hello-ack handshake at channel-open time — see axona-peer's
// axona_node.js for the production wiring.
await alice._transport.openConnection(bobId.id);

function admitSynapse(localPeer, remoteBigInt) {
  const localId = localPeer._node.id;
  const stratum = clz264(localId ^ remoteBigInt);
  const syn = new Synapse({ peerId: remoteBigInt, latencyMs: 1, stratum });
  syn.weight   = 0.5;
  syn.inertia  = 0;
  syn._addedBy = 'demo';
  localPeer._node.synaptome.set(remoteBigInt, syn);
}
admitSynapse(alice, BigInt('0x' + bobId.id));
admitSynapse(bob,   BigInt('0x' + aliceId.id));

console.log('[alice] nodeId:', aliceId.id);
console.log('[bob]   nodeId:', bobId.id);

// Give the kernel a tick to admit each other to their synaptomes.
await new Promise(r => setTimeout(r, 50));

// ── 4. Pub/sub roundtrip ─────────────────────────────────────────────

const TOPIC     = 'us-east/hello-world';
const publisher = regionSynthPublisher(US_EAST);

const received = [];
const sub = await bob.sub(TOPIC, (envelope) => {
  received.push(envelope);
  console.log('[bob]   received:', {
    msgId:        envelope.msgId,
    message:      envelope.message,
    signerPubkey: envelope.signerPubkey?.slice(0, 16) + '…',
  });
}, { publisher, since: 'all' });

console.log('[bob]   subscribed:', sub.topicId);

// Wait for the subscribe-k frame to reach alice's role.
await new Promise(r => setTimeout(r, 100));

const msgId = await alice.pub(TOPIC, 'hello from alice', { publisher });
console.log('[alice] published msgId=' + msgId);

// Let the publish-k → cache → fan-out cycle complete.
await new Promise(r => setTimeout(r, 200));

console.log();
console.log(received.length === 1
  ? '✓ roundtrip ok — bob received alice\'s envelope'
  : `✗ roundtrip failed — expected 1 envelope, got ${received.length}`);

await sub.stop();
process.exit(received.length === 1 ? 0 : 1);
