// =====================================================================
// smoke_pubsub_republish.mjs — re-publishing identical content (same author +
// same message => same msgId) UPSERTS: it replaces the older cache entry
// (one entry per msgId, with a fresh hold) and delivers exactly once, rather
// than adding a duplicate + double-delivering.
//
// Guards the v3.3.0 change to _onPublish / _onPublishDirect. Before it, the
// live publish path deduped only on the random per-publish publishId, so a
// re-publish double-stored (current_count → 2) and double-delivered.
//
// Run: node test/smoke_pubsub_republish.mjs
// =====================================================================

import { AxonaPeer, AxonaDomain, NeuronNode, AxonaManager, Synapse,
         SimNetwork, simTransport, createNodeIdentity, createAuthorIdentity, clz264 }
  from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, ok) => { console.log(`  ${ok ? '✓' : '✗'} ${label}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));
const REGION = { lat: 38, lng: -77 };

async function makePeer(network) {
  const id = await createNodeIdentity(REGION);
  const transport = simTransport({ network, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: BigInt('0x' + id.id), lat: REGION.lat, lng: REGION.lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: id, transport });
  await peer.start();
  const dht = {
    getSelfId:       () => peer.getNodeId(),
    findKClosest:    (...a) => peer.findKClosest(...a),
    routeMessage:    (...a) => peer.routeMessage(...a),
    sendDirect:      async (pid, t, p) => {
      if (pid === peer.getNodeId()) {
        const h = peer._directHandlers?.get(t); if (!h) return false;
        await h(p, { fromId: peer.getNodeId(), type: t }); return true;
      }
      return peer.sendDirect(pid, t, p);
    },
    onRoutedMessage: (t, h) => peer.onRoutedMessage(t, h),
    onDirectMessage: (t, h) => peer.onDirectMessage(t, h),
  };
  peer._axonaManager = new AxonaManager({ dht });
  return { peer, id };
}

function admit(localPeer, remoteHex) {
  const rb = BigInt('0x' + remoteHex);
  const syn = new Synapse({ peerId: rb, latencyMs: 1, stratum: clz264(localPeer._node.id ^ rb) });
  syn.weight = 0.5; syn.inertia = 0; syn._addedBy = 'test';
  localPeer._node.synaptome.set(rb, syn);
}

async function main() {
  console.log('Axona re-publish upsert smoke');
  const net = new SimNetwork();
  const A = await makePeer(net), B = await makePeer(net);
  const author = await createAuthorIdentity();
  await A.peer._transport.openConnection(B.id.id);
  admit(A.peer, B.id.id); admit(B.peer, A.id.id);
  await wait(50);

  const topic = { region: 'useast', name: 'republish-smoke' };
  const got = [];
  await B.peer.sub(topic, (e) => { if (e && 'message' in e) got.push(e.msgId); }, { since: 'all' });
  await wait(80);

  const id1 = await A.peer.pub(topic, 'same message', { signWith: author });
  await wait(120);
  const id2 = await A.peer.pub(topic, 'same message', { signWith: author });   // identical content
  await wait(200);

  check('re-publish yields the same msgId', id1 === id2);
  check('delivered exactly once (no double-delivery)', got.length === 1);
  check('delivered msgId matches', got[0] === id1);

  const m = await A.peer.metrics(topic);
  check('replay cache holds ONE entry (older replaced, not duplicated)', m.current_count === 1);

  // Sanity: a genuinely different message is still a distinct, delivered entry.
  const id3 = await A.peer.pub(topic, 'a different message', { signWith: author });
  await wait(200);
  check('distinct content → distinct msgId', id3 !== id1);
  check('distinct content delivered', got.includes(id3));
  const m2 = await A.peer.metrics(topic);
  check('cache now holds two distinct messages', m2.current_count === 2);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(e => { console.error('smoke threw:', e); process.exit(2); });
