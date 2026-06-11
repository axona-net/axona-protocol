// axona.js — Axona connection for Axona-share. One peer; each channel is a topic
// string, all anchored at a fixed us-east synthetic publisher so every user
// converges on the same roots regardless of their own location (mirrors the
// proven bench reporter / minimal-pubsub-browser connect).
import { AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, geoCellId } from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';

const BRIDGE_URL = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net' : 'wss://bridge.axona.net');
const ANCHOR = { lat: 38.0, lng: -78.0 };                              // us-east
const PUBLISHER = geoCellId(ANCHOR.lat, ANCHOR.lng, 8).toString(16).padStart(2, '0') + '0'.repeat(64);

export async function connectAxona(onStatus = () => {}) {
  onStatus(`connecting ${BRIDGE_URL}…`);
  const identity  = await deriveIdentity({ lat: ANCHOR.lat, lng: ANCHOR.lng });
  const transport = webTransport({ bridgeUrl: BRIDGE_URL, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: ANCHOR.lat, lng: ANCHOR.lng });
  node.transport  = transport;
  const domain    = new AxonaDomain({ k: 20 });
  const peer      = new AxonaPeer({ domain, node, identity, transport });

  await transport.start(identity.id);
  await peer.start();
  const readyBy = Date.now() + 30000;
  while (Date.now() < readyBy && (node.synaptome?.size ?? 0) < 3) {
    onStatus(`forming mesh… (${node.synaptome?.size ?? 0})`);
    await new Promise((r) => setTimeout(r, 600));
  }
  await new Promise((r) => setTimeout(r, 1500));                       // settle so roots are reachable
  onStatus('connected');

  return {
    nodeId: identity.id,
    // Subscribe to a channel topic; cb gets each parsed message object (chunk).
    async sub(topic, cb) {
      return peer.sub(topic, (env) => {
        if (!env || env.deleted || !env.message) return;
        let m; try { m = JSON.parse(env.message); } catch { return; }
        cb(m);
      }, { publisher: PUBLISHER, since: 'all' });
    },
    async unsub(topic) { try { return await peer.unsub?.(topic); } catch { /* */ } },
    // Publish one message object (a chunk) to a channel topic.
    async pub(topic, obj) { return peer.pub(topic, JSON.stringify(obj), { publisher: PUBLISHER }); },
    async close() { try { await peer.leave?.(); } catch { /* */ } try { await transport.stop?.(); } catch { /* */ } },
  };
}
