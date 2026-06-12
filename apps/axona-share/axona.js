// axona.js — Axona connection for Axona-share. One peer; each channel is a topic
// string. The peer's OWN identity AND the topic anchor both come from the
// resolved region (?region=, default useast), so a regional deployment is a
// self-contained keyspace — local nodes root local channels and the bridge is
// only the rendezvous. (Was: both hardcoded us-east, which pinned every peer +
// topic to one region and locked out anyone elsewhere.)
import { AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, KERNEL_VERSION } from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';
import { resolveAnchor } from '../lib/region.js';

export { KERNEL_VERSION };          // surfaced in the app header (kernel-version visibility)

const BRIDGE_URL = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net' : 'wss://bridge.axona.net');
const ANCHOR    = resolveAnchor();                       // { token, name, center:{lat,lng}, publisher }
const PUBLISHER = ANCHOR.publisher;

export const REGION = { token: ANCHOR.token, name: ANCHOR.name, code: ANCHOR.code };   // for the UI

export async function connectAxona(onStatus = () => {}) {
  onStatus(`connecting ${BRIDGE_URL} · region ${ANCHOR.name}…`);
  const identity  = await deriveIdentity({ lat: ANCHOR.center.lat, lng: ANCHOR.center.lng });
  const transport = webTransport({ bridgeUrl: BRIDGE_URL, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: ANCHOR.center.lat, lng: ANCHOR.center.lng });
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
