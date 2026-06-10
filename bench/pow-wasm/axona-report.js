// axona-report.js — publish a bench result to the LIVE Axona network, so a local
// node can collect results from testers anywhere via pub/sub (no HTTP collector,
// works across the internet). Lazy-loaded by bench.js only when reporting is on.
//
// Mirrors the demo's browser connect (examples/minimal-pubsub-browser/). The
// result lands on a fixed topic anchored at the us-east synthetic publisher, so a
// local node collects everything with:
//   node ../../axona-relay/src/cli.js sub "pow-bench/results" --region useast --for 3600
import {
  AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, geoCellId,
} from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';

const BRIDGE_URL = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net'
                                            : 'wss://bridge.axona.net');

// Collection topic + us-east (0x89) synthetic publisher — MUST match the
// collector's `--region useast`. The reporter anchors HERE regardless of the
// tester's own location, so all results converge on one topic.
const TOPIC   = 'pow-bench/results';
const ANCHOR  = { lat: 38.0, lng: -78.0 };                 // us-east
const PUBLISHER = geoCellId(ANCHOR.lat, ANCHOR.lng, 8).toString(16).padStart(2, '0') + '0'.repeat(64);

export const reportTopic = TOPIC;
export const reportBridge = BRIDGE_URL;

export async function reportToAxona(result, onStatus = () => {}) {
  onStatus(`connecting ${BRIDGE_URL}…`);
  const identity  = await deriveIdentity({ lat: ANCHOR.lat, lng: ANCHOR.lng });
  const transport = webTransport({ bridgeUrl: BRIDGE_URL, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: ANCHOR.lat, lng: ANCHOR.lng });
  node.transport  = transport;
  const domain    = new AxonaDomain({ k: 20 });
  const peer      = new AxonaPeer({ domain, node, identity, transport });
  try {
    await transport.start(identity.id);
    await peer.start();
    // Wait for a usable mesh (bridge + a couple of peers) or time out.
    const readyBy = Date.now() + 30000;
    while (Date.now() < readyBy && (node.synaptome?.size ?? 0) < 3) {
      onStatus(`forming mesh… synaptome ${node.synaptome?.size ?? 0}`);
      await new Promise((r) => setTimeout(r, 600));
    }
    await new Promise((r) => setTimeout(r, 1500));          // settle so roots are reachable
    onStatus('publishing result to Axona…');
    const msgId = await peer.pub(TOPIC, JSON.stringify(result), { publisher: PUBLISHER });
    await new Promise((r) => setTimeout(r, 2500));          // let it propagate to roots
    onStatus(`published to Axona ✓ (msgId ${String(msgId).slice(0, 12)}…)`);
    return { ok: true, msgId };
  } finally {
    try { await peer.leave?.(); } catch { /* */ }
    try { await transport.stop?.(); } catch { /* */ }
  }
}
