// axona-report.js — publish bench results to the LIVE Axona network, so a local
// node collects results from testers anywhere via pub/sub (no HTTP collector,
// works across the internet). Lazy-loaded by bench.js only when reporting is on.
//
// Mirrors the demo's browser connect (examples/minimal-pubsub-browser/). Results
// land on a fixed topic anchored at the us-east synthetic publisher, so a local
// node collects everything with:
//   node ../../axona-relay/src/cli.js sub "pow-bench/results" --region useast --for 3600
import {
  AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, geoCellId,
} from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';

const BRIDGE_URL = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net'
                                            : 'wss://bridge.axona.net');

// Collection topic + us-east (0x89) synthetic publisher — MUST match the
// collector's `--region useast`. We anchor HERE regardless of the tester's own
// location, so all results converge on one topic.
const TOPIC       = 'pow-bench/results';
const LEADERBOARD = 'pow-bench/leaderboard';               // collector publishes the comparison here
const ANCHOR      = { lat: 38.0, lng: -78.0 };             // us-east
const PUBLISHER   = geoCellId(ANCHOR.lat, ANCHOR.lng, 8).toString(16).padStart(2, '0') + '0'.repeat(64);

export const reportTopic  = TOPIC;
export const reportBridge = BRIDGE_URL;

/**
 * Persistent reporter — connect ONCE, publish MANY. Use in continuous mode so
 * the (heavy) WebRTC connect isn't repeated every iteration.
 * Returns { nodeId, publish(result)→{msgId}, close() }.
 */
export async function createReporter(onStatus = () => {}, onLeaderboard = null) {
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
    onStatus(`forming mesh… synaptome ${node.synaptome?.size ?? 0}`);
    await new Promise((r) => setTimeout(r, 600));
  }
  await new Promise((r) => setTimeout(r, 1500));            // settle so roots are reachable
  onStatus('Axona connected');

  // Subscribe to the collector's comparison report (replays the latest on
  // connect, then updates live). Lets each device see where it stands.
  if (onLeaderboard) {
    try {
      await peer.sub(LEADERBOARD, (env) => {
        if (!env || !env.message) return;
        try { onLeaderboard(JSON.parse(env.message)); } catch { /* */ }
      }, { publisher: PUBLISHER, since: 'all' });
    } catch { /* leaderboard is best-effort */ }
  }

  return {
    nodeId: identity.id,
    async publish(result) {
      const msgId = await peer.pub(TOPIC, JSON.stringify(result), { publisher: PUBLISHER });
      return { ok: true, msgId };
    },
    async close() {
      try { await peer.leave?.(); } catch { /* */ }
      try { await transport.stop?.(); } catch { /* */ }
    },
  };
}

/** One-shot: connect, publish, grab the latest comparison, disconnect. */
export async function reportToAxona(result, onStatus = () => {}, onLeaderboard = null) {
  const r = await createReporter(onStatus, onLeaderboard);
  try {
    onStatus('publishing result to Axona…');
    const { msgId } = await r.publish(result);
    // Brief wait: lets the publish propagate AND a replayed leaderboard arrive.
    await new Promise((res) => setTimeout(res, onLeaderboard ? 3500 : 2500));
    onStatus(`published to Axona ✓ (msgId ${String(msgId).slice(0, 12)}…)`);
    return { ok: true, msgId };
  } finally {
    await r.close();
  }
}
