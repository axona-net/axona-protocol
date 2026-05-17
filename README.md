# @axona/protocol

Pure-JS protocol kernel for the [Axona](https://axona.net) peer-to-peer mesh. One node, one package — drop it into a browser peer or a Node-side bridge, give it a Transport, and it speaks the Axona wire protocol.

## What's inside

```
@axona/protocol/
├── contracts/          # the three contract surfaces every implementation MUST conform to
│   ├── DHT.js          # application-facing per-node contract (start/stop/join/leave/
│   │                   # lookup/publish/subscribe/getMetrics/onEvent)
│   ├── Transport.js    # network-facing contract (open/send/notify/onPeerDied/getLatency/…)
│   ├── BootstrapService.js  # cold-start sponsor flow
│   ├── types.js        # shared JSDoc typedefs (LookupResult, Metrics, …)
│   └── index.js        # barrel export
│
├── dht/
│   ├── AxonaPeer.js    # per-node DHT contract impl. NH-1 routing + axonal pub/sub.
│   │                   # One instance per running peer.
│   ├── DHTNode.js      # base node state (id, lat/lng, connections, lifecycle)
│   ├── NeuronNode.js   # AxonaPeer's per-node state (synaptome, incomingSynapses, temperature)
│   └── Synapse.js      # routing-table entry (peerId, weight, latency, stratum, …)
│
├── pubsub/
│   ├── AxonManager.js  # axonal-tree pub/sub membership protocol
│   ├── AxonPubSub.js   # feed-style application API: publish/subscribe/pull/reshare/metrics
│   └── post.js         # SignedPost construction + topic-id derivation + verification
│
└── utils/
    ├── geo.js          # haversine, propagation delay, XOR helpers, continent detection
    └── s2.js           # geographic-cell encoding (S2 Hilbert prefix)
```

## What's NOT inside

- Anything that touches the simulator (`SimulatedNetwork`, `SimulatedTransport`, `Engine`, benchmark harness). Those live in [`axona-net/dht-sim`](https://github.com/axona-net/dht-sim).
- A WebRTC transport implementation. That lives in [`axona-net/axona-peer`](https://github.com/axona-net/axona-peer). It implements the `Transport` contract from here.
- A signaling broker. That's [`axona-net/axona-bridge`](https://github.com/axona-net/axona-bridge).
- The benchmark / simulator UI.

## Quickstart

```js
import { AxonaPeer } from '@axona/protocol';

const peer = new AxonaPeer({
  engine,        // sim only; production peer creates one in standalone mode
  node,          // a NeuronNode (in production this is created during peer init)
});

await peer.start();
await peer.join({ kind: 'rendezvous', url: 'wss://bridge.axona.net', manifestSig });

const result = await peer.lookup(targetKey);  // walks the routing layer
const sub = await peer.subscribe('@US-east/social/me', payload => console.log(payload));
await peer.publish('@me/social/post', { text: 'hello mesh' });
```

The full per-peer surface matches `src/contracts/DHT.js`.

## Status

**v1.0.0-beta.0** — initial extraction from `dht-sim`. The contract surfaces are stable; the per-peer NH-1 implementation runs in the simulator and is the basis for the browser-peer integration (`axona-peer` Phase 3).

## Wire protocol

See [`Axona-Wire-Protocol-v0.71.md`](https://github.com/axona-net/dht-sim/blob/main/documents/implementation/Axona-Wire-Protocol-v0.71.md) in the simulator repo for the message-type vocabulary, frame shapes, version handshake, and bridge wire format.

## License

MIT.
