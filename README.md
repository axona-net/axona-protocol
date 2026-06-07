# @axona/protocol

Pure-JS protocol kernel for the [Axona](https://axona.net) peer-to-peer mesh.
One node, one package — give it a Transport and it speaks the Axona wire
protocol. Runs unchanged in browsers, Node servers, and the dht-sim
simulator.

**v2.31.0.** Powers [axona.net](https://axona.net) (browser peers),
[bridge.axona.net](https://bridge.axona.net) (signaling broker), the
[`dht-sim`](https://github.com/axona-net/dht-sim) simulator at 50,000 peers, and
the [`axona-relay`](https://github.com/axona-net/axona-relay) headless supernode.
Every link is mutually authenticated — peers prove their routing identity with an
Ed25519 signature before they can join (see
[Authenticated handshake](#authenticated-handshake)).

> **Network epoch.** This is the `axona/5` line (`WIRE_VERSION 2.0`). The
> protocol epoch is folded into the connect-time signed transcript, so an
> `axona/5` node and a node on the older `axona/4` epoch **cannot** complete a
> handshake — the two networks are cryptographically partitioned by design. The
> SF testnet (`testnet.axona.net`) runs this line; production `axona.net`/
> `bridge.axona.net` still run `axona/4` (kernel 2.16.0) until the flag-day
> cutover.

## Install

```bash
npm install @axona/protocol
```

Pure JS, no native dependencies, ESM only. Node ≥ 20.

## Quickstart

A pub/sub roundtrip in one process using the in-memory `SimNetwork`:

```js
import {
  AxonaPeer, AxonaDomain, NeuronNode,
  AxonaManager, simTransport, SimNetwork,
  deriveIdentity,
} from '@axona/protocol';

const network = new SimNetwork();
const domain  = new AxonaDomain({ k: 20 });

async function makePeer(region) {
  const identity = await deriveIdentity(region);     // 264-bit Ed25519
  const transport = simTransport({ network, identity });
  await transport.start(identity.id);

  const node  = new NeuronNode({
    id:  BigInt('0x' + identity.id),
    lat: region.lat, lng: region.lng,
  });
  node.transport = transport;

  const peer = new AxonaPeer({ domain, node, identity, transport });
  await peer.start();
  return peer;
}

const alice = await makePeer({ lat: 38, lng: -77 });   // US-east
const bob   = await makePeer({ lat: 38, lng: -77 });

await bob.sub('hello-world', env => console.log('got:', env.message));
await alice.pub('hello-world', 'hi from alice');
```

For a runnable version with synaptome admission and the full
SimNetwork wiring, see [`examples/minimal-pubsub/`](examples/minimal-pubsub/).
`npm install && node index.js` — full pub/sub roundtrip in ~150 lines.

## The AxonaPeer surface

The `AxonaPeer` class is the per-node application API, organised into the following
clusters:

| Cluster | Methods |
|---|---|
| **Lifecycle** | `start()` · `stop()` · `join(opts?)` · `leave({drain, notify, timeoutMs}?)` · `getNodeId()` |
| **Pub/sub** | `pub(topic, message, opts?)` · `sub(topic, handler, opts?)` · `unsub(topic)` · `pull(msgId\|topic, opts?)` · `metrics(topic, opts?)` |
| **Pub/sub lifecycle** | `kill(topic, msgId)` (tombstone a message) · `unpub(topic, {destroy}?)` (publisher-only) — bounded queues, per-publisher quota, hold-time TTL |
| **Direct messaging** | `send(targetId, message)` · `notify(targetId, message)` · `onMessage(handler)` |
| **Mesh introspection** | `peers()` · `onPeerJoin(handler)` · `onPeerLeave(handler)` · `lookup(targetKey)` |
| **Telemetry** | `health()` · `onLog(level, handler)` · `onError(handler)` |
| **Snapshot escape hatch** | `snapshot()` · `fromSnapshot(blob)` |

Full reference with parameter types, return shapes, and worked examples:
[axona-docs API Reference](https://github.com/axona-net/axona-docs/tree/main/programmer-guide).

## Transports

Three concrete transports implement the same 12-method `Transport` contract.
The protocol layer can't tell which one it's running on.

| Factory | Where it runs | Use case |
|---|---|---|
| [`simTransport({ network, identity })`](src/transport/sim/) | In-process | Tests, dht-sim, multi-peer-in-one-process demos |
| [`webTransport({ identity, bridgeUrl })`](src/transport/web/) | Browser | WebRTC data channels + WebSocket bridge fallback. Used by [`axona-peer`](https://github.com/axona-net/axona-peer). |
| [`serverTransport` / `clientTransport`](src/transport/node/) | Headless server | Raw WebSocket (also bundled as `nodeTransport.server` / `nodeTransport.client`). Used by [`axona-bridge`](https://github.com/axona-net/axona-bridge). |

All three speak the same authenticated wire protocol (`axona/5`); a peer using
one can communicate with a peer using either of the others. On the web
transport, once peers are on the mesh they also relay WebRTC signaling for each
other (`meshRelay`, on by default), so two peers can connect with **no bridge in
the signaling path** (bridgeless connection).

## Persistence

`PersistenceAdapter` is the abstract contract; three implementations ship:

- `InMemoryPersistence` (default; loses state on stop)
- `IndexedDBPersistence` — browsers
- `FilePersistence` — Node

The platform-specific implementations are sub-path imports so neither pulls
`node:fs` into browser bundles nor `globalThis.indexedDB` into Node:

```js
import { IndexedDBPersistence } from '@axona/protocol/persistence/indexeddb.js';
import { FilePersistence }      from '@axona/protocol/persistence/file.js';
```

Pass to `AxonaPeer`:

```js
const peer = new AxonaPeer({ domain, node, identity, transport, persistence });
```

Persistence captures the synaptome, axonal-tree memberships, and identity
across restarts.

## Identity

264-bit Ed25519 identities anchored to an S2 geographic cell:

```js
import { deriveIdentity, dumpIdentity, loadIdentity } from '@axona/protocol';

const identity = await deriveIdentity({ lat: 38, lng: -77 });
console.log(identity.id);                       // 66-char hex nodeId
console.log(identity.pubkeyHex);                // 64-char hex pubkey

const blob = await dumpIdentity(identity);      // round-trip via JSON
const same = await loadIdentity(blob);
```

`nodeId = [8-bit S2 prefix] || [256-bit SHA-256(pubkey)]`. Topic IDs share
the same 264-bit keyspace.

## Regions

The 8-bit S2 prefix indexes one of **192 valid region cells**
(`face·32 + truncated-Hilbert`; codes `[0,192)`, 192–255 reserved). Each region
carries **two human-readable names** — one for each of the cell's two S2 level-3
sub-cells ("halves") — and **both names resolve to the same code**, so a user
sees the label closest to their actual location while addressing stays stable.
Homogeneous cells (a single country, open ocean) share one name.

```js
import {
  regionNames, regionName, regionCode, resolveRegion, regionNameForLatLng,
  geoCellId, geoCellSubCenters, geoCellHalf,
} from '@axona/protocol';

regionNames(0x89);                 // ['bahamas', 'useast']  → both map to 0x89
regionName(0x89, 40, -75);         // half-aware: 'useast'
regionCode('bahamas');             // 0x89
resolveRegion('useast');           // → { code: 0x89, ... }
regionNameForLatLng(40, -75);      // 'useast'
```

Names match `/^[a-z0-9_]{1,8}$/`; open-ocean cells are `<ocean3>_<hex>`
(`pac_68`, `atl_0a`, …); small islands claim a single cell; large landmasses
that span cells take a single-letter compass suffix. The interactive
[`examples/s2-region-visualizer/`](examples/s2-region-visualizer/) renders all
192 cells with both names and the code on a 3D globe.

## Authenticated handshake

A `nodeId`'s lower 256 bits are `SHA-256(pubkey)`, but that alone proves
nothing — pubkeys are public, so anyone could re-broadcast another peer's
identity. Every fresh channel therefore runs the `axona/5` handshake, which
gates admission on three checks:

1. **Bind** — `SHA-256(pubkey)` equals the nodeId's 256-bit suffix. (The 8-bit
   S2 prefix is *not* bound — it's a routing hint, like an area code, so a
   travelling peer keeps its identity.)
2. **Possess** — an Ed25519 signature proves the peer holds the matching
   private key.
3. **Channel-bind** — the signed transcript folds in a per-connection
   *channel-binding value* (CBV), so a captured hello can't be replayed onto a
   different connection.

The primitive is transport-agnostic and exported for reuse:

```js
import {
  buildAuthHello, verifyAuthHello, AUTH_PROTO,   // 'axona/5'
  makeNonce, cbvFromNonces, pubkeyMatchesNodeId,
} from '@axona/protocol';
```

A peer speaking an older protocol is cleanly rejected: the bridge closes the
WebSocket with code **4426** and an `upgrade required` reason, and the client
logs a developer-visible `UPGRADE REQUIRED` console error telling it to update
`@axona/protocol`. `UpgradeRequiredError` (below) is the typed form.

## Errors

Typed error classes with stable codes for runtime branching:

```js
import { PublishError, UpgradeRequiredError, ErrorCodes } from '@axona/protocol';

try {
  await peer.pub(topic, message);
} catch (e) {
  if (e instanceof UpgradeRequiredError) /* peer needs newer wire version */;
  else if (e.code === ErrorCodes.NO_K_CLOSEST) /* …special-case */;
  else throw e;
}
```

Full list: `AxonaError`, `IdentityError`, `TransportError`, `PublishError`,
`SubscribeError`, `PullError`, `MetricsError`, `UpgradeRequiredError`.

## What's inside

```
@axona/protocol/
├── contracts/          # the contract surfaces every implementation must conform to
│   ├── DHT.js          # application-facing per-node contract (AxonaPeer fulfils this)
│   ├── Transport.js    # network-facing contract (12 methods)
│   ├── BootstrapService.js   # cold-start sponsor flow
│   ├── types.js        # shared JSDoc typedefs
│   └── index.js        # barrel export
│
├── dht/
│   ├── AxonaPeer.js    # per-node DHT contract impl. NH-1 routing + axonal pub/sub.
│   ├── AxonaDomain.js  # shared mesh state — k, simEpoch, per-domain config.
│   ├── DHTNode.js      # base node state (id, lat/lng, lifecycle)
│   ├── NeuronNode.js   # AxonaPeer's per-node state (synaptome, temperature, incomingSynapses)
│   ├── Synapse.js      # routing-table entry (peerId, weight, latency, stratum)
│   └── Subscription.js # handle returned by peer.sub() — .unsubscribe(), .topicId
│
├── pubsub/
│   ├── AxonaManager.js  # axonal-tree pub/sub membership protocol
│   ├── AxonPubSub.js   # feed-style application API (pub/sub/pull/metrics)
│   ├── envelope.js     # buildEnvelope / verifyEnvelope / computeMsgId
│   ├── post.js         # makePost + topic-id derivation + verification
│   └── ed25519.js      # Web Crypto Ed25519 wrapper (sign / verify)
│
├── transport/
│   ├── sim/            # in-process router + simTransport — tests, dht-sim
│   ├── web/            # WebRTC + WebSocket-bridge composite — browsers (axona-peer)
│   ├── node/           # raw WebSocket — Node servers (axona-bridge)
│   ├── handshake.js    # version negotiation + KERNEL_VERSION on every fresh channel
│   ├── handshake-auth.js  # axona/5 authenticated-identity handshake (bind + possess + channel-bind)
│   └── wire.js         # frame shape, codecs
│
├── persistence/
│   ├── interface.js    # PersistenceAdapter + InMemoryPersistence
│   ├── indexeddb.js    # browser-only (sub-path import)
│   └── file.js         # Node-only (sub-path import)
│
├── identity/
│   ├── index.js        # deriveIdentity, dumpIdentity, loadIdentity
│   └── nodeid.js       # computeNodeId, S2-prefix bundling
│
├── utils/
│   ├── hexid.js        # 264-bit identifier math (XOR distance, S2 prefix split, etc.)
│   ├── geo.js          # haversine, propagation delay, continent detection
│   └── s2.js           # geographic-cell encoding (S2 Hilbert prefix)
│
└── errors.js           # typed error classes + ErrorCodes
```

## What's NOT inside

- The simulator itself (`SimulatedTransport`, the benchmark harness, the 3D
  globe visualiser) — those live in [`axona-net/dht-sim`](https://github.com/axona-net/dht-sim).
- The reference browser peer with the production UI, region picker, identity
  persistence flow, and bridge fallback — that's [`axona-net/axona-peer`](https://github.com/axona-net/axona-peer).
- The signaling broker — [`axona-net/axona-bridge`](https://github.com/axona-net/axona-bridge).
- The headless Node supernode (real WebRTC via `node-datachannel`, console
  dashboard) — [`axona-net/axona-relay`](https://github.com/axona-net/axona-relay).
- The whitepaper, paper, explainer, programmer guide, and API reference —
  those live in [`axona-net/axona-docs`](https://github.com/axona-net/axona-docs).

## Examples

- [`examples/minimal-pubsub/`](examples/minimal-pubsub/) — two peers in
  one Node process, pub/sub roundtrip in ~150 lines. Pinned to the
  local kernel source via `file:../..`, so `npm install && node index.js`
  picks up whatever's in `src/`. The right starting point for new
  developers.
- [`examples/minimal-pubsub-browser/`](examples/minimal-pubsub-browser/)
  — browser version of the same demo. It detects its host and targets the
  matching bridge (the SF testnet build runs at
  [`demo-testnet.axona.net`](https://demo-testnet.axona.net)), connects over
  WebSocket, and runs a kernel peer in the page.
- [`examples/s2-region-visualizer/`](examples/s2-region-visualizer/)
  — interactive 3D globe showing the 192 S2 cells that anchor every
  Axona nodeId and pub/sub topic. Each cell shows **both** of its region
  names and the hex code; click a cell in the legend to light it up on the
  globe, or hit "Detect my location" to find your own region. Useful when
  explaining why polar cells shrink, or for picking a region label for a topic.

For real-world wiring (WebRTC + bridge fallback, identity persistence,
region pickers), the canonical example is
[`axona-peer/src/client.js`](https://github.com/axona-net/axona-peer/blob/main/src/client.js)
— the reference browser peer at ~1500 lines.

## Programmer guide

The full programmer-facing documentation lives in
[`axona-net/axona-docs`](https://github.com/axona-net/axona-docs):

| Document | When to read |
|---|---|
| [Quick Start](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Quick-Start-v2.16.0.md) | You want a working pub/sub roundtrip in 5 minutes |
| [API Reference](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-API-Reference-v2.16.0.md) | You're building and need a specific call's signature |
| [Programmer Guide](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-Programmer-Guide-v2.16.0.md) | You're starting a new application against Axona |

## Wire protocol

The frame shapes, message-type vocabulary, version handshake, and bridge
wire format are in [`axona-docs/implementation/`](https://github.com/axona-net/axona-docs/tree/main/implementation).
The handshake modules at [`src/transport/handshake.js`](src/transport/handshake.js)
(version negotiation) and [`src/transport/handshake-auth.js`](src/transport/handshake-auth.js)
(the `axona/5` authenticated-identity gate) are the canonical implementation
reference.

## Tests

```bash
npm test
```

Runs 51 smoke suites covering addressing, the 192 region names (two-per-cell,
name→code uniqueness), errors, persistence (in-memory, file, IndexedDB),
identity (incl. non-extractable keys + key-correspondence checks), all three
transports, version handshake, the `axona/5` authenticated handshake (primitive
+ sim- and node-transport enforcement), mesh-signal relay (bridgeless),
pub/sub (unified API, envelope, pull, metrics, subscribe authorization,
replay idempotency), direct messaging, mesh introspection + eviction,
health, lifecycle (join/leave/snapshot), and Ed25519 post signing.

## License

MIT — see [LICENSE](LICENSE).
