# examples/minimal-pubsub

Hello-world for `@axona/protocol`: two peers in one Node process, connected
by an in-process `SimNetwork`. Alice publishes, Bob subscribes, the signed
envelope arrives. The whole demo is ~150 lines.

## Run

```bash
npm install     # resolves @axona/protocol → ../.. (the kernel source)
node index.js
```

Expected output:

```
[alice] nodeId: dfe5…
[bob]   nodeId: dffe…
[alice] published msgId=ae5f…
[bob]   received: { message: 'hello from alice', signerPubkey: '4a15…' }

✓ roundtrip ok — bob received alice's envelope
```

## Architecture

```
              ┌────────────┐         ┌────────────┐
              │   alice    │         │    bob     │
              │ AxonaPeer  │         │ AxonaPeer  │
              └─────┬──────┘         └─────┬──────┘
                    │                      │
              ┌─────┴──────┐         ┌─────┴──────┐
              │SimTransport│←───────→│SimTransport│
              └─────┬──────┘         └─────┬──────┘
                    │                      │
                    └──────  SimNetwork ───┘
```

`SimNetwork` is an in-process router that delivers frames between
`SimTransport` instances. It satisfies the same `Transport` contract that
`Transport.web()` (WebRTC) and `Transport.node()` (raw WebSocket) do — so
anything you build against it will port to real transports unchanged.

## What this demonstrates

- `deriveIdentity({ lat, lng })` — 264-bit Ed25519 identity anchored to an S2 cell
- Two `simTransport()`s on a shared `SimNetwork` — the kernel's in-process router
- Composing `AxonaPeer` + `AxonaManager` from the kernel primitives
- Region-keyed topics via a synthetic publisher
- `peer.pub()` / `peer.sub()` roundtrip across two distinct peers

## What this does NOT demonstrate

Real bootstrap, identity persistence, region pickers, WebRTC mesh + WebSocket
bridge fallback, and the full transport lifecycle. For all that, see
[`axona-peer/src/client.js`](https://github.com/axona-net/axona-peer/blob/main/src/client.js)
— the reference browser peer (~1500 lines).

## Files

| File | What it is |
|---|---|
| `package.json` | Pin to `../..` (the local `@axona/protocol` source) |
| `index.js` | Full demo, ~150 lines including comments |
