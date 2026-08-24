// =====================================================================
// smoke_dht_presence.mjs — the dht:presence reset record (Connection-Quality
// v0.7 "The reset record", axona-docs 66f50bc; implementation slice 2).
//
// Covers:
//   1. RECORD: build → verify ok; identity key = 256-bit nodeId suffix.
//   2. FORGERY: tampered gen, foreign pubkey, wrong proto, bad shape — refused
//      with the precise reason; a third party cannot mint for another identity.
//   3. REGISTRY: dht:presence is a declared B5 row (bare wire 'presence').
//   4. RECEIVER (always live): verified record advances the per-identity
//      watermark and fires hooks; replay/stale do nothing; NOT a nomination —
//      the synaptome never gains an entry from a record.
//   5. RELAY (armed only): origin-sent records forward exactly ONE hop
//      (hop 0 → hop 1, never further), rate-limited per origin identity;
//      an un-armed receiver never relays.
//   6. ANNOUNCE (armed only): announcePresence signs with the peer's own
//      identity, increments gen, reaches current neighbours; un-armed is inert.
//
// Run: node test/smoke_dht_presence.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';
import { buildPresenceRecord, verifyPresenceRecord, PRESENCE_PROTO } from '../src/dht/presence.js';
import { frameWiring, rowDefs }     from '../src/dht/boundary5Registry.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));

async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}

async function main() {
  console.log('Axona dht:presence smoke (reset record, slice 2)\n');

  // ── 1 + 2. the record itself ──────────────────────────────────────────
  {
    const idA = await createNodeIdentity({ lat: 1, lng: 1 });
    const idB = await createNodeIdentity({ lat: 2, lng: 2 });
    const rec = await buildPresenceRecord({ identity: idA, gen: 3 });
    const v = await verifyPresenceRecord(rec);
    check('1 RECORD: build → verify ok', v.ok === true && v.gen === 3);
    check('1 RECORD: identity key is the 256-bit nodeId suffix', v.identityHex === idA.id.slice(2) && v.identityHex.length === 64);
    const tamper = await verifyPresenceRecord({ ...rec, gen: 4 });
    check('2 FORGERY: tampered gen fails the signature', tamper.ok === false && tamper.reason === 'bad_signature');
    const foreign = await verifyPresenceRecord({ ...rec, nodeId: idB.id });
    check('2 FORGERY: another identity\'s nodeId fails the binding — no third-party minting', foreign.ok === false && foreign.reason === 'pubkey_nodeid_mismatch');
    const proto = await verifyPresenceRecord({ ...rec, proto: 'axona/presence/9' });
    check('2 FORGERY: wrong proto refused', proto.ok === false && proto.reason === 'proto_mismatch');
    const shape = await verifyPresenceRecord({ proto: PRESENCE_PROTO, nodeId: 'zz', pubkey: '', gen: -1 });
    check('2 FORGERY: malformed shape refused', shape.ok === false);
  }

  // ── 3. registry row ───────────────────────────────────────────────────
  {
    const w = frameWiring(rowDefs());
    const row = w.get('presence');
    check('3 REGISTRY: dht:presence declared as a B5 bare-wire notification', row?.type === 'dht:presence' && row?.transportKind === 'notification');
  }

  // ── 4-6. peers on a real sim mesh ─────────────────────────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const A = await makePeer(net, domain, 5, 5, { presence: { announceOnStart: false, relayRateMs: 100 } });
    const B = await makePeer(net, domain, 6, 6, { presence: { announceOnStart: false, relayRateMs: 100 } });
    const C = await makePeer(net, domain, 7, 7, { presence: { announceOnStart: false, relayRateMs: 100 } });
    const D = await makePeer(net, domain, 8, 8, { presence: { announceOnStart: false, relayRateMs: 100 } });
    // Chain A—B, B—C, C—D (no other edges): relay depth is observable per hop.
    await A.transport.openConnection(B.hex);
    await B.transport.openConnection(C.hex);
    await C.transport.openConnection(D.hex);
    await wait(25);
    const keyA = A.hex.slice(2);

    let hooks = 0; B.peer.onPresence(() => hooks++);
    const sent = await A.peer.announcePresence();            // gen 1, hop 0 → B
    await wait(60);                                          // B relays (hop 1) → A + C; C stops
    check('6 ANNOUNCE: armed announce reached current neighbours', sent >= 1, `(sent=${sent})`);
    check('4 RECEIVER: B verified and watermarked the origin identity', B.peer._presenceWatermarks.get(keyA) === 1);
    check('4 RECEIVER: hook fired once on fresh gen', hooks === 1, `(hooks=${hooks})`);
    check('5 RELAY: one hop — C (via B\'s relay) holds the watermark', C.peer._presenceWatermarks.get(keyA) === 1);
    check('5 RELAY: never a second hop — D (via C) holds nothing', D.peer._presenceWatermarks.get(keyA) === undefined);
    check('4 NOT A NOMINATION: C\'s synaptome did not gain the origin', !C.node.synaptome.has(A.big));

    // Replay: hand B the same record again — nothing moves.
    const rec1 = await buildPresenceRecord({ identity: A.id, gen: 1 });
    await A.transport.notify(B.big, 'presence', { ...rec1, hop: 0 }).catch(() => {});
    await wait(30);
    check('4 RECEIVER: replayed gen does nothing (watermark and hooks unchanged)', B.peer._presenceWatermarks.get(keyA) === 1 && hooks === 1);

    // Rate limit: gen 2 inside B's relay window — B watermarks but does not relay.
    await A.peer.announcePresence();                          // gen 2 within 100ms of the gen-1 relay
    await wait(30);
    check('4 RECEIVER: fresh gen advances B\'s watermark', B.peer._presenceWatermarks.get(keyA) === 2);
    check('5 RELAY: rate limit held — C still at gen 1', C.peer._presenceWatermarks.get(keyA) === 1);

    // Un-armed receiver never relays: E (default, no presence cfg) between A2 and F.
    const A2 = await makePeer(net, domain, 9, 9, { presence: { announceOnStart: false } });
    const E  = await makePeer(net, domain, 10, 10, {});       // receiver-only default
    const F  = await makePeer(net, domain, 11, 11, {});
    await A2.transport.openConnection(E.hex);
    await E.transport.openConnection(F.hex);
    await wait(25);
    await A2.peer.announcePresence();
    await wait(60);
    const keyA2 = A2.hex.slice(2);
    check('5 RELAY: un-armed receiver watermarks but NEVER relays', E.peer._presenceWatermarks.get(keyA2) === 1 && F.peer._presenceWatermarks.get(keyA2) === undefined);
    check('6 ANNOUNCE: un-armed peer\'s announce is inert', (await E.peer.announcePresence()) === 0);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
