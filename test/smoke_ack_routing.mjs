// smoke_ack_routing.mjs — Write-Flight Ack Routing (4.62.2) D1, the deaf-flight
// fix, proven at the manager seam. A flight owner opens a write flight; the root
// ingests and signs an ACK PROOF; the proof, delivered from a relay that is NOT
// the root (the last-hop case that #51 got wrong), still completes the flight —
// because completion binds the signed proof to the owner's flight, never the
// last hop. Negative cases: a proof from the wrong identity, a wrong flightNonce,
// and a wrong attemptId all leave the flight open for receipt-bound recovery.
//
// Run: node test/smoke_ack_routing.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { signAckProof, PURPOSE, OP } from '../src/pubsub/ackProof.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { idHex, idBig } from '../src/pubsub/ids.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

function mkOwner(selfId) {
  const clock = { t: 1_700_000_000_000 };
  const dht = {
    verdictsSupported: true,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async () => ({ consumed: true, hops: 1 }),
    neighbors: () => [],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t });
  am.nodeId = selfId;
  am.setLogSink(() => {});
  am._rootBeacons = new Map();       // unversioned flights (epoch 0)
  return am;
}

const flightCount = (am) => (am._writeFlights ? am._writeFlights.size : 0);

// ── actors ────────────────────────────────────────────────────────────
const rootIdent = await createNodeIdentity({ lat: 37.77, lng: -122.42 });
const rootBig   = idBig(rootIdent.id);
const rootHex   = rootIdent.id.toLowerCase();
const ownerBig  = rootBig ^ (1n << 200n);          // some other node — the flight owner
const relayBig  = rootBig ^ (1n << 100n);          // a relay one hop from the root (NOT the root)
const topicBig  = rootBig ^ 0x1234n;
const msgId     = 'a'.repeat(64);                  // valid 64-hex content hash
const payload   = { topicId: idHex(topicBig), json: JSON.stringify({ msgId }) };

// Build a signed INGEST-ACK proof exactly as the root's _sendSignedIngestAck does.
async function proofFor({ ackTo, flightNonce, attemptId, epoch = 0, ident = rootIdent }) {
  return signAckProof((b) => ident.sign(b), {
    purpose: PURPOSE.INGEST_ACK, op: OP.pub,
    topicId: idHex(topicBig), msgId, epoch,
    attemptId, ackTo, flightNonce, rootPub: ident.pubkey,
  });
}

// ── 1. the deaf-flight fix: signed proof completes across a non-root last hop ──
{
  const owner = mkOwner(ownerBig);
  const meta = owner._flightOpen(topicBig, rootHex, T.PUB, payload);
  ok('1a flight opened, ackTo is the owner', meta && meta.ackTo === idHex(ownerBig).toLowerCase(), meta && meta.ackTo);
  ok('1b flight is registered', flightCount(owner) === 1);
  const frame = await proofFor(meta);
  // Delivered from a RELAY, not the root — the exact case #51 dropped.
  await owner._onIngestAck(frame, { fromId: idHex(relayBig) });
  ok('1c signed proof from a non-root relay COMPLETES the flight (deaf-flight fixed)', flightCount(owner) === 0);
}

// ── 2. wrong signing identity → authority mismatch → flight stays open ──
{
  const owner = mkOwner(ownerBig);
  const meta = owner._flightOpen(topicBig, rootHex, T.PUB, payload);
  const other = await createNodeIdentity({ lat: 51.5, lng: -0.12 });
  const frame = await proofFor({ ...meta, ident: other });   // signed by `other`, rootPub = other.pubkey
  await owner._onIngestAck(frame, { fromId: idHex(relayBig) });
  ok('2 a proof from the wrong identity does NOT complete (authority binding)', flightCount(owner) === 1);
}

// ── 3. wrong flightNonce → bind fails → flight stays open ──
{
  const owner = mkOwner(ownerBig);
  const meta = owner._flightOpen(topicBig, rootHex, T.PUB, payload);
  const frame = await proofFor({ ...meta, flightNonce: '00'.repeat(16) });
  await owner._onIngestAck(frame, { fromId: idHex(relayBig) });
  ok('3 a proof with the wrong flightNonce does NOT complete', flightCount(owner) === 1);
}

// ── 4. wrong attemptId → bind fails → flight stays open ──
{
  const owner = mkOwner(ownerBig);
  const meta = owner._flightOpen(topicBig, rootHex, T.PUB, payload);
  const frame = await proofFor({ ...meta, attemptId: 'ff'.repeat(16) });
  await owner._onIngestAck(frame, { fromId: idHex(relayBig) });
  ok('4 a proof with the wrong attemptId does NOT complete', flightCount(owner) === 1);
}

// ── 5. the correct proof still completes after the negatives (control) ──
{
  const owner = mkOwner(ownerBig);
  const meta = owner._flightOpen(topicBig, rootHex, T.PUB, payload);
  const bad  = await proofFor({ ...meta, flightNonce: '00'.repeat(16) });
  await owner._onIngestAck(bad, { fromId: idHex(relayBig) });
  ok('5a still open after a bad proof', flightCount(owner) === 1);
  const good = await proofFor(meta);
  await owner._onIngestAck(good, { fromId: idHex(relayBig) });
  ok('5b the correct proof then completes it', flightCount(owner) === 0);
}

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
