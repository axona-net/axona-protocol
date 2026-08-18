// smoke_ingest_ack.mjs — the typed INGEST-ack (Dead-Root Eviction v0.3,
// phase E2). A root emits {topicId, msgId, epoch, op} one hop back to the
// FORWARDER after topic-store ingest — never at routing, never to the origin
// publisher (no publish-ack: location privacy). PUB and KILL complete
// separately; duplicates ack as success (idempotent retry); verification
// drops ack NOTHING. The receiver records the correlation for the E3 flight.
//
// Real Ed25519 envelopes through the real ingress — nothing stubbed on the
// path under test (the fence_q2 harness discipline).
//
// Run: node test/smoke_ingest_ack.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc = (s) => String(s).toLowerCase();

function mk(selfId) {
  const clock = { t: 1_700_000_000_000 };
  const sends = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      sends.push({ target, type, payload });
      return { consumed: true, hops: 1, atNode: idHex(typeof target === 'bigint' ? target : BigInt(`0x${target}`)) };
    },
    findKClosest: async () => [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)],
    neighbors: () => [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)],
    bridgeId: () => null,
    lookup: async () => ({ path: [idHex(selfId ^ 0x11n)] }),
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = selfId;
  am.setLogSink(() => {});
  return { am, clock, sends };
}

async function rootFor(desc) {
  const topicId = await deriveTopicIdBig(desc);
  const region = topicId >> 256n;
  const selfId = (region << 256n) | 0x5eedn;
  const h = mk(selfId);
  h.am.pubsubSubscribe(topicId);
  const role = h.am._becomeRoot(topicId);
  return { ...h, topicId, role, selfId };
}

const DESC = { region: 'useast', owner: null, name: 'e2-ingest-ack', write: 'open' };
const author = await createAuthorIdentity();
const acksTo = (sends, forwarder) =>
  sends.filter((s) => s.type === T.INGESTACK && s.target === forwarder);

// ── 1. PUB ingest → correlated ack to the FORWARDER, not the routing layer ──
{
  const { am, clock, sends, topicId, role, selfId } = await rootFor(DESC);
  const forwarder = selfId ^ 0x33n;
  const env = await buildEnvelope({ topic: DESC, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  await am._onPub({ topicId: idHex(topicId), json: JSON.stringify(env) }, { fromId: idHex(forwarder), isTerminal: true });
  const acks = acksTo(sends, forwarder);
  ok('1a exactly one INGESTACK to the forwarder', acks.length === 1, `got ${acks.length}`);
  const p = acks[0]?.payload || {};
  ok('1b ack binds topicId + msgId + op', lc(p.topicId) === lc(idHex(topicId)) && p.msgId === env.msgId && p.op === 'pub',
    JSON.stringify(p).slice(0, 140));
  ok('1c ack carries the root incarnation (E1 epoch)', p.epoch === role.epoch && role.epoch >= 1, `ack ${p.epoch}, role ${role.epoch}`);

  // Duplicate publish = idempotent retry → acks again as success.
  await am._onPub({ topicId: idHex(topicId), json: JSON.stringify(env) }, { fromId: idHex(forwarder), isTerminal: true });
  ok('1d duplicate publish acks again (idempotent success)', acksTo(sends, forwarder).length === 2);
}

// ── 2. Verification drop acks NOTHING ───────────────────────────────────────
{
  const { am, clock, sends, topicId, selfId } = await rootFor(DESC);
  const forwarder = selfId ^ 0x33n;
  const env = await buildEnvelope({ topic: DESC, message: { k: 2 }, seq: 1, identity: author, ts: clock.t });
  env.signature = env.signature.replace(/^../, '00');   // break the signature
  await am._onPub({ topicId: idHex(topicId), json: JSON.stringify(env) }, { fromId: idHex(forwarder), isTerminal: true });
  ok('2a tampered envelope → no ack', acksTo(sends, forwarder).length === 0);
  await am._onPub({ topicId: idHex(topicId), json: 'not json' }, { fromId: idHex(forwarder), isTerminal: true });
  ok('2b unparseable body → no ack', acksTo(sends, forwarder).length === 0);
}

// ── 3. KILL completes separately, op:'kill', target msgId bound ─────────────
{
  const { am, clock, sends, topicId, selfId } = await rootFor(DESC);
  const forwarder = selfId ^ 0x33n;
  const env = await buildEnvelope({ topic: DESC, message: { k: 3 }, seq: 1, identity: author, ts: clock.t });
  await am._onPub({ topicId: idHex(topicId), json: JSON.stringify(env) }, { fromId: idHex(forwarder), isTerminal: true });
  const kill = await buildKill({ topicId: idHex(topicId), msgId: env.msgId, ts: clock.t + 1, seq: 2, identity: author });
  await am._onKill({ topicId: idHex(topicId), kill }, { fromId: idHex(forwarder), isTerminal: true });
  const kAcks = acksTo(sends, forwarder).filter((a) => a.payload.op === 'kill');
  ok('3a kill acks with op:kill bound to the TARGET msgId', kAcks.length === 1 && kAcks[0].payload.msgId === env.msgId,
    JSON.stringify(kAcks.map((a) => a.payload)).slice(0, 140));
}

// ── 4. Receipt side: _onIngestAck records the correlation, bounded ──────────
{
  const { am } = mk(0x89n << 248n | 0x5eedn);
  am._onIngestAck({ topicId: idHex(0x89n << 248n | 0x1n), msgId: 'm1', epoch: 3, op: 'pub' }, { fromId: 'aa' });
  const key = `${lc(idHex(0x89n << 248n | 0x1n))}|m1|pub`;
  ok('4a valid ack recorded with epoch', am._ingestAcks?.get(key)?.epoch === 3);
  am._onIngestAck({ topicId: idHex(0x89n << 248n | 0x1n), msgId: 'm1', op: 'garbage' }, {});
  ok('4b malformed op records nothing new', am._ingestAcks.size === 1);
  for (let i = 0; i < 600; i++) am._onIngestAck({ topicId: idHex(0x89n << 248n | BigInt(i + 2)), msgId: `m${i}`, epoch: 0, op: 'pub' }, {});
  ok('4c store is bounded at 512', am._ingestAcks.size <= 512, `size ${am._ingestAcks.size}`);
}

console.log(fail === 0 ? `\nsmoke_ingest_ack: ${n}/${n} ok` : `\nsmoke_ingest_ack: ${fail} FAILED of ${n}`);
process.exit(fail === 0 ? 0 : 1);
