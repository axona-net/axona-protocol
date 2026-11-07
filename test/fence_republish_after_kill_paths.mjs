// fence_republish_after_kill_paths.mjs — 4.131.0, Orion 7b503240 (RAK-1a/1b/2/3 +
// merge retention): every path that can carry an OLDER kill must leave a
// republished body alone, and every path that carries a NEWER kill must still
// retract it.
//
//   K1a  a re-sent pre-republish KILL at the root is acked as settled; the body stays
//   K1c  CONTROL: a new kill signed after the republication still retracts it
//   K1b  a propagated tombstone stamped below the held body is ignored
//   K1e  the superseded kill re-stamped above the held body is ignored (Orion KP-2)
//   K1d  CONTROL: a NEWER signed kill stamped above the held body still applies
//   K2   a live subscriber sees pub1, kill1, pub2, kill2 (4 events) and its since
//        floor reaches pub2's stamp; a stale kill1 replayed after pub2 is not delivered
//   K2c  CONTROL: a body replayed at or below its delivered kill stays dropped;
//        plain exactly-once holds for a never-killed body
//   K3   same-millisecond kill() then pub() on one peer (equal ts, higher seq) is a republication
//   M    merge retention counts a del whose newer body is held (mergeMachine dels line)
//
// Run: node test/fence_republish_after_kill_paths.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { authorAfter } from '../src/pubsub/topicStore.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const TOPIC = { region: 'useast', owner: null, name: 'republish-paths', write: 'open' };
const topicHex = await deriveTopicId(TOPIC), topicBig = await deriveTopicIdBig(TOPIC);
const alice = await createAuthorIdentity();
const T0 = 1_700_000_000_000;
const harness = (self, clock) => {
  const raw = { getSelfId: () => self, onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
    sendDirect: async () => true, verdictsSupported: true, routeMessage: async () => ({ consumed: true }), findKClosest: async () => [] };
  const am = new AxonaManager({ dht: sealTestDht(raw), now: () => clock.t, rootReplicas: 0 });
  const events = []; am._deliveryCallback = (t, json, msgId, publishTs) => events.push({ deleted: !!JSON.parse(json).deleted, msgId, publishTs });
  return { am, events };
};
const meta = { targetId: 10n, fromId: 20n, isTerminal: true };
let n = 0; const body = () => `{"fire":${++n}}`;

console.log('fence_republish_after_kill_paths');
// K1a + K1c — root
{
  const clock = { t: T0 }; const { am } = harness(10n, clock); const role = am._becomeRoot(topicBig, 'fence');
  const msg = body();
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 }); await am._ingestPublish(role, JSON.stringify(e1));
  clock.t += 1000; const k1 = await buildKill({ topicId: topicHex, msgId: e1.msgId, identity: alice, ts: clock.t, seq: 2 });
  await am._onKill({ topicId: topicHex, kill: k1 }, meta);
  clock.t += 1000; const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 3 });
  const r2 = await am._ingestPublish(role, JSON.stringify(e2));
  check('K1a: republication accepted at the root', r2.ok && !r2.suppressed && role.cacheIds.has(e2.msgId));
  clock.t += 500; const seqBefore = role.seq;
  await am._onKill({ topicId: topicHex, kill: k1 }, meta);
  check('K1a: re-sent pre-republish kill leaves the body', role.cacheIds.has(e2.msgId) && !role.tombstones.has(e2.msgId));
  check('K1a: and takes no stamp or seq slot', role.seq === seqBefore, `seq ${seqBefore} -> ${role.seq}`);
  clock.t += 500; const k2 = await buildKill({ topicId: topicHex, msgId: e2.msgId, identity: alice, ts: clock.t, seq: 4 });
  await am._onKill({ topicId: topicHex, kill: k2 }, meta);
  check('K1c CONTROL: a kill signed after the republication retracts it', !role.cacheIds.has(e2.msgId) && role.tombstones.has(e2.msgId));
}
// K1b + K1d — propagated tombstones
{
  const clock = { t: T0 }; const { am } = harness(10n, clock); const role = am._becomeRoot(topicBig, 'fence');
  const msg = body();
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 }); await am._ingestPublish(role, JSON.stringify(e1));
  clock.t += 1000; await am._onKill({ topicId: topicHex, kill: await buildKill({ topicId: topicHex, msgId: e1.msgId, identity: alice, ts: clock.t, seq: 2 }) }, meta);
  const stale = am._activeDels(role);
  clock.t += 1000; const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 3 }); await am._ingestPublish(role, JSON.stringify(e2));
  const heldTs = role.cache.find(c => c.msgId === e2.msgId)?.publishTs;
  await am._syncIngest({ topicId: topicHex, from: '0'.repeat(65) + 'b', msgs: [], dels: stale }, { targetId: 10n }, 'REPLICATE');
  check('K1b: a tombstone stamped below the held body is ignored', role.cacheIds.has(e2.msgId) && !role.tombstones.has(e2.msgId), JSON.stringify({ killTs: stale[0]?.killTs, heldTs }));
  const restamped = stale.map(d => ({ ...d, killTs: heldTs + 1, publishTs: heldTs + 1 }));   // the OLD signed kill, re-stamped above the body
  await am._syncIngest({ topicId: topicHex, from: '0'.repeat(65) + 'b', msgs: [], dels: restamped }, { targetId: 10n }, 'REPLICATE');
  check('K1e: the superseded kill re-stamped above the body is ignored (Orion KP-2)', role.cacheIds.has(e2.msgId) && !role.tombstones.has(e2.msgId));
  const k2 = await buildKill({ topicId: topicHex, msgId: e2.msgId, identity: alice, ts: clock.t + 500, seq: 4 });
  const newer = [{ del: true, msgId: e2.msgId, killTs: heldTs + 1, publishTs: heldTs + 1, signer: alice.pubkeyHex.toLowerCase(), seq: 9, kill: k2 }];
  await am._syncIngest({ topicId: topicHex, from: '0'.repeat(65) + 'b', msgs: [], dels: newer }, { targetId: 10n }, 'REPLICATE');
  check('K1d CONTROL: a NEWER signed kill stamped above the held body still retracts it', !role.cacheIds.has(e2.msgId) && role.tombstones.has(e2.msgId));
}
// K2 + K2c — live subscriber
{
  const clock = { t: T0 }; const { am, events } = harness(30n, clock);
  am.mySubscriptions.set(topicBig, { since: 0, interval: 5000 });
  const from = '0'.repeat(65) + 'a', signer = alice.pubkeyHex.toLowerCase(), msg = body();
  const deliver = (m) => am._onDeliver({ topicId: topicHex, from, msgs: [m] }, { targetId: 30n });
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0, seq: 1 });
  const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0 + 1000, seq: 3 });
  await deliver({ json: JSON.stringify(e1), publishTs: T0 + 100, msgId: e1.msgId, seq: 1 });
  await deliver({ del: true, msgId: e1.msgId, killTs: T0 + 200, publishTs: T0 + 200, signer, seq: 2 });
  await deliver({ json: JSON.stringify(e1), publishTs: T0 + 100, msgId: e1.msgId, seq: 1 });
  check('K2c CONTROL: a body replayed below its delivered kill stays dropped', events.length === 2, JSON.stringify(events));
  await deliver({ json: JSON.stringify(e2), publishTs: T0 + 300, msgId: e2.msgId, seq: 3 });
  check('K2: the republication reaches a live subscriber', events.length === 3 && !events[2].deleted, JSON.stringify(events));
  check('K2: the since floor reaches its stamp', am._sinceFor(topicBig) === T0 + 300, String(am._sinceFor(topicBig)));
  await deliver({ del: true, msgId: e1.msgId, killTs: T0 + 200, publishTs: T0 + 200, signer, seq: 2 });
  check('K2: a stale kill1 replayed after pub2 is not delivered', events.length === 3, JSON.stringify(events));
  await deliver({ json: JSON.stringify(e2), publishTs: T0 + 300, msgId: e2.msgId, seq: 3 });
  check('K2c CONTROL: exactly-once for the live republication', events.length === 3);
  await deliver({ del: true, msgId: e2.msgId, killTs: T0 + 400, publishTs: T0 + 400, signer, seq: 4 });
  check('K2: kill2 reaches the subscriber (4 events)', events.length === 4 && events[3].deleted, JSON.stringify(events));
}
// K3 — same millisecond
{
  check('K3: authorAfter orders equal ts by seq', authorAfter(5, 7, 5, 6) && !authorAfter(5, 6, 5, 6) && !authorAfter(5, 5, 5, 6) && !authorAfter(5, 9, 5, 0));
  const clock = { t: T0 }; const { am } = harness(10n, clock); const role = am._becomeRoot(topicBig, 'fence');
  const msg = body();
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: clock.t + 1 }); await am._ingestPublish(role, JSON.stringify(e1));
  clock.t += 500; await am._onKill({ topicId: topicHex, kill: await buildKill({ topicId: topicHex, msgId: e1.msgId, identity: alice, ts: clock.t, seq: clock.t + 1 }) }, meta);
  const same = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: clock.t + 2 });
  const r = await am._ingestPublish(role, JSON.stringify(same));
  check('K3: same-ms kill() then pub() is accepted as a republication', r.ok && !r.suppressed && role.cacheIds.has(same.msgId), JSON.stringify(r));
  const clock2 = { t: T0 }; const h2 = harness(10n, clock2); const role2 = h2.am._becomeRoot(topicBig, 'fence');
  const f1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock2.t, seq: clock2.t + 1 }); await h2.am._ingestPublish(role2, JSON.stringify(f1));
  clock2.t += 500; await h2.am._onKill({ topicId: topicHex, kill: await buildKill({ topicId: topicHex, msgId: f1.msgId, identity: alice, ts: clock2.t, seq: clock2.t + 2 }) }, meta);
  const lower = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock2.t, seq: clock2.t + 1 });
  const r2 = await h2.am._ingestPublish(role2, JSON.stringify(lower));
  check('K3 CONTROL: same ts with a LOWER seq than the kill stays dead', r2.suppressed === true, JSON.stringify(r2));
}
// M — merge retention line (source fence: the dels count must accept a held body, as the bodies count does)
{
  const { readFileSync } = await import('node:fs');
  const src = readFileSync(new URL('../src/pubsub/mergeMachine.js', import.meta.url), 'utf8');
  check('M: merge dels retention counts a held republication', /for \(const id of p\.manifest\.dels\) if \(\(role\.tombstones\.get\(id\)\?\.exp \?\? 0\) > now \|\| role\.cacheIds\.has\(id\)\) delsActive\+\+/.test(src));
}
console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
