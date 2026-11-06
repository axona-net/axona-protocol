// fence_republish_after_kill_commit.mjs — 4.131.0, Aster 61bad8a8 (R1, R2):
// a tombstone is removed only by a write that COMMITS, and never by an
// unverified frame.
//
//   R1a  root ingress refused at the merge reserve keeps the tombstone; the
//        pre-kill copy is then still suppressed; the republication, once
//        admitted, supersedes
//   R1b  a stamped replay refused at the cut keeps the tombstone
//   R2a  an unsigned DELIVER frame claiming the tombstone's signer and a later
//        stamp neither removes the tombstone nor is cached
//   R2b  an unsigned DELIVER frame claiming ANOTHER signer cannot revoke it
//   R2c  CONTROL: a verified, signed republication on DELIVER above killTs supersedes
//   R1c  a REPLAYUP of the validly signed PRE-kill body with a forged stamp above
//        killTs stays dead (the author's kill time is known at the root)
//   R2d  the same replay on DELIVER at a relay holding the author's kill time stays dead
//   R2e  armed: a DATA-only DELIVER (unpinned sender) cannot lift a kill, even
//        with a verified republication and a stamp above killTs
//   R2f  CONTROL armed: the same frame from the pinned upstream supersedes
//   S    role.superseded is swept with the tombstones at expiry
// (Orion e47eb97c)
//   B1   PROOF-LESS tombstone (no author kill time: every node but the verifying
//        root while tombstoneAuth is off, e.g. a promoted backup): a REPLAYUP of
//        the signed pre-kill body with a later claimed stamp stays dead
//   B2   proof-less, UNARMED DELIVER of the same stays dead
//   B3   proof-less, armed DELIVER from a data-only sender stays dead
// (Aster d165d9ce; R2f is the proof-less armed accepted-upstream control)
//   P2   a proof-less heir's stamped batch counts an unvouched newer body as
//        NOT held, so a HANDOFFACK never claims held === sent for it
//   P4   a superseded tombstone carrying only a verified proof records the
//        proof's (ts, seq) in role.superseded (RAK-1a holds on proof-bearing nodes)
//   P5   the durability ledger re-opens a msgId whose earlier copy is terminal
// (Orion e862068e)
//
// Run: node test/fence_republish_after_kill_commit.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const TOPIC = { region: 'useast', owner: null, name: 'republish-commit', write: 'open' };
const topicHex = await deriveTopicId(TOPIC), topicBig = await deriveTopicIdBig(TOPIC);
const alice = await createAuthorIdentity();
const T0 = 1_700_000_000_000;
const harness = (clock, { armed = true } = {}) => {
  const raw = { getSelfId: () => 10n, onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
    sendDirect: async () => true, verdictsSupported: true, routeMessage: async () => ({ consumed: true }), findKClosest: async () => [] };
  return new AxonaManager({ dht: sealTestDht(raw), now: () => clock.t, rootReplicas: 0, topicLifetime: armed });
};
const meta = { targetId: 10n, fromId: 20n, isTerminal: true };
let n = 0; const body = () => `{"fire":${++n}}`;
// A root holding a verified kill of alice's message; returns the old and the republished envelope.
async function killed(clock, am, role) {
  const msg = body();
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 }); await am._ingestPublish(role, JSON.stringify(e1));
  clock.t += 1000; await am._onKill({ topicId: topicHex, kill: await buildKill({ topicId: topicHex, msgId: e1.msgId, identity: alice, ts: clock.t, seq: 2 }) }, meta);
  clock.t += 1000; const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 3 });
  return { e1, e2, killTs: role.tombstones.get(e1.msgId)?.killTs };
}

console.log('fence_republish_after_kill_commit');
{
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');
  const { e1, e2 } = await killed(clock, am, role);
  role.merge ??= {}; role.merge.reserve = { entries: 0, bytes: 0 };
  const r = await am._ingestPublish(role, JSON.stringify(e2), am._cutCapture(role));
  check('R1a: republish refused at the merge reserve', r.ok === false && r.reason === 'merge-reserve', JSON.stringify(r));
  check('R1a: the tombstone is kept', role.tombstones.has(e2.msgId));
  role.merge.reserve = undefined;
  const old = await am._ingestPublish(role, JSON.stringify(e1), am._cutCapture(role));
  check('R1a: the pre-kill copy is still suppressed afterwards', old.suppressed === true && !role.cacheIds.has(e1.msgId), JSON.stringify(old));
  const ok = await am._ingestPublish(role, JSON.stringify(e2), am._cutCapture(role));
  check('R1a: the admitted republication supersedes', ok.ok && !ok.suppressed && role.cacheIds.has(e2.msgId) && !role.tombstones.has(e2.msgId), JSON.stringify(ok));
}
{
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');
  const { e2, killTs } = await killed(clock, am, role);
  const cut = am._cutCapture(role); role.gen = (role.gen | 0) + 1;            // the generation moves: the cut no longer holds
  const r = await am._ingestStamped(role, { msgId: e2.msgId, json: JSON.stringify(e2), publishTs: killTs + 1, seq: 9 }, cut);
  check('R1b: a stamped replay refused at the cut keeps the tombstone', r === 'refused' && role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId), String(r));
}
{
  const clock = { t: T0 }; const am = harness(clock, { armed: false }); const role = am._becomeRoot(topicBig, 'fence');
  const { e2, killTs } = await killed(clock, am, role);
  role.isRoot = false;                                                        // an UNARMED relay on the DELIVER path
  const from = '0'.repeat(65) + 'a';
  const signer = role.tombstones.get(e2.msgId).signer;
  const forged = JSON.stringify({ msgId: e2.msgId, signerPubkey: signer, message: 'not a signed envelope', ts: clock.t + 5000 });
  await am._onDeliver({ topicId: topicHex, from, msgs: [{ msgId: e2.msgId, json: forged, publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('R2a: an unsigned same-signer frame keeps the tombstone and is not cached', role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId));
  const other = JSON.stringify({ msgId: e2.msgId, signerPubkey: 'ab'.repeat(32), message: 'forged', ts: clock.t });
  await am._onDeliver({ topicId: topicHex, from, msgs: [{ msgId: e2.msgId, json: other, publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('R2b: an unsigned other-signer frame cannot revoke the tombstone', role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId));
  await am._onDeliver({ topicId: topicHex, from, msgs: [{ msgId: e2.msgId, json: JSON.stringify(e2), publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('R2c CONTROL: unarmed, a verified republication on DELIVER above killTs supersedes', !role.tombstones.has(e2.msgId) && role.cacheIds.has(e2.msgId));
}
{
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');
  const { e1, killTs } = await killed(clock, am, role);
  const r = await am._ingestStamped(role, { msgId: e1.msgId, json: JSON.stringify(e1), publishTs: killTs + 5000, seq: 9 }, am._cutCapture(role));
  check('R1c: REPLAYUP of the signed pre-kill body with a forged stamp stays dead', r === 'held' && role.tombstones.has(e1.msgId) && !role.cacheIds.has(e1.msgId), String(r));
  role.isRoot = false;
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'a', msgs: [{ msgId: e1.msgId, json: JSON.stringify(e1), publishTs: killTs + 5000, seq: 9 }] }, { targetId: 10n });
  check('R2d: the same replay on DELIVER stays dead', role.tombstones.has(e1.msgId) && !role.cacheIds.has(e1.msgId));
}
async function armedRelay() {
  const clock = { t: T0 };
  const raw = { getSelfId: () => 10n, onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
    sendDirect: async () => true, verdictsSupported: true, routeMessage: async () => ({ consumed: true }), findKClosest: async () => [] };
  const am = new AxonaManager({ dht: sealTestDht(raw), now: () => clock.t, rootReplicas: 0, topicLifetime: true });
  const role = am._becomeRoot(topicBig, 'fence'); role.isRoot = false;
  const msg = body();
  const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t + 2000, seq: 3 });
  role.tombstones.set(e2.msgId, { exp: clock.t + 600_000, killTs: clock.t + 1000, signer: alice.pubkeyHex.toLowerCase(), seq: 2 });   // a relay's tombstone: no author kill time
  return { am, role, e2, killTs: clock.t + 1000 };
}
const ROOT_HEX = '89' + '0'.repeat(62) + '01', PIN = '0'.repeat(65) + 'c';
{
  const { am, role, e2, killTs } = await armedRelay();
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'd', gen: { root: ROOT_HEX, epoch: 1 }, msgs: [{ msgId: e2.msgId, json: JSON.stringify(e2), publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('R2e: armed, a data-only DELIVER cannot lift a kill', role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId));
}
{
  const { am, role, e2, killTs } = await armedRelay();
  am._upstream.set(topicBig, [PIN]);
  am._attach.set(topicBig, { gen: { root: ROOT_HEX, epoch: 1 }, from: PIN, nonce: null, seq: 0, attempts: new Map(), committed: 0 });
  await am._onDeliver({ topicId: topicHex, from: PIN, gen: { root: ROOT_HEX, epoch: 1 }, msgs: [{ msgId: e2.msgId, json: JSON.stringify(e2), publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('R2f CONTROL: armed, the pinned upstream\'s DELIVER supersedes', !role.tombstones.has(e2.msgId) && role.cacheIds.has(e2.msgId));
}
{
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');
  const { e2 } = await killed(clock, am, role);
  await am._ingestPublish(role, JSON.stringify(e2), am._cutCapture(role));
  const had = role.superseded?.has(e2.msgId) === true;
  am._expireTombstones(role, clock.t + 10 * 24 * 3600_000);
  check('S: role.superseded is recorded, then swept at expiry', had && role.superseded.size === 0, String(had));
}
// D1 / D2 / D3 (Orion a8767b2b)
{
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');   // armed (the kernel default)
  const { e2, killTs } = await killed(clock, am, role); role.isRoot = false;
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'a', msgs: [{ msgId: e2.msgId, json: JSON.stringify(e2), publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('D1: armed, an ungated DELIVER (no generation) cannot lift a kill', role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId));
}
{
  const clock = { t: T0 }; const am = harness(clock, { armed: false }); const role = am._becomeRoot(topicBig, 'fence');
  const { e2, killTs } = await killed(clock, am, role); role.isRoot = false;
  const other = { ...TOPIC, name: 'republish-commit-OTHER' };
  const eB = await buildEnvelope({ topic: other, message: JSON.parse(JSON.stringify(e2.message)), identity: alice, ts: e2.ts + 10, seq: 7 });
  check('D2: the same content signed on another topic has the same msgId', eB.msgId === e2.msgId);
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'a', msgs: [{ msgId: eB.msgId, json: JSON.stringify(eB), publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('D2: a cross-topic body cannot lift this topic\'s kill', role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId));
}
{
  const bob = await createAuthorIdentity();
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');
  const e = await buildEnvelope({ topic: TOPIC, message: body(), identity: alice, ts: clock.t, seq: 1 });
  role.tombstones.set(e.msgId, { exp: clock.t + 600_000, killTs: clock.t - 1, signer: bob.pubkeyHex.toLowerCase(), seq: 1 });   // a provisional kill by a non-author
  role.merge ??= {}; role.merge.reserve = { entries: 0, bytes: 0 };
  const r = await am._ingestPublish(role, JSON.stringify(e), am._cutCapture(role));
  check('D3a: a refused write leaves a revocable tombstone standing', r.ok === false && r.reason === 'merge-reserve' && role.tombstones.has(e.msgId), JSON.stringify(r));
  role.merge.reserve = undefined;
  const r2 = await am._ingestPublish(role, JSON.stringify(e), am._cutCapture(role));
  check('D3a CONTROL: the admitted author body revokes the non-author kill', r2.ok && !r2.suppressed && !role.tombstones.has(e.msgId) && role.cacheIds.has(e.msgId), JSON.stringify(r2));
}
{
  const clock = { t: T0 }; const am = harness(clock, { armed: false }); const role = am._becomeRoot(topicBig, 'fence');
  const { e2, killTs } = await killed(clock, am, role); role.isRoot = false;
  am.mySubscriptions.set(topicBig, { since: 0, interval: 5000 });
  const seen = []; am._deliveryCallback = (t, json, msgId) => seen.push(msgId);
  role.merge ??= {}; role.merge.gate = { staged: false };                       // a gated role: _cachePush refuses
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'a', msgs: [{ msgId: e2.msgId, json: JSON.stringify(e2), publishTs: killTs + 1, seq: 9 }] }, { targetId: 10n });
  check('D3b: a refused cache write on DELIVER keeps the tombstone and delivers nothing', role.tombstones.has(e2.msgId) && !role.cacheIds.has(e2.msgId) && !seen.includes(e2.msgId), JSON.stringify(seen));
}
async function proofless({ armed }) {
  const clock = { t: T0 }; const am = harness(clock, { armed }); const role = am._becomeRoot(topicBig, 'fence');
  const msg = body();
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 });
  role.tombstones.set(e1.msgId, { exp: clock.t + 600_000, killTs: clock.t + 1000, signer: alice.pubkeyHex.toLowerCase(), seq: 2 });   // migrated: no authorTs, no proof
  return { am, role, e1, killTs: clock.t + 1000 };
}
{
  const { am, role, e1, killTs } = await proofless({ armed: true });
  const r = await am._ingestStamped(role, { msgId: e1.msgId, json: JSON.stringify(e1), publishTs: killTs + 5000, seq: 9 }, am._cutCapture(role));
  check('B1: proof-less, REPLAYUP of the signed pre-kill body with a later stamp stays dead (reported unvouched, not held)', r === 'unvouched' && role.tombstones.has(e1.msgId) && !role.cacheIds.has(e1.msgId), String(r));
}
{
  const { am, role, e1, killTs } = await proofless({ armed: false }); role.isRoot = false;
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'a', msgs: [{ msgId: e1.msgId, json: JSON.stringify(e1), publishTs: killTs + 5000, seq: 9 }] }, { targetId: 10n });
  check('B2: proof-less, unarmed DELIVER of it stays dead', role.tombstones.has(e1.msgId) && !role.cacheIds.has(e1.msgId));
}
{
  const { am, role, e1, killTs } = await proofless({ armed: true }); role.isRoot = false;
  await am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'd', gen: { root: ROOT_HEX, epoch: 1 }, msgs: [{ msgId: e1.msgId, json: JSON.stringify(e1), publishTs: killTs + 5000, seq: 9 }] }, { targetId: 10n });
  check('B3: proof-less, armed data-only DELIVER of it stays dead', role.tombstones.has(e1.msgId) && !role.cacheIds.has(e1.msgId));
}
{
  const { am, role, e1, killTs } = await proofless({ armed: true });
  const t = await am._ingestStampedBatch(role, [{ msgId: e1.msgId, json: JSON.stringify(e1), publishTs: killTs + 5000, seq: 9 }], am._cutCapture(role));
  check('P2: an unvouched newer body is counted rejected, not held', t && t.held === 0 && t.rejected === 1 && role.tombstones.has(e1.msgId), JSON.stringify(t));
}
{
  const clock = { t: T0 }; const am = harness(clock); const role = am._becomeRoot(topicBig, 'fence');
  const msg = body();
  const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t + 2000, seq: 3 });
  const proof = await buildKill({ topicId: topicHex, msgId: e2.msgId, identity: alice, ts: clock.t + 1000, seq: 2 });
  const tomb = { exp: clock.t + 600_000, killTs: clock.t + 1000, signer: alice.pubkeyHex.toLowerCase(), seq: 2, kill: proof };   // proof-bearing, no authorTs
  role.tombstones.set(e2.msgId, tomb);
  const v = am._tombstoneVerdict(role, e2.msgId, JSON.stringify(e2), clock.t + 1500);
  const committed = v?.kind === 'supersede' && am._tombstoneCommit(role, e2.msgId, v);
  const sup = role.superseded?.get(e2.msgId);
  check('P4: a proof-only tombstone records the proof (ts, seq) when superseded', committed && sup?.authorTs === proof.ts && sup?.authorSeq === proof.seq, JSON.stringify({ kind: v?.kind, sup }));
}
{
  const { DurabilityLedger } = await import('../src/pubsub/durability.js');
  let now = T0; const led = new DurabilityLedger({ now: () => now });
  const id = 'ab'.repeat(32);
  led.open(id, topicBig);
  led._m.get(id).state = 'verified';                                   // the killed copy's obligation ended
  led.open(id, topicBig);
  check('P5: a republished msgId re-opens a pending durability obligation', led._m.get(id)?.state === 'pending');
  led.open(id, topicBig); const at = led._m.get(id).at; now += 1000; led.open(id, topicBig);
  check('P5 CONTROL: a still-pending obligation is kept, not reset', led._m.get(id).at === at);
}
console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
