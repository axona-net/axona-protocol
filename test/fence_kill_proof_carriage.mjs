// fence_kill_proof_carriage.mjs — 4.131.0 signed-kill proof carriage (David
// 2026-11-06 "build the proof carriage"; Orion e862068e P1-P3, Aster d165d9ce).
// With the kernel DEFAULTS, every holder of a tombstone holds the author's
// signed kill, so it decides a republication exactly as the root that verified it.
//
//   C1  the root retains the verified proof by default
//   C2  REPLICATE carries it: a warm backup's tombstone holds the proof
//   C3  P1: the backup stores an accepted republication sent by REPLICATE
//   C4  ...and a validly signed PRE-kill copy with a forged later stamp stays dead
//   C5  P2: a HANDOFF heir holds the republication and acks held === sent
//   C6  promoted-backup parity: the backup, as root, accepts a fresh republication
//   C7  a relay learns the proof from a fan-out del and decides the same way
//   C8  hostile proofs are stripped: copied signature over an altered ts, another
//       msgId, another topic, a carrier signer that does not match
//   C8b the held-proof shortcut refuses an altered copy carrying the same signature
//   C9  a proof-less tombstone is upgraded when a verified proof arrives
//   C10 killProofs:false carries no proof (proof-less, fail closed)
//
// Run: node test/fence_kill_proof_carriage.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill, verifyKill } from '../src/pubsub/kill.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const TOPIC = { region: 'useast', owner: null, name: 'kill-proof-carriage', write: 'open' };
const OTHER = { region: 'useast', owner: null, name: 'kill-proof-carriage-other', write: 'open' };
const topicHex = await deriveTopicId(TOPIC), topicBig = await deriveTopicIdBig(TOPIC), otherHex = await deriveTopicId(OTHER);
const alice = await createAuthorIdentity(), bob = await createAuthorIdentity();
const T0 = 1_700_000_000_000;
const node = (self, clock, opts = {}) => {
  const routed = [];
  const raw = { getSelfId: () => self, onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
    sendDirect: async () => true, verdictsSupported: true, routeMessage: async (t, type, payload) => { routed.push({ type, payload }); return { consumed: true }; }, findKClosest: async () => [] };
  const am = new AxonaManager({ dht: sealTestDht(raw), now: () => clock.t, rootReplicas: 0, ...opts });
  return { am, routed };
};
const meta = { targetId: 10n, fromId: 20n, isTerminal: true };
let n = 0; const body = () => `{"fire":${++n}}`;
// A root that published, killed and accepted a republication. Returns everything a peer would receive.
async function scene(clock, opts = {}) {
  const R = node(10n, clock, opts); const role = R.am._becomeRoot(topicBig, 'fence');
  const msg = body();
  const e1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 }); await R.am._ingestPublish(role, JSON.stringify(e1));
  clock.t += 1000; const k1 = await buildKill({ topicId: topicHex, msgId: e1.msgId, identity: alice, ts: clock.t, seq: 2 });
  await R.am._onKill({ topicId: topicHex, kill: k1 }, meta);
  const dels = R.am._activeDels(role);
  clock.t += 1000; const e2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 3 });
  const r2 = await R.am._ingestPublish(role, JSON.stringify(e2));
  const held = role.cache.find(c => c.msgId === e2.msgId);
  return { R, role, e1, e2, k1, dels, r2, stamped: held && { msgId: held.msgId, json: held.json, publishTs: held.publishTs, seq: held.seq }, killTs: dels[0]?.killTs };
}

console.log('fence_kill_proof_carriage');
{
  const clock = { t: T0 }; const s = await scene(clock);
  const tomb = s.dels[0];
  check('C1: the root retains the verified proof by default', !!tomb?.kill && (await verifyKill(tomb.kill)).ok === true && tomb.kill.ts === s.k1.ts);
  check('C1: the republication is accepted at the root', s.r2.ok && !s.r2.suppressed && !!s.stamped, JSON.stringify(s.r2));
  // C2-C4: a warm backup fed by REPLICATE (dels first, then bodies, as _syncIngest does)
  const B = node(11n, clock); const brole = B.am._becomeRoot(topicBig, 'fence'); brole.isRoot = false;
  await B.am._applyDels(brole, topicBig, s.dels);
  check('C2: the backup tombstone holds the proof', brole.tombstones.get(s.e1.msgId)?.kill?.ts === s.k1.ts);
  const t = await B.am._ingestStampedBatch(brole, [s.stamped], B.am._cutCapture(brole));
  check('C3 (P1): the backup stores the accepted republication', t.held === 1 && brole.cacheIds.has(s.e2.msgId) && !brole.tombstones.has(s.e2.msgId), JSON.stringify(t));
  const B2 = node(12n, clock); const b2 = B2.am._becomeRoot(topicBig, 'fence'); b2.isRoot = false;
  await B2.am._applyDels(b2, topicBig, s.dels);
  const r = await B2.am._ingestStamped(b2, { msgId: s.e1.msgId, json: JSON.stringify(s.e1), publishTs: s.killTs + 5000, seq: 9 }, B2.am._cutCapture(b2));
  check('C4: a signed pre-kill copy with a forged later stamp stays dead', r === 'held' && b2.tombstones.has(s.e1.msgId) && !b2.cacheIds.has(s.e1.msgId), String(r));
  // C6: promoted-backup parity
  b2.isRoot = true;
  const fresh = await B2.am._ingestPublish(b2, JSON.stringify(s.e2));
  check('C6: a promoted backup accepts a fresh republication', fresh.ok && !fresh.suppressed && b2.cacheIds.has(s.e2.msgId), JSON.stringify(fresh));
}
{
  // C5 (P2): HANDOFF heir
  const clock = { t: T0 }; const s = await scene(clock);
  const H = node(13n, clock); const leaverHex = '0'.repeat(64) + '0a';
  await H.am._syncIngest({ topicId: topicHex, from: leaverHex, msgs: [s.stamped], dels: s.dels }, { targetId: 13n }, 'HANDOFF');
  const hrole = H.am.axonRoles.get(topicBig);
  const ack = H.routed.find(x => x.type === 'pubsub:handoffack');
  check('C5 (P2): the heir holds the republication', !!hrole && hrole.cacheIds.has(s.e2.msgId));
  check('C5 (P2): HANDOFFACK held === sent === 1', ack?.payload?.held === 1 && ack?.payload?.sent === 1, JSON.stringify(ack?.payload));
}
{
  // C7: a relay learns the proof from a fan-out del (unarmed here: the author clock decides, no stamp trust needed)
  const clock = { t: T0 }; const s = await scene(clock);
  const L = node(14n, clock, { topicLifetime: false }); const lrole = L.am._becomeRoot(topicBig, 'fence'); lrole.isRoot = false;
  const from = '0'.repeat(65) + 'a';
  await L.am._onDeliver({ topicId: topicHex, from, msgs: [s.dels[0]] }, { targetId: 14n });
  check('C7: the relay tombstone holds the proof', lrole.tombstones.get(s.e1.msgId)?.kill?.ts === s.k1.ts);
  await L.am._onDeliver({ topicId: topicHex, from, msgs: [{ msgId: s.e1.msgId, json: JSON.stringify(s.e1), publishTs: s.killTs + 5000, seq: 9 }] }, { targetId: 14n });
  check('C7: the forged-stamp pre-kill copy stays dead at the relay', lrole.tombstones.has(s.e1.msgId) && !lrole.cacheIds.has(s.e1.msgId));
  await L.am._onDeliver({ topicId: topicHex, from, msgs: [s.stamped] }, { targetId: 14n });
  check('C7: the republication supersedes at the relay', lrole.cacheIds.has(s.e2.msgId) && !lrole.tombstones.has(s.e2.msgId));
}
{
  // C8: hostile proofs
  const clock = { t: T0 }; const s = await scene(clock);
  const good = s.dels[0];
  const cases = [
    ['copied signature over an altered ts', { ...good, kill: { ...good.kill, ts: good.kill.ts - 999_999 } }],
    ['a proof naming another msgId', { ...good, kill: await buildKill({ topicId: topicHex, msgId: 'cd'.repeat(32), identity: alice, ts: good.kill.ts, seq: 2 }) }],
    ['a proof bound to another topic', { ...good, kill: await buildKill({ topicId: otherHex, msgId: good.msgId, identity: alice, ts: good.kill.ts, seq: 2 }) }],
    ['a carrier signer that does not match the proof', { ...good, signer: bob.pubkeyHex.toLowerCase() }],
  ];
  for (const [label, d] of cases) {
    const X = node(15n, clock); const xr = X.am._becomeRoot(topicBig, 'fence'); xr.isRoot = false;
    await X.am._applyDels(xr, topicBig, [d]);
    check(`C8: stripped — ${label}`, xr.tombstones.has(good.msgId) && !xr.tombstones.get(good.msgId).kill);
  }
  // C8b: the held-proof shortcut never vouches for an altered copy of a held proof
  const V = node(17n, clock); const vr = V.am._becomeRoot(topicBig, 'fence'); vr.isRoot = false;
  await V.am._applyDels(vr, topicBig, [good]);
  const altered = { ...good, kill: { ...good.kill, ts: good.kill.ts + 123 } };
  check('C8b: a node holding the proof refuses an altered copy with the same signature', vr.tombstones.get(good.msgId)?.kill && (await V.am._verifyKillProof(vr, topicBig, altered)) === null);
  check('C8b CONTROL: the identical proof is vouched for from the held copy', (await V.am._verifyKillProof(vr, topicBig, good)) === vr.tombstones.get(good.msgId).kill);
  // C9: upgrade a proof-less tombstone
  const U = node(16n, clock); const ur = U.am._becomeRoot(topicBig, 'fence'); ur.isRoot = false;
  const { kill, ...bare } = good;
  await U.am._applyDels(ur, topicBig, [bare]);
  const before = !!ur.tombstones.get(good.msgId)?.kill;
  await U.am._applyDels(ur, topicBig, [good]);
  check('C9: a proof-less tombstone is upgraded by a verified proof', before === false && ur.tombstones.get(good.msgId)?.kill?.ts === s.k1.ts);
}
{
  // C10: off switch
  const clock = { t: T0 }; const s = await scene(clock, { killProofs: false });
  check('C10: killProofs:false carries no proof', s.dels.length === 1 && !s.dels[0].kill);
}
console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
