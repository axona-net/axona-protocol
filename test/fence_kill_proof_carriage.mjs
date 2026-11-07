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
//   C8c the held-proof shortcut also binds the carrier signer (Aster fbfd8256)
//   C8d a forged claim is stripped, never recorded
//   C9  a proof-less tombstone is upgraded when a verified proof arrives
//   C10 killProofs:false carries no proof (proof-less, fail closed)
//   KP1 another author's VALID kill never re-attributes a tombstone: it is recorded
//       as a CLAIM; the arriving body's author decides (Orion 28a6ac06). The real
//       author's pre-kill body stays dead, and under a poisoned signer the real
//       author's claim still authorizes the kill (the S2 guarantee)
//   KP2 a merge winner holding the republication ignores the loser's copy of the
//       superseded kill re-stamped above it (no splice, body counted held)
//   KP3 the superseded kill never attaches to a later proof-less kill2 tombstone,
//       so it cannot rewind that tombstone's author time
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
  check('C8c: the identical held proof under a different carrier signer is refused (Aster fbfd8256)', (await V.am._verifyKillProof(vr, topicBig, { ...good, signer: bob.pubkeyHex.toLowerCase() })) === null);
  check('C8b CONTROL: the identical proof is vouched for from the held copy', (await V.am._verifyKillProof(vr, topicBig, good)) === vr.tombstones.get(good.msgId).kill);
  // C8d: a forged claim (altered ts under a copied signature) is stripped, never recorded
  const F = node(23n, clock); const fr = F.am._becomeRoot(topicBig, 'fence'); fr.isRoot = false;
  const bobK = await buildKill({ topicId: topicHex, msgId: good.msgId, identity: bob, ts: good.kill.ts, seq: 3 });
  await F.am._applyDels(fr, topicBig, [{ ...good, claims: [{ ...bobK, ts: bobK.ts + 77 }] }]);
  check('C8d: a forged claim is stripped, never recorded', fr.tombstones.has(good.msgId) && !fr.tombstones.get(good.msgId).claims?.size);
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
{
  // KP1 — signer hijack
  const clock = { t: T0 }; const s = await scene(clock);
  const good = s.dels[0]; const { kill: _k, ...bare } = good;
  const N = node(18n, clock); const nr = N.am._becomeRoot(topicBig, 'fence'); nr.isRoot = false;
  await N.am._applyDels(nr, topicBig, [bare]);                               // proof-less tombstone, signer alice
  const bobKill = await buildKill({ topicId: topicHex, msgId: good.msgId, identity: bob, ts: good.kill.ts + 1, seq: 7 });
  const hijack = { ...bare, signer: bob.pubkeyHex.toLowerCase(), kill: bobKill };
  await N.am._applyDels(nr, topicBig, [hijack]);
  const t1 = nr.tombstones.get(good.msgId);
  check('KP1: a foreign valid kill becomes a CLAIM, never the tombstone\'s signer or proof', t1 && t1.signer === alice.pubkeyHex.toLowerCase() && !t1.kill && t1.claims?.get(bob.pubkeyHex.toLowerCase())?.signature === bobKill.signature, JSON.stringify({ signer: t1?.signer?.slice(0, 8), kill: !!t1?.kill, claims: t1?.claims?.size }));
  const r = await N.am._ingestStamped(nr, { msgId: s.e1.msgId, json: JSON.stringify(s.e1), publishTs: s.killTs - 1, seq: 1 }, N.am._cutCapture(nr));
  check('KP1: the real author\'s pre-kill body stays dead (no revoke)', nr.tombstones.has(s.e1.msgId) && !nr.cacheIds.has(s.e1.msgId), String(r));
  // at a root: a direct KILL from bob on alice's tombstone
  const Rt = node(19n, clock); const rr = Rt.am._becomeRoot(topicBig, 'fence');
  await Rt.am._applyDels(rr, topicBig, [bare]);
  await Rt.am._onKill({ topicId: topicHex, kill: bobKill }, { targetId: 19n, fromId: 20n, isTerminal: true });
  const rt = rr.tombstones.get(good.msgId);
  check('KP1: at a root another signer\'s kill is recorded only as a claim', rt?.signer === alice.pubkeyHex.toLowerCase() && !rt?.kill && rt?.claims?.has(bob.pubkeyHex.toLowerCase()));
  const r2 = await Rt.am._ingestStamped(rr, { msgId: s.e1.msgId, json: JSON.stringify(s.e1), publishTs: s.killTs - 1, seq: 1 }, Rt.am._cutCapture(rr));
  check('KP1: ...and alice\'s pre-kill body stays dead there too', rr.tombstones.has(s.e1.msgId) && !rr.cacheIds.has(s.e1.msgId), String(r2));
  // a claim decides for its own author: a body BY BOB under bob's claim is suppressed, a body by a third key revokes
  const carol = await createAuthorIdentity();
  const eCarol = await buildEnvelope({ topic: TOPIC, message: 'carol', identity: carol, ts: clock.t, seq: 1 });
  const P = node(22n, clock); const pr = P.am._becomeRoot(topicBig, 'fence'); pr.isRoot = false;
  pr.tombstones.set(eCarol.msgId, { exp: clock.t + 600_000, killTs: clock.t + 10, signer: alice.pubkeyHex.toLowerCase(), seq: 1 });
  const carolKill = await buildKill({ topicId: topicHex, msgId: eCarol.msgId, identity: carol, ts: clock.t + 5, seq: 2 });
  await P.am._applyDels(pr, topicBig, [{ del: true, msgId: eCarol.msgId, killTs: clock.t + 10, seq: 1, signer: carol.pubkeyHex.toLowerCase(), kill: carolKill }]);
  const v = P.am._tombstoneVerdict(pr, eCarol.msgId, JSON.stringify(eCarol));
  check('KP1 S2: a poisoned signer cannot revive the real author\'s killed body (her claim authorizes the kill)', v?.kind === 'suppress', JSON.stringify(v?.kind));
}
{
  // KP2 — merge winner keeps the republication against the loser's re-stamped superseded kill
  const clock = { t: T0 }; const s = await scene(clock);
  const W = s.R; const wrole = s.role;
  check('KP2: the winner recorded the superseded kill', wrole.superseded?.get(s.e2.msgId)?.authorTs === s.k1.ts);
  const loserDel = { ...s.dels[0], killTs: s.stamped.publishTs + 500, publishTs: s.stamped.publishTs + 500, seq: 99 };
  await W.am._applyDels(wrole, topicBig, [loserDel]);
  check('KP2: the winner keeps the republication and installs no tombstone', wrole.cacheIds.has(s.e2.msgId) && !wrole.tombstones.has(s.e2.msgId));
}
{
  // KP3 — superseded kill1 cannot downgrade a proof-less kill2 tombstone
  const clock = { t: T0 }; const s = await scene(clock);
  const K = node(21n, clock); const kr = K.am._becomeRoot(topicBig, 'fence'); kr.isRoot = false;
  await K.am._applyDels(kr, topicBig, s.dels);                                 // kill1 with proof
  await K.am._ingestStampedBatch(kr, [s.stamped], K.am._cutCapture(kr));      // pub2 supersedes kill1 (records superseded)
  const kill2Ts = s.stamped.publishTs + 1000;
  kr.cache = kr.cache.filter(c => c.msgId !== s.e2.msgId); kr.cacheIds.delete(s.e2.msgId);
  kr.tombstones.set(s.e2.msgId, { exp: clock.t + 600_000, killTs: kill2Ts, signer: alice.pubkeyHex.toLowerCase(), seq: 5 });   // proof-less kill2 (e.g. via a 4.130 hop)
  await K.am._applyDels(kr, topicBig, s.dels);                                 // the old kill1 proof arrives again
  check('KP3: the superseded kill1 does not attach to the kill2 tombstone', kr.tombstones.has(s.e2.msgId) && !kr.tombstones.get(s.e2.msgId).kill);
  const r = await K.am._ingestStamped(kr, { ...s.stamped, publishTs: kill2Ts + 1 }, K.am._cutCapture(kr));
  check('KP3: killed pub2 replayed with a later stamp stays dead', kr.tombstones.has(s.e2.msgId) && !kr.cacheIds.has(s.e2.msgId), String(r));
}
console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
