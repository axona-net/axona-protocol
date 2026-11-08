// fence_kill_claims_state.mjs — 4.131.0, Orion 37d2a5c0 (CL-1..CL-4, TR-1) on the
// claims model: a tombstone's state ADVANCES with its author's newer kills, its
// claims travel every hop, every carried proof obeys the superseded rule and the
// held body's authorship, a claim refused at the cap is never acked, and the
// bridge hello has no await between its liveness check and 'open'.
//
//   CL1a  root: the same author's kill2, arriving before pub2, advances the tombstone
//         (fresh stamp, authorTs = kill2), so pub2 then stays dead; kill2 is acked
//   CL1b  a backup holding kill1 advances to kill2 (proof, stamp); a delayed pub2 stays dead
//   CL1c  a claimant's newer kill advances its claim
//   CL2   claims survive two hops (install fan-out and upgrade fan-out carry them)
//   CL3a  a merge winner holding pub2 ignores the superseded kill carried as a CLAIM
//   CL3b  a foreign signer's deletion of a held body is refused
//   CL4   a claim refused at the cap is NOT acked; a verified body suppressed through
//         a claim promotes it (signer + proof) and clears the other claims
//   TR1   onBridgeAuthHello: no await between the late-hello check and 'open'
//   APPa  a subscriber that joined between kill1 and pub2 still gets kill2 (Orion RAK-APP)
//   APPb  a reordered kill2 ahead of pub2 advances the app's kill record; pub2 stays retracted
//   APPc  the CL-1b advance confirms and reaches a subscribed holder's app
//   CL5a  a promoted claim takes a stamp past the body and is re-fanned
//   CL5b  a claim-bearing deletion from the body's author applies at a body holder even with a low stamp
//   CL6   a re-stamped pre-republish kill1 cannot retract a held pub2 where no superseded record
//         exists (handoff heir): not by REPLAYUP, not by a direct KILL
// (Orion d2a8e83e)
//
// Run: node test/fence_kill_claims_state.mjs
import { readFileSync } from 'node:fs';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const TOPIC = { region: 'useast', owner: null, name: 'kill-claims-state', write: 'open' };
const topicHex = await deriveTopicId(TOPIC), topicBig = await deriveTopicIdBig(TOPIC);
const alice = await createAuthorIdentity(), bob = await createAuthorIdentity();
const T0 = 1_700_000_000_000;
const node = (self, clock, opts = {}) => {
  const routed = [];
  const raw = { getSelfId: () => self, onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
    sendDirect: async () => true, verdictsSupported: true, routeMessage: async (t, type, payload) => { routed.push({ type, payload }); return { consumed: true }; }, findKClosest: async () => [] };
  return { am: new AxonaManager({ dht: sealTestDht(raw), now: () => clock.t, rootReplicas: 0, topicLifetime: false, ...opts }), routed };
};
const meta = (self) => ({ targetId: self, fromId: 20n, isTerminal: true });
const A = alice.pubkeyHex.toLowerCase();
let n = 0; const body = () => `{"cl":${++n}}`;
const fanned = (routed) => routed.filter(x => x.type === 'pubsub:deliver').flatMap(x => x.payload.msgs || []).filter(m => m.del);

console.log('fence_kill_claims_state');
{ // CL1a — root, kill2 before pub2
  const clock = { t: T0 }; const R = node(10n, clock); const role = R.am._becomeRoot(topicBig, 'f');
  const msg = body();
  const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 }); await R.am._ingestPublish(role, JSON.stringify(p1));
  clock.t += 1000; await R.am._onKill({ topicId: topicHex, kill: await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: clock.t, seq: 2 }) }, meta(10n));
  clock.t += 1000; const p2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 3 });
  clock.t += 1000; const k2 = await buildKill({ topicId: topicHex, msgId: p2.msgId, identity: alice, ts: clock.t, seq: 4 });
  const acksBefore = R.routed.filter(x => x.type === 'pubsub:ingestack').length;
  const oldStamp = role.tombstones.get(p1.msgId)?.killTs, oldSeq = role.seq;
  await R.am._onKill({ topicId: topicHex, kill: k2 }, meta(10n));
  check('CL1a: kill2 takes a FRESH root stamp and seq slot (not the duplicate path)', role.tombstones.get(p2.msgId)?.killTs > oldStamp && role.seq === oldSeq + 1, JSON.stringify({ oldStamp, now: role.tombstones.get(p2.msgId)?.killTs, seq: role.seq, oldSeq }));
  const t = role.tombstones.get(p2.msgId);
  check('CL1a: kill2 advances the root tombstone (authorTs = kill2) and is acked', t?.authorTs === k2.ts && t?.kill?.ts === k2.ts && R.routed.filter(x => x.type === 'pubsub:ingestack').length === acksBefore + 1, JSON.stringify({ authorTs: t?.authorTs }));
  const r = await R.am._ingestPublish(role, JSON.stringify(p2));
  check('CL1a: the killed pub2 arriving after kill2 stays dead', r.suppressed === true && !role.cacheIds.has(p2.msgId), JSON.stringify(r));
}
{ // CL1b — backup advances; CL1c — claim advances
  const clock = { t: T0 }; const B = node(11n, clock); const b = B.am._becomeRoot(topicBig, 'f'); b.isRoot = false;
  const msg = body();
  const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 });
  const k1 = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: clock.t + 1000, seq: 2 });
  await B.am._applyDels(b, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 2000, publishTs: T0 + 2000, seq: 2, signer: A, kill: k1 }]);
  const p2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t + 3000, seq: 3 });
  const k2 = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: clock.t + 4000, seq: 4 });
  await B.am._applyDels(b, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 5000, publishTs: T0 + 5000, seq: 5, signer: A, kill: k2 }]);
  const t = b.tombstones.get(p1.msgId);
  check('CL1b: the backup advances to kill2 (proof and stamp)', t?.kill?.ts === k2.ts && t?.killTs === T0 + 5000, JSON.stringify({ kts: t?.kill?.ts, killTs: t?.killTs }));
  const r = await B.am._ingestStamped(b, { msgId: p2.msgId, json: JSON.stringify(p2), publishTs: T0 + 4500, seq: 3 }, B.am._cutCapture(b));
  check('CL1b: a delayed copy of the killed pub2 stays dead at the backup', b.tombstones.has(p2.msgId) && !b.cacheIds.has(p2.msgId), String(r));
  // CL1c
  const C = node(12n, clock); const c = C.am._becomeRoot(topicBig, 'f'); c.isRoot = false;
  c.tombstones.set(p1.msgId, { exp: T0 + 600_000, killTs: T0 + 2000, signer: bob.pubkeyHex.toLowerCase(), seq: 2 });
  await C.am._applyDels(c, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 2000, seq: 2, signer: bob.pubkeyHex.toLowerCase(), claims: [k1] }]);
  await C.am._applyDels(c, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 2000, seq: 2, signer: bob.pubkeyHex.toLowerCase(), claims: [k2] }]);
  check('CL1c: a claimant\'s newer kill advances its claim', c.tombstones.get(p1.msgId)?.claims?.get(A)?.ts === k2.ts);
}
{ // CL2 — claims survive two hops
  const clock = { t: T0 }; const msg = body();
  const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 });
  const kA = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: clock.t + 1000, seq: 2 });
  const hop1 = node(13n, clock); const h1 = hop1.am._becomeRoot(topicBig, 'f'); h1.isRoot = false;
  h1.subscribers.set('0'.repeat(65) + 'e', { lastSeen: clock.t });
  await hop1.am._applyDels(h1, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 2000, publishTs: T0 + 2000, seq: 2, signer: bob.pubkeyHex.toLowerCase(), claims: [kA] }]);
  const out = fanned(hop1.routed).find(m => m.msgId === p1.msgId);
  check('CL2: the install fan-out carries the claim', !!out && Array.isArray(out.claims) && out.claims.some(k => k.signature === kA.signature), JSON.stringify(out && Object.keys(out)));
  const hop2 = node(14n, clock, { topicLifetime: false }); const h2 = hop2.am._becomeRoot(topicBig, 'f'); h2.isRoot = false;
  await hop2.am._onDeliver({ topicId: topicHex, from: '0'.repeat(65) + 'a', msgs: [out] }, { targetId: 14n });
  check('CL2: the second hop holds the claim', h2.tombstones.get(p1.msgId)?.claims?.get(A)?.signature === kA.signature);
  const r = await hop2.am._ingestStamped(h2, { msgId: p1.msgId, json: JSON.stringify(p1), publishTs: T0 + 1, seq: 1 }, hop2.am._cutCapture(h2));
  check('CL2: alice\'s killed body stays dead two hops away', h2.tombstones.has(p1.msgId) && !h2.cacheIds.has(p1.msgId), String(r));
}
{ // CL3a — merge winner, superseded kill carried as a claim; CL3b — foreign deletion of a held body
  const clock = { t: T0 }; const W = node(15n, clock); const w = W.am._becomeRoot(topicBig, 'f'); const msg = body();
  const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 1 }); await W.am._ingestPublish(w, JSON.stringify(p1));
  clock.t += 1000; const k1 = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: clock.t, seq: 2 });
  await W.am._onKill({ topicId: topicHex, kill: k1 }, meta(15n));
  clock.t += 1000; const p2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: clock.t, seq: 3 }); await W.am._ingestPublish(w, JSON.stringify(p2));
  check('CL3a: the superseded record names the republication\'s author', w.superseded?.get(p2.msgId)?.signer === A);
  await W.am._applyDels(w, topicBig, [{ del: true, msgId: p2.msgId, killTs: clock.t + 500, publishTs: clock.t + 500, seq: 9, signer: bob.pubkeyHex.toLowerCase(), claims: [k1] }]);
  check('CL3a: the winner keeps pub2 against the superseded kill carried as a claim', w.cacheIds.has(p2.msgId) && !w.tombstones.has(p2.msgId));
  const bobKill = await buildKill({ topicId: topicHex, msgId: p2.msgId, identity: bob, ts: clock.t + 600, seq: 1 });
  await W.am._applyDels(w, topicBig, [{ del: true, msgId: p2.msgId, killTs: clock.t + 600, publishTs: clock.t + 600, seq: 10, signer: bob.pubkeyHex.toLowerCase(), kill: bobKill }]);
  check('CL3b: a foreign signer\'s deletion of a held body is refused', w.cacheIds.has(p2.msgId) && !w.tombstones.has(p2.msgId));
  clock.t += 1000; const k2 = await buildKill({ topicId: topicHex, msgId: p2.msgId, identity: alice, ts: clock.t, seq: 4 });
  await W.am._applyDels(w, topicBig, [{ del: true, msgId: p2.msgId, killTs: clock.t, publishTs: clock.t, seq: 11, signer: A, kill: k2 }]);
  check('CL3 CONTROL: the author\'s NEWER kill of the held body still retracts it', !w.cacheIds.has(p2.msgId) && w.tombstones.has(p2.msgId));
}
{ // CL4 — Sybil cap, no false ack; promotion on a verified suppressed body
  const clock = { t: T0 }; const R = node(16n, clock); const role = R.am._becomeRoot(topicBig, 'f');
  const p1 = await buildEnvelope({ topic: TOPIC, message: body(), identity: alice, ts: clock.t, seq: 1 });
  for (let i = 0; i < 5; i++) {
    const s = await createAuthorIdentity();
    await R.am._onKill({ topicId: topicHex, kill: await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: s, ts: clock.t + i, seq: 1 }) }, meta(16n));
  }
  const t = role.tombstones.get(p1.msgId);
  check('CL4: five Sybil kills fill signer + 4 claims', !!t && t.claims?.size === 4);
  const acks0 = R.routed.filter(x => x.type === 'pubsub:ingestack').length;
  const kA = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: clock.t + 10, seq: 2 });
  await R.am._onKill({ topicId: topicHex, kill: kA }, meta(16n));
  check('CL4: alice\'s kill refused at the cap is NOT acked', R.routed.filter(x => x.type === 'pubsub:ingestack').length === acks0 && !t.claims.has(A));
  // promotion: a tombstone holding alice's claim suppresses her verified body and promotes the claim
  const P = node(17n, clock); const pr = P.am._becomeRoot(topicBig, 'f');
  const p3 = await buildEnvelope({ topic: TOPIC, message: body(), identity: alice, ts: clock.t, seq: 1 });
  const k3 = await buildKill({ topicId: topicHex, msgId: p3.msgId, identity: alice, ts: clock.t + 5, seq: 2 });
  const filler = []; for (let i = 0; i < 3; i++) { const s = await createAuthorIdentity(); filler.push(await buildKill({ topicId: topicHex, msgId: p3.msgId, identity: s, ts: clock.t + i, seq: 1 })); }
  const s0 = await createAuthorIdentity();
  pr.tombstones.set(p3.msgId, { exp: clock.t + 600_000, killTs: clock.t + 10, signer: s0.pubkeyHex.toLowerCase(), seq: 1, claims: new Map([[A, k3], ...filler.map(k => [k.signerPubkey.toLowerCase(), k])]) });
  const r = await P.am._ingestPublish(pr, JSON.stringify(p3));
  const pt = pr.tombstones.get(p3.msgId);
  check('CL4: a verified body suppressed through a claim stays dead', r.suppressed === true && !pr.cacheIds.has(p3.msgId), JSON.stringify(r));
  check('CL4: ...and promotes that claim (signer + proof) and clears the others', pt?.signer === A && pt?.kill?.signature === k3.signature && pt?.claims?.size === 0, JSON.stringify({ signer: pt?.signer?.slice(0, 8), claims: pt?.claims?.size }));
}
{ // TR1 — no await between the late-hello check and 'open'
  const src = readFileSync(new URL('../src/transport/web/index.js', import.meta.url), 'utf8');
  const i = src.indexOf("log('auth-bridge-late-hello-dropped'"); const j = src.indexOf("setBridgeState('open');", i);
  const seg = i > 0 && j > i ? src.slice(i, j) : '';
  check('TR1: no await between the late-hello check and setBridgeState(\'open\')', seg.length > 0 && !/\bawait\b/.test(seg), String(seg.length));
}
{ // APPa / APPb — app delivery state at a pure subscriber
  const clock = { t: T0 }; const S = node(30n, clock); S.am.mySubscriptions.set(topicBig, { since: 0, interval: 5000 });
  const seen = []; S.am._deliveryCallback = (t, json, msgId) => seen.push(JSON.parse(json).deleted ? 'del' : 'body');
  const from = '0'.repeat(65) + 'a', msg = body();
  const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0, seq: 1 });
  const p2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0 + 3000, seq: 3 });
  const dl = (m) => S.am._onDeliver({ topicId: topicHex, from, msgs: [m] }, { targetId: 30n });
  await dl({ del: true, msgId: p1.msgId, killTs: T0 + 2000, publishTs: T0 + 2000, signer: A, seq: 2 });   // joined after kill1: kill only
  await dl({ json: JSON.stringify(p2), msgId: p2.msgId, publishTs: T0 + 3000, seq: 3 });
  await dl({ del: true, msgId: p2.msgId, killTs: T0 + 4000, publishTs: T0 + 4000, signer: A, seq: 4 });
  check('APPa: a subscriber joined between kill1 and pub2 sees pub2 and then kill2', seen.join(',') === 'body,del', seen.join(','));
  const S2 = node(31n, clock); S2.am.mySubscriptions.set(topicBig, { since: 0, interval: 5000 });
  const seen2 = []; S2.am._deliveryCallback = (t, json) => seen2.push(JSON.parse(json).deleted ? 'del' : 'body');
  const dl2 = (m) => S2.am._onDeliver({ topicId: topicHex, from, msgs: [m] }, { targetId: 31n });
  await dl2({ json: JSON.stringify(p1), msgId: p1.msgId, publishTs: T0 + 1000, seq: 1 });
  await dl2({ del: true, msgId: p1.msgId, killTs: T0 + 2000, publishTs: T0 + 2000, signer: A, seq: 2 });
  await dl2({ del: true, msgId: p2.msgId, killTs: T0 + 4000, publishTs: T0 + 4000, signer: A, seq: 4 });   // kill2 reordered ahead of pub2
  await dl2({ json: JSON.stringify(p2), msgId: p2.msgId, publishTs: T0 + 3000, seq: 3 });
  check('APPb: a reordered kill2 keeps the delayed pub2 retracted', seen2.join(',') === 'body,del', seen2.join(','));
}
{ // APPc — CL-1b advance reaches a subscribed holder's app
  const clock = { t: T0 }; const H = node(32n, clock); const h = H.am._becomeRoot(topicBig, 'f'); h.isRoot = false;
  H.am.mySubscriptions.set(topicBig, { since: 0, interval: 5000 });
  const msg = body(); const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0, seq: 1 });
  const k1 = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: T0 + 1000, seq: 2 });
  const k2 = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: T0 + 4000, seq: 4 });
  await H.am._applyDels(h, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 2000, publishTs: T0 + 2000, seq: 2, signer: A, kill: k1 }]);
  await H.am._applyDels(h, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 5000, publishTs: T0 + 5000, seq: 5, signer: A, kill: k2 }]);
  check('APPc: the advanced kill moves the app\'s since-floor and kill record', H.am._sinceFor(topicBig) >= T0 + 5000 && H.am._appDelivered.get(topicBig.toString(16) + ':kill:' + p1.msgId) === T0 + 5000, String(H.am._sinceFor(topicBig)));
}
{ // CL5a — promotion advances the stamp and re-fans
  const clock = { t: T0 + 50_000 }; const P = node(33n, clock); const pr = P.am._becomeRoot(topicBig, 'f');
  pr.subscribers.set('0'.repeat(65) + 'e', { lastSeen: clock.t });
  const p = await buildEnvelope({ topic: TOPIC, message: body(), identity: alice, ts: T0, seq: 1 });
  const kA = await buildKill({ topicId: topicHex, msgId: p.msgId, identity: alice, ts: T0 + 5, seq: 2 });
  const s0 = await createAuthorIdentity();
  pr.tombstones.set(p.msgId, { exp: clock.t + 600_000, killTs: T0 + 1, signer: s0.pubkeyHex.toLowerCase(), seq: 1, claims: new Map([[A, kA]]) });
  await P.am._ingestPublish(pr, JSON.stringify(p));
  const t = pr.tombstones.get(p.msgId);
  const out = fanned(P.routed).find(m => m.msgId === p.msgId && m.kill);
  check('CL5a: the promoted claim takes a fresh stamp past the body and is re-fanned', t?.signer === A && t.killTs > T0 + 1 && !!out && out.killTs === t.killTs, JSON.stringify({ killTs: t?.killTs, fanned: !!out }));
}
{ // CL5b — claim-bearing deletion from the author applies at a body holder despite a low stamp
  const clock = { t: T0 }; const R2 = node(34n, clock); const r2 = R2.am._becomeRoot(topicBig, 'f');
  const p = await buildEnvelope({ topic: TOPIC, message: body(), identity: alice, ts: T0 + 1000, seq: 1 });
  clock.t = T0 + 1000; await R2.am._ingestPublish(r2, JSON.stringify(p));
  const kA = await buildKill({ topicId: topicHex, msgId: p.msgId, identity: alice, ts: T0 + 2000, seq: 2 });
  const mal = await createAuthorIdentity();
  clock.t = T0 + 3000;
  await R2.am._applyDels(r2, topicBig, [{ del: true, msgId: p.msgId, killTs: T0 + 1, publishTs: T0 + 1, seq: 1, signer: mal.pubkeyHex.toLowerCase(), claims: [kA] }]);
  const t = r2.tombstones.get(p.msgId);
  check('CL5b: the author\'s claim retracts the held body although the carrier stamp is below it', !r2.cacheIds.has(p.msgId) && t?.signer === A && t?.kill?.signature === kA.signature && t.killTs > T0 + 1000, JSON.stringify({ cached: r2.cacheIds.has(p.msgId), killTs: t?.killTs }));
}
{ // CL6 — handoff heir without a superseded record
  const clock = { t: T0 }; const msg = body();
  const p1 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0, seq: 1 });
  const k1 = await buildKill({ topicId: topicHex, msgId: p1.msgId, identity: alice, ts: T0 + 1000, seq: 2 });
  const p2 = await buildEnvelope({ topic: TOPIC, message: msg, identity: alice, ts: T0 + 2000, seq: 3 });
  const Hh = node(35n, clock); const leaver = '0'.repeat(64) + '0b';
  clock.t = T0 + 2500;
  await Hh.am._syncIngest({ topicId: topicHex, from: leaver, msgs: [{ msgId: p2.msgId, json: JSON.stringify(p2), publishTs: T0 + 2500, seq: 3 }], dels: [] }, { targetId: 35n }, 'HANDOFF');
  const hr = Hh.am.axonRoles.get(topicBig);
  check('CL6: the heir holds pub2 and no superseded record', !!hr && hr.cacheIds.has(p2.msgId) && !hr.superseded?.size);
  await Hh.am._applyDels(hr, topicBig, [{ del: true, msgId: p1.msgId, killTs: T0 + 9000, publishTs: T0 + 9000, seq: 9, signer: A, kill: k1 }]);
  check('CL6: a re-stamped kill1 by REPLAYUP cannot retract the held pub2', hr.cacheIds.has(p2.msgId) && !hr.tombstones.has(p2.msgId));
  hr.isRoot = true;
  const seq0 = hr.seq, last0 = hr.lastTs;
  await Hh.am._onKill({ topicId: topicHex, kill: k1 }, meta(35n));
  check('CL6: nor can a re-sent kill1 by direct KILL at the heir as root', hr.cacheIds.has(p2.msgId) && !hr.tombstones.has(p2.msgId));
  check('CL6: ...and the refused kill takes no stamp or seq slot', hr.seq === seq0 && hr.lastTs === last0, JSON.stringify({ seq0, seq: hr.seq }));
  const k2 = await buildKill({ topicId: topicHex, msgId: p2.msgId, identity: alice, ts: T0 + 3000, seq: 4 });
  await Hh.am._onKill({ topicId: topicHex, kill: k2 }, meta(35n));
  check('CL6 CONTROL: the author\'s newer kill2 at the heir retracts pub2', !hr.cacheIds.has(p2.msgId) && hr.tombstones.has(p2.msgId));
}
console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
