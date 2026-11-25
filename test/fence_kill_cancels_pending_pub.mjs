#!/usr/bin/env node
// =====================================================================
// fence_kill_cancels_pending_pub.mjs — a killed body is never sent again by
// the node that killed it.
//
// Found 2026-11-24 as a one-in-a-hundred failure of smoke_pubsub_kill 3a/3b
// (council 47f5dadd, ccf79e54; lines read by Orion a79dd958). The lottery was
// two random ids: the publisher is the topic's root (1 in 8), and a node born
// afterwards is closer to the topic (1 in 9). Then:
//   - the root's own publish sat in _pendingPub for good: every replication
//     push came back unverified, so the ingest returned before _confirmPending;
//   - its kill sat in _pendingKill the same way, and applying the kill
//     cancelled the durability retry but NOT the pending publish;
//   - the tick pump walks [_pendingPub, _pendingKill] in that order, so the new
//     root was sent the BODY and then, one frame behind, the kill. Its app was
//     handed the message and then the retraction.
// This fence PINS both draws, so the case runs every time.
//   K1 the scene is the one that failed: publisher is the root, the late node
//      is born closest.
//   K2 after the kill is applied the root holds no pending publish for it.
//   K3 no PUB carrying the body is sent to anyone after the kill.
//   K4 the late node's app is never handed the body (3b).
//   K5 nor a {deleted} event for a body it never saw (3a).
//   K6 the late node still ends with the tombstone: the kill converged.
//   K7 the kill itself is still pending and still retried (only the body is
//      cancelled).
// The pump, alone:
//   P1 a pending kill is sent BEFORE a pending publish on a tick, so a root
//      that is new to both takes the kill provisionally first.
//   P2 a pending publish whose body this node holds a tombstone for is dropped
//      from the pump and not sent.
//   P3 a pending publish with no tombstone is still re-sent (the retry lives).
// A reader:
//   R1 a node with no role applies a delivered deletion without throwing.
// Defensive (Aster 8aff1137): the retry of a LIVE publish survives every
// deletion not authorized for that exact envelope:
//   D1 a provisional marker, D2 an expired deletion, D3 another signer's,
//   D4 the author's older kill; D5 the author's kill held as a claim does stop
//   it; D6/D7 the same two cases on the early resend pump; D8 to D11 the same
//   four at _applyKill on a cache miss; D12 the judge alone.
// Run: node test/fence_kill_cancels_pending_pub.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { pendingKey } from '../src/pubsub/ids.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { sealTestDht } from './lib/testCapability.mjs';

const LOC = regionCenter('useast');
const EXPECT = 23;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });
const idHex = (b) => b.toString(16).padStart(66, '0');
const freshId = async () => BigInt('0x' + (await createNodeIdentity(LOC)).id);

// The fabric of smoke_pubsub_kill.mjs: no verdicts, every frame to the closest live node.
class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); this.sent = []; }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig, onRoutedMessage: (t, h) => handlers.set(t, h), verdictsSupported: false,
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target); if (dest === null) return;
        const j = { dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } };
        self.sent.push(j); self.queue.push(j);
      },
      findKClosest: async (target, k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => id)
        .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
    };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [], dels: [] };
    am.onPubsubDelivery((_t, json, msgId) => { let o = null; try { o = JSON.parse(json); } catch {} if (o && o.deleted) rec.dels.push(o.msgId); else rec.got.push(msgId); });
    this.nodes.set(idBig, rec); return rec;
  }
  _closest(target) { let b = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; b = id; } } return b; }
  async settle(cap = 500000) {
    let i = 0;
    for (let quiet = 0; quiet < 3;) {
      while (this.queue.length) { if (++i > cap) throw new Error('settle cap'); const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue; const h = n.handlers.get(j.type); if (h) await h(j.payload, j.meta); quiet = 0; }
      for (const n of this.nodes.values()) if (n.am._ingestIdle) await n.am._ingestIdle();
      for (let k = 0; k < 4; k++) await new Promise((r) => setImmediate(r));
      if (!this.queue.length) quiet++;
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}
const carriesBody = (j, msgId) => j.type === 'pubsub:pub' && typeof j.payload?.json === 'string' && j.payload.json.includes(msgId);

console.log('fence_kill_cancels_pending_pub: a killed body is never sent again by the node that killed it');
{
  const alice = await createAuthorIdentity();
  const fab = new Fabric();
  const desc = { region: 'useast', owner: null, name: 'kill-pending-pub', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const nodes = []; for (let i = 0; i < 8; i++) nodes.push(fab.addNode(await freshId()));
  const byDist = [...nodes].sort((a, b) => ((a.id ^ topicId) < (b.id ^ topicId) ? -1 : 1));
  const r = byDist[0];                                   // PINNED: the publisher is the root
  const [subA, subB] = [byDist[3], byDist[5]];
  subA.am.pubsubSubscribe(topicId); subB.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const e = await buildEnvelope({ topic: desc, message: { hi: 1 }, seq: 1, identity: alice, ts: fab.clock });
  r.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  const key = pendingKey(topicId, e.msgId);
  const heldBefore = !!r.am._pendingPub?.has(key);
  const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, ts: fab.clock, identity: alice });
  fab.clock += 100; r.am.pubsubKill(topicId, kill); await fab.settle();
  const heldAfterKill = !!r.am._pendingPub?.has(key);
  const mark = fab.sent.length;
  let lateId; for (let i = 0; ; i++) { lateId = await freshId(); if ((lateId ^ topicId) < (r.id ^ topicId)) break; if (i > 5000) throw new Error('no closer id drawn'); }
  const late = fab.addNode(lateId);                      // PINNED: born closest
  late.am._lastSeenTsByTopic.set(topicId, 0); late.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 5000; await fab.tickAll(); await fab.settle();
  fab.clock += 5000; await fab.tickAll(); await fab.settle();

  check('K1 the scene: the publisher is the root, it held its own publish pending, and the late node is born closest', r.am.axonRoles.get(topicId) !== undefined && heldBefore && fab._closest(topicId) === late.id, JSON.stringify({ heldBefore }));
  check('K2 once the kill is applied, before any later node exists, the root holds no pending publish for it', heldAfterKill === false);
  const bodies = fab.sent.slice(mark).filter((j) => carriesBody(j, e.msgId));
  check('K3 no PUB carrying the body is sent to anyone after the kill', bodies.length === 0, `${bodies.length} sent`);
  check('K4 the late node\'s app is never handed the killed body', !late.got.includes(e.msgId));
  check('K5 nor a {deleted} event for a body it never saw', !late.dels.includes(e.msgId));
  check('K6 the late node ends with the tombstone and no cached body', !!late.am.axonRoles.get(topicId)?.tombstones?.has(e.msgId) && !(late.am.axonRoles.get(topicId)?.cache || []).some((c) => c.msgId === e.msgId));
  const killsAfter = fab.sent.slice(mark).filter((j) => j.type === 'pubsub:kill').length;
  check('K7 the kill itself was still retried toward the new root', killsAfter >= 1, String(killsAfter));
}
{
  // The pump alone: one manager, a recording dht.
  const alice = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'kill-pump-order', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const mk = async () => {
    const out = []; let clock = Date.now();
    const self = await freshId();
    const dht = { getSelfId: () => self, onRoutedMessage: () => {}, verdictsSupported: false, routeMessage: (target, type, payload) => { out.push({ type, payload }); }, findKClosest: async () => [] };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock });
    return { am, out, tick: async () => { clock += 6000; await am.refreshTick(); for (let k = 0; k < 6; k++) await new Promise((r) => setImmediate(r)); } };
  };
  const e = await buildEnvelope({ topic: desc, message: { hi: 2 }, seq: 1, identity: alice, ts: Date.now() });
  const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, ts: Date.now(), identity: alice });
  const key = pendingKey(topicId, e.msgId);
  {
    const X = await mk();
    X.am._pendingPub = new Map([[key, { topicBig: topicId, msgId: e.msgId, json: JSON.stringify(e), at: X.am._now(), tries: 0, via: null }]]);
    X.am._pendingKill = new Map([[key, { topicBig: topicId, msgId: e.msgId, kill, at: X.am._now(), tries: 0, via: null }]]);
    await X.tick();
    const order = X.out.filter((o) => o.type === 'pubsub:pub' || o.type === 'pubsub:kill').map((o) => o.type);
    check('P1 on a tick the pending kill goes out BEFORE the pending publish', order[0] === 'pubsub:kill' && order.includes('pubsub:pub'), JSON.stringify(order));
  }
  {
    const X = await mk();
    const role = X.am._rootClaim.become(topicId, 'sub-terminal');
    role.tombstones.set(e.msgId, { exp: X.am._now() + 3_600_000, killTs: X.am._now(), signer: kill.signerPubkey, kill, seq: 9 });
    X.am._pendingPub = new Map([[key, { topicBig: topicId, msgId: e.msgId, json: JSON.stringify(e), at: X.am._now(), tries: 0, via: null }]]);
    await X.tick();
    check('P2 a pending publish this node holds the AUTHOR\'s live deletion for is dropped, not sent', X.out.filter((o) => carriesBody(o, e.msgId)).length === 0 && !X.am._pendingPub.has(key), JSON.stringify({ sent: X.out.filter((o) => o.type === 'pubsub:pub').length, held: X.am._pendingPub.has(key) }));
  }
  {
    const X = await mk();
    X.am._pendingPub = new Map([[key, { topicBig: topicId, msgId: e.msgId, json: JSON.stringify(e), at: X.am._now(), tries: 0, via: null }]]);
    await X.tick();
    check('P3 a pending publish with no tombstone is still re-sent', X.out.filter((o) => carriesBody(o, e.msgId)).length >= 1 && X.am._pendingPub.has(key));
  }
}
{
  // A READER has no role. A kill delivered to it is applied with role undefined;
  // the first cut of this fix read role.topicId there and threw in two other
  // tests (smoke_tombstone_auth_wiring, fence_topic_late_renewal).
  const alice = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'kill-reader-no-role', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const self = await freshId();
  const dht = { getSelfId: () => self, onRoutedMessage: () => {}, verdictsSupported: false, routeMessage: () => {}, findKClosest: async () => [] };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => Date.now() });
  const e = await buildEnvelope({ topic: desc, message: { hi: 3 }, seq: 1, identity: alice, ts: Date.now() });
  const key = pendingKey(topicId, e.msgId);
  am._pendingPub = new Map([[key, { topicBig: topicId, msgId: e.msgId, json: JSON.stringify(e), at: Date.now(), tries: 0, via: null }]]);
  let threw = null; try { am._applyKill(undefined, topicId, { msgId: e.msgId, killTs: Date.now(), seq: 2, del: true }); } catch (err) { threw = err; }
  check('R1 a reader with no role applies a delivered deletion without throwing', threw === null, String(threw?.message || ''));
}
{
  // DEFENSIVE (Aster 8aff1137): a live pending publish survives every deletion
  // that is not authorized for that exact envelope, and an authorized current
  // one still quenches both resend paths.
  const alice = await createAuthorIdentity(); const bob = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'kill-defensive', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const t0 = Date.now();
  const e = await buildEnvelope({ topic: desc, message: { hi: 4 }, seq: 5, identity: alice, ts: t0 });
  const key = pendingKey(topicId, e.msgId);
  const K = (o) => buildKill({ topicId: idHex(topicId), msgId: e.msgId, ...o });
  const good  = await K({ seq: 6, ts: t0 + 10, identity: alice });       // the author, after the body
  const stale = await K({ seq: 4, ts: t0 - 10_000, identity: alice });   // the author, BEFORE the body
  const forged = await K({ seq: 6, ts: t0 + 10, identity: bob });        // another signer
  const mk = async () => {
    const out = []; let clock = t0 + 100;
    const self = await freshId();
    const dht = { getSelfId: () => self, onRoutedMessage: () => {}, verdictsSupported: false, routeMessage: (target, type, payload) => { out.push({ type, payload }); }, findKClosest: async () => [] };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock });
    const role = am._rootClaim.become(topicId, 'sub-terminal');
    am._pendingPub = new Map([[key, { topicBig: topicId, msgId: e.msgId, json: JSON.stringify(e), at: clock, tries: 0, via: null }]]);
    const bodies = () => out.filter((o) => carriesBody(o, e.msgId)).length;
    return { am, role, out, bodies, tick: async () => { clock += 6000; await am.refreshTick(); for (let k = 0; k < 6; k++) await new Promise((r) => setImmediate(r)); }, now: () => clock };
  };
  const tomb = (X, extra) => X.role.tombstones.set(e.msgId, { exp: X.now() + 3_600_000, killTs: X.now(), signer: null, seq: 9, ...extra });

  { const X = await mk(); tomb(X, {}); await X.tick();
    check('D1 a PROVISIONAL marker (no proof) does not stop the retry', X.bodies() >= 1 && X.am._pendingPub.has(key)); }
  { const X = await mk(); tomb(X, { kill: good, signer: good.signerPubkey, exp: X.now() - 1 }); await X.tick();
    check('D2 an EXPIRED deletion does not stop the retry, even with the author\'s proof', X.bodies() >= 1 && X.am._pendingPub.has(key)); }
  { const X = await mk(); tomb(X, { kill: forged, signer: forged.signerPubkey }); await X.tick();
    check('D3 a deletion by ANOTHER signer does not stop the retry', X.bodies() >= 1 && X.am._pendingPub.has(key)); }
  { const X = await mk(); tomb(X, { kill: stale, signer: stale.signerPubkey }); await X.tick();
    check('D4 the author\'s OLDER kill (before this body) does not stop the retry', X.bodies() >= 1 && X.am._pendingPub.has(key)); }
  { const X = await mk(); tomb(X, { kill: forged, signer: forged.signerPubkey, claims: new Map([[good.signerPubkey.toLowerCase(), good]]) }); await X.tick();
    check('D5 the author\'s kill held as a CLAIM on a foreign marker does stop it', X.bodies() === 0 && !X.am._pendingPub.has(key)); }
  { const X = await mk(); tomb(X, { kill: good, signer: good.signerPubkey });
    X.am._burstTimers ??= new Set();
    X.am._earlyResendPump(topicId, e.msgId, [1]);
    await new Promise((r) => setTimeout(r, 30));
    check('D6 the EARLY resend pump is quenched by the author\'s live deletion', X.bodies() === 0 && !X.am._pendingPub.has(key), String(X.bodies())); }
  { const X = await mk(); tomb(X, {});
    X.am._burstTimers ??= new Set();
    X.am._earlyResendPump(topicId, e.msgId, [1]);
    await new Promise((r) => setTimeout(r, 30));
    check('D7 and is NOT quenched by a provisional marker', X.bodies() === 1 && X.am._pendingPub.has(key), String(X.bodies())); }
  // _applyKill at a root that does NOT hold the body (a cache miss): nothing checks the author there.
  { const X = await mk();
    X.am._applyKill(X.role, topicId, { msgId: e.msgId, killTs: X.now(), signer: forged.signerPubkey, del: true });
    check('D8 applying a proof-less deletion on a cache miss leaves the pending publish', X.am._pendingPub.has(key)); }
  { const X = await mk();
    X.am._applyKill(X.role, topicId, { msgId: e.msgId, killTs: X.now(), signer: forged.signerPubkey, kill: forged, del: true });
    check('D9 applying ANOTHER signer\'s kill on a cache miss leaves the pending publish', X.am._pendingPub.has(key)); }
  { const X = await mk();
    X.am._applyKill(X.role, topicId, { msgId: e.msgId, killTs: X.now(), signer: stale.signerPubkey, kill: stale, del: true });
    check('D10 applying the author\'s OLDER kill leaves the pending publish', X.am._pendingPub.has(key)); }
  { const X = await mk();
    X.am._applyKill(X.role, topicId, { msgId: e.msgId, killTs: X.now(), signer: good.signerPubkey, kill: good, del: true });
    check('D11 applying the author\'s current kill cancels it', !X.am._pendingPub.has(key)); }
  check('D12 the judge alone: no kills, a malformed body, a kill for another topic', (() => {
    const X = new AxonaManager({ dht: sealTestDht({ getSelfId: () => 1n, onRoutedMessage: () => {}, verdictsSupported: false, routeMessage: () => {}, findKClosest: async () => [] }), now: () => t0 });
    const j = JSON.stringify(e);
    return X._killCoversEnvelope(j, e.msgId, topicId, []) === false
      && X._killCoversEnvelope('not json', e.msgId, topicId, [good]) === false
      && X._killCoversEnvelope(j, e.msgId, topicId + 1n, [good]) === false
      && X._killCoversEnvelope(j, e.msgId, topicId, [null, undefined, {}, good]) === true;
  })());
}
console.log(`\nfence_kill_cancels_pending_pub: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
