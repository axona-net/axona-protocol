// =====================================================================
// smoke_tombstone_auth_wiring.mjs — REF-1.1 S2.0c Phase 3: SHADOW-MODE wiring
// of the accepted tombstoneAuth core into AxonaManager, differential + adversarial.
//
// The tranche's contract: the `tombstoneAuth` construction flag is DEFAULT-OFF,
// and when ON it OBSERVES ONLY — it feeds the per-node TombstoneAuthority ONLY
// LOCALLY-VERIFIED material (a verifyEnvelope()-verified body that survived the
// cache write; a verifyKill()-verified signed kill bound to this topic), and it
// never changes legacy behavior. This smoke proves that against the real pipeline:
//
//   A. DIFFERENTIAL: the SAME scripted scenario over the SAME node ids is
//      BYTE-IDENTICAL flag-off vs flag-on; flag-off builds no authority; flag-on
//      observed the verified stream (authorized kill over a co-located body
//      SUPPRESSED; an early kill with no co-located body stayed a bounded candidate).
//   D. ADVERSARIAL (Aster ec7a5a38 class 1): unverified wire metadata NEVER earns
//      shadow authority. A forged JSON body and an unsigned del marker driven
//      through the relay _onDeliver path are NOT observed; a bad-signature kill is
//      dropped before observation; the positive full-signed-proof path DOES suppress.
//   E. CACHE FIDELITY (class 2): the shadow body mirror never retains a body the
//      live cache dropped — immediate byte-cap eviction, role teardown, and
//      resetState leave no stale body able to authorize a later kill.
//
// Run: node test/smoke_tombstone_auth_wiring.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc = (s) => (typeof s === 'string' ? s.toLowerCase() : s);
const pub = (id) => lc(id.authorId ?? id.pubkeyHex);

class Fabric {
  constructor({ tombstoneAuth = false, replayCacheBytes } = {}) { this.nodes = new Map(); this.queue = []; this.clock = 1_000_000_000_000; this._ta = tombstoneAuth; this._rcb = replayCacheBytes; }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      verdictsSupported: false,
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target); if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive)
        .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
    };
    const opts = { dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, tombstoneAuth: self._ta };
    if (Number.isFinite(self._rcb)) opts.replayCacheBytes = self._rcb;
    const am = new AxonaManager(opts);
    const rec = { id: idBig, am, handlers, alive: true, got: [], dels: [] };
    am.onPubsubDelivery((_t, json, msgId) => { let o = null; try { o = JSON.parse(json); } catch {} if (o && o.deleted) rec.dels.push(o.msgId); else rec.got.push(msgId); });
    this.nodes.set(idBig, rec); return rec;
  }
  _closest(target){ let b=null,bd=null; for(const[id,n]of this.nodes){if(!n.alive)continue;const d=id^target;if(bd===null||d<bd){bd=d;b=id;}} return b; }
  async settle(cap=500000){ let i=0; while(this.queue.length){ if(++i>cap) throw new Error('settle cap'); const j=this.queue.shift(); const n=this.nodes.get(j.dest); if(!n||!n.alive)continue; const h=n.handlers.get(j.type); if(h) await h(j.payload,j.meta); } }
  async tickAll(){ for(const n of this.nodes.values()) if(n.alive) await n.am.refreshTick(); await this.settle(); }
}
const root = (fab, t) => fab.nodes.get(fab._closest(t));
const cacheIds = (rec, t) => (rec.am.axonRoles.get(t)?.cache || []).map(c => c.msgId).sort();
const tombIds  = (rec, t) => [...(rec.am.axonRoles.get(t)?.tombstones?.keys() || [])].sort();

// ---- A: differential scenario (same as before; hooks moved to verified sites) ---
async function runScenario({ tombstoneAuth, nodeIds, alice, bob }) {
  const fab = new Fabric({ tombstoneAuth });
  const nodes = nodeIds.map(id => fab.addNode(id));
  const d1 = { region:'useast', owner:null, name:'ta-wire-1', write:'open' };
  const t1 = await deriveTopicIdBig(d1);
  nodes[0].am.pubsubSubscribe(t1); nodes[1].am.pubsubSubscribe(t1);
  await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const e1 = await buildEnvelope({ topic: d1, message: { hi: 1 }, seq: 1, identity: alice, ts: fab.clock });
  nodes[2].am.pubsubPublish(t1, JSON.stringify(e1)); await fab.settle();
  const r1 = root(fab, t1);
  const forged = await buildKill({ topicId: idHex(t1), msgId: e1.msgId, seq: 1, identity: bob });
  fab.clock += 100; r1.am.pubsubKill(t1, forged); await fab.settle();
  const kill1 = await buildKill({ topicId: idHex(t1), msgId: e1.msgId, seq: 2, identity: alice });
  fab.clock += 100; r1.am.pubsubKill(t1, kill1); await fab.settle();

  const d2 = { region:'useast', owner:null, name:'ta-wire-2', write:'open' };
  const t2 = await deriveTopicIdBig(d2);
  nodes[0].am.pubsubSubscribe(t2); await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const e2 = await buildEnvelope({ topic: d2, message: { yo: 2 }, seq: 1, identity: alice, ts: fab.clock });
  const r2 = root(fab, t2);
  const kill2 = await buildKill({ topicId: idHex(t2), msgId: e2.msgId, seq: 1, identity: alice });
  r2.am.pubsubKill(t2, kill2); await fab.settle();
  fab.clock += 100; nodes[1].am.pubsubPublish(t2, JSON.stringify(e2)); await fab.settle();
  fab.clock += 5000; await fab.tickAll();

  const outcome = {
    nodes: nodes.map(n => ({ got: [...n.got].sort(), dels: [...n.dels].sort() })),
    r1: { cache: cacheIds(r1, t1), tomb: tombIds(r1, t1) },
    r2: { cache: cacheIds(r2, t2), tomb: tombIds(r2, t2) },
  };
  const agg = { enabledNodes: 0, bodies: 0, kills: 0, errors: 0, tombstones: 0, candidates: 0, verdicts: {} };
  for (const n of nodes) {
    const s = n.am.tombstoneAuthShadow(); if (!s.enabled) continue;
    agg.enabledNodes++; agg.bodies += s.stats.bodies; agg.kills += s.stats.kills; agg.errors += s.stats.errors;
    agg.tombstones += s.sizes.tombstones; agg.candidates += s.sizes.candidates;
    for (const [k, v] of Object.entries(s.stats.verdicts)) agg.verdicts[k] = (agg.verdicts[k] || 0) + v;
    if (s.sizes.tombstones > s.profile.tombMaxCount || s.sizes.candidates > s.profile.candMax) agg.errors++;
  }
  return { outcome, agg, offDisabled: nodes.every(n => n.am.tombstoneAuthShadow().enabled === false) };
}

async function main() {
  console.log('REF-1.1 S2.0c Phase 3 — tombstoneAuth SHADOW wiring (default-off, verified-only observe)\n');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();
  const nodeIds = [];
  for (let i = 0; i < 8; i++) nodeIds.push(BigInt('0x' + (await createNodeIdentity(__LOC)).id));

  // ---- A. DIFFERENTIAL ----------------------------------------------------------
  const off = await runScenario({ tombstoneAuth: false, nodeIds, alice, bob });
  const on  = await runScenario({ tombstoneAuth: true,  nodeIds, alice, bob });
  check('A1. per-node delivery + retraction identical flag-off vs flag-on',
    JSON.stringify(off.outcome.nodes) === JSON.stringify(on.outcome.nodes));
  check('A2. root cache + tombstone state identical (topic 1)', JSON.stringify(off.outcome.r1) === JSON.stringify(on.outcome.r1));
  check('A3. root cache + tombstone state identical (topic 2)', JSON.stringify(off.outcome.r2) === JSON.stringify(on.outcome.r2));
  check('A4. scenario is non-trivial: topic-1 body killed', off.outcome.r1.cache.length === 0 && off.outcome.r1.tomb.length === 1);
  check('B1. flag-off builds NO authority', off.offDisabled && off.agg.enabledNodes === 0);
  check('C1. flag-on built a per-node authority on every node', on.agg.enabledNodes === nodeIds.length);
  check('C2. authority observed VERIFIED bodies AND kills', on.agg.bodies > 0 && on.agg.kills > 0, JSON.stringify(on.agg));
  check('C3. authority never threw + stayed within caps', on.agg.errors === 0, JSON.stringify(on.agg));
  check('C4. authorized kill over a co-located verified body SUPPRESSED', (on.agg.verdicts['kill:SUPPRESSED'] || 0) >= 1, JSON.stringify(on.agg.verdicts));
  check('C5. provisional early kill stayed a bounded candidate', on.agg.candidates >= 1 || (on.agg.verdicts['kill:ADMITTED'] || 0) >= 1, JSON.stringify(on.agg));

  // ---- D. ADVERSARIAL: unverified wire metadata never earns authority -----------
  {
    const fab = new Fabric({ tombstoneAuth: true });
    const nodes = nodeIds.map(id => fab.addNode(id));
    const dD = { region:'useast', owner:null, name:'ta-adv', write:'open' };
    const tD = await deriveTopicIdBig(dD);
    nodes[0].am.pubsubSubscribe(tD); await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const eLeg = await buildEnvelope({ topic: dD, message: { legit: 1 }, seq: 1, identity: alice, ts: fab.clock });
    nodes[1].am.pubsubPublish(tD, JSON.stringify(eLeg)); await fab.settle();
    const r = root(fab, tD);
    const before = r.am.tombstoneAuthShadow();
    check('D0. legit verified body was observed by the root shadow', before.stats.bodies >= 1);

    // D1: a FORGED JSON body through the relay _onDeliver path (no local verifyEnvelope).
    const Mf = 'f'.repeat(64);
    await r.am._onDeliver({ topicId: idHex(tD), from: idHex(nodeIds[7]), msgs: [{ msgId: Mf, json: JSON.stringify({ fake: 1, signerPubkey: pub(alice) }), publishTs: fab.clock, seq: 500 }] }, { targetId: r.id });
    await fab.settle();
    const afterBody = r.am.tombstoneAuthShadow();
    check('D1. forged body via _onDeliver NOT observed (no shadow authority basis)', afterBody.stats.bodies === before.stats.bodies, JSON.stringify({ before: before.stats.bodies, after: afterBody.stats.bodies }));

    // D2: an UNSIGNED del marker for the same forged msgId through _onDeliver.
    await r.am._onDeliver({ topicId: idHex(tD), from: idHex(nodeIds[7]), msgs: [{ del: true, msgId: Mf, signer: pub(alice), killTs: fab.clock, seq: 501 }] }, { targetId: r.id });
    await fab.settle();
    const afterDel = r.am.tombstoneAuthShadow();
    check('D2. unsigned del marker via _onDeliver produced NO shadow tombstone + no observed kill',
      afterDel.sizes.tombstones === 0 && afterDel.stats.kills === before.stats.kills, JSON.stringify({ tombs: afterDel.sizes.tombstones, killsBefore: before.stats.kills, killsAfter: afterDel.stats.kills }));

    // D3: a BAD-SIGNATURE kill is dropped at verifyKill before any observation.
    const goodK = await buildKill({ topicId: idHex(tD), msgId: eLeg.msgId, seq: 9, identity: alice });
    const badK = { ...goodK, signature: 'ed25519:' + '0'.repeat(128) };
    fab.clock += 100; r.am.pubsubKill(tD, badK); await fab.settle();
    const afterBad = r.am.tombstoneAuthShadow();
    check('D3. bad-signature kill dropped before observation (kills unchanged)', afterBad.stats.kills === before.stats.kills, JSON.stringify({ before: before.stats.kills, after: afterBad.stats.kills }));

    // D4: the positive FULL-SIGNED path — an authorized kill over the verified body SUPPRESSES.
    const killLeg = await buildKill({ topicId: idHex(tD), msgId: eLeg.msgId, seq: 10, identity: alice });
    fab.clock += 100; r.am.pubsubKill(tD, killLeg); await fab.settle();
    const afterGood = r.am.tombstoneAuthShadow();
    check('D4. authorized signed kill over the verified body SUPPRESSED in the shadow',
      (afterGood.stats.verdicts['kill:SUPPRESSED'] || 0) >= 1 && afterGood.sizes.tombstones >= 1, JSON.stringify(afterGood.stats.verdicts));
    check('D5. zero hot-path throws across the adversarial run', afterGood.stats.errors === 0);
  }

  // ---- E. CACHE FIDELITY: no stale body survives eviction / teardown / reset -----
  {
    // E1: immediate byte-cap eviction — the just-written body does not survive, so
    //     it is never observed (survived-guard). Tiny replayCacheBytes forces it.
    const fab = new Fabric({ tombstoneAuth: true, replayCacheBytes: 100 });
    const nodes = nodeIds.map(id => fab.addNode(id));
    const dE = { region:'useast', owner:null, name:'ta-evict', write:'open' };
    const tE = await deriveTopicIdBig(dE);
    nodes[0].am.pubsubSubscribe(tE); await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const big = await buildEnvelope({ topic: dE, message: { pad: 'x'.repeat(400) }, seq: 1, identity: alice, ts: fab.clock });
    nodes[1].am.pubsubPublish(tE, JSON.stringify(big)); await fab.settle();
    const rE = root(fab, tE);
    check('E1. immediate byte-cap eviction: legacy cache empty AND shadow holds no stale body',
      cacheIds(rE, tE).length === 0 && rE.am.tombstoneAuthShadow().sizes.bodies === 0, JSON.stringify({ cache: cacheIds(rE, tE).length, shadowBodies: rE.am.tombstoneAuthShadow().sizes.bodies }));

    // E2: role teardown purges the topic's shadow bodies, removing the co-location basis.
    const fab2 = new Fabric({ tombstoneAuth: true });
    const n2 = nodeIds.map(id => fab2.addNode(id));
    const dT = { region:'useast', owner:null, name:'ta-teardown', write:'open' };
    const tT = await deriveTopicIdBig(dT);
    n2[0].am.pubsubSubscribe(tT); await fab2.settle(); fab2.clock += 6000; await fab2.tickAll();
    const eT = await buildEnvelope({ topic: dT, message: { t: 1 }, seq: 1, identity: alice, ts: fab2.clock });
    n2[1].am.pubsubPublish(tT, JSON.stringify(eT)); await fab2.settle();
    const rT = root(fab2, tT);
    check('E2a. body observed before teardown', rT.am.tombstoneAuthShadow().sizes.bodies >= 1);
    rT.am._taPurgeTopic(tT);   // the exact method wired at the repairPlane role-teardown site
    const afterPurge = rT.am.tombstoneAuthShadow();
    // a later authorized kill for that msgId must NOT co-locate (body basis is gone) → candidate, not SUPPRESSED
    const preSup = afterPurge.stats.verdicts['kill:SUPPRESSED'] || 0;
    const killT = await buildKill({ topicId: idHex(tT), msgId: eT.msgId, seq: 2, identity: alice });
    fab2.clock += 100; rT.am.pubsubKill(tT, killT); await fab2.settle();
    const postKill = rT.am.tombstoneAuthShadow();
    check('E2b. teardown purged the topic body; a later kill can no longer co-locate on it',
      afterPurge.sizes.bodies === 0 && (postKill.stats.verdicts['kill:SUPPRESSED'] || 0) === preSup, JSON.stringify({ purgedBodies: afterPurge.sizes.bodies, supBefore: preSup, supAfter: postKill.stats.verdicts['kill:SUPPRESSED'] || 0 }));

    // E3: resetState rebuilds the shadow — no stale body/candidate/tombstone survives.
    const fab3 = new Fabric({ tombstoneAuth: true });
    const n3 = nodeIds.map(id => fab3.addNode(id));
    const dR = { region:'useast', owner:null, name:'ta-reset', write:'open' };
    const tR = await deriveTopicIdBig(dR);
    n3[0].am.pubsubSubscribe(tR); await fab3.settle(); fab3.clock += 6000; await fab3.tickAll();
    const eR = await buildEnvelope({ topic: dR, message: { r: 1 }, seq: 1, identity: alice, ts: fab3.clock });
    n3[1].am.pubsubPublish(tR, JSON.stringify(eR)); await fab3.settle();
    const rR = root(fab3, tR);
    check('E3a. body observed before reset', rR.am.tombstoneAuthShadow().sizes.bodies >= 1);
    rR.am.resetState();
    const afterReset = rR.am.tombstoneAuthShadow();
    check('E3b. resetState rebuilt the shadow — zero bodies/candidates/tombstones', afterReset.enabled && afterReset.sizes.bodies === 0 && afterReset.sizes.candidates === 0 && afterReset.sizes.tombstones === 0, JSON.stringify(afterReset.sizes));
  }

  // ---- F. CROSS-TOPIC binding: a body signed for A can't seed B (Aster b188a223) ----
  {
    const fab = new Fabric({ tombstoneAuth: true });
    const nodes = nodeIds.map(id => fab.addNode(id));
    const dA = { region:'useast', owner:null, name:'ta-xtopic-A', write:'open' };
    const dB = { region:'useast', owner:null, name:'ta-xtopic-B', write:'open' };
    const tA = await deriveTopicIdBig(dA);
    const tB = await deriveTopicIdBig(dB);
    nodes[0].am.pubsubSubscribe(tB); await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const eBleg = await buildEnvelope({ topic: dB, message: { b: 1 }, seq: 1, identity: alice, ts: fab.clock });
    nodes[1].am.pubsubPublish(tB, JSON.stringify(eBleg)); await fab.settle();
    const rB = root(fab, tB);
    const roleB = rB.am.axonRoles.get(tB);
    const beforeF = rB.am.tombstoneAuthShadow();
    // a VALID envelope signed for topic A, fed to _ingestStamped under role B (the replay-up/handoff path)
    const eA = await buildEnvelope({ topic: dA, message: { a: 1 }, seq: 5, identity: alice, ts: fab.clock });
    const verdict = await rB.am._ingestStamped(roleB, { msgId: eA.msgId, publishTs: fab.clock, json: JSON.stringify(eA), seq: 5 });
    const afterF = rB.am.tombstoneAuthShadow();
    // Belt AND suspenders (Aster Phase-3 blocker a): the LIVE path binds the signed
    // descriptor to the role topic and REJECTS the cross-topic body before it is
    // cached; the shadow observer independently re-derives and would refuse it too,
    // so the shadow never gains a cross-topic body under B.
    check('F1. cross-topic stamped body REJECTED at the live path (topic binding)', verdict === 'rejected' && !roleB.cacheIds.has(eA.msgId), `verdict=${verdict}`);
    check('F2. shadow gained NO cross-topic body under B', afterF.stats.bodies === beforeF.stats.bodies, JSON.stringify({ before: beforeF.stats.bodies, after: afterF.stats.bodies }));
    // a valid signed kill (topic-agnostic V1 msgId) must NOT fabricate B co-location from the migrated A body
    const preSup = afterF.stats.verdicts['kill:SUPPRESSED'] || 0;
    const killA = await buildKill({ topicId: idHex(tB), msgId: eA.msgId, seq: 6, identity: alice });
    fab.clock += 100; rB.am.pubsubKill(tB, killA); await fab.settle();
    const postF = rB.am.tombstoneAuthShadow();
    check('F3. no fabricated B co-location: the cross-topic kill did NOT suppress via a migrated A body', (postF.stats.verdicts['kill:SUPPRESSED'] || 0) === preSup, JSON.stringify(postF.stats.verdicts));
  }

  // ---- G. SIGNED-KILL PROOF TRANSPORT — verify + BIND before retain/re-fan, and
  //      UPGRADE a proof-less tombstone (Aster Phase-3 blocker b, B1 + B2). Driven
  //      through the ACTUAL receive funnels (_onDeliver, _applyDels), NOT direct
  //      helper calls: a propagated proof is retained + re-fanned ONLY after it is
  //      verified and bound to its carrier (topicId + msgId + signer); a mismatched
  //      or forged proof is stripped; a later valid proof upgrades a proof-less tomb.
  {
    const fab = new Fabric({ tombstoneAuth: true });
    const nodes = nodeIds.map(id => fab.addNode(id));
    const dG = { region:'useast', owner:null, name:'ta-killproof', write:'open' };
    const tG = await deriveTopicIdBig(dG);
    nodes[0].am.pubsubSubscribe(tG); nodes[2].am.pubsubSubscribe(tG);
    await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const e1 = await buildEnvelope({ topic: dG, message: { g: 1 }, seq: 1, identity: alice, ts: fab.clock });
    const e2 = await buildEnvelope({ topic: dG, message: { g: 2 }, seq: 2, identity: alice, ts: fab.clock + 1 });
    const e3 = await buildEnvelope({ topic: dG, message: { g: 3 }, seq: 3, identity: alice, ts: fab.clock + 2 });
    nodes[1].am.pubsubPublish(tG, JSON.stringify(e1)); await fab.settle();
    nodes[1].am.pubsubPublish(tG, JSON.stringify(e2)); await fab.settle();
    nodes[1].am.pubsubPublish(tG, JSON.stringify(e3)); await fab.settle();
    const rG = root(fab, tG);
    const roleG   = () => rG.am.axonRoles.get(tG);
    const tombKill = (mid) => roleG()?.tombstones?.get(mid)?.kill;
    // Did any del carrying THIS proof get fanned/replicated on the wire since the drain?
    const refanned = (mid) => fab.queue.some(j => [j.payload?.msgs, j.payload?.dels].filter(Array.isArray)
      .some(a => a.some(x => x && x.del && x.msgId === mid && x.kill)));
    const deliverDel = (marker) => rG.am._onDeliver({ topicId: idHex(tG), from: idHex(nodeIds[7]), msgs: [marker] }, { targetId: rG.id });

    // G1/G2/G3 — a VALID, bound proof via _onDeliver: retained, observed, re-fanned.
    const k1 = await buildKill({ topicId: idHex(tG), msgId: e1.msgId, seq: 9, identity: alice });
    const b1 = rG.am.tombstoneAuthShadow(); fab.queue.length = 0;
    await deliverDel({ del: true, msgId: e1.msgId, killTs: fab.clock + 40, seq: 9, signer: k1.signerPubkey, kill: k1 });
    const a1 = rG.am.tombstoneAuthShadow();
    check('G1. valid propagated proof RETAINED in the tombstone (via _onDeliver)', tombKill(e1.msgId)?.signature === k1.signature);
    check('G2. valid propagated proof observed by the shadow after LOCAL verify', a1.stats.kills > b1.stats.kills, JSON.stringify({ before: b1.stats.kills, after: a1.stats.kills }));
    check('G3. valid proof RE-FANNED downstream (transport)', refanned(e1.msgId));
    await fab.settle();

    // G4/G5/G6 — a proof for a DIFFERENT carrier (kill names e2, del names e3) is
    // STRIPPED: never retained, never re-fanned, never observed under the wrong carrier.
    const kMis = await buildKill({ topicId: idHex(tG), msgId: e2.msgId, seq: 10, identity: alice });
    const b2 = rG.am.tombstoneAuthShadow(); fab.queue.length = 0;
    await deliverDel({ del: true, msgId: e3.msgId, killTs: fab.clock + 50, seq: 10, signer: kMis.signerPubkey, kill: kMis });
    const a2 = rG.am.tombstoneAuthShadow();
    check('G4. carrier/proof mismatch: e3 tombstone installed but carries NO proof (stripped)', roleG()?.tombstones?.has(e3.msgId) === true && tombKill(e3.msgId) === undefined);
    check('G5. carrier/proof mismatch: nothing re-fanned with the mismatched proof', !refanned(e3.msgId) && !refanned(e2.msgId));
    check('G6. carrier/proof mismatch: the shadow did NOT observe the mismatched proof', a2.stats.kills === b2.stats.kills);
    await fab.settle();

    // G7 — a FORGED proof via _applyDels is stripped: proof-less tombstone, not observed, no throw.
    const good2 = await buildKill({ topicId: idHex(tG), msgId: e2.msgId, seq: 11, identity: alice });
    const forged2 = { ...good2, signature: 'ed25519:' + '0'.repeat(128) };
    const b3 = rG.am.tombstoneAuthShadow();
    await rG.am._applyDels(roleG(), tG, [{ del: true, msgId: e2.msgId, killTs: fab.clock + 60, seq: 11, signer: good2.signerPubkey, kill: forged2 }]);
    const a3 = rG.am.tombstoneAuthShadow();
    check('G7. forged proof via _applyDels: proof-less tombstone, not observed, no throw',
      roleG()?.tombstones?.has(e2.msgId) === true && tombKill(e2.msgId) === undefined && a3.stats.kills === b3.stats.kills && a3.stats.errors === b3.stats.errors);

    // G8/G9 — a later VALID proof UPGRADES the proof-less e2 tombstone (B2): attach + observe + re-fan.
    const k2 = await buildKill({ topicId: idHex(tG), msgId: e2.msgId, seq: 12, identity: alice });
    const b4 = rG.am.tombstoneAuthShadow(); fab.queue.length = 0;
    await deliverDel({ del: true, msgId: e2.msgId, killTs: fab.clock + 70, seq: 12, signer: k2.signerPubkey, kill: k2 });
    const a4 = rG.am.tombstoneAuthShadow();
    check('G8. B2 upgrade: a later valid proof attaches to the proof-less tombstone', tombKill(e2.msgId)?.signature === k2.signature);
    check('G9. B2 upgrade: the upgraded proof is observed + re-fanned downstream', a4.stats.kills > b4.stats.kills && refanned(e2.msgId), JSON.stringify({ before: b4.stats.kills, after: a4.stats.kills, refan: refanned(e2.msgId) }));
    await fab.settle();
  }

  // ---- H. PROOF-SIGNER CONSISTENCY — the verified proof signer is authoritative
  //      (Aster S1 + S2). A proof carried with a MISSING or CONFLICTING signer is
  //      stripped; a retained/emitted proof-bearing marker always carries the verified
  //      signer (never null); an upgrade OVERRIDES a stale/poisoned prior signer; a
  //      SECOND receiver retains the re-emitted consistent proof + strips a poisoned one.
  {
    const fab = new Fabric({ tombstoneAuth: true });
    const nodes = nodeIds.map(id => fab.addNode(id));
    const dH = { region:'useast', owner:null, name:'ta-signerconsistency', write:'open' };
    const tH = await deriveTopicIdBig(dH);
    nodes[0].am.pubsubSubscribe(tH); nodes[2].am.pubsubSubscribe(tH);
    await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const eH1 = await buildEnvelope({ topic: dH, message: { h: 1 }, seq: 1, identity: alice, ts: fab.clock });
    const eH2 = await buildEnvelope({ topic: dH, message: { h: 2 }, seq: 2, identity: alice, ts: fab.clock + 1 });
    const eH3 = await buildEnvelope({ topic: dH, message: { h: 3 }, seq: 3, identity: alice, ts: fab.clock + 2 });
    nodes[1].am.pubsubPublish(tH, JSON.stringify(eH1)); await fab.settle();
    nodes[1].am.pubsubPublish(tH, JSON.stringify(eH2)); await fab.settle();
    nodes[1].am.pubsubPublish(tH, JSON.stringify(eH3)); await fab.settle();
    const rH = root(fab, tH);
    const tombH = (mid) => rH.am.axonRoles.get(tH)?.tombstones?.get(mid);
    const fannedMarker = (mid) => { for (const j of fab.queue) { const x = (j.payload?.msgs || []).find(y => y && y.del && y.msgId === mid && y.kill); if (x) return x; } return null; };
    const mallory = bob.pubkeyHex;   // an arbitrary signer DISTINCT from alice's
    const deliverH = (marker) => rH.am._onDeliver({ topicId: idHex(tH), from: idHex(nodeIds[7]), msgs: [marker] }, { targetId: rH.id });

    // H1 (S1) — a valid proof with a MISSING carrier signer is stripped: not retained/observed.
    const kH1 = await buildKill({ topicId: idHex(tH), msgId: eH1.msgId, seq: 9, identity: alice });
    const hb1 = rH.am.tombstoneAuthShadow();
    await deliverH({ del: true, msgId: eH1.msgId, killTs: fab.clock + 40, seq: 9, kill: kH1 });   // NO signer field
    const ha1 = rH.am.tombstoneAuthShadow();
    check('H1. S1: proof with a MISSING carrier signer is stripped (not retained/observed)',
      tombH(eH1.msgId)?.kill === undefined && ha1.stats.kills === hb1.stats.kills);
    await fab.settle();

    // H2 (S1) — a consistent proof (signer present + matching) is retained with the VERIFIED
    //           signer and re-fanned carrying it — never null.
    const kH3 = await buildKill({ topicId: idHex(tH), msgId: eH3.msgId, seq: 10, identity: alice });
    fab.queue.length = 0;
    await deliverH({ del: true, msgId: eH3.msgId, killTs: fab.clock + 45, seq: 10, signer: kH3.signerPubkey, kill: kH3 });
    const fmH2 = fannedMarker(eH3.msgId);
    check('H2. S1: consistent proof retained with the VERIFIED signer + re-fanned carrying it (never null)',
      lc(tombH(eH3.msgId)?.signer) === lc(kH3.signerPubkey) && !!fmH2 && lc(fmH2.signer) === lc(kH3.signerPubkey));
    await fab.settle();

    // H3 (S2) — proof-less tombstone installed with a CONFLICTING prior signer (Mallory), then a
    //           valid Alice proof upgrades it: the tombstone signer becomes Alice, and the
    //           re-emitted marker is CONSISTENT (signer Alice + Alice proof), never poisoned.
    await rH.am._applyDels(rH.am.axonRoles.get(tH), tH, [{ del: true, msgId: eH2.msgId, killTs: fab.clock + 50, seq: 11, signer: mallory }]);   // unsigned, signer=Mallory
    check('H3a. proof-less tombstone carries the prior (Mallory) signer, no proof', tombH(eH2.msgId) && tombH(eH2.msgId).kill === undefined && lc(tombH(eH2.msgId).signer) === lc(mallory));
    const kH2 = await buildKill({ topicId: idHex(tH), msgId: eH2.msgId, seq: 12, identity: alice });
    fab.queue.length = 0;
    await deliverH({ del: true, msgId: eH2.msgId, killTs: fab.clock + 55, seq: 12, signer: kH2.signerPubkey, kill: kH2 });
    const fmH3 = fannedMarker(eH2.msgId);
    check('H3b. S2 upgrade OVERRIDES the stale signer: tombstone signer is now the verified proof signer',
      tombH(eH2.msgId)?.kill?.signature === kH2.signature && lc(tombH(eH2.msgId).signer) === lc(kH2.signerPubkey));
    check('H3c. S2 upgrade re-emits a CONSISTENT marker (verified signer + its proof), never poisoned',
      !!fmH3 && lc(fmH3.signer) === lc(kH2.signerPubkey) && fmH3.kill?.signature === kH2.signature);
    await fab.settle();

    // H4 (S2 convergence) — a SECOND, independent receiver retains the re-emitted CONSISTENT
    //     marker and strips the poisoned one (signer Mallory + Alice proof), at its verify gate.
    const rx = nodes[5];
    const okConsistent = await rx.am._taVerifyBoundKill(tH, fmH3);
    const okPoison = await rx.am._taVerifyBoundKill(tH, { del: true, msgId: eH2.msgId, killTs: fab.clock + 60, seq: 12, signer: mallory, kill: kH2 });
    check('H4a. second receiver ACCEPTS the re-emitted consistent proof (converges — would retain)', !!okConsistent && okConsistent.signature === kH2.signature);
    check('H4b. second receiver STRIPS a poisoned marker (signer Mallory + Alice proof)', okPoison === null);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
