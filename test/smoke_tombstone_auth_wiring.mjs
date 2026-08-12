// =====================================================================
// smoke_tombstone_auth_wiring.mjs — REF-1.1 S2.0c Phase 3: SHADOW-MODE wiring
// of the accepted tombstoneAuth core into AxonaManager, differential.
//
// The tranche's contract: the `tombstoneAuth` construction flag is DEFAULT-OFF,
// and when ON it OBSERVES ONLY — it feeds the per-node TombstoneAuthority the real
// body/kill/evict stream at the existing funnels and changes NO behavior. This
// smoke proves exactly that against the real pipeline (a sim Fabric of
// AxonaManagers driving publish + authorized-kill + provisional-early-kill):
//
//   A. DIFFERENTIAL: the SAME scripted scenario, replayed over the SAME node ids,
//      produces BYTE-IDENTICAL delivery / cache / tombstone outcomes with the flag
//      OFF vs ON. The observer never perturbs the legacy path.
//   B. FLAG-OFF is inert: no authority is built; the shadow surface reports disabled.
//   C. FLAG-ON observes: the authority saw the live bodies + kills, made its own
//      co-located authorization decisions (an authorized kill over a cached body
//      SUPPRESSES; an early kill with no co-located body stays a bounded CANDIDATE
//      — the post-gate-shadow behavior the enforcement cutover will change), the
//      bounded stores stayed within caps, and it never threw into the hot path.
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

class Fabric {
  constructor({ tombstoneAuth = false } = {}) { this.nodes = new Map(); this.queue = []; this.clock = 1_000_000_000_000; this._ta = tombstoneAuth; }
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
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, tombstoneAuth: self._ta });
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

// Run the identical scripted scenario over a fabric built from the given node
// ids + author identities. Returns the observable outcome (for A/B compare) and
// the aggregate shadow observation across all nodes (for the flag-on assertions).
async function runScenario({ tombstoneAuth, nodeIds, alice, bob }) {
  const fab = new Fabric({ tombstoneAuth });
  const nodes = nodeIds.map(id => fab.addNode(id));

  // topic 1 — publish, forged kill (rejected), authorized kill (suppresses)
  const d1 = { region:'useast', owner:null, name:'ta-wire-1', write:'open' };
  const t1 = await deriveTopicIdBig(d1);
  nodes[0].am.pubsubSubscribe(t1); nodes[1].am.pubsubSubscribe(t1);
  await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const e1 = await buildEnvelope({ topic: d1, message: { hi: 1 }, seq: 1, identity: alice, ts: fab.clock });
  nodes[2].am.pubsubPublish(t1, JSON.stringify(e1)); await fab.settle();
  const r1 = root(fab, t1);
  const forged = await buildKill({ topicId: idHex(t1), msgId: e1.msgId, seq: 1, identity: bob });   // non-author → rejected
  fab.clock += 100; r1.am.pubsubKill(t1, forged); await fab.settle();
  const kill1 = await buildKill({ topicId: idHex(t1), msgId: e1.msgId, seq: 2, identity: alice });   // author → suppresses
  fab.clock += 100; r1.am.pubsubKill(t1, kill1); await fab.settle();

  // topic 2 — provisional early kill (arrives before target), then target
  const d2 = { region:'useast', owner:null, name:'ta-wire-2', write:'open' };
  const t2 = await deriveTopicIdBig(d2);
  nodes[0].am.pubsubSubscribe(t2); await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const e2 = await buildEnvelope({ topic: d2, message: { yo: 2 }, seq: 1, identity: alice, ts: fab.clock });
  const r2 = root(fab, t2);
  const kill2 = await buildKill({ topicId: idHex(t2), msgId: e2.msgId, seq: 1, identity: alice });
  r2.am.pubsubKill(t2, kill2); await fab.settle();                                                  // provisional
  fab.clock += 100; nodes[1].am.pubsubPublish(t2, JSON.stringify(e2)); await fab.settle();          // target → suppressed

  fab.clock += 5000; await fab.tickAll();

  // Observable outcome: per-node delivery + retraction, plus root cache/tombstone state.
  const outcome = {
    nodes: nodes.map(n => ({ got: [...n.got].sort(), dels: [...n.dels].sort() })),
    r1: { cache: cacheIds(r1, t1), tomb: tombIds(r1, t1) },
    r2: { cache: cacheIds(r2, t2), tomb: tombIds(r2, t2) },
  };

  // Aggregate shadow observation across every node.
  const agg = { enabledNodes: 0, bodies: 0, kills: 0, evicts: 0, errors: 0, tombstones: 0, candidates: 0, verdicts: {} };
  for (const n of nodes) {
    const s = n.am.tombstoneAuthShadow();
    if (!s.enabled) continue;
    agg.enabledNodes++;
    agg.bodies += s.stats.bodies; agg.kills += s.stats.kills; agg.evicts += s.stats.evicts; agg.errors += s.stats.errors;
    agg.tombstones += s.sizes.tombstones; agg.candidates += s.sizes.candidates;
    for (const [k, v] of Object.entries(s.stats.verdicts)) agg.verdicts[k] = (agg.verdicts[k] || 0) + v;
    // per-node cap invariant: bounded stores never exceed their profile caps
    if (s.sizes.tombstones > s.profile.tombMaxCount || s.sizes.candidates > s.profile.candMax) agg.errors++;
  }
  return { outcome, agg, offDisabled: nodes.every(n => n.am.tombstoneAuthShadow().enabled === false) };
}

async function main() {
  console.log('REF-1.1 S2.0c Phase 3 — tombstoneAuth SHADOW wiring (default-off, observe-only)\n');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();
  const nodeIds = [];
  for (let i = 0; i < 8; i++) nodeIds.push(BigInt('0x' + (await createNodeIdentity(__LOC)).id));

  const off = await runScenario({ tombstoneAuth: false, nodeIds, alice, bob });
  const on  = await runScenario({ tombstoneAuth: true,  nodeIds, alice, bob });

  // ---- A. DIFFERENTIAL: flag-on outcomes are byte-identical to flag-off --------
  check('A1. per-node delivery + retraction identical flag-off vs flag-on',
    JSON.stringify(off.outcome.nodes) === JSON.stringify(on.outcome.nodes),
    `\n      off=${JSON.stringify(off.outcome.nodes)}\n      on =${JSON.stringify(on.outcome.nodes)}`);
  check('A2. root cache + tombstone state identical (topic 1)',
    JSON.stringify(off.outcome.r1) === JSON.stringify(on.outcome.r1), JSON.stringify({ off: off.outcome.r1, on: on.outcome.r1 }));
  check('A3. root cache + tombstone state identical (topic 2)',
    JSON.stringify(off.outcome.r2) === JSON.stringify(on.outcome.r2), JSON.stringify({ off: off.outcome.r2, on: on.outcome.r2 }));
  // sanity: the scenario actually exercised a kill (both runs) — else A1-A3 are vacuous
  check('A4. scenario is non-trivial: topic-1 body killed (spliced from cache + tombstoned)',
    off.outcome.r1.cache.length === 0 && off.outcome.r1.tomb.length === 1);

  // ---- B. FLAG-OFF is inert ---------------------------------------------------
  check('B1. flag-off builds NO authority (shadow disabled on every node)', off.offDisabled && off.agg.enabledNodes === 0);

  // ---- C. FLAG-ON observes the live stream without perturbing it --------------
  check('C1. flag-on built a per-node authority on every node', on.agg.enabledNodes === nodeIds.length);
  check('C2. authority observed live bodies AND kills', on.agg.bodies > 0 && on.agg.kills > 0, JSON.stringify(on.agg));
  check('C3. authority never threw into the hot path + stayed within caps', on.agg.errors === 0, JSON.stringify(on.agg));
  check('C4. an authorized kill over a co-located body SUPPRESSED in the shadow',
    (on.agg.verdicts['kill:SUPPRESSED'] || 0) >= 1, JSON.stringify(on.agg.verdicts));
  check('C5. the provisional early kill (no co-located body) stayed a bounded candidate',
    on.agg.candidates >= 1 || (on.agg.verdicts['kill:ADMITTED'] || 0) >= 1, JSON.stringify(on.agg));

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
