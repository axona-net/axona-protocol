// =====================================================================
// smoke_partial_root_union.mjs — a root that inherits PART of a topic's history
// must converge the rest.
//
// WHY THIS EXISTS. #353's interloper scenario creates a state the protocol has
// no other test for: a joiner strictly closer to the topic becomes root BY
// ROUTING while holding only the messages it happened to see. It is a root with
// a PARTIAL history, and the older half lives on the peer it displaced.
//
// This started as a hypothesis test. Reading src/pubsub/repairPlane.js, every
// gate on the empty-root cohort pull requires the cache to be EXACTLY empty —
//   :382  if (!role.isRoot || role.cache.length) continue;
//   :412  if (!pre || !pre.isRoot || pre.cache.length) return;
//   :439  if (!role || !role.isRoot || role.cache.length || !cand.size) return;
// which reads as "some history implies all history". The one path that repairs a
// partial root, SPLIT_UNION (wireHandlers.js:142), is gated on `payload.lw > 0`
// — it fires only when a SUBSCRIBER advertises older history than the root has,
// and a fresh since:'all' subscriber has none to advertise. So the prediction was
// that a partial root would serve a fresh subscriber its partial set forever.
//
// THAT PREDICTION WAS WRONG (measured 2026-07-29, kernel 4.49.0). The union
// completes on the FIRST renewal tick and the subscriber gets the full history.
// Some other path covers it. The test is kept — inverted — because the guarantee
// it now pins down is real, was untested, and the reasoning above shows how
// plausible it is for a future change to a `cache.length` gate to break it
// silently.
//
// The CONTROL is what makes this meaningful: it asserts the older half is still
// held by a live peer. Without that, "the subscriber didn't get it" is ordinary
// data loss and says nothing about reconciliation.
//
// Run: node test/smoke_partial_root_union.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';

const __LOC = regionCenter('useast') || { lat: 38, lng: -77 };
const idHex = (b) => b.toString(16).padStart(66, '0');
const lcg = (s) => () => ((s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);

let passed = 0, failed = 0;
const check = (l, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${l}${c ? '' : '  ' + extra}`); c ? passed++ : failed++; };

class Fabric {
  constructor({ drop = 0, seed = 1 } = {}) { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); this.drop = drop; this.rand = lcg(seed); }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      verdictsSupported: false,   // audited: returns a push-count / undefined, never a verdict
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target); if (dest === null) return;
        if (self.drop && self.rand() < self.drop) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive)
        .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [], dels: [] };
    am.onPubsubDelivery((_t, json, msgId) => { let o = null; try { o = JSON.parse(json); } catch {} if (o && o.deleted) rec.dels.push(o.msgId); else rec.got.push(msgId); });
    this.nodes.set(idBig, rec); return rec;
  }
  _closest(target) { let b = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; b = id; } } return b; }
  async settle(cap = 500000) { let i = 0; while (this.queue.length) { if (++i > cap) throw new Error('settle cap'); const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue; const h = n.handlers.get(j.type); if (h) await h(j.payload, j.meta); } }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}
const rootOf = (fab, t) => fab.nodes.get(fab._closest(t));
const has = (rec, t, id) => (rec.am.axonRoles.get(t)?.cache || []).some(c => c.msgId === id);
const cacheMsgs = (rec, t) => (rec.am.axonRoles.get(t)?.cache || []).map(c => { try { return JSON.parse(c.json).message; } catch { return '?'; } });

async function main() {
  console.log('#412 hypothesis — does a PARTIAL root ever pull the history it is missing?\n');
  const alice = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = [];
  for (let i = 0; i < 8; i++) nodes.push(fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id)));

  const desc = { region: 'useast', owner: null, name: 'partial-root-412', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // ── phase 1: a normal warm topic. Two seated subscribers, three messages. ──
  const subA = nodes[0], subB = nodes[1], pub = nodes[2];
  subA.am.pubsubSubscribe(topicId); subB.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6000; await fab.tickAll();

  const ids = {};
  for (const m of ['m1', 'm2', 'm3']) {
    const e = await buildEnvelope({ topic: desc, message: m, seq: Object.keys(ids).length + 1, identity: alice, ts: fab.clock });
    ids[m] = e.msgId;
    pub.am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle();
    fab.clock += 10;
  }
  const r0 = rootOf(fab, topicId);
  check('setup: original root holds m1..m3', ['m1', 'm2', 'm3'].every(m => has(r0, topicId, ids[m])),
    `cache=${cacheMsgs(r0, topicId).join(',')}`);

  // ── phase 2: an interloper strictly closer to the topic appears and publishes.
  // It becomes root BY ROUTING and caches only what it saw: m4..m6. ──
  let X = null;
  for (let t = 0; t < 4000 && !X; t++) {
    const cand = BigInt('0x' + (await createNodeIdentity(__LOC)).id);
    if ((cand ^ topicId) < (r0.id ^ topicId)) X = fab.addNode(cand);
  }
  check('setup: minted an interloper strictly closer than the original root', !!X);
  if (!X) { console.log('\nFAIL (could not mint)'); process.exit(1); }

  for (const m of ['m4', 'm5', 'm6']) {
    const e = await buildEnvelope({ topic: desc, message: m, seq: Object.keys(ids).length + 1, identity: alice, ts: fab.clock });
    ids[m] = e.msgId;
    X.am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle();
    fab.clock += 10;
  }
  check('setup: interloper is now the root', rootOf(fab, topicId).id === X.id);
  const xHasNew = ['m4', 'm5', 'm6'].every(m => has(X, topicId, ids[m]));
  const xHasOld = ['m1', 'm2', 'm3'].some(m => has(X, topicId, ids[m]));
  check('setup: the new root holds a PARTIAL history (new yes, old no)', xHasNew && !xHasOld,
    `cache=${cacheMsgs(X, topicId).join(',')}`);

  // THE CONTROL: the old half must still exist somewhere live, or this proves nothing.
  check('CONTROL: m1..m3 still held by a live peer (history exists in the network)',
    ['m1', 'm2', 'm3'].every(m => [...fab.nodes.values()].some(n => n.alive && has(n, topicId, ids[m]))),
    'the old half is genuinely GONE — different bug');

  // ── phase 3: a fresh subscriber asks for everything. ──
  const late = fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id));
  late.am._lastSeenTsByTopic.set(topicId, 0);
  late.am.pubsubSubscribe(topicId);
  await fab.settle();

  const ALL = ['m1', 'm2', 'm3', 'm4', 'm5', 'm6'];
  const gotWhat = () => ALL.filter(m => late.got.includes(ids[m]));
  console.log(`\n  after subscribe:            got ${gotWhat().join(',') || '(nothing)'}`);

  // Run the repair plane HARD. 200 renewal-scale ticks is far beyond any
  // convergence budget the protocol claims.
  let doneAt = -1;
  for (let i = 0; i < 200; i++) {
    fab.clock += 5000; await fab.tickAll();
    if (doneAt < 0 && ALL.every(m => late.got.includes(ids[m]))) { doneAt = i + 1; break; }
  }
  console.log(`  union COMPLETED at renewal tick: ${doneAt < 0 ? 'NEVER (200 ticks)' : doneAt}`);
  console.log(`  after ticks:                got ${gotWhat().join(',') || '(nothing)'}`);
  console.log(`  root cache now:             ${cacheMsgs(rootOf(fab, topicId), topicId).join(',')}`);

  check('THE CLAIM: a fresh since:all subscriber replays the FULL history m1..m6',
    ALL.every(m => late.got.includes(ids[m])), `got ${gotWhat().join(',')}`);

  console.log(`\n${failed === 0 ? 'PASS' : 'FAIL'}  (${passed} passed, ${failed} failed)`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error(e); process.exit(1); });
