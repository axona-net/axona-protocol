#!/usr/bin/env node
// =====================================================================
// fence_merge_carrier_named.mjs — dual-root packet Part 2b / Part 5.1
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// A HISTORY THAT CROSSES INTO A ROOT LEAVES A RECORD THAT NAMES ITS CARRIER.
// On 2026-10-17 03:04Z eleven #council entries arrived at the Windows
// relay's root from a reconnecting seat and no log on six hosts said how:
// the HANDOFF ingest logged at debug unless something was rejected, and the
// SUB low-water pull (SPLIT_UNION) logged nothing. The carrier stayed a
// hypothesis. This fence runs both paths on the manager fabric and
// requires each to leave an info line at the receiver carrying `policy`
// and the sender's prefix, distinguishable from the other.
//
// A: HANDOFF — L roots T while W is away, publishes three entries, W
//    returns, L leaves; W must hold the three and log handoff-ingested
//    {policy:'HANDOFF', from:L, sent:3, held:3} at info, and no
//    split-union line.
// B: SPLIT_UNION — L roots T with three old entries, W then becomes the
//    terminus and stamps two newer ones, L's stale root demotes under W
//    with its hw/lw; W must pull L's full range, hold five, and log
//    split-union-pull {policy:'SPLIT_UNION', from:L} at info, and no
//    handoff line.
//
// Mutant (red): put handoff-ingested back at debug without `policy` → A's
// record checks fail; delete the split-union-pull line → B's fail.
// Run: node test/fence_merge_carrier_named.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { sealTestDht } from './lib/testCapability.mjs';

const LOC = regionCenter('useast');
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const idHex = (big) => big.toString(16).padStart(66, '0');

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = 1_700_000_000_000; }
  addNode(idBig) {
    const handlers = new Map(); const self = this; const logs = [];
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      verdictsSupported: false,
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => id)
        .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k).map(idHex),
      neighbors: () => [...self.nodes.keys()].filter((id) => id !== idBig && self.nodes.get(id).alive).map(idHex),
      bridgeId: () => null,
    };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, rootReplicas: 0,
      emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) });
    const rec = { id: idBig, am, handlers, alive: true, logs };
    this.nodes.set(idBig, rec);
    return rec;
  }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(cap = 100_000) {
    let i = 0;
    for (let round = 0; ; round++) {
      if (round > 1000) throw new Error('settle: did not converge');
      while (this.queue.length) {
        if (++i > cap) throw new Error('settle: did not converge');
        const j = this.queue.shift(); const n = this.nodes.get(j.dest);
        if (!n || !n.alive) continue;
        const h = n.handlers.get(j.type); if (!h) continue;
        await h(j.payload, j.meta);
      }
      let flushed = false;
      for (const n of this.nodes.values()) {
        if (n.alive && ((n.am._ingestQueue && n.am._ingestQueue.length) || n.am._ingestPumping)) { await n.am._ingestIdle(); flushed = true; }
      }
      if (!this.queue.length && !flushed) return;
    }
  }
}
const cacheOf = (rec, t) => rec.am.axonRoles.get(t)?.cache ?? [];
const rec = (node, ev) => node.logs.filter((l) => l.ev === `pubsub:${ev}`);

/** Two nodes: W strictly closer to the topic than L. Returns {fab, L, W, topicId, desc}. */
async function pair(name) {
  const desc = { region: 'useast', owner: null, name, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const fab = new Fabric();
  const a = BigInt('0x' + (await createNodeIdentity(LOC)).id);
  const b = BigInt('0x' + (await createNodeIdentity(LOC)).id);
  const [wId, lId] = ((a ^ topicId) < (b ^ topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId); const L = fab.addNode(lId);
  return { fab, L, W, topicId, desc };
}
async function publishAt(fab, node, desc, topicId, author, k, seq) {
  const e = await buildEnvelope({ topic: desc, message: { k }, seq, identity: author, ts: fab.clock });
  node.am.pubsubPublish(topicId, JSON.stringify(e));
  await fab.settle();
  return e.msgId;
}

console.log('fence_merge_carrier_named: Part 2b/5.1 — a crossing history names its carrier at the receiver');
const author = await createAuthorIdentity();

{ // A — HANDOFF
  const { fab, L, W, topicId, desc } = await pair('carrier-handoff');
  W.alive = false;                                              // L is the terminus while W is away
  const ids = [];
  for (let k = 0; k < 3; k++) { fab.clock += 1000; ids.push(await publishAt(fab, L, desc, topicId, author, k, k + 1)); }
  check('A setup: L roots T with three entries while W is away', L.am.axonRoles.get(topicId)?.isRoot === true && cacheOf(L, topicId).length === 3, `cache=${cacheOf(L, topicId).length}`);
  W.alive = true;
  await L.am.pubsubLeaveHandoff(); await fab.settle();
  const line = rec(W, 'handoff-ingested')[0];
  check('A W holds the three entries after L leaves', ids.every((id) => W.am.axonRoles.get(topicId)?.cacheIds.has(id)), `cache=${cacheOf(W, topicId).length}`);
  check('A W logs handoff-ingested at INFO naming the policy and the leaver', !!line && line.lvl === 'info' && line.ctx.policy === 'HANDOFF' && line.ctx.from === idHex(L.id).slice(0, 12) && line.ctx.sent === 3 && line.ctx.held === 3, JSON.stringify(line));
  check('A …and no split-union line: the record distinguishes the carrier', rec(W, 'split-union-pull').length === 0);
}
{ // B — SPLIT_UNION
  const { fab, L, W, topicId, desc } = await pair('carrier-split-union');
  W.alive = false;
  const oldIds = [];
  for (let k = 0; k < 3; k++) { fab.clock += 1000; oldIds.push(await publishAt(fab, L, desc, topicId, author, k, k + 1)); }
  W.alive = true; fab.clock += 60_000;
  const newIds = [];
  for (let k = 3; k < 5; k++) { fab.clock += 1000; newIds.push(await publishAt(fab, L, desc, topicId, author, k, k + 1)); }   // routes to W now
  // THE KERNEL RUNS THIS CASE ITSELF. When L's publish routed to W, W was born
  // root (pub-terminal) and announced; L's stale root heard the closer beacon
  // and yielded by rootElection's beacon path, and the SUB that demote() sends
  // carried L's low water, below W's; W pulled L's full range as SPLIT_UNION.
  // So the record under test is produced by the shipped path, not by a manual
  // demote, which is the stronger reading.
  check('B setup: W is the terminus and root; L\'s stale root yielded to W\'s beacon', W.am.axonRoles.get(topicId)?.isRoot === true && L.am.axonRoles.get(topicId)?.isRoot === false && (L.am._upstream.get(topicId) || [])[0] === idHex(W.id), `W.isRoot=${W.am.axonRoles.get(topicId)?.isRoot} L.isRoot=${L.am.axonRoles.get(topicId)?.isRoot}`);
  const lines = rec(W, 'split-union-pull');
  const line = lines[0];
  check('B W holds all five: the three older pulled from L on its SUB\'s low water, the two it stamped', [...oldIds, ...newIds].every((id) => W.am.axonRoles.get(topicId)?.cacheIds.has(id)), `cache=${cacheOf(W, topicId).length}`);
  check('B exactly one split-union pull was recorded at W', lines.length === 1, `n=${lines.length}`);
  check('B W logs split-union-pull at INFO naming the policy and the sender', !!line && line.lvl === 'info' && line.ctx.policy === 'SPLIT_UNION' && line.ctx.from === idHex(L.id).slice(0, 12), JSON.stringify(line));
  check('B …and no handoff line: the record distinguishes the carrier', rec(W, 'handoff-ingested').length === 0);
}

console.log(`\nfence_merge_carrier_named: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
