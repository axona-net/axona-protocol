// =====================================================================
// smoke_pubsub_kill.mjs — kill = a verified publish with a delete side-effect
// (v4.8.7). Covers the four properties of the reworked kill:
//   1. AUTHORIZED kill (signed by the message's author) deletes the cached copy,
//      tombstones it, and delivers {deleted} to subscribers.
//   2. FORGERY rejected: a kill signed by a NON-author is dropped at the root
//      when the target is held (authorship enforced).
//   3. SELF-HEAL: the tombstone rides renewal replay like a cached message, so a
//      late since:'all' subscriber (and one that missed the live delete) receives
//      the kill — under message loss it recovers, exactly like a publish.
//   4. PROVISIONAL + enforce-on-arrival: a kill that reaches the root BEFORE the
//      target is accepted provisionally; when the target arrives it's suppressed
//      iff its author matches the kill's signer — a forged early kill is revoked.
//
// Run: node test/smoke_pubsub_kill.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');
function lcg(s){s>>>=0;return()=>{s=(s*1664525+1013904223)>>>0;return s/4294967296;};}

class Fabric {
  constructor({ drop = 0, seed = 1 } = {}) { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); this.drop = drop; this.rand = lcg(seed); }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target); if (dest === null) return;
        if (self.drop && self.rand() < self.drop) return;            // packet lost
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
  _closest(target){ let b=null,bd=null; for(const[id,n]of this.nodes){if(!n.alive)continue;const d=id^target;if(bd===null||d<bd){bd=d;b=id;}} return b; }
  async settle(cap=500000){ let i=0; while(this.queue.length){ if(++i>cap) throw new Error('settle cap'); const j=this.queue.shift(); const n=this.nodes.get(j.dest); if(!n||!n.alive)continue; const h=n.handlers.get(j.type); if(h) await h(j.payload,j.meta); } }
  async tickAll(){ for(const n of this.nodes.values()) if(n.alive) await n.am.refreshTick(); await this.settle(); }
}
const root = (fab, topicBig) => fab.nodes.get(fab._closest(topicBig));
const cacheHas = (rec, t, id) => (rec.am.axonRoles.get(t)?.cache || []).some(c => c.msgId === id);
const tombstoned = (rec, t, id) => rec.am.axonRoles.get(t)?.tombstones?.has(id);

async function mkNodes(fab, n, salt) { const a = []; for (let i = 0; i < n; i++) { const id = await createNodeIdentity({ lat:(i*11+salt)%80-40, lng:(i*17+salt)%300-150 }); a.push(fab.addNode(BigInt('0x'+id.id))); } return a; }

async function main() {
  console.log('Axona pub/sub — kill = verified publish + delete side-effect (v4.8.7)');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();   // a DIFFERENT author (forger)

  // ── 1+2+3: authorized kill, forgery reject, self-heal under loss ──────
  {
    const fab = new Fabric();
    const nodes = await mkNodes(fab, 8, 1);
    const desc = { region:'useast', owner:null, name:'kill-basic', write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    const subA = nodes[0], subB = nodes[1], pub = nodes[2];
    subA.am.pubsubSubscribe(topicId); subB.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 6000; await fab.tickAll();
    // publish M (author = alice)
    const e = await buildEnvelope({ topic: desc, message: { hi: 1 }, seq: 1, identity: alice, ts: fab.clock });
    pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
    const r = root(fab, topicId);
    check('1a. message delivered + cached at root', cacheHas(r, topicId, e.msgId) && subA.got.includes(e.msgId) && subB.got.includes(e.msgId));

    // FORGERY: bob (not the author) tries to kill alice's message → rejected
    const forged = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 1, identity: bob });
    fab.clock += 100; r.am.pubsubKill(topicId, forged); await fab.settle();
    check('2. forged kill (non-author) REJECTED — message still cached, not tombstoned',
      cacheHas(r, topicId, e.msgId) && !tombstoned(r, topicId, e.msgId), `(cached=${cacheHas(r,topicId,e.msgId)} tomb=${tombstoned(r,topicId,e.msgId)})`);

    // AUTHORIZED: alice kills her own message → accepted
    const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, identity: alice });
    fab.clock += 100; r.am.pubsubKill(topicId, kill); await fab.settle();
    check('1b. authorized kill removes target from cache + tombstones it', !cacheHas(r, topicId, e.msgId) && tombstoned(r, topicId, e.msgId));
    check('1c. subscribers received the {deleted} callback', subA.dels.includes(e.msgId) && subB.dels.includes(e.msgId));

    // SELF-HEAL: a late since:'all' subscriber learns of the kill via replay
    const late = fab.addNode(BigInt('0x' + (await createNodeIdentity({ lat: 3, lng: 4 })).id));
    late.am._lastSeenTsByTopic.set(topicId, 0); late.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 5000; await fab.tickAll(); await fab.settle();
    check('3a. late since:all subscriber received the kill (tombstone replays)', late.dels.includes(e.msgId));
    check('3b. late subscriber did NOT receive the killed message body', !late.got.includes(e.msgId));
  }

  // ── 3c: kill SELF-HEALS under message loss (the residual this closes) ──
  {
    let recovered = 0, total = 0;
    for (let seed = 1; seed <= 8; seed++) {
      const fab = new Fabric({ drop: 0, seed });   // publish loss-free
      const nodes = await mkNodes(fab, 8, seed);
      const desc = { region:'useast', owner:null, name:`kill-loss-${seed}`, write:'open' };
      const topicId = await deriveTopicIdBig(desc);
      nodes[0].am.pubsubSubscribe(topicId); await fab.settle(); fab.clock += 6000; await fab.tickAll();
      const e = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: alice, ts: fab.clock });
      nodes[1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
      const r = root(fab, topicId);
      const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, identity: alice });
      fab.clock += 100; r.am.pubsubKill(topicId, kill); await fab.settle();
      // now LOSS on: late subscribers join, must still converge on the kill over renewals
      fab.drop = 0.3;
      const subs = [];
      for (let i = 0; i < 3; i++) { const id = await createNodeIdentity({ lat: 5+i, lng: 6+i }); const s = fab.addNode(BigInt('0x'+id.id)); s.am._lastSeenTsByTopic.set(topicId, 0); s.am.pubsubSubscribe(topicId); subs.push(s); }
      for (let t = 0; t < 30; t++) { fab.clock += 5000; await fab.tickAll(); }
      for (const s of subs) { total++; if (s.dels.includes(e.msgId)) recovered++; }
    }
    check('3c. kill self-heals under 30% loss (all late subs eventually get it)', recovered === total, `(${recovered}/${total})`);
  }

  // ── 4: provisional kill (before target) + enforce-on-arrival ──────────
  {
    // 4a: authorized early kill → the later target is SUPPRESSED
    const fab = new Fabric();
    const nodes = await mkNodes(fab, 8, 99);
    const desc = { region:'useast', owner:null, name:'kill-early', write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    nodes[0].am.pubsubSubscribe(topicId); await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const e = await buildEnvelope({ topic: desc, message: { e: 1 }, seq: 1, identity: alice, ts: fab.clock });
    const r = root(fab, topicId);
    // kill arrives FIRST (root doesn't hold the target yet) — provisional accept
    const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 1, identity: alice });
    r.am.pubsubKill(topicId, kill); await fab.settle();
    check('4a-i. provisional tombstone recorded before target seen', tombstoned(r, topicId, e.msgId));
    // now the (authorized) target arrives → suppressed (author matches kill signer)
    fab.clock += 100; nodes[1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
    check('4a-ii. authorized early kill SUPPRESSES the later target', !cacheHas(r, topicId, e.msgId) && !nodes[0].got.includes(e.msgId));

    // 4b: FORGED early kill → revoked when the authentic target arrives
    const fab2 = new Fabric();
    const n2 = await mkNodes(fab2, 8, 123);
    const desc2 = { region:'useast', owner:null, name:'kill-early-forged', write:'open' };
    const t2 = await deriveTopicIdBig(desc2);
    n2[0].am.pubsubSubscribe(t2); await fab2.settle(); fab2.clock += 6000; await fab2.tickAll();
    const e2 = await buildEnvelope({ topic: desc2, message: { e: 2 }, seq: 1, identity: alice, ts: fab2.clock });
    const r2 = root(fab2, t2);
    const forgedEarly = await buildKill({ topicId: idHex(t2), msgId: e2.msgId, seq: 1, identity: bob });  // bob ≠ author
    r2.am.pubsubKill(t2, forgedEarly); await fab2.settle();                 // provisional (signer=bob)
    fab2.clock += 100; n2[1].am.pubsubPublish(t2, JSON.stringify(e2)); await fab2.settle();
    check('4b. forged early kill REVOKED on arrival — authentic message delivered',
      cacheHas(r2, t2, e2.msgId) && n2[0].got.includes(e2.msgId) && !tombstoned(r2, t2, e2.msgId));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
