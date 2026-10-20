// test/lib/mergeFixture.mjs — one-manager fixture for the dual-root fences
// (axona-docs 14a6bd8 Part 4(d); David "build it" 2026-10-17).
//
// A single AxonaManager over a recording dht stub: every routed frame is
// captured, every log line kept, the clock is manual. `mergeCapable` is an
// explicit argument so each fence runs the same scene armed and unarmed.
import { AxonaManager } from '../../src/pubsub/AxonaManager.js';
import { sealTestDht } from './testCapability.mjs';

export const idHex = (big) => big.toString(16).padStart(66, '0');

export const REG  = 0x89n << 248n;
export const SELF = REG | 0x1000n;
export const T    = SELF ^ 0x100n;      // the topic; self is 0x100 away
export const NEAR = T ^ 0x1n;           // strictly closer than self
export const NEAR2 = T ^ 0x2n;          // closer than self, farther than NEAR
export const FAR  = T ^ 0x40000n;       // farther than self

/**
 * @param {object} o
 * @param {boolean} o.mergeCapable
 * @param {bigint[]} [o.neighbors]   ids dht.neighbors() reports (reachability for liveCloserRoot)
 * @param {function} [o.route]       (target, type, payload) => verdict; default records and returns undefined
 * @param {boolean} [o.verdicts]     dht.verdictsSupported
 * @param {bigint[]} [o.lookupPath]  ids dht.lookup resolves with
 */
export function makeManager({ mergeCapable, neighbors = [], route = null, verdicts = false, lookupPath = null, maxDirect = undefined, now = 1_000_000 } = {}) {
  const routed = [];
  const logs = [];
  const clock = { t: now };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: verdicts,
    routeMessage: (target, type, payload) => {
      routed.push({ target, type, payload });
      return route ? route(target, type, payload) : undefined;
    },
    neighbors: () => neighbors.map(idHex),
    bridgeId: () => null,
    servingCapable: () => true,
    lookup: async () => ({ path: (lookupPath || [SELF]).map(idHex), hops: (lookupPath || [SELF]).length - 1, time: 10, found: false }),
  };
  const opts = { dht: sealTestDht(dht), mergeCapable, topicLifetime: false, emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) };
  if (maxDirect !== undefined) opts.maxDirect = maxDirect;
  const am = new AxonaManager(opts);
  am._now = () => clock.t;
  return { am, rc: am._rootClaim, routed, logs, clock };
}

/** Seed `n` cache bodies on a role (custody by body). */
export function seedCache(ctx, role, n = 3) {
  for (let i = 0; i < n; i++) {
    ctx.am._cachePush(role, { msgId: `m${i}-${role.topicId.toString(16).slice(-6)}`, json: `{"k":${i}}`, publishTs: ctx.clock.t - 1000 * (n - i), seq: i + 1 });
  }
  return role;
}

/** Seed one tombstone on a role; `expired` places it past its TTL (no custody). */
export function seedTombstone(ctx, role, { expired = false } = {}) {
  role.tombstones.set('dead-msg', { exp: expired ? ctx.clock.t - 1 : ctx.clock.t + 60_000, killTs: ctx.clock.t - 5000, signer: null, seq: 9 });
  return role;
}

export const log = (ctx, ev) => ctx.logs.filter((l) => l.ev === `pubsub:${ev}`);
export const subsTo = (ctx, hex) => ctx.routed.filter((r) => r.type === 'pubsub:sub' && Array.isArray(r.payload?.via) && r.payload.via[0] === hex);
export const upstreamOf = (ctx, t = T) => (ctx.am._upstream.get(t) || [])[0] || null;

/** A root beacon from `rootBig` for topic `t` at `epoch`, delivered to the manager. */
export function hearBeacon(ctx, rootBig, epoch, t = T, seq = 1) {
  ctx.am._onRootBeacon({ root: idHex(rootBig), topics: [idHex(t)], epochs: [epoch], beaconId: `b-${idHex(rootBig).slice(-6)}-${seq}-${ctx.clock.t}`, layer: 0 }, { targetId: SELF, isTerminal: false, hopCount: 1 });
}

export const settle = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise((r) => setImmediate(r)); };

// ── stage d2 additions: real topics, signed writes, gated roots, a two-node fabric ──
import { AxonaManager as _AM } from '../../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../../src/pubsub/envelope.js';
import { buildKill } from '../../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../../src/identity/index.js';
import { regionCenter } from '../../src/utils/region-names.js';

/** A real topic (descriptor-derived id) and a layout around it: self 0x100 away, NEAR closer, FAR farther. */
export async function realTopic(name) {
  const desc = { region: 'useast', owner: null, name, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  return { desc, topicId, self: topicId ^ 0x100n, near: topicId ^ 0x1n, near2: topicId ^ 0x2n, far: topicId ^ 0x40000n, publisher: topicId ^ 0x50000n };
}

/** Like makeManager but at an explicit self id (for real topics). */
export function makeManagerAt(selfBig, { mergeCapable = true, neighbors = [], route = null, verdicts = false, lookupPath = null, now = 1_000_000, extra = {} } = {}) {
  const routed = [];
  const logs = [];
  const clock = { t: now };
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: () => {},
    verdictsSupported: verdicts,
    routeMessage: (target, type, payload) => { routed.push({ target, type, payload }); return route ? route(target, type, payload) : undefined; },
    neighbors: () => neighbors.map(idHex),
    bridgeId: () => null,
    servingCapable: () => true,
    lookup: async () => ({ path: (lookupPath || [selfBig]).map(idHex), hops: 0, time: 10, found: false }),
  };
  const am = new _AM({ dht: sealTestDht(dht), mergeCapable, rootReplicas: 0, topicLifetime: false, emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }), ...extra });   // unarmed unless `extra` arms it: these scenes predate Topic Lifetime
  am._now = () => clock.t;
  return { am, rc: am._rootClaim, routed, logs, clock, self: selfBig };
}

export const author = await createAuthorIdentity();
/** A signed publish for the topic: {env, json, msgId}. ts = the fixture clock so freshness passes. */
export async function signedPub(ctx, desc, k, seq = k + 1) {
  const env = await buildEnvelope({ topic: desc, message: { k }, seq, identity: author, ts: ctx.clock.t });
  return { env, json: JSON.stringify(env), msgId: env.msgId };
}
/** A signed kill of msgId on the topic, by the fixture author. */
export async function signedKill(ctx, topicId, msgId, seq = 1) {
  return buildKill({ topicId: idHex(topicId), msgId, ts: ctx.clock.t, seq, identity: author });
}
/** Meta for a bare write that terminates at ctx.self, sent by `from`. */
export const bareMeta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 2, fromId: idHex(from) });

/** Root at ctx for topicId with `n` signed bodies in its cache (custody by body), stamped through the real ingress. */
export async function rootWithBodies(ctx, L, n = 3) {
  const role = ctx.rc.become(L.topicId, 'sub-terminal');
  const ids = [];
  for (let k = 0; k < n; k++) {
    ctx.clock.t += 10;
    const w = await signedPub(ctx, L.desc, k);
    const r = await ctx.am._ingestPublish(role, w.json, ctx.am._cutCapture(role));
    if (!r?.ok) throw new Error('rootWithBodies: ingest failed ' + JSON.stringify(r));
    ids.push(w.msgId);
  }
  return { role, ids };
}
/** Cut and gate ctx's root for topicId toward `winner` at `winnerEpoch` under `corr`. */
export function gateRoot(ctx, topicId, winner, winnerEpoch = 3, corr = 'c0000001') {
  return ctx.am._mergeCut(topicId, { winner: idHex(winner), winnerEpoch, corr });
}
export const pubsTo = (ctx, hex) => ctx.routed.filter((r) => r.type === 'pubsub:pub' && Array.isArray(r.payload?.via) && r.payload.via[0] === hex);
export const killsTo = (ctx, hex) => ctx.routed.filter((r) => r.type === 'pubsub:kill' && Array.isArray(r.payload?.via) && r.payload.via[0] === hex);
export const acksFrom = (ctx) => ctx.routed.filter((r) => r.type === 'pubsub:ingestack');
export const flightKeyOf = (topicId, w, epoch) => `${topicId.toString(16)}|${idHex(w)}|${epoch}`;

/** Two-node (or more) fabric of real managers routed by XOR closeness; per-node opts; no verdicts. */
export class Fabric {
  constructor(now = 1_700_000_000_000) { this.nodes = new Map(); this.queue = []; this.clock = now; }
  addNode(idBig, opts = {}) {
    const handlers = new Map(); const self = this; const logs = []; const routed = [];
    const dht = {
      getSelfId: () => idBig, onRoutedMessage: (type, h) => handlers.set(type, h), verdictsSupported: false,
      routeMessage: (target, type, payload) => {
        routed.push({ target, type, payload });
        const dest = self._closestAlive(target); if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, k = 3) => [...self.nodes.keys()].sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k).map(idHex),
      neighbors: () => [...self.nodes.keys()].filter((id) => id !== idBig && self.nodes.get(id).alive).map(idHex),
      bridgeId: () => null, servingCapable: () => true,
      lookup: async () => ({ path: [idHex(idBig)], hops: 0, time: 1, found: false }),
    };
    const am = new _AM({ dht: sealTestDht(dht), now: () => self.clock, rootReplicas: 0, mergeCapable: true, topicLifetime: false, emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }), ...opts });
    const rec = { id: idBig, am, rc: am._rootClaim, handlers, logs, routed, alive: true, self: idBig, clock: { get t() { return self.clock; }, set t(v) { self.clock = v; } } };
    this.nodes.set(idBig, rec); return rec;
  }
  _closestAlive(target) { let best = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
  /** Drain the wire queue AND every node's ingest queue until both stay quiet across a few macrotask yields. */
  async settle() {
    let i = 0;
    for (let round = 0; ; round++) {
      if (round > 2000) throw new Error('settle: did not converge');
      while (this.queue.length) {
        if (++i > 50_000) throw new Error('settle: did not converge');
        const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue;
        const h = n.handlers.get(j.type); if (h) await h(j.payload, j.meta);
      }
      let flushed = false;
      for (const n of this.nodes.values()) {
        if (n.alive && ((n.am._ingestQueue && n.am._ingestQueue.length) || n.am._ingestPumping)) { await n.am._ingestIdle(); flushed = true; }
      }
      for (let k = 0; k < 4; k++) await new Promise((r) => setImmediate(r));
      if (!this.queue.length && !flushed) {
        // one more quiet confirmation: async work (a hash, a verify) may still enqueue
        for (let k = 0; k < 4; k++) await new Promise((r) => setImmediate(r));
        if (!this.queue.length) return;
      }
    }
  }
}
export const LOC = regionCenter('useast');
export const freshId = async () => BigInt('0x' + (await createNodeIdentity(LOC)).id);

// ── stage d3: the split scene on the fabric ──
import { ROOT_VERIFY_FIRST_MS as _RVF } from '../../src/pubsub/constants.js';
/**
 * Two real managers W (closer) and L (farther), both root for a real topic,
 * L not hearing W (W's beacons silenced), L's lookup visiting W. `fab.tap`
 * (j => j | null | j') edits or drops wire jobs before dispatch.
 */
export async function splitScene(name, { wOpts = {}, lOpts = {} } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await freshId(), b = await freshId();
  const [wId, lId] = ((a ^ Lt.topicId) < (b ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(wId, wOpts); const Ln = fab.addNode(lId, lOpts);
  W.am._announceRoot = () => {};
  Ln.am.dht.lookup = async () => ({ path: [idHex(lId), idHex(wId)], hops: 1, time: 1, found: false });
  const wRole = W.rc.become(Lt.topicId, 'sub-terminal');
  const lRole = Ln.rc.become(Lt.topicId, 'sub-terminal');
  const meta = (node) => ({ targetId: node.id, isTerminal: true, hopCount: 2, fromId: idHex(Lt.publisher) });
  const stamp = async (node, k, size = 0) => {
    fab.clock += 10;
    const env = await buildEnvelope({ topic: Lt.desc, message: size ? { k, pad: 'x'.repeat(size) } : { k }, seq: k, identity: author, ts: fab.clock });
    await node.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, meta(node));
    await fab.settle();
    return env.msgId;
  };
  const kill = async (node, msgId, seq = 1) => {
    fab.clock += 10;
    const k = await buildKill({ topicId: idHex(Lt.topicId), msgId, ts: fab.clock, seq, identity: author });
    await node.handlers.get('pubsub:kill')({ topicId: idHex(Lt.topicId), kill: k }, meta(node));
    await fab.settle();
  };
  /** Fire L's self-verify (the trigger) and let the machine run to quiet. */
  const trigger = async () => {
    lRole.formedAt = fab.clock - _RVF - 1; lRole.lastVerify = 0;
    Ln.am._verifyRoots(fab.clock);
    for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r));
    await fab.settle();
  };
  const logs = (n, ev) => n.logs.filter((l) => l.ev === `pubsub:${ev}`);
  const frames = (type, pred = () => true) => fab.sent.filter((j) => j.type === type && pred(j));
  return { Lt, fab, W, Ln, wId, lId, wRole, lRole, stamp, kill, trigger, logs, frames, meta };
}
// Fabric: a tap on every job, and a record of every job sent
Fabric.prototype._origSettle = Fabric.prototype.settle;
Fabric.prototype.settle = async function () {
  if (!this.sent) this.sent = [];
  if (!this._tapped) {
    this._tapped = true;
    const q = this.queue; const self = this;
    const push = q.push.bind(q);
    q.push = (j) => { self.sent.push(j); const t = self.tap ? self.tap(j) : j; return t ? push(t) : q.length; };
  }
  return this._origSettle();
};
