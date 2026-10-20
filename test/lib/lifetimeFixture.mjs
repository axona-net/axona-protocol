// test/lib/lifetimeFixture.mjs — one-manager fixture for the Topic Lifetime
// fences (axona-docs architecture/Axona-Topic-Lifetime-v0.1.md v0.9 @ 11288d7;
// David "Build" 2026-10-20).
//
// A single AxonaManager over a recording dht stub: every routed frame is
// captured with its target, every log line kept, the wall clock AND the
// monotonic clock are the same manual clock (so a fence's clock steps never
// read as a wall-clock jump). `armed` is explicit so each scene can run armed
// and unarmed.
import { AxonaManager } from '../../src/pubsub/AxonaManager.js';
import { makeRole } from '../../src/pubsub/rootClaim.js';
import { T } from '../../src/pubsub/constants.js';
import { sealTestDht } from './testCapability.mjs';

export const idHex = (big) => big.toString(16).padStart(66, '0');
export const lc = (s) => String(s).toLowerCase();
export const REG  = 0x89n << 248n;
export const SELF = REG | 0x1000n;
export const T0   = 1_700_000_000_000;
export const MIN  = 60_000;
export const TP   = (k) => BigInt(k) << 240n;            // distinct 12-hex log prefixes per topic
export const NODE = (k) => REG | (0x2000n + BigInt(k));  // other nodes, farther from every TP than SELF

export function manager({ armed = true, keyspace = false, neighbors = [], verdicts = true, rootReplicas = 2, maxDirect = undefined, extra = {} } = {}) {
  const routed = [];
  const rows = [];
  const clock = { t: T0 };
  const dht = {
    verdictsSupported: verdicts,
    routeMessage: async (target, type, payload) => { routed.push({ target, type, payload, at: clock.t }); return verdicts ? { consumed: true } : undefined; },
    getSelfId: () => SELF,
    onRoutedMessage() {}, onDirectMessage() {},
    neighbors: () => neighbors.map(idHex), bridgeId: () => null,
    findKClosest: async (target, k = 3) => [SELF, ...neighbors].sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k).map(idHex),
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
    servingCapable: () => true,
  };
  const opts = { dht: sealTestDht(dht), topicLifetime: armed, rootReplicas, emitLog: (lvl, ev, ctx) => rows.push({ lvl, event: ev, ctx }), ...extra };
  if (maxDirect !== undefined) opts.maxDirect = maxDirect;
  const am = new AxonaManager(opts);
  am._now = () => clock.t;
  am._mono = () => clock.t;
  am._hostKeyspace = keyspace;
  const ctx = { am, rc: am._rootClaim, routed, rows, clock, self: SELF };
  ctx.tick = async (at) => { if (at != null) clock.t = at; await am.refreshTick(); };
  ctx.log = (ev) => rows.filter((r) => r.event === `pubsub:${ev}`);
  ctx.sent = (type, pred = () => true) => routed.filter((r) => r.type === type && pred(r));
  ctx.held = (t) => am.axonRoles.has(t);
  ctx.role = (t) => am.axonRoles.get(t);
  return ctx;
}

/** Seed a role directly (the construction path fixtures use); `isRoot` roots it with epoch 1. */
export function seed(ctx, topic, { isRoot = true, cache = 0, lastTs = null, createdAt = null } = {}) {
  const at = createdAt ?? ctx.clock.t;
  const role = makeRole(topic, isRoot, at);
  if (isRoot) role.epoch = 1;
  role.lastTs = lastTs ?? at;
  for (let i = 0; i < cache; i++) { role.cache.push({ msgId: `m${i}`, publishTs: at, json: '{}', bytes: 80 }); role.cacheIds.add(`m${i}`); }
  ctx.am.axonRoles.set(topic, role);
  return role;
}

/** A bare SUB from `from` terminating at SELF. `decl` = { standby, demand, gen } (the 4.3 declaration). */
export const meta = (ctx, from) => ({ targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
export async function sub(ctx, topic, from, decl = {}, { viaSelf = false } = {}) {
  // viaSelf: the SUB is via-pinned to SELF (how a CHILD relay's seats renew); a bare terminal SUB promotes a non-root role.
  return ctx.am._onSub({ topicId: idHex(topic), via: viaSelf ? [idHex(ctx.self)] : [], subscriberId: idHex(from), since: 0, ...decl }, meta(ctx, from));
}
/** A REPLICATE from `from` routed to SELF; `gen`/`issuedAt` as the armed sender would set them. */
export async function replicate(ctx, topic, from, { gen = null, issuedAt = null, msgs = [], dels = [] } = {}) {
  const payload = { topicId: idHex(topic), from: idHex(from), msgs, dels };
  if (gen) payload.gen = gen;
  if (issuedAt != null) payload.issuedAt = issuedAt;
  await ctx.am._onReplicate(payload, { targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
  await ctx.am._ingestIdle();
}
/** A TOPIC_TERMINATED from `from` naming `gen`, routed to SELF. */
export async function terminated(ctx, topic, from, gen) {
  return ctx.am._onTopicTerminated({ topicId: idHex(topic), from: idHex(from), gen }, { targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
}
/** A REHOME from `from` naming `upstream`, routed to SELF. */
export async function rehome(ctx, topic, from, upstream) {
  return ctx.am._onRehome({ topicId: idHex(topic), from: idHex(from), upstream: idHex(upstream) }, { targetId: ctx.self, isTerminal: true, hopCount: 1, fromId: idHex(from) });
}
export const gen = (nodeBig, epoch = 1) => ({ root: lc(idHex(nodeBig)), epoch });
/** The attachment nonce this node's last SUB for `topic` carried (a reader's subscription or a relay role's sync ledger); minted if none. */
export function attachNonceFor(ctx, topic) {
  const sub = ctx.am.mySubscriptions.get(topic);
  const role = ctx.am.axonRoles.get(topic);
  const holder = sub || (role && !role.isRoot ? role.sync : null);
  if (!holder) return null;
  if (!holder.attachNonce) ctx.am._emitSubscribe(topic, []);   // mint a real attempt (nonce + sequence) the way the kernel does
  return holder.attachNonce;
}
/** A DELIVER from `from` to SELF. `authorized` (default true) echoes this node's attachment nonce, as a reply to its own SUB does; false = fan-out / a delayed frame. */
export async function deliver(ctx, topic, from, { msgs = [], gen = null, authorized = true } = {}) {
  const payload = { topicId: idHex(topic), from: idHex(from), msgs };
  if (gen) payload.gen = gen;
  if (authorized) {
    // an authorized reply answers a LIVE attempt: if the current one is already committed, renew first (a new SUB, a new attempt), as the kernel would have
    const sub = ctx.am.mySubscriptions.get(topic); const role = ctx.am.axonRoles.get(topic);
    const holder = sub || (role && !role.isRoot ? role.sync : null);
    if (holder && holder.attachNonce && (holder.attachCommitted | 0) >= (holder.attachNonceSeq | 0)) ctx.am._emitSubscribe(topic, []);
    const n = attachNonceFor(ctx, topic); if (n) payload.attach = n;
  }
  return ctx.am._onDeliver(payload, { targetId: ctx.self, isTerminal: true, hopCount: 1 });
}
export { T };
