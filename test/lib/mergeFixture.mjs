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
  const opts = { dht: sealTestDht(dht), mergeCapable, emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) };
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
