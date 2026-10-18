#!/usr/bin/env node
// =====================================================================
// fence_merge_adopt_pending_delegation.mjs — dual-root packet Part 4(d)
// "pending delegation" (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// THE PARENT RELEASES DELEGATED SUBSCRIBERS ONLY ON A CONFIRMED ADOPTION.
// _promoteChild deleted the batch from role.subscribers at the send; a child
// whose adoptChild is now refused (a root with custody), or that never heard
// the ADOPT, left them with no server. The batch moves to
// role.merge.pendingDelegation under the ADOPT's nonce, the parent keeps
// serving, and ADOPTACK releases the ids it names; a refusal, a lost ack or
// the ADOPT_ACK_MS sweep keeps them.
//
// Checks: (1) after _promoteChild the ADOPT carries a nonce and the batch is
// still in role.subscribers; (2) the ledger holds it; (3) a publish during
// the window still fans to the batch from the parent; (4) a full ack
// releases the batch and clears the ledger; (5) a PARTIAL ack releases only
// the named ids; (6) a refused ack keeps every id and un-flags the child;
// (7) the ADOPT_ACK_MS sweep rolls back an unanswered delegation the same
// way, and the leaf is a leaf again; (8) an ack for an unknown nonce changes
// nothing; (9) end to end on the manager fabric: the child adopts, acks, and
// the parent's seat shrinks by the batch; (10) UNARMED the ledger is used
// just the same — the protocol does not depend on the merge option.
//
// Mutant (red): delete the batch at the send as before → 1 and 3 fail;
// release on timeout → 7 fails.
// Run: node test/fence_merge_adopt_pending_delegation.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { ADOPT_ACK_MS } from '../src/pubsub/constants.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { makeManager, log, idHex, SELF, T } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const LEAF = idHex(T ^ 0x6001n), X1 = idHex(T ^ 0x6002n), X2 = idHex(T ^ 0x6003n);

/** Root with three leaves seated: LEAF, X1, X2. _promoteChild promotes LEAF and delegates [X1, X2] (DELEGATE_BATCH ≥ 2). */
function scene(mergeCapable = true) {
  const ctx = makeManager({ mergeCapable, maxDirect: 3 });
  const role = ctx.rc.become(T, 'sub-terminal');
  for (const s of [LEAF, X1, X2]) role.subscribers.set(s, { since: 0, lastRenewed: ctx.clock.t });
  const ok = ctx.am._promoteChild(role);
  const adopt = ctx.routed.find((r) => r.type === 'pubsub:adopt');
  return { ctx, role, ok, adopt, nonce: adopt?.payload?.nonce };
}
const ack = (ctx, nonce, accepted, refused) => ctx.am._onAdoptAck({ topicId: idHex(T), nonce, accepted, ...(refused ? { refused } : {}) }, { targetId: SELF, isTerminal: true, hopCount: 1 });

console.log('fence_merge_adopt_pending_delegation: 4(d) — the parent keeps serving until the child confirms');

{ // 1–3
  const { ctx, role, ok, adopt, nonce } = scene();
  check('1 the ADOPT carries a nonce and the delegated batch is still seated at the parent', ok === true && typeof nonce === 'string' && nonce.length === 16 && adopt.payload.subs.length === 2 && role.subscribers.has(X1) && role.subscribers.has(X2) && role.children.has(LEAF), JSON.stringify(adopt?.payload));
  const pend = role.merge?.pendingDelegation?.get(nonce);
  check('2 the ledger holds the batch under the nonce with the child', !!pend && pend.child === LEAF && pend.subs.length === 2 && pend.subs.includes(X1) && pend.subs.includes(X2));
  ctx.am._fanout(role, { msgId: 'live-1', json: '{"k":1}', publishTs: ctx.clock.t, seq: 7 }, null);
  const delivered = ctx.routed.filter((r) => r.type === 'pubsub:deliver').map((r) => idHex(r.target));
  check('3 a publish during the window still fans to the delegated subscribers from the parent', delivered.includes(X1) && delivered.includes(X2), JSON.stringify(delivered.map((d) => d.slice(-4))));
}
{ // 4 full ack
  const { ctx, role, nonce } = scene();
  ack(ctx, nonce, [X1, X2]);
  const s = log(ctx, 'delegation-settled')[0];
  check('4 a full ack releases the batch and clears the ledger', !role.subscribers.has(X1) && !role.subscribers.has(X2) && role.subscribers.has(LEAF) && role.children.has(LEAF) && role.merge.pendingDelegation.size === 0 && s?.ctx.why === 'acked' && s.ctx.released === 2 && s.ctx.kept === 0, JSON.stringify(s?.ctx));
}
{ // 5 partial ack
  const { ctx, role, nonce } = scene();
  ack(ctx, nonce, [X1]);
  const s = log(ctx, 'delegation-settled')[0];
  check('5 a partial ack releases only the named id; the other stays seated', !role.subscribers.has(X1) && role.subscribers.has(X2) && role.children.has(LEAF) && s?.ctx.released === 1 && s.ctx.kept === 1);
}
{ // 6 refused
  const { ctx, role, nonce } = scene();
  ack(ctx, nonce, [X1, X2], 'custody');
  const s = log(ctx, 'delegation-settled')[0];
  check('6 a refused ack keeps every id and un-flags the child', role.subscribers.has(X1) && role.subscribers.has(X2) && !role.children.has(LEAF) && role.subscribers.has(LEAF) && s?.ctx.why === 'refused:custody' && s.ctx.released === 0 && s.ctx.kept === 2);
}
{ // 7 timeout sweep
  const { ctx, role } = scene();
  ctx.clock.t += ADOPT_ACK_MS - 1; ctx.am._delegationSweep(ctx.clock.t);
  const early = role.merge.pendingDelegation.size === 1 && role.subscribers.has(X1);
  ctx.clock.t += 2; ctx.am._delegationSweep(ctx.clock.t);
  const s = log(ctx, 'delegation-settled')[0];
  check('7 the ADOPT_ACK_MS sweep rolls back an unanswered delegation: ids kept, leaf a leaf again, ledger cleared', early && role.merge.pendingDelegation.size === 0 && role.subscribers.has(X1) && role.subscribers.has(X2) && !role.children.has(LEAF) && s?.ctx.why === 'ack-timeout' && s.ctx.released === 0);
}
{ // 8 unknown nonce
  const { ctx, role } = scene();
  ack(ctx, 'ffffffffffffffff', [X1, X2]);
  check('8 an ack for an unknown nonce changes nothing', role.subscribers.has(X1) && role.subscribers.has(X2) && role.merge.pendingDelegation.size === 1 && log(ctx, 'delegation-settled').length === 0);
}
{ // 9 end to end on a fabric
  class Fabric {
    constructor() { this.nodes = new Map(); this.queue = []; this.clock = 1_700_000_000_000; }
    addNode(idBig, opts = {}) {
      const handlers = new Map(); const self = this; const logs = [];
      const dht = {
        getSelfId: () => idBig, onRoutedMessage: (type, h) => handlers.set(type, h), verdictsSupported: false,
        routeMessage: (target, type, payload) => { const dest = self._closestAlive(target); if (dest !== null) self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } }); },
        findKClosest: async (target, k = 3) => [...self.nodes.keys()].sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k).map(idHex),
        neighbors: () => [...self.nodes.keys()].filter((id) => id !== idBig).map(idHex), bridgeId: () => null,
      };
      const am = new AxonaManager({ dht: sealTestDht(dht), now: () => self.clock, rootReplicas: 0, emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }), ...opts });
      const rec = { id: idBig, am, handlers, logs }; this.nodes.set(idBig, rec); return rec;
    }
    _closestAlive(target) { let best = null, bd = null; for (const id of this.nodes.keys()) { const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
    async settle() { let i = 0; while (this.queue.length) { if (++i > 10_000) throw new Error('settle'); const j = this.queue.shift(); const h = this.nodes.get(j.dest)?.handlers.get(j.type); if (h) await h(j.payload, j.meta); } for (let k = 0; k < 4; k++) await new Promise((r) => setImmediate(r)); }
  }
  const LOC = regionCenter('useast');
  const topic = T;
  const fab = new Fabric();
  const ids = []; for (let i = 0; i < 2; i++) ids.push(BigInt('0x' + (await createNodeIdentity(LOC)).id));
  ids.sort((a, b) => { const da = a ^ topic, db = b ^ topic; return da < db ? -1 : da > db ? 1 : 0; });
  const P = fab.addNode(ids[0], { maxDirect: 3, mergeCapable: true });
  const C = fab.addNode(ids[1], { mergeCapable: true });
  const role = P.am._rootClaim.become(topic, 'sub-terminal');
  const chex = idHex(C.id);
  for (const s of [chex, X1, X2]) role.subscribers.set(s, { since: 0, lastRenewed: fab.clock });
  P.am._promoteChild(role);
  const pendingBefore = role.merge?.pendingDelegation?.size === 1 && role.subscribers.has(X1);
  await fab.settle();
  const croles = C.am.axonRoles.get(topic);
  const settled = P.logs.find((l) => l.ev === 'pubsub:delegation-settled');
  check('9 end to end: the child adopts and acks; the parent releases the batch and keeps the child', pendingBefore && !!croles && croles.isRoot === false && croles.subscribers.has(X1) && croles.subscribers.has(X2) && !role.subscribers.has(X1) && !role.subscribers.has(X2) && role.children.has(chex) && role.merge.pendingDelegation.size === 0 && settled?.ctx.why === 'acked' && settled.ctx.released === 2, JSON.stringify({ pendingBefore, c: !!croles, cs: croles ? [...croles.subscribers.keys()].map((s) => s.slice(-4)) : null, ps: [...role.subscribers.keys()].map((s) => s.slice(-4)), settled: settled?.ctx }));
}
{ // 10 unarmed
  const { ctx, role, nonce } = scene(false);
  const held = role.subscribers.has(X1) && role.merge?.pendingDelegation?.size === 1;
  ack(ctx, nonce, [X1, X2]);
  check('10 unarmed, the ledger works the same: held until the ack, released on it', held && !role.subscribers.has(X1) && role.merge.pendingDelegation.size === 0);
}

console.log(`\nfence_merge_adopt_pending_delegation: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
