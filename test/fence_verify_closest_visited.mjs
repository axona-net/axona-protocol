#!/usr/bin/env node
// =====================================================================
// fence_verify_closest_visited.mjs — dual-root packet (b) (axona-docs
// 14a6bd8, Part 4(b); David "build it" 2026-10-17).
//
// THE ROOT SELF-VERIFY READS THE CLOSEST NODE THE WALK VISITED, NOT THE
// WALK'S LAST HOP. A greedy walk with no memory across hops bounces between
// two nodes that are each other's closest-known until the hop cap, so its
// last hop is an arbitrary node. On 2026-10-17 a seat's walk for #council
// visited the true root at hop 9 of 41 and ended on a node 0x00aa… from the
// topic against the root's 0x00039d…; _verifyRoots read hop 41 and kept a
// spurious claim for nine hours.
//
// Checks: a strictly-closer node mid-path demotes (A); a closer LAST hop still
// demotes (B); a path with no closer node keeps the claim (C); a path of self
// alone keeps it (D); the demotion pairs with the confirming SUB toward the
// closer node, the existing one-sided-link remedy (E); a role that is no
// longer root when the lookup resolves is left alone (F).
//
// Mutant (red): restore `r.path[r.path.length - 1]` as the terminus → A fails.
// Run: node test/fence_verify_closest_visited.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { ROOT_VERIFY_FIRST_MS } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const idHex = (big) => big.toString(16).padStart(66, '0');

const REG  = 0x89n << 248n;
const SELF = REG | 0x1000n;
const T    = SELF ^ 0x100n;             // the topic; self is 0x100 away
const NEAR = T ^ 0x1n;                  // strictly closer than self (distance 1)
const FAR1 = T ^ 0x40000n;              // farther than self
const FAR2 = T ^ 0x80000n;              // farther still

function makeManager(path) {
  const routed = [];
  const logs = [];
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: false,
    routeMessage: (target, type, payload) => { routed.push({ target, type, payload }); },
    neighbors: () => [],
    bridgeId: () => null,
    lookup: async () => ({ path: path.map(idHex), hops: path.length - 1, time: 4000, found: false }),
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) });
  am._now = () => clock.t;
  return { am, rc: am._rootClaim, routed, logs, clock };
}

/** Become root for T, make the first verify due, run it, let the lookup resolve. */
async function verifyOnce({ am, rc, clock }) {
  const role = rc.become(T, 'sub-terminal');
  role.formedAt = clock.t - ROOT_VERIFY_FIRST_MS - 1;
  role.lastVerify = 0;
  am._verifyRoots(clock.t);
  for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  return am.axonRoles.get(T);
}
const demotedTo = (ctx) => (ctx.am._upstream.get(T) || [])[0] || null;
const demoteLog = (ctx) => ctx.logs.find((l) => l.ev === 'pubsub:root-verify-demote');

console.log('fence_verify_closest_visited: (b) — the verify reads the closest visited node, not the last hop');

{ // A — the case that cost nine hours: closer node mid-path, far node last.
  const ctx = makeManager([SELF, FAR1, NEAR, FAR2]);
  const role = await verifyOnce(ctx);
  check('A closer node at hop 2 of 3, far node last: the claim yields to the closer node', role.isRoot === false && demotedTo(ctx) === idHex(NEAR), `isRoot=${role.isRoot} to=${String(demotedTo(ctx)).slice(0, 10)}`);
  check('A …and the demote is logged with the closer node', !!demoteLog(ctx) && demoteLog(ctx).ctx.to === idHex(NEAR).slice(0, 10));
}
{ // B — today's case still holds: the closer node IS the last hop.
  const ctx = makeManager([SELF, FAR1, NEAR]);
  const role = await verifyOnce(ctx);
  check('B closer node as the last hop: yields, as before', role.isRoot === false && demotedTo(ctx) === idHex(NEAR));
}
{ // C — no closer node anywhere: keep the claim.
  const ctx = makeManager([SELF, FAR1, FAR2]);
  const role = await verifyOnce(ctx);
  check('C no strictly-closer node visited: the claim stands', role.isRoot === true && !demoteLog(ctx));
}
{ // D — self alone: confirmed.
  const ctx = makeManager([SELF]);
  const role = await verifyOnce(ctx);
  check('D a path of self alone: the claim stands', role.isRoot === true && !demoteLog(ctx));
}
{ // E — the demotion pairs with the confirming SUB toward the closer node (one-sided-link lesson).
  const ctx = makeManager([SELF, FAR1, NEAR, FAR2]);
  await verifyOnce(ctx);
  const sub = ctx.routed.find((r) => r.type === 'pubsub:sub' && Array.isArray(r.payload?.via) && r.payload.via[0] === idHex(NEAR));
  check('E the demotion sends the confirming SUB via the closer node', !!sub, JSON.stringify(ctx.routed.map((r) => r.type)));
}
{ // F — the role stopped being root while the lookup was in flight: nothing moves.
  const ctx = makeManager([SELF, FAR1, NEAR, FAR2]);
  const role = ctx.rc.become(T, 'sub-terminal');
  role.formedAt = ctx.clock.t - ROOT_VERIFY_FIRST_MS - 1; role.lastVerify = 0;
  ctx.am._verifyRoots(ctx.clock.t);
  ctx.rc.demote(T, idHex(FAR1), 'test-demote');            // something else yielded first
  for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));
  check('F a role already demoted when the lookup resolves is left where it is', ctx.am.axonRoles.get(T).isRoot === false && demotedTo(ctx) === idHex(FAR1) && !demoteLog(ctx));
}

console.log(`\nfence_verify_closest_visited: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
