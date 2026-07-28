// =====================================================================
// smoke_decline_paths.mjs — RULE 5 FENCE: "a nullable return is a protocol
// change; the change is not done until every call site handles it."
//
// THE DEFECT THIS GUARDS (F1, found 2026-07-28, live on both prod bridges).
// v4.46.0 gave RootClaim.become() a nullable return — the HARD bridge fence.
// Four of its five callers were taught about it. The fifth,
// RootClaim.claimReachable(), was not:
//
//     const role = m.axonRoles.get(t) || this.become(t, 'reachable-fallback');
//     this._set(role, true, 'reachable-fallback');   // role === null → TypeError
//
// refreshTick is driven by `this.refreshTick().catch(() => {})`, so the throw
// was SWALLOWED. Every tick died at step 1 and never reached beacons, root
// self-verify, cohort replication, the empty-root probe, the pending pub/kill
// retry, the subscriber eviction sweep, or the mesh re-warm — silently, for the
// life of the process, while /healthz kept saying ok.
//
// Both production bridges satisfied every precondition: BRIDGE_NEVER_ROOT is on
// by default, and the embedded peer subscribes to the directory topic in every
// bridge region.
//
// WHAT THIS FENCE ASSERTS
//   1. every become() caller survives a HARD refusal without throwing
//   2. claimReachable() REPORTS the refusal (returns null) instead of crashing
//   3. a refused claim leaves the node an ordinary unattached subscriber —
//      state untouched, and the renew still goes out
//   4. the tick RUNS TO ITS LAST STATEMENT on a refusing node (the assertion
//      that would have caught F1: not "did it throw" — the catch hid that —
//      but "did the work after the throw site still happen")
//   5. a NORMAL node is unaffected: the reachable-root fallback still claims
//
// Run: node test/smoke_decline_paths.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T, ROOT_CLAIM_MS, ROLE_GRACE_MS, BEACON_MS } from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) { console.log(`  ok ${++n} - ${m}`); }
  else   { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};

const REG   = 0x87n << 248n;
const SELF  = REG | 0x100n;          // deliberately very close to TOPIC
const FAR   = REG | 0xf000n;         // a neighbour, farther from TOPIC than SELF
const TOPIC = REG | 0x101n;
const hex   = (b) => b.toString(16).padStart(66, '0');
const settle = async (r = 4) => { for (let i = 0; i < r; i++) await new Promise(s => setImmediate(s)); };

function mk({ neverRoot = true } = {}) {
  const clock = { t: 1_000_000 };
  const sends = [], logs = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => [FAR],                       // 1 neighbour < MESH_REWARM_MIN
    bridgeId: () => null,
    findKClosest: async () => [SELF],
    reintegrate: () => {},                        // present so the tick's LAST stage runs
  };
  const am = new AxonaManager({ dht, now: () => clock.t, neverRoot });
  am.nodeId = SELF;
  am.setLogSink((level, type, data) => logs.push({ level, type, data }));
  return { am, sends, logs, clock };
}

/** Drive a subscribed-but-unattached peer past the reachable-root window. */
async function driveToFallback(ctx) {
  const { am, clock } = ctx;
  am.pubsubSubscribe(TOPIC);
  clock.t += ROLE_GRACE_MS + 10_000;      // past grace
  await am.refreshTick(); await settle(); // arms _unattachedSince
  clock.t += ROOT_CLAIM_MS + 5_000;       // past the unconfirmed-deferral window
}

console.log('decline paths — every become() caller survives a HARD refusal\n');

// ── 1. claimReachable REPORTS the refusal instead of throwing ──────────────
{
  const ctx = mk();
  await driveToFallback(ctx);
  ok('precondition: node is self-closest-reachable', ctx.am._rootClaim.selfClosestReachable(TOPIC));
  ok('precondition: admission refuses HARD (bridge fence)',
    ctx.am.canAcceptRole().hard === true && ctx.am.canAcceptRole().why === 'bridge');

  let threw = null, ret;
  try { ret = ctx.am._rootClaim.claimReachable(TOPIC); } catch (e) { threw = e; }
  ok('claimReachable does not throw on a refused claim', threw === null,
    threw ? `${threw.constructor.name}: ${threw.message}` : '');
  ok('claimReachable returns null to report the refusal', ret === null, `got ${ret}`);
  ok('no role was created', ctx.am.axonRoles.size === 0, `roles=${ctx.am.axonRoles.size}`);
}

// ── 2. a refused claim leaves the node an ordinary unattached subscriber ───
{
  const ctx = mk();
  await driveToFallback(ctx);
  ctx.am._rootHint.set(TOPIC, { via: hex(FAR), at: ctx.clock.t });
  const sinceBefore = ctx.am._unattachedSince.get(TOPIC);
  // Guarded so this fence reports ALL its cases on a broken tree instead of
  // aborting at the first one — a test that truncates is the same defect as a
  // suite that truncates (rule 10).
  try { ctx.am._rootClaim.claimReachable(TOPIC); } catch { /* asserted in case 1 */ }
  ok('refusal does not clear _unattachedSince', ctx.am._unattachedSince.get(TOPIC) === sinceBefore);
  ok('refusal does not clear the root hint', ctx.am._rootHint.has(TOPIC));
  ok('node holds no root claim', !ctx.am.axonRoles.get(TOPIC)?.isRoot);
}

// ── 3. THE F1 ASSERTION: the tick runs to its LAST statement ──────────────
// Not "did refreshTick reject" — it never did; the caller's .catch() hid the
// throw. The question that matters is whether the work AFTER the throw site
// still happens. _meshStarvedTicks is incremented by the final stage of the
// tick body, so a non-zero value proves execution reached the end.
{
  const ctx = mk();
  await driveToFallback(ctx);
  ctx.clock.t += BEACON_MS;         // so the beacon stage is genuinely DUE this tick
  ctx.sends.length = 0;

  let threw = null;
  try { await ctx.am.refreshTick(); } catch (e) { threw = e; }
  await settle();

  ok('refreshTick did not throw', threw === null,
    threw ? `${threw.constructor.name}: ${threw.message}` : '');
  ok('tick reached its LAST stage (mesh re-warm) — the work F1 silently skipped',
    ctx.am._meshStarvedTicks > 0, `_meshStarvedTicks=${ctx.am._meshStarvedTicks}`);
  ok('tick reached the beacon stage', ctx.am._lastBeaconAt === ctx.clock.t);
  ok('the refused topic still renewed its subscribe (not stranded)',
    ctx.sends.some(s => s.type === T.SUB && s.payload.topicId === hex(TOPIC)));

  // And it keeps working — the failure mode was permanent, so one tick is not
  // enough evidence.
  for (let i = 0; i < 5; i++) { ctx.clock.t += 5_000; await ctx.am.refreshTick(); await settle(); }
  ok('still healthy after 5 further ticks', ctx.am._meshStarvedTicks > 0 && ctx.am.axonRoles.size === 0);
}

// ── 4. the other four become() callers, under the same HARD refusal ────────
{
  const ctx = mk();
  const { am } = ctx;
  const meta = { isTerminal: true, targetId: SELF, fromId: hex(FAR) };
  const base = { topicId: hex(TOPIC), via: [] };
  const cases = [
    ['_onSub',       () => am._onSub({ ...base, subscriberId: hex(FAR), since: 0 }, meta)],
    ['_onPub',       () => am._onPub({ ...base, json: '{}' }, meta)],
    ['_onKill',      () => am._onKill({ ...base, kill: { msgId: 'a'.repeat(64) } }, meta)],
    ['_onMetricsOn', () => am._onMetricsOn({ ...base, requesterId: hex(FAR) }, meta)],
    ['HANDOFF',      () => am._syncIngest({ topicId: hex(TOPIC), from: hex(FAR), msgs: [], dels: [] },
                                          { fromId: hex(FAR) }, 'HANDOFF')],
  ];
  for (const [label, fn] of cases) {
    let threw = null;
    try { await fn(); } catch (e) { threw = e; }
    ok(`${label} survives the HARD refusal`, threw === null,
      threw ? `${threw.constructor.name}: ${threw.message}` : '');
  }
  ok('a refusing node seated no roles at all', am.axonRoles.size === 0, `roles=${am.axonRoles.size}`);
}

// ── 5. REGRESSION GUARD: a normal node still takes the fallback ────────────
{
  const ctx = mk({ neverRoot: false });
  await driveToFallback(ctx);
  const role = ctx.am._rootClaim.claimReachable(TOPIC);
  ok('a non-bridge node still claims the reachable-root fallback',
    !!role && role.isRoot === true);
  ok('…and clears its unattached timer + hint', !ctx.am._unattachedSince.has(TOPIC) && !ctx.am._rootHint.has(TOPIC));
  ok('…and logged root-claimed-reachable',
    ctx.logs.some(l => l.type === 'pubsub:root-claimed-reachable'));
}

console.log(`\n${fail === 0 ? '✓' : '✗'} decline paths: ${n} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
