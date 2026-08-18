// =====================================================================
// smoke_tick_lag_window.mjs — RULE 11 FENCE: "a high-water metric that can
// put a node into a degraded state must be able to leave it."
//
// THE DEFECT THIS GUARDS (F6/N1, found 2026-07-28, live on prod at 4.47/4.48).
// helloPressure = _tickLagMax / HELLO_DEADLINE_MS, and _tickLagMax was an
// ALL-TIME high-water mark with no decay. That made it a ratchet:
//
//   one 60s browser-tab suspension  ->  helloPressure 11.0 (18x the 0.6
//   threshold), instantaneous lag back to 0, and STILL saturated after 2,050
//   healthy ticks (~2.8h). Only a page reload cleared it.
//
// iOS suspends JS on screen lock; Android throttles a background tab to ~1/min.
// So every mobile browser peer that had ever been backgrounded was permanently
// `saturated`, and a saturated node refuses HANDOFF — the path that carries a
// departing node's LAST copy of a topic's history. That is the mechanism behind
// the unexplained replay-history gaps seen in the chat client.
//
// WHAT THIS FENCE ASSERTS — and note it is TWO-SIDED. Recovery alone is not the
// property we want: a window that recovers but no longer DETECTS is just as
// broken, in the opposite direction. So all four cases below are required
// (review pass 6, amendment 2):
//
//   1. foreground peer, steady ticks          -> never saturated
//   2. genuinely slow peer, sustained lag     -> saturated, and STAYS saturated
//   3. backgrounded browser returning         -> saturated during, false within
//                                                one window after
//   4. repeated suspend/recover cycles        -> no ratchet; recovers each time
//
// Run: node test/smoke_tick_lag_window.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { TICK_LAG_WINDOW, HELLO_DEADLINE_MS, SATURATION_PRESSURE } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) { console.log(`  ok ${++n} - ${m}`); }
  else   { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};

const REG = 0x87n << 248n, SELF = REG | 0x100n, PEER = REG | 0x200n;
const TICK = 5_000;

function mk() {
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF, onRoutedMessage: () => {}, verdictsSupported: false, routeMessage: () => {},
    neighbors: () => [PEER], bridgeId: () => null, findKClosest: async () => [PEER],
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, refreshIntervalMs: TICK });
  am.nodeId = SELF;
  am._joinedAt = clock.t - 200_000;          // past grace, so `seated` is not the variable
  return { am, clock };
}
/** Advance by `gapMs` and run one tick. gapMs === TICK means "on schedule". */
const tick = async (ctx, gapMs = TICK) => { ctx.clock.t += gapMs; await ctx.am.refreshTick(); };
const cap  = (ctx) => ctx.am.inspectCapacity();

console.log(`tick-lag window — rolling max over ${TICK_LAG_WINDOW} ticks (~${TICK_LAG_WINDOW * TICK / 1000}s)\n`);

// ── 1. foreground peer: steady ticks are never saturating ──────────────────
{
  const ctx = mk();
  for (let i = 0; i < 40; i++) await tick(ctx);
  const c = cap(ctx);
  ok('steady 5s ticks: helloPressure stays 0', c.helloPressure === 0, JSON.stringify(c.helloPressure));
  ok('steady 5s ticks: not saturated', ctx.am.saturated() === false);
  ok('a little jitter (200ms) does not saturate', await (async () => {
    for (let i = 0; i < 20; i++) await tick(ctx, TICK + 200);
    return ctx.am.saturated() === false;
  })(), `helloPressure=${cap(ctx).helloPressure}`);
}

// ── 2. genuinely slow peer: DETECTION must survive the window ──────────────
// The failure mode of an over-eager window is a node that is really struggling
// but keeps forgetting. Sustained lag must hold the node saturated.
{
  const ctx = mk();
  const BAD = TICK + 4_000;                 // 4s lag — 0.8 of the deadline, over 0.6
  for (let i = 0; i < 4; i++) await tick(ctx, BAD);
  ok('sustained lag saturates', ctx.am.saturated() === true, `helloPressure=${cap(ctx).helloPressure}`);
  // Keep it up for FIVE windows. It must not "recover" while still lagging.
  let everFalse = false;
  for (let i = 0; i < TICK_LAG_WINDOW * 5; i++) {
    await tick(ctx, BAD);
    if (!ctx.am.saturated()) everFalse = true;
  }
  ok('a still-lagging node NEVER drops out of saturated', everFalse === false);
  ok('…and helloPressure reflects the real lag', cap(ctx).helloPressure >= SATURATION_PRESSURE);
}

// ── 3. THE N1 CASE: a backgrounded browser returns to service ──────────────
{
  const ctx = mk();
  for (let i = 0; i < 5; i++) await tick(ctx);
  ok('healthy before suspension', ctx.am.saturated() === false);

  await tick(ctx, 60_000);                  // screen lock: one 60s frozen tab
  const during = cap(ctx);
  ok('the suspension IS detected (not swallowed)',
    during.helloPressure >= SATURATION_PRESSURE && ctx.am.saturated() === true,
    `helloPressure=${during.helloPressure}`);

  // Recovery must land inside one window, and not before the window is out
  // (otherwise the signal is being discarded, not aged out).
  let recoveredAt = -1;
  for (let i = 1; i <= TICK_LAG_WINDOW * 3; i++) {
    await tick(ctx);
    if (recoveredAt < 0 && !ctx.am.saturated()) recoveredAt = i;
  }
  ok(`recovers within one window (at tick ${recoveredAt} of ${TICK_LAG_WINDOW})`,
    recoveredAt > 0 && recoveredAt <= TICK_LAG_WINDOW, `recoveredAt=${recoveredAt}`);
  const after = cap(ctx);
  ok('helloPressure returns to 0 after recovery', after.helloPressure === 0);
  ok('admission accepts a pushed HANDOFF again', ctx.am.admitPushedRole(REG | 0x999n) === true);
  ok('the all-time peak is RETAINED for diagnosis', after.tickLagPeakMs >= 55_000,
    `tickLagPeakMs=${after.tickLagPeakMs}`);
  ok('…while the windowed max, which drives admission, has fallen',
    after.tickLagMaxMs === 0, `tickLagMaxMs=${after.tickLagMaxMs}`);
  ok('the stall is still counted', after.tickStalls >= 1);
}

// ── 4. repeated suspend/recover: no ratchet across cycles ──────────────────
{
  const ctx = mk();
  // Prime with one on-schedule tick: lag is a GAP between two ticks, so the
  // very first tick of a manager's life has nothing to be late relative to and
  // deliberately records no sample.
  await tick(ctx);
  for (let cycle = 1; cycle <= 5; cycle++) {
    await tick(ctx, 60_000);                                  // suspend
    const sat = ctx.am.saturated();
    for (let i = 0; i < TICK_LAG_WINDOW + 1; i++) await tick(ctx);   // recover
    ok(`cycle ${cycle}: saturates on suspend, clears on recovery`,
      sat === true && ctx.am.saturated() === false,
      `sat=${sat} after=${ctx.am.saturated()} hp=${cap(ctx).helloPressure}`);
  }
  ok('5 suspend/recover cycles left no residue', cap(ctx).helloPressure === 0);
}

// ── 5. a node that STOPS ticking keeps its reading (no false recovery) ─────
// The window only advances when a tick runs. A frozen node must not be treated
// as healthy just because time passed — we have no evidence from it at all.
{
  const ctx = mk();
  for (let i = 0; i < 5; i++) await tick(ctx);
  await tick(ctx, 60_000);
  ok('saturated after the stall', ctx.am.saturated() === true);
  ctx.clock.t += 10 * 60_000;               // ten minutes pass with NO ticks
  ok('still saturated — time alone is not evidence of health', ctx.am.saturated() === true);
}

console.log(`\n${fail === 0 ? '✓' : '✗'} tick-lag window: ${n} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
