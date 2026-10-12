// =====================================================================
// fence_mesh_rewarm_backoff.mjs — R5 of Routed-Walk-Terminals v0.6
// (axona-docs 88d6871), the kernel's half.
//
// The defect: a node whose mesh stays starved re-requests the bridge's peer
// list every MESH_REWARM_COOLDOWN_MS (60 s) for ever. One Safari tab did it
// 272 times in five hours on 2026-10-11 and, with each re-request, re-offered
// to every peer on the list.
//
// R5: the gap DOUBLES after every ask that found the mesh still starved —
// 1, 2, 4, 8 min … up to MESH_REWARM_BACKOFF_MAX_MS (10 min) — and resets to
// the cooldown on ANY bound channel (a rise in the mesh count), or when the
// mesh is no longer starved.
//
// On a bare AxonaManager with a stub dht and an injected clock, ticking
// refreshTick every 5 simulated seconds:
//   1  the first ask comes on the third starved tick (unchanged)
//   2  the gaps between asks are 120, 240, 480, 600, 600 s (doubling, capped)
//   3  a bound channel (mesh 0 → 1, still starved) resets the gap to 60 s
//   4  a mesh at the floor stops the asks and resets the gap
//   5  inspectRewarm() reports the asks and the live backoff
//
// Mutant: delete the doubling line → the gaps read 60 s throughout → 2 fails.
//
// Run: node test/fence_mesh_rewarm_backoff.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { depositDispatchCapability } from '../src/registry/index.js';
import { MESH_REWARM_COOLDOWN_MS, MESH_REWARM_BACKOFF_MAX_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const H = (b) => b.toString(16).padStart(2, '0').repeat(33);

function manager() {
  let clock = 1_800_000_000_000;
  let mesh = [];
  const asks = [];
  const dht = {
    verdictsSupported: true,
    routeMessage: async () => ({ consumed: false }),
    getSelfId: () => H(0x01),
    onRoutedMessage() {}, onDirectMessage() {},
    neighbors: () => mesh, bridgeId: () => null,
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
    findKClosest: async () => [],
    reintegrate: () => { asks.push(clock); return Promise.resolve(0); },
  };
  depositDispatchCapability(dht, { routed: () => {} });
  const m = new AxonaManager({ dht, now: () => clock });
  m._log = () => {};
  return {
    m, asks,
    setMesh: (n) => { mesh = Array.from({ length: n }, (_, i) => H(0x10 + i)); },
    tick: async (ms = 5000) => { clock += ms; await m.refreshTick(); },
    now: () => clock,
  };
}

(async () => {
  console.log('fence_mesh_rewarm_backoff: R5 — the re-warm ask backs off and resets on a bound channel');
  const f = manager();
  f.setMesh(0);

  // 1. the first ask on the third starved tick
  await f.tick(); await f.tick();
  check('1 no ask on the first two starved ticks', f.asks.length === 0, String(f.asks.length));
  await f.tick();
  check('1 the third starved tick asks', f.asks.length === 1, String(f.asks.length));

  // 2. run 40 simulated minutes of 5 s ticks and read the gaps
  for (let i = 0; i < (40 * 60) / 5; i++) await f.tick();
  const gaps = f.asks.slice(1).map((t, i) => (t - f.asks[i]) / 1000);
  // Each gap is the backoff plus up to one tick of quantisation (the ask fires
  // on the first tick at or past the gap), so compare at tick resolution.
  const near = (g, want) => g >= want && g < want + 10;
  check('2 gaps double then cap: 120, 240, 480, 600, 600 s',
    gaps.length >= 5 && near(gaps[0], 120) && near(gaps[1], 240) && near(gaps[2], 480) && near(gaps[3], 600) && near(gaps[4], 600),
    JSON.stringify(gaps));
  check('2 the backoff reports the cap', f.m.inspectRewarm().backoffMs === MESH_REWARM_BACKOFF_MAX_MS, JSON.stringify(f.m.inspectRewarm()));

  // 3. a bound channel resets the gap
  const before = f.asks.length;
  f.setMesh(1);                               // mesh 0 → 1: a channel bound, still starved (< 3)
  await f.tick();
  // The reset takes effect on that tick: the 600 s gap no longer holds, the
  // last ask was more than 60 s ago, so the ask fires AT ONCE and the gap
  // after it is the cooldown doubled once (120 s), not the cap.
  check('3 a bound channel resets the gap: the ask fires on that tick and the gap after it is 120 s, not 600',
    f.asks.length === before + 1 && f.m.inspectRewarm().backoffMs === MESH_REWARM_COOLDOWN_MS * 2,
    `asked=${f.asks.length - before} backoff=${f.m.inspectRewarm().backoffMs}`);
  let t0 = f.now();
  for (let i = 0; i < 40; i++) { await f.tick(); if (f.asks.length > before + 1) break; }
  check('3 the following ask comes about two minutes later, not ten', f.asks.length === before + 2 && (f.now() - t0) <= 140_000, `asked=${f.asks.length - before} after ${(f.now() - t0) / 1000}s`);

  // 4. a mesh at the floor stops the asking
  const atFloor = f.asks.length;
  f.setMesh(3);
  for (let i = 0; i < 200; i++) await f.tick();
  check('4 a mesh at the floor asks nothing and resets the gap', f.asks.length === atFloor && f.m.inspectRewarm().backoffMs === MESH_REWARM_COOLDOWN_MS, `asks=${f.asks.length - atFloor}`);

  // 5. the surface
  const r = f.m.inspectRewarm();
  check('5 inspectRewarm reports asks, backoff, cap and the last ask time', r.asks === f.asks.length && r.maxBackoffMs === MESH_REWARM_BACKOFF_MAX_MS && r.lastAskAt === f.asks[f.asks.length - 1], JSON.stringify(r));

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
