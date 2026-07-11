// smoke_leave_handoff_burst.mjs — graceful-leave handoff mechanics (v4.19.5).
//
// Field case (alert-bot, 2026-07-11): a fresh node published 123 messages over
// 112 fresh topics in ~7s and left holding 25 roots; a fresh subscriber then
// replayed 0 for ~10% of topics. Three defects, each tested here at the
// mechanics level (deterministic mocked dht; the live-stack validation script
// exercises the same path end-to-end):
//   1. the handoff resolved heirs ONE AT A TIME (an iterative lookup each) —
//      the tail of topics never handed off inside leave()'s bound;
//   2. a leaver with a THIN local table (findKClosest sees only itself) gave
//      up instead of probing with the iterative lookup — zero handoffs;
//   3. the heir ADOPTED the history and then immediately demoted back toward
//      the departing node's still-live beacon, undoing the handoff.
//
// Run: node test/smoke_leave_handoff_burst.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const idHex = (big) => big.toString(16).padStart(66, '0');
const __LOC = regionCenter('useast');

function makeManager({ selfBig, findKClosest, lookup, onRoute }) {
  const handlers = new Map();
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: (t, h) => handlers.set(t, h),
    routeMessage: (target, type, payload) => onRoute?.(target, type, payload),
    neighbors: () => [],
    bridgeId: () => null,
    ...(findKClosest ? { findKClosest } : {}),
    ...(lookup ? { lookup } : {}),
  };
  const am = new AxonaManager({ dht, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
  return { am, handlers, dht };
}

async function main() {
  console.log('graceful-leave handoff mechanics\n');
  const ident = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
  const SELF = BigInt('0x' + ident.id);
  const HEIR = SELF ^ 0xFFn;

  // ── 1. parallel heir resolution: 24 topics @150ms/lookup ────────────
  {
    const routed = [];
    const { am } = makeManager({
      selfBig: SELF,
      findKClosest: async () => { await sleep(150); return [SELF, HEIR]; },
      onRoute: (target, type, payload) => { if (type === 'pubsub:handoff') routed.push(payload); },
    });
    for (let i = 0; i < 24; i++) {
      const t = SELF ^ BigInt(0x1000 + i);
      const role = { topicId: t, isRoot: true, cache: [{ msgId: 'm'+i, publishTs: i+1, json: '{}', seq: 1 }],
                     cacheIds: new Set(), children: new Set(), subscribers: new Map(), tombstones: new Map() };
      am.axonRoles.set(t, role);
    }
    const t0 = Date.now();
    await am.pubsubLeaveHandoff();
    const ms = Date.now() - t0;
    check('all 24 rooted topics handed off', routed.length === 24, `${routed.length}`);
    check(`parallel: 24 lookups @150ms took ${ms}ms (serial would be ~3600ms)`, ms < 1400, `${ms}ms`);
    check('HANDOFF carries the departing node (`from`)',
      routed.every(p => p.from === idHex(SELF)));
  }

  // ── 2. thin-table leaver falls back to the iterative lookup ─────────
  {
    const routed = [];
    let lookups = 0;
    const { am } = makeManager({
      selfBig: SELF,
      findKClosest: async () => [SELF],                    // local view: nobody but me
      lookup: async () => { lookups++; return { path: [idHex(HEIR)] }; },
      onRoute: (target, type, payload) => { if (type === 'pubsub:handoff') routed.push({ target, payload }); },
    });
    const t = SELF ^ 0x2000n;
    am.axonRoles.set(t, { topicId: t, isRoot: true, cache: [{ msgId: 'x', publishTs: 1, json: '{}', seq: 1 }],
      cacheIds: new Set(), children: new Set(), subscribers: new Map(), tombstones: new Map() });
    await am.pubsubLeaveHandoff();
    check('thin-table leaver probed with the iterative lookup', lookups === 1, `${lookups}`);
    check('…and handed off to the discovered heir', routed.length === 1 && routed[0].target === HEIR);
  }

  // ── 3. heir must not defer back to the departing sender ─────────────
  {
    const { am, handlers } = makeManager({ selfBig: HEIR });
    const t = SELF ^ 0x3000n;
    // heir holds the LEAVER's still-fresh root beacon (leaver is closer)
    am._rootBeacons.set(t, { root: idHex(SELF), at: am._now(), exp: am._now() + 50_000 });
    const onHandoff = handlers.get('pubsub:handoff');
    await onHandoff({ topicId: idHex(t), msgs: [], dels: [], from: idHex(SELF) }, { targetId: HEIR });
    const role = am.axonRoles.get(t);
    check('heir adopted the topic as ROOT (no demote toward the leaver)', !!role && role.isRoot === true,
      role ? `isRoot=${role.isRoot}` : 'no role');
    check('leaver\'s ghost beacon purged', !am._rootBeacons.has(t));
  }

  // ── 4. peer-death purges ghost beacons generally ─────────────────────
  {
    const { am } = makeManager({ selfBig: HEIR });
    const t1 = SELF ^ 0x4000n, t2 = SELF ^ 0x5000n;
    am._rootBeacons.set(t1, { root: idHex(SELF), at: am._now(), exp: am._now() + 50_000 });
    am._rootBeacons.set(t2, { root: idHex(HEIR ^ 0x2n), at: am._now(), exp: am._now() + 50_000 });
    am.pubsubPeerDied(idHex(SELF));
    check('dead peer\'s beacons purged', !am._rootBeacons.has(t1));
    check('other beacons untouched', am._rootBeacons.has(t2));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
