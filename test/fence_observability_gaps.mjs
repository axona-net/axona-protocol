// =====================================================================
// fence_observability_gaps.mjs — 4.77.0. Three quantities the kernel already
//   computes and did not surface. Each one, on 2026-09-07, let a conclusion
//   outrun the evidence during the #58 / #60 investigation.
//
//   GAP 1  seated subtree at a root transition.
//          role.subscribers / role.children exist; no log carried them. The
//          relay status line's subs= is mySubscriptions — this node's OWN
//          subscriptions — and an exclusion of #60's precondition was drawn
//          from it and had to be retracted. A yield is exactly the moment the
//          question "was anything seated under this seat?" becomes decisive.
//
//   GAP 2  routed outcome. routeMessage reports failure by RESOLVING
//          {consumed:false} and emits nothing, so absent routed-failure lines
//          meant UNLOGGED, not no-failures, and routed reachability was
//          unmeasurable in production.
//
//   GAP 3  a query surface (relay-side, SIGUSR1 health dump; not fenced here —
//          it lives in axona-relay/src/index.js and is exercised by hand).
//
// PURELY ADDITIVE. No behaviour changes: the tally reads routing's own verdict
// and returns it untouched, and the transition log gains two counts. The 4.76.2
// replicate-failure patch is the precedent — name WHO, not just that it failed.
//
// Run: node test/fence_observability_gaps.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { ROUTE_FAIL_TRACK_MAX } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');
// The kernel's _log sink prefixes events with the plane name; match either form.
const ev = (logs, name) => logs.filter((l) => new RegExp(`(^|:)${name}$`).test(l.e));

const SELF = (0x80n << 248n) | 0x1000n;
const T    = SELF ^ 0x10n;

function mk({ verdict } = {}) {
  const logs = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: true,
    routeMessage: async () => (typeof verdict === 'function' ? verdict() : verdict),
    neighbors: () => [],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), emitLog: (l, e, c) => logs.push({ l, e, c }) });
  am.nodeId = SELF;
  return { am, logs };
}

console.log('4.77.0 — surfacing three values the kernel computed and never logged\n');

// ── GAP 1: a root transition names its seated subtree ───────────────────
{
  console.log('— gap 1: seated subtree at a transition —');
  const { am, logs } = mk({ verdict: { consumed: true } });
  am.pubsubSubscribe(T);
  am._becomeRoot(T);
  const role = am.axonRoles.get(T);

  const born = ev(logs, 'root-transition');
  check('1. a transition log carries seated counts', born.length > 0 &&
    typeof born[0].c?.subs === 'number' && typeof born[0].c?.kids === 'number',
    JSON.stringify(born[0]?.c));
  check('1b. an empty seat reports zero, not absent', born[0]?.c?.subs === 0 && born[0]?.c?.kids === 0,
    JSON.stringify(born[0]?.c));

  // Seat a subscriber and a child, then flip the seat.
  role.subscribers.set('aa'.repeat(33), { at: 0 });
  role.children.add('bb'.repeat(33));
  logs.length = 0;
  am._rootClaim._set(role, false, 'test-yield');
  const yielded = ev(logs, 'root-transition');
  check('2. a YIELD reports what was seated beneath it — the #60 quantity',
    yielded.length === 1 && yielded[0].c.subs === 1 && yielded[0].c.kids === 1,
    JSON.stringify(yielded[0]?.c));
  check('2b. …and still records why and which way it flipped',
    yielded[0]?.c?.why === 'test-yield' && yielded[0]?.c?.isRoot === false,
    JSON.stringify(yielded[0]?.c));
}

// ── GAP 2: routed outcomes are counted and attributed ───────────────────
{
  console.log('\n— gap 2: routed outcome —');
  // Two targets whose id PREFIXES differ, because the summary keys on the
  // leading hex (real node ids are distinctive there; adjacent synthetic ids
  // are not — that cost a confused reading while building this).
  const A = (0x80n << 248n) | (0xaan << 232n);
  const B = (0x80n << 248n) | (0xbbn << 232n);

  {
    const { am, logs } = mk({ verdict: { consumed: false } });
    await am._route(A, 'pubsub:pub', {});
    await am._route(A, 'pubsub:pub', {});
    await am._route(B, 'pubsub:pub', {});
    am._reportRouteOutcomes();
    const s = ev(logs, 'routed-outcomes')[0]?.c;
    check('3. failures are counted', s?.failed === 3, JSON.stringify(s));
    check('4. …and ATTRIBUTED to targets, worst first (the 4.76.2 move)',
      Array.isArray(s?.top) && s.top[0]?.n === 2 && s.top.length === 2,
      JSON.stringify(s?.top));
    check('4b. distinct targets are tracked separately', s?.tracked === 2, JSON.stringify(s));
  }

  {
    const { am, logs } = mk({ verdict: { consumed: true } });
    await am._route(A, 'pubsub:pub', {});
    am._reportRouteOutcomes();
    check('5. SILENCE when every verdict was consumed (no per-tick noise)',
      ev(logs, 'routed-outcomes').length === 0);
  }

  {
    // A non-reporting adapter resolves no verdict. That is NOT evidence of
    // failure and must never be counted as one — the 4.58.0 rule.
    const { am, logs } = mk({ verdict: undefined });
    await am._route(A, 'pubsub:pub', {});
    am._reportRouteOutcomes();
    check('6. a NON-REPORTING adapter is not counted as a failure',
      ev(logs, 'routed-outcomes').length === 0);
  }

  {
    // The verdict must pass through untouched — observability may not alter routing.
    const { am } = mk({ verdict: { consumed: false, exhausted: true, mark: 'x' } });
    const r = await am._route(A, 'pubsub:pub', {});
    check('7. the routing verdict is returned UNCHANGED (no behaviour change)',
      r?.consumed === false && r?.exhausted === true && r?.mark === 'x', JSON.stringify(r));
  }

  {
    // A churning mesh must not grow the map without bound.
    const { am, logs } = mk({ verdict: { consumed: false } });
    for (let i = 0; i < ROUTE_FAIL_TRACK_MAX + 8; i++) {
      await am._route((0x80n << 248n) | (BigInt(i + 1) << 216n), 'pubsub:pub', {});
    }
    am._reportRouteOutcomes();
    const s = ev(logs, 'routed-outcomes')[0]?.c;
    check('8. the failing-target map is BOUNDED', s?.tracked <= ROUTE_FAIL_TRACK_MAX,
      `tracked=${s?.tracked} max=${ROUTE_FAIL_TRACK_MAX}`);
    check('8b. …while the total failure count stays exact',
      s?.failed === ROUTE_FAIL_TRACK_MAX + 8, `failed=${s?.failed}`);
  }

  {
    // Counters drain on report, so each line covers one interval.
    const { am, logs } = mk({ verdict: { consumed: false } });
    await am._route(A, 'pubsub:pub', {});
    am._reportRouteOutcomes();
    logs.length = 0;
    am._reportRouteOutcomes();
    check('9. counters DRAIN — a second report with no new traffic is silent',
      ev(logs, 'routed-outcomes').length === 0);
  }
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
