// smoke_root_stepdown_hold.mjs — option A with a clock (4.102.0).
//
// A node that yields a root claim to a named closer root must not take the
// root back for STEPDOWN_HOLD_MS, even when it cannot reach that root. Before
// 4.102.0 the only hold was the ~1.5-beacon freshness of the yielded-to record,
// and a node that could not reach the real root retook the seat 45-89 s later,
// every time: two live roots for council on 2026-10-02/03 (GH #58).
//
// Contract:
//   · demote() arms the hold; promote() and claimReachable() refuse while it
//     is live, and the node stays non-root;
//   · after the hold the node may retake the root, and logs root-hold-expired;
//   · STEPDOWN_HOLD_MS = 0 disables the hold (the pre-4.102.0 behaviour);
//   · a bare-topic PUB that terminates at a held, non-root node is FORWARDED
//     to the held root — not rerouted toward the topic id, which would loop;
//   · a PUB or SUB with no role at a held node goes to the held root and does
//     not birth a new root.
//
// Run: node test/smoke_root_stepdown_hold.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

function makeManager({ selfBig, neighbors = [], holdMs } = {}) {
  const routed = [];
  const logs = [];
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: () => {},
    verdictsSupported: false,
    routeMessage: (target, type, payload) => { routed.push({ target, type, payload }); },
    neighbors: () => neighbors.map(idHex),
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) });
  am._now = () => clock.t;
  if (holdMs !== undefined) am._stepDownHoldMs = holdMs;
  return { am, rc: am._rootClaim, routed, logs, clock };
}
const has = (logs, ev) => logs.some((l) => l.ev === `pubsub:${ev}`);

async function main() {
  console.log('step-down hold — option A with a clock\n');
  const SELF = (0x80n << 248n) | 0x1000n;
  const T    = SELF ^ 0x10n;
  const NEAR = T ^ 0x1n;                 // strictly closer to T than self
  const bare = { via: [] };
  const terminal = { isTerminal: true };

  // ── 1. demote arms the hold; promote refuses; expiry releases ───────────
  {
    const { am, rc, logs, clock } = makeManager({ selfBig: SELF, neighbors: [] });
    const role = rc.become(T, 'sub-terminal');
    check('precondition: became root', role.isRoot === true);
    check('demote toward a closer root succeeds', rc.demote(T, idHex(NEAR), 'verify-closer') === true);
    check('hold armed by demote', !!rc.holdFor(T) && rc.holdFor(T).to === idHex(NEAR));

    // the exact 2026-10-02 shape: the closer root is NOT reachable/beaconing,
    // so liveCloserRoot() is null and the old code promoted immediately
    check('precondition: no live closer root visible', rc.liveCloserRoot(T) === null);
    clock.t += 90_000;                                   // the observed retake gap
    rc.promote(role, bare, terminal);
    check('promote refused inside the hold (90 s after stepping down)', role.isRoot === false);
    check('root-hold logged', has(logs, 'root-hold'));

    clock.t += am._stepDownHoldMs;                       // past the hold
    rc.promote(role, bare, terminal);
    check('promote allowed after the hold expires', role.isRoot === true);
    check('root-hold-expired logged', has(logs, 'root-hold-expired'));
    check('hold cleared once root', rc.holdFor(T) === null);
  }

  // ── 2. claimReachable refuses under the hold ───────────────────────────
  {
    const { am, rc, clock } = makeManager({ selfBig: SELF, neighbors: [] });
    rc.become(T, 'sub-terminal');
    rc.demote(T, idHex(NEAR), 'beacon-closer');
    check('claimReachable refused (null) inside the hold', rc.claimReachable(T) === null);
    check('…and the role stayed non-root', am.axonRoles.get(T).isRoot === false);
    clock.t += am._stepDownHoldMs + 1;
    const r = rc.claimReachable(T);
    check('claimReachable allowed after the hold', !!r && r.isRoot === true);
  }

  // ── 3. STEPDOWN_HOLD_MS = 0 disables ───────────────────────────────────
  {
    const { rc } = makeManager({ selfBig: SELF, neighbors: [], holdMs: 0 });
    const role = rc.become(T, 'sub-terminal');
    rc.demote(T, idHex(NEAR), 'verify-closer');
    rc.promote(role, bare, terminal);
    check('hold 0 → immediate retake (pre-4.102.0 behaviour)', role.isRoot === true);
  }

  // ── 4. a bare PUB at a held non-root terminus goes to the held root ────
  //       (held root REACHABLE: a live neighbour)
  {
    const { am, rc, routed } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
    rc.become(T, 'sub-terminal');
    rc.demote(T, idHex(NEAR), 'verify-closer');
    routed.length = 0;
    let threw = null;
    try { await am._onPub({ topicId: idHex(T), via: [], json: '{}' }, { isTerminal: true, targetId: SELF }); }
    catch (e) { threw = e; }
    check('PUB handler returned (no synchronous loop, no throw)', threw === null, threw?.message);
    const pubs = routed.filter((r) => r.type === 'pubsub:pub' || r.payload?.json === '{}');
    check('exactly one forward of the PUB', pubs.length === 1, `got ${pubs.length}`);
    check('…pinned to the held root', pubs[0]?.payload?.via?.[0] === idHex(NEAR));
    check('…and the node did not retake the root', am.axonRoles.get(T).isRoot === false);
  }

  // ── 5. no role + hold: PUB and SUB go to the held root, no new root ────
  {
    const { am, rc, routed } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
    rc.become(T, 'sub-terminal');
    rc.demote(T, idHex(NEAR), 'verify-closer');
    am.axonRoles.delete(T);                               // role reaped, hold still live
    routed.length = 0;
    await am._onPub({ topicId: idHex(T), via: [], json: '{}' }, { isTerminal: true, targetId: SELF });
    check('no-role PUB did not birth a root', !am.axonRoles.has(T));
    check('…forwarded to the held root', routed.some((r) => r.payload?.via?.[0] === idHex(NEAR) && r.payload?.json === '{}'));
    routed.length = 0;
    const OTHER = (0x80n << 248n) | 0x9999n;
    await am._onSub({ topicId: idHex(T), via: [], subscriberId: idHex(OTHER), since: 0 }, { isTerminal: true, targetId: SELF });
    check('no-role SUB did not birth a root', !am.axonRoles.has(T));
    check('…sent pinned to the held root', routed.some((r) => r.payload?.via?.[0] === idHex(NEAR) && r.payload?.subscriberId === idHex(OTHER)));
  }

  // ── 5b. yielding to a FARTHER node (epoch-superseded) arms NO hold ─────
  //        Otherwise the true root and a spurious one can each defer to the
  //        other and hold: zero roots for the whole hold.
  {
    const FAR = T ^ (0x1n << 200n);
    const { rc } = makeManager({ selfBig: SELF, neighbors: [] });
    const role = rc.become(T, 'sub-terminal');
    rc.demote(T, idHex(FAR), 'epoch-superseded');
    check('demote to a FARTHER node arms no hold', rc.holdFor(T) === null);
    rc.promote(role, bare, terminal);
    check('…so the closer node may reclaim at once', role.isRoot === true);
  }

  // ── 6. held root UNREACHABLE: hold, send nothing, log — never loop ─────
  //       A send pinned to an unreachable root falls back to topic-id routing,
  //       which lands on this terminus again. The hold must not send at all.
  {
    const { am, rc, routed, logs } = makeManager({ selfBig: SELF, neighbors: [] });
    rc.become(T, 'sub-terminal');
    rc.demote(T, idHex(NEAR), 'verify-closer');
    routed.length = 0;
    let threw = null;
    try { await am._onPub({ topicId: idHex(T), via: [], json: '{}' }, { isTerminal: true, targetId: SELF }); }
    catch (e) { threw = e; }
    check('unreachable held root: PUB returns, no throw', threw === null, threw?.message);
    check('…nothing sent (no loop seed)', !routed.some((r) => r.payload?.json === '{}'), JSON.stringify(routed.map((r) => r.type)));
    check('…logged undeliverable step-down-hold', logs.some((l) => l.ev === 'pubsub:undeliverable' && l.ctx?.why === 'step-down-hold'));
    check('…and still held, not re-rooted', am.axonRoles.get(T).isRoot === false);
  }

  // ── 7. KILL with an EXISTING (demoted, non-root) role (Aster dced6098) ──
  {
    const KILL = { msgId: 'a'.repeat(64) };
    // reachable held root → forwarded, not applied locally
    {
      const { am, rc, routed } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
      rc.become(T, 'sub-terminal');
      rc.demote(T, idHex(NEAR), 'verify-closer');
      const seq0 = am.axonRoles.get(T).seq;
      routed.length = 0;
      await am._onKill({ topicId: idHex(T), via: [], kill: KILL }, { isTerminal: true, targetId: SELF });
      check('existing-role KILL, reachable held root → forwarded to it',
        routed.some((r) => r.payload?.via?.[0] === idHex(NEAR) && r.payload?.kill?.msgId === KILL.msgId));
      check('…not applied locally (role.seq unchanged)', am.axonRoles.get(T).seq === seq0);
    }
    // unreachable held root → held, nothing sent, undeliverable
    {
      const { am, rc, routed, logs } = makeManager({ selfBig: SELF, neighbors: [] });
      rc.become(T, 'sub-terminal');
      rc.demote(T, idHex(NEAR), 'verify-closer');
      routed.length = 0;
      await am._onKill({ topicId: idHex(T), via: [], kill: KILL }, { isTerminal: true, targetId: SELF });
      check('existing-role KILL, unreachable held root → nothing sent', !routed.some((r) => r.payload?.kill));
      check('…logged undeliverable step-down-hold',
        logs.some((l) => l.ev === 'pubsub:undeliverable' && l.ctx?.why === 'step-down-hold' && l.ctx?.type === 'pubsub:kill'));
    }
  }

  // ── 8. NO channel to the held root, no role (Aster dced6098) ───────────
  //       The intercept runs FIRST and forwards on exactly the evidence the
  //       existing gate for that verb accepts. Anything weaker sends NOTHING
  //       (a send pinned to an unreachable root falls back and loops here).
  const subCall  = (am) => am._onSub({ topicId: idHex(T), via: [], subscriberId: idHex((0x80n << 248n) | 0x9999n), since: 0 }, { isTerminal: true, targetId: SELF });
  const pubCall  = (am) => am._onPub({ topicId: idHex(T), via: [], json: '{}' }, { isTerminal: true, targetId: SELF });
  const killCall = (am) => am._onKill({ topicId: idHex(T), via: [], kill: { msgId: 'b'.repeat(64) } }, { isTerminal: true, targetId: SELF });
  const held = () => {
    const ctx = makeManager({ selfBig: SELF, neighbors: [] });
    ctx.rc.become(T, 'sub-terminal');
    ctx.rc.demote(T, idHex(NEAR), 'verify-closer');
    ctx.am.axonRoles.delete(T);
    return ctx;
  };
  const undeliv = (logs) => logs.some((l) => l.ev === 'pubsub:undeliverable' && l.ctx?.why === 'step-down-hold');
  // 8a. SUB, fresh UNVERIFIED beacon: the SUB gate is strict, so it sends nothing
  {
    const { am, rc, routed, logs } = held();
    am._rootBeacons.set(T, { root: idHex(NEAR), at: am._now(), exp: am._now() + 90_000 });
    check('8a precondition: the LOOSE gate would accept this beacon', rc.liveCloserRoot(T, { requireReachable: false }) === idHex(NEAR));
    routed.length = 0;
    await subCall(am);
    check('8a SUB, fresh unverified beacon, no channel → NOTHING sent', routed.length === 0, JSON.stringify(routed.map((r) => r.type)));
    check('8a …held + undeliverable, no root born', !am.axonRoles.has(T) && undeliv(logs));
  }
  // 8b. PUB / KILL, STALE beacon (outside 1.5×BEACON_MS): nothing sent
  for (const [verb, call] of [['PUB', pubCall], ['KILL', killCall]]) {
    const { am, rc, routed, logs } = held();
    am._rootBeacons.set(T, { root: idHex(NEAR), at: am._now() - rc._beaconMs * 2, exp: am._now() + 90_000 });
    check(`8b ${verb} precondition: no gate accepts the stale beacon`, rc.liveCloserRoot(T, { requireReachable: false }) === null);
    routed.length = 0;
    await call(am);
    check(`8b ${verb}, stale beacon, no channel → NOTHING sent`, routed.length === 0, JSON.stringify(routed.map((r) => r.type)));
    check(`8b ${verb} …held + undeliverable, no root born`, !am.axonRoles.has(T) && undeliv(logs));
  }
  // 8c. PUB / KILL, FRESH beacon: steered ONCE to the held root — the corpse
  //     window council 146/147 accepted, unchanged by the hold
  for (const [verb, call] of [['PUB', pubCall], ['KILL', killCall]]) {
    const { am, routed } = held();
    am._rootBeacons.set(T, { root: idHex(NEAR), at: am._now(), exp: am._now() + 90_000 });
    routed.length = 0;
    await call(am);
    check(`8c ${verb}, fresh beacon → exactly one send, pinned to the held root`,
      routed.length === 1 && routed[0].payload?.via?.[0] === idHex(NEAR), JSON.stringify(routed.map((r) => r.type)));
    check(`8c ${verb} …no root born`, !am.axonRoles.has(T));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
