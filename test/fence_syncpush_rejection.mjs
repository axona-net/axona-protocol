// fence_syncpush_rejection.mjs — crash safety for the v4.57.0 async _syncPush.
//
// SCOPE, stated up front because both reviewers insisted on it: this is a RUNTIME
// CRASH-SAFETY regression, NOT a Q2/C4 durability repair. It asserts that the
// process survives a rejecting transport and that the ACK/no-claim semantics of
// the two fire-and-forget sites are unchanged. It makes no claim whatsoever about
// replication evidence, and must never be cited as one.
//
// THE REGRESSION IT GUARDS. Before v4.57.0, _syncPush returned undefined — there
// was nothing to reject. Q2 made it return dht.routeMessage(...), and two callers
// drop that promise:
//     repairPlane.js  root HANDOFF in the unacked() loop
//     repairPlane.js  last-gasp fallback for leftovers
// Node >=15 TERMINATES the process on an unhandled rejection. So a shipped kernel
// could be killed by a transport that rejects — on the LEAVE path, while the node
// is already departing. Aster caught it; I had shipped it to testnet.
//
// WHY A CONTROL EXISTS BELOW. An unhandledRejection listener that never fires
// looks identical whether the code is safe or the harness is broken. Section 0
// deliberately creates an unhandled rejection and asserts the harness SEES it.
// Without that, this file would be a green light wired to nothing — which is
// exactly the failure that left the bridge's pubsub guard stuck red for months.
//
// Run: node test/fence_syncpush_rejection.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const NB = [idHex(REG | 0xaa0n), idHex(REG | 0xaafn), idHex(REG | 0xab0n)];

// ── Catch unhandled rejections for the whole run ────────────────────────────
const unhandled = [];
process.on('unhandledRejection', (r) => { unhandled.push(String(r?.message || r)); });
const settle = () => new Promise((r) => setTimeout(r, 60));

console.log('syncPush rejection — a rejecting transport must not kill the process\n');

// ── 0. CONTROL: the harness can actually see an unhandled rejection ─────────
{
  Promise.reject(new Error('control-unhandled'));            // deliberately dropped
  await settle();
  ok('0a. CONTROL — the harness DETECTS an unhandled rejection (else this file is a no-op)',
    unhandled.some(u => u.includes('control-unhandled')), JSON.stringify(unhandled));
  unhandled.length = 0;                                      // reset for the real cases
}

// ── the harness: a transport that REJECTS every route ───────────────────────
function mk({ reject = true } = {}) {
  const routed = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type) => {
      routed.push(type);
      if (reject) throw new Error('transport rejected');
      return { consumed: true, atNode: NB[0], hops: 1 };
    },
    neighbors: () => [...NB],
    findKClosest: async () => [...NB],
    bridgeId: () => null,
    isReachableId: () => true,
  };
  const am = new AxonaManager({ dht, now: () => Date.now(), rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, routed };
}

function seed(am, topicBig, { isRoot }) {
  am.pubsubSubscribe(topicBig);
  const role = isRoot ? am._becomeRoot(topicBig) : am.axonRoles.get(topicBig);
  if (!role) return null;
  role.isRoot = isRoot;
  role.cache.push({ msgId: 'm1', publishTs: 100, json: '{}', seq: 1, bytes: 80 });
  role.cacheIds?.add?.('m1');
  return role;
}

// ── 1. ROOT HANDOFF — the site in the unacked() loop ────────────────────────
{
  const { am, routed } = mk({ reject: true });
  const T = REG | 0x1001n;
  const role = seed(am, T, { isRoot: true });
  ok('1a. precondition: a ROOT role holding state exists', !!role && role.isRoot && role.cache.length === 1);

  await am.pubsubLeaveHandoff();
  await settle();

  ok('1b. the leave path really ran (routeMessage was called)', routed.length > 0, `routed=${routed.length}`);
  ok('1c. EVERY route rejected, and the process saw NO unhandled rejection',
    unhandled.length === 0, JSON.stringify(unhandled));
  ok('1d. ACK SEMANTICS UNCHANGED — a rejecting HANDOFF earns no exemption, so the ' +
     'job stays retryable rather than being silently retired',
    !(am._handoffAcked?.size > 0 && routed.every(t => String(t).includes('handoff'))),
    `handoffAcked=${am._handoffAcked?.size ?? 'n/a'}`);
}

// ── 2. LAST-GASP FALLBACK — leftovers after nothing acked ──────────────────
// With every route rejecting, no job can ack, so the leftovers path is forced.
{
  unhandled.length = 0;
  const { am, routed } = mk({ reject: true });
  const T = REG | 0x2001n;
  seed(am, T, { isRoot: false });

  await am.pubsubLeaveHandoff();
  await settle();

  ok('2a. the non-root leave path ran', routed.length > 0, `routed=${routed.length}`);
  ok('2b. the last-gasp fallback also survives a rejecting transport — no unhandled rejection',
    unhandled.length === 0, JSON.stringify(unhandled));

  // Aster, council 2026-08-01. 2b alone is satisfied by a WRONG implementation:
  // one that swallowed the rejection to `undefined` would also raise no unhandled
  // rejection — and dispatchVerdict(undefined) is 'unreported', NOT 'failed', so
  // the departing holder would be handed a PERMANENT retry exemption on a push
  // that went nowhere. Surviving is not the same as being correct, and only this
  // assertion tells them apart.
  ok('2c. a rejecting REPLICATE leaves _handoffAcked EMPTY — no permanent exemption ' +
     'earned from a push that failed (survival alone would not catch this)',
    (am._handoffAcked?.size ?? 0) === 0, `handoffAcked=${am._handoffAcked?.size ?? 'n/a'}`);
}

// ── 3. CONTROL: a WORKING transport still completes the handoff ─────────────
// Without this, "absorb everything and do nothing" would pass sections 1 and 2.
{
  unhandled.length = 0;
  const { am, routed } = mk({ reject: false });
  const T = REG | 0x3001n;
  seed(am, T, { isRoot: true });

  await am.pubsubLeaveHandoff();
  await settle();

  ok('3a. CONTROL — a healthy transport still dispatches the handoff',
    routed.length > 0, `routed=${routed.length}`);
  ok('3b. …and a healthy run raises no unhandled rejection either',
    unhandled.length === 0, JSON.stringify(unhandled));
}

// ── 4. THE RETURNED CONTRACT, asserted directly on the primitive ───────────
// Sections 1-3 prove the process lives. They do NOT prove _route hands back a
// verdict the ledger can classify. Both are required: survival without a correct
// verdict is how 'unreported' would quietly become 'delivered'.
{
  unhandled.length = 0;
  const { am } = mk({ reject: true });
  const T = REG | 0x4001n;

  const routeOut = await am._route(REG | 0xaa0n, 'pubsub:replicate',
    { topicId: idHex(T), from: idHex(SELF), msgs: [], dels: [] });
  ok('4a. a REJECTING _route resolves (does not throw) to a verdict object',
    routeOut && typeof routeOut === 'object', JSON.stringify(routeOut));
  ok('4b. …that verdict is consumed:false + transportError:true — the shape ' +
     'dispatchVerdict() classifies as FAILED, not as unreported',
    routeOut?.consumed === false && routeOut?.transportError === true, JSON.stringify(routeOut));

  const sendOut = await am._send('pubsub:replicate',
    { topicId: idHex(T), from: idHex(SELF), msgs: [], dels: [] });
  ok('4c. _send delegates to _route and returns the SAME failed verdict — it is ' +
     'not a second, unhardened emission path',
    sendOut?.consumed === false && sendOut?.transportError === true, JSON.stringify(sendOut));

  ok('4d. …and neither direct call raised an unhandled rejection',
    unhandled.length === 0, JSON.stringify(unhandled));
}

{
  // CONTROL for section 4: a healthy transport must still report success, or
  // "always return failed" would satisfy 4a-4c.
  const { am } = mk({ reject: false });
  const okOut = await am._route(REG | 0xaa0n, 'pubsub:replicate', { topicId: idHex(REG | 0x5001n) });
  ok('4e. CONTROL — a healthy _route still reports consumed:true and no transportError',
    okOut?.consumed === true && !okOut?.transportError, JSON.stringify(okOut));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
