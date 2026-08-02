// fence_durability_lifecycle.mjs — the durability machine must actually RUN its
// lifecycle, not merely document one.
//
// WHAT ASTER FOUND (council 2026-08-01, CHANGES-REQUIRED df41e05). I shipped
// durability.js with an attempt budget, an `expired` terminal state, and a module
// comment saying "the tick replicates again until the attempt budget runs out".
// None of that executed. record() was called at EXACTLY ONE site — the eager
// replicate at ingress — and refreshTick's _replicateRoots() was fire-and-forget,
// so no periodic result ever reached the ledger. Consequences, all measured:
//
//   · an entry that started verified:0 stayed pending FOREVER, even if a later
//     replication succeeded
//   · `expired` was unreachable: nothing decremented the budget
//   · rootReplicas:0 skipped record() entirely (the call sits inside the
//     `role.isRoot && this._rootReplicas` gate), so the entry was pending forever
//     and leave() cleared a STALLED entry after its 1.5s stall — which reads as
//     success and is not
//
// That is this version's own defect committed inside the module that removes it:
// I wrote the intent as though it were the implementation. This file exists so
// the lifecycle is asserted by execution rather than by comment.
//
// Run: node test/fence_durability_lifecycle.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { DURABILITY_ATTEMPTS } from '../src/pubsub/durability.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const idHex = (b) => b.toString(16).padStart(66, '0');
const TICK = 6_000;
const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });

// `verdict` is a function so a case can CHANGE the transport's answer mid-run —
// which is exactly what "initially failed, later consumed" requires.
async function root(name, verdict, { rootReplicas = 2 } = {}) {
  const desc = DESC(name);
  const topicId = await deriveTopicIdBig(desc);
  const region = topicId >> 256n;
  const selfId = (region << 256n) | 0x5eedn;
  const clock = { t: 1_700_000_000_000 };
  const dht = {
    verdictsSupported: true,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async () => verdict(),
    findKClosest: async () => [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)],
    neighbors: () => [idHex(selfId ^ 0x11n), idHex(selfId ^ 0x22n)],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas });
  am.nodeId = selfId; am.setLogSink(() => {});
  am.pubsubSubscribe(topicId);
  const role = am._becomeRoot(topicId);
  const author = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  await am._ingestPublish(role, JSON.stringify(env));
  return { am, clock, topicId, role, env };
}
const tick = async (am, clock, times = 1) => {
  for (let i = 0; i < times; i++) {
    clock.t += TICK;
    await am.refreshTick();
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));
  }
};

const FAIL = () => ({ consumed: false, exhausted: true });
const PASS = () => ({ consumed: true, hops: 1 });

console.log('durability lifecycle — the machine must RUN, not just be described\n');

// ── 1. rootReplicas:0 REACHES AN EXPLICITLY CHOSEN TERMINAL STATE ─────────
// Aster: "rootReplicas:0 reaches the explicitly chosen terminal state". The
// choice is `expired` with reason 'no-replication-configured' — this node will
// never attempt cohort replication, so the message can NEVER become durable and
// there is nothing to wait for. Leaving it pending (what shipped) made leave()
// burn its stall clock on a verdict that cannot arrive, then clear the entry.
{
  const { am, env } = await root('dl-nocohort', FAIL, { rootReplicas: 0 });
  const e = am._durability.get(env.msgId);
  ok('1a. with NO cohort configured the entry is TERMINAL immediately, not pending',
    e?.state === 'expired', JSON.stringify(e?.state));
  ok('1b. …and the terminal state names WHY, so it is a decision and not an accident',
    e?.reason === 'no-replication-configured', JSON.stringify(e?.reason));
  ok('1c. …it counts as UNDURABLE — history this node alone carries',
    am.durabilityUndurable() === 1 && am.durabilityPending() === 0,
    `undurable=${am.durabilityUndurable()} pending=${am.durabilityPending()}`);
}

// ── 2. INITIALLY FAILED, LATER CONSUMED → VERIFIED ────────────────────────
// The transition that could not happen before: nothing carried a later success
// into the ledger, so a first-attempt failure was permanent.
{
  let answer = FAIL;
  const { am, clock, env } = await root('dl-latewin', () => answer());
  ok('2a. after a failed eager replicate the entry is PENDING, not terminal',
    am._durability.state(env.msgId) === 'pending', String(am._durability.state(env.msgId)));
  await tick(am, clock, 1);
  ok('2b. …still pending while the transport keeps failing',
    am._durability.state(env.msgId) === 'pending', String(am._durability.state(env.msgId)));
  answer = PASS;                                   // the cohort comes back
  await tick(am, clock, 1);
  ok('2c. …a LATER cohort verdict drives it to VERIFIED — the periodic path now ' +
     'reaches the ledger, which is the leg that was missing',
    am._durability.state(env.msgId) === 'verified', String(am._durability.state(env.msgId)));
  ok('2d. …and it is no longer counted as outstanding work',
    am.durabilityPending() === 0 && am.durabilityUndurable() === 0,
    `pending=${am.durabilityPending()} undurable=${am.durabilityUndurable()}`);
}

// ── 3. REPEATED FAILURE REACHES EXPIRED AT THE CONFIGURED BOUND ───────────
// `expired` was unreachable because nothing decremented the budget. The bound is
// asserted against the exported constant, not a magic number, so changing the
// budget cannot silently invalidate this check.
{
  const { am, clock, env } = await root('dl-expire', FAIL);
  ok('3a. precondition — the attempt budget is a real exported bound',
    Number.isFinite(DURABILITY_ATTEMPTS) && DURABILITY_ATTEMPTS > 1, String(DURABILITY_ATTEMPTS));
  // The eager ingress replicate already consumed one attempt.
  await tick(am, clock, DURABILITY_ATTEMPTS - 2);
  ok('3b. …still PENDING one attempt short of the bound — it does not expire early',
    am._durability.state(env.msgId) === 'pending',
    `state=${am._durability.state(env.msgId)} attempts=${am._durability.get(env.msgId)?.attempts}`);
  await tick(am, clock, 1);
  ok('3c. …and reaches EXPIRED exactly AT the bound',
    am._durability.state(env.msgId) === 'expired',
    `state=${am._durability.state(env.msgId)} attempts=${am._durability.get(env.msgId)?.attempts}`);
  ok('3d. …counted as undurable, which is the honest answer: we tried and failed',
    am.durabilityUndurable() === 1 && am.durabilityPending() === 0,
    `undurable=${am.durabilityUndurable()} pending=${am.durabilityPending()}`);
}

// ── 4. leave() OBSERVES A TERMINAL STATE, not a cleared stall ─────────────
// Aster: "leave observes that terminal state rather than merely clearing a
// stalled pending entry." The drain reads durabilityPending(); once every entry
// is terminal that count is 0, so the drain exits because the work is DONE —
// including when the answer was no.
{
  const { am, clock } = await root('dl-drain', FAIL);
  ok('4a. precondition — outstanding durability work exists before the bound',
    am.durabilityPending() === 1, `pending=${am.durabilityPending()}`);
  await tick(am, clock, DURABILITY_ATTEMPTS);
  ok('4b. every entry is TERMINAL, so the drain has nothing to wait on',
    am.durabilityPending() === 0, `pending=${am.durabilityPending()}`);
  ok('4c. …and the terminal state is RETAINED and reportable, not erased — an ' +
     'operator can still see this node holds sole copies',
    am.durabilityUndurable() === 1, `undurable=${am.durabilityUndurable()}`);
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
