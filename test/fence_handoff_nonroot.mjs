// fence_handoff_nonroot.mjs — Aster's fourth requirement, on the branch that
// actually implements it.
//
// THE CORRECTION. fence_handoff_two_node claimed this property and did not test
// it: its pair() calls _becomeRoot(), so every case drove the ROOT path
// (HANDOFF → HANDOFFACK). A departing NON-ROOT holder takes a different branch —
// repairPlane's leave-handoff REPLICATE leg, where the exemption is granted from
// a DISPATCH PROMISE and there is no ack at all:
//
//     const dispatched = (p) => Promise.resolve(p).then(
//       (r) => dispatchVerdict(r, decl) === 'consumed', () => false);
//     const sends = [dispatched(this._syncPush(j.heir, j.t, j.role, 'REPLICATE'))];
//     Promise.all(sends).then((oks) => { if (oks.some(Boolean)) this._handoffAcked.add(j.key); });
//
// That is the line Aster named in the ORIGINAL Q2 review: it used to read
// `dispatchVerdict(r) !== 'failed'`, a NEGATIVE test, which is precisely how
// 'unknown' sneaks into a success path. It now demands an explicit 'consumed'.
// This file is the control for that, driven through the real code.
//
// WHY THE DISTINCTION MATTERS. There is no REPLICATE ack, so dispatch is the ONLY
// evidence available on this path. That makes it the place where a wrong answer
// is least visible and most expensive: a departing holder that grants itself the
// exemption on no evidence stops retrying and stops the cohort spray, and the
// last copy of a topic leaves with it (#340).
//
// Run: node test/fence_handoff_nonroot.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const idHex = (b) => b.toString(16).padStart(66, '0');
const lc = (s) => String(s).toLowerCase();
const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });

// A NON-ROOT holder: it has the topic's history but does not root it. Built by
// the production transitions — adoptChild forms the non-root relay and writes the
// upstream pin, becomeBackup is what syncEngine calls on a REPLICATE ingest.
// `rootVisible` decides whether the principal is a CURRENT direct neighbour.
// It is the gate (_rootAliveForLeave): a non-root holder that can prove its root
// is alive right now does not hand off at all, because the history demonstrably
// exists elsewhere. Section 0 pins that; every other case here hides the
// principal, which is the situation the branch is FOR — the root cannot be
// confirmed, so this departing holder may be carrying the last copy.
async function nonRootHolder(name, { verdict, declares = true, rootVisible = false }) {
  const desc = DESC(name);
  const topicId = await deriveTopicIdBig(desc);
  const region = topicId >> 256n;
  const selfId    = (region << 256n) | 0x00a1n;
  const principal = (region << 256n) | 0x00b2n;    // the root we sit under
  const heir      = (region << 256n) | 0x00c3n;
  const clock = { t: 1_700_000_000_000 };
  const sent = [];
  const dht = {
    verdictsSupported: declares,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async (t, type) => { sent.push(type); return verdict(); },
    findKClosest: async () => [idHex(heir)],
    neighbors: () => (rootVisible ? [idHex(principal), idHex(heir)] : [idHex(heir)]),
    bridgeId: () => null,
    lookup: async () => ({ path: [idHex(heir)] }),
    isReachableId: () => true,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = selfId; am.setLogSink(() => {});

  const role = am._rootClaim.adoptChild(topicId, idHex(principal));   // NON-root relay
  am._rootClaim.becomeBackup(topicId, role, idHex(principal));        // → _backupTopics
  // Give it real history to hand off: a signed envelope, ingested as stamped state.
  const author = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: clock.t });
  role.cache.push({ msgId: env.msgId, publishTs: clock.t, json: JSON.stringify(env), seq: 1, bytes: 200 });
  role.cacheIds.add(env.msgId);
  return { am, clock, topicId, role, sent, env };
}

const CONSUMED = () => ({ consumed: true, hops: 1 });
const FAILED   = () => ({ consumed: false, exhausted: true });
const VOID     = () => undefined;
const REJECT   = () => { throw new Error('transport rejected'); };

async function run(label, opts, expectExempt) {
  const { am, topicId, sent } = await nonRootHolder(`nr-${label}`, opts);
  await am.pubsubLeaveHandoff();
  await new Promise(r => setImmediate(r));
  const exempt = am._handoffAcked?.has(lc(idHex(topicId))) ?? false;
  ok(`${label}: exemption ${expectExempt ? 'GRANTED' : 'REFUSED'}`,
    exempt === expectExempt,
    `exempt=${exempt}, want ${expectExempt}; sent=${JSON.stringify(sent)}`);
  return { am, sent };
}

console.log('non-root handoff — dispatch is the ONLY evidence, so it must be explicit\n');

// ── 0. THE BRANCH IS REALLY THE NON-ROOT ONE ──────────────────────────────
// Without this the whole file could be exercising the root path again, which is
// exactly the mistake being corrected.
{
  const { am, topicId, role } = await nonRootHolder('nr-shape', { verdict: CONSUMED });
  ok('0a. the holder is NOT a root — this is the branch the previous fence missed',
    role.isRoot === false, `isRoot=${role.isRoot}`);
  ok('0b. …it is a registered BACKUP holding real history',
    am._backupTopics.has(topicId) && role.cache.length === 1, `cache=${role.cache.length}`);
}

// ── 0c. THE LIVENESS GATE — and why every case below hides the root ───────
// My first draft of this file left the principal in neighbors() and measured
// sent=[]: NOTHING dispatched, so the four "no exemption" results were vacuous —
// they would have passed against any implementation whatsoever. That is the same
// confident-false-negative this whole version is about, reproduced in the test
// meant to catch it, and it is why section 2 exists.
//
// The cause was correct behaviour: a holder that can prove its root is a CURRENT
// direct neighbour skips the handoff, because the history demonstrably lives
// somewhere that is not leaving. Asserting it here means the fixture's shape is
// a stated premise rather than a silent tweak that makes the test go green.
{
  const { am, sent } = await nonRootHolder('nr-alive', { verdict: CONSUMED, rootVisible: true });
  await am.pubsubLeaveHandoff();
  await new Promise(r => setImmediate(r));
  ok('0c. a non-root holder whose root is PROVABLY alive hands off nothing — so ' +
     'every case below must hide the root, or it measures an empty run',
    sent.length === 0, JSON.stringify(sent));
}

// ── 1. ONLY AN EXPLICIT `consumed` GRANTS THE EXEMPTION ───────────────────
await run('consumed',            { verdict: CONSUMED },                  true);   // CONTROL
await run('explicit-failure',    { verdict: FAILED },                    false);
await run('rejection',           { verdict: REJECT },                    false);
await run('declared-false/void', { verdict: VOID, declares: false },     false);
await run('declared-true/void',  { verdict: VOID, declares: true },      false);

// ── 2. THE REFUSAL IS NOT SILENCE — the send really was attempted ─────────
// A "no exemption" that happened because nothing was sent proves nothing. This
// is the same trap that made the single-node handoff checks vacuous.
{
  const { sent } = await run('evidence-check', { verdict: FAILED }, false);
  ok('2a. the REPLICATE really was dispatched — the refusal is a judgement about ' +
     'the verdict, not a consequence of nothing happening',
    sent.some(t => /replicate|handoff/i.test(t)), JSON.stringify(sent));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
