// fence_dispatch_contract.mjs — Q2 / C4. A root's replica ledger must record the
// EVIDENCE it has, not the hope it started with.
//
// THE DEFECT. The chain is _replicateRole → _syncPush → _route → dht.routeMessage.
// `_route` (AxonaManager.js) DISCARDS the promise routeMessage returns, and
// `_syncPush` (syncEngine.js) does not return it either. routeMessage is async and
// reports failure by RESOLVING {consumed:false, exhausted:true} — it does not
// throw. So the try/catch around the push sees nothing, and the ledger records a
// backup for a peer that received zero bytes.
//
// The predecessor fence (fence_replica_ledger) stubbed _syncPush out and threw
// synchronously, which exercised the catch block rather than the dispatch chain,
// and so certified its own premise. This one drives the REAL _syncPush and varies
// only what the TRANSPORT reports. Nothing in the push path is replaced.
//
// THREE OUTCOMES, NOT TWO. Aster's split is selection / dispatch / receipt, and an
// adapter that returns nothing at all is a fourth thing: no report. Collapsing
// "the transport told me it failed" into "the transport told me nothing" is the
// same confident-false-negative that cost a day on Q1, so:
//
//   consumed === true          → recorded, via:'consumed'    (verified dispatch)
//   consumed === false         → NOT recorded                (explicit failure)
//   rejection                  → NOT recorded                (explicit failure)
//   undefined / no report      → recorded, via:'unreported'  (honest ignorance)
//
// 'unreported' exists because sim and test adapters legitimately return nothing;
// treating their silence as failure would fabricate a negative in the other
// direction. It is recorded but never counted as verified.
//
// WHAT THIS STILL DOES NOT PROVE. consumed:true means the remote ACCEPTED the
// message into its ingest queue — _onReplicate returns after _ingestEnqueue. It is
// not proof of durable possession. This fence pins dispatch evidence, which is a
// strictly better claim than "a local call did not throw" and strictly weaker than
// receipt. Receipt needs an ack and is not in Q2.
//
// EXPECTED RED against the current tree: 1a/1b/1c/1d all fail — every outcome,
// including explicit exhaustion, is recorded as a replica.
//
// Run: node test/fence_dispatch_contract.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const NB1 = idHex(REG | 0xaa0n), NB2 = idHex(REG | 0xaafn);
const TICK = 5_000;

// `report` is what the TRANSPORT says. Everything above it is production code.
function mk(report, { verdictsSupported = true } = {}) {
  const clock = { t: 1_000_000 };
  const routed = [];
  const dht = {
    // v4.58.0: capability is DECLARED, never inferred from a return value.
    verdictsSupported,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type) => { routed.push(type); return report(); },
    neighbors: () => [NB1, NB2],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, routed };
}

async function rootWithState(am, clock, T) {
  am.pubsubSubscribe(T);
  am._becomeRoot(T);
  am.axonRoles.get(T).cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });
  for (let i = 0; i < 4; i++) { clock.t += TICK; await am.refreshTick(); }
  return am.axonRoles.get(T);
}

const EXHAUSTED = () => ({ consumed: false, atNode: NB1, hops: 3, exhausted: true });
const TERMINAL  = () => ({ consumed: false, atNode: NB1, hops: 1, terminal: true });
const CONSUMED  = () => ({ consumed: true,  atNode: NB1, hops: 2 });
const REJECTS   = () => { throw new Error('route rejected'); };   // async fn → rejection
const SILENT    = () => undefined;

console.log('dispatch contract — a root records the evidence it has, not the hope it started with\n');

// ── 1. EXPLICIT FAILURE IS NEVER A REPLICA ─────────────────────────────────
{
  const { am, clock, routed } = mk(EXHAUSTED);
  const role = await rootWithState(am, clock, REG | 0x1001n);
  ok('1a. the real dispatch chain ran (routeMessage was actually called)',
    routed.length > 0, `routed=${routed.length}`);
  ok('1b. routing EXHAUSTED — no cohort member is recorded as holding a replica',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}
{
  const { am, clock } = mk(TERMINAL);
  const role = await rootWithState(am, clock, REG | 0x2001n);
  ok('1c. routing TERMINAL (delivered nowhere) — still no replica recorded',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}
{
  const { am, clock } = mk(REJECTS);
  const role = await rootWithState(am, clock, REG | 0x3001n);
  ok('1d. routeMessage REJECTS asynchronously — still no replica recorded ' +
     '(the old sync try/catch could not see this at all)',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}

// ── 2. CONTROL: a working transport DOES populate the ledger ───────────────
// Without this, "record nothing, ever" passes section 1 and is a regression
// wearing a fix's clothing.
{
  const { am, clock } = mk(CONSUMED);
  const role = await rootWithState(am, clock, REG | 0x4001n);
  ok('2a. CONTROL — consumed:true records the cohort',
    role.replicas.size > 0, `replicas=${role.replicas.size}`);
  ok('2b. …the recorded members are the wanted cohort, not arbitrary ids',
    [...role.replicas.keys()].every(h => h === NB1 || h === NB2),
    JSON.stringify([...role.replicas.keys()]));
  ok('2c. …and each entry carries WHY it is believed (via:"consumed")',
    [...role.replicas.values()].every(v => v && v.via === 'consumed'),
    JSON.stringify([...role.replicas.values()]));
}

// ── 3. A VOID RETURN NEVER CREDITS (v4.58.0 — REWRITTEN) ──────────────────
// THIS SECTION PREVIOUSLY ASSERTED THE OPPOSITE, and had to be rewritten rather
// than kept. It pinned "an adapter that reports NOTHING still records the cohort
// … via:'unreported'" — which made the suite DEFEND the bug: a silent adapter
// earned a replica it had no evidence for, and I had let test doubles set a
// production durability semantic.
//
// Aster and Orion both rejected the INFERENCE itself (council 2026-08-01):
// capability is DECLARED, never guessed. A void return from an adapter that
// CLAIMS to report verdicts is a CONTRACT VIOLATION — loud, classified FAILED.
// An adapter that declares it cannot report credits nothing. Neither is a
// creditable unknown, and there is no degraded mode.
{
  const { am, clock } = mk(SILENT, { verdictsSupported: true });
  const role = await rootWithState(am, clock, REG | 0x5001n);
  ok('3a. declared-reporting adapter returning VOID = contract violation, NO ' +
     'replica credited (this is the exact inverse of the assertion it replaced)',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}
{
  const { am, clock } = mk(SILENT, { verdictsSupported: false });
  const role = await rootWithState(am, clock, REG | 0x5101n);
  ok('3b. declared-NON-reporting adapter credits nothing either — an honest ' +
     'admission of no evidence is still no evidence',
    role.replicas.size === 0, `replicas=${role.replicas.size}`);
}

// ── 4. THE CALLER CAN GATE ON IT ───────────────────────────────────────────
// wireHandlers.js:339 / :654 confirm a publish (and a kill) to the app after
// awaiting _replicateRole. Today they .catch(()=>{}) and confirm regardless, so
// a publish whose every replication push exhausted still reports durable. They
// cannot do better while _replicateRole returns undefined.
{
  const { am, clock } = mk(EXHAUSTED);
  const role = await rootWithState(am, clock, REG | 0x6001n);
  const r = await am._replicateRole(REG | 0x6001n, role, null, clock.t += TICK);
  ok('4a. _replicateRole REPORTS its outcome to the caller',
    r && typeof r === 'object', JSON.stringify(r));
  ok('4b. …counting verified dispatches separately from failures',
    r && r.verified === 0 && r.failed > 0, JSON.stringify(r));
}
{
  const { am, clock } = mk(CONSUMED);
  const role = await rootWithState(am, clock, REG | 0x7001n);
  const r = await am._replicateRole(REG | 0x7001n, role, null, clock.t += TICK);
  ok('4c. CONTROL — a working transport reports verified dispatches',
    r && r.verified > 0 && r.failed === 0, JSON.stringify(r));
}
{
  // Every early return must also answer, or a caller gating on the shape
  // crashes on the paths that matter least and are hit most.
  const { am, clock } = mk(CONSUMED);
  am.pubsubSubscribe(REG | 0x8001n);
  am._becomeRoot(REG | 0x8001n);
  const empty = am.axonRoles.get(REG | 0x8001n);           // root holding nothing
  const r = await am._replicateRole(REG | 0x8001n, empty, null, clock.t);
  ok('4d. the "nothing to replicate" early return answers in the same shape',
    r && typeof r === 'object' && typeof r.verified === 'number', JSON.stringify(r));
}

// ── 5. role.attempted IS BOUNDED (v4.58.0 amendment) ───────────────────────
// It was CALLED bounded and was not. Every failed / unsupported / violating
// target stayed in the map forever, so a long-lived root under churn accumulated
// one entry per peer it had ever tried — a leak wearing a log's clothing
// (Aster, council 2026-08-01). Now pruned to the CURRENT cohort on the same tick
// role.replicas is, and cleared when there is no cohort at all.
//
// Driven by CHURNING the cohort: findKClosest returns a different pair each
// call, so without pruning the map grows by two per tick without limit.
{
  const clock = { t: 1_000_000 };
  let gen = 0;
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async () => EXHAUSTED(),          // every push fails → all land in `attempted`
    // A fresh, disjoint cohort every call — the churn case.
    findKClosest: async () => { gen++; return [idHex(REG | BigInt(0x1000 + gen * 2)),
                                               idHex(REG | BigInt(0x1001 + gen * 2))]; },
    neighbors: () => [NB1, NB2],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF; am.setLogSink(() => {});
  const T = REG | 0x9001n;
  am.pubsubSubscribe(T); am._becomeRoot(T);
  am.axonRoles.get(T).cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });

  for (let i = 0; i < 12; i++) { clock.t += TICK; await am.refreshTick(); }
  const role = am.axonRoles.get(T);
  ok('5a. after 12 ticks with a fully-churning cohort, attempted holds at most ' +
     'the cohort size — it does not accumulate one entry per peer ever tried',
    role.attempted.size <= 2, `attempted=${role.attempted.size} after 12 churned ticks`);
  ok('5b. …and what it holds is the CURRENT cohort, not history',
    [...role.attempted.keys()].every(h => h.endsWith((0x1000 + gen * 2).toString(16)) ||
                                          h.endsWith((0x1001 + gen * 2).toString(16))),
    JSON.stringify([...role.attempted.keys()].map(h => h.slice(-4))));
}
{
  // No cohort at all: nothing is outstanding, so nothing may be REMEMBERED as
  // outstanding. The early return used to leave the last cohort's failures
  // pinned for the lifetime of the role.
  const clock = { t: 1_000_000 };
  let empty = false;
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async () => EXHAUSTED(),
    findKClosest: async () => (empty ? [] : [NB1, NB2]),
    neighbors: () => [],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF; am.setLogSink(() => {});
  const T = REG | 0x9101n;
  am.pubsubSubscribe(T); am._becomeRoot(T);
  am.axonRoles.get(T).cache.push({ msgId: 'm1', ts: clock.t, json: '{}' });
  // Driven through _replicateRole directly rather than the tick: replication is
  // PACED, so a fixed number of ticks does not reliably reach the push and the
  // precondition would be establishing nothing. Section 4 uses the same handle.
  const role = am.axonRoles.get(T);
  await am._replicateRole(T, role, null, clock.t += TICK);
  ok('5c. precondition — failures were recorded while a cohort existed',
    role.attempted.size > 0, `attempted=${role.attempted.size}`);
  empty = true;
  await am._replicateRole(T, role, null, clock.t += TICK);
  ok('5d. the cohort empties → attempted is CLEARED, not left pinned for the ' +
     'lifetime of the role',
    role.attempted.size === 0, `attempted=${role.attempted.size}`);
}

// ── 6. EVERY RETURN CARRIES EVERY KEY ──────────────────────────────────────
// The early return used to emit a dead `unreported` (a v4.57.0 leftover) and
// OMIT unsupported/violation, so a caller reading those got undefined on exactly
// the quiet paths. Undefined is not zero, and `undefined > 0` is false — the
// kind of silent asymmetry this whole version exists to remove.
{
  const { am, clock } = mk(CONSUMED);
  const T = REG | 0x9201n;
  am.pubsubSubscribe(T); am._becomeRoot(T);                    // root holding nothing
  const r = await am._replicateRole(T, am.axonRoles.get(T), null, clock.t);
  const keys = ['attempted', 'verified', 'failed', 'unsupported', 'violation'];
  ok('6a. the early return carries every counter as a number',
    keys.every(k => typeof r[k] === 'number'), JSON.stringify(r));
  ok('6b. …and carries no dead `unreported` key',
    !('unreported' in r), JSON.stringify(Object.keys(r)));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
