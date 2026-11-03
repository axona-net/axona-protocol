#!/usr/bin/env node
// =====================================================================
// fence_cbr_hb_hp_findings.mjs — Orion 3046d876 on the handoff binding
// (2e5c24e) and 3a (bd5fd7e).
//   HB-1  W becomes root while it verifies a signed offer: it answers root to
//         root (a frame to L), never silence.
//   HB-2  L's epoch bumps while its first offer is being signed: the
//         superseded offer is never sent, and its corr is closed.
//   HP-1  one unreachable target among four: the three reached all answer
//         no-role, and the try is refunded.
//   HP-2  the budget is checked before any lookup: past the window with
//         nobody to ask, no findKClosest or lookup runs, and the root reports
//         no-holder-found.
// Run: node test/fence_cbr_hb_hp_findings.mjs
// =====================================================================
import { Fabric, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { HOLDER_PROBE_WINDOW_MS } from '../src/pubsub/constants.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { handoffTranscript } from '../src/pubsub/handoffBinding.js';
import { bytesToHex } from '../src/pubsub/ackProof.js';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { makeMergeState } from '../src/pubsub/rootClaim.js';

const EXPECT = 4;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });
const LOC = { lat: 40.7, lng: -74 };
const settleMore = async (fab) => { for (let i = 0; i < 8; i++) { await fab.settle(); await new Promise((r) => setImmediate(r)); } };
const armed = { cutover: true, handoffBinding: true };

async function pair(name) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const a = await createNodeIdentity(LOC), b = await createNodeIdentity(LOC);
  const [wI, lI] = ((BigInt('0x' + a.id) ^ Lt.topicId) < (BigInt('0x' + b.id) ^ Lt.topicId)) ? [a, b] : [b, a];
  const W = fab.addNode(BigInt('0x' + wI.id), { identity: wI, ...armed });
  const L = fab.addNode(BigInt('0x' + lI.id), { identity: lI, ...armed });
  W.am._announceRoot = () => {}; L.am._announceRoot = () => {};
  const lRole = L.rc.become(Lt.topicId, 'sub-terminal');
  fab.clock += 10;
  const env = await buildEnvelope({ topic: Lt.desc, message: { k: 1 }, seq: 1, identity: author, ts: fab.clock });
  await L.handlers.get('pubsub:pub')({ topicId: idHex(Lt.topicId), json: JSON.stringify(env) }, { targetId: L.id, isTerminal: true, hopCount: 2, fromId: idHex(Lt.publisher) });
  await fab.settle();
  return { Lt, fab, W, L, lI, lRole };
}

console.log('fence_cbr_hb_hp_findings: Orion 3046d876 HB-1, HB-2, HP-1, HP-2');

{ const S = await pair('cbr-hb1');
  const corr = 'f'.repeat(32);
  const offer = { topicId: idHex(S.Lt.topicId), corr, loser: idHex(S.L.id), loserEpoch: S.lRole.epoch | 0, hw: S.L.am._highWater(S.lRole), lw: S.L.am._lowWater(S.lRole), count: S.lRole.cache.length, timing: S.L.am._mergeTiming(), handoff: 1 };
  const sig = await S.lI.sign(handoffTranscript({ topicId: offer.topicId, corr, loser: offer.loser, winner: idHex(S.W.id), loserEpoch: offer.loserEpoch }));
  const mark = (S.fab.sent ?? []).length;
  await S.W.handlers.get('pubsub:mergeoffer')({ ...offer, handoffSig: bytesToHex(sig), handoffPub: bytesToHex(S.lI.pubkey) }, { targetId: S.W.id, isTerminal: true, hopCount: 1, fromId: idHex(S.L.id) });
  S.W.rc.become(S.Lt.topicId, 'sub-terminal');   // W roots while the signature is being verified
  await settleMore(S.fab);
  const answers = (S.fab.sent ?? []).slice(mark).filter((j) => (j.type === 'pubsub:pullup' || j.type === 'pubsub:replayack') && j.payload?.corr === corr);
  check('HB-1 W roots during verification: it answers the offer root to root, never silence', answers.length >= 1, JSON.stringify({ answers: answers.map((j) => j.type) })); }

{ const S = await pair('cbr-hb2');
  S.lRole.merge ??= makeMergeState(); S.lRole.merge.candidate = { id: idHex(S.W.id).toLowerCase(), epoch: 0 };
  const mark = (S.fab.sent ?? []).length;
  S.L.am._mergeOffer(S.Lt.topicId, S.lRole);                  // corr1: signing starts (async)
  const corr1 = [...S.lRole.sync.openMerges.keys()][0];
  S.lRole.epoch = (S.lRole.epoch | 0) + 1;                     // the epoch bumps before the signature resolves
  S.L.am._mergeOffer(S.Lt.topicId, S.lRole);                  // corr2 supersedes corr1
  await settleMore(S.fab);
  const offers = (S.fab.sent ?? []).slice(mark).filter((j) => j.type === 'pubsub:mergeoffer').map((j) => j.payload.corr);
  check('HB-2 a superseded offer is never sent, and its corr is closed', offers.length === 1 && offers[0] !== corr1 && S.lRole.merge.closedCorrs?.has(corr1), JSON.stringify({ offers: offers.map((c) => c.slice(0, 6)), corr1: corr1.slice(0, 6), closed: S.lRole.merge.closedCorrs?.has(corr1) ?? false })); }

{ const REG = 0x87n << 248n, hex = (b) => b.toString(16).padStart(66, '0'), H = (x) => x << 224n;
  const T = REG | 0x1000n, SELF = T ^ H(0x1n);
  const peers = [0x10n, 0x20n, 0x30n, 0x40n].map((x) => hex(T ^ H(x)));
  const clock = { t: 1_000_000 };
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [], bridgeId: () => null, findKClosest: async () => peers };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 0, holderProbe: true });
  am.nodeId = SELF; am.setLogSink(() => {});
  am.pubsubSubscribe(T); am._becomeRoot(T);
  const role = am.axonRoles.get(T);
  am._syncPullAwait = (b, t, policy, opts) => {
    const h = hex(b), reachable = h !== peers[3];
    if (reachable) queueMicrotask(() => am._onProbeNack({ topicId: hex(T), corr: opts.probe, from: h, disposition: 'no-role' }));
    return Promise.resolve({ consumed: reachable });
  };
  await am._emptyRootProbe(T);
  const st = am.probeStatus(T);
  check('HP-1 one unreachable target, three no-role NACKs: the try is refunded', st.tries === 0 && st.roleless === 3, JSON.stringify(st)); }

{ const REG = 0x87n << 248n, hex = (b) => b.toString(16).padStart(66, '0'), H = (x) => x << 224n;
  const T = REG | 0x2000n, SELF = T ^ H(0x1n);
  const clock = { t: 1_000_000 }; let lookups = 0;
  const dht = { verdictsSupported: true, getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [], bridgeId: () => null,
                findKClosest: async () => { lookups++; return []; }, lookup: async () => { lookups++; return { path: [] }; } };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 0, holderProbe: true });
  am.nodeId = SELF; am.setLogSink(() => {});
  am.pubsubSubscribe(T); am._becomeRoot(T);
  const role = am.axonRoles.get(T);
  role.sync.probeStart = clock.t - HOLDER_PROBE_WINDOW_MS - 1;   // earlier refunded tries opened the window
  await new Promise((r) => setTimeout(r, 50));
  lookups = 0;                                                     // subscribe's own root-hint resolution is not the probe
  await am._emptyRootProbe(T);
  check('HP-2 past the window with nobody to ask: no lookup runs and the root reports no-holder-found', lookups === 0 && am.probeStatus(T).degraded === 'no-holder-found', JSON.stringify({ lookups, st: am.probeStatus(T) })); }

console.log(`\nfence_cbr_hb_hp_findings: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
