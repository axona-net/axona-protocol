#!/usr/bin/env node
// =====================================================================
// fence_merge_sim_partition.mjs — dual-root packet Part 6, the sim fences
// (ring / hand-fork / partition) on the kernel's in-tree sim transport
// (axona-docs 14a6bd8; David "build it" 2026-10-17).
//
// Real AxonaPeers over SimNetwork, mergeCapable on, real lookups and
// beacons, no fabric shortcuts. Two scenes:
//   PARTITION: six peers in two clusters joined by one link; the link is cut;
//     a publish on each side forms a root on each side (two roots, each with
//     its own body); the link is restored; the farther root's self-verify
//     visits the closer root and the machine runs. End: ONE root, the
//     XOR-closest, holding both bodies; the other a child pinned to it.
//   HAND-FORK: with the mesh whole, a farther peer is made root by hand
//     while the true root holds bodies. The hand-made root mints
//     knownEpoch + 1 and WINS under the Part 3 relation (epoch before
//     closeness): the true root demotes INTO the fork through a round, the
//     fork holds every body, and the fork's later offer to the now non-root
//     true root is answered not-root. The packet promises the union, not
//     the owner.
//
// Checks: (P1) two roots form under the partition, each holding its own
// body; (P2) after the heal and the verify the farther root records the
// closer as its candidate, offers, and the round completes (merge-step5 at
// the loser, merge-complete at the winner); (P3) exactly one root remains,
// the XOR-closest, holding both bodies, no custody-refused anywhere; (P4)
// the loser is a child with its upstream pinned to the winner; (H1) the
// true root demotes INTO the higher-epoch fork through a round (no direct
// demote) and the fork holds every body; (H2) the fork's offer to the
// non-root true root is answered not-root, the fork keeps its claim with
// the candidate cleared (no legacy fallback), one root remains.
//
// Mutant (red): MERGE_CAPABLE=0 (both scenes run unarmed) → P2 and H1 fail
// (no offer; direct demotes); drop the not-root answer in _onMergeOffer →
// H2 fails (the fork's offer times out into the legacy fallback).
// Run: node test/fence_merge_sim_partition.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { deriveTopicIdBig }         from '../src/pubsub/post.js';
import { ROOT_VERIFY_FIRST_MS }     from '../src/pubsub/constants.js';
import { buildEnvelope }            from '../src/pubsub/envelope.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const hx = (b) => b.toString(16).slice(0, 8);
const MERGE_ON = process.env.MERGE_CAPABLE !== '0';

const net = new SimNetwork();
const domain = new AxonaDomain();
const logsOf = new Map();

async function makePeer(lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, mergeCapable: MERGE_ON });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  const am = peer._requireAxonaManager('fence');   // the manager is built lazily on first use
  const logs = []; const orig = am._log.bind(am);
  am._log = (lvl, ev, ctx) => { logs.push({ lvl, ev: ev.startsWith('pubsub:') ? ev : `pubsub:${ev}`, ctx }); return orig(lvl, ev, ctx); };
  logsOf.set(id.id, logs);
  return { id, hex: id.id, big: fromHex(id.id), node, peer, transport, am, logs };
}
const link = async (a, b) => {
  a.node.synaptome.set(b.big, new Synapse({ peerId: b.big, latencyMs: 5, stratum: clz264(a.big ^ b.big) }));
  b.node.synaptome.set(a.big, new Synapse({ peerId: a.big, latencyMs: 5, stratum: clz264(b.big ^ a.big) }));
  await a.transport.openConnection(b.hex);
  await wait(10);
};
const unlink = async (a, b) => {
  a.node.synaptome.delete(b.big); b.node.synaptome.delete(a.big);
  try { await a.transport.closeConnection(b.hex); } catch { /* */ }
  try { await b.transport.closeConnection(a.hex); } catch { /* */ }
  await wait(10);
};
const log = (p, ev) => p.logs.filter((l) => l.ev === `pubsub:${ev}`);
const roleOf = (p, t) => p.am.axonRoles.get(t);
const rootsFor = (peers, t) => peers.filter((p) => roleOf(p, t)?.isRoot);
const until = async (fn, ms = 8000, step = 50) => { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (fn()) return true; await wait(step); } return fn(); };

console.log(`fence_merge_sim_partition: Part 6 — partition + hand-fork on the in-tree sim (mergeCapable ${MERGE_ON ? 'on' : 'OFF'})`);

const author = await createAuthorIdentity();
const desc = { region: 'useast', owner: null, name: 'sim-partition-' + Date.now().toString(36), write: 'open' };
const T = await deriveTopicIdBig(desc);
// six peers; A-cluster = the three farthest from T, B-cluster = the three closest; one bridge link A0—B0
const raw = []; for (let i = 0; i < 6; i++) raw.push(await makePeer(40 + i * 0.01, -70 + i * 0.01));
raw.sort((p, q) => ((p.big ^ T) < (q.big ^ T) ? -1 : 1));
const B = raw.slice(0, 3), A = raw.slice(3);                   // B[0] is the XOR-closest = the true terminus
for (const c of [A, B]) { await link(c[0], c[1]); await link(c[1], c[2]); await link(c[0], c[2]); }
await link(A[0], B[0]);
const all = [...A, ...B];

{ // PARTITION
  await unlink(A[0], B[0]);
  await A[2].peer.pub(desc, { side: 'A', k: 1 }, { signWith: author }); await wait(300);
  await B[2].peer.pub(desc, { side: 'B', k: 2 }, { signWith: author }); await wait(300);
  const roots = rootsFor(all, T);
  const aRoot = rootsFor(A, T)[0], bRoot = rootsFor(B, T)[0];
  check('P1 two roots form under the partition, one per side, each holding its own body', roots.length === 2 && !!aRoot && !!bRoot && roleOf(aRoot, T).cache.length === 1 && roleOf(bRoot, T).cache.length === 1, JSON.stringify({ roots: roots.map((p) => hx(p.big)), a: aRoot && roleOf(aRoot, T).cache.length, b: bRoot && roleOf(bRoot, T).cache.length }));
  const aBody = roleOf(aRoot, T)?.cache[0]?.msgId, bBody = roleOf(bRoot, T)?.cache[0]?.msgId;
  // HEAL, then the farther root's self-verify (the trigger)
  await link(A[0], B[0]);
  const lr = roleOf(aRoot, T); lr.formedAt = Date.now() - ROOT_VERIFY_FIRST_MS - 1; lr.lastVerify = 0;
  aRoot.am._verifyRoots(Date.now());
  const done = await until(() => log(aRoot, 'merge-step5').length >= 1 && log(bRoot, 'merge-complete').length >= 1, 10000);
  check('P2 after the heal the farther root names the closer as its candidate, offers, and the round completes (step 5 at the loser, completion at the winner)', MERGE_ON ? (done && log(aRoot, 'merge-candidate').length >= 1 && log(aRoot, 'merge-offer').length >= 1) : (log(aRoot, 'merge-offer').length === 0), JSON.stringify({ cand: log(aRoot, 'merge-candidate').length, offer: log(aRoot, 'merge-offer').length, step5: log(aRoot, 'merge-step5').length, complete: log(bRoot, 'merge-complete').length, aborts: log(aRoot, 'merge-abort').map((l) => l.ctx.why), stale: [...log(aRoot, 'merge-stale'), ...log(bRoot, 'merge-stale')].map((l) => l.ctx.why) }));
  await until(() => roleOf(bRoot, T).cacheIds.has(aBody) && rootsFor(all, T).length === 1, 5000);
  const rootsAfter = rootsFor(all, T);
  check('P3 exactly one root remains, the XOR-closest, holding both bodies; no custody-refused anywhere', rootsAfter.length === 1 && rootsAfter[0] === B[0] && roleOf(B[0], T).cacheIds.has(aBody) && roleOf(B[0], T).cacheIds.has(bBody) && all.every((p) => log(p, 'custody-refused').length === 0), JSON.stringify({ roots: rootsAfter.map((p) => hx(p.big)), closest: hx(B[0].big), hasA: roleOf(B[0], T)?.cacheIds.has(aBody), hasB: roleOf(B[0], T)?.cacheIds.has(bBody) }));
  check('P4 the loser is a child with its upstream pinned to the winner', roleOf(aRoot, T)?.isRoot === false && (aRoot.am._upstream.get(T) || [])[0] === B[0].hex, JSON.stringify({ isRoot: roleOf(aRoot, T)?.isRoot, up: (aRoot.am._upstream.get(T) || [])[0]?.slice(0, 8) }));
}
{ // HAND-FORK — a root made by hand mints knownEpoch + 1 and WINS on epoch (Part 3: epoch before closeness).
  // The packet promises the union, not the owner: the true root demotes INTO the fork through a round, the fork
  // holds everything, and the fork's later offer to the (now non-root) true root is answered not-root, so the fork
  // keeps its claim with the candidate cleared instead of misreading silence as a legacy winner.
  const forked = A[2];
  const fr = forked.am._rootClaim.become(T, 'sub-terminal');     // epoch above W's
  const fenv = await buildEnvelope({ topic: desc, message: { side: 'fork', k: 3 }, seq: 3, identity: author, ts: Date.now() });
  const fr0 = await forked.am._ingestPublish(fr, JSON.stringify(fenv), forked.am._cutCapture(fr));
  const fBody = fr0?.ok ? fenv.msgId : null;
  const wBodies = roleOf(B[0], T).cache.map((c) => c.msgId);
  forked.am._lastAnnounce?.clear?.(); forked.am._emitRootBeacons();                     // the fork's beacon reaches the true root
  fr.formedAt = Date.now() - ROOT_VERIFY_FIRST_MS - 1; fr.lastVerify = 0; forked.am._verifyRoots(Date.now());
  const done = await until(() => log(B[0], 'merge-step5').length >= 1 && roleOf(forked, T)?.isRoot === true && wBodies.every((id) => roleOf(forked, T).cacheIds.has(id)), 12000);
  const directAtW = log(B[0], 'root-transition').filter((l) => l.ctx.isRoot === false && l.ctx.why !== 'merge-step5');
  check('H1 the fork wins on epoch: the true root demotes INTO it through a round (merge-step5, no direct demote) and the fork holds every body', done && fr.epoch > 1 && directAtW.length === 0 && !!fBody && roleOf(forked, T).cacheIds.has(fBody), JSON.stringify({ done, forkEpoch: fr.epoch, step5AtW: log(B[0], 'merge-step5').length, directAtW: directAtW.map((l) => l.ctx.why), forkHolds: roleOf(forked, T)?.cache.length }));
  // the fork now names the (closer, non-root) true root through the one candidate entry — what its verify does on the next cadence
  const offersBefore = log(forked, 'merge-offer').length;
  forked.am._mergeCandidate(T, B[0].hex, null, 'verify-closer');
  const refused = await until(() => log(forked, 'merge-refused').some((l) => l.ctx.why === 'not-root'), 8000);
  check('H2 the fork\'s offer to the non-root true root is answered not-root; the fork keeps its claim with the candidate cleared, no legacy fallback; one root remains', log(forked, 'merge-offer').length > offersBefore && refused && !roleOf(forked, T).merge?.candidate && rootsFor(all, T).length === 1 && rootsFor(all, T)[0] === forked && log(forked, 'merge-legacy-fallback').length === 0, JSON.stringify({ roots: rootsFor(all, T).map((p) => hx(p.big)), offers: log(forked, 'merge-offer').length, refused: log(forked, 'merge-refused').map((l) => l.ctx.why), cand: roleOf(forked, T)?.merge?.candidate }));
  if (process.env.DEBUG_FORK) { const pick = (p) => p.logs.filter((l) => /merge|custody|root-transition|handoff|split-union|cut-refused|undeliverable/.test(l.ev)).map((l) => l.ev.slice(7) + ':' + JSON.stringify(l.ctx).slice(0, 110)); console.log('FORK', pick(forked).join('\n  ')); console.log('W', pick(B[0]).join('\n  ')); }
}

for (const p of all) { try { await p.peer.stop(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } }
console.log(`\nfence_merge_sim_partition: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
