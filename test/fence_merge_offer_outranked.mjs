#!/usr/bin/env node
// =====================================================================
// fence_merge_offer_outranked.mjs — a root answers an offer from a claim that
// OUTRANKS its own (David 2026-10-23, "send council the merge fix").
//
// Before: _onMergeOffer logged merge-offer-loses and sent nothing back. The
// offerer's sweep reads an unanswered, reached offer as a LEGACY winner and
// demotes the claim that outranks W toward W (merge-legacy-fallback): 8
// fallbacks against 994 merge-step5 rounds on the 4.122.0 fleet
// (ops/signal-trace-20261023), and the hand-fork scene of
// fence_merge_sim_partition whenever the fork names the true root before the
// true root offers. After: W answers REPLAYACK disposition 'outranked'; the
// offerer closes the offer, keeps its claim, drops the candidate, and W's own
// offer runs the round the relation calls for.
//
// Three real AxonaPeers over SimNetwork, every pair linked, so every route is
// one hop and the ORDER is forced by hand, not by timing:
//   O1 F (hand-made root, epoch above W's) offers to W first; W logs
//      merge-offer-loses and F logs merge-refused why 'outranked'.
//   O2 F keeps its claim with the candidate cleared, and an offer-timeout sweep
//      run past MERGE_OFFER_TIMEOUT_MS makes NO merge-legacy-fallback.
//   O3 W's own offer completes the round: W demotes through merge-step5 and F
//      holds W's body and its own.
//
// Mutants: drop W's answer → O1, O2; drop 'outranked' from the offerer's
// refusal set → O1, O2 (merge-stale, then the fallback).
// Run: node test/fence_merge_offer_outranked.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { fromHex, clz264 }          from '../src/utils/hexid.js';
import { deriveTopicIdBig }         from '../src/pubsub/post.js';
import { MERGE_OFFER_TIMEOUT_MS }   from '../src/pubsub/constants.js';
import { buildEnvelope }            from '../src/pubsub/envelope.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const until = async (fn, ms = 8000, step = 50) => { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (fn()) return true; await wait(step); } return fn(); };

const net = new SimNetwork();
const domain = new AxonaDomain();
async function makePeer(lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, mergeCapable: true });
  await peer.start();
  if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
  const am = peer._requireAxonaManager('fence');
  const logs = []; const orig = am._log.bind(am);
  am._log = (lvl, ev, ctx) => { logs.push({ ev: ev.startsWith('pubsub:') ? ev.slice(7) : ev, ctx }); return orig(lvl, ev, ctx); };
  return { hex: id.id, big: fromHex(id.id), node, peer, transport, am, logs };
}
const link = async (a, b) => {
  a.node.synaptome.set(b.big, new Synapse({ peerId: b.big, latencyMs: 5, stratum: clz264(a.big ^ b.big) }));
  b.node.synaptome.set(a.big, new Synapse({ peerId: a.big, latencyMs: 5, stratum: clz264(b.big ^ a.big) }));
  await a.transport.openConnection(b.hex);
  await wait(10);
};
const log = (p, ev) => p.logs.filter((l) => l.ev === ev);
const roleOf = (p, t) => p.am.axonRoles.get(t);

process.on('exit', (code) => { if (code === 0 && passed + failed < 3) { console.log(`  ✗ exited after ${passed + failed} of 3 checks`); process.exitCode = 1; } });

console.log('fence_merge_offer_outranked: a root answers an offer from a claim that outranks its own');
const author = await createAuthorIdentity();
const desc = { region: 'useast', owner: null, name: 'offer-outranked-' + Date.now().toString(36), write: 'open' };
const T = await deriveTopicIdBig(desc);
const raw = []; for (let i = 0; i < 3; i++) raw.push(await makePeer(40 + i * 0.01, -70 + i * 0.01));
raw.sort((p, q) => ((p.big ^ T) < (q.big ^ T) ? -1 : 1));
const [W, M, F] = raw;                                              // W the XOR-closest, F the farthest
await link(W, M); await link(M, F); await link(W, F);

// W is the true root, with a body
await W.peer.pub(desc, { who: 'W', k: 1 }, { signWith: author }); await wait(300);
const wRole = roleOf(W, T);
const wBody = wRole?.cache[0]?.msgId;
// F is made root by hand, one epoch above W, with its own body
// F's announcement is held so W cannot learn F's claim before F's offer: the
// order this fence needs is the one the fleet hits when the announcement loses
// the race, forced here instead of timed.
const announce = F.am._announceRoot.bind(F.am); F.am._announceRoot = () => {};
const fr = F.am._rootClaim.become(T, 'sub-terminal');
fr.epoch = (wRole?.epoch | 0) + 1;
const fenv = await buildEnvelope({ topic: desc, message: { who: 'F', k: 2 }, seq: 2, identity: author, ts: Date.now() });
const fr0 = await F.am._ingestPublish(fr, JSON.stringify(fenv), F.am._cutCapture(fr));
const fBody = fr0?.ok ? fenv.msgId : null;

// F names W first (what its verify does when the lookup lands before W's offer)
F.am._mergeCandidate(T, W.hex, null, 'verify-closer');
F.am._announceRoot = announce;
const answered = await until(() => log(F, 'merge-refused').some((l) => l.ctx.why === 'outranked'), 4000);
check('O1 F offers to W first; W logs merge-offer-loses and F logs merge-refused why outranked',
  log(W, 'merge-offer-loses').length >= 1 && answered,
  JSON.stringify({ loses: log(W, 'merge-offer-loses').length, refused: log(F, 'merge-refused').map((l) => l.ctx.why), stale: log(F, 'merge-stale').map((l) => l.ctx.why) }));

const candAfter = roleOf(F, T)?.merge?.candidate ?? null;
F.am._mergeSweep(Date.now() + MERGE_OFFER_TIMEOUT_MS + 1);
check('O2 F keeps its claim, its candidate cleared, and a sweep past the offer timeout makes no legacy fallback',
  !!wBody && !!fBody && candAfter === null && log(F, 'merge-legacy-fallback').length === 0 && log(F, 'root-transition').every((l) => l.ctx.why !== 'merge-legacy-fallback'),
  JSON.stringify({ wBody: !!wBody, fBody: !!fBody, cand: candAfter?.id?.slice(0, 10) ?? null, fallback: log(F, 'merge-legacy-fallback').length }));

const done = await until(() => log(W, 'merge-step5').length >= 1 && roleOf(F, T)?.isRoot === true && roleOf(F, T).cacheIds.has(wBody) && roleOf(F, T).cacheIds.has(fBody), 8000);
check('O3 W\'s own offer runs the round: W demotes through merge-step5, F is root holding both bodies',
  done && roleOf(W, T)?.isRoot === false,
  JSON.stringify({ step5: log(W, 'merge-step5').length, fRoot: roleOf(F, T)?.isRoot, wRoot: roleOf(W, T)?.isRoot, fHolds: roleOf(F, T)?.cache.length }));

for (const p of raw) { try { await p.peer.stop(); } catch { /* */ } try { await p.transport.stop(); } catch { /* */ } }
console.log(`\nfence_merge_offer_outranked: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
