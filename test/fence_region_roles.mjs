#!/usr/bin/env node
// fence_region_roles — A ROLE GOES ONLY TO A NODE IN THE TOPIC'S REGION (4.138.0).
//
// David, 2026-11-13: "A topic role can only be assigned to a node that shares
// the region of that topic." Routing is not affected. Child relays and hosting
// are roles. The bridge region is not excluded outright: the rule is region
// EQUALITY. The app is told "no nodes found in region".
// Design and three council reads: axona-docs design/same-region-role-rule.md.
//
// DEFAULT OFF (option regionRoles / env REGION_ROLES). smoke_region_hint.mjs
// still fences the unarmed kernel: region gates nothing there.
//
//   A. Unarmed controls: every case below DOES take the role without the option.
//   B. The walk's end: an out-of-region terminal stops, NOT consumed, no role.
//   C. Self: birth, promotion, reachable claim, and the receiver's commit of
//      HANDOFF, ADOPT, REPLICATE, staging, host, read holder.
//   D. Lists: heir, backup cohort, child relay.
//   E. The election: region first; same-side comparisons unchanged.
//   F. A foreign root: not deferred to, not yielded to, its beacon kept.
//   G. The bridge region by equality (David's answer 4).
//   H. The app's error: REGION_UNPOPULATED, and when it must NOT fire.
//   I. Statics: the self guard on the write retry, the verify nominee, and
//      that routing and the "am I alone?" guard compare no regions.
//
// Mutants (run by hand on a scratch copy; the section named fails):
//   drop the SUB stop → B1; return 'consumed' from the stop → B2;
//   drop the gate in become() → C1; drop the promote gate → C2;
//   drop the HANDOFF refusal → C4; drop the ADOPT refusal → C5;
//   drop either REPLICATE refusal → C6; drop the staging gate → C7;
//   drop the host backstop → C8; drop the heir filter → D1;
//   drop the cohort filter → D3; drop the child filter → D5;
//   put region after epoch in claimWins → E1; drop the defer gate → F1;
//   drop the yield gate → F2; refuse when the lookup fails → H5.
import { readFileSync } from 'node:fs';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { RootClaim, makeRole } from '../src/pubsub/rootClaim.js';
import { sameRegion, regionOf } from '../src/pubsub/regionRoles.js';
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { PublishError, PullError, ErrorCodes } from '../src/errors.js';
import { T } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m}${c ? '' : ' ' + extra}`); c ? n++ : fail++; };
const idHex = (b) => b.toString(16).padStart(66, '0');
const mk = (region, tag) => (BigInt(region) << 256n) | BigInt(tag);   // region = the top 8 of 264 bits

const SELF    = mk(0x89, 0x11);
const HOME    = mk(0x89, 0xabc);            // a topic in this node's region
const FOREIGN = mk(0x12, 0xabc);            // a topic in another region
const PEER_IN  = mk(0x89, 0x77), PEER_IN2 = mk(0x89, 0x78);
const PEER_OUT = mk(0x12, 0xBB), PEER_OUT2 = mk(0x12, 0xCC);

function newAM({ armed = true, self = SELF, neighbors = [] } = {}) {
  const sent = [];
  const dht = {
    getSelfId: () => self,
    onRoutedMessage: () => {},
    verdictsSupported: false,
    routeMessage: (target, type, payload) => { sent.push({ target, type, payload }); },
    neighbors: () => neighbors, bridgeId: () => null, findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => Date.now(), regionRoles: armed, roleGraceMs: 0 });
  am.nodeId = self;
  const logs = [];
  am.setLogSink((level, event, data) => logs.push({ level, event, data }));
  return { am, sent, logs };
}
const term = { isTerminal: true };
const bare = (t) => ({ topicId: idHex(t), via: [] });
const subOf = (t) => ({ ...bare(t), subscriberId: idHex(PEER_IN), since: 0 });

console.log('fence_region_roles: a role goes only to a node in the topic\'s region; routing compares no regions');

// ── 0. the predicate ───────────────────────────────────────────────────
ok('0a the region is the top 8 of 264 bits: two ids differing in the top HASH byte share a region', sameRegion(mk(0x89, 0xAAn << 248n), mk(0x89, 0x11n << 248n)) && regionOf(mk(0x89, 1)) === 0x89);
ok('0b an unreadable id shares no region', sameRegion(HOME, 'zz') === false && sameRegion(HOME, undefined) === false);
{ const { am } = newAM({ armed: false });
  ok('0c the option defaults OFF and is read from the constructor', am._regionRoles === false && newAM().am._regionRoles === true);
  ok('0d unarmed, every id is eligible and nothing is filtered', am._roleRegionOk(FOREIGN) === true && am._regionIneligible(FOREIGN) === false && am._regionFilter(HOME, [PEER_OUT]).length === 1); }

// ── A. unarmed controls ────────────────────────────────────────────────
{ const { am } = newAM({ armed: false });
  const r = await am._onSub(subOf(FOREIGN), term);
  ok('A1 unarmed: a foreign SUB at the walk\'s end births the root and is consumed', r === 'consumed' && am.axonRoles.get(FOREIGN)?.isRoot === true);
  ok('A2 unarmed: host, heir and child accept a foreign topic or node', (am.pubsubHost(FOREIGN), am._hostedTopics.has(FOREIGN)) && am._pickHeirs(HOME, [PEER_OUT]).heir === PEER_OUT && am.admitPushedRole(FOREIGN) === true); }

// ── B. the walk's end ──────────────────────────────────────────────────
{ const { am, logs } = newAM();
  const r = await am._onSub(subOf(FOREIGN), term);
  ok('B1 armed: a foreign SUB at the walk\'s end births NO root', !am.axonRoles.has(FOREIGN));
  ok('B2 and the handler returns NOT consumed (undefined), so the origin reads not delivered', r === undefined, String(r));
  ok('B3 PUB, KILL and METRICSON stop the same way', (await am._onPub(bare(FOREIGN), term)) === undefined && (await am._onKill(bare(FOREIGN), term)) === undefined && am._onMetricsOn(bare(FOREIGN), term) === undefined && !am.axonRoles.has(FOREIGN));
  const st = am.inspectRegionRoles();
  ok('B4 each stop is counted by type; the log line is rate-limited per topic', st.stops === 4 && st.byType[T.SUB] === 1 && st.byType[T.PUB] === 1 && logs.filter((l) => l.event === 'pubsub:region-no-terminal').length === 1, JSON.stringify(st));
  const r2 = await am._onSub(subOf(HOME), term);
  ok('B5 an in-region SUB still births the root and is consumed', r2 === 'consumed' && am.axonRoles.get(HOME)?.isRoot === true);
  ok('B6 a message that is NOT at its end is still forwarded: the stop is only at the walk\'s end', am._topicDecision(bare(FOREIGN), { isTerminal: false }) === 'forward'); }

// ── C. self ────────────────────────────────────────────────────────────
{ const { am } = newAM();
  ok('C1 become(): no root is born outside the region; inside, one is', am._rootClaim.become(FOREIGN, 'reachable-fallback') === null && !am.axonRoles.has(FOREIGN) && am._rootClaim.become(HOME, 'reachable-fallback')?.isRoot === true);
  const rf = makeRole(FOREIGN, false, Date.now()); am.axonRoles.set(FOREIGN, rf);
  am._rootClaim.promote(rf, { via: [] }, term);
  const rh = makeRole(mk(0x89, 0xdef), false, Date.now()); am.axonRoles.set(rh.topicId, rh);
  am._rootClaim.promote(rh, { via: [] }, term);
  ok('C2 promote(): a held foreign role is not promoted; an in-region one is', rf.isRoot === false && rh.isRoot === true);
  am.axonRoles.delete(FOREIGN);
  ok('C3 the reachable-root claim returns null for a foreign topic', am._rootClaim.claimReachable(FOREIGN) === null && !am.axonRoles.has(FOREIGN)); }
{ const { am } = newAM();
  ok('C4 a HANDOFF pushed at a foreign node is refused; in region it is admitted', am.admitPushedRole(FOREIGN) === false && am.admitPushedRole(HOME) === true); }
{ const { am, sent } = newAM();
  const r = am._onAdopt({ topicId: idHex(FOREIGN), parent: idHex(PEER_OUT), subs: [], nonce: 'n1' }, { targetId: SELF });
  const ack = sent.find((s) => s.type === T.ADOPTACK);
  ok('C5 ADOPT: a foreign node is no child relay, and the parent is told why', r === 'consumed' && !am.axonRoles.has(FOREIGN) && ack?.payload?.refused === 'region' && Array.isArray(ack.payload.accepted) && ack.payload.accepted.length === 0, JSON.stringify(ack?.payload)); }
{ const { am } = newAM();
  const r = await am._onReplicate({ topicId: idHex(FOREIGN), from: idHex(PEER_OUT), msgs: [], dels: [] }, { targetId: SELF, fromId: idHex(PEER_OUT) });
  await am._syncIngest({ topicId: idHex(FOREIGN), from: idHex(PEER_OUT), msgs: [], dels: [] }, { fromId: idHex(PEER_OUT) }, 'REPLICATE');
  ok('C6 REPLICATE: no backup outside the region, at the door and at the ingest; not consumed', r === undefined && !am.axonRoles.has(FOREIGN) && am.inspectRegionRoles().refusals.backup === 2, JSON.stringify(am.inspectRegionRoles())); }
{ const { am, logs } = newAM();
  for (let i = 0; i < 50; i++) await am._onReplicate({ topicId: idHex(FOREIGN), from: idHex(PEER_OUT), msgs: [], dels: [] }, { targetId: SELF, fromId: idHex(PEER_OUT) });
  await am._onReplicate({ topicId: idHex(mk(0x12, 0xabd)), from: idHex(PEER_OUT), msgs: [], dels: [] }, { targetId: SELF, fromId: idHex(PEER_OUT) });
  const lines = logs.filter((l) => l.event === 'pubsub:role-refused' && l.data?.why === 'region');
  ok('C6b fifty-one refusals are all counted and log ONE line per topic (the 2,922-line flood of 2026-11-13)', am.inspectRegionRoles().refusals.backup === 51 && lines.length === 2, `count ${am.inspectRegionRoles().refusals.backup} lines ${lines.length}`); }
{ const { am } = newAM({ self: mk(0x13, 0x1) });          // closer to FOREIGN (0x12) than the loser below, and outside its region
  const loser = idHex(mk(0x89, 0x5));
  const staged = am._stageOffer(FOREIGN, undefined, loser, { topicId: idHex(FOREIGN), corr: 'c'.repeat(16), handoff: 1, count: 3, loserEpoch: 1, timing: {} });
  ok('C7 a staged handover is not taken by a closer node outside the region', staged === false && !am.axonRoles.has(FOREIGN)); }
{ const { am } = newAM();
  ok('C8 host: the manager backstop holds nothing for a foreign topic', am.pubsubHost(FOREIGN) === false && !am._hostedTopics.has(FOREIGN) && (am.pubsubHost(HOME), am._hostedTopics.has(HOME)));
  am.mySubscriptions.set(FOREIGN, { createdAt: Date.now(), lastRenewSent: null });
  await am._readRepair(FOREIGN);
  ok('C9 a read holder is not created outside the region', !am.axonRoles.has(FOREIGN)); }

// ── D. lists ───────────────────────────────────────────────────────────
{ const { am } = newAM({ neighbors: [PEER_OUT, PEER_IN, PEER_OUT2, PEER_IN2] });
  const h = am._pickHeirs(HOME, [PEER_OUT, PEER_IN, PEER_OUT2, PEER_IN2]);
  ok('D1 the heir and its runner-up are in the region, in the order supplied', h.heir === PEER_IN && h.alt === PEER_IN2);
  const none = am._pickHeirs(HOME, [PEER_OUT, PEER_OUT2]);
  ok('D2 with no in-region candidate there is NO heir (the history has nowhere to go: accepted)', none.heir === null && none.alt === null);
  const near = am._nearestReachable(HOME, 4, null);
  ok('D3 the neighbour-based backup pick names in-region nodes only', near.length === 2 && near.every((h2) => sameRegion(HOME, h2)), JSON.stringify(near));
  const role = makeRole(HOME, true, Date.now());
  const inh = am._inheritCohort(HOME, role, [idHex(PEER_OUT), idHex(PEER_IN), idHex(PEER_OUT2)], null);
  ok('D4 the inherit pick names in-region nodes only', inh.length === 1 && inh[0] === idHex(PEER_IN), JSON.stringify(inh)); }
{ const { am } = newAM();
  await am._onSub(subOf(HOME), term);
  const role = am.axonRoles.get(HOME);
  role.subscribers.clear();
  role.subscribers.set(idHex(PEER_OUT), { since: 0, lastRenewed: Date.now() });
  role.subscribers.set(idHex(PEER_OUT2), { since: 0, lastRenewed: Date.now() });
  ok('D5 two foreign leaves: no child relay is promoted', am._promoteChild(role) === false && role.children.size === 0);
  role.subscribers.set(idHex(PEER_IN), { since: 0, lastRenewed: Date.now() });
  role.subscribers.set(idHex(PEER_IN2), { since: 0, lastRenewed: Date.now() });
  ok('D6 with two in-region leaves the child is one of THEM', am._promoteChild(role) === true && [...role.children].every((c) => sameRegion(HOME, c))); }

// ── E. the election ────────────────────────────────────────────────────
{ const inR = { id: idHex(PEER_IN), epoch: 1 }, outR = { id: idHex(PEER_OUT), epoch: 9 };
  ok('E1 region first: an in-region claim at epoch 1 wins over a foreign claim at epoch 9', RootClaim.claimWins(HOME, inR, outR, true) === true && RootClaim.claimWins(HOME, outR, inR, true) === false);
  ok('E2 without the option the higher epoch wins, as before', RootClaim.claimWins(HOME, outR, inR) === true && RootClaim.claimWins(HOME, inR, outR, false) === false);
  const a = { id: idHex(PEER_IN), epoch: 2 }, b = { id: idHex(PEER_IN2), epoch: 5 };
  ok('E3 two in-region claims: epoch, then distance, unchanged by the option', RootClaim.claimWins(HOME, b, a, true) === RootClaim.claimWins(HOME, b, a, false) && RootClaim.claimWins(HOME, b, a, true) === true);
  const c = { id: idHex(PEER_OUT), epoch: 2 }, d = { id: idHex(PEER_OUT2), epoch: 5 };
  ok('E4 two foreign claims: unchanged by the option', RootClaim.claimWins(HOME, d, c, true) === RootClaim.claimWins(HOME, d, c, false));
  ok('E5 an epoch-0 claim: the option agrees with distance, which already ranks in-region first', RootClaim.claimWins(HOME, { id: idHex(PEER_IN), epoch: 0 }, outR, true) === RootClaim.claimWins(HOME, { id: idHex(PEER_IN), epoch: 0 }, outR, false));
  const ids = [PEER_IN, PEER_IN2, PEER_OUT, PEER_OUT2, mk(0x13, 0x9)].map(idHex); let cyc = 0, tri = 0;
  const claims = []; for (const id of ids) for (const e of [1, 2, 3]) claims.push({ id, epoch: e });
  for (const x of claims) for (const y of claims) for (const z of claims) {
    if (x.id === y.id || y.id === z.id || x.id === z.id) continue; tri++;
    if (RootClaim.claimWins(HOME, x, y, true) && RootClaim.claimWins(HOME, y, z, true) && RootClaim.claimWins(HOME, z, x, true)) cyc++;
  }
  ok(`E6 among versioned claims, region first has no cycle (${tri} ordered triples)`, cyc === 0 && tri > 1000, `cycles ${cyc}`); }

// ── F. a foreign root ──────────────────────────────────────────────────
{ const selfX = mk(0x89, 0x11), topicX = mk(0x12, 0xabc), rootX = mk(0x13, 0x5);   // root outside the topic's region, and closer than self
  const mkB = (armed) => { const { am } = newAM({ armed, self: selfX }); am._rootBeacons.set(topicX, { root: idHex(rootX), at: am._now(), exp: am._now() + 60_000, epoch: 3 }); return am; };
  ok('F1 a closer root OUTSIDE the topic\'s region is not deferred to; unarmed it is', mkB(true)._rootClaim.liveCloserRoot(topicX, { requireReachable: false }) === null && mkB(false)._rootClaim.liveCloserRoot(topicX, { requireReachable: false }) === idHex(rootX)); }
{ const run = async (armed) => {
    const { am } = newAM({ armed });
    await am._onSub(subOf(HOME), term);
    const role = am.axonRoles.get(HOME);
    am._onRootBeacon({ root: idHex(PEER_OUT), topics: [idHex(HOME)], epochs: [role.epoch + 7], beaconId: 'b-' + armed, layer: 1 }, { fromId: idHex(PEER_OUT) });
    return { isRoot: am.axonRoles.get(HOME)?.isRoot, rec: am._rootBeacons.get(HOME), want: role.epoch + 7 };
  };
  const on = await run(true), off = await run(false);
  ok('F2 a foreign root at a HIGHER epoch is not yielded to; unarmed the root yields', on.isRoot === true && off.isRoot === false, JSON.stringify({ on: on.isRoot, off: off.isRoot }));
  ok('F3 and its beacon is still recorded: the epoch is seat history either way', on.rec?.root === idHex(PEER_OUT) && on.rec?.epoch === on.want, JSON.stringify(on.rec)); }

// ── G. the bridge region, by equality ──────────────────────────────────
{ const BRIDGE = mk(0xff, 0x1), BTOPIC = mk(0xff, 0xd1);
  const { am } = newAM({ self: BRIDGE });
  ok('G1 a node in region 0xFF takes no role for an ordinary topic', am._regionIneligible(HOME) === true && am._rootClaim.become(HOME, 'reachable-fallback') === null);
  ok('G2 and MAY hold a topic minted in region 0xFF: the rule is equality, not an exclusion', am._regionIneligible(BTOPIC) === false && am._rootClaim.become(BTOPIC, 'reachable-fallback')?.isRoot === true);
  const { am: am2 } = newAM();
  ok('G3 an ordinary node takes no role for a topic in region 0xFF', am2._regionIneligible(BTOPIC) === true && am2._pickHeirs(HOME, [BRIDGE, PEER_IN]).heir === PEER_IN); }

// ── H. the app's error ─────────────────────────────────────────────────
{ const guard = AxonaPeer.prototype._assertRegionPopulated;
  // `walk`: what the routed lookup visits. Default: one foreign node, so the walk agrees with a local "none".
  const peer = (found, opts = {}) => ({ _node: { id: SELF }, calls: 0, walks: 0, _regionWalkMs: opts.walkMs,
    async findKClosest() { this.calls++; if (opts.throws) throw new Error('lookup failed'); return found; },
    lookup(_t, o) { this.walks++; this.walkOpts = o; if (opts.walkThrows) return Promise.reject(new Error('walk failed')); if (opts.walkHangs) return new Promise(() => {}); return Promise.resolve('walk' in opts ? opts.walk : { found: true, path: [idHex(mk(0x13, 0x2))] }); } });
  const armedAm = { _regionRoles: true }, off = { _regionRoles: false };
  const tryIt = async (p, am, topic) => { try { await guard.call(p, am, topic, PublishError, 'pub'); return null; } catch (e) { return e; } };
  { const p = peer([PEER_OUT]); ok('H1 unarmed: no lookup, no error', (await tryIt(p, off, FOREIGN)) === null && p.calls === 0); }
  { const p = peer([]); ok('H2 a node IN the topic\'s region never asks and never refuses', (await tryIt(p, armedAm, HOME)) === null && p.calls === 0); }
  { const p = peer([SELF, mk(0x13, 0x1), mk(0xff, 0x1)]); const e = await tryIt(p, armedAm, FOREIGN);
    ok('H3 the lookup names other nodes and none in the region → REGION_UNPOPULATED, "no nodes found in region"', e instanceof PublishError && e.code === ErrorCodes.REGION_UNPOPULATED && /no nodes found in region 0x12/.test(e.message), String(e?.code ?? e));
    const e2 = await tryIt(p, armedAm, FOREIGN);
    ok('H3b the answer is cached: the second call asks nobody', e2?.code === ErrorCodes.REGION_UNPOPULATED && p.calls === 1, `calls ${p.calls}`); }
  { const p = peer([mk(0x13, 0x1), PEER_OUT]); ok('H4 one node in the region is enough', (await tryIt(p, armedAm, FOREIGN)) === null); }
  { const p = peer(null, { throws: true }); ok('H5 a lookup that FAILS is "cannot say": no error', (await tryIt(p, armedAm, FOREIGN)) === null); }
  { const p = peer([SELF]); ok('H6 a lookup that names nobody else is "cannot say": no error', (await tryIt(p, armedAm, FOREIGN)) === null); }
  { const p = peer([123, 'x', null]); ok('H7 hostile lookup entries are not nodes and refuse nothing', (await tryIt(p, armedAm, FOREIGN)) === null); }
  // RR-LOOKUP-RADIUS (4.141.0; Orion e3b03ec5): the local lookup's reach ends two hops out.
  { const p = peer([mk(0x13, 0x1)], { walk: { found: true, path: [idHex(mk(0x13, 0x2)), idHex(mk(0xff, 0x1)), idHex(PEER_OUT)] } });
    const e = await tryIt(p, armedAm, FOREIGN);
    ok('H8 the local lookup finds none, the ROUTED walk visits a node in the region → no error', e === null && p.walks === 1 && p.walkOpts?.noCache === true, String(e?.code ?? e));
    const before = p.calls; await tryIt(p, armedAm, FOREIGN);
    ok('H8b and that answer is cached as found', p.calls === before && p.walks === 1); }
  { const p = peer([mk(0x13, 0x1)]); const e = await tryIt(p, armedAm, FOREIGN);
    ok('H9 the walk also visits no node in the region → REGION_UNPOPULATED', e?.code === ErrorCodes.REGION_UNPOPULATED && p.walks === 1); }
  { const p = peer([mk(0x13, 0x1)], { walkThrows: true }); ok('H10 a walk that FAILS is "cannot say": no error, nothing cached', (await tryIt(p, armedAm, FOREIGN)) === null && (await tryIt(p, armedAm, FOREIGN)) === null && p.walks === 2); }
  { const p = peer([mk(0x13, 0x1)], { walkHangs: true, walkMs: 25 }); const t0 = Date.now(); const keep = setInterval(() => {}, 20);   // the kernel's timer is unref'd; hold the process open for the test
    const e = await tryIt(p, armedAm, FOREIGN); clearInterval(keep);
    ok('H11 a walk that does not answer in time is "cannot say": no error, and the call returns', e === null && Date.now() - t0 < 1000, `${Date.now() - t0} ms`); }
  { const p = peer([mk(0x13, 0x1)], { walk: { found: false, path: [idHex(SELF)] } }); ok('H12 a walk that visited nobody else is "cannot say"', (await tryIt(p, armedAm, FOREIGN)) === null); }
  { const p = peer([mk(0x13, 0x1)], { walk: null }); ok('H13 a walk with no result is "cannot say"', (await tryIt(p, armedAm, FOREIGN)) === null); }
  { const p = peer([mk(0x13, 0x1), PEER_OUT]); await tryIt(p, armedAm, FOREIGN); ok('H14 when the local lookup already found a node, the walk is not run', p.walks === 0); }
  // A READ raises the same error (4.142.0; David 2026-11-13: "a read in a region with no nodes raises the same error").
  { const p = peer([mk(0x13, 0x1)]); let e = null; try { await guard.call(p, armedAm, FOREIGN, PullError, 'pull'); } catch (x) { e = x; }
    ok('H15 the guard raises the caller\'s own error class: a PullError with REGION_UNPOPULATED, naming peer.pull', e instanceof PullError && e.code === ErrorCodes.REGION_UNPOPULATED && /^peer\.pull: no nodes found in region 0x12/.test(e.message), String(e?.message ?? e)); }
  { const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const body = (name, next) => src.slice(src.indexOf(`  async ${name}(msgId, { topic, timeoutMs = 1000 } = {}) {`), src.indexOf(next));
    const pull = body('pull', '  async pullOutcome('), po = body('pullOutcome', '   * Resolve an author\'s self-declared class');
    const before = (b, a, c) => b.indexOf(a) > 0 && b.indexOf(c) > b.indexOf(a);
    ok('H16 peer.pull runs the guard BEFORE it asks the network', pull.length > 200 && before(pull, "await this._assertRegionPopulated(am, topicIdBig, PullError, 'pull');", 'am.requestPull('));
    ok('H17 peer.pullOutcome runs the same guard before it asks: two doors onto one read agree', po.length > 200 && before(po, "await this._assertRegionPopulated(am, desc.topicIdBig, PullError, 'pullOutcome');", 'am.requestPull(')); } }

// ── I. statics ─────────────────────────────────────────────────────────
{ const rd = (p) => readFileSync(new URL(p, import.meta.url), 'utf8');
  const wf = rd('../src/pubsub/writeFlight.js');
  ok('I1 the write retry tests SELF as well as the neighbours (Aster cd1da075)', /if \(this\._regionRoles && !this\._roleRegionOk\(tBig, nb\)\) continue;/.test(wf) && /if \(!this\._regionIneligible\(tBig\) && \(bestD === null \|\| selfD < bestD\)\)/.test(wf));
  const re = rd('../src/pubsub/rootElection.js');
  ok('I2 the verify nominee is in the topic\'s region', /if \(this\._regionRoles && !this\._roleRegionOk\(t, b\)\) continue;/.test(re));
  const rp = rd('../src/pubsub/repairPlane.js');
  ok('I3 the discovered backup cohort and the retry heir are filtered', /if \(this\._regionRoles && !this\._roleRegionOk\(t, b\)\) continue;/.test(rp) && /if \(this\._regionRoles && !this\._roleRegionOk\(j\.t, b\)\) continue;/.test(rp));
  const rc = rd('../src/pubsub/rootClaim.js');
  const alone = rc.slice(rc.indexOf('  meshBare() {'), rc.indexOf('  selfClosestReachable('));
  ok('I4 the "am I alone?" guard compares no regions', alone.length > 100 && !/region/i.test(alone.replace(/non-bridge/g, '')));
  const ap = rd('../src/dht/AxonaPeer.js');
  const greedy = ap.slice(ap.indexOf('  _greedyNextHopToward(targetId) {'), ap.indexOf('  async _findCloserInTwoHops('));
  const fwd = ap.slice(ap.indexOf('  _capableFallbackHop(targetId, exclude) {'), ap.indexOf('  _greedyNextHopToward(targetId) {'));
  ok('I5 routing compares no regions: greedy, the lookahead entry and the fallback hop', greedy.length > 200 && fwd.length > 200 && !/extractS2Prefix|_regionRoles|sameRegion/.test(greedy + fwd));
  const hp = rd('../src/pubsub/holderProbe.js');
  ok('I6 reads are not filtered: the holder probe compares no regions', !/_regionRoles|sameRegion|_roleRegionOk/.test(hp)); }

console.log(`\n${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
