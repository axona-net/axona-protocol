#!/usr/bin/env node
// =====================================================================
// fence_cbr_3a_holder_probe.mjs — Closer-Born Relay Inherits the Root v0.6
// Part 3a (David 2026-11-03 "go ahead with 3a"), with the Part 7 probe
// budget it needs; manager option holderProbe / env HOLDER_PROBE, default off.
//
// Scene (merge Fabric): R the empty root, closest to T; five role-less nodes
// next closest (findKClosest's whole reach, fanout+1 = 5); the holder S
// farther out, holding three bodies, reachable by name. S's SUB to R
// advertised its history (hw > 0) but R's immediate REPLAY_UP pull is lost.
//   E1 on: S is ranked first by evidence; one try fills R from S.
//   E2 off (control): the probe asks only the five role-less nodes; R stays
//      empty after its three counted tries.
//   N1 on, no evidence: the role-less nodes NACK no-role, the try is
//      refunded (tries 0), they are never asked again.
//   N2 on: past HOLDER_PROBE_WINDOW_MS with nobody left to ask, the probe
//      stops and logs degraded no-holder-found once; no further pulls.
//   N3 on: the frame budget spent (HOLDER_PROBE_FRAMES): no pull is sent.
//   N4 a plain PULLUP (no probe) at an armed role-less node draws no NACK;
//      a probe at a node WITH a role draws none either.
//   N5 a holder found later clears the degraded state (holder-found row).
// Run: node test/fence_cbr_3a_holder_probe.mjs
// =====================================================================
import { Fabric, freshId, realTopic, author, idHex } from './lib/mergeFixture.mjs';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { HOLDER_PROBE_WINDOW_MS, HOLDER_PROBE_FRAMES, EMPTY_ROOT_PROBE_MAX } from '../src/pubsub/constants.js';
import { makeRole } from '../src/pubsub/rootClaim.js';

const EXPECT = 8;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

async function scene(name, { armed = true, evidence = true } = {}) {
  const Lt = await realTopic(name);
  const fab = new Fabric();
  const ids = []; for (let i = 0; i < 8; i++) ids.push(await freshId());
  ids.sort((a, b) => ((a ^ Lt.topicId) < (b ^ Lt.topicId) ? -1 : 1));
  const opts = armed ? { holderProbe: true } : {};
  const R = fab.addNode(ids[0], opts);
  const roleless = ids.slice(1, 6).map((id) => fab.addNode(id, opts));
  const S = fab.addNode(ids[7], opts);
  for (const n of [R, ...roleless, S]) n.am._announceRoot = () => {};
  // S holds three bodies under a role of its own (a reader that kept the old root's history).
  const sRole = makeRole(Lt.topicId, false, fab.clock); S.am.axonRoles.set(Lt.topicId, sRole);
  for (let k = 1; k <= 3; k++) {
    fab.clock += 10;
    const env = await buildEnvelope({ topic: Lt.desc, message: { k }, seq: k, identity: author, ts: fab.clock });
    sRole.cache.push({ msgId: env.msgId, publishTs: fab.clock, json: JSON.stringify(env), bytes: 1 }); sRole.cacheIds.add(env.msgId); sRole.lastTs = fab.clock;
  }
  const rRole = R.rc.become(Lt.topicId, 'sub-terminal');
  // S's SUB reaches R advertising its history; R's immediate REPLAY_UP pull is lost.
  await fab.settle();   // installs the Fabric tap before anything is sent
  if (evidence) {
    fab.tap = (j) => (j.type === 'pubsub:pullup' && !j.payload?.probe ? null : j);
    await R.handlers.get('pubsub:sub')({ topicId: idHex(Lt.topicId), via: [], subscriberId: idHex(S.id), since: 0, hw: sRole.lastTs, lw: sRole.cache[0].publishTs }, { targetId: R.id, isTerminal: true, hopCount: 2, fromId: idHex(S.id) });
    await fab.settle();
    fab.tap = null;
  }
  const setupSent = (fab.sent ?? []).length;
  const probe = async () => { await R.am._emptyRootProbe(Lt.topicId); await fab.settle(); for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r)); await fab.settle(); };
  const pulls = () => (fab.sent ?? []).slice(setupSent).filter((j) => j.type === 'pubsub:pullup');
  const nacks = () => (fab.sent ?? []).filter((j) => j.type === 'pubsub:replayack' && j.payload?.disposition === 'no-role');
  const ev = (n) => n.logs.map((l) => l.ev.replace('pubsub:', ''));
  return { Lt, fab, R, S, roleless, rRole, probe, pulls, nacks, ev };
}

console.log('fence_cbr_3a_holder_probe: an empty root asks holders first; role-less nodes NACK; a budget bounds it');

{ const X = await scene('cbr-3a-evidence');
  await X.probe();
  const first = X.pulls().find((j) => j.payload?.probe);
  check('E1 on: S ranked first by evidence; one try fills R with S\'s three bodies',
    X.rRole.cache.length === 3 && first?.dest === X.S.id,
    JSON.stringify({ cache: X.rRole.cache.length, firstDest: String(first?.dest ?? null).slice(0, 12), S: String(X.S.id).slice(0, 12) })); }
{ const X = await scene('cbr-3a-off', { armed: false });
  for (let i = 0; i < EMPTY_ROOT_PROBE_MAX + 1; i++) { X.fab.clock += 6000; await X.probe(); }
  const dests = new Set(X.pulls().map((j) => j.dest));
  check('E2 off (control): the probe asks only role-less nodes and R stays empty after its counted tries',
    X.rRole.cache.length === 0 && !dests.has(X.S.id) && (X.rRole.sync.probeTries | 0) === EMPTY_ROOT_PROBE_MAX, JSON.stringify({ cache: X.rRole.cache.length, tries: X.rRole.sync.probeTries, askedS: dests.has(X.S.id) })); }
{ const X = await scene('cbr-3a-noevidence', { evidence: false });
  await X.probe();
  const after1 = X.pulls().length;
  X.fab.clock += 6000; await X.probe();
  const st = X.R.am.probeStatus(X.Lt.topicId);
  check('N1 on, no evidence: role-less nodes NACK no-role, the try is refunded, they are not asked again',
    X.nacks().length >= 4 && st.tries === 0 && st.roleless >= 4 && X.pulls().length - after1 <= 1 && X.ev(X.R).includes('empty-root-probe-refunded'),
    JSON.stringify({ nacks: X.nacks().length, st, more: X.pulls().length - after1 }));
  X.fab.clock += HOLDER_PROBE_WINDOW_MS + 1;
  await X.probe();
  const before = X.pulls().length;
  for (let i = 0; i < 3; i++) { X.fab.clock += 6000; X.R.am._emptyRootProbeSweep(X.fab.clock); await X.fab.settle(); }
  X.fab.clock += 6000; await X.probe();   // the direct path (the birth timer's) as well as the sweep
  const deg = X.R.logs.filter((l) => /degraded/.test(l.ev) && l.ctx?.why === 'no-holder-found');
  check('N2 on: past the window with nobody left, degraded no-holder-found is logged once and no further pull goes out',
    X.R.am.probeStatus(X.Lt.topicId).degraded === 'no-holder-found' && deg.length === 1 && X.pulls().length === before, JSON.stringify({ deg: deg.length, more: X.pulls().length - before }));
  // N5: a holder found later clears the degraded state.
  X.rRole.cache.push({ msgId: 'late', publishTs: X.fab.clock, json: '{}', bytes: 1 });
  X.R.am._emptyRootProbeSweep(X.fab.clock);
  check('N5 a holder found later clears degraded (holder-found row)', X.R.am.probeStatus(X.Lt.topicId).degraded === null && X.ev(X.R).includes('holder-found'), JSON.stringify(X.R.am.probeStatus(X.Lt.topicId))); }
{ const X = await scene('cbr-3a-frames', { evidence: false });
  X.rRole.sync.probeFrames = HOLDER_PROBE_FRAMES;
  const before = X.pulls().length;
  await X.probe();
  check('N3 on: the frame budget spent, no pull is sent and the root reports no-holder-found', X.pulls().length === before && X.R.am.probeStatus(X.Lt.topicId).degraded === 'no-holder-found', JSON.stringify({ more: X.pulls().length - before })); }
{ const X = await scene('cbr-3a-quiet', { evidence: false });
  const N = X.roleless[0];
  const n0 = X.nacks().length;
  await N.handlers.get('pubsub:pullup')({ topicId: idHex(X.Lt.topicId), sinceHw: 0, parentId: idHex(X.R.id) }, { targetId: N.id, isTerminal: true, hopCount: 1, fromId: idHex(X.R.id) });
  await X.fab.settle();
  const plain = X.nacks().length - n0;
  N.am.axonRoles.set(X.Lt.topicId, makeRole(X.Lt.topicId, false, X.fab.clock));
  await N.handlers.get('pubsub:pullup')({ topicId: idHex(X.Lt.topicId), sinceHw: 0, parentId: idHex(X.R.id), probe: 'abcd' }, { targetId: N.id, isTerminal: true, hopCount: 1, fromId: idHex(X.R.id) });
  await X.fab.settle();
  const withRole = X.nacks().length - n0 - plain;
  check('N4 no NACK for a plain PULLUP, and none from a node that holds a role', plain === 0 && withRole === 0, JSON.stringify({ plain, withRole })); }
{ const X = await scene('cbr-3a-off-nack', { armed: false, evidence: false });
  await X.probe();
  check('N4b off (control): no probe nonce is sent and no NACK comes back', X.pulls().every((j) => !j.payload?.probe) && X.nacks().length === 0, JSON.stringify({ nacks: X.nacks().length })); }

console.log(`\nfence_cbr_3a_holder_probe: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
