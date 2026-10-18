#!/usr/bin/env node
// =====================================================================
// fence_merge_gate_forwards_writes.mjs — dual-root packet 4(d) "the
// linearization point is the cut" (axona-docs 14a6bd8; David "build it"
// 2026-10-17).
//
// A PUBLISHER WRITES THROUGH L EVERY 10 ms FROM BEFORE THE BIND TO AFTER
// THE DEMOTION. Every message is stamped either at L before the cut or at
// W after it, and none at both. On a two-node fabric: L (farther) is root
// with custody; W (closer) is root at W's epoch; at t=50 L binds (cut +
// gate toward W at W's epoch); at t=150 L demotes with the release token
// (step 5 stands in for d3's exchange) and the gate clears. Writes between
// the bind and the demotion are forwarded to W by name and acked by W to
// L's flight; writes after the demotion reach W by the step-down hold.
//
// Checks: (1) writes before the bind are stamped at L (L acked exactly
// those); (2) every write after the bind is held at W; (3) L stamped none
// of them: W acked every one, L acked none, and L forwarded each gated one
// (L may HOLD them afterwards, replayed down as W's child, which is the
// shipped path and not a stamp); (4) every gated forward's flight
// was discharged by W's ack (no open merge flight); (5) L ends as W's
// child with upstream pinned to W.
//
// Mutant (red): _mergeGateForward marks undeliverable instead of forwarding
// → 2 fails.
// Run: node test/fence_merge_gate_forwards_writes.mjs
// =====================================================================
import { realTopic, Fabric, freshId, signedPub, idHex } from './lib/mergeFixture.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

console.log('fence_merge_gate_forwards_writes: 4(d) — every write lands at exactly one side of the cut');

const L = await realTopic('gate-forwards-writes');
const fab = new Fabric();
const a = await freshId(), b = await freshId();
const [wId, lId] = ((a ^ L.topicId) < (b ^ L.topicId)) ? [a, b] : [b, a];
const W = fab.addNode(wId); const Ln = fab.addNode(lId);
const PUB = L.publisher;
// W roots at its epoch; L roots too (the split): both become root directly.
// THE SPLIT IS THAT L DOES NOT HEAR W: W's beacons are silenced on this
// fabric, or the shipped last-mile correction at L forwards every write to
// the closer beaconing root before any bind (which is right, and not the
// scene under test).
W.am._announceRoot = () => {};
const wRole = W.rc.become(L.topicId, 'sub-terminal');
const lRole = Ln.rc.become(L.topicId, 'sub-terminal');
const writes = [];
const writeAt = async (t) => {
  fab.clock = t;
  const w = await signedPub({ clock: { t } }, L.desc, 200 + writes.length);
  writes.push({ t, msgId: w.msgId });
  await Ln.handlers.get('pubsub:pub')({ topicId: idHex(L.topicId), json: w.json }, { targetId: lId, isTerminal: true, hopCount: 2, fromId: idHex(PUB) });
  await fab.settle();
};
for (let t = 1_700_000_000_000; t < 1_700_000_000_050; t += 10) await writeAt(t);
const beforeCut = writes.map((w) => w.msgId);
// THE BIND at t=50: cut + gate toward W at W's epoch
fab.clock = 1_700_000_000_050;
const gate = Ln.am._mergeCut(L.topicId, { winner: idHex(wId), winnerEpoch: wRole.epoch, corr: 'c0000077' });
if (!gate) throw new Error('bind failed');
for (let t = 1_700_000_000_050; t < 1_700_000_000_150; t += 10) await writeAt(t);
const gated = writes.slice(beforeCut.length).map((w) => w.msgId);
// STEP 5 stands in: demote with the release token, clear the gate
fab.clock = 1_700_000_000_150;
lRole.merge.releaseToken = 'tok-step5';
const demoted = Ln.rc.demote(L.topicId, idHex(wId), 'merge-step5', { releaseToken: 'tok-step5' });
Ln.am._mergeGateClear(lRole, 'completed');
await fab.settle();
for (let t = 1_700_000_000_150; t < 1_700_000_000_200; t += 10) await writeAt(t);
const after = writes.slice(beforeCut.length + gated.length).map((w) => w.msgId);

const lAcks = Ln.routed.filter((r) => r.type === 'pubsub:ingestack');          // L acks only what L STAMPED
const wAcks = W.routed.filter((r) => r.type === 'pubsub:ingestack');
check('1 writes before the bind are stamped at L (L acked exactly those)', beforeCut.every((id) => lRole.cacheIds.has(id)) && lAcks.length === beforeCut.length && beforeCut.every((id) => lAcks.some((a) => a.payload.msgId === id)), `L=${lRole.cache.length} lAcks=${lAcks.length}`);
check('2 every write after the bind is held at W (gated forwards and post-demotion holds alike)', gated.every((id) => wRole.cacheIds.has(id)) && after.every((id) => wRole.cacheIds.has(id)), JSON.stringify({ gatedAtW: gated.filter((id) => wRole.cacheIds.has(id)).length, of: gated.length, afterAtW: after.filter((id) => wRole.cacheIds.has(id)).length, ofAfter: after.length }));
check('3 L STAMPED none of the writes after the bind: W acked every one of them, L acked none (L may HOLD them later, replayed down as W\'s child — the shipped path)', [...gated, ...after].every((id) => wAcks.some((a) => a.payload.msgId === id)) && [...gated, ...after].every((id) => !lAcks.some((a) => a.payload.msgId === id)) && Ln.logs.filter((l) => l.ev === 'pubsub:merge-gate-forward').length === gated.length, JSON.stringify({ wAcks: wAcks.length, lAcks: lAcks.length, fwd: Ln.logs.filter((l) => l.ev === 'pubsub:merge-gate-forward').length }));
check('4 every gated forward\'s flight was discharged by W\'s ack: no open flight at L', !Ln.am._writeFlights || Ln.am._writeFlights.size === 0, JSON.stringify([...(Ln.am._writeFlights?.keys() || [])].map((k) => k.slice(-6))));
check('5 L ends as W\'s child: demoted, upstream pinned to W, gate cleared', demoted === true && lRole.isRoot === false && (Ln.am._upstream.get(L.topicId) || [])[0] === idHex(wId) && lRole.merge.gate === null);

console.log(`\nfence_merge_gate_forwards_writes: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
