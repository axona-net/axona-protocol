#!/usr/bin/env node
// =====================================================================
// fence_deleted_body_bound.mjs — THE RESIDUAL, recorded not passed (Topic
// Lifetime 4.6; David 2026-10-20: "There is no need for a deletion record").
//
// Nothing survives the tree. A holder partitioned through BOTH the kill and
// the termination, still holding the deleted body, can regenerate a root with
// it: the body is then served until its own TTL, unless the deleting author
// sends the kill again. A holder that saw the kill holds no body and
// regenerates nothing with it. This fence pins the bound as the packet
// states it; it is not a claim that the body cannot return.
// Run: TOPIC_LIFETIME=1 node test/fence_deleted_body_bound.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, signedKill, bareMeta, idHex as fxHex } from './lib/mergeFixture.mjs';
import { TTL_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };

console.log('fence_deleted_body_bound: the one case weaker than today, sized');

console.log('\n[B1] a holder that missed the kill AND the notice regenerates a root carrying the body; it is served until its TTL');
{
  const L = await realTopic('lifetime-deleted-body');
  // the ROOT: a body, then its kill, then termination (cache empty, demand 0)
  const root = makeManagerAt(L.self, { neighbors: [L.near], extra: { topicLifetime: true, rootReplicas: 2 } });
  const { role, ids } = await rootWithBodies(root, L, 1);
  const body = { ...role.cache[0] };
  // the partitioned HOLDER: a backup that received the body and then heard nothing
  const holder = makeManagerAt(L.near, { neighbors: [L.self], extra: { topicLifetime: true, rootReplicas: 2 } });
  await holder.am._onReplicate({ topicId: fxHex(L.topicId), from: fxHex(L.self), msgs: [{ json: body.json, publishTs: body.publishTs, msgId: body.msgId, seq: body.seq }], dels: [], gen: { root: fxHex(L.self), epoch: 1 }, issuedAt: holder.clock.t }, { targetId: L.near, isTerminal: true, hopCount: 1, fromId: fxHex(L.self) });
  await holder.am._ingestIdle();
  const hRole = holder.am.axonRoles.get(L.topicId);
  check('the holder has the body as a backup', hRole && hRole.cache.length === 1 && hRole.backupOf === fxHex(L.self).toLowerCase());
  // the kill lands at the root only (the partition)
  const kill = await signedKill(root, L.topicId, ids[0]);
  root.clock.t += 1000;
  await root.am._onKill({ topicId: fxHex(L.topicId), kill }, bareMeta(root, L.publisher));
  check('at the root: body gone, tombstone standing', role.cache.length === 0 && role.tombstones.size === 1);
  // the root terminates (nobody is seated; the holder's seat never arrived) and drops the tombstone
  for (let i = 0; i < 40; i++) { root.clock.t += 5000; await root.am.refreshTick(); }
  check('the root terminated and the tombstone went with it: no record anywhere on the root', !root.am.axonRoles.has(L.topicId) && root.am.inspectAdmission().reaped.terminated === 1 && root.am._leftRecords.size === 0);
  // the holder, partitioned through both, lapses its lease — but holds a BODY, so it keeps the election obligation
  holder.clock.t = root.clock.t;
  for (let i = 0; i < 4; i++) { holder.clock.t += 5000; await holder.am.refreshTick(); }
  check('the holder keeps the body and its standby obligation (4.4(1)): the body can be served again', hRole.cache.length === 1 && holder.am._backupTopics.has(L.topicId));
  check(`the bound: the body lives at most its own TTL (${TTL_MS / 3_600_000} h) from its stamp`, body.publishTs + TTL_MS > holder.clock.t);
  holder.clock.t = body.publishTs + TTL_MS + 1;
  await holder.am.refreshTick();
  check('…and is gone at the TTL', hRole.cache.length === 0);
}

console.log('\n[B2] a holder that SAW the kill holds no body and regenerates nothing with it');
{
  const L = await realTopic('lifetime-deleted-body-seen');
  const root = makeManagerAt(L.self, { neighbors: [L.near], extra: { topicLifetime: true, rootReplicas: 2 } });
  const { role, ids } = await rootWithBodies(root, L, 1);
  const body = { ...role.cache[0] };
  const holder = makeManagerAt(L.near, { neighbors: [L.self], extra: { topicLifetime: true, rootReplicas: 2 } });
  await holder.am._onReplicate({ topicId: fxHex(L.topicId), from: fxHex(L.self), msgs: [{ json: body.json, publishTs: body.publishTs, msgId: body.msgId, seq: body.seq }], dels: [], gen: { root: fxHex(L.self), epoch: 1 }, issuedAt: holder.clock.t }, { targetId: L.near, isTerminal: true, hopCount: 1, fromId: fxHex(L.self) });
  await holder.am._ingestIdle();
  const kill = await signedKill(root, L.topicId, ids[0]);
  root.clock.t += 1000;
  await root.am._onKill({ topicId: fxHex(L.topicId), kill }, bareMeta(root, L.publisher));
  const dels = root.am._activeDels(role);
  await holder.am._onReplicate({ topicId: fxHex(L.topicId), from: fxHex(L.self), msgs: [], dels, gen: { root: fxHex(L.self), epoch: 1 }, issuedAt: holder.clock.t }, { targetId: L.near, isTerminal: true, hopCount: 1, fromId: fxHex(L.self) });
  await holder.am._ingestIdle();
  const hRole = holder.am.axonRoles.get(L.topicId);
  check('the kill reached the holder: body gone, tombstone held', hRole.cache.length === 0 && hRole.tombstones.size === 1);
  await holder.am._onTopicTerminated({ topicId: fxHex(L.topicId), from: fxHex(L.self), gen: { root: fxHex(L.self), epoch: 1 } }, { targetId: L.near, isTerminal: true, hopCount: 1, fromId: fxHex(L.self) });
  check('the notice ends it: role, tombstone and seat gone; nothing to regenerate with', !holder.am.axonRoles.has(L.topicId) && !holder.am._backupTopics.has(L.topicId));
}

console.log(`\nfence_deleted_body_bound: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
