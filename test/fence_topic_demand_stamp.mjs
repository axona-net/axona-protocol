#!/usr/bin/env node
// =====================================================================
// fence_topic_demand_stamp.mjs — WHAT STAMPS lastDemandAt on the REAL
// ingress paths (Topic Lifetime 4.3): a stamped publish and a stamped kill
// do; a DUPLICATE kill (the 4.116.0 no-restamp rule), a REPLICATE, a beacon
// and a standby SUB do not. And the peer: a terminated row ends the app's
// Subscription handles (AxonaPeer._dispatchDelivery).
//
// Mutants (red): stamp lastDemandAt in _applyKill instead of at the fresh
// kill's stamp → D2 (the duplicate restamps); drop the stamp from
// _ingestPublish → D1.
// Run: TOPIC_LIFETIME=1 node test/fence_topic_demand_stamp.mjs
// =====================================================================
import { realTopic, makeManagerAt, rootWithBodies, signedPub, signedKill, bareMeta, idHex as fxHex } from './lib/mergeFixture.mjs';
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { Subscription } from '../src/dht/Subscription.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };

console.log('fence_topic_demand_stamp: demand is stamped by a stamped write and by nothing else');

console.log('\n[D1] a stamped publish stamps; a REPLICATE at the root (union) and a beacon do not');
{
  const L = await realTopic('lifetime-demand-stamp');
  const ctx = makeManagerAt(L.self, { neighbors: [L.near], extra: { topicLifetime: true } });
  const { role } = await rootWithBodies(ctx, L, 1);
  const stampedAt = role.lastDemandAt;
  check('the stamp set lastDemandAt to the ingest clock', stampedAt === ctx.clock.t);
  ctx.clock.t += 20_000;
  await ctx.am._onReplicate({ topicId: fxHex(L.topicId), from: fxHex(L.near), msgs: [], dels: [], gen: { root: fxHex(L.near), epoch: 1 }, issuedAt: ctx.clock.t }, { targetId: L.self, isTerminal: true, hopCount: 1, fromId: fxHex(L.near) });
  await ctx.am._ingestIdle();
  ctx.am._onRootBeacon({ root: fxHex(L.near), topics: [fxHex(L.topicId)], epochs: [1], beaconId: 'b-1', layer: 0 }, { targetId: L.self, isTerminal: false, hopCount: 1 });
  check('a REPLICATE union at the root and a beacon leave lastDemandAt alone', role.lastDemandAt === stampedAt);
  ctx.clock.t += 20_000;
  const w = await signedPub(ctx, L.desc, 7);
  const r = await ctx.am._ingestPublish(role, w.json, ctx.am._cutCapture(role));
  check('a second stamped publish stamps again', r?.ok && role.lastDemandAt === ctx.clock.t);
}

console.log('\n[D2] a fresh kill stamps; the DUPLICATE kill does not (no age laundering)');
{
  const L = await realTopic('lifetime-demand-kill');
  const ctx = makeManagerAt(L.self, { neighbors: [L.near], extra: { topicLifetime: true } });
  const { role, ids } = await rootWithBodies(ctx, L, 1);
  const kill = await signedKill(ctx, L.topicId, ids[0]);
  ctx.clock.t += 1000;
  await ctx.am._onKill({ topicId: fxHex(L.topicId), kill }, bareMeta(ctx, L.publisher));
  const killAt = ctx.clock.t;
  check('the fresh kill stamps lastDemandAt', role.tombstones.size === 1 && role.lastDemandAt === killAt);
  ctx.clock.t += 60_000;
  await ctx.am._onKill({ topicId: fxHex(L.topicId), kill }, bareMeta(ctx, L.publisher));
  check('the duplicate kill, a minute later, stamps nothing', role.lastDemandAt === killAt && role.tombstones.size === 1);
}

console.log('\n[P1] the peer: a terminated row reaches each Subscription handle once and stops it; the set is dropped');
{
  const got = [];
  const peer = { _subscriptions: new Map() };
  const t = 0x1234n;
  const s1 = new Subscription({ peer, topicId: t, topicName: 'lifetime/peer', handler: (e) => got.push(['s1', e]) });
  const s2 = new Subscription({ peer, topicId: t, topicName: 'lifetime/peer', handler: (e) => got.push(['s2', e]) });
  peer._subscriptions.set(t, new Set([s1, s2]));
  AxonaPeer.prototype._dispatchDelivery.call(peer, t, JSON.stringify({ terminated: true, msgId: null, topic: null, generation: { root: 'ab', epoch: 3 } }), null, 1000, null);
  check('both handlers received one terminated row carrying the generation and the topic name', got.length === 2 && got.every(([, e]) => e.terminated === true && e.generation.epoch === 3 && e.topic === 'lifetime/peer'), JSON.stringify(got));
  check('both handles are stopped and the topic\'s set is gone', s1.stopped && s2.stopped && !peer._subscriptions.has(t));
  AxonaPeer.prototype._dispatchDelivery.call(peer, t, JSON.stringify({ terminated: true, generation: { root: 'ab', epoch: 3 } }), null, 1000, null);
  check('a second row finds no set and delivers nothing', got.length === 2);
}

console.log(`\nfence_topic_demand_stamp: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
