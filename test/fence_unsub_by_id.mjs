// fence_unsub_by_id.mjs — unsub accepts what sub accepts (GH #64, howard-stearns).
//
// The API contract: "share the ID for reading; share the descriptor for writing."
// sub()/pull() take a descriptor OR a bare 66-hex topic id. unsub() went through the
// descriptor-only resolver, so a reader holding only the shared id could subscribe
// and never unsubscribe — PUBLISH_INVALID_TOPIC on the way out. This fence pins:
//   1. unsub(66-hex id) does not throw and reports { ok:true, removed:0 } when there
//      is nothing to remove (resolution is the thing under test, not the network).
//   2. unsub(descriptor) and unsub(id derived from that descriptor) target the SAME
//      topicIdBig — a sub made by descriptor is removed by its id, and vice versa.
//   3. A malformed string still throws PUBLISH_INVALID_TOPIC (no silent no-op).
//   4. Source: unsub resolves through _resolveReadTopic, the same helper as sub.
//
// Offline: mock transport, no bridge, no network. Run: node test/fence_unsub_by_id.mjs
import { readFileSync } from 'node:fs';
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { AxonaDomain } from '../src/dht/AxonaDomain.js';
import { ErrorCodes } from '../src/errors.js';
import { sealByOwnMethods } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

function makePeer() {
  const tx = sealByOwnMethods({
    onNotification(wire, cb) { return { wire, unsub() {} }; },
    async notify() { return true; },
  });
  const node = { id: 1n, alive: true, transport: tx };
  const peer = new AxonaPeer({ node, transport: tx, domain: new AxonaDomain() });
  peer._emitLog = () => {};
  return peer;
}

console.log('\nGH #64 — unsub accepts a 66-hex topic id\n');

const peer = makePeer();
const desc = { region: 'eagle', name: 'fence-unsub-by-id' };
const resolved = await peer._resolveTopicOrThrow(desc, 'fence');
const id = resolved.topicId;
check('descriptor resolves to a 66-hex id', typeof id === 'string' && /^[0-9a-f]{66}$/.test(id), id);

// 1. id form does not throw
let r1, e1 = null;
try { r1 = await peer.unsub(id); } catch (e) { e1 = e; }
check('unsub(id) does not throw', e1 === null, e1 && `${e1.code || e1.name}: ${e1.message}`);
check('unsub(id) with nothing subscribed → { ok:true, removed:0 }', r1 && r1.ok === true && r1.removed === 0, JSON.stringify(r1));

// 2. both forms target the same feed
const byDesc = await peer._resolveReadTopic(desc, 'fence');
const byId   = await peer._resolveReadTopic(id, 'fence');
check('descriptor and id resolve to the same topicIdBig', byDesc.topicIdBig === byId.topicIdBig);
check('uppercase / padded id is normalised, not rejected', (await peer._resolveReadTopic(`  ${id.toUpperCase()} `, 'fence')).topicIdBig === byId.topicIdBig);

// A registered subscription keyed by the descriptor's id is removed by the bare id.
// _subscriptions is keyed by topicIdBig; seed one stub subscription the way sub() would.
let stopped = 0;
const stub = { async stop() { stopped++; peer._subscriptions.get(byDesc.topicIdBig)?.delete(stub); } };
peer._subscriptions.set(byDesc.topicIdBig, new Set([stub]));
const r2 = await peer.unsub(id);
check('a subscription registered under the descriptor is removed by unsub(id)', r2.ok === true && r2.removed === 1 && stopped === 1, JSON.stringify(r2));
peer._subscriptions.set(byDesc.topicIdBig, new Set([stub]));
const r3 = await peer.unsub(desc);
check('and by unsub(descriptor) — same feed either way', r3.ok === true && r3.removed === 1 && stopped === 2, JSON.stringify(r3));

// 3. malformed string still refuses
let e3 = null;
try { await peer.unsub('not-a-topic-id'); } catch (e) { e3 = e; }
check('unsub("not-a-topic-id") throws PUBLISH_INVALID_TOPIC', e3 && e3.code === ErrorCodes.PUBLISH_INVALID_TOPIC, e3 && `${e3.code}: ${e3.message}`);
let e4 = null;
try { await peer.unsub(id.slice(0, 64)); } catch (e) { e4 = e; }
check('unsub(64-hex) throws — the read handle is 66 hex, region byte included', e4 && e4.code === ErrorCodes.PUBLISH_INVALID_TOPIC);

// 4. source
const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
const unsubBody = src.slice(src.indexOf('async unsub('), src.indexOf('async unsub(') + 600);
check('source: unsub resolves via _resolveReadTopic (same helper as sub/pull)', /_resolveReadTopic\(topic, 'unsub'\)/.test(unsubBody) && !/_resolveTopicOrThrow\(topic, 'unsub'\)/.test(unsubBody));

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
