// =====================================================================
// smoke_persist_namespaces.mjs — RULE 13 FENCE: "silence is not success."
//
// THE DEFECT THIS GUARDS (F13.1 + N3, found 2026-07-28).
// _writeNamespace() was a chain of `if (ns === …)` with no `else`. An unknown
// namespace fell straight through, returned undefined, and was therefore
// INDISTINGUISHABLE FROM A COMPLETED WRITE. The flush loop clears the dirty set
// BEFORE writing and only re-queues on a THROW, so:
//
//   host()/unhost() marked 'hosting' dirty at four sites
//     -> _writeNamespace('hosting') matched nothing, returned undefined
//     -> flush treated that as success, dirty bit consumed, nothing logged
//     -> the adapter reported no failure because it was never called
//
// Hosting intent has therefore been silently discarded on every flush since the
// feature shipped. The generalisation is worse than the instance: ANY namespace
// added to _markPersistDirty without a matching writer fails the same way.
//
// WHAT THIS FENCE ASSERTS
//   1. every namespace that is MARKED dirty in the source has a writer
//      (the used->declared direction — the check that would have caught this)
//   2. an unsupported namespace throws a TYPED, identifiable error
//   3. the flush reports it LOUDLY at error level
//   4. …and does NOT re-queue it — retrying cannot conjure a writer, and a
//      spinning debounce would be a second bug wearing the first one's clothes
//   5. a genuine (transient) write failure still warns AND re-queues
//
// Run: node test/smoke_persist_namespaces.mjs
// =====================================================================

import { readFileSync } from 'node:fs';
import { AxonaPeer, AxonaDomain, NeuronNode } from '../src/index.js';
import { ErrorCodes } from '../src/errors.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) { console.log(`  ok ${++n} - ${m}`); }
  else   { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};

const PEER_SRC = new URL('../src/dht/AxonaPeer.js', import.meta.url);

function mkPeer({ save = async () => {} } = {}) {
  const logs = [];
  const domain = new AxonaDomain();
  const node = new NeuronNode({ id: 7n, lat: 51.5, lng: -0.1 });
  node.alive = true;
  const peer = new AxonaPeer({
    domain, node,
    persist: { save, load: async () => undefined, remove: async () => {} },
  });
  peer._emitLog = (level, type, data) => logs.push({ level, type, data });
  return { peer, logs };
}

console.log('persistence namespaces — an unwritable namespace fails LOUDLY\n');

// ── 1. used -> declared: every dirty-marked namespace has a writer ─────────
// This is the static check that would have caught F13.1 the day it landed.
{
  const src = readFileSync(PEER_SRC, 'utf8');
  const marked  = new Set([...src.matchAll(/_markPersistDirty\(\s*'([a-zA-Z]+)'/g)].map(m => m[1]));
  const written = new Set([...src.matchAll(/ns === '([a-zA-Z]+)'/g)].map(m => m[1]));
  const missing = [...marked].filter(ns => !written.has(ns));
  console.log(`     marked dirty: ${[...marked].join(', ')}`);
  console.log(`     has a writer: ${[...written].join(', ')}`);
  // 'hosting' is EXPECTED to be missing until M7 decides whether hosting is
  // persisted or the four dirty marks are removed. What must never regress is
  // that the gap is SILENT — cases 2-4 below are what make it loud. When M7
  // lands, this assertion tightens to `missing.length === 0`.
  ok('every dirty-marked namespace is either written or loudly refused',
    missing.every(ns => ns === 'hosting'),
    `unexpected silent namespaces: ${missing.filter(ns => ns !== 'hosting').join(', ')}`);
  ok('the known gap is exactly `hosting` (M7 owns the fix)',
    missing.length === 1 && missing[0] === 'hosting', `missing=${missing.join(',')}`);
}

// ── 2. an unsupported namespace throws, typed ─────────────────────────────
{
  const { peer } = mkPeer();
  let err = null;
  try { await peer._writeNamespace('hosting'); } catch (e) { err = e; }
  ok('unsupported namespace throws instead of returning undefined', err !== null);
  ok('…with the typed, switchable code',
    err?.code === ErrorCodes.PERSIST_UNSUPPORTED_NAMESPACE, `code=${err?.code}`);
  ok('…and names the namespace in its context', err?.context?.ns === 'hosting');

  // A supported one still writes normally.
  const saved = [];
  const { peer: p2 } = mkPeer({ save: async (ns, v) => saved.push(ns) });
  await p2._writeNamespace('subscriptions');
  ok('a SUPPORTED namespace still writes', saved.includes('subscriptions'), saved.join(','));
}

// ── 3+4. the flush is loud and does NOT spin ──────────────────────────────
{
  const { peer, logs } = mkPeer();
  peer._persistDirty.add('hosting');
  await peer._flushDirtyToPersist();
  const loud = logs.find(l => l.type === 'persist-namespace-unsupported');
  ok('the flush logs the drop at ERROR level', loud?.level === 'error',
    `got ${loud ? loud.level : '(no log)'}`);
  ok('…identifying the namespace', loud?.data?.ns === 'hosting');
  ok('the unwritable namespace is NOT re-queued (no debounce spin)',
    peer._persistDirty.has('hosting') === false);

  // Prove the no-spin property over repeated flushes rather than asserting it once.
  logs.length = 0;
  for (let i = 0; i < 5; i++) await peer._flushDirtyToPersist();
  ok('five further flushes produce no further noise', logs.length === 0, `logs=${logs.length}`);
}

// ── 5. a genuine adapter failure still warns AND retries ──────────────────
// The point of the typed code is to SEPARATE these two, not to stop retrying.
{
  const { peer, logs } = mkPeer({ save: async () => { throw new Error('disk on fire'); } });
  peer._persistDirty.add('subscriptions');
  await peer._flushDirtyToPersist();
  ok('a transient write failure warns', logs.some(l => l.level === 'warn' && /persist-write/.test(l.type)));
  ok('…and IS re-queued for the next debounce', peer._persistDirty.has('subscriptions') === true);
  ok('…and is not misreported as an unsupported namespace',
    !logs.some(l => l.type === 'persist-namespace-unsupported'));
}

console.log(`\n${fail === 0 ? '✓' : '✗'} persistence namespaces: ${n} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
