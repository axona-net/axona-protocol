// =====================================================================
// fence_health_role_projection.mjs — health() carries the WHOLE role row
// out of inspectRoles(), and says so when it could not read it at all.
//
// WHY THIS EXISTS. AxonaManager.inspectRoles() has always returned
// { topicId, isRoot, nature, holder, children, subscribers, replayCacheSize,
// lastReplicaAt, lastReplicaAgeMs }. health() consumed that and copied FOUR
// fields, dropping the rest on the next line. Nothing failed, because nothing
// tested the seam.
//
// The cost was concrete and it took a question from David to surface it: a
// relay serves no /diag, so its only role-level view is the SIGUSR1 health
// dump, and the dump can only pass on what health() hands it. "Do I hold a
// role with no subscribers and no messages" was therefore answerable on the
// two bridges and on none of the other 52 nodes in the fleet.
//
// Aster made the sharper point at council 655: the relay's own fences feed
// hand-built health objects into the dump, so they verify the dump's
// projection and CANNOT catch health() re-narrowing the row upstream. Delete
// the kernel fix and those stay green. This file is the missing half — it
// drives the real health() over a real inspectRoles() shape.
//
// It calls health() with a minimal `this` rather than constructing a peer:
// the method is a pure projection over _axonaManager, and building a live node
// to test a projection would make the test slower than the thing it guards.
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let pass = 0, fail = 0;
const ok = (cond, name, got) => {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fail++; console.log(`  ✗ ${name}${got !== undefined ? ` — got ${JSON.stringify(got)}` : ''}`); }
};

// Exactly the shape AxonaManager.inspectRoles() emits (AxonaManager.js:965-975).
const ROW = {
  topicId: '808c5e5dce7763235d3de3fa98455069fff3ceae550261d8e66afc6564b9805d',
  isRoot: false,
  nature: 'backup',
  holder: false,
  children: [],
  subscribers: 0,
  replayCacheSize: 0,
  lastReplicaAt: 1790824000000,
  lastReplicaAgeMs: 3794,
};

// The minimum `this` health() touches. Kept explicit rather than mocked with a
// proxy: when health() grows a new dependency this fence fails loudly with the
// missing member's name, which is the behaviour we want from a seam test.
// The minimum `this` health() touches, enumerated from the method body rather
// than discovered one TypeError at a time. Kept explicit rather than proxied:
// when health() grows a dependency this fence fails loudly naming the missing
// member, which is what you want from a seam test.
const basePeer = () => ({
  _node: { id: 'n1' },
  _engine: null,
  _transport: null,
  _started: true,
  _subscriptions: new Map(),
  _errorHandlers: new Set(),
  _logHandlers: new Set(),
  _nodeIdHex: () => 'ff00aabb',
  _installTransportLogHook: () => {},
  peers: () => [],
  lookaheadStats: () => null,
});
const peerWith = (inspectRoles) => ({ ...basePeer(), _axonaManager: { inspectRoles } });

const callHealth = (self) => AxonaPeer.prototype.health.call(self);

console.log('A. the whole row survives inspectRoles -> health');
{
  const h = callHealth(peerWith(() => [ROW]));
  const r = (h.axonRoles || [])[0] || {};
  ok(h.axonRoles.length === 1, 'the role is projected at all', h.axonRoles.length);
  ok(r.subscribers === 0, 'subscribers survives AND 0 is preserved, not dropped', r.subscribers);
  ok(r.nature === 'backup', 'nature survives', r.nature);
  ok(r.holder === false, 'holder survives', r.holder);
  ok(r.lastReplicaAgeMs === 3794, 'lastReplicaAgeMs survives', r.lastReplicaAgeMs);
  ok(r.lastReplicaAt === 1790824000000, 'lastReplicaAt survives', r.lastReplicaAt);
  ok(r.cacheSize === 0, 'replayCacheSize is projected as cacheSize', r.cacheSize);
  ok(r.children === 0, 'children is projected as a COUNT, not the array', r.children);
  ok(r.topic === ROW.topicId, 'the FULL topic id is carried, never a prefix', r.topic);
}

console.log('B. a non-zero subscriber count is not special-cased away');
{
  const h = callHealth(peerWith(() => [{ ...ROW, subscribers: 4, nature: 'root', isRoot: true }]));
  const r = h.axonRoles[0];
  ok(r.subscribers === 4 && r.nature === 'root' && r.isRoot === true, 'a populated root projects whole');
}

console.log('C. UNREADABLE is not the same fact as EMPTY');
{
  const good   = callHealth(peerWith(() => []));
  const thrown = callHealth(peerWith(() => { throw new Error('manager wedged'); }));
  const absent = callHealth(basePeer());   // no _axonaManager at all

  ok(good.axonRoles.length === 0 && good.axonRolesComplete === true,
     'a genuinely empty node reports complete=true');
  ok(thrown.axonRoles.length === 0 && thrown.axonRolesComplete === false,
     'a THROWING inspection reports complete=false, not a clean zero');
  ok(absent.axonRolesComplete === false,
     'no manager at all reports complete=false');
  ok(good.axonRoles.length === thrown.axonRoles.length,
     'the two are byte-identical in axonRoles — which is exactly why the flag is needed');
}

console.log('D. a throwing inspection must not take the rest of health() down');
{
  let threw = null, h;
  try { h = callHealth(peerWith(() => { throw new Error('boom'); })); } catch (e) { threw = e; }
  ok(threw === null, 'health() does not propagate an inspectRoles throw', threw && threw.message);
  ok(h && typeof h === 'object', 'and still returns a health object');
}

// A partially-populated row is what an older or mid-upgrade manager emits.
// It must degrade field by field, never drop the row.
console.log('E. a sparse row degrades per-field, it does not vanish');
{
  const h = callHealth(peerWith(() => [{ topicId: 'abc', isRoot: true, replayCacheSize: 2 }]));
  const r = h.axonRoles[0];
  ok(h.axonRoles.length === 1, 'the row is still projected');
  ok(r.subscribers === null, 'missing subscribers is null, NOT 0', r.subscribers);
  ok(r.nature === null, 'missing nature is null', r.nature);
  ok(r.lastReplicaAgeMs === null, 'missing replica age is null', r.lastReplicaAgeMs);
  ok(r.cacheSize === 2, 'what IS present still arrives', r.cacheSize);
}

console.log(`\n${fail === 0 ? '✓' : '✗'} ${pass}/${pass + fail}`);
process.exit(fail === 0 ? 0 : 1);
