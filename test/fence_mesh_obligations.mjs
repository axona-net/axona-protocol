// fence_mesh_obligations.mjs — the bounded mesh degree must not retire a
// channel this node owes something to.
//
// WHY (council, 2026-09-24). Aster 6ad4f075: "an unpopulated protected set
// cannot establish that a retirement candidate channel is free of root,
// upstream, or standby obligations." Orion b57e6f1a required, before the cap
// binds under production traffic, that the set be populated with "channels
// serving as primary topic roots, active upstream subscription links, and
// designated standby election peers." Both were right and the cap was
// contained on both production bridges until this landed.
//
// THE CHAIN, and every link is a place it can go wrong:
//     meshId --nodeIdFor--> nodeId --obligedPeers--> duty?
// The first link is the one 4.95.0 got wrong by reading the bridge's own
// connection handle as a nodeId, which made every peer look unauthenticated
// and the cap a no-op. The second is the kernel's answer, and only the
// manager can give it — the mesh layer holds channels, not roles.
//
// FAIL CLOSED. Absent provider, a throwing provider, a non-Set return, or an
// unresolvable binding all report PROTECTED. "Cannot say" is not "no duty".
// The cost of the safe answer is a channel we keep; the cost of the unsafe one
// is a dropped obligation on a live bridge.
//
// Aster's bar for this fence, taken literally: exercise the REAL enforcement
// path with a REAL authenticated mapping and REAL obligations, including
// missing bindings and rebinding — not only the pure selector.
//
// Run: node test/fence_mesh_obligations.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { MeshManager } from '../src/transport/web/mesh.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { makeProtectionResolver } from '../src/transport/web/mesh_degree.js';
import { depositDispatchCapability } from '../src/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};

// Real 264-bit ids: region in the TOP byte, 66 hex chars. Built from the hex
// spelling, never by shifting — a 256-bit value padded to 66 puts the region
// two bytes off, which is how the first version of the sibling fence failed.
const idOf = (region, tail) => BigInt('0x' + region + tail.padStart(64, '0'));
const hexOf = (b) => b.toString(16).padStart(66, '0');

const SELF     = idOf('89', 'e1f');
const UPSTREAM = idOf('89', 'a01');   // we are homed under it for a topic
const PRINCIPAL= idOf('80', 'b02');   // the root replicating to our backup
const REPLICA  = idOf('80', 'c03');   // holds a warm copy of our root
const CHILD    = idOf('89', 'd04');   // seated subscriber in a role we hold
const SPARE1   = idOf('89', 'f05');   // no duty
const SPARE2   = idOf('80', 'f06');   // no duty
const TOPIC_A  = idOf('89', '111');
const TOPIC_B  = idOf('80', '222');

function manager() {
  const dht = {
    verdictsSupported: true,
    routeMessage: async () => ({ consumed: false }),
    getSelfId: () => hexOf(SELF),
    onRoutedMessage() {}, onDirectMessage() {},
    neighbors: () => [], bridgeId: () => null,
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
  };
  depositDispatchCapability(dht, { routed: () => {} });
  const m = new AxonaManager({ dht });
  m._log = () => {};
  return m;
}

console.log('mesh obligations — a channel carrying a duty is never retired\n');

// ── 1. THE KERNEL'S ANSWER: what counts as an obligation ────────────────
const am = manager();
{
  // Homed under UPSTREAM for topic A.
  am._upstream.set(TOPIC_A, [hexOf(UPSTREAM).toLowerCase()]);
  // A backup we hold, replicated to us by PRINCIPAL.
  const backup = makeRole(TOPIC_B, false, Date.now());
  backup.backupOf = hexOf(PRINCIPAL).toLowerCase();
  am.axonRoles.set(TOPIC_B, backup);
  // A root we hold, with a credited replica and a seated child.
  const root = makeRole(TOPIC_A, true, Date.now());
  root.replicas.set(hexOf(REPLICA).toLowerCase(), { at: Date.now() });
  root.subscribers.set(hexOf(CHILD).toLowerCase(), { since: 0, lastRenewed: Date.now() });
  am.axonRoles.set(TOPIC_A, root);

  const duty = am.obligedPeers();
  ok('1a. the UPSTREAM we are homed under is an obligation', duty.has(hexOf(UPSTREAM)));
  ok('1b. the PRINCIPAL replicating to our backup is an obligation', duty.has(hexOf(PRINCIPAL)));
  ok('1c. a credited REPLICA of our root is an obligation', duty.has(hexOf(REPLICA)));
  ok('1d. a SEATED SUBSCRIBER is an obligation — the channel IS the delivery path',
    duty.has(hexOf(CHILD)));
  ok('1e. a peer with no duty is NOT in the set — the set is not vacuous',
    !duty.has(hexOf(SPARE1)) && !duty.has(hexOf(SPARE2)), [...duty].length + ' entries');
  ok('1f. it is a fresh Set each call, so a duty acquired later is seen',
    am.obligedPeers() !== duty);
}

// ── the transport half: the resolver webTransport installs ──────────────
/**
 * THE SHIPPED RESOLVER, not a copy of it. makeProtectionResolver is what
 * webTransport installs; a fence that re-implemented the same logic would
 * certify its own re-implementation and prove nothing about the code on the
 * bridge. `transport` stands in for the WebRTCTransport, supplying the same
 * nodeIdFor(meshId) binding it records at authentication.
 */
function makeIsProtected({ bindings, provider }) {
  return makeProtectionResolver({
    transport: () => ({ nodeIdFor: (meshId) => bindings.get(meshId) ?? null }),
    provider:  () => provider,
  });
}
/** A mesh whose teardown is observed, driven through the REAL _enforceDegree. */
function meshWith({ bindings, provider, cap = 2 }) {
  const retired = [];
  const mesh = new MeshManager({
    sendSignal: () => {}, log: () => {},
    degree: {
      maxPeers: cap, slack: 0, minUptimeMs: 0,
      regionOf: (meshId) => {
        const nid = bindings.get(meshId);
        return (typeof nid === 'bigint') ? hexOf(nid).slice(0, 2).toLowerCase() : null;
      },
      isProtected: makeIsProtected({ bindings, provider }),
    },
  });
  mesh._myId = 'self';
  mesh._retire = (id, why) => { retired.push({ id, why }); mesh._peers.delete(id); };
  mesh.getLatency = () => null;
  const open = (meshId) => {
    const st = mesh._newPeerState(meshId, 'offerer');
    st.state = 'open';
    st.openedAt = Date.now() - 600_000;
    mesh._peers.set(meshId, st);
  };
  return { mesh, retired, open };
}
// Connection handles, exactly as the bridge mints them: c${seq.toString(36)}.
const BIND = new Map([
  ['c1', UPSTREAM], ['c2', PRINCIPAL], ['c3', REPLICA],
  ['c4', CHILD], ['c5', SPARE1], ['c6', SPARE2],
]);

console.log('\n[2] THROUGH THE REAL ENFORCEMENT PATH: duties survive, spares go');
{
  const { mesh, retired, open } = meshWith({ bindings: BIND, provider: () => am.obligedPeers() });
  for (const id of BIND.keys()) open(id);
  // 6 open against cap 2 + slack 0: drain it one pass at a time.
  for (let i = 0; i < 10; i++) { mesh._lastRetireAt = 0; mesh._enforceDegree(); }
  const gone = new Set(retired.map((r) => r.id));
  ok('2a. the UPSTREAM channel was never retired', !gone.has('c1'), [...gone].join(','));
  ok('2b. the PRINCIPAL channel was never retired', !gone.has('c2'));
  ok('2c. the REPLICA channel was never retired', !gone.has('c3'));
  ok('2d. the SEATED SUBSCRIBER channel was never retired', !gone.has('c4'));
  ok('2e. BOTH spares were retired — protection is narrow, not a blanket',
    gone.has('c5') && gone.has('c6'), [...gone].join(','));
  ok('2f. the degree stops above the band rather than sacrificing a duty',
    mesh._peers.size === 4, `open=${mesh._peers.size}`);
}

console.log('\n[3] FAIL CLOSED: every way of not knowing keeps the channel');
{
  const cases = [
    ['no provider installed at all',        undefined],
    ['a provider that throws',              () => { throw new Error('boom'); }],
    ['a provider returning a non-Set',      () => ['not', 'a', 'set']],
    ['a provider returning null',           () => null],
  ];
  for (const [label, provider] of cases) {
    const { mesh, retired, open } = meshWith({ bindings: BIND, provider });
    for (const id of BIND.keys()) open(id);
    for (let i = 0; i < 10; i++) { mesh._lastRetireAt = 0; mesh._enforceDegree(); }
    ok(`3. ${label} ⇒ nothing is retired`, retired.length === 0, `retired=${retired.length}`);
  }
}

console.log('\n[4] A MISSING BINDING IS NOT A SPARE');
{
  // An authenticated peer whose meshId has no binding yet: unresolvable, so it
  // cannot be SHOWN to be free of duty. It is also regionless, so the selector
  // would skip it anyway — this pins that both layers agree.
  const bindings = new Map(BIND);
  bindings.delete('c5');
  const { mesh, retired, open } = meshWith({ bindings, provider: () => am.obligedPeers() });
  for (const id of BIND.keys()) open(id);
  for (let i = 0; i < 10; i++) { mesh._lastRetireAt = 0; mesh._enforceDegree(); }
  const gone = new Set(retired.map((r) => r.id));
  ok('4a. an unbound channel is NOT retired', !gone.has('c5'), [...gone].join(','));
  ok('4b. …and the bound spare still is', gone.has('c6'));
  const isProt = makeIsProtected({ bindings, provider: () => am.obligedPeers() });
  ok('4c. the resolver reports an unbound channel as PROTECTED, not unknown',
    isProt('c5') === true && isProt('cZZ') === true);
}

console.log('\n[5] REBINDING: protection follows the nodeId, not the channel');
{
  // The upstream peer drops and re-opens under a NEW connection handle. No
  // bookkeeping in the mesh should be needed — the duty is on the node.
  const bindings = new Map(BIND);
  bindings.delete('c1');
  bindings.set('c9', UPSTREAM);            // same node, new handle
  const { mesh, retired, open } = meshWith({ bindings, provider: () => am.obligedPeers() });
  for (const id of bindings.keys()) open(id);
  for (let i = 0; i < 10; i++) { mesh._lastRetireAt = 0; mesh._enforceDegree(); }
  const gone = new Set(retired.map((r) => r.id));
  ok('5a. the re-bound upstream is protected under its NEW handle', !gone.has('c9'),
    [...gone].join(','));
  ok('5b. …with no per-channel state to go stale',
    mesh._retiredRecently.has('c9') === false);
}

console.log('\n[6] AN OBLIGATION ACQUIRED BETWEEN PASSES IS HONOURED ON THE NEXT');
{
  // The reader is called per pass, deliberately, so this is the behaviour that
  // distinguishes it from handing over a frozen snapshot.
  const live = manager();
  const { mesh, retired, open } = meshWith({ bindings: BIND, provider: () => live.obligedPeers(), cap: 5 });
  for (const id of BIND.keys()) open(id);
  ok('6a. precondition: this node owes nothing yet', live.obligedPeers().size === 0);
  mesh._lastRetireAt = 0; mesh._enforceDegree();
  ok('6b. with no duties, a spare is retired', retired.length === 1);
  // Now acquire a duty on the very peer that would be chosen next.
  const next = mesh._peers.keys().next().value;
  live._upstream.set(TOPIC_A, [hexOf(BIND.get(next)).toLowerCase()]);
  mesh._lastRetireAt = 0; mesh._enforceDegree();
  ok('6c. the newly-owed channel is spared on the NEXT pass',
    !retired.some((r) => r.id === next), `retired=${retired.map((r) => r.id).join(',')}`);
}

console.log('\n[7] THE READER IS CALLED ONCE PER PASS, NOT ONCE PER CANDIDATE');
{
  // ASTER'S FINDING (8aa72cab, source read of 4.97.0). _enforceDegree maps
  // EVERY open candidate through the resolver, the resolver called its provider
  // each time, and obligedPeers() rebuilds its Set by walking every upstream
  // and every role. One above-band pass therefore repeated the whole obligation
  // walk once per resolved channel — and three separate comments claimed the
  // opposite. The claim was the defect; the cost followed from it.
  let calls = 0;
  const provider = () => { calls++; return am.obligedPeers(); };
  const { mesh, open } = meshWith({ bindings: BIND, provider, cap: 2 });
  for (const id of BIND.keys()) open(id);

  calls = 0;
  mesh._lastRetireAt = 0;
  mesh._enforceDegree();
  ok('7a. one pass over 6 resolved candidates reads the obligation set ONCE',
    calls === 1, `provider calls: ${calls}`);

  calls = 0;
  mesh._lastRetireAt = 0;
  mesh._enforceDegree();
  ok('7b. the NEXT pass reads it again — cached on the pass, never on a clock',
    calls === 1, `provider calls: ${calls}`);

  // And the cache must not outlive its pass: a duty acquired between passes is
  // seen on the next one. This is the property a time-based cache would lose.
  const live = manager();
  let n2 = 0;
  const p2 = () => { n2++; return live.obligedPeers(); };
  const m2 = meshWith({ bindings: BIND, provider: p2, cap: 5 });
  for (const id of BIND.keys()) m2.open(id);
  m2.mesh._lastRetireAt = 0; m2.mesh._enforceDegree();
  const firstGone = m2.retired[0]?.id;
  const survivor = [...m2.mesh._peers.keys()][0];
  live._upstream.set(TOPIC_A, [hexOf(BIND.get(survivor)).toLowerCase()]);
  m2.mesh._lastRetireAt = 0; m2.mesh._enforceDegree();
  ok('7c. a duty acquired after the first pass is honoured on the second',
    !m2.retired.some((r) => r.id === survivor),
    `first=${firstGone} survivor=${survivor} retired=${m2.retired.map((r) => r.id).join(',')}`);
}

console.log(`\nResult: ${n} passed, ${fail} failed`);
if (fail) process.exit(1);
