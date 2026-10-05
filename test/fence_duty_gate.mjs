// =====================================================================
// fence_duty_gate.mjs — Hold-and-Fill v0.7 (axona-docs 95c2ff4), repair row
// 4: the duty gate. mayRetire(id) over the kernel's existing role state AND
// the queued ingest dependencies.
//
// What this fences:
//   1. REGISTRY COMPLETENESS. AxonaManager.obligationsOf(hex) names every
//      duty the design lists: upstream, principal (role.backupOf), replica,
//      seated subscriber, handoff party in flight, queued ingest dependency.
//      obligedPeers() is the same walk as a Set.
//   2. QUEUED DEPENDENCY (Aster c34f3c85 P1-B). A queued REPLICATE pins its
//      principal from enqueue, through the in-flight count, until fn() ends;
//      an overflow drop pins nothing; REPLAY_UP declares none; the pin
//      transfers to the installed role in the same synchronous step as the
//      dequeue (no await between shift and the start of fn).
//   2b. CONCURRENCY (Aster 01bb6555 R4-A): in-flight is a count per
//      dependency; an inline fn awaiting beside a pumped one keeps both
//      pinned in either completion order; identical dependencies stay
//      pinned until the last use completes; a null REPLAY_UP overlapping
//      changes nothing.
//   2c. THE REAL PATH (R4-B): am._onReplicate → _ingestEnqueue → _syncIngest
//      → becomeBackup, inline and through the actual q.shift(), with the
//      pin probed inside becomeBackup at the install and at an injected
//      await placed before the install; a NEGATIVE CONTROL with the pin
//      removed shows the probe sees the gap (75cd61d8); overflow installs no
//      role and pins no dependency.
//   3. CALLER DECLARATION. Every _ingestEnqueue call site in src passes a
//      second argument (static read of the source).
//   4. mayRetire(id): ok with no duty; refused with the duty named; fails
//      closed when the reader throws; ok with no manager.
//   5. THE GRACE CLOSE ASKS FIRST. With the admission gate armed and a
//      closeGraceMs, a refused candidate that this node owes a duty to is
//      NOT closed when the grace timer fires (refuse-grace-blocked), its
//      channel stays open, and the timer re-arms; once the duty is gone the
//      next fire closes it. The swap victim path refuses the swap when the
//      victim carries a duty (swap-blocked).
//
// With mayRetire deleted (or its call sites), 4 and 5 fail; with the
// dependency argument dropped from _onReplicate, 2 and 3 fail.
//
// Run: node test/fence_duty_gate.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { Synapse }                  from '../src/dht/Synapse.js';
import { AxonaManager }             from '../src/pubsub/AxonaManager.js';
import { makeRole }                 from '../src/pubsub/rootClaim.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex, clz264 }   from '../src/utils/hexid.js';
import { depositDispatchCapability } from '../src/registry/index.js';
import { INGEST_QUEUE_MAX }         from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const H = (b) => b.toString(16).padStart(2, '0').repeat(33);   // a lowercase 66-hex id from one byte

// A bare AxonaManager with a stub dht, as test/fence_mesh_obligations.mjs builds it.
function manager() {
  const dht = {
    verdictsSupported: true,
    routeMessage: async () => ({ consumed: false }),
    getSelfId: () => H(0x01),
    onRoutedMessage() {}, onDirectMessage() {},
    neighbors: () => [], bridgeId: () => null,
    isTransit: () => false, isIntroduction: () => false, introductionIds: () => [],
    findKClosest: async () => [],
  };
  depositDispatchCapability(dht, { routed: () => {} });
  const m = new AxonaManager({ dht });
  m._log = () => {};
  return m;
}

// ── 1. registry completeness, on a bare AxonaManager ──────────────────
console.log('fence_duty_gate: row 4 — the duty gate');
{
  const am = manager();
  const topicA = 0x1111n, topicB = 0x2222n, topicC = 0x3333n;
  const UP = H(0x10), PRINCIPAL = H(0x20), REPLICA = H(0x30), SUB = H(0x40), HEIR = H(0x50), ALT = H(0x51), QUEUED = H(0x60), NOBODY = H(0x70);
  am._upstream.set(topicA, [UP]);
  const rb = makeRole(topicB, false, Date.now()); rb.backupOf = PRINCIPAL; am.axonRoles.set(topicB, rb);
  const rc = makeRole(topicC, true, Date.now());
  rc.replicas = new Map([[REPLICA, { at: Date.now() }]]);
  rc.subscribers = new Map([[SUB, { at: Date.now() }]]);
  am.axonRoles.set(topicC, rc);
  am._handoffInFlight = [{ heir: fromHex(HEIR), alt: fromHex(ALT) }];
  am._ingestQueue = [{ fn: async () => {}, dep: QUEUED }];
  const set = am.obligedPeers();
  check('1 upstream',    am.obligationsOf(UP).includes('upstream') && set.has(UP));
  check('1 principal',   am.obligationsOf(PRINCIPAL).includes('principal') && set.has(PRINCIPAL));
  check('1 replica',     am.obligationsOf(REPLICA).includes('replica') && set.has(REPLICA));
  check('1 subscriber',  am.obligationsOf(SUB).includes('subscriber') && set.has(SUB));
  check('1 handoff heir and alt (BigInt in the job, hex in the registry)', am.obligationsOf(HEIR).includes('handoff') && am.obligationsOf(ALT).includes('handoff') && set.has(HEIR) && set.has(ALT));
  check('1 queued ingest dependency', am.obligationsOf(QUEUED).includes('queued-ingest') && set.has(QUEUED));
  check('1 nobody owes nothing', am.obligationsOf(NOBODY).length === 0 && !set.has(NOBODY));
  check('1 case-insensitive input', am.obligationsOf(UP.toUpperCase()).includes('upstream'));
  am._handoffInFlight = null; am._ingestQueue = [];
}

// ── 2. queued dependency through _ingestEnqueue ───────────────────────
{
  const am = manager();
  const DEP = H(0x61);
  // force the queued path: mark inline active so the first enqueue queues
  am._ingestInlineActive = true;
  let started = 0, release;
  const gate = new Promise(r => { release = r; });
  const p = am._ingestEnqueue(async () => { started++; await gate; }, DEP);
  check('2 queued: dependency pinned while in the queue', am.queuedDependencies().has(DEP) && am.obligationsOf(DEP).includes('queued-ingest'));
  am._ingestInlineActive = false;
  // the pump started synchronously at enqueue (not pumping before) — it is
  // waiting on gate now; the pin moved to the in-flight slot
  await wait(0);
  check('2 in flight: pinned while fn() runs (no gap at the dequeue)', started === 1 && am.queuedDependencies().has(DEP));
  release(); await p; await am._ingestIdle();
  check('2 done: pin released after fn() ends', !am.queuedDependencies().has(DEP));
  // inline path pins while running too
  let inlineSeen = false;
  await am._ingestEnqueue(async () => { inlineSeen = am.queuedDependencies().has(DEP); }, DEP);
  check('2 inline: pinned during the inline run, released after', inlineSeen && !am.queuedDependencies().has(DEP));
  // null declares nothing
  await am._ingestEnqueue(async () => {}, null);
  check('2 null dependency pins nothing', am.queuedDependencies().size === 0);
  // overflow drop pins nothing
  // hold the pump so the queue fills to its bound without draining
  am._ingestInlineActive = true; am._ingestPumping = true;
  const big = [];
  for (let i = 0; i < INGEST_QUEUE_MAX; i++) big.push(am._ingestEnqueue(async () => {}, H(0x62)));
  const dropped = am._ingestDropped || 0;
  const DROPPED = H(0x63);
  await am._ingestEnqueue(async () => {}, DROPPED);
  check('2 overflow: the dropped payload pins nothing', (am._ingestDropped || 0) > dropped && !am.queuedDependencies().has(DROPPED) && am.queuedDependencies().has(H(0x62)), `dropped=${am._ingestDropped} q=${am._ingestQueue.length}`);
  am._ingestInlineActive = false; am._ingestPumping = true; am._ingestPump(); await Promise.all(big); await am._ingestIdle();
  check('2 overflow: after draining, nothing pinned', am.queuedDependencies().size === 0);
}

// ── 2b. concurrency (Aster 01bb6555 R4-A): in-flight is a count per dependency
{
  const am = manager();
  const A = H(0x64), B = H(0x65);
  let relA, relB; const gA = new Promise(r => { relA = r; }); const gB = new Promise(r => { relB = r; });
  // inline A, awaiting
  const pA = am._ingestEnqueue(async () => { await gA; }, A);
  check('2b inline A awaiting: [A]', am.queuedDependencies().has(A) && !am.queuedDependencies().has(B));
  // B arrives while A awaits → queued → pump starts and runs B concurrently
  const pB = am._ingestEnqueue(async () => { await gB; }, B);
  await wait(0);
  const s1 = am.queuedDependencies();
  check('2b B in flight beside A: BOTH pinned', s1.has(A) && s1.has(B), JSON.stringify([...s1]));
  relA(); await pA; await wait(0);
  const s2 = am.queuedDependencies();
  check('2b A done, B still awaiting: B pinned, A released', !s2.has(A) && s2.has(B), JSON.stringify([...s2]));
  relB(); await pB; await am._ingestIdle();
  check('2b both done: empty', am.queuedDependencies().size === 0);
  // the other completion order
  let relA2, relB2; const gA2 = new Promise(r => { relA2 = r; }); const gB2 = new Promise(r => { relB2 = r; });
  const pA2 = am._ingestEnqueue(async () => { await gA2; }, A);
  const pB2 = am._ingestEnqueue(async () => { await gB2; }, B);
  await wait(0);
  relB2(); await pB2; await wait(0);
  const s3 = am.queuedDependencies();
  check('2b B done first, A still awaiting: A pinned, B released', s3.has(A) && !s3.has(B), JSON.stringify([...s3]));
  relA2(); await pA2; await am._ingestIdle();
  // identical dependencies: pinned until the LAST use completes
  let relX, relY; const gX = new Promise(r => { relX = r; }); const gY = new Promise(r => { relY = r; });
  const pX = am._ingestEnqueue(async () => { await gX; }, A);
  const pY = am._ingestEnqueue(async () => { await gY; }, A);
  await wait(0);
  relX(); await pX; await wait(0);
  check('2b identical deps: first use done, still pinned by the second', am.queuedDependencies().has(A));
  relY(); await pY; await am._ingestIdle();
  check('2b identical deps: last use done, released', !am.queuedDependencies().has(A));
  // a null (REPLAY_UP) overlapping a pinned one changes nothing
  let relN, relC; const gN = new Promise(r => { relN = r; }); const gC = new Promise(r => { relC = r; });
  const pC = am._ingestEnqueue(async () => { await gC; }, A);
  const pN = am._ingestEnqueue(async () => { await gN; }, null);
  await wait(0);
  check('2b null REPLAY_UP overlapping: A pinned, nothing else', am.queuedDependencies().size === 1 && am.queuedDependencies().has(A));
  relN(); await pN; await wait(0);
  check('2b null done: A still pinned', am.queuedDependencies().has(A));
  relC(); await pC; await am._ingestIdle();
  check('2b all done: empty', am.queuedDependencies().size === 0);
}

// ── 2c. the REAL path (Aster 01bb6555 R4-B): _onReplicate → _ingestEnqueue →
//        _syncIngest → becomeBackup, inline and queued, with the pin probed at
//        the install and at an injected pre-install await.
{
  const am = manager();
  am._rootReplicas = true;                       // backup duty enabled on this node
  const PRINCIPAL = H(0x66);
  const topicHex = H(0x0a);                      // a topic id this node does not root
  const payload = { topicId: topicHex, from: PRINCIPAL, msgs: [], dels: [] };
  const meta = { targetId: am.nodeId, fromId: null };
  // probe inside becomeBackup: the registry must still name the principal at the install
  const rc = am._rootClaim; const origBecome = rc.becomeBackup.bind(rc);
  const atInstall = [];
  rc.becomeBackup = (t, role, from) => { atInstall.push({ pinned: am.queuedDependencies().has(PRINCIPAL), kinds: am.obligationsOf(PRINCIPAL) }); return origBecome(t, role, from); };
  // INLINE path (queue empty, no inline active)
  await am._onReplicate(payload, meta);
  await am._ingestIdle();
  const roleBig = BigInt('0x' + topicHex);
  check('2c inline: becomeBackup installed backupOf = principal', am.axonRoles.get(roleBig)?.backupOf === PRINCIPAL);
  check('2c inline: at the install the principal was pinned (queued-ingest) — the union never dropped it', atInstall.length === 1 && atInstall[0].pinned === true && atInstall[0].kinds.includes('queued-ingest'), JSON.stringify(atInstall));
  check('2c inline: after processing, protected by the installed role (principal)', am.obligationsOf(PRINCIPAL).includes('principal') && !am.queuedDependencies().has(PRINCIPAL));
  // QUEUED path on a second topic: force the queue by holding the pump
  const topic2 = H(0x0b); const payload2 = { ...payload, topicId: topic2 };
  atInstall.length = 0;
  am._ingestInlineActive = true; am._ingestPumping = true;      // the entry queues
  const p2 = am._onReplicate(payload2, meta);
  check('2c queued: pinned while in the queue, no role yet', am.queuedDependencies().has(PRINCIPAL) && am.axonRoles.get(BigInt('0x' + topic2)) === undefined);
  // the principal is ALREADY protected by topic-1's installed role; remove it to isolate the queue pin
  am.axonRoles.delete(roleBig);
  check('2c queued: with the installed role gone, the queue pin alone still names the principal', am.obligationsOf(PRINCIPAL).join() === 'queued-ingest');
  am._ingestInlineActive = false; am._ingestPumping = true; am._ingestPump();   // the actual shift
  await p2; await am._ingestIdle();
  check('2c queued: installed through the actual q.shift() path', am.axonRoles.get(BigInt('0x' + topic2))?.backupOf === PRINCIPAL);
  check('2c queued: at the install the principal was pinned', atInstall.length === 1 && atInstall[0].pinned === true, JSON.stringify(atInstall));
  // INJECTED AWAIT before the install: the pin must cover the gap
  const topic3 = H(0x0c); const payload3 = { ...payload, topicId: topic3 };
  am.axonRoles.delete(BigInt('0x' + topic2));
  const origIngest = am._syncIngest.bind(am);
  const duringGap = [];
  am._syncIngest = async (pl, mt, pol) => { await wait(5); duringGap.push(am.obligationsOf(PRINCIPAL)); return origIngest(pl, mt, pol); };
  atInstall.length = 0;
  await am._onReplicate(payload3, meta); await am._ingestIdle();
  check('2c injected await before the install: the union still named the principal during the gap', duringGap.length === 1 && duringGap[0].includes('queued-ingest'), JSON.stringify(duringGap));
  check('2c injected await: install still pinned, role installed', atInstall[0]?.pinned === true && am.axonRoles.get(BigInt('0x' + topic3))?.backupOf === PRINCIPAL);
  am._syncIngest = origIngest; rc.becomeBackup = origBecome;
  // NEGATIVE CONTROL (Aster 75cd61d8): remove BOTH protections before the
  // install (no in-flight pin, no queued entry since inline runs it) and
  // keep the injected await: the probe must now see the principal
  // UNPROTECTED during the gap. This shows the probes detect the gap the
  // accounting closes; it is the fence's teeth, not the design's behaviour.
  const topic5 = H(0x0e); const payload5 = { ...payload, topicId: topic5 };
  am.axonRoles.delete(BigInt('0x' + topic3));
  const origPin = am._pinInFlight; am._pinInFlight = () => {};           // the defect, reintroduced
  const gapBroken = [];
  am._syncIngest = async (pl, mt, pol) => { await wait(5); gapBroken.push(am.obligationsOf(PRINCIPAL)); return origIngest(pl, mt, pol); };
  await am._onReplicate(payload5, meta); await am._ingestIdle();
  check('2c NEGATIVE CONTROL: with the pin removed the probe sees the principal unprotected during the gap', gapBroken.length === 1 && gapBroken[0].length === 0, JSON.stringify(gapBroken));
  check('2c NEGATIVE CONTROL: the role still installs afterwards (so the gap, not the install, is what the pin closes)', am.axonRoles.get(BigInt('0x' + topic5))?.backupOf === PRINCIPAL);
  am._pinInFlight = origPin; am._syncIngest = origIngest;
  am.axonRoles.delete(BigInt('0x' + topic5));
  // OVERFLOW admits no dependency and installs no role
  const topic4 = H(0x0d); const payload4 = { ...payload, topicId: topic4, from: H(0x67) };
  am._ingestInlineActive = true; am._ingestPumping = true;
  const held = []; for (let i = 0; i < INGEST_QUEUE_MAX; i++) held.push(am._ingestEnqueue(async () => {}, null));
  const before = am._ingestDropped || 0;
  await am._onReplicate(payload4, meta);
  check('2c overflow: dropped, no dependency pinned, no role installed', (am._ingestDropped || 0) === before + 1 && !am.queuedDependencies().has(H(0x67)) && am.axonRoles.get(BigInt('0x' + topic4)) === undefined);
  am._ingestInlineActive = false; am._ingestPumping = true; am._ingestPump(); await Promise.all(held); await am._ingestIdle();
  check('2c overflow: after draining, still no role for the dropped topic', am.axonRoles.get(BigInt('0x' + topic4)) === undefined);
}

// ── 3. caller declaration (static) ────────────────────────────────────
{
  const src = readFileSync(new URL('../src/pubsub/wireHandlers.js', import.meta.url), 'utf8');
  const calls = [...src.matchAll(/_ingestEnqueue\(([^;]*?)\);/g)].map(m => m[1]);
  check('3 exactly two _ingestEnqueue callers in wireHandlers', calls.length === 2, String(calls.length));
  check('3 every caller passes a dependency argument', calls.every(c => /,\s*(dep|null)\s*\)?\s*$/.test(c)), JSON.stringify(calls));
  const others = [...readFileSync(new URL('../src/pubsub/repairPlane.js', import.meta.url), 'utf8').matchAll(/this\._ingestEnqueue\(/g)].length;
  check('3 no undeclared caller in repairPlane', others === 0);
}

// ── 4. mayRetire on a peer ────────────────────────────────────────────
async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}
function craft(peerRec, xorSeed, weight = 0.5) {
  const id = peerRec.big ^ xorSeed;
  const syn = new Synapse({ peerId: id, latencyMs: 50, stratum: clz264(peerRec.big ^ id) });
  syn.weight = weight; syn.inertia = 0; syn._addedBy = 'crafted';
  peerRec.node.synaptome.set(id, syn);
  return id;
}
// As test/smoke_seed_admission_gate.mjs: mint a REAL peer whose id lands in
// the wanted XOR band relative to `rec` (geo-derived ids need a spread of
// coordinates to reach the top bands).
const groupOf = (rec, big, domain) => Math.min(domain.STRATA_GROUPS - 1, clz264(rec.big ^ big) >>> 2);
async function mintInGroup(net, domain, rec, wantGroups, maxTries = 400) {
  for (let i = 0; i < maxTries; i++) {
    const p = await makePeer(net, domain, (i * 13) % 80 - 40, (i * 29) % 340 - 170, {});
    if (wantGroups.includes(groupOf(rec, p.big, domain))) return p;
    await p.peer.stop().catch(() => {}); await p.transport.stop?.();
  }
  return null;
}
(async () => {
  const net = new SimNetwork(); const domain = new AxonaDomain();
  {
    const a = await makePeer(net, domain, 1, 1, {});
    const X = H(0x77);
    check('4 no manager yet → ok (no pubsub, no duties)', a.peer._axonaManager === null && a.peer.mayRetire(X).ok === true);
    a.peer._requireAxonaManager('fence');          // the default manager a first pub/sub call would build
    check('4 no duty → ok', a.peer.mayRetire(X).ok === true && a.peer.mayRetire(fromHex(X)).ok === true);
    const am = a.peer._axonaManager;
    check('4 setup: peer has a manager', !!am && typeof am.obligationsOf === 'function');
    am._upstream.set(0x4444n, [X]);
    const r = a.peer.mayRetire(fromHex(X));
    check('4 duty → refused with the duty named', r.ok === false && r.duty === 'upstream', JSON.stringify(r));
    am._upstream.delete(0x4444n);
    const orig = am.obligationsOf; am.obligationsOf = () => { throw new Error('boom'); };
    check('4 reader throws → fail closed', a.peer.mayRetire(X).ok === false && /reader-threw/.test(a.peer.mayRetire(X).duty));
    am.obligationsOf = orig;
    const saved = a.peer._axonaManager; a.peer._axonaManager = null;
    check('4 no manager → ok (no pubsub, no duties)', a.peer.mayRetire(X).ok === true);
    a.peer._axonaManager = saved;
    await a.peer.stop().catch(() => {}); await a.transport.stop().catch(() => {});
  }

  // ── 5. the grace close asks first (case 46's shape) ─────────────────
  {
    const a = await makePeer(net, domain, 5, 5, { admissionGate: { kNear: 5, sparseFloor: 2, closeGraceMs: 60 } });
    a.peer._requireAxonaManager('fence');
    a.node._maxSynaptome = 11;
    // crafted table exactly at cap (as smoke_seed_admission_gate: 5 kNear, 2 sparse, 4 dense)
    [1n, 2n, 3n, 4n, 5n].forEach(x => craft(a, x));
    craft(a, 1n << 259n); craft(a, 1n << 258n);
    const dense = [craft(a, (1n << 263n) + 7n, 0.9), craft(a, (1n << 262n) + 3n, 0.9), craft(a, (1n << 261n) + 9n, 0.05), craft(a, (1n << 260n) + 5n, 0.9)];
    check('5 setup: at cap', a.node.synaptome.size === 11);
    // a real candidate in the dense band → refused → grace timer
    const dense0 = await mintInGroup(net, domain, a, [0]);
    check('5 setup: dense-band candidate minted', dense0 !== null);
    // make the candidate a DUTY before it binds: this node backs up a topic whose principal is the candidate
    const am = a.peer._axonaManager;
    const rb = makeRole(0x5555n, false, Date.now()); rb.backupOf = dense0.hex.toLowerCase(); am.axonRoles.set(0x5555n, rb);
    const logs = [];
    const origLog = a.peer._emitLog.bind(a.peer);
    a.peer._emitLog = (lvl, msg, ctx) => { logs.push([msg, ctx]); return origLog(lvl, msg, ctx); };
    await dense0.transport.openConnection(a.hex); await wait(20);
    check('5 refused at cap (not in the table)', !a.node.synaptome.has(dense0.big) && a.node.synaptome.size === 11);
    check('5 grace pending for the candidate', a.peer._gracePending.has(dense0.big));
    await wait(90);   // first grace fire
    check('5 the grace fire was BLOCKED by the duty: channel still open, logged', logs.some(([m, c]) => m === 'refuse-grace-blocked' && c?.duty === 'principal') && a.transport.isConnected(dense0.hex), JSON.stringify(logs.filter(([m]) => /grace/.test(m))));
    check('5 re-armed: still pending', a.peer._gracePending.has(dense0.big));
    // the duty ends: the next fire closes
    am.axonRoles.delete(0x5555n);
    await wait(90);
    check('5 after the duty ends, the next fire closes the channel', !a.transport.isConnected(dense0.hex) && !a.peer._gracePending.has(dense0.big));
    // swap victim carrying a duty: the swap is refused
    const victim = dense[2];   // lowest vitality dense
    const rv = makeRole(0x6666n, false, Date.now()); rv.backupOf = toHex(victim).toLowerCase(); am.axonRoles.set(0x6666n, rv);
    const improver = await mintInGroup(net, domain, a, [1, 2]);
    check('5 setup: improver minted', improver !== null);
    logs.length = 0;
    await improver.transport.openConnection(a.hex); await wait(30);
    check('5 swap refused: victim with a duty stays, improver not admitted, swap-blocked logged', a.node.synaptome.has(victim) && !a.node.synaptome.has(improver.big) && logs.some(([m]) => m === 'swap-blocked'), JSON.stringify(logs.map(([m]) => m)));
    await a.peer.stop().catch(() => {}); await a.transport.stop().catch(() => {});
    await dense0.peer.stop().catch(() => {}); await dense0.transport.stop().catch(() => {});
    await improver.peer.stop().catch(() => {}); await improver.transport.stop().catch(() => {});
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
