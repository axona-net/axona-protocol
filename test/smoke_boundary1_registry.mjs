// =====================================================================
// smoke_boundary1_registry.mjs — REF-1.1 S2/S3 (recut-3): the Boundary-1 (pub/sub
// + DHT control) frame-contract registry TABLE + the shadow-wrap of the 19 routed
// handlers in _registerHandlers.
//
// The tranche contract: the `frameRegistry` construction flag is DEFAULT-OFF; when
// ON the 19 handlers are shadow-wrapped to OBSERVE a decoder-certified snapshot
// beside each handler and emit a trace — never mutating, suppressing, or reordering
// a handler or its arguments. With the runtime shadow flag OFF the handler runs
// verbatim, so flag-off is byte-identical to legacy. Dispatch is NOT migrated.
//
//   T. TABLE: 20 rows (19 frames; INGESTACK signed+legacy) mint + register; the
//      row contract is COMPLETE per row (F2); the correlation model is the recut-3
//      pair algebra (F3): conversation opposite+pairing, IngressRef binds the
//      attempt, signed INGESTACK binds the exact D1 flight+incarnation+signer,
//      REPLICATE binds nothing.
//   W. WIRING: frameRegistry:false builds nothing; true builds the 20-row table
//      and wraps the handlers; INGESTACK carries the typeof-sig discriminator (F4).
//   D. DIFFERENTIAL: the SAME scripted scenario over the SAME node ids is
//      BYTE-IDENTICAL registry-off vs registry-on+flag-on (per-node delivery + root
//      cache/tomb). Flag-on emits traces; flag-OFF emits ZERO (inert wrap). D5:
//      with CERTIFIED frames flowing through the REAL handlers, the outcome is
//      STILL byte-identical AND the registry observes those real frames as BRANDED.
//   R. REAL-HANDLER CERTIFIED SWEEP (F6): a certified representative frame is
//      driven through the ACTUAL registered+wrapped handler of a live node for
//      every one of the 19 wires — forward/no-op, asynchronous, and malformed
//      (schema-invalid / handler-throw) cases — asserting branded observation.
//   E. EVALUATOR SWEEP (retained separately): the standalone reg.wrap evaluator
//      over a dummy handler across all wires + async-pass/reject/sync-throw/
//      schema-invalid, plus the unbranded no-reflection floor.
//
// Run: node test/smoke_boundary1_registry.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { buildBoundary1Registry, boundary1Rows, rowDefs } from '../src/pubsub/boundary1Registry.js';
import { setShadowEnabled } from '../src/registry/index.js';
import { certify } from '../src/registry/snapshotMint.js';
import { T } from '../src/pubsub/constants.js';

const __LOC = regionCenter('useast');
let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');

// The 19 wire types _registerHandlers registers (wireHandlers.js).
const WIRED = [T.SUB, T.UNSUB, T.PUB, T.DELIVER, T.ADOPT, T.PULLUP, T.HANDOFFACK, T.REPLAYUP,
  T.HANDOFF, T.REPLICATE, T.KILL, T.INGESTACK, T.RECEIPTPROBE, T.RECEIPTNACK, T.TOUCH, T.PULL,
  T.PULLRESP, T.ROOTBEACON, T.METRICSON];

// A schema-satisfying representative frame per wire (INGESTACK gets three).
const CERT = {
  [T.SUB]: { topicId: 'aa', subscriberId: 'bb', since: 0 },
  [T.UNSUB]: { topicId: 'aa', subscriberId: 'bb' },
  [T.PUB]: { topicId: 'aa', json: '{"m":1}', via: 'n0', ackTo: 'n0', attemptId: 'x1', flightNonce: 'fn' },
  [T.KILL]: { topicId: 'aa', kill: { msgId: 'm1', signerPubkey: 'pk' }, ackTo: 'n0', attemptId: 'x1', flightNonce: 'fn' },
  [T.DELIVER]: { topicId: 'aa', from: 'nn', msgs: [] },
  [T.ADOPT]: { topicId: 'aa', parent: 'pp', subs: [] },
  [T.PULLUP]: { topicId: 'aa', sinceHw: 0, parentId: 'pp' },
  [T.HANDOFFACK]: { topicId: 'aa', held: 1, sent: 1 },
  [T.REPLAYUP]: { topicId: 'aa', msgs: [], dels: [] },
  [T.HANDOFF]: { topicId: 'aa', from: 'nn', msgs: [], dels: [] },
  [T.REPLICATE]: { topicId: 'aa', from: 'nn', msgs: [], dels: [] },
  [T.RECEIPTPROBE]: { topicId: 'aa', msgId: 'm1', op: 'PUB' },
  [T.RECEIPTNACK]: { topicId: 'aa', msgId: 'm1', op: 'PUB', reason: 'not-held' },
  [T.TOUCH]: { topicId: 'aa' },
  [T.PULL]: { topicId: 'aa', postHash: 'ph', corrId: 'c1', requesterId: 'r1' },
  [T.PULLRESP]: { corrId: 'c1', json: null, publishTs: 0, requesterId: 'r1' },
  [T.ROOTBEACON]: { root: 'rr', topics: [], epochs: [], beaconId: 'b1', layer: 0 },
  [T.METRICSON]: { topicId: 'aa' },
};
const IA_SIGNED = { topicId: 'aa', msgId: 'm1', op: 'PUB', epoch: 1, attemptId: 'x1', ackTo: 'n0', flightNonce: 'fn', rootPub: 'rp', purpose: 'ingest', sig: 'sigstr' };
const IA_LEGACY = { topicId: 'aa', msgId: 'm1', op: 'PUB', epoch: 1 };
const certFrame = (obj) => certify(JSON.stringify(obj));

class Fabric {
  constructor({ frameRegistry = false, certifyInTransit = false } = {}) { this.nodes = new Map(); this.queue = []; this.clock = 1_000_000_000_000; this._fr = frameRegistry; this.certifyInTransit = certifyInTransit; }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      verdictsSupported: false,
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target); if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive)
        .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, frameRegistry: self._fr });
    const rec = { id: idBig, am, handlers, alive: true, got: [], dels: [] };
    am.onPubsubDelivery((_t, json, msgId) => { let o = null; try { o = JSON.parse(json); } catch {} if (o && o.deleted) rec.dels.push(o.msgId); else rec.got.push(msgId); });
    this.nodes.set(idBig, rec); return rec;
  }
  _closest(target) { let b = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; b = id; } } return b; }
  // In-transit certification (F6): mirror the production transport, which decodes
  // wire bytes and CERTIFIES the frame before handing it to the handler. Certifying
  // here means the REAL wrapped handlers observe branded frames. JSON round-trip is
  // lossless for Boundary-1 payloads (verified: none carry bigints), so the handler
  // sees the same data — branding is inert.
  _wrapPayload(payload) { return this.certifyInTransit ? certFrame(payload) : payload; }
  async settle(cap = 500000) { let i = 0; while (this.queue.length) { if (++i > cap) throw new Error('settle cap'); const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue; const h = n.handlers.get(j.type); if (h) await h(this._wrapPayload(j.payload), j.meta); } }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}

// A scripted scenario: node0 subscribes, node1 publishes two envelopes, node1 kills
// the first, everything settles. Returns a CANONICAL snapshot of observable outcome
// (per-node delivery + root cache/tomb) plus the aggregate trace list.
async function runScenario({ frameRegistry, shadowOn, certifyInTransit = false }, nodeIds, alice) {
  setShadowEnabled(shadowOn ? true : false);
  const fab = new Fabric({ frameRegistry, certifyInTransit });
  const nodes = nodeIds.map((id) => fab.addNode(id));
  const desc = { region: 'useast', owner: null, name: 'b1-reg', write: 'open' };
  const t = await deriveTopicIdBig(desc);
  nodes[0].am.pubsubSubscribe(t); await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const e1 = await buildEnvelope({ topic: desc, message: { m: 1 }, seq: 1, identity: alice, ts: fab.clock });
  const e2 = await buildEnvelope({ topic: desc, message: { m: 2 }, seq: 2, identity: alice, ts: fab.clock + 1 });
  nodes[1].am.pubsubPublish(t, JSON.stringify(e1)); await fab.settle();
  nodes[1].am.pubsubPublish(t, JSON.stringify(e2)); await fab.settle();
  const kill = await buildKill({ topicId: idHex(t), msgId: e1.msgId, seq: 3, identity: alice });
  nodes[1].am.pubsubKill(t, kill); await fab.settle();
  const rootRec = fab.nodes.get(fab._closest(t));
  const role = rootRec.am.axonRoles.get(t);
  const outcome = {
    nodes: nodes.map((n) => ({ got: [...n.got].sort(), dels: [...n.dels].sort() })),
    rootCache: (role?.cache || []).map((c) => c.msgId).sort(),
    rootTombs: [...(role?.tombstones?.keys() || [])].sort(),
  };
  const traces = [];
  for (const n of nodes) traces.push(...n.am.frameRegistryShadow().traces);
  setShadowEnabled(false);
  return { outcome, traces };
}

async function main() {
  const alice = await createAuthorIdentity();
  const nodeIds = [];
  for (let i = 0; i < 8; i++) nodeIds.push(BigInt('0x' + (await createNodeIdentity(__LOC)).id));

  // ── T. TABLE ────────────────────────────────────────────────────────────────
  const rows = boundary1Rows();
  check('T1. all 20 rows mint (19 frames; INGESTACK signed+legacy)', rows.length === 20);
  const reg = buildBoundary1Registry({ enabled: () => false });
  check('T2. registry registers all 20 rows', reg.size() === 20);
  check('T3. wiring covers every registered handler wire type', WIRED.every((w) => reg.wiring.has(w)) && reg.wiring.size === WIRED.length,
    JSON.stringify(WIRED.filter((w) => !reg.wiring.has(w))));
  const iaw = reg.wiring.get(T.INGESTACK);
  check('T4. INGESTACK wiring is type-gated on typeof sig === string (F4)',
    iaw && iaw.variantBy && iaw.variantBy.path === 'sig' && iaw.variantBy.valueType === 'string' && iaw.variantBy.whenPresent === 'signed' && iaw.variantBy.whenAbsent === 'legacy');
  // Read MINTED rows (defineRow normalizes conversation.localKey, correlation.binding, etc.).
  const byType = new Map(); for (const d of rows) byType.set(d.variant ? `${d.type}#${d.variant}` : d.type, d);

  // F2: the row contract is COMPLETE — every applicable descriptor set (value or n/a), never silently null.
  const DESC = ['topicProfile', 'eventIdScheme', 'replayCursorType', 'orderingModel', 'outcome', 'terminalOutcome', 'retry'];
  const missing = [];
  for (const r of rows) for (const d of DESC) if (r[d] == null) missing.push(`${r.type}${r.variant ? '#' + r.variant : ''}.${d}`);
  check('T5. F2: every row declares all 7 descriptors + a retry class (no silent null)', missing.length === 0, missing.join(','));
  const budgeted = rows.every((r) => r.budget && r.budget.maxLeaves != null && r.budget.maxBytes != null);
  check('T6. F2: every row declares an observation budget (maxLeaves + maxBytes)', budgeted);
  const guardsNamed = rows.every((r) => [r.authGuard, r.admissionGuard, r.placementGuard].every((g) => typeof g === 'string' && g.length > 0));
  check('T7. F2: every guard slot is an explicit value or n/a, never empty', guardsNamed);

  // F3: PUB binds the ingress ATTEMPT (IngressRef), not topicId alone.
  const pub = byType.get('pubsub:pub');
  check('T8. F3: PUB is an IngressRef binding the attempt (topicId+attemptId+flightNonce), no msgId idempotency (F5)',
    pub.correlation.kind === 'IngressRef' && pub.correlation.binding.flight.join(',') === 'topicId,attemptId,flightNonce' && pub.idempotency == null);
  // F3: signed INGESTACK is the only LegacyAuthorityRef and binds the exact flight + incarnation + proof signer.
  const iaS = byType.get('pubsub:ingestack#signed');
  check('T9. F3: signed INGESTACK binding = {flight, authority:[epoch], proofSigner:[rootPub]}, all within requires',
    iaS.correlation.kind === 'LegacyAuthorityRef'
    && iaS.correlation.binding.flight.join(',') === 'topicId,msgId,op,attemptId,ackTo,flightNonce'
    && iaS.correlation.binding.authority.join(',') === 'epoch' && iaS.correlation.binding.proofSigner.join(',') === 'rootPub'
    && ['epoch', 'attemptId', 'flightNonce', 'rootPub'].every((f) => iaS.correlation.requires.includes(f)));
  // F3: conversation is a pair algebra with an opposite + a meta-sourced return-identity leg.
  const rep = byType.get('pubsub:replayup');
  check('T10. F3: REPLAYUP is a RESPONSE to pullup; the requester identity is a meta-sourced pairing leg (routing return)',
    rep.conversation.role === 'RESPONSE' && rep.conversation.opposite === 'pubsub:pullup'
    && rep.conversation.pairing.some((p) => p.from === 'meta' && p.remote === 'parentId') && rep.conversation.localKey.join(',') === 'topicId');
  const ha = byType.get('pubsub:handoffack');
  check('T11. F3: HANDOFFACK is a RESPONSE to handoff with a meta-sourced departing-node leg',
    ha.conversation.role === 'RESPONSE' && ha.conversation.opposite === 'pubsub:handoff' && ha.conversation.pairing.some((p) => p.from === 'meta' && p.remote === 'from'));
  // F3: REPLICATE (unsigned cohort spray) binds no authority subject and no conversation.
  const replc = byType.get('pubsub:replicate');
  check('T12. F3: REPLICATE binds no authority subject and no conversation (unsigned cohort spray)',
    replc.correlation == null && replc.conversation == null);

  // ── W. WIRING (construction flag) ────────────────────────────────────────────
  const fabOff = new Fabric({ frameRegistry: false }); const nOff = fabOff.addNode(nodeIds[0]);
  check('W1. frameRegistry:false builds no registry', nOff.am.frameRegistryShadow().built === false && nOff.am.frameRegistryShadow().rows === 0);
  const fabOn = new Fabric({ frameRegistry: true }); const nOn = fabOn.addNode(nodeIds[0]);
  const sOn = nOn.am.frameRegistryShadow();
  check('W2. frameRegistry:true builds the 20-row table', sOn.built === true && sOn.rows === 20);
  check('W3. every routed handler is registered (wrapped or not)', WIRED.every((w) => typeof nOn.handlers.get(w) === 'function') && WIRED.every((w) => typeof nOff.handlers.get(w) === 'function'));

  // ── D. DIFFERENTIAL ──────────────────────────────────────────────────────────
  const base = await runScenario({ frameRegistry: false, shadowOn: false }, nodeIds, alice);
  const on = await runScenario({ frameRegistry: true, shadowOn: true }, nodeIds, alice);
  const inert = await runScenario({ frameRegistry: true, shadowOn: false }, nodeIds, alice);
  const certd = await runScenario({ frameRegistry: true, shadowOn: true, certifyInTransit: true }, nodeIds, alice);
  check('D1. registry-on+flag-on outcome is BYTE-IDENTICAL to registry-off',
    JSON.stringify(on.outcome) === JSON.stringify(base.outcome), `\n   off=${JSON.stringify(base.outcome)}\n   on =${JSON.stringify(on.outcome)}`);
  check('D2. scenario is non-trivial (a body was delivered and one was killed)',
    base.outcome.rootTombs.length === 1 && base.outcome.nodes.some((x) => x.got.length > 0));
  check('D3. flag-on (unbranded live traffic) emits traces on the pubsub+dht boundary',
    on.traces.length > 0 && on.traces.every((r) => r.boundary === 'pubsub+dht'));
  check('D4. registry-on but flag-OFF is byte-identical AND emits ZERO traces (inert wrap)',
    JSON.stringify(inert.outcome) === JSON.stringify(base.outcome) && inert.traces.length === 0);
  // D5 (F6): certified frames through the REAL handlers stay byte-identical AND are observed as BRANDED.
  const certdBranded = certd.traces.filter((r) => r.registered === true && r.verdict !== 'unobserved');
  check('D5. F6: certify-in-transit scenario is byte-identical AND the real frames are observed BRANDED (not unbranded-source)',
    JSON.stringify(certd.outcome) === JSON.stringify(base.outcome) && certdBranded.length > 0
    && certdBranded.some((r) => typeof r.type === 'string' && r.type.startsWith('pubsub:') && r.schemaOk === true),
    `\n   certd=${JSON.stringify(certd.outcome)}\n   branded=${certdBranded.length}`);

  // ── R. REAL-HANDLER CERTIFIED SWEEP across all 19 wires (F6) ──────────────────
  // Drive a certified representative frame through the ACTUAL registered+wrapped
  // handler of a live node, for every wire. The handler does its real work
  // (forward/no-op/mutate/throw); we assert the registry observed the frame as
  // BRANDED regardless of the handler's disposition. INGESTACK exercises both
  // variants. A handler that throws on a bare frame is the malformed/rejection case.
  {
    setShadowEnabled(true);
    const tr = [];
    const fab = new Fabric({ frameRegistry: true });
    const node = fab.addNode(nodeIds[0]);
    // redirect this node's registry sink into tr (rebuild with our sink)
    const drive = async (wire, frame) => {
      tr.length = 0;
      const shadow = node.am.frameRegistryShadow();
      const before = shadow.traces.length;
      const h = node.handlers.get(wire);
      let threw = false;
      try { await h(certFrame(frame), { targetId: nodeIds[0], isTerminal: true, hopCount: 1, fromId: idHex(nodeIds[1]) }); } catch { threw = true; }
      const after = node.am.frameRegistryShadow().traces;
      return { rec: after[after.length - 1], grew: after.length > before, threw };
    };
    let brandedOk = 0, sweptN = 0; const rmiss = [];
    for (const wire of WIRED) {
      const frames = wire === T.INGESTACK ? [IA_SIGNED, IA_LEGACY] : [CERT[wire]];
      for (const f of frames) {
        sweptN++;
        const { rec, grew } = await drive(wire, f);
        // branded = the registry produced an observation for a real frame (registered:true),
        // NOT the unbranded-source floor. Handler disposition (forward/throw) is irrelevant here.
        const branded = grew && rec && rec.registered === true && rec.verdict !== 'unobserved';
        if (branded) brandedOk++; else rmiss.push(`${String(wire)}:${JSON.stringify(rec)}`);
      }
    }
    check(`R1. F6: all ${sweptN} certified frames through REAL wrapped handlers (19 wires + 2 INGESTACK variants) observed BRANDED`,
      brandedOk === sweptN, `\n   ${rmiss.slice(0, 4).join('\n   ')}`);

    // R2 (F6 malformed): a certified but schema-INVALID frame through the real handler is still branded, schemaOk=false.
    tr.length = 0;
    const shadow = node.am.frameRegistryShadow();
    const before = shadow.traces.length;
    try { await node.handlers.get(T.SUB)(certFrame({ topicId: 'aa' }), { targetId: nodeIds[0], isTerminal: true, hopCount: 1, fromId: idHex(nodeIds[1]) }); } catch {}
    const after = node.am.frameRegistryShadow().traces;
    const rec = after[after.length - 1];
    check('R2. F6: schema-invalid certified frame through the real handler is branded with schemaOk=false + a schema fault',
      after.length > before && rec && rec.registered === true && rec.schemaOk === false && (rec.faults || []).some((f) => f.startsWith('schema:')));
    setShadowEnabled(false);
  }

  // ── E. STANDALONE EVALUATOR SWEEP (retained separately, per Aster) ────────────
  {
    setShadowEnabled(true);
    const tr = [];
    const reg2 = buildBoundary1Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
    const wrapFor = (wire, handler) => { const w = reg2.wiring.get(wire); return reg2.wrap(w.type, handler, w.variantBy ? { variantBy: w.variantBy } : {}); };

    // E1 — evaluator over a dummy handler: every non-variant wire observed registered+schemaOk, verdict preserved.
    let ok = 0, n = 0; const miss = [];
    for (const wire of WIRED) {
      if (wire === T.INGESTACK) continue;
      n++; tr.length = 0;
      wrapFor(wire, () => undefined).call({}, certFrame(CERT[wire]), {});
      const r = tr[0];
      if (tr.length === 1 && r.type === reg2.wiring.get(wire).type && r.registered === true && r.schemaOk === true && r.verdict === 'passed' && r.faults == null) ok++;
      else miss.push(`${String(wire)}:${JSON.stringify(r)}`);
    }
    check(`E1. standalone evaluator: all ${n} non-variant wires observed registered+schemaOk, verdict preserved`, ok === n, `\n   ${miss.join('\n   ')}`);

    // E2 — INGESTACK variant discriminator: signed / legacy / numeric-sig → signed / legacy / legacy (F4).
    const drive = (frame) => { tr.length = 0; wrapFor(T.INGESTACK, () => undefined).call({}, certFrame(frame), {}); return tr[0]; };
    const rS = drive(IA_SIGNED), rL = drive(IA_LEGACY), rN = drive({ ...IA_SIGNED, sig: 123 });
    check('E2. INGESTACK signed/legacy/numeric-sig → signed/legacy/legacy (typeof-sig gate)',
      rS.variant === 'signed' && rL.variant === 'legacy' && rN.variant === 'legacy', `\n   ${rS.variant}/${rL.variant}/${rN.variant}`);

    // E3 — async handler: the returned Promise is passed through UNTOUCHED; the sync verdict is inert 'object'
    //      (S1 F1: the generic core never awaits/observes settlement, never suppresses unhandledRejection).
    tr.length = 0;
    const pPass = Promise.resolve(7);
    const retA = wrapFor(T.SUB, () => pPass).call({}, certFrame(CERT[T.SUB]), {});
    check('E3. async handler: returned Promise passed through by identity; inert sync verdict, no settlement observation',
      retA === pPass && tr.length === 1 && tr[0].verdict === 'object');
    // E4 — a rejecting Promise return is NOT marked handled (unhandledRejection stays the caller's concern).
    tr.length = 0;
    const pRej = Promise.reject(new Error('nack'));
    const retR = wrapFor(T.SUB, () => pRej).call({}, certFrame(CERT[T.SUB]), {});
    let caught = false; try { await retR; } catch { caught = true; }
    check('E4. rejecting Promise: same object returned, verdict inert object, caller still owns the rejection', retR === pRej && caught && tr[0].verdict === 'object');

    // E5 — synchronous throw: rethrown to the caller; verdict threw.
    tr.length = 0;
    let sthrew = false; try { wrapFor(T.SUB, () => { throw new Error('boom'); }).call({}, certFrame(CERT[T.SUB]), {}); } catch { sthrew = true; }
    check('E5. sync throw: rethrown to caller AND verdict threw emitted', sthrew && tr.length === 1 && tr[0].verdict === 'threw');

    // E6 — schema-invalid: registered, schemaOk=false, handler still ran.
    tr.length = 0;
    wrapFor(T.SUB, () => undefined).call({}, certFrame({ topicId: 'aa' }), {});
    check('E6. schema-invalid certified frame: registered, schemaOk=false, schema fault, handler ran',
      tr.length === 1 && tr[0].registered === true && tr[0].schemaOk === false && (tr[0].faults || []).some((f) => f.startsWith('schema:')));

    // E7 — the unbranded floor: an uncertified LIVE frame is never reflected on.
    tr.length = 0;
    const retU = wrapFor(T.SUB, () => 'handle').call({}, { topicId: 'aa', subscriberId: 'bb' }, {});
    check('E7. uncertified live frame: handler verbatim + unbranded-source (no reflection)',
      retU === 'handle' && tr.length === 1 && tr[0].verdict === 'unobserved' && (tr[0].faults || []).includes('unbranded-source'));

    setShadowEnabled(false);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((err) => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
