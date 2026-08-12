// =====================================================================
// smoke_boundary1_registry.mjs — REF-1.1 S2/S3: the Boundary-1 (pub/sub + DHT
// control) frame-contract registry TABLE + the shadow-wrap of the 19 routed
// handlers in _registerHandlers.
//
// The tranche contract: the `frameRegistry` construction flag is DEFAULT-OFF;
// when ON the 19 handlers are shadow-wrapped to OBSERVE a decoder-certified
// snapshot beside each handler and emit a trace — never mutating, suppressing,
// or reordering a handler or its arguments. With the runtime shadow flag OFF the
// handler runs verbatim, so flag-off is byte-identical to legacy. Dispatch is NOT
// migrated: the registry is source material observed alongside the live handlers.
//
//   T. TABLE: all 20 rows (19 frames; INGESTACK = signed+legacy variants) mint,
//      register, and the wire->row wiring covers every handler _registerHandlers
//      registers. Representative row shapes match §4.3 (correlation subjects,
//      evidence↔proof pairings).
//   W. WIRING: frameRegistry:false builds no registry (built:false); true builds
//      the 20-row table and wraps the handlers.
//   D. DIFFERENTIAL: the SAME scripted scenario over the SAME node ids is
//      BYTE-IDENTICAL registry-off vs registry-on+flag-on (per-node delivery +
//      root cache/tombstone state); registry-on+flag-on emits traces; registry-on
//      but flag-OFF emits ZERO traces and stays byte-identical (inert wrap).
//   C. OBSERVATION: a decoder-certified SUB frame through the wrapped handler
//      yields a schema-validated trace (registered + schemaOk); an uncertified
//      live frame is observed as nothing (unbranded-source) — never reflected on.
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
const lc = (s) => (typeof s === 'string' ? s.toLowerCase() : s);

// The 19 wire types _registerHandlers registers (wireHandlers.js:31-49).
const WIRED = [T.SUB, T.UNSUB, T.PUB, T.DELIVER, T.ADOPT, T.PULLUP, T.HANDOFFACK, T.REPLAYUP,
  T.HANDOFF, T.REPLICATE, T.KILL, T.INGESTACK, T.RECEIPTPROBE, T.RECEIPTNACK, T.TOUCH, T.PULL,
  T.PULLRESP, T.ROOTBEACON, T.METRICSON];

class Fabric {
  constructor({ frameRegistry = false } = {}) { this.nodes = new Map(); this.queue = []; this.clock = 1_000_000_000_000; this._fr = frameRegistry; }
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
  async settle(cap = 500000) { let i = 0; while (this.queue.length) { if (++i > cap) throw new Error('settle cap'); const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue; const h = n.handlers.get(j.type); if (h) await h(j.payload, j.meta); } }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}

// A scripted scenario: node0 subscribes, node1 publishes two envelopes, node1
// kills the first, everything settles. Returns a CANONICAL snapshot of observable
// outcome (per-node delivery + root cache/tomb) plus the aggregate trace list.
async function runScenario({ frameRegistry, shadowOn }, nodeIds, alice) {
  setShadowEnabled(shadowOn ? true : false);
  const fab = new Fabric({ frameRegistry });
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
  return { outcome, traces, builtRows: nodes[0].am.frameRegistryShadow() };
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
  check('T4. INGESTACK wiring carries a sig-presence variant discriminator',
    iaw && iaw.type === 'pubsub:ingestack' && iaw.variantBy && iaw.variantBy.path === 'sig' && iaw.variantBy.whenPresent === 'signed' && iaw.variantBy.whenAbsent === 'legacy');
  const byType = new Map(); for (const d of rowDefs()) byType.set(d.variant ? `${d.type}#${d.variant}` : d.type, d);
  check('T5. PUB is an INGESTED ONE_WAY that is an IngressRef, NOT an authority ref (F3); no msgId idempotency key (F5)',
    byType.get('pubsub:pub').kind === 'ONE_WAY' && byType.get('pubsub:pub').evidence === 'INGESTED'
    && byType.get('pubsub:pub').correlation.kind === 'IngressRef' && byType.get('pubsub:pub').idempotency == null);
  check('T6. DELIVER is an OBSERVED MULTICAST with no correlation contract',
    byType.get('pubsub:deliver').kind === 'MULTICAST' && byType.get('pubsub:deliver').evidence === 'OBSERVED' && byType.get('pubsub:deliver').correlation == null);
  check('T7. ROOTBEACON is an UNSOLICITED_EVENT with no correlation (no opposite)',
    byType.get('pubsub:rootbeacon').kind === 'UNSOLICITED_EVENT' && byType.get('pubsub:rootbeacon').correlation == null && byType.get('pubsub:rootbeacon').idempotency.from.includes('beaconId'));
  check('T8. read path (PULL) is a CONVERSATION keyed by corrId, NOT an authority correlation (F3)',
    byType.get('pubsub:pull').kind === 'ONE_WAY' && byType.get('pubsub:pull').correlation == null
    && byType.get('pubsub:pull').conversation && byType.get('pubsub:pull').conversation.key.includes('corrId'));
  check('T9. INGESTACK signed vs legacy differ in owningService + sig requirement',
    byType.get('pubsub:ingestack#signed').owningService === 'WriteIngress' && byType.get('pubsub:ingestack#legacy').owningService === 'writeFlight');
  const iaS = byType.get('pubsub:ingestack#signed');
  check('T10. signed INGESTACK is the ONLY LegacyAuthorityRef and binds the full flight + proof signer (F3), naming ackProof.js under LEGACY_ROOT_V4 (F2)',
    iaS.correlation.kind === 'LegacyAuthorityRef'
    && ['topicId', 'msgId', 'op', 'attemptId', 'ackTo', 'flightNonce', 'rootPub'].every((f) => iaS.correlation.requires.includes(f))
    && iaS.authGuard === 'verifyAckProof' && iaS.capabilityRange.proofModule === 'ackProof.js' && iaS.capabilityRange.profile === 'LEGACY_ROOT_V4');
  check('T11. REPLICATE (cohort spray on an unsigned `from`) claims NO authenticated holder subject (F3); handoff pair is a conversation',
    byType.get('pubsub:replicate').correlation == null && byType.get('pubsub:replicate').conversation == null
    && byType.get('pubsub:handoff').conversation && byType.get('pubsub:handoff').conversation.key.join(',') === 'topicId,from');
  check('T12. write frames name real auth/admission guards, not `none` (F2)',
    byType.get('pubsub:pub').authGuard === 'verifyEnvelope' && byType.get('pubsub:pub').admissionGuard === 'checkFreshness+writePolicy+topicBinding'
    && byType.get('pubsub:kill').authGuard === 'verifyKill' && Array.isArray(byType.get('pubsub:pub').errorContract) && byType.get('pubsub:pub').errorContract.length > 0);

  // ── W. WIRING (construction flag) ────────────────────────────────────────────
  const fabOff = new Fabric({ frameRegistry: false }); const nOff = fabOff.addNode(nodeIds[0]);
  check('W1. frameRegistry:false builds no registry', nOff.am.frameRegistryShadow().built === false && nOff.am.frameRegistryShadow().rows === 0);
  const fabOn = new Fabric({ frameRegistry: true }); const nOn = fabOn.addNode(nodeIds[0]);
  const sOn = nOn.am.frameRegistryShadow();
  check('W2. frameRegistry:true builds the 20-row table', sOn.built === true && sOn.rows === 20);
  check('W3. every routed handler is registered (wrapped or not)', WIRED.every((w) => typeof nOn.handlers.get(w) === 'function') && WIRED.every((w) => typeof nOff.handlers.get(w) === 'function'));

  // ── D. DIFFERENTIAL ──────────────────────────────────────────────────────────
  const base = await runScenario({ frameRegistry: false, shadowOn: false }, nodeIds, alice);
  const on   = await runScenario({ frameRegistry: true,  shadowOn: true  }, nodeIds, alice);
  const inert = await runScenario({ frameRegistry: true,  shadowOn: false }, nodeIds, alice);
  check('D1. registry-on+flag-on outcome is BYTE-IDENTICAL to registry-off',
    JSON.stringify(on.outcome) === JSON.stringify(base.outcome), `\n   off=${JSON.stringify(base.outcome)}\n   on =${JSON.stringify(on.outcome)}`);
  check('D2. scenario is non-trivial (a body was delivered and one was killed)',
    base.outcome.rootTombs.length === 1 && base.outcome.nodes.some((x) => x.got.length > 0));
  check('D3. flag-on emitted traces on the pubsub+dht boundary for registered frame types',
    on.traces.length > 0 && on.traces.every((r) => r.boundary === 'pubsub+dht') && on.traces.some((r) => typeof r.type === 'string' && r.type.startsWith('pubsub:')));
  check('D4. registry-on but flag-OFF is byte-identical AND emits ZERO traces (inert wrap)',
    JSON.stringify(inert.outcome) === JSON.stringify(base.outcome) && inert.traces.length === 0);

  // ── C. OBSERVATION across ALL 19 wires (certified in-transit) ────────────────
  // F6 (Aster): the D block proves byte-identity on LIVE (unbranded) traffic —
  // that is its job. Branded observation depth is proven HERE by certifying a
  // schema-satisfying frame per wire type (as the wire decoder does in production)
  // and driving it through the actually-wrapped handler, then asserting the branded
  // verdict — plus the INGESTACK variant discriminator, an async (Promise) handler,
  // a schema-invalid frame, a rejecting handler, a throwing handler, and the
  // unbranded no-reflection floor.
  {
    setShadowEnabled(true);
    // A schema-satisfying representative frame per wire (INGESTACK has three).
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
    const iaBase = { topicId: 'aa', msgId: 'm1', op: 'PUB', epoch: 1, attemptId: 'x1', ackTo: 'n0', flightNonce: 'fn', rootPub: 'rp', purpose: 'ingest' };
    const iaSigned = { ...iaBase, sig: 'sigstr' };          // typeof sig === 'string' → signed
    const iaLegacy = { topicId: 'aa', msgId: 'm1', op: 'PUB', epoch: 1 };  // sig absent → legacy
    const iaNumSig = { ...iaBase, sig: 123 };               // sig present but numeric → legacy (F4)

    const tr = [];
    const reg2 = buildBoundary1Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
    const wrapFor = (wire, handler) => { const w = reg2.wiring.get(wire); return reg2.wrap(w.type, handler, w.variantBy ? { variantBy: w.variantBy } : {}); };
    const drive = (wire, frame) => { tr.length = 0; wrapFor(wire, () => undefined).call({}, certify(JSON.stringify(frame)), {}); return tr[0]; };

    // C1 — every non-variant wire: certified frame → registered + schemaOk, handler verdict preserved.
    let sweepOk = 0, sweepN = 0; const misses = [];
    for (const wire of WIRED) {
      if (wire === T.INGESTACK) continue;
      sweepN++;
      const r = drive(wire, CERT[wire]);
      const ok = tr.length === 1 && r.type === reg2.wiring.get(wire).type && r.registered === true && r.schemaOk === true && r.verdict === 'passed' && r.faults == null;
      if (ok) sweepOk++; else misses.push(`${String(wire)}:${JSON.stringify(r)}`);
    }
    check(`C1. certified sweep: all ${sweepN} non-variant wires observed registered+schemaOk, handler verdict preserved`, sweepOk === sweepN, `\n   ${misses.join('\n   ')}`);

    // C2 — INGESTACK variant discriminator mirrors the handler's typeof-sig gate (F4).
    const rS = drive(T.INGESTACK, iaSigned), rL = drive(T.INGESTACK, iaLegacy), rN = drive(T.INGESTACK, iaNumSig);
    check('C2. INGESTACK signed/legacy/numeric-sig select signed/legacy/legacy (typeof-sig gate)',
      rS.variant === 'signed' && rS.registered === true && rL.variant === 'legacy' && rL.registered === true && rN.variant === 'legacy',
      `\n   signed=${rS.variant} legacy=${rL.variant} numsig=${rN.variant}`);

    // C3 — a conversation frame observes conversationPresent, with NO authority correlation.
    const rPull = drive(T.PULL, CERT[T.PULL]);
    check('C3. conversation frame (PULL): conversationPresent observed, no authority correlation',
      rPull.conversationPresent === true && rPull.correlationPresent == null);
    // C4 — a write frame observes its IngressRef correlation, with NO conversation.
    const rPub = drive(T.PUB, CERT[T.PUB]);
    check('C4. write frame (PUB): IngressRef correlationPresent observed, no conversation',
      rPub.correlationPresent === true && rPub.conversationPresent == null);

    // C5 — certified-but-schema-invalid: still observed (registered), schemaOk=false, handler ran.
    tr.length = 0; wrapFor(T.SUB, () => undefined).call({}, certify(JSON.stringify({ topicId: 'aa' })), {});
    check('C5. certified but schema-invalid SUB: registered, schemaOk=false, schema fault, handler still ran',
      tr.length === 1 && tr[0].registered === true && tr[0].schemaOk === false && tr[0].verdict === 'passed' && (tr[0].faults || []).some((f) => f.startsWith('schema:')));

    // C6 — async handler: the returned Promise is passed through UNTOUCHED; the settled verdict is emitted after resolution (F1).
    tr.length = 0;
    const pPass = Promise.resolve(undefined);
    const retA = wrapFor(T.SUB, () => pPass).call({}, certify(JSON.stringify(CERT[T.SUB])), {});
    check('C6a. async handler: returned Promise passed through by identity (not awaited/rewrapped)', retA === pPass);
    check('C6b. async handler: NO synchronous verdict emitted before the Promise settles', tr.length === 0);
    await pPass; await Promise.resolve();
    check('C6c. async pass: settled verdict emitted after resolution', tr.length === 1 && tr[0].registered === true && tr[0].verdict === 'passed');

    // C7 — rejecting handler: the rejection propagates to the caller untouched; the settled verdict is `threw` (F1).
    tr.length = 0;
    const pRej = Promise.reject(new Error('nack'));
    const retR = wrapFor(T.SUB, () => pRej).call({}, certify(JSON.stringify(CERT[T.SUB])), {});
    check('C7a. async reject: caller receives the same rejected Promise', retR === pRej);
    let caught = false; try { await retR; } catch { caught = true; }
    check('C7b. async reject: caller sees the rejection', caught);
    await Promise.resolve();
    check('C7c. async reject: settled verdict is threw', tr.length === 1 && tr[0].verdict === 'threw');

    // C8 — synchronous throw: rethrown to the caller; verdict `threw`.
    tr.length = 0;
    let sthrew = false; try { wrapFor(T.SUB, () => { throw new Error('boom'); }).call({}, certify(JSON.stringify(CERT[T.SUB])), {}); } catch { sthrew = true; }
    check('C8. sync throw: rethrown to caller AND verdict threw emitted', sthrew && tr.length === 1 && tr[0].verdict === 'threw');

    // C9 — the unbranded floor: an uncertified LIVE frame is never reflected on.
    tr.length = 0;
    const retU = wrapFor(T.SUB, () => 'handle').call({}, { topicId: 'aa', subscriberId: 'bb' }, {});
    check('C9. uncertified live frame: handler verbatim + unbranded-source (no reflection)',
      retU === 'handle' && tr.length === 1 && tr[0].verdict === 'unobserved' && (tr[0].faults || []).includes('unbranded-source'));

    setShadowEnabled(false);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((err) => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
