// smoke_registry_core.mjs — REF-1.1 S1/S1b/S1c: the shadow registry CORE in
// isolation, hardened across two adversarial review rounds. Reproduces and
// closes every probe from Aster's S1 and S1b dispositions. No kernel wiring
// here (that is S2). Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind, ShadowRegistry, setShadowEnabled } from '../src/pubsub/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };

const V = { min: 4, max: 4 };
const pubRow = () => defineRow({
  type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
  evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
  projection: { payload: ['topicId'] },
  schema: (p) => (p && typeof p.topicId === 'string') ? { ok: true } : { ok: false, reason: 'missing topicId' },
  correlation: (p) => ({ kind: 'IngressRef', topicId: p && p.topicId }),
  correlationFields: ['topicId'], subjectShape: CorrelationSubjectKind.IngressRef,
  idempotencyKey: (p) => p && p.topicId,
});
let _on = false;
const mk = (sink, extra = {}) => { const r = new ShadowRegistry({ boundary: 'test', sink, enabled: () => _on, ...extra }); r.register(pubRow()); return r; };

// ── 1. flag OFF: verbatim pass-through, this, ALL args, no trace ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = false;
  let sawThis = null, sawArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { sawThis = this; sawArgs = a; return 'consumed'; });
  const ctx = {}; const r = w.call(ctx, { topicId: 'aa' }, { fromId: 'bb' }, 3, 4);
  ok('1a returns verbatim', r === 'consumed'); ok('1b preserves this', sawThis === ctx);
  ok('1c forwards ALL args', sawArgs.length === 4 && sawArgs[3] === 4); ok('1d no trace', traces.length === 0);
}

// ── 2. flag ON: variadic + verdict + declared trace facts ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = true;
  let sawArgs = null;
  const r = reg.wrap('pubsub:pub', function (...a) { sawArgs = a; return 'consumed'; }).call({}, { topicId: 'aa' }, {}, 'x');
  ok('2a forwards ALL args', sawArgs.length === 3 && sawArgs[2] === 'x');
  ok('2b verdict verbatim', r === 'consumed');
  ok('2c trace facts', traces.length === 1 && traces[0].kind === 'ONE_WAY' && traces[0].owningService === 'WriteIngress' && traces[0].verdict === 'consumed');
}

// ── 3. OBSERVATION ISOLATION (Aster S1b#1): callbacks get a bounded frozen
//      projection; they cannot mutate live input, and non-scalar fields are not
//      even exposed (no shared graph on any path). ──
{
  const traces = []; const reg = new ShadowRegistry({ boundary: 'test', sink: (r) => traces.push(r), enabled: () => true });
  reg.register(defineRow({
    type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
    projection: { payload: ['topicId', 'nested'] },
    schema: (p) => { try { p.topicId = 'HACK'; } catch {} return { ok: true }; },     // frozen → throws, contained
    correlation: (p) => { try { if (p.nested) p.nested.deep = 'HACK'; } catch {} return { kind: 'IngressRef' }; },
    correlationFields: ['topicId'], subjectShape: CorrelationSubjectKind.IngressRef,
  }));
  _on = true;
  const live = { topicId: 'aa', nested: { deep: 'orig' } };
  let sawTopic = null;
  reg.wrap('pubsub:pub', function (p) { sawTopic = p.topicId; return 'consumed'; }).call({}, live, {});
  ok('3a handler saw original topicId (projection is disjoint + frozen)', sawTopic === 'aa', sawTopic);
  ok('3b live top-level field untouched', live.topicId === 'aa');
  ok('3c live NESTED object untouched (non-scalars never exposed)', live.nested.deep === 'orig', live.nested.deep);
}

// ── 4. contained faults: throwing enabled()/sink never reach the handler ──
{
  _on = true;
  const rbf = new ShadowRegistry({ boundary: 't', enabled: () => { throw new Error('flag'); } }); rbf.register(pubRow());
  let r, t = null; try { r = rbf.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {}); } catch (e) { t = e; }
  ok('4a throwing enabled() → pass-through', t === null && r === 'consumed');
  const rbs = new ShadowRegistry({ boundary: 't', sink: () => { throw new Error('sink'); }, enabled: () => true }); rbs.register(pubRow());
  let r2, t2 = null; try { r2 = rbs.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {}); } catch (e) { t2 = e; }
  ok('4b throwing sink → verdict intact', t2 === null && r2 === 'consumed');
}

// ── 5. throwing `then` getter contained; value verbatim (Aster S1b#2a) ──
{
  const reg = mk(() => {}); _on = true;
  const evil = {}; Object.defineProperty(evil, 'then', { get() { throw new Error('then'); } });
  let r, t = null; try { r = reg.wrap('pubsub:pub', () => evil).call({}, { topicId: 'aa' }, {}); } catch (e) { t = e; }
  ok('5 throwing then-getter contained, value verbatim', t === null && r === evil);
}

// ── 6. throwing `consumed` getter contained (Aster S1b#2) ──
{
  const reg = mk(() => {}); _on = true;
  const evil = {}; Object.defineProperty(evil, 'consumed', { get() { throw new Error('consumed getter'); } });
  let r, t = null; try { r = reg.wrap('pubsub:pub', () => evil).call({}, { topicId: 'aa' }, {}); } catch (e) { t = e; }
  ok('6 throwing consumed-getter contained, value verbatim', t === null && r === evil, t && t.message);
}

// ── 7. invalid selector coercion (Aster S1b#3): non-string selector output must
//      NOT be coerced; resolves no contract; handler still runs ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = true;
  const evilVariant = { toString() { throw new Error('coercion'); } };
  let r, t = null;
  try { r = reg.wrap('pubsub:pub', () => 'consumed', { select: () => evilVariant }).call({}, { topicId: 'aa' }, {}); } catch (e) { t = e; }
  ok('7a non-string selector output does not coerce/escape', t === null && r === 'consumed', t && t.message);
  ok('7b selector fault recorded, no contract resolved', traces[0].registered === false && !!traces[0].selectorFault && traces[0].kind === null);
}

// ── 8. forged brand rejected (Aster S1b#4): a frozen raw object is not a row ──
{
  const reg = new ShadowRegistry({ boundary: 't' });
  const forged = Object.freeze({ type: 'pubsub:pub', variant: null, kind: 'ONE_WAY', owningService: 'X' });
  let e = null; try { reg.register(forged); } catch (x) { e = x; }
  ok('8 forged (unbranded) row rejected', e !== null);
}

// ── 9. duplicate registration + unknown variant (no fallback) ──
{
  const reg = new ShadowRegistry({ boundary: 't' }); reg.register(pubRow());
  let e = null; try { reg.register(pubRow()); } catch (x) { e = x; }
  ok('9a duplicate key rejected', e !== null);
  const traces = []; const reg2 = new ShadowRegistry({ boundary: 't', sink: (r) => traces.push(r), enabled: () => true }); reg2.register(pubRow()); _on = true;
  const r = reg2.wrap('pubsub:pub', () => 'consumed', { select: () => 'ghost' }).call({}, { topicId: 'aa' }, {});
  ok('9b unknown variant: handler runs, no base fallback', r === 'consumed' && traces[0].registered === false && !!traces[0].variantFault);
}

// ── 10. defineRow strict validation (Aster S1b#6) ──
{
  const rej = (label, fn) => { let e = null; try { fn(); } catch (x) { e = x; } ok(label, e !== null); };
  rej('10a missing versionRange rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S' }));
  rej('10b reversed versionRange rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: { min: 5, max: 4 } }));
  rej('10c non-string guard rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, authGuard: 7 }));
  rej('10d string-where-array (correlationFields) rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, correlation: () => ({}), correlationFields: 'topicId', subjectShape: CorrelationSubjectKind.IngressRef }));
  rej('10e negative budget rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxBytes: -1 } }));
  rej('10f non-unique correlationFields rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, correlation: () => ({}), correlationFields: ['a', 'a'], subjectShape: CorrelationSubjectKind.IngressRef }));
  rej('10g REQUEST_RESPONSE without correlation rejected', () => defineRow({ type: 'q', kind: FrameKind.REQUEST_RESPONSE, owningService: 'S', versionRange: V }));
  rej('10h COMMITTED without producedPolicy rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, evidence: EvidenceLevel.COMMITTED }));
}

// ── 11. telemetry bounded + declared (Aster S1b#5): variant clamped, correlation
//       kind fixed to declared subjectShape (not callback), no raw payload ──
{
  const traces = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => traces.push(r), enabled: () => true });
  reg.register(defineRow({
    type: 'pubsub:pub', variant: 'v', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
    projection: { payload: ['topicId'] },
    correlation: () => ({ kind: 'SECRET-IN-KIND', topicId: 'x' }),   // callback tries to control kind
    correlationFields: ['topicId'], subjectShape: CorrelationSubjectKind.IngressRef,
  }));
  _on = true;
  const bigVariant = 'V'.repeat(10000);
  reg.wrap('pubsub:pub', () => 'consumed', { select: () => bigVariant }).call({}, { topicId: 'SECRET-topic' }, {});
  const t = traces[0]; const blob = JSON.stringify(t);
  // An oversized selector output is REJECTED at the selector (no coercion, no
  // contract) — it never reaches the trace as a variant at all.
  ok('11a oversized selector output never enters trace as a variant', t.variant === null && blob.length < 2000, `varlen=${blob.length}`);
  ok('11b oversized selector output resolves no contract (selector fault)', t.registered === false && !!t.selectorFault);
  ok('11c no raw payload value in trace', !blob.includes('SECRET-topic'));
  // now a registered call to check declared correlationKind wins over callback
  traces.length = 0;
  reg.wrap('pubsub:pub', () => 'consumed', { select: () => 'v' }).call({}, { topicId: 'aa' }, {});
  ok('11d correlation kind = DECLARED subjectShape, not callback output', traces[0].correlationKind === 'IngressRef' && !JSON.stringify(traces[0]).includes('SECRET-IN-KIND'));
}

// ── 12. sampling: clean 1-of-N, faults always emit ──
{
  const traces = []; const reg = mk((r) => traces.push(r), { sampleEvery: 3 }); _on = true;
  const w = reg.wrap('pubsub:pub', () => 'consumed');
  for (let i = 0; i < 6; i++) w.call({}, { topicId: 'aa' }, {});
  ok('12a clean traces sampled (1 of 3)', traces.length === 2, `${traces.length}`);
  const before = traces.length;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, {}, {});   // schema-invalid → fault
  ok('12b fault bypasses sampling', traces.length === before + 1 && traces[traces.length - 1].schemaOk === false);
}

_on = false; setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
