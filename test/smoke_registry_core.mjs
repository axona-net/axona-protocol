// smoke_registry_core.mjs — REF-1.1 S1..S1d: the shadow registry CORE in
// isolation, re-cut around a declarative side-effect-free boundary and hardened
// across three adversarial review rounds (Aster S1/S1b/S1c). Reproduces every
// probe those reviews raised. No kernel wiring here (that is S2).
// Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind, ShadowRegistry, setShadowEnabled } from '../src/pubsub/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };

const V = { min: 4, max: 4 };
const pubRow = () => defineRow({
  type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
  evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
  projection: { payload: ['topicId'] },
  schema: (p) => (p && typeof p.topicId === 'string') ? { ok: true } : { ok: false, code: 'missing-topic' },
  correlation: (p) => (p && p.topicId) ? { kind: 'IngressRef' } : null,
  correlationFields: ['topicId'], subjectShape: CorrelationSubjectKind.IngressRef,
  idempotencyKey: (p) => p && p.topicId,
  errorContract: ['missing-topic'],
});
let _on = false;
const mk = (sink, extra = {}) => { const r = new ShadowRegistry({ boundary: 'test', sink, enabled: () => _on, ...extra }); r.register(pubRow()); return r; };

// ── 1. flag OFF verbatim + variadic; flag ON verdict + declared facts ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = false;
  let sawThis = null, sawArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { sawThis = this; sawArgs = a; return 'consumed'; });
  const ctx = {}; const r = w.call(ctx, { topicId: 'aa' }, {}, 9);
  ok('1a off: verbatim + this + all args', r === 'consumed' && sawThis === ctx && sawArgs.length === 3 && sawArgs[2] === 9);
  ok('1b off: no trace', tr.length === 0);
  _on = true; const r2 = reg.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {});
  ok('1c on: verdict verbatim + one trace with declared facts', r2 === 'consumed' && tr.length === 1 && tr[0].kind === 'ONE_WAY' && tr[0].verdict === 'consumed' && tr[0].owningService === 'WriteIngress');
}

// ── 2. declarative variant discriminator getter is NEVER invoked (Aster S1c#1) ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'pubsub:ingestack', variant: 'signed', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V, projection: { payload: ['topicId'] } }));
  _on = true;
  let getterCalls = 0;
  const live = {}; Object.defineProperty(live, 'sig', { enumerable: true, get() { getterCalls++; return 'x'; } });
  live.topicId = 'aa';
  let handlerSawSig = 'untouched';
  const w = reg.wrap('pubsub:ingestack', function (p) { try { handlerSawSig = Object.getOwnPropertyDescriptor(p, 'sig'); } catch {} return 'consumed'; },
    { variantBy: { path: 'sig', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } });
  const r = w.call({}, live, {});
  ok('2a discriminator accessor never invoked', getterCalls === 0, `calls=${getterCalls}`);
  ok('2b handler still ran verbatim', r === 'consumed');
  ok('2c accessor discriminator recorded as a variant fault (no contract)', tr[0].registered === false && (tr[0].faults || []).some(f => f.startsWith('variant-')));
}

// ── 3. accessor-backed PROJECTION field never invoked; live frame untouched (Aster S1c#1) ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V, projection: { payload: ['danger'] }, schema: () => ({ ok: true }) }));
  _on = true;
  let projGetter = 0;
  const live = { topicId: 'aa' };
  Object.defineProperty(live, 'danger', { enumerable: true, get() { projGetter++; return 'boom'; } });
  reg.wrap('pubsub:pub', () => 'consumed').call({}, live, {});
  ok('3a projected accessor never invoked', projGetter === 0, `calls=${projGetter}`);
  ok('3b recorded as projection-accessor fault', (tr[0].faults || []).includes('projection-accessor'));
}

// ── 4. return inspection is PRIMITIVE-ONLY — no getter/thenable invoked (Aster S1c#2) ──
{
  const reg = mk(() => {}); _on = true;
  let thenCalls = 0, consumedCalls = 0, spyCalls = 0;
  const evil = {};
  Object.defineProperty(evil, 'then', { get() { thenCalls++; return () => {}; } });
  Object.defineProperty(evil, 'consumed', { get() { consumedCalls++; return true; } });
  Object.defineProperty(evil, 'spy', { get() { spyCalls++; return 1; } });
  let r, threw = null; try { r = reg.wrap('pubsub:pub', () => evil).call({}, { topicId: 'aa' }, {}); } catch (e) { threw = e; }
  ok('4a returned-object getters never invoked', thenCalls === 0 && consumedCalls === 0 && spyCalls === 0);
  ok('4b value verbatim, verdict opaque object', threw === null && r === evil);
  // custom thenable
  let customThen = 0; const thenable = { then() { customThen++; } };
  const r2 = reg.wrap('pubsub:pub', () => thenable).call({}, { topicId: 'aa' }, {});
  ok('4c custom thenable.then never called by observation', customThen === 0 && r2 === thenable);
}

// ── 5. telemetry emits CODES only — no schema reason / exception / dynamic text (Aster S1c#3) ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V, projection: { payload: ['topicId'] }, schema: () => ({ ok: false, code: 'missing-topic', reason: 'SECRET-REASON' }), errorContract: ['missing-topic'] }));
  _on = true;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {});
  const blob1 = JSON.stringify(tr[0]);
  ok('5a schema code allowlisted, reason text NOT emitted', tr[0].schemaCode === 'missing-topic' && !blob1.includes('SECRET-REASON'));
  tr.length = 0;
  let caught = null; try { reg.wrap('pubsub:pub', () => { throw new Error('SECRET-EXCEPTION'); }).call({}, { topicId: 'aa' }, {}); } catch (e) { caught = e; }
  const blob2 = JSON.stringify(tr[0]);
  ok('5b handler exception rethrown, message NOT emitted', caught && caught.message === 'SECRET-EXCEPTION' && tr[0].verdict === 'threw' && !blob2.includes('SECRET-EXCEPTION'));
}

// ── 6. nested-map keys cannot collide (Aster S1c#4) ──
{
  const reg = new ShadowRegistry({ boundary: 't' });
  reg.register(defineRow({ type: 'a#b', kind: FrameKind.ONE_WAY, owningService: 'WRONG', versionRange: V }));
  reg.register(defineRow({ type: 'a', variant: 'b', kind: FrameKind.ONE_WAY, owningService: 'RIGHT', versionRange: V }));
  ok('6a (type a, variant b) resolves its own contract, not a#b', reg.row('a', 'b').owningService === 'RIGHT');
  ok('6b type a#b base resolves independently', reg.row('a#b').owningService === 'WRONG');
}

// ── 7. defineRow strict validation (Aster S1c#5) ──
{
  const rej = (label, fn) => { let e = null; try { fn(); } catch (x) { e = x; } ok(label, e !== null); };
  const F = (o) => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, ...o });
  rej('7a >24 projection fields rejected (not truncated)', () => F({ projection: { payload: Array.from({ length: 25 }, (_, i) => 'f' + i) } }));
  rej('7b non-object budget rejected', () => F({ budget: 'x' }));
  rej('7c non-object capabilityRange rejected', () => F({ capabilityRange: 'x' }));
  rej('7d oversized capability string rejected', () => F({ capabilityRange: { k: 'Z'.repeat(9999) } }));
  rej('7e non-finite capability number rejected', () => F({ capabilityRange: { k: Infinity } }));
  rej('7f non-string note rejected', () => F({ note: 123 }));
  rej('7g evidence/proof contradiction rejected', () => F({ evidence: EvidenceLevel.OBSERVED, proves: Proves.ROUTING }));
  rej('7h correlationField not in projection rejected', () => F({ projection: { payload: ['topicId'] }, correlation: () => ({}), correlationFields: ['ghost'], subjectShape: CorrelationSubjectKind.IngressRef }));
}

// ── 8. projection is schema-FAITHFUL: exact scalar within budget, structural
//      facts for array/bytes/object, oversized → budget fault not truncation (Aster S1c#6) ──
{
  const seen = {};
  const reg = new ShadowRegistry({ boundary: 't', sink: () => {}, enabled: () => true });
  reg.register(defineRow({
    type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
    budget: { maxBytes: 8 },
    projection: { payload: ['small', 'big', 'arr', 'bytes', 'nest.leaf'] },
    schema: (p) => { Object.assign(seen, p); return { ok: true }; },
  }));
  _on = true;
  const tr = []; reg._sink = (r) => tr.push(r);
  reg.wrap('pubsub:pub', () => 'consumed').call({}, {
    small: 'ok', big: 'X'.repeat(50), arr: [1, 2, 3], bytes: new Uint8Array(9), nest: { leaf: 'deep' },
  }, {});
  ok('8a exact scalar within budget', seen.small === 'ok');
  ok('8b oversized scalar → NOT a truncated value in facts', seen.big === undefined);
  ok('8c oversized scalar → projection-budget fault', (tr[0].faults || []).includes('projection-budget'));
  ok('8d array → structural fact {arr,len}', seen.arr && seen.arr.k === 'arr' && seen.arr.len === 3);
  ok('8e bytes → structural fact {bytes,len}', seen.bytes && seen.bytes.k === 'bytes' && seen.bytes.len === 9);
  ok('8f nested leaf path → exact scalar', seen['nest.leaf'] === 'deep');
}

// ── 9. contained faults + non-forgeable brand + duplicate ──
{
  _on = true;
  const rbf = new ShadowRegistry({ boundary: 't', enabled: () => { throw new Error('flag'); } }); rbf.register(pubRow());
  let r, t = null; try { r = rbf.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {}); } catch (e) { t = e; }
  ok('9a throwing enabled() → pass-through', t === null && r === 'consumed');
  const reg = new ShadowRegistry({ boundary: 't' });
  let e1 = null; try { reg.register(Object.freeze({ type: 'x', variant: null, kind: 'ONE_WAY', owningService: 'X' })); } catch (x) { e1 = x; }
  ok('9b forged (unbranded) row rejected', e1 !== null);
  reg.register(pubRow()); let e2 = null; try { reg.register(pubRow()); } catch (x) { e2 = x; }
  ok('9c duplicate registration rejected', e2 !== null);
}

// ── 10. declarative variant selection picks the right registered row ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'pubsub:ingestack', variant: 'signed', kind: FrameKind.ONE_WAY, owningService: 'SIGNED-D1', versionRange: V, projection: { payload: ['sig'] } }));
  reg.register(defineRow({ type: 'pubsub:ingestack', variant: 'legacy-unsigned', kind: FrameKind.ONE_WAY, owningService: 'LEGACY', versionRange: V, projection: { payload: ['sig'] } }));
  _on = true;
  const w = reg.wrap('pubsub:ingestack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } });
  w.call({}, { sig: 'deadbeef' }, {});
  ok('10a present discriminator → signed row', tr[0].owningService === 'SIGNED-D1' && tr[0].variant === 'signed');
  tr.length = 0; w.call({}, { }, {});
  ok('10b absent discriminator → legacy row', tr[0].owningService === 'LEGACY' && tr[0].variant === 'legacy-unsigned');
}

// ── 11. sampling: clean 1-of-N, faults always ──
{
  const tr = []; const reg = mk((r) => tr.push(r), { sampleEvery: 3 }); _on = true;
  const w = reg.wrap('pubsub:pub', () => 'consumed');
  for (let i = 0; i < 6; i++) w.call({}, { topicId: 'aa' }, {});
  ok('11a clean sampled 1-of-3', tr.length === 2, `${tr.length}`);
  const before = tr.length;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, {}, {});   // schema-invalid → fault
  ok('11b fault bypasses sampling', tr.length === before + 1 && tr[tr.length - 1].schemaOk === false);
}

_on = false; setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
