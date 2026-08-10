// smoke_registry_core.mjs — REF-1.1 S1..S1e: the shadow registry CORE in
// isolation. S1e re-cuts the observation boundary around DECLARATIVE DATA — no
// row-supplied code runs in the dispatch thread — per Aster's S1d disposition.
// This gate reproduces every failure that disposition demonstrated:
//   no row declaration can execute user code, mutate dispatch-visible state,
//   inspect undeclared live data, exceed work/byte budgets, select variants from
//   undeclared fields, alter object prototypes, or bypass collection caps.
// Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind, FactType, ShadowRegistry, setShadowEnabled } from '../src/pubsub/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const rej = (m, fn) => { let e = null; try { fn(); } catch (x) { e = x; } ok(m, e !== null); };
const V = { min: 4, max: 4 };
let _on = false;

const pubRow = () => defineRow({
  type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
  evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
  projection: { payload: ['topicId'] },
  schema: { require: ['topicId'], types: { topicId: 'string' } },
  correlation: { kind: 'IngressRef', requires: ['topicId'] },
  idempotency: { from: ['topicId'] },
});
const mk = (sink, extra = {}) => { const r = new ShadowRegistry({ boundary: 'test', sink, enabled: () => _on, ...extra }); r.register(pubRow()); return r; };

// ── 1. flag OFF verbatim + variadic + this; flag ON verdict + declared facts ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = false;
  let sawThis = null, sawArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { sawThis = this; sawArgs = a; return 'consumed'; });
  const ctx = {}; const r = w.call(ctx, { topicId: 'aa' }, {}, 9);
  ok('1a off: verbatim + this + all args', r === 'consumed' && sawThis === ctx && sawArgs.length === 3 && sawArgs[2] === 9);
  ok('1b off: no trace', tr.length === 0);
  _on = true; const r2 = reg.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {});
  ok('1c on: verbatim verdict + trace with declared facts', r2 === 'consumed' && tr.length === 1 && tr[0].kind === 'ONE_WAY' && tr[0].verdict === 'consumed' && tr[0].schemaOk === true && tr[0].correlationPresent === true && tr[0].idempotencyPresent === true);
}

// ── 2. NO row code — schema/correlation/idempotency must be data, not functions (Aster S1d #1) ──
{
  const F = (o) => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['x'] }, ...o });
  rej('2a function schema rejected', () => F({ schema: () => ({ ok: true }) }));
  rej('2b function correlation rejected', () => F({ correlation: () => ({}) }));
  rej('2c function idempotency rejected', () => F({ idempotency: () => 'k' }));
  rej('2d legacy idempotencyKey callback rejected', () => F({ idempotencyKey: () => 'k' }));
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['x', 'y'] }, schema: { require: ['x'], forbid: ['y'], types: { x: 'string' } } }));
  _on = true;
  const w = reg.wrap('q', () => 'consumed');
  w.call({}, { x: 'ok' }, {});
  ok('2e schema require+forbid satisfied → ok', tr[0].schemaOk === true);
  tr.length = 0; w.call({}, { y: 'present' }, {});
  ok('2f missing required → fixed code', tr[0].schemaOk === false && tr[0].schemaCode === 'missing-required');
  tr.length = 0; w.call({}, { x: 'ok', y: 'nope' }, {});
  ok('2g forbidden present → fixed code', tr[0].schemaOk === false && tr[0].schemaCode === 'forbidden-present');
  tr.length = 0; w.call({}, { x: 12345 }, {});
  ok('2h type mismatch → fixed code', tr[0].schemaOk === false && tr[0].schemaCode === 'type-mismatch');
}

// ── 3. shadow observation cannot mutate what the handler sees (Aster S1d #1,#2) ──
{
  const reg = mk(() => {}); _on = true;
  const live = { topicId: 'aa', arr: [1, 2, 3] };
  let handlerSaw = null;
  reg.wrap('pubsub:pub', (p) => { handlerSaw = p; return 'consumed'; }).call({}, live, {});
  ok('3a live payload identity + contents unchanged by observation', handlerSaw === live && live.arr.length === 3 && live.topicId === 'aa');
  const tr = []; const reg2 = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg2.register(defineRow({ type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V, projection: { payload: ['danger'] } }));
  let getterCalls = 0; const p2 = { topicId: 'aa' };
  Object.defineProperty(p2, 'danger', { enumerable: true, get() { getterCalls++; return 'boom'; } });
  reg2.wrap('pubsub:pub', () => 'consumed').call({}, p2, {});
  ok('3b projected accessor never invoked, recorded as fault', getterCalls === 0 && (tr[0].faults || []).includes('projection-accessor'));
}

// ── 4. never enumerate a live object; never read undeclared fields (Aster S1d #3) ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = true;
  let ownKeys = 0; const queried = [];
  const target = { topicId: 'aa', secret: 'leak' };
  const proxy = new Proxy(target, {
    ownKeys(t) { ownKeys++; return Reflect.ownKeys(t); },
    getOwnPropertyDescriptor(t, k) { queried.push(k); return Reflect.getOwnPropertyDescriptor(t, k); },
  });
  reg.wrap('pubsub:pub', () => 'consumed').call({}, proxy, {});
  ok('4a live object never enumerated (no ownKeys/Proxy trap)', ownKeys === 0, `ownKeys=${ownKeys}`);
  ok('4b only the declared path was inspected, never "secret"', queried.includes('topicId') && !queried.includes('secret'));
  const tr2 = []; const reg2 = mk((r) => tr2.push(r));
  const exotic = Object.create({ inheritedTopic: 'x' });
  let read = false;
  Object.defineProperty(exotic, 'topicId', { enumerable: true, get() { read = true; return 'aa'; } });
  reg2.wrap('pubsub:pub', () => 'consumed').call({}, exotic, {});
  ok('4c non-plain root → source fault, no field read', read === false && (tr2[0].faults || []).includes('projection-source'));
}

// ── 5. budgets are enforced (Aster S1d #4) ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'w', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxWork: 1 }, projection: { payload: ['a', 'b'] }, correlation: { kind: 'IngressRef', requires: ['b'] } }));
  _on = true;
  reg.wrap('w', () => 'consumed').call({}, { a: 'x', b: 'y' }, {});
  ok('5a maxWork=1 → 2nd field unread (correlation on it absent) + work fault', tr[0].correlationPresent === false && (tr[0].faults || []).includes('projection-work'));
  const tr2 = []; const reg2 = new ShadowRegistry({ boundary: 't', sink: (r) => tr2.push(r), enabled: () => true });
  reg2.register(defineRow({ type: 'b', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxBytes: 4 }, projection: { payload: ['s'] }, correlation: { kind: 'IngressRef', requires: ['s'] } }));
  const wb = reg2.wrap('b', () => 'consumed');
  wb.call({}, { s: 'ééé' }, {});   // 3 UTF-16 units but 6 UTF-8 bytes
  ok('5b "ééé" exceeds a 4-byte cap → budget fault, value not projected', tr2[0].correlationPresent === false && (tr2[0].faults || []).includes('projection-budget'));
  tr2.length = 0; wb.call({}, { s: 'abcd' }, {});   // 4 ASCII = 4 bytes
  ok('5c "abcd" within a 4-byte cap → projected', tr2[0].correlationPresent === true && !(tr2[0].faults || []).includes('projection-budget'));
  const tr3 = []; const reg3 = new ShadowRegistry({ boundary: 't', sink: (r) => tr3.push(r), enabled: () => true });
  reg3.register(defineRow({ type: 'g', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['big'] }, schema: { types: { big: 'bigint' } } }));
  const huge = BigInt('9'.repeat(4000));
  reg3.wrap('g', () => 'consumed').call({}, { big: huge }, {});
  ok('5d huge bigint → structural fact, matches bigint type, no unbounded conversion', tr3[0].schemaOk === true);
}

// ── 6. variant selection is bound to the declared projection (Aster S1d #5) ──
{
  const reg = new ShadowRegistry({ boundary: 't', enabled: () => true });
  reg.register(defineRow({ type: 'ack', variant: 'signed', kind: FrameKind.ONE_WAY, owningService: 'D1', versionRange: V, projection: { payload: ['sig'] } }));
  reg.register(defineRow({ type: 'ack', variant: 'legacy-unsigned', kind: FrameKind.ONE_WAY, owningService: 'LEGACY', versionRange: V, projection: { payload: ['sig'] } }));
  rej('6a variantBy.path not in projection → wrap throws', () => reg.wrap('ack', () => 'consumed', { variantBy: { path: 'undeclared', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } }));
  rej('6b variantBy result not a registered variant → wrap throws', () => reg.wrap('ack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'ghost', whenAbsent: 'legacy-unsigned' } }));
  const tr = []; reg._sink = (r) => tr.push(r); _on = true;
  const w = reg.wrap('ack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } });
  w.call({}, { sig: 'deadbeef' }, {});
  ok('6c present discriminator → signed row', tr[0].owningService === 'D1' && tr[0].variant === 'signed');
  tr.length = 0; w.call({}, {}, {});
  ok('6d absent discriminator → legacy row', tr[0].owningService === 'LEGACY' && tr[0].variant === 'legacy-unsigned');
  tr.length = 0; let g = 0; const live = {}; Object.defineProperty(live, 'sig', { enumerable: true, get() { g++; return 'x'; } });
  w.call({}, live, {});
  ok('6e discriminator accessor never invoked → variant fault', g === 0 && (tr[0].faults || []).some((f) => f.startsWith('variant-')));
}

// ── 7. prototypes & containers hardened (Aster S1d #6) ──
{
  const F = (o) => defineRow({ type: 'p', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, ...o });
  rej('7a projection path "__proto__" rejected', () => F({ projection: { payload: ['__proto__'] } }));
  rej('7b nested "a.__proto__" rejected', () => F({ projection: { payload: ['a.__proto__'] } }));
  rej('7c "constructor" segment rejected', () => F({ projection: { payload: ['constructor'] } }));
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(pubRow()); _on = true;
  const evil = JSON.parse('{"topicId":"aa","__proto__":{"polluted":true}}');
  reg.wrap('pubsub:pub', () => 'consumed').call({}, evil, {});
  ok('7d __proto__ payload key cannot pollute (base object clean)', ({}).polluted === undefined && tr.length === 1);
  const reg2 = new ShadowRegistry({ boundary: 't' });
  reg2.register(defineRow({ type: 'a#b', kind: FrameKind.ONE_WAY, owningService: 'BASE-AhashB', versionRange: V }));
  reg2.register(defineRow({ type: 'a', variant: 'b', kind: FrameKind.ONE_WAY, owningService: 'VARIANT-b', versionRange: V }));
  ok('7e (type a, variant b) ≠ base row typed a#b (Symbol keying)', reg2.row('a', 'b').owningService === 'VARIANT-b' && reg2.row('a#b').owningService === 'BASE-AhashB');
}

// ── 8. every declared collection is capped (Aster S1d #7) ──
{
  const F = (o) => defineRow({ type: 'c', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, ...o });
  const many = (n2) => Object.fromEntries(Array.from({ length: n2 }, (_, i) => ['k' + i, 1]));
  rej('8a capabilityRange >16 keys rejected', () => F({ capabilityRange: many(17) }));
  rej('8b errorContract >16 entries rejected', () => F({ errorContract: Array.from({ length: 17 }, (_, i) => 'e' + i) }));
  rej('8c traceFields >16 entries rejected', () => F({ traceFields: Array.from({ length: 17 }, (_, i) => 't' + i) }));
  rej('8d >24 projection fields rejected', () => F({ projection: { payload: Array.from({ length: 25 }, (_, i) => 'f' + i) } }));
  const reg = new ShadowRegistry({ boundary: 't' });
  reg.register(defineRow({ type: 'vc', variant: 'x', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['d'] } }));
  const cases = Object.fromEntries(Array.from({ length: 33 }, (_, i) => ['c' + i, 'x']));
  rej('8e variantBy.cases >32 rejected', () => reg.wrap('vc', () => 'consumed', { variantBy: { path: 'd', cases, default: 'x' } }));
}

// ── 9. return inspection primitive-only; telemetry fixed-codes; contained faults ──
{
  const reg = mk(() => {}); _on = true;
  let thenG = 0, consG = 0;
  const evil = {};
  Object.defineProperty(evil, 'then', { get() { thenG++; return () => {}; } });
  Object.defineProperty(evil, 'consumed', { get() { consG++; return true; } });
  const r = reg.wrap('pubsub:pub', () => evil).call({}, { topicId: 'aa' }, {});
  ok('9a returned-object getters never invoked; opaque verdict', thenG === 0 && consG === 0 && r === evil);
  const tr = []; const reg2 = mk((x) => tr.push(x));
  let caught = null; try { reg2.wrap('pubsub:pub', () => { throw new Error('SECRET-EXCEPTION'); }).call({}, { topicId: 'aa' }, {}); } catch (e) { caught = e; }
  ok('9b handler exception rethrown, message NOT in trace', caught && caught.message === 'SECRET-EXCEPTION' && tr[0].verdict === 'threw' && !JSON.stringify(tr[0]).includes('SECRET-EXCEPTION'));
  const rbf = new ShadowRegistry({ boundary: 't', enabled: () => { throw new Error('flag'); } }); rbf.register(pubRow());
  let t = null, rr; try { rr = rbf.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {}); } catch (e) { t = e; }
  ok('9c throwing enabled() → verbatim pass-through', t === null && rr === 'consumed');
  const reg3 = new ShadowRegistry({ boundary: 't' });
  rej('9d forged unbranded row rejected', () => reg3.register(Object.freeze({ type: 'x', variant: null, kind: 'ONE_WAY' })));
  reg3.register(pubRow());
  rej('9e duplicate registration rejected', () => reg3.register(pubRow()));
}

// ── 10. sampling: clean 1-of-N, faults always ──
{
  const tr = []; const reg = mk((r) => tr.push(r), { sampleEvery: 3 }); _on = true;
  const w = reg.wrap('pubsub:pub', () => 'consumed');
  for (let i = 0; i < 6; i++) w.call({}, { topicId: 'aa' }, {});
  ok('10a clean sampled 1-of-3', tr.length === 2, `${tr.length}`);
  const before = tr.length;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, {}, {});   // schema-invalid → fault
  ok('10b fault bypasses sampling', tr.length === before + 1 && tr[tr.length - 1].schemaOk === false);
}

// ── 11. retained S1d validation gates ──
{
  rej('11a OBSERVED + proves:routing rejected', () => defineRow({ type: 'z', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, evidence: EvidenceLevel.OBSERVED, proves: Proves.ROUTING }));
  rej('11b correlation path not in projection rejected', () => defineRow({ type: 'z', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['a'] }, correlation: { kind: CorrelationSubjectKind.IngressRef, requires: ['ghost'] } }));
  rej('11c schema path not in projection rejected', () => defineRow({ type: 'z', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['a'] }, schema: { require: ['b'] } }));
  ok('11d FactType export present', FactType && FactType.bigint === 'bigint' && FactType.present === 'present');
}

_on = false; setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
