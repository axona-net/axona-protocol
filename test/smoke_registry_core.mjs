// smoke_registry_core.mjs — REF-1.1 S1..S1f: the shadow registry CORE in
// isolation. S1f closes Aster's S1e disposition: the shadow layer NEVER reflects
// on a live handler argument (reflection is itself Proxy-trap-capable). It reads
// ONLY a decoder-branded snapshot; anything unbranded (any Proxy) is observed as
// nothing and the handler runs verbatim. This gate reproduces every failure the
// S1e disposition demonstrated.
// Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind, FactType, ShadowRegistry, setShadowEnabled, snapshot } from '../src/pubsub/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const rej = (m, fn) => { let e = null; try { fn(); } catch (x) { e = x; } ok(m, e !== null); };
const V = { min: 4, max: 4 };
const S = (o) => snapshot(o);   // brand a decoder-produced frame
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

// ── 1. flag OFF verbatim; flag ON observes a BRANDED snapshot ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = false;
  let sawThis = null, sawArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { sawThis = this; sawArgs = a; return 'consumed'; });
  const ctx = {}; const r = w.call(ctx, S({ topicId: 'aa' }), {}, 9);
  ok('1a off: verbatim + this + all args', r === 'consumed' && sawThis === ctx && sawArgs.length === 3 && sawArgs[2] === 9);
  ok('1b off: no trace', tr.length === 0);
  _on = true; const r2 = reg.wrap('pubsub:pub', () => 'consumed').call({}, S({ topicId: 'aa' }), {});
  ok('1c on: verbatim verdict + declared facts from snapshot', r2 === 'consumed' && tr.length === 1 && tr[0].kind === 'ONE_WAY' && tr[0].verdict === 'consumed' && tr[0].schemaOk === true && tr[0].correlationPresent === true && tr[0].idempotencyPresent === true);
}

// ── 2. NO row code — schema/correlation/idempotency are data, evaluated to fixed codes ──
{
  const F = (o) => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['x'] }, ...o });
  rej('2a function schema rejected', () => F({ schema: () => ({ ok: true }) }));
  rej('2b function correlation rejected', () => F({ correlation: () => ({}) }));
  rej('2c function idempotency rejected', () => F({ idempotency: () => 'k' }));
  rej('2d legacy idempotencyKey callback rejected', () => F({ idempotencyKey: () => 'k' }));
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['x', 'y'] }, schema: { require: ['x'], forbid: ['y'], types: { x: 'string' } } }));
  _on = true; const w = reg.wrap('q', () => 'consumed');
  w.call({}, S({ x: 'ok' }), {}); ok('2e require+forbid satisfied → ok', tr[0].schemaOk === true);
  tr.length = 0; w.call({}, S({ y: 'present' }), {}); ok('2f missing required → fixed code', tr[0].schemaOk === false && tr[0].schemaCode === 'missing-required');
  tr.length = 0; w.call({}, S({ x: 'ok', y: 'nope' }), {}); ok('2g forbidden present → fixed code', tr[0].schemaOk === false && tr[0].schemaCode === 'forbidden-present');
  tr.length = 0; w.call({}, S({ x: 12345 }), {}); ok('2h type mismatch → fixed code', tr[0].schemaOk === false && tr[0].schemaCode === 'type-mismatch');
}

// ── 3. LIVE-INPUT NONINTERFERENCE — no reflection on unbranded args (Aster S1e #1,#2) ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = true;
  // 3a: root Proxy with side-effecting traps, UNBRANDED → never reflected on
  const target = { topicId: 'aa' };
  let gpo = 0, gopd = 0, ownK = 0, ran = 0, sawArg = null;
  const proxy = new Proxy(target, {
    getPrototypeOf(t) { gpo++; target.topicId = 'mutated-by-getPrototypeOf'; return Object.getPrototypeOf(t); },
    getOwnPropertyDescriptor(t, k) { gopd++; return Reflect.getOwnPropertyDescriptor(t, k); },
    ownKeys(t) { ownK++; return Reflect.ownKeys(t); },
  });
  reg.wrap('pubsub:pub', (p) => { ran++; sawArg = p; return 'consumed'; }).call({}, proxy, {});
  ok('3a root Proxy never reflected on; no trap fired; payload unmutated', gpo === 0 && gopd === 0 && ownK === 0 && target.topicId === 'aa');
  ok('3b handler ran verbatim with the same arg; trace is unbranded-source', ran === 1 && sawArg === proxy && tr[0].verdict === 'unobserved' && (tr[0].faults || []).includes('unbranded-source'));
  // 3c: throwing getPrototypeOf must NOT suppress the handler
  let ran2 = 0, escaped = null;
  const thrower = new Proxy({ topicId: 'aa' }, { getPrototypeOf() { throw new Error('trap-explode'); } });
  try { reg.wrap('pubsub:pub', () => { ran2++; return 'consumed'; }).call({}, thrower, {}); } catch (e) { escaped = e; }
  ok('3c throwing root trap → handler ran once, no observer exception escaped', ran2 === 1 && escaped === null);
  // 3d: proxied array as payload, unbranded → get/length trap never fired
  let lenGet = 0, ran3 = 0;
  const parr = new Proxy([1, 2, 3], { get(t, k) { if (k === 'length') lenGet++; return Reflect.get(t, k); } });
  reg.wrap('pubsub:pub', () => { ran3++; return 'consumed'; }).call({}, parr, {});
  ok('3d proxied array unbranded → no length trap, handler verbatim', lenGet === 0 && ran3 === 1);
  // 3e: revoked Proxy unbranded → no throw from the shadow layer, handler runs
  const { proxy: rp, revoke } = Proxy.revocable({ topicId: 'aa' }, {}); revoke();
  let ran4 = 0, esc2 = null;
  try { reg.wrap('pubsub:pub', () => { ran4++; return 'consumed'; }).call({}, rp, {}); } catch (e) { esc2 = e; }
  ok('3e revoked Proxy unbranded → contained, handler ran once', ran4 === 1 && esc2 === null);
  // 3f: a branded snapshot IS read — but accessors on it are still never invoked
  const tr2 = []; const reg2 = new ShadowRegistry({ boundary: 't', sink: (r) => tr2.push(r), enabled: () => true });
  reg2.register(defineRow({ type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V, projection: { payload: ['danger'] } }));
  let g = 0; const snap = S({ topicId: 'aa' });
  Object.defineProperty(snap, 'danger', { enumerable: true, get() { g++; return 'x'; } });
  reg2.wrap('pubsub:pub', () => 'consumed').call({}, snap, {});
  ok('3f accessor on a snapshot is never invoked → accessor fault', g === 0 && (tr2[0].faults || []).includes('projection-accessor'));
}

// ── 4. budgets (Aster S1e #4) + fixed UTF-8 surrogate accounting (Aster S1e #3) ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'w', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxLeaves: 1 }, projection: { payload: ['a', 'b'] }, correlation: { kind: 'IngressRef', requires: ['b'] } }));
  _on = true;
  reg.wrap('w', () => 'consumed').call({}, S({ a: 'x', b: 'y' }), {});
  ok('4a maxLeaves=1 → 2nd field unread + leaves fault', tr[0].correlationPresent === false && (tr[0].faults || []).includes('projection-leaves'));
  const trb = []; const regb = new ShadowRegistry({ boundary: 't', sink: (r) => trb.push(r), enabled: () => true });
  regb.register(defineRow({ type: 'b', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxBytes: 4 }, projection: { payload: ['s'] }, correlation: { kind: 'IngressRef', requires: ['s'] } }));
  const wb = regb.wrap('b', () => 'consumed');
  wb.call({}, S({ s: 'ééé' }), {}); ok('4b "ééé" (6 UTF-8 bytes) > 4 → budget fault', trb[0].correlationPresent === false && (trb[0].faults || []).includes('projection-budget'));
  trb.length = 0; wb.call({}, S({ s: 'abcd' }), {}); ok('4c "abcd" (4 bytes) within cap → projected', trb[0].correlationPresent === true);
  trb.length = 0; wb.call({}, S({ s: '\uD800é' }), {}); ok('4d lone high surrogate + é (5 bytes) → budget fault (not 4)', trb[0].correlationPresent === false && (trb[0].faults || []).includes('projection-budget'));
  trb.length = 0; wb.call({}, S({ s: '𝄞' }), {}); ok('4e valid surrogate pair (4 bytes) within cap → projected', trb[0].correlationPresent === true);
  const trg = []; const regg = new ShadowRegistry({ boundary: 't', sink: (r) => trg.push(r), enabled: () => true });
  regg.register(defineRow({ type: 'g', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['big'] }, schema: { types: { big: 'bigint' } } }));
  regg.wrap('g', () => 'consumed').call({}, S({ big: BigInt('9'.repeat(4000)) }), {});
  ok('4f huge bigint → structural fact, no unbounded conversion', trg[0].schemaOk === true);
}

// ── 5. variant selection bound to the projection; discriminator read on snapshot ──
{
  const reg = new ShadowRegistry({ boundary: 't', enabled: () => true });
  reg.register(defineRow({ type: 'ack', variant: 'signed', kind: FrameKind.ONE_WAY, owningService: 'D1', versionRange: V, projection: { payload: ['sig'] } }));
  reg.register(defineRow({ type: 'ack', variant: 'legacy-unsigned', kind: FrameKind.ONE_WAY, owningService: 'LEGACY', versionRange: V, projection: { payload: ['sig'] } }));
  rej('5a variantBy.path not in projection → wrap throws', () => reg.wrap('ack', () => 'consumed', { variantBy: { path: 'undeclared', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } }));
  rej('5b variantBy result not a registered variant → wrap throws', () => reg.wrap('ack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'ghost', whenAbsent: 'legacy-unsigned' } }));
  const tr = []; reg._sink = (r) => tr.push(r); _on = true;
  const w = reg.wrap('ack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } });
  w.call({}, S({ sig: 'deadbeef' }), {}); ok('5c present → signed row', tr[0].owningService === 'D1' && tr[0].variant === 'signed');
  tr.length = 0; w.call({}, S({}), {}); ok('5d absent → legacy row', tr[0].owningService === 'LEGACY' && tr[0].variant === 'legacy-unsigned');
}

// ── 6. side-qualified recipe resolution: payload/meta path collision rejected (Aster S1e #4) ──
{
  rej('6a same path on payload AND meta → rejected', () => defineRow({ type: 'coll', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['id'], meta: ['id'] } }));
  // a meta-only path is read from a branded meta snapshot
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'mo', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { meta: ['mid'] }, correlation: { kind: 'IngressRef', requires: ['mid'] } }));
  _on = true;
  reg.wrap('mo', () => 'consumed').call({}, S({}), S({ mid: 'v' }));
  ok('6b meta path resolved from branded meta snapshot', tr[0].correlationPresent === true);
}

// ── 7. hard bounds & caps (Aster S1e #5, #7) ──
{
  const F = (o) => defineRow({ type: 'c', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, ...o });
  rej('7a capabilityRange >16 keys rejected', () => F({ capabilityRange: Object.fromEntries(Array.from({ length: 17 }, (_, i) => ['k' + i, 1])) }));
  rej('7b capabilityRange 100k-char KEY rejected', () => F({ capabilityRange: { ['Z'.repeat(100000)]: 1 } }));
  rej('7c errorContract >16 rejected', () => F({ errorContract: Array.from({ length: 17 }, (_, i) => 'e' + i) }));
  rej('7d traceFields >16 rejected', () => F({ traceFields: Array.from({ length: 17 }, (_, i) => 't' + i) }));
  rej('7e >24 projection fields rejected', () => F({ projection: { payload: Array.from({ length: 25 }, (_, i) => 'f' + i) } }));
  rej('7f maxBytes above the hard ceiling rejected', () => F({ budget: { maxBytes: 1e12 } }));
  rej('7g legacy budget.maxWork rejected (renamed to maxLeaves)', () => F({ budget: { maxWork: 4 } }));
  const reg = new ShadowRegistry({ boundary: 't' });
  reg.register(defineRow({ type: 'vc', variant: 'x', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['d'] } }));
  rej('7h variantBy.cases >32 rejected', () => reg.wrap('vc', () => 'consumed', { variantBy: { path: 'd', cases: Object.fromEntries(Array.from({ length: 33 }, (_, i) => ['c' + i, 'x'])), default: 'x' } }));
  rej('7i variantBy.cases 100k-char key rejected', () => reg.wrap('vc', () => 'consumed', { variantBy: { path: 'd', cases: { ['Z'.repeat(100000)]: 'x' }, default: 'x' } }));
}

// ── 8. prototypes & Symbol keying (retained) ──
{
  const F = (o) => defineRow({ type: 'p', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, ...o });
  rej('8a projection "__proto__" rejected', () => F({ projection: { payload: ['__proto__'] } }));
  rej('8b nested "a.__proto__" rejected', () => F({ projection: { payload: ['a.__proto__'] } }));
  const reg = new ShadowRegistry({ boundary: 't' });
  reg.register(defineRow({ type: 'a#b', kind: FrameKind.ONE_WAY, owningService: 'BASE-AhashB', versionRange: V }));
  reg.register(defineRow({ type: 'a', variant: 'b', kind: FrameKind.ONE_WAY, owningService: 'VARIANT-b', versionRange: V }));
  ok('8c (a,b) ≠ base a#b (Symbol keying, no NUL)', reg.row('a', 'b').owningService === 'VARIANT-b' && reg.row('a#b').owningService === 'BASE-AhashB');
  // a __proto__ key inside a branded snapshot cannot pollute the fact map
  const tr = []; const reg2 = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg2.register(pubRow()); _on = true;
  reg2.wrap('pubsub:pub', () => 'consumed').call({}, S(JSON.parse('{"topicId":"aa","__proto__":{"polluted":true}}')), {});
  ok('8d __proto__ snapshot key does not pollute', ({}).polluted === undefined && tr.length === 1);
}

// ── 9. return primitive-only; telemetry fixed-codes; contained faults ──
{
  const reg = mk(() => {}); _on = true;
  let thenG = 0, consG = 0;
  const evil = {}; Object.defineProperty(evil, 'then', { get() { thenG++; return () => {}; } }); Object.defineProperty(evil, 'consumed', { get() { consG++; return true; } });
  const r = reg.wrap('pubsub:pub', () => evil).call({}, S({ topicId: 'aa' }), {});
  ok('9a returned-object getters never invoked; opaque verdict', thenG === 0 && consG === 0 && r === evil);
  const tr = []; const reg2 = mk((x) => tr.push(x));
  let caught = null; try { reg2.wrap('pubsub:pub', () => { throw new Error('SECRET-EXCEPTION'); }).call({}, S({ topicId: 'aa' }), {}); } catch (e) { caught = e; }
  ok('9b handler exception rethrown, message NOT in trace', caught && caught.message === 'SECRET-EXCEPTION' && tr[0].verdict === 'threw' && !JSON.stringify(tr[0]).includes('SECRET-EXCEPTION'));
  const rbf = new ShadowRegistry({ boundary: 't', enabled: () => { throw new Error('flag'); } }); rbf.register(pubRow());
  let t = null, rr; try { rr = rbf.wrap('pubsub:pub', () => 'consumed').call({}, S({ topicId: 'aa' }), {}); } catch (e) { t = e; }
  ok('9c throwing enabled() → verbatim pass-through', t === null && rr === 'consumed');
  const reg3 = new ShadowRegistry({ boundary: 't' });
  rej('9d forged unbranded row rejected', () => reg3.register(Object.freeze({ type: 'x', variant: null, kind: 'ONE_WAY' })));
  reg3.register(pubRow()); rej('9e duplicate registration rejected', () => reg3.register(pubRow()));
}

// ── 10. sampling: clean 1-of-N, faults always ──
{
  const tr = []; const reg = mk((r) => tr.push(r), { sampleEvery: 3 }); _on = true;
  const w = reg.wrap('pubsub:pub', () => 'consumed');
  for (let i = 0; i < 6; i++) w.call({}, S({ topicId: 'aa' }), {});
  ok('10a clean sampled 1-of-3', tr.length === 2, `${tr.length}`);
  const before = tr.length;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, S({}), {});
  ok('10b fault bypasses sampling', tr.length === before + 1 && tr[tr.length - 1].schemaOk === false);
}

// ── 11. retained validation gates ──
{
  rej('11a OBSERVED + proves:routing rejected', () => defineRow({ type: 'z', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, evidence: EvidenceLevel.OBSERVED, proves: Proves.ROUTING }));
  rej('11b correlation path not in projection rejected', () => defineRow({ type: 'z', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['a'] }, correlation: { kind: CorrelationSubjectKind.IngressRef, requires: ['ghost'] } }));
  ok('11c FactType export present', FactType && FactType.bigint === 'bigint' && FactType.present === 'present');
}

_on = false; setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
