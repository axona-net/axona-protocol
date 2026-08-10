// smoke_registry_core.mjs — REF-1.1 S1..S1g: the shadow registry CORE in
// isolation. S1g closes Aster's S1f disposition: the snapshot brand is now
// UNFORGEABLE (the mint is decoder-private, not importable by public consumers)
// and TRANSITIVE (the mint brands every reachable node; the dispatcher checks
// membership before every reflective op, so a nested or post-mint Proxy is never
// touched). Reproduces every failure that disposition demonstrated.
// Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind, FactType, ShadowRegistry, setShadowEnabled } from '../src/pubsub/registry/index.js';
import * as publicSurface from '../src/pubsub/registry/index.js';
import { certify } from '../src/pubsub/registry/snapshotMint.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const rej = (m, fn) => { let e = null; try { fn(); } catch (x) { e = x; } ok(m, e !== null); };
const V = { min: 4, max: 4 };
const S = (o) => certify(JSON.stringify(o));   // decoder-certified frame (whole graph branded)
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

// ── 0. the mint is UNFORGEABLE (Aster S1f #1) ──
{
  ok('0a public registry export has no certify', publicSurface.certify === undefined);
  ok('0b public registry export has no snapshot/isSnapshot', publicSurface.snapshot === undefined && publicSurface.isSnapshot === undefined);
  let blocked = false;
  try { await import('@axona/protocol/pubsub/registry/snapshotMint.js'); } catch { blocked = true; }
  ok('0c mint subpath is not a package export (public consumers cannot import it)', blocked);
}

// ── 1. flag OFF verbatim; flag ON observes a CERTIFIED frame ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = false;
  let sawThis = null, sawArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { sawThis = this; sawArgs = a; return 'consumed'; });
  const ctx = {}; const r = w.call(ctx, S({ topicId: 'aa' }), {}, 9);
  ok('1a off: verbatim + this + all args', r === 'consumed' && sawThis === ctx && sawArgs.length === 3 && sawArgs[2] === 9);
  ok('1b off: no trace', tr.length === 0);
  _on = true; const r2 = reg.wrap('pubsub:pub', () => 'consumed').call({}, S({ topicId: 'aa' }), {});
  ok('1c on: verbatim verdict + declared facts', r2 === 'consumed' && tr.length === 1 && tr[0].verdict === 'consumed' && tr[0].schemaOk === true && tr[0].correlationPresent === true && tr[0].idempotencyPresent === true);
}

// ── 2. an UNCERTIFIED root is observed as nothing (not a certified graph) ──
{
  const tr = []; const reg = mk((r) => tr.push(r)); _on = true;
  let ran = 0, sawArg = null; const plain = { topicId: 'aa' };   // a plain literal is NOT certified
  reg.wrap('pubsub:pub', (p) => { ran++; sawArg = p; return 'consumed'; }).call({}, plain, {});
  ok('2a uncertified plain root → handler verbatim, unbranded-source', ran === 1 && sawArg === plain && tr[0].verdict === 'unobserved' && (tr[0].faults || []).includes('unbranded-source'));
}

// ── 3. TRANSITIVE provenance — nested & post-mint Proxies are never reflected on (Aster S1f #2,#3) ──
{
  const tr = []; const reg2 = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg2.register(defineRow({ type: 'nest', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['evil', 'arr', 'a.b'] } }));
  _on = true; const w = reg2.wrap('nest', () => 'consumed');
  // legit nested data (branded by the mint) IS observed
  const seen = {}; reg2.register(defineRow({ type: 'nest2', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['a.b', 'arr'] }, schema: { types: { arr: 'arr' } } }));
  const w2 = reg2.wrap('nest2', () => 'consumed');
  tr.length = 0; w2.call({}, S({ a: { b: 'deep' }, arr: [1, 2, 3] }), {});
  ok('3a legit nested object + array in a certified graph are observed', tr[0].schemaOk === true && !(tr[0].faults || []).length);
  // nested Proxy inserted AFTER minting → its traps never fire
  let gopd = 0; const frame = S({ a: { b: 'x' } });
  frame.evil = new Proxy({ x: 1 }, { getOwnPropertyDescriptor(t, k) { gopd++; return Reflect.getOwnPropertyDescriptor(t, k); }, get(t, k) { gopd++; return Reflect.get(t, k); } });
  tr.length = 0; w.call({}, frame, {});
  ok('3b nested Proxy (post-mint) never reflected on; recorded unbranded', gopd === 0 && (tr[0].faults || []).includes('projection-unbranded'));
  // proxied array inserted after minting → length get trap never fires
  let lenTrap = 0; const frame2 = S({ a: { b: 'x' } });
  frame2.arr = new Proxy([1, 2, 3], { get(t, k) { if (k === 'length') lenTrap++; return Reflect.get(t, k); } });
  tr.length = 0; w.call({}, frame2, {});
  ok('3c proxied array (post-mint) → no length trap', lenTrap === 0 && (tr[0].faults || []).includes('projection-unbranded'));
  // throwing nested Proxy → contained, handler verbatim
  let ran = 0, escaped = null; const frame3 = S({ a: { b: 'x' } });
  frame3.evil = new Proxy({}, { getOwnPropertyDescriptor() { throw new Error('boom'); }, get() { throw new Error('boom'); } });
  try { reg2.wrap('nest', () => { ran++; return 'consumed'; }).call({}, frame3, {}); } catch (e) { escaped = e; }
  ok('3d throwing nested Proxy → contained, handler ran once', ran === 1 && escaped === null);
  // metadata equivalent: nested Proxy in a certified meta graph
  const reg3 = new ShadowRegistry({ boundary: 't', sink: () => {}, enabled: () => true });
  reg3.register(defineRow({ type: 'm', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { meta: ['evil'] } }));
  let gopdM = 0; const meta = S({ ok: 1 }); meta.evil = new Proxy({}, { getOwnPropertyDescriptor() { gopdM++; return undefined; } });
  let ranM = 0; reg3.wrap('m', () => { ranM++; return 'consumed'; }).call({}, S({}), meta);
  ok('3e nested Proxy in certified meta → never reflected on, handler verbatim', gopdM === 0 && ranM === 1);
}

// ── 4. NO row code — declarative recipes to fixed codes ──
{
  const F = (o) => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['x'] }, ...o });
  rej('4a function schema rejected', () => F({ schema: () => ({ ok: true }) }));
  rej('4b function correlation rejected', () => F({ correlation: () => ({}) }));
  rej('4c legacy idempotencyKey rejected', () => F({ idempotencyKey: () => 'k' }));
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['x', 'y'] }, schema: { require: ['x'], forbid: ['y'], types: { x: 'string' } } }));
  _on = true; const w = reg.wrap('q', () => 'consumed');
  w.call({}, S({ x: 'ok' }), {}); ok('4d require+forbid ok', tr[0].schemaOk === true);
  tr.length = 0; w.call({}, S({ y: 'present' }), {}); ok('4e missing required', tr[0].schemaCode === 'missing-required');
  tr.length = 0; w.call({}, S({ x: 'ok', y: 'z' }), {}); ok('4f forbidden present', tr[0].schemaCode === 'forbidden-present');
  tr.length = 0; w.call({}, S({ x: 12345 }), {}); ok('4g type mismatch', tr[0].schemaCode === 'type-mismatch');
}

// ── 5. budgets + fixed UTF-8 surrogate accounting ──
{
  const tr = []; const reg = new ShadowRegistry({ boundary: 't', sink: (r) => tr.push(r), enabled: () => true });
  reg.register(defineRow({ type: 'w', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxLeaves: 1 }, projection: { payload: ['a', 'b'] }, correlation: { kind: 'IngressRef', requires: ['b'] } }));
  _on = true;
  reg.wrap('w', () => 'consumed').call({}, S({ a: 'x', b: 'y' }), {});
  ok('5a maxLeaves=1 → 2nd field unread + leaves fault', tr[0].correlationPresent === false && (tr[0].faults || []).includes('projection-leaves'));
  const trb = []; const regb = new ShadowRegistry({ boundary: 't', sink: (r) => trb.push(r), enabled: () => true });
  regb.register(defineRow({ type: 'b', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, budget: { maxBytes: 4 }, projection: { payload: ['s'] }, correlation: { kind: 'IngressRef', requires: ['s'] } }));
  const wb = regb.wrap('b', () => 'consumed');
  wb.call({}, S({ s: 'ééé' }), {}); ok('5b "ééé" (6 bytes) > 4 → budget fault', (trb[0].faults || []).includes('projection-budget'));
  trb.length = 0; wb.call({}, S({ s: '\uD800é' }), {}); ok('5c lone high surrogate + é (5 bytes) → budget fault', (trb[0].faults || []).includes('projection-budget'));
  trb.length = 0; wb.call({}, S({ s: 'abcd' }), {}); ok('5d "abcd" (4 bytes) → projected', trb[0].correlationPresent === true);
}

// ── 6. variant selection bound to the projection ──
{
  const reg = new ShadowRegistry({ boundary: 't', enabled: () => true });
  reg.register(defineRow({ type: 'ack', variant: 'signed', kind: FrameKind.ONE_WAY, owningService: 'D1', versionRange: V, projection: { payload: ['sig'] } }));
  reg.register(defineRow({ type: 'ack', variant: 'legacy-unsigned', kind: FrameKind.ONE_WAY, owningService: 'LEGACY', versionRange: V, projection: { payload: ['sig'] } }));
  rej('6a variantBy.path not in projection → throws', () => reg.wrap('ack', () => 'consumed', { variantBy: { path: 'nope', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } }));
  rej('6b variantBy result not registered → throws', () => reg.wrap('ack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'ghost', whenAbsent: 'legacy-unsigned' } }));
  const tr = []; reg._sink = (r) => tr.push(r); _on = true;
  const w = reg.wrap('ack', () => 'consumed', { variantBy: { path: 'sig', whenPresent: 'signed', whenAbsent: 'legacy-unsigned' } });
  w.call({}, S({ sig: 'deadbeef' }), {}); ok('6c present → signed', tr[0].variant === 'signed' && tr[0].owningService === 'D1');
  tr.length = 0; w.call({}, S({}), {}); ok('6d absent → legacy', tr[0].variant === 'legacy-unsigned');
}

// ── 7. side-qualified recipes + hard bounds + caps (retained/accepted S1e/S1f) ──
{
  rej('7a payload/meta path collision rejected', () => defineRow({ type: 'coll', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, projection: { payload: ['id'], meta: ['id'] } }));
  const F = (o) => defineRow({ type: 'c', kind: FrameKind.ONE_WAY, owningService: 'S', versionRange: V, ...o });
  rej('7b maxBytes above ceiling rejected', () => F({ budget: { maxBytes: 1e12 } }));
  rej('7c legacy maxWork rejected', () => F({ budget: { maxWork: 4 } }));
  rej('7d capabilityRange >16 keys rejected', () => F({ capabilityRange: Object.fromEntries(Array.from({ length: 17 }, (_, i) => ['k' + i, 1])) }));
  rej('7e capabilityRange 100k-char key rejected', () => F({ capabilityRange: { ['Z'.repeat(100000)]: 1 } }));
  rej('7f projection "__proto__" rejected', () => F({ projection: { payload: ['__proto__'] } }));
  rej('7g >24 projection fields rejected', () => F({ projection: { payload: Array.from({ length: 25 }, (_, i) => 'f' + i) } }));
  rej('7h OBSERVED + proves:routing rejected', () => F({ evidence: EvidenceLevel.OBSERVED, proves: Proves.ROUTING }));
}

// ── 8. return primitive-only; telemetry fixed codes; contained faults; Symbol keying ──
{
  const reg = mk(() => {}); _on = true;
  let thenG = 0, consG = 0; const evil = {}; Object.defineProperty(evil, 'then', { get() { thenG++; } }); Object.defineProperty(evil, 'consumed', { get() { consG++; } });
  const r = reg.wrap('pubsub:pub', () => evil).call({}, S({ topicId: 'aa' }), {});
  ok('8a returned-object getters never invoked', thenG === 0 && consG === 0 && r === evil);
  const tr = []; const reg2 = mk((x) => tr.push(x));
  let caught = null; try { reg2.wrap('pubsub:pub', () => { throw new Error('SECRET'); }).call({}, S({ topicId: 'aa' }), {}); } catch (e) { caught = e; }
  ok('8b handler exception rethrown, message not in trace', caught && caught.message === 'SECRET' && tr[0].verdict === 'threw' && !JSON.stringify(tr[0]).includes('SECRET'));
  const reg3 = new ShadowRegistry({ boundary: 't' });
  reg3.register(defineRow({ type: 'a#b', kind: FrameKind.ONE_WAY, owningService: 'BASE', versionRange: V }));
  reg3.register(defineRow({ type: 'a', variant: 'b', kind: FrameKind.ONE_WAY, owningService: 'VAR', versionRange: V }));
  ok('8c (a,b) ≠ base a#b (Symbol keying)', reg3.row('a', 'b').owningService === 'VAR' && reg3.row('a#b').owningService === 'BASE');
  rej('8d forged unbranded row rejected', () => reg3.register(Object.freeze({ type: 'z', kind: 'ONE_WAY' })));
}

// ── 9. sampling ──
{
  const tr = []; const reg = mk((r) => tr.push(r), { sampleEvery: 3 }); _on = true;
  const w = reg.wrap('pubsub:pub', () => 'consumed');
  for (let i = 0; i < 6; i++) w.call({}, S({ topicId: 'aa' }), {});
  ok('9a clean sampled 1-of-3', tr.length === 2, `${tr.length}`);
  const before = tr.length; reg.wrap('pubsub:pub', () => 'consumed').call({}, S({}), {});
  ok('9b fault bypasses sampling', tr.length === before + 1 && tr[tr.length - 1].schemaOk === false);
  ok('9c FactType export present', FactType && FactType.bigint === 'bigint');
}

_on = false; setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
