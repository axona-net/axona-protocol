// smoke_registry_core.mjs — REF-1.1 S1/S1b: the shadow registry CORE, in
// isolation, hardened per Aster's S1 disposition. Proves the invariants that
// make shadow mode safe to wire into a running kernel (§4.3): flag-off verbatim
// pass-through; flag-on read-only validate+trace that never alters the handler's
// verdict/this/args/throw; observation isolation (callbacks cannot mutate live
// dispatch input); strict registry identity; no silent variant/contract
// fallback; contained flag/thenable/selector/sink faults; and allowlisted,
// hashed, bounded, sampled telemetry. No kernel wiring here — that is S2.
//
// Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind, ShadowRegistry, setShadowEnabled } from '../src/pubsub/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };

const pubRow = () => defineRow({
  type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress',
  evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
  schema: (p) => (p && typeof p.topicId === 'string') ? { ok: true } : { ok: false, reason: 'missing topicId' },
  correlation: (p) => ({ kind: 'IngressRef', topicId: p && p.topicId }),
  correlationFields: ['topicId'], subjectShape: CorrelationSubjectKind.IngressRef,
  idempotencyKey: (p) => p && p.topicId,
});
let _on = false;
const mk = (sink, extra = {}) => { const r = new ShadowRegistry({ boundary: 'test', sink, enabled: () => _on, ...extra }); r.register(pubRow()); return r; };

// ── 1. flag OFF: verbatim pass-through incl. `this`, ALL args, no trace ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = false;
  let seenThis = null, seenArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { seenThis = this; seenArgs = a; return 'consumed'; });
  const ctx = {}; const r = w.call(ctx, { topicId: 'aa' }, { fromId: 'bb' }, 'third', 4);
  ok('1a flag-off returns verbatim', r === 'consumed');
  ok('1b flag-off preserves this', seenThis === ctx);
  ok('1c flag-off forwards ALL args (variadic)', seenArgs.length === 4 && seenArgs[2] === 'third' && seenArgs[3] === 4);
  ok('1d flag-off emits no trace', traces.length === 0);
}

// ── 2. flag ON: variadic forwarding preserved + verdict unchanged + trace ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = true;
  let seenArgs = null;
  const w = reg.wrap('pubsub:pub', function (...a) { seenArgs = a; return 'consumed'; });
  const r = w.call({}, { topicId: 'aa' }, {}, 'x');
  ok('2a flag-on forwards ALL args', seenArgs.length === 3 && seenArgs[2] === 'x');
  ok('2b verdict verbatim', r === 'consumed');
  ok('2c one trace, verdict consumed', traces.length === 1 && traces[0].verdict === 'consumed');
  ok('2d trace carries row facts', traces[0].kind === 'ONE_WAY' && traces[0].owningService === 'WriteIngress' && traces[0].evidence === 'INGESTED');
}

// ── 3. OBSERVATION ISOLATION: a callback that mutates its input cannot change
//      what the handler sees (callbacks get an immutable snapshot, not live). ──
{
  const traces = []; const reg = new ShadowRegistry({ boundary: 'test', sink: (r) => traces.push(r), enabled: () => true });
  reg.register(defineRow({
    type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress',
    schema: (p) => { if (p) p.topicId = 'HACKED_BY_SCHEMA'; return { ok: true }; },
    correlation: (p) => { if (p) p.topicId = 'HACKED_BY_CORR'; return { kind: 'IngressRef', topicId: 'x' }; },
    correlationFields: ['topicId'], subjectShape: CorrelationSubjectKind.IngressRef,
  }));
  _on = true;
  const live = { topicId: 'aa' };
  let handlerSaw = null;
  const w = reg.wrap('pubsub:pub', function (p) { handlerSaw = p.topicId; return 'consumed'; });
  w.call({}, live, {});
  ok('3a handler saw the ORIGINAL topicId (callbacks mutated only a snapshot)', handlerSaw === 'aa', handlerSaw);
  ok('3b live object itself untouched', live.topicId === 'aa', live.topicId);
}

// ── 4. contained faults: throwing enabled() / sink never reach the handler ──
{
  _on = true;
  const regBadFlag = new ShadowRegistry({ boundary: 'test', enabled: () => { throw new Error('flag boom'); } });
  regBadFlag.register(pubRow());
  let r, threw = null;
  try { r = regBadFlag.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {}); } catch (e) { threw = e; }
  ok('4a throwing enabled() → pass-through, no throw', threw === null && r === 'consumed', threw && threw.message);

  const regBadSink = new ShadowRegistry({ boundary: 'test', sink: () => { throw new Error('sink boom'); }, enabled: () => true });
  regBadSink.register(pubRow());
  let r2, threw2 = null;
  try { r2 = regBadSink.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'aa' }, {}); } catch (e) { threw2 = e; }
  ok('4b throwing sink → handler verdict intact, no throw', threw2 === null && r2 === 'consumed', threw2 && threw2.message);
}

// ── 5. throwing `then` getter must not escape; value returned verbatim ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = true;
  const evil = {}; Object.defineProperty(evil, 'then', { get() { throw new Error('then boom'); } });
  let r, threw = null;
  try { r = reg.wrap('pubsub:pub', () => evil).call({}, { topicId: 'aa' }, {}); } catch (e) { threw = e; }
  ok('5a throwing then-getter contained', threw === null, threw && threw.message);
  ok('5b evil value returned verbatim', r === evil);
}

// ── 6. strict registry identity: raw rows + duplicate keys rejected ──
{
  const reg = new ShadowRegistry({ boundary: 'test' });
  let e1 = null; try { reg.register({ type: 'x', kind: 'ONE_WAY', owningService: 'y' }); } catch (e) { e1 = e; }
  ok('6a register rejects a raw (unbranded) row', e1 !== null);
  reg.register(pubRow());
  let e2 = null; try { reg.register(pubRow()); } catch (e) { e2 = e; }
  ok('6b register rejects a duplicate type/variant key', e2 !== null);
}

// ── 7. defineRow contract-completeness validation ──
{
  const rej = (label, fn) => { let e = null; try { fn(); } catch (x) { e = x; } ok(label, e !== null); };
  rej('7a REQUEST_RESPONSE without correlation rejected', () => defineRow({ type: 'q', kind: FrameKind.REQUEST_RESPONSE, owningService: 'S' }));
  rej('7b correlation without correlationFields rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', correlation: () => ({}), subjectShape: CorrelationSubjectKind.IngressRef }));
  rej('7c correlation without subjectShape rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', correlation: () => ({}), correlationFields: ['a'] }));
  rej('7d COMMITTED without producedPolicy rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', evidence: EvidenceLevel.COMMITTED }));
  rej('7e non-function idempotencyKey rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY, owningService: 'S', idempotencyKey: 'nope' }));
  rej('7f bad frame kind rejected', () => defineRow({ type: 'q', kind: 'WAT', owningService: 'S' }));
  rej('7g missing owningService rejected', () => defineRow({ type: 'q', kind: FrameKind.ONE_WAY }));
}

// ── 8. no silent fallback: unknown variant + selector fault resolve NO contract ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = true;   // base row registered
  const wUnknown = reg.wrap('pubsub:pub', () => 'consumed', { select: () => 'ghost' });
  const r1 = wUnknown.call({}, { topicId: 'aa' }, {});
  ok('8a unknown variant: handler still runs', r1 === 'consumed');
  ok('8b unknown variant: trace registered:false + variantFault (no base fallback)', traces[0].registered === false && !!traces[0].variantFault && traces[0].kind === null);

  traces.length = 0;
  const wSelBoom = reg.wrap('pubsub:pub', () => 'consumed', { select: () => { throw new Error('sel boom'); } });
  const r2 = wSelBoom.call({}, { topicId: 'aa' }, {});
  ok('8c selector fault: handler still runs', r2 === 'consumed');
  ok('8d selector fault: reported, no contract resolved', traces[0].registered === false && !!traces[0].selectorFault && traces[0].kind === null);
}

// ── 9. telemetry is sanitized: correlation hashed to {kind,digest}, idempotency
//      hashed, NO raw topicId anywhere in the trace record ──
{
  const traces = []; const reg = mk((r) => traces.push(r)); _on = true;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, { topicId: 'SECRET-topic-123' }, {});
  const t = traces[0];
  const blob = JSON.stringify(t);
  ok('9a correlation reduced to {kind,digest}', t.correlation && t.correlation.kind === 'IngressRef' && /^h:/.test(t.correlation.digest));
  ok('9b idempotency emitted as a hash tag', typeof t.idempotencyTag === 'string' && /^h:/.test(t.idempotencyTag));
  ok('9c raw payload value never appears in the trace', !blob.includes('SECRET-topic-123'), blob);
}

// ── 10. sampling: non-fault traces sampled 1-of-N; faults ALWAYS emit ──
{
  const traces = []; const reg = mk((r) => traces.push(r), { sampleEvery: 3 }); _on = true;
  const w = reg.wrap('pubsub:pub', () => 'consumed');
  for (let i = 0; i < 6; i++) w.call({}, { topicId: 'aa' }, {});     // 6 clean → ~2 sampled
  const clean = traces.length;
  ok('10a clean traces sampled down (1 of 3)', clean === 2, `${clean}`);
  // a schema-invalid frame is a fault → always emitted regardless of sampling
  const before = traces.length;
  reg.wrap('pubsub:pub', () => 'consumed').call({}, { /* no topicId → schema fail */ }, {});
  ok('10b fault trace bypasses sampling', traces.length === before + 1 && traces[traces.length - 1].schemaOk === false);
}

_on = false; setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
