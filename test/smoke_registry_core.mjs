// smoke_registry_core.mjs — REF-1.1 S1: the shadow registry CORE, in isolation.
// Proves the invariants that make shadow mode safe to ship into a running kernel
// (§4.3): flag-off is a verbatim pass-through; flag-on validates + traces without
// altering the handler's verdict, `this`, timing semantics, or throw behavior;
// and faults in the shadow path (schema/correlation/sink) never perturb the
// handler. No kernel wiring here — that is S2.
//
// Run: node test/smoke_registry_core.mjs
import { defineRow, FrameKind, EvidenceLevel, Proves, ShadowRegistry, setShadowEnabled } from '../src/pubsub/registry/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };

function mkReg(sink) {
  const reg = new ShadowRegistry({ boundary: 'test', sink, enabled: () => _on });
  reg.register(defineRow({
    type: 'pubsub:pub', kind: FrameKind.ONE_WAY, owningService: 'WriteIngress',
    evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION,
    schema: (p) => (p && typeof p.topicId === 'string') ? { ok: true } : { ok: false, reason: 'missing topicId' },
    correlation: (p) => ({ kind: 'IngressRef', topicId: p && p.topicId }),
  }));
  return reg;
}
let _on = false;

// ── 1. flag OFF: verbatim pass-through, sink never called ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = false;
  let sawThis = null;
  const handler = function (p, m) { sawThis = this; return 'consumed'; };
  const wrapped = reg.wrap('pubsub:pub', handler);
  const ctx = { name: 'mgr' };
  const r = wrapped.call(ctx, { topicId: 'aa' }, { fromId: 'bb' });
  ok('1a flag-off returns the handler value verbatim', r === 'consumed', r);
  ok('1b flag-off preserves `this`', sawThis === ctx);
  ok('1c flag-off emits NO trace', traces.length === 0, `${traces.length}`);
}

// ── 2. flag ON: trace emitted, sync verdict unchanged ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  const wrapped = reg.wrap('pubsub:pub', function () { return 'consumed'; });
  const r = wrapped.call({}, { topicId: 'aa' }, {});
  ok('2a flag-on still returns the handler value verbatim', r === 'consumed', r);
  ok('2b one trace emitted', traces.length === 1, `${traces.length}`);
  ok('2c trace carries the row facts', traces[0] && traces[0].kind === 'ONE_WAY' && traces[0].owningService === 'WriteIngress' && traces[0].evidence === 'INGESTED');
  ok('2d trace verdict = consumed', traces[0] && traces[0].verdict === 'consumed', traces[0] && traces[0].verdict);
  ok('2e schema pass recorded + correlation captured', traces[0].schemaOk === true && traces[0].correlation && traces[0].correlation.kind === 'IngressRef');
}

// ── 3. flag ON: a declining handler is traced as passed, verdict preserved ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  const wrapped = reg.wrap('pubsub:pub', function () { return undefined; });
  const r = wrapped.call({}, { topicId: 'aa' }, {});
  ok('3a decline (undefined) returned verbatim', r === undefined);
  ok('3b traced verdict = passed', traces[0] && traces[0].verdict === 'passed', traces[0] && traces[0].verdict);
}

// ── 4. flag ON: schema-invalid frame is TRACED as invalid but handler still runs unchanged (report mode, not enforcement) ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  let ran = false;
  const wrapped = reg.wrap('pubsub:pub', function () { ran = true; return 'consumed'; });
  const r = wrapped.call({}, { /* no topicId */ }, {});
  ok('4a handler STILL ran despite schema-invalid (report mode)', ran === true);
  ok('4b verdict preserved', r === 'consumed');
  ok('4c trace records schemaOk=false + reason', traces[0].schemaOk === false && traces[0].schemaReason === 'missing topicId');
}

// ── 5. flag ON: handler throw is re-thrown verbatim AND traced ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  const boom = new Error('handler boom');
  const wrapped = reg.wrap('pubsub:pub', function () { throw boom; });
  let caught = null;
  try { wrapped.call({}, { topicId: 'aa' }, {}); } catch (e) { caught = e; }
  ok('5a original throw preserved', caught === boom);
  ok('5b throw traced as verdict=threw', traces[0] && traces[0].verdict === 'threw' && traces[0].error === 'handler boom');
}

// ── 6. flag ON: async handler — original promise returned, resolution unchanged, observed passively ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  const wrapped = reg.wrap('pubsub:pub', async function () { return 'consumed'; });
  const p = wrapped.call({}, { topicId: 'aa' }, {});
  ok('6a returns a thenable', p && typeof p.then === 'function');
  const v = await p;
  ok('6b async value unchanged', v === 'consumed', v);
  await Promise.resolve(); // let the passive observer settle
  ok('6c async verdict traced', traces.length === 1 && traces[0].verdict === 'consumed', JSON.stringify(traces.map(t => t.verdict)));
}

// ── 7. flag ON: async REJECTION still propagates to the real caller (observer does not swallow) ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  const boom = new Error('async boom');
  const wrapped = reg.wrap('pubsub:pub', async function () { throw boom; });
  let caught = null;
  try { await wrapped.call({}, { topicId: 'aa' }, {}); } catch (e) { caught = e; }
  ok('7a async rejection propagates to caller', caught === boom);
  await Promise.resolve();
  ok('7b rejection traced', traces[0] && traces[0].verdict === 'rejected' && traces[0].error === 'async boom');
}

// ── 8. defensive: a THROWING sink / schema / correlation never affects the handler ──
{
  _on = true;
  const reg = new ShadowRegistry({ boundary: 'test', sink: () => { throw new Error('bad sink'); }, enabled: () => true });
  reg.register(defineRow({
    type: 'x', kind: FrameKind.ONE_WAY, owningService: 'S',
    schema: () => { throw new Error('bad schema'); },
    correlation: () => { throw new Error('bad corr'); },
  }));
  const wrapped = reg.wrap('x', function () { return 'consumed'; });
  let r, threw = null;
  try { r = wrapped.call({}, {}, {}); } catch (e) { threw = e; }
  ok('8a throwing sink/schema/correlation does NOT throw into handler path', threw === null, threw && threw.message);
  ok('8b handler verdict still returned', r === 'consumed', r);
}

// ── 9. unregistered frame under flag-on: handler runs; trace marks registered:false ──
{
  const traces = [];
  const reg = mkReg((r) => traces.push(r));
  _on = true;
  const wrapped = reg.wrap('pubsub:unlisted', function () { return 'consumed'; });
  const r = wrapped.call({}, { topicId: 'aa' }, {});
  ok('9a unregistered frame handler runs unchanged', r === 'consumed');
  ok('9b trace marks registered:false (catalogue gap signal)', traces[0] && traces[0].registered === false);
}

_on = false; // leave module flag clean
setShadowEnabled(null);
console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
