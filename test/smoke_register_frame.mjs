// =====================================================================
// smoke_register_frame.mjs — REF-1.1 E1: the canonical registerFrame door,
// proven on Boundary-1's rows.
//
// E1 lands registerFrame and proves it wires + observes identically to the
// current wireHandlers wrap path — WITHOUT migrating the live sites (E2) or
// sealing the raw primitive (E3). This smoke drives registerFrame against a
// capturing receiver over Boundary-1's real registry and asserts:
//
//   OWN. it refuses at registration a (recv, wire) the rows do not declare, and
//        a receiver lacking the primitive (exit criterion 4).
//   SEL. it selects the primitive INTERNALLY from the row's transportKind
//        (Boundary-1 routed -> onRoutedMessage), installed under the given wire.
//   OFF. with the shadow flag OFF the wrapped handler runs the original handler
//        verbatim — same args in, same value out — and emits ZERO traces.
//   ON.  with the shadow flag ON it OBSERVES (emits a branded trace) while still
//        running the handler verbatim (same args, same return) — byte-identity of
//        handler I/O preserved, observation beside it.
//   EQ.  registerFrame adds no behavior over registry.wrap: a handler wired via
//        registerFrame and one wrapped directly produce identical I/O.
//
// Run: node test/smoke_register_frame.mjs
// =====================================================================
import { buildBoundary1Registry } from '../src/pubsub/boundary1Registry.js';
import { registerFrame, setShadowEnabled } from '../src/registry/index.js';
import { certifyBigint } from '../src/registry/snapshotMint.js';
import { encode } from '../src/transport/wire.js';
import { T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

console.log('\nREF-1.1 E1 — registerFrame canonical door (proven on Boundary-1)\n');

// The boundary's live-path certifier — the same one wireHandlers threads (M1c):
// re-encode through the canonical bigint-faithful codec, certify the TEXT.
const mintLive = (x) => certifyBigint(encode(x));

// A capturing receiver: records every (wire -> wrapped handler) installed on the
// routed primitive, standing in for the DHT adapter. Boundary-1 registers through
// onRoutedMessage only.
function makeRecv() {
  const installed = new Map();
  return {
    installed,
    onRoutedMessage(wire, handler) { installed.set(wire, handler); return { wire, unsub: () => installed.delete(wire) }; },
  };
}

// A schema-satisfying representative SUB frame + meta, so the flag-on path can mint
// and brand it.
const SUB_PAYLOAD = { topicId: 'aa', subscriberId: 'bb', since: 0 };
const SUB_META = { fromId: 'n0', type: T.SUB };

// ── OWN: ownership refuse ──
{
  const reg = buildBoundary1Registry({ enabled: true, sink: () => {} });
  const recv = makeRecv();
  let threwUndeclared = false;
  try { registerFrame(recv, 'no-such-wire-xyz', () => {}, { registry: reg }); } catch { threwUndeclared = true; }
  check('OWN1. refuses a wire no registry row declares (throws, installs nothing)', threwUndeclared && recv.installed.size === 0);

  let threwNoPrim = false;
  try { registerFrame({}, T.SUB, () => {}, { registry: reg }); } catch { threwNoPrim = true; }
  check('OWN2. refuses a receiver lacking the dispatch primitive (throws)', threwNoPrim);

  let threwBadArgs = false;
  try { registerFrame(recv, T.SUB, 'not-a-fn', { registry: reg }); } catch { threwBadArgs = true; }
  check('OWN3. refuses a non-function handler (throws)', threwBadArgs);
}

// ── SEL: internal primitive selection + install ──
{
  const reg = buildBoundary1Registry({ enabled: true, sink: () => {} });
  const recv = makeRecv();
  const ret = registerFrame(recv, T.SUB, (p) => p, { registry: reg });
  check('SEL1. installs the handler under the given wire on the routed primitive', recv.installed.has(T.SUB));
  check('SEL2. returns the primitive install result (passthrough)', ret && ret.wire === T.SUB && typeof ret.unsub === 'function');
}

// ── OFF: flag-off byte-identity + zero traces ──
{
  setShadowEnabled(false);
  const traces = [];
  const reg = buildBoundary1Registry({ enabled: true, sink: (r) => traces.push(r) });
  const recv = makeRecv();
  const seen = [];
  registerFrame(recv, T.SUB, (p, m) => { seen.push([p, m]); return { ok: true, echo: p }; }, { registry: reg, mintLive });
  const wrapped = recv.installed.get(T.SUB);
  const out = wrapped(SUB_PAYLOAD, SUB_META);
  check('OFF1. wrapped handler returns the original return value verbatim', out && out.ok === true && out.echo === SUB_PAYLOAD);
  check('OFF2. wrapped handler receives the original args verbatim', seen.length === 1 && seen[0][0] === SUB_PAYLOAD && seen[0][1] === SUB_META);
  check('OFF3. flag-off emits ZERO traces (inert wrap)', traces.length === 0);
}

// ── ON: flag-on observes while preserving handler I/O ──
{
  setShadowEnabled(true);
  const traces = [];
  const reg = buildBoundary1Registry({ enabled: true, sink: (r) => traces.push(r) });
  const recv = makeRecv();
  const seen = [];
  registerFrame(recv, T.SUB, (p, m) => { seen.push([p, m]); return { ok: true, echo: p }; }, { registry: reg, mintLive });
  const wrapped = recv.installed.get(T.SUB);
  const out = wrapped(SUB_PAYLOAD, SUB_META);
  check('ON1. handler still returns the original value verbatim under observation', out && out.ok === true && out.echo === SUB_PAYLOAD);
  check('ON2. handler still receives the original args verbatim under observation', seen.length === 1 && seen[0][0] === SUB_PAYLOAD && seen[0][1] === SUB_META);
  check('ON3. flag-on emits at least one trace (observation fired)', traces.length >= 1);
  setShadowEnabled(false);
}

// ── EQ: registerFrame adds no behavior over a direct registry.wrap ──
{
  setShadowEnabled(false);
  const reg = buildBoundary1Registry({ enabled: true, sink: () => {} });
  const recv = makeRecv();
  const handler = (p) => ({ doubled: p.since + p.since });
  registerFrame(recv, T.SUB, handler, { registry: reg, mintLive });
  const viaDoor = recv.installed.get(T.SUB);
  const wire = reg.wiring.get(T.SUB);
  const viaWrap = reg.wrap(wire.type, handler, { mintLive });
  const a = viaDoor({ topicId: 'aa', subscriberId: 'bb', since: 21 }, SUB_META);
  const b = viaWrap({ topicId: 'aa', subscriberId: 'bb', since: 21 }, SUB_META);
  check('EQ1. registerFrame I/O equals a direct registry.wrap of the same handler', a && b && a.doubled === 42 && b.doubled === 42);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
