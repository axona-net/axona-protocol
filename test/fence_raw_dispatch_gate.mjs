// =====================================================================
// fence_raw_dispatch_gate.mjs — REF-1.1 E1: the enforcement gates.
//
// Two gates, both from the accepted v5 design:
//
//  A. BASELINE-DIFF IDENTIFIER GATE [R3] + fail-closed core [A3].
//     The raw dispatch primitives {onRequest,onNotification,onRoutedMessage} are
//     discovered by the shared sound acorn walk. Every non-allowlisted registration
//     is a "raw reference". At E1 the EXISTING references are frozen into a baseline
//     (REF-1.1-raw-dispatch-baseline.json). From E1 the build FAILS on any NEW raw
//     reference not in the baseline; a baseline reference may be REMOVED (E2 migrates
//     it) without failing — the baseline shrinks and reaches empty at E4. It is a
//     baseline DIFF, not warn-only: a new raw call is stopped while the legacy sites
//     still legitimately exist. Aliases / re-exports / computed / loose method-name
//     literals outside the allowlist are fail-closed (unresolved), per the partition:
//     named access is this gate's case; computed access is the runtime boundary's
//     (structurally closed at E3). [Q1] the named gate cannot catch a NEW computed
//     access in the E1-E3 window — stated, not papered over.
//
//  B. WIRE-LITERAL GATE [V2].
//     A registerFrame(recv, wire, ...) call must pass a literal frame-type constant
//     as `wire` — a string Literal or a T.<name> member. A variable fails the build.
//
//   node test/fence_raw_dispatch_gate.mjs            # CI gate (default)
//   node test/fence_raw_dispatch_gate.mjs --write    # freeze/refresh the baseline
//
// Registration discipline only; shadow default-off; wire unchanged; 4.63.0.
// =====================================================================
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join, relative, dirname } from 'node:path';
import { execSync } from 'node:child_process';
import * as acorn from 'acorn';
import { discover, walk } from './lib/registrationScan.mjs';
import { SEALED, RAW_DISPATCH_ALLOWLIST } from './lib/e0Allowlist.mjs';
import { T } from '../src/pubsub/constants.js';

const REPO = dirname(dirname(fileURLToPath(import.meta.url)));
const SRC = join(REPO, 'src');
const BASELINE = join(dirname(fileURLToPath(import.meta.url)), 'REF-1.1-raw-dispatch-baseline.json');

function listJs(dir) { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; }
const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));

// A raw reference key is stable across line moves: file | receiver | primitive |
// wire. RECEIVER is in the key (Vega E1 review): transport/web/index.js has
// bridge.onNotification('hello') AND webrtc.onNotification('hello') — without the
// receiver the two collapse to one key and a third hello would not fail the diff.
// The bridge-ws `dispatch` switch is a different registration style (not one of
// the three sealed primitives) — excluded, like the E0 manifest.
const keyOf = (s) => `${s.file}|${s.recv}|${s.callee}|${s.wire}`;
const rawKeys = (sites) => sites.filter((s) => s.callee !== 'dispatch').map(keyOf).sort();

let ok = true;
const fail = (m) => { console.error(`  ✗ ${m}`); ok = false; };
const pass = (m) => console.log(`  ✓ ${m}`);

console.log('\nREF-1.1 E1 — raw-dispatch enforcement gates\n');

// ── A. baseline-diff identifier gate ──
const scan = discover(files, { methods: SEALED, mechanismExempt: RAW_DISPATCH_ALLOWLIST });
scan.parseErrors.length === 0 ? pass(`source coverage: all ${files.length} src files parsed`) : fail(`parse errors: ${scan.parseErrors.join('; ')}`);
scan.unresolved.length === 0
  ? pass('no unresolved raw reference (alias / computed / re-export / loose method-name literal) outside the allowlist')
  : fail(`unresolved raw references (fail-closed):\n     ${scan.unresolved.join('\n     ')}`);

const current = rawKeys(scan.sites);
const args = process.argv.slice(2);
if (args.includes('--write')) {
  let treeHash = 'UNKNOWN';
  try { treeHash = execSync('git rev-parse HEAD', { cwd: REPO }).toString().trim(); } catch { /* detached / no git */ }
  writeFileSync(BASELINE, JSON.stringify({ note: 'REF-1.1 E1 frozen raw-dispatch baseline; shrinks as E2 migrates, empty at E4', treeHash, count: current.length, keys: current }, null, 2) + '\n');
  pass(`froze baseline: ${current.length} raw-dispatch references (tree ${treeHash.slice(0, 7)})`);
} else {
  let base;
  try { base = JSON.parse(readFileSync(BASELINE, 'utf8')); } catch { fail('no committed baseline — run with --write first'); }
  if (base) {
    const baseSet = new Set(base.keys);
    const added = current.filter((k) => !baseSet.has(k));   // NEW raw references — the failure case
    const removed = base.keys.filter((k) => !current.includes(k)); // migrated (E2) — allowed, the baseline shrinks
    added.length === 0
      ? pass(`baseline-diff: no NEW raw reference (${current.length} present, all in the frozen baseline)`)
      : fail(`baseline-diff: ${added.length} NEW raw reference(s) not in the baseline — a new raw registration landed:\n     ${added.join('\n     ')}`);
    if (removed.length) console.log(`    · ${removed.length} baseline reference(s) migrated away (allowed; refresh the baseline with --write)`);
  }
}

// ── B. wire-literal gate [V2] ──
function wireLiteralViolations(fileList) {
  const bad = [];
  for (const { path: fp, code } of fileList) {
    let ast;
    try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'module', allowHashBang: true, locations: true }); }
    catch { try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'script', allowHashBang: true, allowReturnOutsideFunction: true, locations: true }); } catch { continue; } }
    // WALK THE BINDING (Vega E1 review, extended for the [V2] residual): a call
    // reaches registerFrame under its imported name, a named import alias
    // (import { registerFrame as rf }), a const alias (const rf = registerFrame),
    // a DEFAULT import (import rf from '.../registerFrame.js'), or a NAMESPACE
    // member callee (import * as ns from '.../registerFrame.js'; ns.registerFrame).
    // registerFrame.js default-exports, so the last two are real reach paths; a
    // narrower NEG-B3 does not cover the whole alias space. Collect every local
    // name bound to it (Identifier callees) and every namespace binder (member
    // callees) so none of `rf(recv, computed, h)` / `ns.registerFrame(recv, computed, h)`
    // is invisible to [V2].
    const isDoorSource = (v) => typeof v === 'string' && /(^|\/)registerFrame\.js$/.test(v);
    const bound = new Set(['registerFrame']); // Identifier callees
    const nsBinders = new Set();               // `import * as ns` from the door module
    walk(ast, (n) => {
      if (n.type !== 'ImportDeclaration') return;
      const fromDoor = isDoorSource(n.source?.value);
      for (const s of n.specifiers || []) {
        if (s.type === 'ImportSpecifier' && s.imported?.name === 'registerFrame' && s.local?.name) bound.add(s.local.name);
        if (s.type === 'ImportDefaultSpecifier' && fromDoor && s.local?.name) bound.add(s.local.name);
        if (s.type === 'ImportNamespaceSpecifier' && fromDoor && s.local?.name) nsBinders.add(s.local.name);
      }
    });
    let grew = true;
    while (grew) { // fixpoint: const g = rf; const h = g; …
      grew = false;
      walk(ast, (n) => {
        if (n.type === 'VariableDeclarator' && n.id?.type === 'Identifier' && n.init?.type === 'Identifier' && bound.has(n.init.name) && !bound.has(n.id.name)) { bound.add(n.id.name); grew = true; }
      });
    }
    const doorCallLabel = (callee) => callee.type === 'Identifier' ? callee.name
      : `${callee.object.name}.${callee.property.name}`;
    walk(ast, (n) => {
      if (n.type !== 'CallExpression') return;
      const c = n.callee;
      const idCall = c?.type === 'Identifier' && bound.has(c.name);
      const nsCall = c?.type === 'MemberExpression' && !c.computed
        && c.object?.type === 'Identifier' && nsBinders.has(c.object.name)
        && c.property?.type === 'Identifier' && c.property.name === 'registerFrame';
      if (!idCall && !nsCall) return;
      const a1 = n.arguments[1];
      const literalString = a1 && a1.type === 'Literal' && typeof a1.value === 'string';
      const tConst = a1 && a1.type === 'MemberExpression' && !a1.computed
        && a1.object?.type === 'Identifier' && a1.object.name === 'T' && a1.property?.type === 'Identifier';
      if (!literalString && !tConst) bad.push(`${fp}:${n.loc?.start?.line ?? '?'} ${doorCallLabel(c)}(…, ${a1 ? a1.type : 'none'}, …) — wire must be a string literal or T.<name>`);
    });
  }
  return bad;
}
const wlv = wireLiteralViolations(files);
wlv.length === 0
  ? pass('wire-literal gate: every registerFrame call in src passes a literal / T.<name> wire')
  : fail(`wire-literal gate: non-literal wire argument(s):\n     ${wlv.join('\n     ')}`);

// ── NEG teeth: each construction must be caught ──
const synth = (code) => discover([{ path: 'src/__synth__.js', code }], { methods: SEALED, mechanismExempt: RAW_DISPATCH_ALLOWLIST });
{
  // A new DIRECT raw registration → a new site whose key is not in the baseline.
  const d = synth("x.onRoutedMessage('__new_ghost_wire__', () => {});");
  const newKey = d.sites.some((s) => s.wire === '__new_ghost_wire__');
  newKey ? pass('NEG-A1. a new direct raw registration is DISCOVERED as a site (baseline-diff would fail it)') : fail('NEG-A1. new direct raw registration not discovered');

  // An ALIASED capture → unresolved (fail-closed), never a silent pass.
  const d2 = synth("const f = transport.onNotification; f('__x__', () => {});");
  d2.unresolved.length >= 1 ? pass('NEG-A2. an aliased raw capture FAILS closed (unresolved)') : fail('NEG-A2. aliased capture not caught');

  // A loose method-name literal (the re-export / dynamic-handle class) → unresolved.
  const d3 = synth("const m = 'onRoutedMessage'; export { m };");
  d3.unresolved.length >= 1 ? pass('NEG-A3. a loose raw-method-name literal (re-export/handle) FAILS closed (unresolved)') : fail('NEG-A3. loose method-name literal not caught');
}
{
  // wire-literal gate must bite a variable, and must NOT bite a literal or T.<name>.
  const bad = wireLiteralViolations([{ path: 'src/__wl_bad__.js', code: 'const w = "sub"; registerFrame(recv, w, () => {});' }]);
  bad.length === 1 ? pass('NEG-B1. registerFrame with a variable wire FAILS the wire-literal gate') : fail(`NEG-B1. variable wire not caught (${bad.length})`);
  const good = wireLiteralViolations([{ path: 'src/__wl_ok__.js', code: "registerFrame(recv, 'sub', () => {}); registerFrame(recv, T.PUB, () => {});" }]);
  good.length === 0 ? pass('NEG-B2. registerFrame with a string literal / T.<name> wire PASSES') : fail(`NEG-B2. literal/T.<name> wire wrongly flagged (${good.length})`);
  // Vega E1 review: an IMPORT ALIAS or const alias of registerFrame must not hide a variable wire.
  const aliased = wireLiteralViolations([{ path: 'src/__wl_alias__.js', code: "import { registerFrame as rf } from '../registry/index.js'; const w = 'sub'; rf(recv, w, () => {});" }]);
  aliased.length === 1 ? pass('NEG-B3. an import-aliased registerFrame with a variable wire FAILS the gate (binding walked)') : fail(`NEG-B3. aliased registerFrame variable wire not caught (${aliased.length})`);
  // Vega E1 review [V2] residual: registerFrame.js DEFAULT-exports, so a default import
  // and a namespace member callee are real reach paths. Neither may hide a variable wire.
  const defImp = wireLiteralViolations([{ path: 'src/__wl_default__.js', code: "import rf from '../registry/registerFrame.js'; const w = 'sub'; rf(recv, w, () => {});" }]);
  defImp.length === 1 ? pass('NEG-B4. a DEFAULT-imported registerFrame with a variable wire FAILS the gate') : fail(`NEG-B4. default-imported registerFrame variable wire not caught (${defImp.length})`);
  const nsImp = wireLiteralViolations([{ path: 'src/__wl_ns__.js', code: "import * as ns from '../registry/registerFrame.js'; const w = 'sub'; ns.registerFrame(recv, w, () => {});" }]);
  nsImp.length === 1 ? pass('NEG-B5. a NAMESPACE member callee ns.registerFrame with a variable wire FAILS the gate') : fail(`NEG-B5. namespace member-callee variable wire not caught (${nsImp.length})`);
  // Source-scoped: a default/namespace import from a NON-door module must NOT be bound (no false positive).
  const notDoor = wireLiteralViolations([{ path: 'src/__wl_notdoor__.js', code: "import rf from './somethingElse.js'; import * as ns from './other.js'; const w = 'sub'; rf(recv, w, () => {}); ns.registerFrame(recv, w, () => {});" }]);
  notDoor.length === 0 ? pass('NEG-B6. default/namespace imports from a non-door module are NOT bound (no false positive)') : fail(`NEG-B6. non-door import wrongly flagged (${notDoor.length})`);
}

// keep T imported-and-used (the wire-literal gate references the T namespace by design)
void T;

console.log(`\nResult: ${ok ? 'PASS' : 'FAIL'}\n`);
process.exit(ok ? 0 : 1);
