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
    // Default-import door source = the DEFINITION module (registerFrame.js default-exports).
    const isDefaultDoorSource = (v) => typeof v === 'string' && /(^|\/)registerFrame\.js$/.test(v);
    // Namespace door sources (Vega 5aa39c82) = the definition module AND the public
    // barrel registry/index.js, which re-exports registerFrame — so `import * as ns
    // from '../registry/index.js'; ns.registerFrame(...)` is a real public reach path.
    // Cover extensionless and bare-directory import forms of the barrel too.
    const isNsDoorSource = (v) => typeof v === 'string' && (
      /(^|\/)registerFrame\.js$/.test(v)
      || /(^|\/)registry\/index(\.js)?$/.test(v)
      || /(^|\/)registry$/.test(v)
    );
    const bound = new Set(['registerFrame']); // Identifier callees
    const nsBinders = new Set();               // `import * as ns` from a door module (definition or barrel)
    walk(ast, (n) => {
      if (n.type !== 'ImportDeclaration') return;
      const src = n.source?.value;
      for (const s of n.specifiers || []) {
        if (s.type === 'ImportSpecifier' && s.imported?.name === 'registerFrame' && s.local?.name) bound.add(s.local.name);
        if (s.type === 'ImportDefaultSpecifier' && isDefaultDoorSource(src) && s.local?.name) bound.add(s.local.name);
        if (s.type === 'ImportNamespaceSpecifier' && isNsDoorSource(src) && s.local?.name) nsBinders.add(s.local.name);
      }
    });
    // extract `registerFrame` off a door namespace `ns` into an Identifier: matches
    // both `ns.registerFrame` (member) and `{ registerFrame: x }` (ObjectPattern key).
    const nsExtractName = (init, id) => {
      if (init?.type === 'MemberExpression' && !init.computed
          && init.object?.type === 'Identifier' && nsBinders.has(init.object.name)
          && init.property?.type === 'Identifier' && init.property.name === 'registerFrame'
          && id?.type === 'Identifier') return id.name; // (5) const rf = ns.registerFrame
      return null;
    };
    let grew = true;
    while (grew) { // fixpoint (Aster/Vega V2 enum): Identifier→Identifier copy, plus the two
                   // namespace-extraction hops 5 & 6 that copy a member/destructure into an Identifier
      grew = false;
      walk(ast, (n) => {
        if (n.type !== 'VariableDeclarator') return;
        // Identifier -> Identifier copy: const g = rf; const h = g; …
        if (n.id?.type === 'Identifier' && n.init?.type === 'Identifier' && bound.has(n.init.name) && !bound.has(n.id.name)) { bound.add(n.id.name); grew = true; }
        // (5) const rf = ns.registerFrame  (door-namespace member copied to an Identifier)
        const m = nsExtractName(n.init, n.id);
        if (m && !bound.has(m)) { bound.add(m); grew = true; }
        // (6) const { registerFrame: rf } = ns  (door-namespace destructured to an Identifier)
        if (n.id?.type === 'ObjectPattern' && n.init?.type === 'Identifier' && nsBinders.has(n.init.name)) {
          for (const p of n.id.properties || []) {
            if (p.type === 'Property' && !p.computed && p.key?.type === 'Identifier' && p.key.name === 'registerFrame'
                && p.value?.type === 'Identifier' && !bound.has(p.value.name)) { bound.add(p.value.name); grew = true; }
          }
        }
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
  // Vega 5aa39c82: the public barrel registry/index.js re-exports registerFrame, so a
  // barrel-namespace member callee is a real reach path and must be bound too.
  const barrelNs = wireLiteralViolations([{ path: 'src/__wl_barrel__.js', code: "import * as ns from '../registry/index.js'; const w = 'sub'; ns.registerFrame(recv, w, () => {});" }]);
  barrelNs.length === 1 ? pass('NEG-B7. a barrel-namespace callee ns.registerFrame (registry/index.js) with a variable wire FAILS the gate') : fail(`NEG-B7. barrel-namespace member-callee variable wire not caught (${barrelNs.length})`);
  const barrelDir = wireLiteralViolations([{ path: 'src/__wl_barreldir__.js', code: "import * as ns from '../registry'; const w = 'sub'; ns.registerFrame(recv, w, () => {});" }]);
  barrelDir.length === 1 ? pass('NEG-B8. a bare-directory barrel namespace (../registry) callee with a variable wire FAILS the gate') : fail(`NEG-B8. bare-directory barrel namespace not caught (${barrelDir.length})`);
  // Aster bf095d87 = Vega 725a61f0: the two namespace-EXTRACTION hops that copy a door
  // namespace member/destructure into an Identifier, then call it. Fold into the fixpoint.
  const nsMember = wireLiteralViolations([{ path: 'src/__wl_nsmember__.js', code: "import * as ns from '../registry/index.js'; const f = ns.registerFrame; const w = 'sub'; f(recv, w, () => {});" }]);
  nsMember.length === 1 ? pass('NEG-B9. `const f = ns.registerFrame; f(recv, var, h)` (shape 5) FAILS the gate') : fail(`NEG-B9. ns-member extraction not caught (${nsMember.length})`);
  const nsDestr = wireLiteralViolations([{ path: 'src/__wl_nsdestr__.js', code: "import * as ns from '../registry/index.js'; const { registerFrame: f } = ns; const w = 'sub'; f(recv, w, () => {});" }]);
  nsDestr.length === 1 ? pass('NEG-B10. `const { registerFrame: f } = ns; f(recv, var, h)` (shape 6) FAILS the gate') : fail(`NEG-B10. ns-destructure extraction not caught (${nsDestr.length})`);
  // no false positive: the same extraction hops off a NON-door namespace must NOT bind.
  const nsExtractNotDoor = wireLiteralViolations([{ path: 'src/__wl_nsx_notdoor__.js', code: "import * as ns from './other.js'; const f = ns.registerFrame; const { registerFrame: g } = ns; const w = 'sub'; f(recv, w, () => {}); g(recv, w, () => {});" }]);
  nsExtractNotDoor.length === 0 ? pass('NEG-B11. extraction hops off a NON-door namespace are NOT bound (no false positive)') : fail(`NEG-B11. non-door extraction wrongly flagged (${nsExtractNotDoor.length})`);
  // Vega 7e3232d6: OptionalCallExpression — f?.(…) and ns.registerFrame?.(…) — is a
  // CallExpression the walk must still match (statically decidable). Add teeth.
  const optIdent = wireLiteralViolations([{ path: 'src/__wl_optident__.js', code: "import { registerFrame as f } from '../registry/index.js'; const w = 'sub'; f?.(recv, w, () => {});" }]);
  optIdent.length === 1 ? pass('NEG-B12. an optional call f?.(recv, var, h) on a bound name FAILS the gate') : fail(`NEG-B12. optional identifier call not caught (${optIdent.length})`);
  const optNs = wireLiteralViolations([{ path: 'src/__wl_optns__.js', code: "import * as ns from '../registry/index.js'; const w = 'sub'; ns.registerFrame?.(recv, w, () => {});" }]);
  optNs.length === 1 ? pass('NEG-B13. an optional barrel-namespace call ns.registerFrame?.(recv, var, h) FAILS the gate') : fail(`NEG-B13. optional ns member call not caught (${optNs.length})`);
}

// keep T imported-and-used (the wire-literal gate references the T namespace by design)
void T;

console.log(`\nResult: ${ok ? 'PASS' : 'FAIL'}\n`);
process.exit(ok ? 0 : 1);
