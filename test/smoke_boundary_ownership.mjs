// =====================================================================
// smoke_boundary_ownership.mjs — REF-1.1 S5: the cross-boundary ownership
// FENCE, over an AST scan of every registration site in src/.
//
// Aster S5 F1 (recut-3): a recursive walk + comment-stripping regex is still not
// fail-closed JavaScript parsing — the regex only matched a bare-word receiver
// directly before .onRoutedMessage/.onNotification, so x?.onNotification(),
// x['onNotification'](), and x .onNotification() were neither discovered nor
// flagged; the hand stripper did not model regex/template literals. This recut
// parses every recursively-enumerated src file with a real JavaScript AST (acorn),
// which sees ALL call forms regardless of syntax (comments and regex literals are
// not call nodes at all). Fail-closed rules:
//   · a registration CALL (X.onRoutedMessage/onNotification, dotted OR computed
//     ['onNotification'] OR optional ?.  OR any whitespace) with a STRING-LITERAL
//     first arg → a site; with a non-literal (identifier/template/other) arg →
//     UNRESOLVED unless it matches a narrowly-keyed MECHANISM_EXEMPT entry.
//   · a registration METHOD REFERENCED but not directly called (alias:
//     const r = x.onNotification; r(...)) → UNRESOLVED.
//   · a registration METHOD DESTRUCTURED out of any object
//     (const { onNotification: r } = x; r(...)) → UNRESOLVED: the bound local
//     is called with no MemberExpression, so its call site cannot be tracked.
//   · a COMPUTED DYNAMIC-METHOD callee (x[expr](...) where expr is not a string
//     literal, e.g. const m='onNotification'; x[m](...)) → UNRESOLVED: expr
//     cannot be proven not to be onNotification/onRoutedMessage. (Aster recut-4 F1.)
//   · a COMPUTED DESTRUCTURE KEY that is not a string literal ({ [expr]: r },
//     e.g. const m='onNotification'; const { [m]: r } = x) → UNRESOLVED, symmetric
//     with the computed callee rule. (Vega recut-5 F1.)
//   · the on() B1 helper called with any first arg that is not a literal
//     T.<Identifier> (on(T['SUB']), on(T[expr]), on(bareVar), on()) → UNRESOLVED:
//     only the literal T.<name> form resolves to a B1 site.
//   · a registration-method NAME as a loose string literal anywhere (const m =
//     'onNotification'; …) → UNRESOLVED. This one rule closes the whole literal-name
//     class — every Aster/Vega dynamic construction names the method with a literal,
//     so alias/destructure/callback/multi-hop are all caught by the literal alone.
//     (Exempt: the property of a bracket-access site x['onNotification'], a classified site.)
//   · a computed-member-derived local that is later invoked (const r = x[expr]; r(...))
//     → UNRESOLVED (Vega recut-6 F1). The one construction OUTSIDE this fence is a
//     method reached only via a runtime-computed name threaded across object/array
//     indirection (no literal, multi-hop) — a points-to problem, measured absent in src.
//   · on(T.X,…) → B1 routed (wire from T); signaling.dispatch switch-case string
//     labels → bridge-ws.
//   · a file that fails to parse → a hard parse-error (INV0c), so source coverage
//     is provably complete, not assumed.
// Then every site is classified IN_SCOPE (→ boundary + registry wire) or a
// documented OUT_OF_SCOPE plane; INV0 fails on anything unclassified. NEG4 injects
// a synthetic fourth FILE through the real walk; NEG5-7 feed syntax-variant,
// alias/computed/template, and comment/regex snippets and prove each is discovered
// and fails (or, for comment/regex, is correctly NOT a call and creates no site).
// =====================================================================
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join, relative } from 'node:path';
import * as acorn from 'acorn';
import { buildBoundary1Registry } from '../src/pubsub/boundary1Registry.js';
import { buildBoundary2Registry } from '../src/transport/boundary2Registry.js';
import { buildBoundary3Registry } from '../src/transport/boundary3Registry.js';
import { buildBoundary4Registry } from '../src/transport/boundary4Registry.js';
import { buildBoundary6Registry } from '../src/dht/boundary6Registry.js';
import { buildBoundary5Registry } from '../src/dht/boundary5Registry.js';
import { T } from '../src/pubsub/constants.js';
import { discoverDoors, DEFAULT_DOOR_REGISTRIES } from './lib/registrationScan.mjs';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

console.log('\nREF-1.1 S5 — cross-boundary ownership fence (AST scan of src/)\n');

const R1 = buildBoundary1Registry().wiring, R2 = buildBoundary2Registry().wiring,
      R3 = buildBoundary3Registry().wiring, R4 = buildBoundary4Registry().wiring,
      R5 = buildBoundary5Registry().wiring, R6 = buildBoundary6Registry().wiring;
// B6 is the sole COMPOSITE-keyed registry (frameWiringKey = `${transportKind} ${wire}`):
// two axona:direct legs share one wire. REG is the set of bare WIRES a boundary owns, so
// B6 maps its wiring VALUES to `.wire` (not the composite keys) — the INV0 prefix scan and
// WIRING both iterate values, so they stay correct on the composite map unchanged.
const REG = {
  B1: new Set(R1.keys()), B2: new Set(R2.keys()), B3: new Set(R3.keys()), B4: new Set(R4.keys()),
  B5: new Set(R5.keys()),   // B5 is BARE-keyed like B1–B4: R5.keys() are the 10 plain wires
  B6: new Set([...R6.values()].map((v) => v.wire)),
};
const WIRING = { B1: R1, B2: R2, B3: R3, B4: R4, B5: R5, B6: R6 };
const REG_PREFIX = { B1: 'pubsub:', B2: 'transport:', B3: 'mesh:', B4: 'bridge:', B5: 'dht:', B6: 'direct:' };

const METHODS = new Set(['onRoutedMessage', 'onNotification']);
// Non-literal registration calls that are DOCUMENTED mechanisms/planes, keyed by
// (file, receiver, method, arg) so a NEW computed registration anywhere else fails.
// E3c (SEAL — close the E0 instrumentation): EMPTY. After the E3 seal, no source file
// names a raw dispatch primitive — the canonical door and every mechanism shim reach
// the primitive only through the deposited capability closure (readDispatchCapability),
// never a literal `recv.onX(...)`. So there is no non-literal named-primitive registration
// left to exempt; any re-introduced one matches nothing and fails closed under this fence.
// The surviving shims are frozen by MODULE IDENTITY (who imports readDispatchCapability)
// in fence_readcap_importer_freeze.mjs, not by a per-call exemption here.
const MECHANISM_EXEMPT = [];
const isMechanism = (fp, recv, method, arg) => MECHANISM_EXEMPT.some((x) => fp.endsWith(x.file) && recv === x.recv && method === x.method && arg === x.arg);

// ── AST helpers ──
function walk(node, visit) {
  if (!node || typeof node.type !== 'string') return;
  visit(node);
  for (const k of Object.keys(node)) {
    const v = node[k];
    if (Array.isArray(v)) { for (const c of v) walk(c, visit); }
    else if (v && typeof v.type === 'string') walk(v, visit);
  }
}
const methodName = (m) => (m && m.type === 'MemberExpression')
  ? (!m.computed && m.property?.type === 'Identifier' ? m.property.name
    : (m.computed && m.property?.type === 'Literal' && typeof m.property.value === 'string' ? m.property.value : null))
  : null;
function receiverLabel(o) {
  if (!o) return '?';
  if (o.type === 'Identifier') return o.name;
  if (o.type === 'ThisExpression') return 'this';
  if (o.type === 'MemberExpression') return !o.computed && o.property?.type === 'Identifier' ? o.property.name
    : (o.computed && o.property?.type === 'Literal' ? String(o.property.value) : '?');
  return '?';
}

// ── discover: AST-parse a file list → { sites, unresolved, parseErrors } ──
function discover(fileList) {
  const sites = [], unresolved = [], parseErrors = [];
  for (const { path: fp, code } of fileList) {
    let ast;
    try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'module', allowHashBang: true }); }
    catch { try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'script', allowHashBang: true, allowReturnOutsideFunction: true }); }
      catch (e) { parseErrors.push(`${fp}: ${e.message}`); continue; } }
    const called = new Set(), members = [], typeofGuarded = new Set();
    const memberBoundLocals = new Map(); // local name → node, bound to a computed non-literal member (const r = x[expr])
    const calledIdents = new Set();      // identifier names used as a call callee (r(...))
    const exemptNameLiterals = new Set(); // method-name string literals that are a bracket-access property (x['onNotification']) — a classified site, not a loose handle
    walk(ast, (n) => {
      // `typeof X.onNotification` is a duck-type guard, not a registration or alias
      // (it returns a string, can never register) — exclude its operand.
      if (n.type === 'UnaryExpression' && n.operator === 'typeof' && n.argument?.type === 'MemberExpression') {
        const mn = methodName(n.argument); if (mn && METHODS.has(mn)) typeofGuarded.add(n.argument);
      }
      // A bracket-access property that names a registration method (x['onNotification']) IS a
      // classified site (handled by methodName below); exempt its literal from the loose-string rule.
      if (n.type === 'MemberExpression' && n.computed && n.property?.type === 'Literal' && typeof n.property.value === 'string' && METHODS.has(n.property.value)) {
        exemptNameLiterals.add(n.property);
      }
      // A registration-method NAME appearing as a loose string literal anywhere else is a dynamic
      // registration handle (const m = 'onNotification'; …x[m]… / {[m]:…} / passed as a callback).
      // Every Aster/Vega dynamic construction names the method with a literal; flag them all at once.
      if (n.type === 'Literal' && typeof n.value === 'string' && METHODS.has(n.value) && !exemptNameLiterals.has(n)) {
        unresolved.push(`${fp} '${n.value}' — registration-method name as a loose string literal; a dynamic registration handle`);
      }
      // Track a local bound to a computed non-literal member — by DECLARATION (const r = x[expr])
      // OR by ASSIGNMENT (r = x[expr]) — and identifier calls (r(...)); if such a local is later
      // invoked it is an untrackable dynamic method ref.
      const cmInit = (e) => e?.type === 'MemberExpression' && e.computed && !(e.property?.type === 'Literal' && typeof e.property.value === 'string');
      if (n.type === 'VariableDeclarator' && n.id?.type === 'Identifier' && cmInit(n.init)) memberBoundLocals.set(n.id.name, n);
      if (n.type === 'AssignmentExpression' && n.operator === '=' && n.left?.type === 'Identifier' && cmInit(n.right)) memberBoundLocals.set(n.left.name, n);
      if (n.type === 'CallExpression' && n.callee?.type === 'Identifier') calledIdents.add(n.callee.name);
      // register.call(...) / .apply(...) / .bind(...) invokes the bound local through Function.prototype
      // (the callee is a MemberExpression, so the direct-Identifier check above misses it) — count it.
      if (n.type === 'CallExpression' && n.callee?.type === 'MemberExpression' && !n.callee.computed
        && n.callee.object?.type === 'Identifier' && ['call', 'apply', 'bind'].includes(n.callee.property?.name)) calledIdents.add(n.callee.object.name);
      if (n.type === 'VariableDeclarator' && n.id?.name === 'signaling' && n.init?.type === 'ObjectExpression') {
        const disp = n.init.properties.find((p) => p.key && (p.key.name === 'dispatch' || p.key.value === 'dispatch'));
        if (disp?.value) walk(disp.value, (m) => { if (m.type === 'SwitchCase' && m.test?.type === 'Literal' && typeof m.test.value === 'string') sites.push({ surface: 'bridge-ws', wire: m.test.value, site: `${fp} dispatch case '${m.test.value}'` }); });
      }
      if (n.type === 'CallExpression' && n.callee?.type === 'Identifier' && n.callee.name === 'on') {
        const a0 = n.arguments[0];
        // Only the literal T.<Identifier> (non-computed) form is a resolvable B1 site.
        // on(T['SUB']), on(T[expr]), on(bareVar), on() cannot be resolved statically → fail closed.
        if (a0?.type === 'MemberExpression' && !a0.computed && a0.object?.type === 'Identifier' && a0.object.name === 'T' && a0.property?.type === 'Identifier') {
          const NAME = a0.property.name, w = T[NAME];
          if (typeof w === 'string' && w) sites.push({ surface: 'routed', wire: w, site: `${fp} on(T.${NAME})` });
          else unresolved.push(`${fp} on(T.${NAME}) — T.${NAME} does not resolve`);
        } else {
          unresolved.push(`${fp} on(${a0 ? a0.type : 'none'}) — B1 routed registration without a literal T.<name> first arg`);
        }
      }
      // A registration method DESTRUCTURED out of any object binds a bare local
      // that is then called with no MemberExpression — untrackable, so fail closed.
      if (n.type === 'ObjectPattern') {
        for (const pr of n.properties) {
          if (pr.type !== 'Property') continue; // RestElement ({...rest}) forwards other props; a later rest.method() is a normal MemberExpression call, caught below
          // A COMPUTED destructure key that is not a string literal ({ [method]: r }) can
          // resolve to a registration method and cannot be proven otherwise — fail closed,
          // symmetric with the computed dynamic-method callee rule.
          if (pr.computed && !(pr.key?.type === 'Literal' && typeof pr.key.value === 'string')) {
            unresolved.push(`${fp} { [${pr.key?.type || 'expr'}]: … } — computed dynamic destructure key, cannot prove it is not a registration method`);
            continue;
          }
          const kn = !pr.computed && pr.key?.type === 'Identifier' ? pr.key.name
            : (pr.computed && pr.key?.type === 'Literal' && typeof pr.key.value === 'string' ? pr.key.value : null);
          if (kn && METHODS.has(kn)) unresolved.push(`${fp} { ${kn} } — registration method destructured out; call site is an untrackable bare local`);
        }
      }
      if (n.type === 'MemberExpression') { const mn = methodName(n); if (mn && METHODS.has(mn)) members.push(n); }
      if (n.type === 'CallExpression' && n.callee?.type === 'MemberExpression') {
        // A COMPUTED callee whose property is not a string literal (x[expr](...)) can
        // resolve to a registration method at runtime and cannot be proven otherwise —
        // fail closed. (Bracket-property x['onNotification'] is a string Literal and is
        // handled as a normal site by methodName below.)
        if (n.callee.computed && !(n.callee.property?.type === 'Literal' && typeof n.callee.property.value === 'string')) {
          unresolved.push(`${fp} ${receiverLabel(n.callee.object)}[${n.callee.property?.type || 'expr'}](…) — computed dynamic-method call, cannot prove non-registration`);
        }
        const mn = methodName(n.callee);
        if (mn && METHODS.has(mn)) {
          called.add(n.callee);
          const recv = receiverLabel(n.callee.object), arg = n.arguments[0];
          if (arg?.type === 'Literal' && typeof arg.value === 'string') {
            const surface = mn === 'onRoutedMessage' ? 'routed-dht' : `${recv}-notif`;
            sites.push({ surface, wire: arg.value, site: `${fp} ${recv}.${mn}('${arg.value}')` });
          } else {
            const argName = arg?.type === 'Identifier' ? arg.name : (arg?.type === 'TemplateLiteral' ? 'template' : (arg ? arg.type : 'none'));
            if (!isMechanism(fp, recv, mn, argName)) unresolved.push(`${fp} ${recv}.${mn}(${argName}) — non-literal registration arg`);
          }
        }
      }
    });
    for (const m of members) if (!called.has(m) && !typeofGuarded.has(m)) unresolved.push(`${fp} ${receiverLabel(m.object)}.${methodName(m)} — referenced/aliased, not directly called`);
    for (const [name] of memberBoundLocals) if (calledIdents.has(name)) unresolved.push(`${fp} const ${name} = x[expr]; ${name}(…) — computed-member-derived local is invoked; cannot prove it is not a registration method`);
  }
  return { sites, unresolved, parseErrors };
}

// ── classification ──
const IN_SCOPE = new Map([
  ['bridge-ws|welcome', ['B2', 'welcome']], ['bridge-ws|turn', ['B4', 'turn']],
  ['bridge-ws|peer-list', ['B3', 'peer-list']], ['bridge-ws|peer-joined', ['B3', 'peer-joined']],
  ['bridge-ws|peer-left', ['B3', 'peer-left']], ['bridge-ws|signal', ['B3', 'signal']],
  ['bridge-ws|pong', ['B4', 'pong']], ['bridge-ws|version-gate', ['B4', 'version-gate']],
  ['bridge-notif|hello', ['B2', 'hello']], ['bridge-notif|hello-ack', ['B2', 'hello-ack']],
  ['webrtc-notif|hello', ['B3', 'hello']], ['webrtc-notif|hello-sig', ['B3', 'hello-sig']],
  ['webrtc-notif|cap-attest', ['B2', 'cap-attest']],
  ['routed-dht|mesh:signal', ['B3', 'mesh:signal']],   // E2.0 (Aster ASTER-E2-KEY-SCOPE): distinct routed wire, no longer folded onto `signal`
]);
const OUT_OF_SCOPE = new Map([
  // E2.4 removed axona:direct/__tunneled_direct__ (now B6 doors). E2.5 removes the five
  // dht:transport NOTIFICATION wires (reinforce, triadic_introduce, hop_cache,
  // lateral_spread, peer-leaving) — now B5 DOORS, IN scope. (The five dht:transport
  // REQUEST wires were never here: this fence's inline scan reads onNotification/
  // onRoutedMessage only, so onRequest sites were invisible until they became doors.)
  // A RAW reappearance of any of them now FAILS INV0 CLOSED (it must be a door).
]);
const classify = (e) => e.surface === 'routed' ? ['B1', e.wire]
  : (IN_SCOPE.get(`${e.surface}|${e.wire}`) || (OUT_OF_SCOPE.has(`${e.surface}|${e.wire}`) ? 'OUT' : null));

// ── walk src/ ──
const SRC = fileURLToPath(new URL('../src/', import.meta.url));
function listJs(dir) { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; }
const realFiles = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
const { sites, unresolved, parseErrors } = discover(realFiles);
// REF-1.1 E2 (council: Aster 389be28f / Vega eb71240a / Orion e2200e2b): recognize
// canonical registerFrame DOOR sites through the ONE SHARED grammar (discoverDoors
// in test/lib/registrationScan.mjs), so a migrated registration counts as a live
// endpoint under raw XOR door. The inline scanner above is the S5-hardened RAW scan;
// the door grammar is the single shared migration-aware addition both gates consume.
// DEFAULT_DOOR_REGISTRIES is empty until a boundary migrates → zero doors on the
// unmigrated tree, so L is unchanged and every ownership check is byte-identical
// (parity). Door fail-closed findings join the INV0b unresolved set.
const { doors: doorSites, unresolved: doorUnresolved } = discoverDoors(realFiles, { doorRegistries: DEFAULT_DOOR_REGISTRIES });
unresolved.push(...doorUnresolved);

// ── INV0c parse coverage / INV0 classification / INV0b resolvable ──
check('INV0c. source coverage: EVERY src file parsed as a JavaScript AST (no parse errors) — coverage is proven, not assumed',
  parseErrors.length === 0, `\n   parse errors: ${parseErrors.join(', ')}`);
{
  const unclassified = sites.filter((e) => classify(e) === null);
  check('INV0. fail-closed: every discovered registration site is classified in-scope or documented-exclusion',
    unclassified.length === 0, `\n   unclassified: ${unclassified.map((e) => e.site).join(', ')}`);
}
check('INV0b. fail-closed within the SOUNDLY-CHECKED envelope: a registration-method name never appears as a loose string literal (closes every literal-name form); no registration method is aliased or destructured out (static alias, ObjectPattern extraction, computed-key destructure); no computed dynamic-method callee; a computed-member-derived local (bound by const/let init OR assignment) is not invoked directly nor via .call/.apply/.bind; registration CALLs resolve to a string-literal wire; on() resolves to a literal T.<name> — else unresolved; only the documented MECHANISM_EXEMPT shims plus the REF-1.1 E1 canonical registerFrame door are allowed non-literal. ESCAPE BOUNDARY (this scanner is not a points-to analysis, so these are NOT claimed closed and are a recorded governance surface, measured absent in src): a computed-member value re-aliased through a further binding (const g=r; g()), invoked via Reflect.apply, passed as an argument to an invoker, or threaded across object/array/return indirection. A SOUND no-alias guarantee requires the canonical-registrar enforcement (part of the held cutover), not a syntactic scan.',
  unresolved.length === 0, `\n   unresolved: ${unresolved.join(', ')}`);

const L = sites.map((e) => ({ e, c: classify(e) })).filter((x) => Array.isArray(x.c))
  .map((x) => ({ key: `${x.e.surface}|${x.e.wire}`, boundary: x.c[0], regWire: x.c[1], surface: x.e.surface, wire: x.e.wire }));
// REF-1.1 E2 migration-aware: a canonical door site is a LIVE in-scope endpoint for
// its boundary+wire, exactly like a raw site — the registration MOVED from raw to
// door, it did not vanish. Zero doors on the unmigrated tree, so L is unchanged (parity).
// INV1 forward then also proves each door's registry OWNS its wire (wrong-boundary
// door → REG[boundary] lacks the wire → INV1 fails); INV2 backward stays satisfied
// after a boundary migrates because its door endpoints keep the wires live.
const doorL = doorSites.map((d) => ({ key: `door|${d.wire}`, boundary: d.boundary, regWire: d.wire, surface: 'door', wire: d.wire }));
L.push(...doorL);
// raw XOR door: no E0 target may be registered BOTH ways (a half-migrated site).
{
  const rawKeys = new Set(L.filter((x) => x.surface !== 'door').map((x) => `${x.boundary}|${x.regWire}`));
  const both = doorL.filter((x) => rawKeys.has(`${x.boundary}|${x.regWire}`));
  check('INVdoor. raw XOR door: no (boundary, wire) is registered BOTH raw and through the door (a half-migrated site FAILS)',
    both.length === 0, `\n   both: ${both.map((x) => `${x.boundary}:${x.regWire}`).join(', ')}`);
}
const A = (s, w) => L.some((x) => x.surface === s && x.wire === w);
check(`A1. AST-walked ${realFiles.length} src files; ${sites.length} raw sites + ${doorL.length} door sites → ${L.length} in-scope endpoints (B1=${L.filter((x) => x.boundary === 'B1').length}, raw XOR door)`,
  realFiles.length >= 60 && L.filter((x) => x.boundary === 'B1').length === 19 && L.length >= 33);

check('INV1. forward: every live in-scope endpoint’s boundary registry contains its mapped wire',
  L.every((x) => REG[x.boundary].has(x.regWire)), `\n   miss: ${L.filter((x) => !REG[x.boundary].has(x.regWire)).map((x) => x.key).join(', ')}`);
const TABLE_ONLY = new Set(['B4:client-hello', 'B4:ping', 'B4:turn-refresh', 'B4:peer-list-request']);
{
  const liveByB = {}; for (const x of L) (liveByB[x.boundary] ??= new Set()).add(x.regWire);
  const miss = [];
  for (const [b, wires] of Object.entries(REG)) for (const w of wires) if (!liveByB[b]?.has(w) && !TABLE_ONLY.has(`${b}:${w}`)) miss.push(`${b}:${w}`);
  check('INV2. backward: every registry wire is a live in-scope endpoint OR a documented TABLE_ONLY frame', miss.length === 0, `\n   miss: ${miss.join(', ')}`);
}
check('INV3. B3 signalling endpoints map to DISTINCT wires (E2.0 Aster ASTER-E2-KEY-SCOPE, do not reuse signal): bridge-ws|signal → B3 raw wire `signal` (a dispatch case, not migrated); mesh:signal → B3 DOOR wire `mesh:signal` (E2.3: migrated raw routed→door)',
  A('bridge-ws', 'signal') && L.find((x) => x.surface === 'bridge-ws' && x.wire === 'signal')?.regWire === 'signal'
  && L.some((x) => x.surface === 'door' && x.wire === 'mesh:signal' && x.boundary === 'B3')
  && REG.B3.has('signal') && REG.B3.has('mesh:signal'));
check('INV4. hello is TWO endpoints pinned by surface, BOTH now DOORs (E2.2 + E2.3): the bridge hello → B2 DOOR AND the webrtc hello → B3 DOOR — one wire `hello`, two boundaries, split by the door registry (composite._b2door vs composite._b3door)',
  L.some((x) => x.surface === 'door' && x.wire === 'hello' && x.boundary === 'B2') && L.some((x) => x.surface === 'door' && x.wire === 'hello' && x.boundary === 'B3') && REG.B2.has('hello') && REG.B3.has('hello'));
{
  const bad = []; for (const b of Object.keys(REG)) for (const [wire, info] of WIRING[b]) if (!String(info.type).startsWith(REG_PREFIX[b])) bad.push(`${b}:${wire}`);
  check('O1. every registered row type is namespaced to its boundary', bad.length === 0, `\n   ${bad.join(', ')}`);
}
check('E1. welcome → B2 only, never B3', L.find((x) => x.key === 'bridge-ws|welcome')?.boundary === 'B2' && !REG.B3.has('welcome'));
check('E2. cap-attest is the WIRE (carries write-flight-ack-v1 capability codec) → B2 (E2.2: now a DOOR); no separate `write-flight-ack` wire',
  L.some((x) => x.surface === 'door' && x.wire === 'cap-attest' && x.boundary === 'B2') && REG.B2.has('cap-attest') && !REG.B2.has('write-flight-ack'));
check('E3. peer-list → B3 only, never B4', L.find((x) => x.key === 'bridge-ws|peer-list')?.boundary === 'B3' && !REG.B4.has('peer-list'));
check('E4. peer-list-request → B4 only + TABLE_ONLY', REG.B4.has('peer-list-request') && !REG.B3.has('peer-list-request') && TABLE_ONLY.has('B4:peer-list-request'));
check('E5. turn → B4; no `turn` wire in B2', L.find((x) => x.key === 'bridge-ws|turn')?.boundary === 'B4' && !REG.B2.has('turn'));

// ── NEG teeth ──
check('NEG1. an in-scope endpoint whose registry lacks the wire FAILS INV1', [...L, { boundary: 'B2', regWire: 'ghost-wire' }].some((x) => !REG[x.boundary].has(x.regWire)));
check('NEG2. a wrong reassignment (pong B4→B2) FAILS INV1', L.map((x) => x.key === 'bridge-ws|pong' ? { ...x, boundary: 'B2' } : x).some((x) => !REG[x.boundary].has(x.regWire)));
{
  const liveByB = {}; for (const x of L) (liveByB[x.boundary] ??= new Set()).add(x.regWire);
  check('NEG3. a registered-but-not-live wire (not table-only) FAILS INV2', !liveByB.B1?.has('phantom-wire') && !TABLE_ONLY.has('B1:phantom-wire'));
}
// NEG4: inject a synthetic FOURTH FILE through the real walk → discovered + unclassified.
{
  const d = discover([...realFiles, { path: 'src/__neg4__.js', code: "x.onRoutedMessage('__neg4_ghost__', () => {});" }]);
  check('NEG4. a synthetic FOURTH file is discovered by the walk and its new registration FAILS INV0 until classified',
    d.sites.some((e) => e.wire === '__neg4_ghost__') && d.sites.some((e) => classify(e) === null));
}
// NEG5: syntax-variant registration CALLS are each DISCOVERED (not silently missed) and fail INV0.
{
  const variants = {
    'optional-chaining': "a?.onNotification('__gv__', () => {});",
    'bracket-property':  "a['onNotification']('__gv__', () => {});",
    'member-whitespace': "a\n   .onNotification('__gv__', () => {});",
  };
  const allCaught = Object.entries(variants).every(([, code]) => {
    const d = discover([{ path: 'src/__v__.js', code }]);
    return d.sites.some((e) => e.wire === '__gv__') && d.sites.some((e) => classify(e) === null);
  });
  check('NEG5. optional-chaining / bracket-property / member-whitespace registration calls are each DISCOVERED by the AST and FAIL INV0 (regex missed all three; AST catches all)', allCaught);
}
// NEG6: alias / computed-identifier / template-literal args each FAIL closed as unresolved.
{
  const fails = {
    'alias':    "const r = a.onNotification; r('__gv__');",
    'computed': "a.onNotification(someVar, () => {});",
    'template': "a.onNotification(`p_${x}`, () => {});",
  };
  const allFail = Object.values(fails).every((code) => discover([{ path: 'src/__u__.js', code }]).unresolved.length >= 1);
  check('NEG6. alias (referenced-not-called), computed-identifier arg, and template-literal arg each FAIL closed as unresolved (INV0b would fail)', allFail);
}
// NEG7: comment and regex traps are NOT calls → the AST creates NO site (no false positive).
{
  const traps = {
    'comment': "// a.onNotification('__gv__', () => {});\nconst y = 1;",
    'regex':   "const re = /a.onNotification\\('__gv__'\\)/; const z = re;",
  };
  const noSite = Object.values(traps).every((code) => { const d = discover([{ path: 'src/__t__.js', code }]); return d.sites.length === 0 && d.unresolved.length === 0 && d.parseErrors.length === 0; });
  check('NEG7. a commented-out registration and a regex literal are NOT call nodes → the AST creates no site and no false unresolved (proves the parser does not hallucinate registrations)', noSite);
}
// NEG8: method extraction (ObjectPattern) and computed dynamic-method callees each
// FAIL closed as unresolved — the two forms methodName() alone could not see. (Aster recut-4 F1.)
{
  const fails = {
    'destructure-rename':     "const { onNotification: register } = transport; register('__gv__', () => {});",
    'destructure-shorthand':  "const { onRoutedMessage } = dht; onRoutedMessage('__gv__', () => {});",
    'destructure-litkey':     "const { ['onNotification']: reg } = transport; reg('__gv__', () => {});",
    'destructure-computed':   "const method = 'onNotification'; const { [method]: reg } = transport; reg('__gv__', () => {});",
    'computed-const-method':  "const method = 'onNotification'; transport[method]('__gv__', () => {});",
    'computed-member-callee': "transport[names.reg]('__gv__', () => {});",
  };
  const allFail = Object.entries(fails).every(([, code]) => discover([{ path: 'src/__d__.js', code }]).unresolved.length >= 1);
  check('NEG8. ObjectPattern method extraction ({onNotification: r}, {onRoutedMessage}, {[\'onNotification\']: r}, {[method]: r}) and computed dynamic-method callees (transport[method](…)) each FAIL closed as unresolved (Aster/Vega recut-4 & recut-5 F1)', allFail);
}
// NEG9: the on() B1 helper fails closed on any first arg that is not a literal T.<name>.
{
  const fails = {
    'on-bracket-literal': "on(T['SUB'], () => {});",
    'on-computed-var':    "const w = 'SUB'; on(T[w], () => {});",
    'on-bare-var':        "const w = T.SUB; on(w, () => {});",
  };
  const allFail = Object.values(fails).every((code) => discover([{ path: 'src/__o__.js', code }]).unresolved.length >= 1);
  check('NEG9. the on() B1 helper FAILS closed on a bracket/computed/bare-var first arg (on(T[\'SUB\']), on(T[w]), on(w)) — only literal on(T.NAME) resolves to a site', allFail);
}
// NEG10: a computed-member alias later invoked (const r = x[method]; r(...)) — the recut-6 residual
// Vega found — and the loose-string-literal rule that closes the whole literal-name class in one pass.
{
  const fails = {
    'member-alias-litname': "const method = 'onNotification'; const r = transport[method]; r('__gv__', () => {});", // caught by BOTH string-literal and member-alias rules
    'member-alias-nolit':   "const r = transport[dyn]; r('__gv__', () => {});",                                    // no literal name → caught by member-alias (const-init) rule
    'member-alias-assign':  "let r; r = transport[dyn]; r('__gv__', () => {});",                                   // ASSIGNMENT not declaration → caught by member-alias (assignment) rule (Vega recut-7 F1)
    'member-alias-dotcall': "const r = transport[dyn]; r.call(transport, '__gv__', () => {});",                    // invoked via Function.prototype.call → caught by the .call/.apply/.bind clause (Aster recut-7 F2)
    'name-literal-loose':   "const m = 'onRoutedMessage';",                                                        // bare loose method-name literal → caught by string-literal rule
  };
  const allFail = Object.values(fails).every((code) => discover([{ path: 'src/__a__.js', code }]).unresolved.length >= 1);
  check('NEG10. a computed-member alias later invoked — by const-init OR assignment, called directly OR via .call/.apply/.bind (const r = x[m]; r(…) / let r; r = x[dyn]; r(…) / r.call(…)), incl. a runtime-only name, and any loose registration-method-name string literal each FAIL closed (Vega recut-6/7 & Aster recut-7 F1/F2 + literal-name class)', allFail);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
