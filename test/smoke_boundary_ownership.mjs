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
import { T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

console.log('\nREF-1.1 S5 — cross-boundary ownership fence (AST scan of src/)\n');

const R1 = buildBoundary1Registry().wiring, R2 = buildBoundary2Registry().wiring,
      R3 = buildBoundary3Registry().wiring, R4 = buildBoundary4Registry().wiring;
const REG = { B1: new Set(R1.keys()), B2: new Set(R2.keys()), B3: new Set(R3.keys()), B4: new Set(R4.keys()) };
const WIRING = { B1: R1, B2: R2, B3: R3, B4: R4 };
const REG_PREFIX = { B1: 'pubsub:', B2: 'transport:', B3: 'mesh:', B4: 'bridge:' };

const METHODS = new Set(['onRoutedMessage', 'onNotification']);
// Non-literal registration calls that are DOCUMENTED mechanisms/planes, keyed by
// (file, receiver, method, arg) so a NEW computed registration anywhere else fails.
const MECHANISM_EXEMPT = [
  { file: 'pubsub/wireHandlers.js', recv: 'dht',       method: 'onRoutedMessage', arg: 'type',     why: 'B1 routed registration helper; concrete wires from on(T.X)' },
  { file: 'dht/AxonaPeer.js',       recv: 'peer',      method: 'onRoutedMessage', arg: 'type',     why: 'transport-adapter delegation shim' },
  { file: 'web/composite.js',       recv: 't',         method: 'onNotification',  arg: 'type',     why: 'CompositeTransport fan-out over recorded handlers' },
  { file: 'dht/AxonaPeer.js',       recv: 'transport', method: 'onNotification',  arg: 'wireType', why: 'direct-messaging direct_<type> family, out of scope' },
];
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
    walk(ast, (n) => {
      // `typeof X.onNotification` is a duck-type guard, not a registration or alias
      // (it returns a string, can never register) — exclude its operand.
      if (n.type === 'UnaryExpression' && n.operator === 'typeof' && n.argument?.type === 'MemberExpression') {
        const mn = methodName(n.argument); if (mn && METHODS.has(mn)) typeofGuarded.add(n.argument);
      }
      if (n.type === 'VariableDeclarator' && n.id?.name === 'signaling' && n.init?.type === 'ObjectExpression') {
        const disp = n.init.properties.find((p) => p.key && (p.key.name === 'dispatch' || p.key.value === 'dispatch'));
        if (disp?.value) walk(disp.value, (m) => { if (m.type === 'SwitchCase' && m.test?.type === 'Literal' && typeof m.test.value === 'string') sites.push({ surface: 'bridge-ws', wire: m.test.value, site: `${fp} dispatch case '${m.test.value}'` }); });
      }
      if (n.type === 'CallExpression' && n.callee?.type === 'Identifier' && n.callee.name === 'on'
        && n.arguments[0]?.type === 'MemberExpression' && n.arguments[0].object?.type === 'Identifier' && n.arguments[0].object.name === 'T'
        && n.arguments[0].property?.type === 'Identifier') {
        const NAME = n.arguments[0].property.name, w = T[NAME];
        if (typeof w === 'string' && w) sites.push({ surface: 'routed', wire: w, site: `${fp} on(T.${NAME})` });
        else unresolved.push(`${fp} on(T.${NAME}) — T.${NAME} does not resolve`);
      }
      if (n.type === 'MemberExpression') { const mn = methodName(n); if (mn && METHODS.has(mn)) members.push(n); }
      if (n.type === 'CallExpression' && n.callee?.type === 'MemberExpression') {
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
  ['routed-dht|mesh:signal', ['B3', 'signal']],
]);
const OUT_OF_SCOPE = new Map([
  ['routed-dht|__tunneled_direct__', 'direct-messaging tunnel plane'],
  ['transport-notif|reinforce', 'synaptome-learning gossip'],
  ['transport-notif|triadic_introduce', 'synaptome-learning gossip'],
  ['transport-notif|hop_cache', 'routing-hint learning'],
  ['transport-notif|lateral_spread', 'routing-hint learning'],
  ['transport-notif|peer-leaving', 'membership-departure gossip'],
  ['t-notif|axona:direct', 'direct-messaging plane'],
]);
const classify = (e) => e.surface === 'routed' ? ['B1', e.wire]
  : (IN_SCOPE.get(`${e.surface}|${e.wire}`) || (OUT_OF_SCOPE.has(`${e.surface}|${e.wire}`) ? 'OUT' : null));

// ── walk src/ ──
const SRC = fileURLToPath(new URL('../src/', import.meta.url));
function listJs(dir) { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; }
const realFiles = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
const { sites, unresolved, parseErrors } = discover(realFiles);

// ── INV0c parse coverage / INV0 classification / INV0b resolvable ──
check('INV0c. source coverage: EVERY src file parsed as a JavaScript AST (no parse errors) — coverage is proven, not assumed',
  parseErrors.length === 0, `\n   parse errors: ${parseErrors.join(', ')}`);
{
  const unclassified = sites.filter((e) => classify(e) === null);
  check('INV0. fail-closed: every discovered registration site is classified in-scope or documented-exclusion',
    unclassified.length === 0, `\n   unclassified: ${unclassified.map((e) => e.site).join(', ')}`);
}
check('INV0b. fail-closed: every registration CALL resolves to a string-literal wire, and every referenced registration method is directly called — else unresolved; only the FOUR documented MECHANISM_EXEMPT shims are allowed non-literal',
  unresolved.length === 0, `\n   unresolved: ${unresolved.join(', ')}`);

const L = sites.map((e) => ({ e, c: classify(e) })).filter((x) => Array.isArray(x.c))
  .map((x) => ({ key: `${x.e.surface}|${x.e.wire}`, boundary: x.c[0], regWire: x.c[1], surface: x.e.surface, wire: x.e.wire }));
const A = (s, w) => L.some((x) => x.surface === s && x.wire === w);
check(`A1. AST-walked ${realFiles.length} src files; ${sites.length} registration sites → ${L.length} in-scope + ${sites.length - L.length} documented exclusions (routed B1=${L.filter((x) => x.boundary === 'B1').length})`,
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
check('INV3. B3 `signal` has TWO endpoints pinned by surface: bridge-ws|signal AND routed-dht|mesh:signal, both → B3 wire signal',
  A('bridge-ws', 'signal') && A('routed-dht', 'mesh:signal') && L.find((x) => x.surface === 'routed-dht' && x.wire === 'mesh:signal')?.regWire === 'signal' && REG.B3.has('signal'));
check('INV4. hello is TWO endpoints pinned by surface: bridge-notif|hello→B2 AND webrtc-notif|hello→B3',
  L.find((x) => x.key === 'bridge-notif|hello')?.boundary === 'B2' && L.find((x) => x.key === 'webrtc-notif|hello')?.boundary === 'B3' && REG.B2.has('hello') && REG.B3.has('hello'));
{
  const bad = []; for (const b of Object.keys(REG)) for (const [wire, info] of WIRING[b]) if (!String(info.type).startsWith(REG_PREFIX[b])) bad.push(`${b}:${wire}`);
  check('O1. every registered row type is namespaced to its boundary', bad.length === 0, `\n   ${bad.join(', ')}`);
}
check('E1. welcome → B2 only, never B3', L.find((x) => x.key === 'bridge-ws|welcome')?.boundary === 'B2' && !REG.B3.has('welcome'));
check('E2. cap-attest is the WIRE (carries write-flight-ack-v1 capability codec) → B2; no separate `write-flight-ack` wire',
  L.find((x) => x.key === 'webrtc-notif|cap-attest')?.boundary === 'B2' && REG.B2.has('cap-attest') && !REG.B2.has('write-flight-ack'));
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

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
