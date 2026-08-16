// =====================================================================
// registrationScan.mjs — REF-1.1 shared registration-site DISCOVERY.
//
// The sound acorn walk that finds every raw registration-primitive site in a
// file list, extracted VERBATIM from smoke_boundary_ownership.mjs (the S5
// ownership fence, hardened over 8 adversarial recuts). Two consumers now share
// ONE scanner:
//   · smoke_boundary_ownership.mjs (S5) — the cross-boundary ownership fence,
//     over {onRoutedMessage, onNotification}.
//   · ref11_e0_manifest.mjs (E0) — the generated per-site inventory, over
//     {onRequest, onNotification, onRoutedMessage}.
// A hand-count diverging from this scanner is exactly the bug class E0 closes;
// the fix is that both the fence and the inventory read from the same walk.
//
// Changes vs the S5 inline original, all ADDITIVE (S5 behaviour byte-identical):
//   · `methods` and `mechanismExempt` are parameters (defaults = S5's values).
//   · acorn parses with locations:true and every pushed record carries `line`,
//     `file`, `callee`, `recv` (S5 reads none of these; its checks are unchanged).
//   · exempt non-literal registrations are RECORDED in a `mechanisms` bucket
//     instead of silently skipped (S5 does not read `mechanisms`; exempt sites
//     still never reach `sites` or `unresolved`, so its counts are unchanged).
// =====================================================================
import * as acorn from 'acorn';
import { T } from '../../src/pubsub/constants.js';

export const DEFAULT_METHODS = new Set(['onRoutedMessage', 'onNotification']);

// Non-literal registration calls that are DOCUMENTED mechanisms/planes, keyed by
// (file, receiver, method, arg) so a NEW computed registration anywhere else fails.
// `class`/`why` are metadata the E0 manifest reads; S5's default list omits them.
export const DEFAULT_MECHANISM_EXEMPT = [
  { file: 'pubsub/wireHandlers.js', recv: 'dht',       method: 'onRoutedMessage', arg: 'type',     why: 'B1 routed registration helper; concrete wires from on(T.X)' },
  { file: 'dht/AxonaPeer.js',       recv: 'peer',      method: 'onRoutedMessage', arg: 'type',     why: 'transport-adapter delegation shim' },
  { file: 'web/composite.js',       recv: 't',         method: 'onNotification',  arg: 'type',     why: 'CompositeTransport fan-out over recorded handlers' },
  { file: 'dht/AxonaPeer.js',       recv: 'transport', method: 'onNotification',  arg: 'wireType', why: 'direct-messaging direct_<type> family, out of scope' },
];

// ── AST helpers ──
export function walk(node, visit) {
  if (!node || typeof node.type !== 'string') return;
  visit(node);
  for (const k of Object.keys(node)) {
    if (k === 'loc' || k === 'start' || k === 'end' || k === 'range') continue;
    const v = node[k];
    if (Array.isArray(v)) { for (const c of v) walk(c, visit); }
    else if (v && typeof v.type === 'string') walk(v, visit);
  }
}
export const methodName = (m) => (m && m.type === 'MemberExpression')
  ? (!m.computed && m.property?.type === 'Identifier' ? m.property.name
    : (m.computed && m.property?.type === 'Literal' && typeof m.property.value === 'string' ? m.property.value : null))
  : null;
export function receiverLabel(o) {
  if (!o) return '?';
  if (o.type === 'Identifier') return o.name;
  if (o.type === 'ThisExpression') return 'this';
  if (o.type === 'MemberExpression') return !o.computed && o.property?.type === 'Identifier' ? o.property.name
    : (o.computed && o.property?.type === 'Literal' ? String(o.property.value) : '?');
  return '?';
}
const lineOf = (n) => n?.loc?.start?.line ?? 0;

// ── discover: AST-parse a file list → { sites, unresolved, parseErrors, mechanisms } ──
export function discover(fileList, { methods = DEFAULT_METHODS, mechanismExempt = DEFAULT_MECHANISM_EXEMPT } = {}) {
  const METHODS = methods;
  const exemptOf = (fp, recv, method, arg) => mechanismExempt.find((x) => fp.endsWith(x.file) && recv === x.recv && method === x.method && arg === x.arg) || null;
  const sites = [], unresolved = [], parseErrors = [], mechanisms = [];
  for (const { path: fp, code } of fileList) {
    let ast;
    try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'module', allowHashBang: true, locations: true }); }
    catch { try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'script', allowHashBang: true, allowReturnOutsideFunction: true, locations: true }); }
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
        if (disp?.value) walk(disp.value, (m) => { if (m.type === 'SwitchCase' && m.test?.type === 'Literal' && typeof m.test.value === 'string') sites.push({ surface: 'bridge-ws', wire: m.test.value, site: `${fp} dispatch case '${m.test.value}'`, file: fp, line: lineOf(m), callee: 'dispatch', recv: 'signaling' }); });
      }
      if (n.type === 'CallExpression' && n.callee?.type === 'Identifier' && n.callee.name === 'on') {
        const a0 = n.arguments[0];
        // Only the literal T.<Identifier> (non-computed) form is a resolvable B1 site.
        // on(T['SUB']), on(T[expr]), on(bareVar), on() cannot be resolved statically → fail closed.
        if (a0?.type === 'MemberExpression' && !a0.computed && a0.object?.type === 'Identifier' && a0.object.name === 'T' && a0.property?.type === 'Identifier') {
          const NAME = a0.property.name, w = T[NAME];
          if (typeof w === 'string' && w) sites.push({ surface: 'routed', wire: w, site: `${fp} on(T.${NAME})`, file: fp, line: lineOf(n), callee: 'on(T.*)', recv: 'dht' });
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
            sites.push({ surface, wire: arg.value, site: `${fp} ${recv}.${mn}('${arg.value}')`, file: fp, line: lineOf(n), callee: mn, recv });
          } else {
            const argName = arg?.type === 'Identifier' ? arg.name : (arg?.type === 'TemplateLiteral' ? 'template' : (arg ? arg.type : 'none'));
            const ex = exemptOf(fp, recv, mn, argName);
            if (!ex) unresolved.push(`${fp} ${recv}.${mn}(${argName}) — non-literal registration arg`);
            else mechanisms.push({ file: fp, line: lineOf(n), recv, callee: mn, arg: argName, class: ex.class || 'mechanism', why: ex.why });
          }
        }
      }
    });
    for (const m of members) if (!called.has(m) && !typeofGuarded.has(m)) unresolved.push(`${fp} ${receiverLabel(m.object)}.${methodName(m)} — referenced/aliased, not directly called`);
    for (const [name] of memberBoundLocals) if (calledIdents.has(name)) unresolved.push(`${fp} const ${name} = x[expr]; ${name}(…) — computed-member-derived local is invoked; cannot prove it is not a registration method`);
  }
  return { sites, unresolved, parseErrors, mechanisms };
}
