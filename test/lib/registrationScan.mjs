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

// REF-1.1 E2 MIGRATION-AWARE door discovery (council: Aster 389be28f, Vega
// eb71240a, Orion e2200e2b). A migrated registration is a registerFrame call, no
// longer a raw sealed-primitive call. The ONE canonical door grammar, enforced by
// this shared scanner (NOT by [V2] alone — [V2] decides only the wire argument;
// this decides the callee binding, the registry argument, the options shape, and
// rejects every wrapper):
//   registerFrame(recv, T.NAME, handler, { registry: <canonical boundary registry> })
// with a DIRECT registerFrame callee (no alias/wrapper), a literal T.NAME (or string)
// wire, and an options ObjectExpression whose `registry` is one of the explicit
// canonical registry references below — nothing else. Any deviation (aliased/wrapped
// registerFrame, computed/aliased wire, aliased/computed/spread registry, indirect
// options) is a NONCANONICAL look-alike and fails closed, exactly as an aliased raw
// primitive does — this is what preserves S5's closed soundness claim.
//
// DEFAULT_DOOR_REGISTRIES maps (file, context, registry-expr) → boundary. Each entry
// names the exact enclosing lexical context (<container>.<method>) the door lives in;
// context is MANDATORY (doorOf refuses a context-less entry). A boundary adds its row
// when it migrates; before E2.1 this was empty (zero doors → the raw scan verbatim).
// E2.1 B1 (2026-08-17): the 19 routed pub/sub handlers register through registerFrame
// with { registry: this._frameDoor } inside wireHandlersMethods._registerHandlers, so
// B1's row lands here and those 19 sites are discovered as doors, not raw on(T.*).
export const DEFAULT_DOOR_REGISTRIES = [
  { file: 'pubsub/wireHandlers.js', context: 'wireHandlersMethods._registerHandlers', registry: 'this._frameDoor', boundary: 'B1' },
  // E2.2 B2 (2026-08-17): transport hello / hello-ack / cap-attest register through
  // registerFrame with { registry: composite._b2door } inside the webTransport()
  // function body. (The webrtc hello/hello-sig sites in the same file are B3 and stay
  // raw until E2.3, so they do NOT match this boundary:'B2' entry.)
  { file: 'transport/web/index.js', context: 'webTransport', registry: 'composite._b2door', boundary: 'B2' },
  // E2.3 B3 (2026-08-17): the WebRTC mesh-base-auth notifications hello / hello-sig
  // register through registerFrame with { registry: composite._b3door } inside webTransport()
  // — the SAME file+context as B2, disambiguated by the registry expression (composite._b3door
  // vs composite._b2door). So bridge hello binds B2 and webrtc hello binds B3, one wire 'hello'
  // across two boundaries, split by the door registry.
  { file: 'transport/web/index.js', context: 'webTransport', registry: 'composite._b3door', boundary: 'B3' },
  // The routed B3 site mesh:signal lives in AxonaPeer._installRoutingHandlers with its own
  // door holder this._b3door (transportKind 'routed' resolved from the B3 row).
  { file: 'dht/AxonaPeer.js', context: 'AxonaPeer._installRoutingHandlers', registry: 'this._b3door', boundary: 'B3' },
  // E2.5 B5: the ten dht:transport routing frames register through registerFrame with
  // { registry: this._b5door } inside AxonaPeer._installRoutingHandlers — the SAME
  // file+context as the B3 mesh:signal door, disambiguated purely by the registry
  // expression (this._b5door vs this._b3door). B5 is bare-keyed single-primitive, so the
  // sites OMIT transportKind (each row's own transportKind selects onRequest/onNotification).
  { file: 'dht/AxonaPeer.js', context: 'AxonaPeer._installRoutingHandlers', registry: 'this._b5door', boundary: 'B5' },
  // E2.4 B6 (2026-08-17): direct messaging. axona:direct binds TWO primitives on ONE wire
  // (onRequest + onNotification) inside _installDirectHandlers on this._b6door; the routed
  // __tunneled_direct__ leg registers inside _buildDefaultAxonaManager on peer._b6door (peer===this).
  // B6 is the sole composite registry — the two axona:direct legs share this (file,context,registry)
  // row and are disambiguated by the SUPPLIED transportKind (captured below), not by this key.
  { file: 'dht/AxonaPeer.js', context: 'AxonaPeer._installDirectHandlers', registry: 'this._b6door', boundary: 'B6' },
  { file: 'dht/AxonaPeer.js', context: 'AxonaPeer._buildDefaultAxonaManager', registry: 'peer._b6door', boundary: 'B6' },
];

// Render a registry-argument expression to its explicit canonical source string, or
// null if it is not one of the two allowed explicit forms (this.<name> | <id>.<name>).
// A bare Identifier ({ registry: reg }) is an ALIAS → null → noncanonical → fail
// closed. Computed member, call, spread, anything else → null likewise.
export function canonicalRegistryExpr(node) {
  if (node && node.type === 'MemberExpression' && !node.computed && node.property?.type === 'Identifier') {
    if (node.object?.type === 'ThisExpression') return `this.${node.property.name}`;
    if (node.object?.type === 'Identifier') return `${node.object.name}.${node.property.name}`;
  }
  return null;
}

// Non-literal registration calls that are DOCUMENTED mechanisms/planes, keyed by
// (file, receiver, method, arg) so a NEW computed registration anywhere else fails.
// `class`/`why` are metadata the E0 manifest reads; S5's default list omits them.
export const DEFAULT_MECHANISM_EXEMPT = [
  // (E2.1: the pubsub/wireHandlers.js on() helper — a `dht.onRoutedMessage(type, h)`
  // shim — is DELETED; its 19 wires now register through the canonical door. The
  // exemption that documented it is removed, so a future raw dht.onRoutedMessage(type)
  // in wireHandlers fails closed.)
  { file: 'dht/AxonaPeer.js',       recv: 'peer',      method: 'onRoutedMessage', arg: 'type',     why: 'transport-adapter delegation shim' },
  { file: 'web/composite.js',       recv: 't',         method: 'onNotification',  arg: 'type',     why: 'CompositeTransport fan-out over recorded handlers' },
  { file: 'registry/registerDirectFrame.js', recv: 'recv', method: 'onNotification', arg: 'wire', why: 'registerDirectFrame direct_<type> family — the ONE named direct registrar (E3 decision 2); transitional named fallback, removed when E3b seals every transport' },
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
export function discover(fileList, { methods = DEFAULT_METHODS, mechanismExempt = DEFAULT_MECHANISM_EXEMPT, doorRegistries = DEFAULT_DOOR_REGISTRIES } = {}) {
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
  const dd = discoverDoors(fileList, { doorRegistries });
  return { sites, unresolved: unresolved.concat(dd.unresolved), parseErrors, mechanisms, doors: dd.doors };
}

// discoverDoors — the SHARED canonical registerFrame DOOR grammar (council: Aster
// 389be28f / Vega eb71240a / Orion e2200e2b), factored so BOTH gates recognize a
// door through ONE grammar: fence_e0_manifest via discover() (which merges this),
// smoke_boundary_ownership (the S5 fence) by calling it directly. A door is
// registerFrame(recv, T.NAME, handler, { registry: R }) with a DIRECT registerFrame
// callee, a literal T.NAME/string wire, and an inline { registry: <explicit
// this.X|id.X> } naming a canonical boundary registry in doorRegistries. Every
// indirection FAILS CLOSED (→ unresolved) — aliased/wrapped door, computed/aliased
// wire, aliased/computed/spread registry, non-inline options — which is what
// preserves S5's closed soundness; [V2] decides ONLY the wire arg (Vega). On the
// unmigrated tree no src file imports the door, so this yields zero doors and
// discovery is byte-identical to the raw scan (parity).
export function discoverDoors(fileList, { doorRegistries = DEFAULT_DOOR_REGISTRIES } = {}) {
  // A door binds only to an entry with the matching FILE, canonical registry
  // EXPRESSION, and the exact enclosing lexical CONTEXT (<container>.<method>).
  // Context is MANDATORY: an entry that OMITS it never binds — a refuse, not a
  // permissive default (Vega 0193ec7f, 2026-08-17). Requiring context closes the last
  // hole the (file, spelled-expr) pair left open: two classes in one file sharing
  // this._frameDoor would both match a context-less entry. With context required, the
  // door pins to B1's actual receiver method and refuses a same-spelled field in
  // another class/method/file, peer._frameDoor, an alias, or the nullable canary
  // this._frameRegistry (Aster 067e1bde / Vega da6cbdb4 / Orion fee0355a).
  const doorOf = (fp, registry, ctx) => doorRegistries.find((d) =>
    fp.endsWith(d.file) && d.registry === registry && d.context != null && d.context === ctx) || null;
  const doors = [], unresolved = [], parseErrors = [];
  for (const { path: fp, code } of fileList) {
    let ast;
    try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'module', allowHashBang: true, locations: true }); }
    catch { try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'script', allowHashBang: true, allowReturnOutsideFunction: true, locations: true }); }
      catch (e) { parseErrors.push(`${fp}: ${e.message}`); continue; } }
    // Canonical door binding = a plain named `import { registerFrame }` whose local
    // name is exactly 'registerFrame'. An aliased import, a default/namespace handle,
    // or a `const x = registerFrame` rebinding is NON-canonical: calls through it fail closed.
    let canonicalDoor = null;
    const doorAliases = new Set();
    walk(ast, (n) => {
      if (n.type === 'ImportDeclaration') {
        const fromDoorModule = /(^|\/)registerFrame\.js$/.test(n.source?.value || '') || /(^|\/)registry\/index\.js$/.test(n.source?.value || '');
        for (const sp of n.specifiers) {
          if (sp.type === 'ImportSpecifier' && sp.imported?.name === 'registerFrame') {
            if (sp.local.name === 'registerFrame') canonicalDoor = 'registerFrame';
            else doorAliases.add(sp.local.name);                       // import { registerFrame as rf }
          } else if ((sp.type === 'ImportDefaultSpecifier' || sp.type === 'ImportNamespaceSpecifier') && fromDoorModule) {
            doorAliases.add(sp.local.name);                            // default / * as ns handle to the door
          }
        }
      }
      if (n.type === 'VariableDeclarator' && n.id?.type === 'Identifier' && n.init?.type === 'Identifier' && n.init.name === 'registerFrame') doorAliases.add(n.id.name);
      if (n.type === 'AssignmentExpression' && n.operator === '=' && n.left?.type === 'Identifier' && n.right?.type === 'Identifier' && n.right.name === 'registerFrame') doorAliases.add(n.left.name);
    });
    const doorNames = new Set([canonicalDoor, ...doorAliases].filter(Boolean));
    if (!doorNames.size) continue;
    // Context-threading walk: carry the enclosing <container>.<method> lexical path so
    // a door binds only in its allowlisted receiver context. A same-spelled
    // this._frameDoor in another method/class/file, peer._frameDoor, an alias,
    // computed access, or the nullable canary this._frameRegistry all miss the
    // (file, context, registry) entry and refuse (Aster/Vega/Orion, 2026-08-17).
    const containerName = (nd) =>
      ((nd.type === 'ClassDeclaration' || nd.type === 'ClassExpression') && nd.id?.name) ? nd.id.name
      : (nd.type === 'VariableDeclarator' && nd.id?.type === 'Identifier' && (nd.init?.type === 'ObjectExpression' || nd.init?.type === 'ClassExpression')) ? nd.id.name
      // A named FunctionDeclaration is a lexical container too — a door registered
      // directly in a `function foo(){…}` body binds to context `foo` (E2.2: the B2
      // sites live in `export function webTransport(){…}`, not a class/object method).
      : (nd.type === 'FunctionDeclaration' && nd.id?.name) ? nd.id.name
      : null;
    const enclosingMethod = (nd) => (nd.type === 'MethodDefinition' || (nd.type === 'Property' && nd.method))
      ? (nd.key?.type === 'Identifier' ? nd.key.name : (nd.key?.type === 'Literal' && typeof nd.key.value === 'string' ? nd.key.value : null))
      : null;
    const inspectDoor = (n, ctx) => {
      // door.call/.apply/.bind(...) — an indirect invoke → fail closed.
      if (n.type === 'CallExpression' && n.callee?.type === 'MemberExpression' && !n.callee.computed
        && n.callee.object?.type === 'Identifier' && doorNames.has(n.callee.object.name)
        && ['call', 'apply', 'bind'].includes(n.callee.property?.name)) {
        unresolved.push(`${fp} ${n.callee.object.name}.${n.callee.property.name}(…) — indirect registerFrame invoke, not a canonical door call`); return;
      }
      // <nsHandle>.registerFrame(...) — a namespace/default door handle invoked by
      // member access is NOT the canonical direct call → fail closed.
      if (n.type === 'CallExpression' && n.callee?.type === 'MemberExpression' && !n.callee.computed
        && n.callee.object?.type === 'Identifier' && doorAliases.has(n.callee.object.name)
        && n.callee.property?.name === 'registerFrame') {
        unresolved.push(`${fp} ${n.callee.object.name}.registerFrame(…) — namespace/default door handle, not the canonical direct registerFrame call`); return;
      }
      if (n.type !== 'CallExpression' || n.callee?.type !== 'Identifier' || !doorNames.has(n.callee.name)) return;
      if (n.callee.name !== canonicalDoor) { unresolved.push(`${fp} ${n.callee.name}(…) — registerFrame called through an alias, not the canonical direct name`); return; }
      // canonical direct registerFrame(recv, wire, handler, options)
      const wireArg = n.arguments[1], optsArg = n.arguments[3];
      let wire = null;
      if (wireArg?.type === 'MemberExpression' && !wireArg.computed && wireArg.object?.type === 'Identifier' && wireArg.object.name === 'T' && wireArg.property?.type === 'Identifier') wire = (typeof T[wireArg.property.name] === 'string') ? T[wireArg.property.name] : null;
      else if (wireArg?.type === 'Literal' && typeof wireArg.value === 'string') wire = wireArg.value;
      if (wire == null) { unresolved.push(`${fp} registerFrame(…) — wire arg is not a literal T.<name>/string (noncanonical door)`); return; }
      let registryProp = null, hasSpread = false, transportKindProp = null;
      if (optsArg?.type === 'ObjectExpression') for (const p of optsArg.properties) {
        if (p.type === 'SpreadElement') hasSpread = true;
        else if (p.type === 'Property' && !p.computed && ((p.key?.type === 'Identifier' && p.key.name === 'registry') || (p.key?.type === 'Literal' && p.key.value === 'registry'))) registryProp = p;
        // E2.4 B6: capture a SUPPLIED transportKind (the composite-key case — axona:direct on two
        // primitives). Additive: B1–B5 doors OMIT it → transportKind stays null, no consumer affected.
        else if (p.type === 'Property' && !p.computed && ((p.key?.type === 'Identifier' && p.key.name === 'transportKind') || (p.key?.type === 'Literal' && p.key.value === 'transportKind'))) transportKindProp = p;
      }
      if (!optsArg || optsArg.type !== 'ObjectExpression' || hasSpread || !registryProp) { unresolved.push(`${fp} registerFrame('${wire}', …) — options must be an inline { registry: … } object with no spread (noncanonical door)`); return; }
      const registryExpr = canonicalRegistryExpr(registryProp.value);
      if (registryExpr == null) { unresolved.push(`${fp} registerFrame('${wire}', { registry: <non-explicit> }) — registry must be an explicit this.<name>/<id>.<name>, not an alias/computed (noncanonical door)`); return; }
      const transportKind = (transportKindProp?.value?.type === 'Literal' && typeof transportKindProp.value.value === 'string') ? transportKindProp.value.value : null;
      const entry = doorOf(fp, registryExpr, ctx);
      if (!entry) { unresolved.push(`${fp} registerFrame('${wire}', { registry: ${registryExpr} }) @ ${ctx || '<top>'} — not a canonical (file, context, registry) boundary door (wrong receiver/context, wrong-boundary, or unlisted)`); return; }
      doors.push({ surface: 'door', wire, site: `${fp} registerFrame('${wire}', { registry: ${registryExpr} })`, file: fp, line: lineOf(n), callee: 'registerFrame', registry: registryExpr, context: ctx, transportKind, boundary: entry.boundary });
    };
    const walkDoors = (node, ctx) => {
      if (!node || typeof node.type !== 'string') return;
      let childCtx = ctx;
      const cn = containerName(node); if (cn) childCtx = cn;                  // enter a named object/class
      const mn = enclosingMethod(node); if (mn) childCtx = (ctx ? ctx + '.' : '') + mn;   // enter a method → container.method
      inspectDoor(node, childCtx);
      for (const k of Object.keys(node)) {
        if (k === 'loc' || k === 'start' || k === 'end' || k === 'range') continue;
        const v = node[k];
        if (Array.isArray(v)) { for (const c of v) walkDoors(c, childCtx); }
        else if (v && typeof v.type === 'string') walkDoors(v, childCtx);
      }
    };
    walkDoors(ast, null);
  }
  return { doors, unresolved, parseErrors };
}
