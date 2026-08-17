// =====================================================================
// smoke_e2_door_grammar.mjs — REF-1.1 E2.1 STANDALONE pre-migration proof
// (council: Aster 389be28f / Vega eb71240a / Orion e2200e2b).
//
// The ONE shared canonical registerFrame DOOR grammar (discoverDoors in
// test/lib/registrationScan.mjs) recognizes a MIGRATED registration site and FAILS
// CLOSED on every noncanonical look-alike. This is the adversarial parser proof the
// council required for aliases, computed wires, wrappers, spread/indirect options,
// and unlisted registries — the guarantee that teaching the E0/ownership gates to
// see registerFrame does NOT relax S5's closed soundness. A scan is not a points-to
// analysis, so the grammar is deliberately NARROW: exactly one shape is a door,
// everything else fails closed. [V2] (fence_raw_dispatch_gate) decides ONLY the wire
// argument; this grammar decides the callee binding, the options shape, and the
// registry, and rejects every wrapper (Vega eb71240a).
//
// The canonical door, and the ONLY shape recognized:
//   registerFrame(recv, T.NAME, handler, { registry: <this.X | id.X in the table> })
// with a DIRECT `registerFrame` callee bound by a plain named import.
//
// Plus the REAL-TREE checks: with an EMPTY door table the grammar is inert (zero
// doors, whatever the tree holds); with the DEFAULT table (E2.1 B1 landed) the migrated
// tree yields EXACTLY B1's 19 door sites, all bound to wireHandlersMethods.
// _registerHandlers, and ZERO residual raw registration remains for any B1 wire (raw
// XOR door) — proven with teeth by an injected-residual negative (P2b-neg).
//
// E2.2 B2 EVIDENCE (2026-08-17, Aster 3527d5b0 / Vega 572106bb, on David's confirmation
// 8241855e): the F-family proves the FunctionDeclaration container widening (containerName
// now names `function webTransport(){…}`) fails closed — intended context binds, an
// unrelated function / a same-spelled method / a top-level lookalike / a wrong file / a
// same-named arrow all REFUSE. The P3-family proves B2-scoped ZERO-RESIDUAL RAW: the 3
// migrated (recv,wire) pairs hold zero raw sites, scoped by RECEIVER so B3's shared-wire
// webrtc 'hello'/'hello-sig' (deferred to E2.3) are not miscounted, with a teeth negative.
//
// Run: node test/smoke_e2_door_grammar.mjs
// =====================================================================
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join, relative } from 'node:path';
import { discoverDoors, discover } from './lib/registrationScan.mjs';
import { T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

console.log('\nREF-1.1 E2.1 — canonical registerFrame door grammar (adversarial + parity)\n');

const NAME = 'SUB';                               // T.SUB — a real Boundary-1 wire
// Context is MANDATORY (Vega 0193ec7f): every real door entry names its enclosing
// <container>.<method>, so the grammar fixtures below live inside that context. `inCtx`
// wraps a body in the allowlisted receiver method so the canonical door can bind;
// the negatives then fail purely for their own indirection, not a context mismatch.
const CTXPATH = 'Mgr._registerHandlers';
const DOORS = [{ file: 'fx.js', context: CTXPATH, registry: 'this._frameDoor', boundary: 'B1' }];
const IMP = "import { registerFrame } from '../registry/index.js';\n";
const inCtx = (body) => `const Mgr = { _registerHandlers() { ${body} } };`;
const run = (body, imp = IMP, doorRegistries = DOORS) => discoverDoors([{ path: 'src/fx.js', code: imp + inCtx(body) }], { doorRegistries });

// ── POSITIVE: the canonical door is recognized as exactly one boundary site ──
{
  const r = run(`registerFrame(this.dht, T.${NAME}, this._onSub.bind(this), { registry: this._frameDoor });`);
  check('D1. canonical registerFrame(recv, T.SUB, handler, { registry: this._frameDoor }) → ONE B1 door site, zero unresolved',
    r.doors.length === 1 && r.doors[0].boundary === 'B1' && r.doors[0].wire === T[NAME] && r.unresolved.length === 0,
    `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}
{
  const r = run(`registerFrame(this.dht, '${T[NAME]}', h, { registry: this._frameDoor });`);
  check('D1b. a string-literal wire is equally canonical (recognized, zero unresolved)', r.doors.length === 1 && r.unresolved.length === 0,
    `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}
{
  // extra allowed options (transportKind) alongside registry stay canonical.
  const r = run(`registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor, transportKind: 'routed' });`);
  check('D1c. sibling options (transportKind) alongside an explicit registry are still canonical', r.doors.length === 1 && r.unresolved.length === 0);
}

// ── NEGATIVE: every indirection FAILS CLOSED (no door recognized, ≥1 unresolved) ──
const neg = {
  'D2  aliased import (registerFrame as rf)': { imp: "import { registerFrame as rf } from '../registry/index.js';\n", body: `rf(this.dht, T.${NAME}, h, { registry: this._frameDoor });` },
  'D3  const-alias rebinding':                { body: `const rf = registerFrame; rf(this.dht, T.${NAME}, h, { registry: this._frameDoor });` },
  'D4  .call indirect invoke':                { body: `registerFrame.call(null, this.dht, T.${NAME}, h, { registry: this._frameDoor });` },
  'D4b .bind indirect invoke':                { body: `registerFrame.bind(this)(this.dht, T.${NAME}, h, { registry: this._frameDoor });` },
  'D5  namespace member call (ns.registerFrame)': { imp: "import * as ns from '../registry/registerFrame.js';\n", body: `ns.registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor });` },
  'D5b default-import handle call':           { imp: "import rf from '../registry/registerFrame.js';\n", body: `rf(this.dht, T.${NAME}, h, { registry: this._frameDoor });` },
  'D6  computed wire T[w]':                    { body: `const w = 'x'; registerFrame(this.dht, T[w], h, { registry: this._frameDoor });` },
  'D6b bracket wire T[\'SUB\']':               { body: `registerFrame(this.dht, T['${NAME}'], h, { registry: this._frameDoor });` },
  'D7  spread in options':                     { body: `registerFrame(this.dht, T.${NAME}, h, { ...o, registry: this._frameDoor });` },
  'D8  non-inline options (a variable)':       { body: `const o = { registry: this._frameDoor }; registerFrame(this.dht, T.${NAME}, h, o);` },
  'D9  aliased registry ({ registry: reg })':  { body: `const reg = this._frameDoor; registerFrame(this.dht, T.${NAME}, h, { registry: reg });` },
  'D10 computed registry (this[k])':           { body: `registerFrame(this.dht, T.${NAME}, h, { registry: this[k] });` },
  'D11 unlisted registry (this._other)':       { body: `registerFrame(this.dht, T.${NAME}, h, { registry: this._other });` },
  'D12 missing options object':                { body: `registerFrame(this.dht, T.${NAME}, h);` },
};
for (const [name, f] of Object.entries(neg)) {
  // each negative runs INSIDE the allowlisted context, so it fails for its own
  // indirection (alias/computed/wrapper/registry), never merely a context mismatch.
  const r = discoverDoors([{ path: 'src/fx.js', code: (f.imp || IMP) + inCtx(f.body) }], { doorRegistries: DOORS });
  check(`${name} → FAILS CLOSED (no door, ≥1 unresolved)`, r.doors.length === 0 && r.unresolved.length >= 1,
    `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}

// ── D13: recognition is TABLE-GATED — an empty door table refuses even a canonical
// shape. This is exactly why the standalone recut ships an empty table: zero doors on
// the real tree, so parity is exact. ──
{
  const r = run(`registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor });`, IMP, []);
  check('D13. EMPTY door table → even a canonical shape is unresolved (recognition is table-gated; the standalone recut ships zero doors → exact parity)',
    r.doors.length === 0 && r.unresolved.length >= 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}
// ── D14: NO false positive — a file that never imports the door produces no door and
// no unresolved, however registerFrame-shaped its own local helpers look. ──
{
  const r = discoverDoors([{ path: 'src/fx.js', code: "function registerFrame(a,b,c,d){}\nregisterFrame(this.dht, 'x', h, { registry: this._frameDoor });" }], { doorRegistries: DOORS });
  check('D14. a file with a LOCAL registerFrame (no door import) yields no door and no unresolved — the grammar keys on the imported door binding, not the bare name',
    r.doors.length === 0 && r.unresolved.length === 0, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}

// ── RECEIVER-CONTEXT BINDING (Aster 067e1bde / Vega da6cbdb4 / Orion fee0355a): a
// door entry may bind this.<field> to its exact lexical context <container>.<method>.
// this._frameDoor is B1's canonical reference ONLY inside that receiver context; a
// SAME-SPELLED field in another method/class (Vega's "two classes in one file" gap),
// peer._frameDoor, or the nullable canary this._frameRegistry must refuse. ──
const CTX = [{ file: 'fx.js', context: 'Mgr._registerHandlers', registry: 'this._frameDoor', boundary: 'B1' }];
const runCtx = (body) => discoverDoors([{ path: 'src/fx.js', code: IMP + body }], { doorRegistries: CTX });
{
  const r = runCtx(`const Mgr = { _registerHandlers() { registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor }); } };`);
  check('R1. context-bound: this._frameDoor inside the allowlisted Mgr._registerHandlers → one B1 door, zero unresolved',
    r.doors.length === 1 && r.doors[0].boundary === 'B1' && r.doors[0].context === 'Mgr._registerHandlers' && r.unresolved.length === 0,
    `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}
const ctxNeg = {
  'R2 same field, wrong method (Mgr._other)':       `const Mgr = { _other() { registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor }); } };`,
  'R3 same field, ANOTHER class same file (Vega)':  `const Mgr = { _registerHandlers() {} }; class Other { _registerHandlers() { registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor }); } }`,
  'R4 peer._frameDoor (wrong receiver)':            `const Mgr = { _registerHandlers() { registerFrame(this.dht, T.${NAME}, h, { registry: peer._frameDoor }); } };`,
  'R5 nullable canary this._frameRegistry':         `const Mgr = { _registerHandlers() { registerFrame(this.dht, T.${NAME}, h, { registry: this._frameRegistry }); } };`,
  'R6 top-level, no enclosing context':             `registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor });`,
};
for (const [name, body] of Object.entries(ctxNeg)) {
  const r = runCtx(body);
  check(`${name} → FAILS CLOSED vs the context-bound B1 entry (no door, ≥1 unresolved)`, r.doors.length === 0 && r.unresolved.length >= 1,
    `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}

// ── R7: context is MANDATORY — an entry that OMITS context is a REFUSE, not a
// permissive default (Vega 0193ec7f). A context-less entry can never bind, even for a
// canonical shape in the right file/registry; this is what closes the last
// (file, spelled-expr)-alone hole (two classes in one file sharing this._frameDoor). ──
{
  const noCtx = [{ file: 'fx.js', registry: 'this._frameDoor', boundary: 'B1' }];
  const r = discoverDoors([{ path: 'src/fx.js', code: IMP + inCtx(`registerFrame(this.dht, T.${NAME}, h, { registry: this._frameDoor });`) }], { doorRegistries: noCtx });
  check('R7. a door entry that OMITS context REFUSES even a canonical shape in the right file/registry — context is mandatory (Vega 0193ec7f: an omitted-context entry is a refuse, not a permissive default)',
    r.doors.length === 0 && r.unresolved.length >= 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
}

// ── F-family: the FunctionDeclaration CONTAINER FENCE (E2.2 B2 shared-scanner change).
// containerName now names a bare `function foo(){…}` so a door registered directly in
// `export function webTransport(){…}` resolves to context 'webTransport' and can bind the
// B2 entry. Aster (3527d5b0) required this widening to be proven fail-closed: the INTENDED
// function context PASSES, while (F2) an unrelated function, (F3) a same-spelled METHOD in
// another container, (F4) a top-level lookalike, (F5) the same function in the wrong file,
// and (F6) a same-named arrow/const (NOT a FunctionDeclaration) all REFUSE. This shows the
// new branch widens the lexical-container surface by exactly one node kind and nothing more.
{
  const B2FILE = 'transport/web/index.js';           // the real B2 site file
  const B2REG  = 'composite._b2door';                // the real B2 door registry expr
  const B2DOORS = [{ file: B2FILE, context: 'webTransport', registry: B2REG, boundary: 'B2' }];
  // 'hello' is a real bare-keyed B2 wire (a string literal, not T.<name> — B2 is bare-keyed).
  const DOORCALL = `registerFrame(bridge, 'hello', h, { registry: ${B2REG} });`;
  const runRaw = (src, path = 'src/' + B2FILE, doorRegistries = B2DOORS) =>
    discoverDoors([{ path, code: IMP + src }], { doorRegistries });

  {
    const r = runRaw(`export function webTransport(){ ${DOORCALL} }`);
    check("F1. INTENDED: a door in `export function webTransport(){…}` binds to context 'webTransport' → ONE B2 door, zero unresolved (the widening's positive case)",
      r.doors.length === 1 && r.doors[0].boundary === 'B2' && r.doors[0].context === 'webTransport' && r.doors[0].wire === 'hello' && r.unresolved.length === 0,
      `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runRaw(`export function otherFn(){ ${DOORCALL} }`);
    check("F2. UNRELATED FUNCTION: the SAME door shape in `function otherFn(){…}` → context 'otherFn' ≠ 'webTransport' → ZERO doors, refused (fails closed)",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runRaw(`class Composite { webTransport(){ ${DOORCALL} } }`);
    check("F3. SAME-SPELLED METHOD: `class Composite { webTransport(){…} }` → context 'Composite.webTransport' ≠ 'webTransport' → ZERO doors, refused (a method must not collide with the bare function)",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runRaw(DOORCALL);
    check("F4. TOP-LEVEL LOOKALIKE: the SAME door shape at top level (no container) → context <top> (null) → ZERO doors, refused",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runRaw(`export function webTransport(){ ${DOORCALL} }`, 'src/transport/node/index.js');
    check("F5. WRONG FILE: the intended `webTransport(){…}` door in a DIFFERENT file (node, not web) → file mismatch → ZERO doors, refused",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runRaw(`const webTransport = () => { ${DOORCALL} };`);
    check("F6. NARROWNESS: a same-named arrow `const webTransport = () => {…}` is NOT a FunctionDeclaration → no container → context <top> → ZERO doors, refused (the branch widens by exactly one node kind)",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
}

// ── G-family: the ROUTED AxonaPeer CONTEXT FENCE (E2.3 B3, Aster ASTER-E23-B3-AUTH-REVIEW).
// The routed mesh:signal door lives in dht/AxonaPeer.js, context 'AxonaPeer._installRoutingHandlers',
// on this._b3door. B3 added NO new scanner branch — a class method resolves through the existing
// container/method threading (same machinery as B1's wireHandlersMethods._registerHandlers) — so
// this family makes the fail-closed binding of that SPECIFIC routed context explicit and
// source-backed: the intended context binds; a different method, a different class, top-level, and
// the wrong file all REFUSE. (Flag-off byte-identical equivalence for these B3 wires — including
// mesh:signal — is owned by smoke_boundary3_registry.mjs's D. FLAG-OFF IDENTITY block, 39/39.)
{
  const PFILE = 'dht/AxonaPeer.js';
  const PCTX = 'AxonaPeer._installRoutingHandlers';
  const PREG = 'this._b3door';
  const B3PEER = [{ file: PFILE, context: PCTX, registry: PREG, boundary: 'B3' }];
  const DOORCALL = `registerFrame(this, 'mesh:signal', h, { registry: ${PREG} });`;
  const runP = (src, path = 'src/' + PFILE, doorRegistries = B3PEER) =>
    discoverDoors([{ path, code: IMP + src }], { doorRegistries });

  {
    const r = runP(`class AxonaPeer { _installRoutingHandlers(){ ${DOORCALL} } }`);
    check("G1. INTENDED: mesh:signal door in `class AxonaPeer { _installRoutingHandlers(){…} }` binds context 'AxonaPeer._installRoutingHandlers' → ONE B3 routed door, zero unresolved",
      r.doors.length === 1 && r.doors[0].boundary === 'B3' && r.doors[0].context === PCTX && r.doors[0].wire === 'mesh:signal' && r.unresolved.length === 0,
      `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runP(`class AxonaPeer { someOtherMethod(){ ${DOORCALL} } }`);
    check("G2. WRONG METHOD: the same door in `AxonaPeer.someOtherMethod(){…}` → context ≠ '…._installRoutingHandlers' → ZERO doors, refused",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runP(`class Other { _installRoutingHandlers(){ ${DOORCALL} } }`);
    check("G3. WRONG CLASS: the same method name in `class Other` → context 'Other._installRoutingHandlers' → ZERO doors, refused",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runP(DOORCALL);
    check("G4. TOP-LEVEL: the same door at top level (no class/method) → context <top> (null) → ZERO doors, refused",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    const r = runP(`class AxonaPeer { _installRoutingHandlers(){ ${DOORCALL} } }`, 'src/pubsub/wireHandlers.js');
    check("G5. WRONG FILE: the intended AxonaPeer routed door in a DIFFERENT file (pubsub/wireHandlers.js) → file mismatch → ZERO doors, refused",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
}

// ── REAL TREE: (P1) with an EMPTY door table the grammar is inert — zero doors,
// whatever the tree holds; (P2) with the DEFAULT (B1) table the migrated tree yields
// EXACTLY B1's 19 doors, all in wireHandlersMethods._registerHandlers, and the 19 raw
// on(T.*) sites are gone — the registrations moved raw→door, conserved (raw XOR door). ──
{
  const SRC = fileURLToPath(new URL('../src/', import.meta.url));
  const listJs = (dir) => { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; };
  const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
  const METH = { methods: new Set(['onRequest', 'onNotification', 'onRoutedMessage']) };
  const inert = discover(files, { ...METH, doorRegistries: [] });
  check('P1. INERT: an empty door table yields ZERO doors on the real tree — the grammar contributes nothing until a boundary is listed',
    inert.doors.length === 0, `\n   doors=${inert.doors.length}`);
  const migd = discover(files, METH);   // DEFAULT_DOOR_REGISTRIES = B1
  const b1doors = migd.doors.filter((d) => d.boundary === 'B1');
  check('P2. MIGRATED: the default (B1) table discovers EXACTLY 19 B1 doors, all bound to wireHandlersMethods._registerHandlers',
    b1doors.length === 19 && b1doors.every((d) => d.context === 'wireHandlersMethods._registerHandlers'),
    `\n   b1doors=${b1doors.length} contexts=${[...new Set(migd.doors.map((d) => d.context))].join(',')}`);
  // P2b (Vega a901bc92): the prior check filtered callee==='on(T.*)', which is trivially
  // zero once the on() helper is deleted — discover() emits that callee only for a live
  // on(T.X) call, so the filter passed on ANY migrated tree and guarded nothing (a raw
  // this.dht.onRoutedMessage('sub', h) — callee 'onRoutedMessage' — would slip past it).
  // The substantive property is ZERO RESIDUAL RAW: no B1 wire is registered raw ANYWHERE
  // (B1 raw sites === 0), and wireHandlers.js holds zero residual raw registration sites
  // (any callee). Every B1 wire lives at a door — raw XOR door, proven fail-closed.
  const b1Wires = new Set(b1doors.map((d) => d.wire));
  const b1RawSites = migd.sites.filter((s) => b1Wires.has(s.wire));
  const wireHandlersRaw = migd.sites.filter((s) => s.file && s.file.endsWith('pubsub/wireHandlers.js'));
  check('P2b. ZERO-RESIDUAL RAW: no B1 wire is registered raw anywhere (B1 raw sites === 0) AND wireHandlers.js holds zero residual raw registration sites — every B1 wire lives at a door (raw XOR door)',
    b1RawSites.length === 0 && wireHandlersRaw.length === 0,
    `\n   b1RawSites=${JSON.stringify(b1RawSites.map((s) => s.site))} wireHandlersRaw=${JSON.stringify(wireHandlersRaw.map((s) => s.site))}`);
  check('P2c. no door-grammar false unresolved on the real tree under the default (B1) table',
    discoverDoors(files).unresolved.length === 0, `\n   unresolved=${JSON.stringify(discoverDoors(files).unresolved)}`);
  // P2b-neg (Vega a901bc92): prove the zero-residual guard has TEETH — it is not vacuous
  // the way the old callee==='on(T.*)' filter was. Inject a residual raw
  // this.dht.onRoutedMessage('sub', …) (a B1 wire) beside the door; discover() must surface
  // it as a wireHandlers raw site, so P2b's b1RawSites/wireHandlersRaw would be non-zero and
  // P2b would FAIL. (The old filter counted only on(T.*) and would have passed this.)
  {
    const resid = discover([{ path: 'pubsub/wireHandlers.js', code: IMP + `const Mgr = { _registerHandlers() { registerFrame(this.dht, T.SUB, h, { registry: this._frameDoor }); this.dht.onRoutedMessage('sub', h2); } };` }], METH);
    const wh = resid.sites.filter((s) => s.file && s.file.endsWith('pubsub/wireHandlers.js') && s.wire === 'sub');
    check('P2b-neg. the zero-residual guard has TEETH: a residual raw dht.onRoutedMessage(\'sub\', …) beside the door IS discovered as a wireHandlers raw site — P2b would FAIL (not vacuous)',
      wh.length >= 1, `\n   wireHandlers raw sites=${JSON.stringify(resid.sites.map((s) => s.site))}`);
  }
}

// ── REAL TREE — B2 (E2.2, Vega: B2-scoped zero-residual). The 3 auth doors exist, and
// NONE of the 3 migrated B2 (recv, wire) pairs — (bridge,'hello'), (bridge,'hello-ack'),
// (webrtc,'cap-attest') — remains a raw site, WITHOUT miscounting the B3 webrtc
// hello/hello-sig sites that share the 'hello' wire and STAY RAW until E2.3. The scope
// is (recv, wire), not wire alone: 'hello' belongs to BOTH the migrated bridge door (B2)
// and the deferred webrtc raw site (B3); a wire-only filter would false-positive on B3. ──
{
  const SRC = fileURLToPath(new URL('../src/', import.meta.url));
  const listJs = (dir) => { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; };
  const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
  const METH = { methods: new Set(['onRequest', 'onNotification', 'onRoutedMessage']) };
  const migd = discover(files, METH);
  const b2doors = migd.doors.filter((d) => d.boundary === 'B2');
  const b2Wires = ['hello', 'hello-ack', 'cap-attest'];
  check('P3. MIGRATED B2: the default table discovers EXACTLY 3 B2 doors (hello, hello-ack, cap-attest), all bound to context webTransport',
    b2doors.length === 3 && b2Wires.every((w) => b2doors.some((d) => d.wire === w)) && b2doors.every((d) => d.context === 'webTransport'),
    `\n   b2doors=${JSON.stringify(b2doors.map((d) => ({ wire: d.wire, ctx: d.context })))}`);
  // The 3 migrated B2 receiver/wire identities. Scoping by (recv, wire) is what makes
  // this correct against the shared 'hello' wire (Vega's finding).
  const isB2Raw = (s) => (s.recv === 'bridge' && (s.wire === 'hello' || s.wire === 'hello-ack')) || (s.recv === 'webrtc' && s.wire === 'cap-attest');
  const b2Raw = migd.sites.filter(isB2Raw);
  check("P3b. ZERO-RESIDUAL RAW (B2-scoped): none of the 3 migrated B2 (recv,wire) pairs — (bridge,hello),(bridge,hello-ack),(webrtc,cap-attest) — remains a raw site (raw XOR door, scoped by RECEIVER so the B3 webrtc 'hello' is not miscounted)",
    b2Raw.length === 0, `\n   b2Raw=${JSON.stringify(b2Raw.map((s) => s.site))}`);
  // CROSS-BOUNDARY (E2.3): the shared 'hello' wire is now TWO doors — bridge hello → B2
  // (registry composite._b2door) and webrtc hello → B3 (registry composite._b3door). The
  // boundary is split by the DOOR REGISTRY, not the wire; B2 did not absorb B3's hello.
  const b2hello = migd.doors.find((d) => d.wire === 'hello' && d.boundary === 'B2');
  const b3hello = migd.doors.find((d) => d.wire === 'hello' && d.boundary === 'B3');
  check("P3c. the shared 'hello' wire is TWO distinct doors: bridge hello → B2 (composite._b2door) AND webrtc hello → B3 (composite._b3door) — split by the door registry, B2 did not swallow B3",
    !!b2hello && !!b3hello && b2hello.registry === 'composite._b2door' && b3hello.registry === 'composite._b3door',
    `\n   b2hello=${JSON.stringify(b2hello)} b3hello=${JSON.stringify(b3hello)}`);
  // P3b-neg: TEETH — inject a residual raw bridge.onNotification('hello', …) beside the
  // B2 door; discover() surfaces it as recv='bridge' wire='hello' → b2Raw non-zero →
  // P3b would FAIL. (Also self-checks the recv='bridge' label the scope relies on.)
  {
    const resid = discover([{ path: 'transport/web/index.js', code: IMP + `export function webTransport(){ registerFrame(bridge, 'hello', h, { registry: composite._b2door }); bridge.onNotification('hello', h2); }` }], METH);
    const injected = resid.sites.filter((s) => s.recv === 'bridge' && s.wire === 'hello');
    check("P3b-neg. the B2 zero-residual guard has TEETH: a residual raw bridge.onNotification('hello', …) beside the door IS discovered (recv=bridge, wire=hello) → P3b would FAIL (not vacuous)",
      injected.length >= 1, `\n   sites=${JSON.stringify(resid.sites.map((s) => s.site))}`);
  }
}

// ── REAL TREE — B3 (E2.3, Vega-pattern B3-scoped zero-residual). The 3 WebRTC-signalling
// doors exist (webrtc hello + hello-sig in webTransport; mesh:signal in AxonaPeer), and
// NONE of the 3 migrated B3 (recv,wire) pairs remains a raw site. 'hello' is scoped by
// receiver (it is ALSO a B2 bridge door); hello-sig and mesh:signal are B3-unique wires. ──
{
  const SRC = fileURLToPath(new URL('../src/', import.meta.url));
  const listJs = (dir) => { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; };
  const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
  const METH = { methods: new Set(['onRequest', 'onNotification', 'onRoutedMessage']) };
  const migd = discover(files, METH);
  const b3doors = migd.doors.filter((d) => d.boundary === 'B3');
  const b3Wires = ['hello', 'hello-sig', 'mesh:signal'];
  check('P4. MIGRATED B3: the default table discovers EXACTLY 3 B3 doors (hello, hello-sig, mesh:signal) — hello + hello-sig in context webTransport, mesh:signal in AxonaPeer._installRoutingHandlers',
    b3doors.length === 3 && b3Wires.every((w) => b3doors.some((d) => d.wire === w))
    && b3doors.filter((d) => d.context === 'webTransport').length === 2
    && b3doors.some((d) => d.wire === 'mesh:signal' && d.context === 'AxonaPeer._installRoutingHandlers'),
    `\n   b3doors=${JSON.stringify(b3doors.map((d) => ({ wire: d.wire, ctx: d.context })))}`);
  // B3-scoped zero-residual. 'hello' scoped by receiver (shared with the B2 bridge hello);
  // hello-sig + mesh:signal are B3-unique wires, so wire-scoping is exact for them.
  const isB3Raw = (s) => (s.recv === 'webrtc' && (s.wire === 'hello' || s.wire === 'hello-sig')) || s.wire === 'mesh:signal';
  const b3Raw = migd.sites.filter(isB3Raw);
  check("P4b. ZERO-RESIDUAL RAW (B3-scoped): none of the 3 migrated B3 (recv,wire) pairs — (webrtc,hello),(webrtc,hello-sig),(this,mesh:signal) — remains a raw site (raw XOR door); 'hello' scoped by receiver so the B2 bridge 'hello' door is not miscounted",
    b3Raw.length === 0, `\n   b3Raw=${JSON.stringify(b3Raw.map((s) => s.site))}`);
  // P4b-neg: TEETH — inject a residual raw webrtc.onNotification('hello', …) beside the B3
  // door; discover() surfaces it as recv='webrtc' wire='hello' → b3Raw non-zero → P4b would
  // FAIL. (Also self-checks the recv='webrtc' label the scope relies on.)
  {
    const resid = discover([{ path: 'transport/web/index.js', code: IMP + `export function webTransport(){ registerFrame(webrtc, 'hello', h, { registry: composite._b3door }); webrtc.onNotification('hello', h2); }` }], METH);
    const injected = resid.sites.filter((s) => s.recv === 'webrtc' && s.wire === 'hello');
    check("P4b-neg. the B3 zero-residual guard has TEETH: a residual raw webrtc.onNotification('hello', …) beside the door IS discovered (recv=webrtc, wire=hello) → P4b would FAIL (not vacuous)",
      injected.length >= 1, `\n   sites=${JSON.stringify(resid.sites.map((s) => s.site))}`);
  }
}

// ── H-family: the COMPOSITE-KEY door (E2.4 B6, Aster ASTER-E2-SITE-IDENTITY). B6 is the
// SOLE boundary where ONE wire (axona:direct) binds TWO doors in ONE context, split only by
// the SUPPLIED transportKind. This family proves the grammar keeps the two legs DISTINCT (no
// silent collapse onto one) and that the routed tunneled leg binds its own context. The
// generic context fence (wrong method/class/file/top-level) is the SAME machinery the
// G-family already proves; H adds only the composite-specific evidence. (Flag-off byte-
// identical equivalence for all three B6 legs is owned by smoke_boundary6_registry.mjs's
// D. FLAG-OFF IDENTITY block, 15/15.)
{
  const PFILE = 'dht/AxonaPeer.js';
  const B6PEER = [
    { file: PFILE, context: 'AxonaPeer._installDirectHandlers', registry: 'this._b6door', boundary: 'B6' },
    { file: PFILE, context: 'AxonaPeer._buildDefaultAxonaManager', registry: 'peer._b6door', boundary: 'B6' },
  ];
  const runP = (src, path = 'src/' + PFILE) => discoverDoors([{ path, code: IMP + src }], { doorRegistries: B6PEER });
  {
    // The two axona:direct legs, SAME wire, SAME context, SAME registry — distinguished only
    // by transportKind. The grammar must yield TWO doors, not one (no collision, no collapse).
    const r = runP(`class AxonaPeer { _installDirectHandlers(){
      registerFrame(t, 'axona:direct', h1, { registry: this._b6door, transportKind: 'request' });
      registerFrame(t, 'axona:direct', h2, { registry: this._b6door, transportKind: 'notification' });
    } }`);
    const tks = new Set(r.doors.map((d) => d.transportKind));
    check("H1. COMPOSITE INTENDED: two axona:direct legs (request + notification) in AxonaPeer._installDirectHandlers on this._b6door → TWO distinct B6 doors, ONE wire, DISTINCT transportKind, zero unresolved (no collapse)",
      r.doors.length === 2 && r.doors.every((d) => d.wire === 'axona:direct' && d.boundary === 'B6' && d.context === 'AxonaPeer._installDirectHandlers')
      && tks.size === 2 && tks.has('request') && tks.has('notification') && r.unresolved.length === 0,
      `\n   doors=${JSON.stringify(r.doors.map((d) => ({ wire: d.wire, tk: d.transportKind })))} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    // The routed tunneled leg binds its OWN context (_buildDefaultAxonaManager) on peer._b6door.
    const r = runP(`class AxonaPeer { _buildDefaultAxonaManager(){ const peer = this;
      registerFrame(peer, '__tunneled_direct__', h, { registry: peer._b6door, transportKind: 'routed' });
    } }`);
    check("H2. the routed tunneled leg in AxonaPeer._buildDefaultAxonaManager on peer._b6door → ONE B6 routed door, zero unresolved",
      r.doors.length === 1 && r.doors[0].boundary === 'B6' && r.doors[0].wire === '__tunneled_direct__' && r.doors[0].transportKind === 'routed'
      && r.doors[0].context === 'AxonaPeer._buildDefaultAxonaManager' && r.unresolved.length === 0,
      `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
  {
    // WRONG CONTEXT: the composite legs in a method the B6 table does not name → refused.
    const r = runP(`class AxonaPeer { someOtherMethod(){ registerFrame(t, 'axona:direct', h, { registry: this._b6door, transportKind: 'request' }); } }`);
    check("H3. WRONG METHOD: an axona:direct door in AxonaPeer.someOtherMethod → context mismatch → ZERO doors, refused (shared context fence, B6-scoped)",
      r.doors.length === 0 && r.unresolved.length === 1, `\n   doors=${JSON.stringify(r.doors)} unresolved=${JSON.stringify(r.unresolved)}`);
  }
}

// ── REAL TREE — B6 (E2.4, composite-key zero-residual). The 3 direct-messaging doors exist
// (two axona:direct legs in AxonaPeer._installDirectHandlers, one __tunneled_direct__ routed
// leg in AxonaPeer._buildDefaultAxonaManager), keyed by the (wire, transportKind) composite so
// the two same-wire legs stay DISTINCT, and NONE of the 3 migrated (recv,wire) primitives
// remains a raw site. axona:direct is the ONLY same-wire-two-primitive case in the 38-site set. ──
{
  const SRC = fileURLToPath(new URL('../src/', import.meta.url));
  const listJs = (dir) => { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; };
  const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
  const METH = { methods: new Set(['onRequest', 'onNotification', 'onRoutedMessage']) };
  const migd = discover(files, METH);
  const b6doors = migd.doors.filter((d) => d.boundary === 'B6');
  const directLegs = b6doors.filter((d) => d.wire === 'axona:direct');
  const directTks = new Set(directLegs.map((d) => d.transportKind));
  check('P5. MIGRATED B6: the default table discovers EXACTLY 3 B6 doors — the two axona:direct legs (request + notification) in AxonaPeer._installDirectHandlers with DISTINCT transportKind (composite key, no collapse), and __tunneled_direct__ routed in AxonaPeer._buildDefaultAxonaManager',
    b6doors.length === 3
    && directLegs.length === 2 && directLegs.every((d) => d.context === 'AxonaPeer._installDirectHandlers') && directTks.size === 2 && directTks.has('request') && directTks.has('notification')
    && b6doors.some((d) => d.wire === '__tunneled_direct__' && d.transportKind === 'routed' && d.context === 'AxonaPeer._buildDefaultAxonaManager'),
    `\n   b6doors=${JSON.stringify(b6doors.map((d) => ({ wire: d.wire, tk: d.transportKind, ctx: d.context })))}`);
  // B6 zero-residual. The migrated wires axona:direct and __tunneled_direct__ are B6-unique
  // (no other boundary owns them), so wire-scoping is exact — no receiver disambiguation needed.
  const b6Raw = migd.sites.filter((s) => s.wire === 'axona:direct' || s.wire === '__tunneled_direct__');
  check("P5b. ZERO-RESIDUAL RAW (B6): neither axona:direct (either primitive) nor __tunneled_direct__ remains a raw site — all three legs live at a door (raw XOR door)",
    b6Raw.length === 0, `\n   b6Raw=${JSON.stringify(b6Raw.map((s) => s.site))}`);
  // P5b-neg: TEETH — inject a residual raw t.onNotification('axona:direct', …) beside the door;
  // discover() surfaces it as wire='axona:direct' → b6Raw non-zero → P5b would FAIL (not vacuous).
  // This is the composite case: the residual is the NOTIFY leg, distinct from the migrated request
  // leg, so it proves the guard catches a per-PRIMITIVE residual, not merely a per-wire one.
  {
    const resid = discover([{ path: 'dht/AxonaPeer.js', code: IMP + `class AxonaPeer { _installDirectHandlers(){ registerFrame(t, 'axona:direct', h, { registry: this._b6door, transportKind: 'request' }); t.onNotification('axona:direct', h2); } }` }], METH);
    const injected = resid.sites.filter((s) => s.wire === 'axona:direct');
    check("P5b-neg. the B6 zero-residual guard has TEETH: a residual raw t.onNotification('axona:direct', …) beside the request-leg door IS discovered (wire=axona:direct) → P5b would FAIL (not vacuous)",
      injected.length >= 1, `\n   sites=${JSON.stringify(resid.sites.map((s) => s.site))}`);
  }
}

// ── REAL TREE — B5 (E2.5, bare-keyed zero-residual). The 10 dht:transport routing doors
// exist in AxonaPeer._installRoutingHandlers on this._b5door (5 request + 5 notification
// legs), bare-keyed so each OMITS transportKind (→ null), and NONE of the 10 wires remains
// a raw site. The B3 mesh:signal door shares this method on this._b3door — the
// boundary==='B5' scope keeps it out of the B5 count. All 10 B5 wires are B5-unique, so
// wire-scoping is exact for the zero-residual check. B5 is the TERMINAL slice: after it, all
// 38 migration-targets are doors and zero raw sites remain. ──
{
  const SRC = fileURLToPath(new URL('../src/', import.meta.url));
  const listJs = (dir) => { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; };
  const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
  const METH = { methods: new Set(['onRequest', 'onNotification', 'onRoutedMessage']) };
  const migd = discover(files, METH);
  const b5doors = migd.doors.filter((d) => d.boundary === 'B5');
  const B5_REQUEST = ['lookup_step', 'lookahead_probe', 'local_probe', 'find_closest_set', 'route_msg'];
  const B5_NOTIFY  = ['reinforce', 'triadic_introduce', 'hop_cache', 'lateral_spread', 'peer-leaving'];
  const B5_WIRES = [...B5_REQUEST, ...B5_NOTIFY];
  check('P6. MIGRATED B5: the default table discovers EXACTLY 10 B5 doors, all in AxonaPeer._installRoutingHandlers on this._b5door, bare-keyed (transportKind OMITTED → null), covering the 5 request + 5 notification wires — the B3 mesh:signal door in the SAME method (this._b3door) is NOT miscounted',
    b5doors.length === 10
    && b5doors.every((d) => d.context === 'AxonaPeer._installRoutingHandlers' && d.registry === 'this._b5door' && d.transportKind === null)
    && B5_WIRES.every((w) => b5doors.some((d) => d.wire === w))
    && b5doors.every((d) => d.wire !== 'mesh:signal')
    && migd.doors.some((d) => d.wire === 'mesh:signal' && d.boundary === 'B3' && d.context === 'AxonaPeer._installRoutingHandlers'),
    `\n   b5doors=${JSON.stringify(b5doors.map((d) => ({ wire: d.wire, tk: d.transportKind, reg: d.registry })))}`);
  const b5Wireset = new Set(B5_WIRES);
  const b5Raw = migd.sites.filter((s) => b5Wireset.has(s.wire));
  check('P6b. ZERO-RESIDUAL RAW (B5): none of the 10 dht:transport wires remains a raw onRequest/onNotification site — all live at a door (raw XOR door)',
    b5Raw.length === 0, `\n   b5Raw=${JSON.stringify(b5Raw.map((s) => s.site))}`);
  // P6b-neg: TEETH — inject a residual raw transport.onNotification('reinforce', …) beside
  // the door; discover() surfaces it as wire='reinforce' → b5Raw non-zero → P6b would FAIL.
  {
    const resid = discover([{ path: 'dht/AxonaPeer.js', code: IMP + `class AxonaPeer { _installRoutingHandlers(){ registerFrame(transport, 'reinforce', h, { registry: this._b5door }); transport.onNotification('reinforce', h2); } }` }], METH);
    const injected = resid.sites.filter((s) => s.wire === 'reinforce');
    check("P6b-neg. the B5 zero-residual guard has TEETH: a residual raw transport.onNotification('reinforce', …) beside the door IS discovered (wire=reinforce) → P6b would FAIL (not vacuous)",
      injected.length >= 1, `\n   sites=${JSON.stringify(resid.sites.map((s) => s.site))}`);
  }
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
