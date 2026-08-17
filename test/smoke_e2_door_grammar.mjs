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

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
