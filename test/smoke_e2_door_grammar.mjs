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
// Plus PARITY: on the real unmigrated src tree the migration-aware discover() with an
// empty door table yields ZERO doors and the unchanged raw site set — byte-identical
// to the pre-door scan (the standalone recut lands before B1, so src has no doors).
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
const DOORS = [{ file: 'fx.js', registry: 'this._frameDoor', boundary: 'B1' }];
const IMP = "import { registerFrame } from '../registry/index.js';\n";
const run = (body, imp = IMP, doorRegistries = DOORS) => discoverDoors([{ path: 'src/fx.js', code: imp + body }], { doorRegistries });

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
  const r = discoverDoors([{ path: 'src/fx.js', code: (f.imp || IMP) + f.body }], { doorRegistries: DOORS });
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

// ── PARITY on the real unmigrated src tree: migration-aware discover() with the
// default (empty) door table yields ZERO doors and the unchanged raw site set. ──
{
  const SRC = fileURLToPath(new URL('../src/', import.meta.url));
  const listJs = (dir) => { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; };
  const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));
  const r = discover(files, { methods: new Set(['onRequest', 'onNotification', 'onRoutedMessage']) });
  check('P1. PARITY: the unmigrated src tree yields ZERO doors — the door grammar is inert until a boundary migrates',
    r.doors.length === 0, `\n   doors=${r.doors.length}`);
  check('P1b. PARITY: the raw site set is intact (≥40) — the door pass never touches sites/mechanisms',
    r.sites.length >= 40, `\n   sites=${r.sites.length}`);
  check('P1c. PARITY: no door-grammar false unresolved on the real tree (src imports no door pre-migration)',
    r.doors.length === 0 && files.every((f) => discoverDoors([f]).unresolved.length === 0));
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
