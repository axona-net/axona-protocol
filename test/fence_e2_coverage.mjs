// =====================================================================
// fence_e2_coverage.mjs — REF-1.1 E2.0 source-site → row coverage, proven as an
// EXACT SET EQUALITY over a GENERATED artifact (Aster E2.0 re-review 2795636a
// cond.3 + correction a767b880; Vega e7b1d3aa; Orion c927a0e7).
//
// Aster requires the coverage proof to be a GENERATED ARTIFACT mapping each
// IMMUTABLE source-site identity to its intended boundary row, with exact
// source-site-to-intended-row SET EQUALITY asserted. The distinct
// (boundary, wire, transportKind) triples are only an injection implementation-
// check — NOT a substitute for the source-site-to-row mapping.
//
// So this gate does NOT re-derive a count. It:
//   (1) loads test/REF-1.1-E2-coverage-manifest.json (the generated artifact);
//   (2) re-runs the generator in memory and asserts the committed artifact is
//       byte-identical — the artifact can never drift from the E0 sites or the
//       registries (a set equality on the whole mapping);
//   (3) asserts the 38 IMMUTABLE site identities are distinct (a real site set);
//   (4) OWNERSHIP: every mapped (boundary, wire, transportKind) → rowType is
//       actually declared by that boundary registry;
//   (5) INJECTION: the 38 mapped (boundary, rowType) are distinct;
//   (6) EXACT SET EQUALITY on the two NEW registries: the set of rowTypes the
//       sites map into B5 (resp. B6) EQUALS the set of rowTypes B5 (resp. B6)
//       declares — both directions, no missing row, no dangling row;
//   (7) the two multiplicity cases proven by site identity (hello → B2+B3;
//       axona:direct → B6 on two primitives);
//   (8) the B1–B3 differential audit (B5-last soundness) unchanged.
//
// Run: node test/fence_e2_coverage.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { generate } from './gen_e2_coverage_manifest.mjs';
import { rowDefs as r1, frameWiring as f1 } from '../src/pubsub/boundary1Registry.js';
import { rowDefs as r2, frameWiring as f2 } from '../src/transport/boundary2Registry.js';
import { rowDefs as r3, frameWiring as f3 } from '../src/transport/boundary3Registry.js';
import { rowDefs as r4, frameWiring as f4 } from '../src/transport/boundary4Registry.js';
import { rowDefs as r5, frameWiring as f5 } from '../src/dht/boundary5Registry.js';
import { rowDefs as r6, frameWiring as f6 } from '../src/dht/boundary6Registry.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const setEqual = (a, b) => a.size === b.size && [...a].every((x) => b.has(x));

console.log('\nREF-1.1 E2.0 — source-site → row coverage as exact set equality (generated artifact)\n');

// ── (1) load the GENERATED artifact ──
const artifactPath = fileURLToPath(new URL('./REF-1.1-E2-coverage-manifest.json', import.meta.url));
const committedText = readFileSync(artifactPath, 'utf8');
const artifact = JSON.parse(committedText);
const sites = artifact.sites;

// ── (2) DRIFT GUARD: the committed artifact must equal a fresh regeneration ──
const fresh = generate();
const freshText = JSON.stringify(fresh, null, 2) + '\n';
check('C1. the committed coverage artifact is byte-identical to a fresh regeneration from E0 + the registries — it cannot drift',
  committedText === freshText, `\n   run: node test/gen_e2_coverage_manifest.mjs --write`);
check('C2. exactly 38 migration-target source sites in the artifact', sites.length === 38, `\n   got ${sites.length}`);
check('C3. every entry carries the CALL-SITE locator (file:line) + source primitive, an immutable identity, a transportKind, an intended boundary, and a resolved rowType',
  sites.every((s) => s.callSite && s.primitive && s.identity && s.transportKind && s.boundary && s.rowType),
  `\n   incomplete: ${sites.filter((s) => !(s.callSite && s.primitive && s.identity && s.transportKind && s.boundary && s.rowType)).map((s) => s.callSite || '(no callSite)').join(', ')}`);

// ── (3) the 38 IMMUTABLE call-site identities are distinct, and each maps to
// EXACTLY ONE intended boundary row (Aster a6f19517 / Vega 20191473: the identity
// must carry the actual call-site LOCATOR, not just a descriptor set — a file is
// not a call-site; file:line pins the exact registration call) ──
check('C4. the 38 immutable identities (file:line ‖ wire ‖ transportKind ‖ receiver — LOCATOR-led) are DISTINCT',
  new Set(sites.map((s) => s.identity)).size === 38,
  `\n   distinct=${new Set(sites.map((s) => s.identity)).size}`);
{
  const m = new Map();
  let multi = 0;
  for (const s of sites) {
    const rk = `${s.boundary}|${s.rowType}`;
    if (m.has(s.identity) && m.get(s.identity) !== rk) multi++;
    m.set(s.identity, rk);
  }
  check('C4b. each immutable call-site identity maps to EXACTLY ONE intended boundary row — asserted once onto its row',
    m.size === 38 && multi === 0, `\n   distinctIdentities=${m.size} multiMapped=${multi}`);
}

// ── the live registries, keyed by intended-boundary label ──
const REG = { B1: f1(r1()), B2: f2(r2()), B3: f3(r3()), B4: f4(r4()), B5: f5(r5()), B6: f6(r6()) };
const resolveType = (boundary, wire, tk) => {
  for (const [key, v] of REG[boundary]) if ((v.wire ?? key) === wire && v.transportKind === tk) return v.type;
  return null;
};

// ── (4) OWNERSHIP: each mapped (boundary, wire, transportKind) → rowType is really declared ──
const unowned = sites.filter((s) => resolveType(s.boundary, s.wire, s.transportKind) !== s.rowType);
check('C5. OWNERSHIP: every mapped site (boundary, wire, transportKind) resolves in that registry to EXACTLY the artifact\'s rowType',
  unowned.length === 0,
  `\n   ${unowned.map((s) => `${s.site} → ${s.boundary}(${s.wire},${s.transportKind}) got ${resolveType(s.boundary, s.wire, s.transportKind)} ≠ ${s.rowType}`).join('\n   ')}`);

// ── (5) INJECTION: the 38 mapped (boundary, rowType) are distinct — no two sites share a row ──
const rowKeys = sites.map((s) => `${s.boundary}|${s.rowType}`);
check('C6. INJECTION: the 38 mapped (boundary, rowType) targets are DISTINCT — no two source sites claim the same row',
  new Set(rowKeys).size === 38,
  `\n   collisions: ${[...new Set(rowKeys.filter((k, i) => rowKeys.indexOf(k) !== i))].join(', ')}`);

// ── (6) EXACT SET EQUALITY on the two NEW registries (B5, B6): mapped rowTypes ↔ declared rowTypes ──
for (const b of ['B5', 'B6']) {
  const declared = new Set([...REG[b].values()].map((v) => v.type));
  const mapped = new Set(sites.filter((s) => s.boundary === b).map((s) => s.rowType));
  check(`C7.${b} EXACT SET EQUALITY: the rowTypes the sites map into ${b} EQUAL the rowTypes ${b} declares (no missing, no dangling)`,
    setEqual(mapped, declared),
    `\n   declared=[${[...declared].sort()}]\n   mapped=[${[...mapped].sort()}]`);
}

// ── (7) the two multiplicity cases, by SITE IDENTITY not count ──
check('C8. hello is TWO distinct sites owned by DIFFERENT registries — B2 (bridge) and B3 (webrtc), disambiguated by receiver',
  sites.filter((s) => s.wire === 'hello').length === 2
  && new Set(sites.filter((s) => s.wire === 'hello').map((s) => s.boundary)).size === 2
  && new Set(sites.filter((s) => s.wire === 'hello').map((s) => s.receiver)).size === 2);
check('C9. axona:direct is TWO distinct sites in ONE registry (B6) on different primitives — request + notification',
  sites.filter((s) => s.wire === 'axona:direct').length === 2
  && sites.filter((s) => s.wire === 'axona:direct').every((s) => s.boundary === 'B6')
  && new Set(sites.filter((s) => s.wire === 'axona:direct').map((s) => s.transportKind)).size === 2);

// ── (8) B1–B3 differential dependency audit (Aster/Vega: is B5-last sound?) ──
const DIFF_SMOKES = ['smoke_boundary1_registry.mjs', 'smoke_boundary2_registry.mjs', 'smoke_boundary3_registry.mjs'];
const B5_WIRES = new Set(sites.filter((s) => s.boundary === 'B5').map((s) => s.wire));
const leak = [];
for (const f of DIFF_SMOKES) {
  const src = readFileSync(fileURLToPath(new URL('./' + f, import.meta.url)), 'utf8');
  if (/boundary5Registry/.test(src)) leak.push(`${f} imports boundary5Registry`);
  for (const w of B5_WIRES) if (src.includes(`'${w}'`) || src.includes(`"${w}"`)) leak.push(`${f} references B5 wire ${w}`);
}
check('C10. B1–B3 differential audit: no B1–B3 smoke imports B5 or names a DHT-routing wire — B5-last is sound', leak.length === 0, `\n   ${leak.join('\n   ')}`);

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
