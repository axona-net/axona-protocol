// =====================================================================
// ref11_e0_manifest.mjs — REF-1.1 E0 generated frame-registration manifest.
//
// The machine-checkable inventory the council required (Aster carry-forward #2,
// Vega/Orion concurring) after a hand-count of the E0 table diverged from the
// tree in three places. It emits REF-1.1-E0-manifest.json: one row per site that
// touches a sealed registration primitive, each with {file, line, callee,
// receiver, wire, classification}, pinned to the axona-protocol tree hash.
//
// Discovery is the SAME sound acorn walk the S5 ownership fence uses
// (test/lib/registrationScan.mjs), so the manifest cannot silently omit a raw
// registration the way a markdown table can: a non-literal/aliased/computed
// registration that is not a documented mechanism becomes an UNRESOLVED error,
// and every src file must parse (coverage is proven, not assumed).
//
//   node test/fence_e0_manifest.mjs            # CI gate (default): self-check
//                                                counts + fail if the tree drifts
//                                                from the committed manifest
//   node test/fence_e0_manifest.mjs --write    # regenerate the committed manifest
//
// SEALED SET = { onRequest, onNotification, onRoutedMessage }. onMessage is the
// public delivery API and is OUT (enumerated as out-of-scope, never sealed).
// onDirectMessage is the parameterized registrar for the open direct_${type}
// family — its own E1 fence, not a manifest row (see the E0 design doc).
// =====================================================================
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join, relative, dirname } from 'node:path';
import { execSync } from 'node:child_process';
import * as acorn from 'acorn';
import { discover, walk } from './lib/registrationScan.mjs';
import { SEALED, RAW_DISPATCH_ALLOWLIST as MECHANISM_EXEMPT } from './lib/e0Allowlist.mjs';

// Wire → boundary hint for migration-target rows (context, not the classification).
const BOUNDARY = new Map([
  ['hello', 'B2/B3'], ['hello-ack', 'B2'], ['hello-sig', 'B3'], ['cap-attest', 'B2'],
  ['mesh:signal', 'B3'], ['__tunneled_direct__', 'direct'], ['axona:direct', 'direct'],
]);
const boundaryOf = (callee, wire, recv) =>
  callee === 'on(T.*)' ? 'B1'
    : (BOUNDARY.get(wire) || (SEALED.has(callee) ? `dht:${recv}` : '—'));

const REPO = dirname(dirname(fileURLToPath(import.meta.url)));
const SRC = join(REPO, 'src');
const MANIFEST = join(dirname(fileURLToPath(import.meta.url)), 'REF-1.1-E0-manifest.json');

function listJs(dir) { const o = []; for (const n of readdirSync(dir)) { const p = join(dir, n); const s = statSync(p); if (s.isDirectory()) o.push(...listJs(p)); else if (n.endsWith('.js')) o.push(p); } return o; }
const files = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: readFileSync(p, 'utf8') }));

// ── PRIMARY: the sealed-primitive scan (fail-closed) ──
const { sites, unresolved, parseErrors, mechanisms, doors } = discover(files, { methods: SEALED, mechanismExempt: MECHANISM_EXEMPT });

// migration-targets: literal/T.<name>-wired registrations of a sealed primitive.
// Exclude the bridge-ws `dispatch` switch — it is a separate registration style,
// not one of the three sealed primitives (counted apart, informational).
const migration = sites.filter((s) => s.callee !== 'dispatch').map((s) => ({
  file: s.file, line: s.line, callee: s.callee, receiver: s.recv, wire: s.wire,
  classification: 'migration-target', boundary: boundaryOf(s.callee, s.wire, s.recv),
}));
// REF-1.1 E2 (council 389be28f / eb71240a / e2200e2b): a MIGRATED site is a
// canonical registerFrame door, discovered by the ONE shared grammar
// (discover→discoverDoors). It is a migration-target exactly like the raw site it
// replaced — the target MOVED from raw to door, the count is unchanged. Empty until
// a boundary migrates → zero door rows on the unmigrated tree, so the manifest + all
// counts are byte-identical (parity). receiver carries the canonical registry expr.
const doorRows = doors.map((d) => ({
  file: d.file, line: d.line, callee: 'registerFrame', receiver: d.registry, wire: d.wire,
  classification: 'migration-target', boundary: d.boundary,
  // B6 is the sole COMPOSITE boundary: two axona:direct legs share one wire, so the
  // door's SUPPLIED transportKind is the only way the coverage manifest can tell them
  // apart. Thread it through when the door names one; B1–B3 doors omit it (d.transportKind
  // is null) → spread {} → key absent → those committed rows stay byte-identical.
  ...(d.transportKind ? { transportKind: d.transportKind } : {}),
}));
const dispatch = sites.filter((s) => s.callee === 'dispatch').map((s) => ({
  file: s.file, line: s.line, callee: 'dispatch', receiver: s.recv, wire: s.wire,
  classification: 'bridge-ws-dispatch', boundary: 'bridge-ws',
}));
const mechRows = mechanisms.map((m) => ({
  file: m.file, line: m.line, callee: m.callee, receiver: m.recv, wire: `<${m.arg}>`,
  classification: m.class, boundary: '—', why: m.why,
}));

// ── DEFINITIONS: where a sealed primitive is DEFINED (sealed at E3). A real
// definition is a class MethodDefinition or an object shorthand method
// (`onRequest(t,h){…}`); an arrow-valued property (`onRoutedMessage: (t,h)=>…`)
// is an adapter passthrough, NOT a definition — it is caught as a mechanism above.
const defRows = [];
const secondary = [];  // onDirectMessage / onMessage — enumerated, never sealed
for (const { path: fp, code } of files) {
  let ast;
  try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'module', allowHashBang: true, locations: true }); }
  catch { try { ast = acorn.parse(code, { ecmaVersion: 'latest', sourceType: 'script', allowHashBang: true, allowReturnOutsideFunction: true, locations: true }); } catch { continue; } }
  walk(ast, (n) => {
    const keyName = (k) => k?.type === 'Identifier' ? k.name : (k?.type === 'Literal' && typeof k.value === 'string' ? k.value : null);
    const isFnDef = (n.type === 'MethodDefinition') || (n.type === 'Property' && n.method === true);
    if (isFnDef) {
      const kn = keyName(n.key);
      if (kn && SEALED.has(kn)) defRows.push({ file: fp, line: n.loc.start.line, callee: kn, receiver: 'def', wire: '—', classification: 'primitive-definition', boundary: '—' });
    }
    // onDirectMessage / onMessage — the non-sealed wrappers, enumerated so the
    // manifest is a superset of the narrative §C (covers the 3090 passthrough and
    // the public onMessage). Calls AND defs; classification names why each is out.
    if (n.type === 'CallExpression' && n.callee?.type === 'MemberExpression' && !n.callee.computed) {
      const mn = n.callee.property?.name;
      if (mn === 'onDirectMessage') secondary.push({ file: fp, line: n.loc.start.line, callee: 'onDirectMessage', receiver: n.callee.object?.type === 'Identifier' ? n.callee.object.name : (n.callee.object?.type === 'ThisExpression' ? 'this' : '?'), wire: '<type>', classification: 'wrapper-passthrough', boundary: 'direct' });
      if (mn === 'onMessage') secondary.push({ file: fp, line: n.loc.start.line, callee: 'onMessage', receiver: '?', wire: '—', classification: 'public-api-out-of-scope', boundary: '—' });
    }
    const isFnDef2 = (n.type === 'MethodDefinition') || (n.type === 'Property' && n.method === true);
    if (isFnDef2) {
      const kn = keyName(n.key);
      if (kn === 'onDirectMessage') secondary.push({ file: fp, line: n.loc.start.line, callee: 'onDirectMessage', receiver: 'def', wire: '—', classification: 'wrapper-passthrough', boundary: 'direct' });
      if (kn === 'onMessage') secondary.push({ file: fp, line: n.loc.start.line, callee: 'onMessage', receiver: 'def', wire: '—', classification: 'public-api-out-of-scope', boundary: '—' });
    }
  });
}

// ── assemble ──
const rows = [...migration, ...doorRows, ...mechRows, ...defRows, ...dispatch, ...secondary]
  .sort((a, b) => a.file === b.file ? a.line - b.line : a.file.localeCompare(b.file));

const count = (c) => rows.filter((r) => r.classification === c).length;
const summary = {
  'migration-target': count('migration-target'),
  'registration-helper': count('registration-helper'),
  'mechanism-shim': count('mechanism-shim'),
  'canonical-door': count('canonical-door'),
  'parameterized-registrar': count('parameterized-registrar'),
  'primitive-definition': count('primitive-definition'),
  'bridge-ws-dispatch': count('bridge-ws-dispatch'),
  'wrapper-passthrough': count('wrapper-passthrough'),
  'public-api-out-of-scope': count('public-api-out-of-scope'),
};
const b1 = [...migration, ...doorRows].filter((r) => r.boundary === 'B1').length;   // raw B1 + door B1 (a B1 target migrated from raw to door stays counted)

let treeHash = 'UNKNOWN';
try { treeHash = execSync('git rev-parse HEAD', { cwd: REPO }).toString().trim(); } catch { /* detached / no git */ }

const manifest = {
  generator: 'test/ref11_e0_manifest.mjs',
  sealedSet: [...SEALED],
  treeHash,
  filesScanned: files.length,
  summary,
  b1PubsubFrames: b1,
  rows,
};
const serialized = JSON.stringify(manifest, (k, v) => k === 'treeHash' ? 'PINNED' : v, 2); // tree-hash-independent body for drift diff

// ── fail-closed gates ──
let ok = true;
const fail = (m) => { console.error(`  ✗ ${m}`); ok = false; };
const pass = (m) => console.log(`  ✓ ${m}`);

console.log('\nREF-1.1 E0 — generated frame-registration manifest\n');
parseErrors.length === 0 ? pass(`source coverage: all ${files.length} src files parsed`) : fail(`parse errors: ${parseErrors.join('; ')}`);
unresolved.length === 0 ? pass('no unresolved (aliased/computed/loose-literal/escaped) sealed registration') : fail(`unresolved sites:\n     ${unresolved.join('\n     ')}`);
b1 === 19 ? pass('B1 pub/sub migration-targets = 19 (matches the S5 fence assertion)') : fail(`B1 pub/sub = ${b1}, expected 19`);
summary['migration-target'] === 38 ? pass('migration-target frames = 38') : fail(`migration-target = ${summary['migration-target']}, expected 38`);
// REF-1.1 E3a sealed the node WebSocketTransport (13 → 11). E3b.1 seals the base
// Transport.js contract: its onRequest/onNotification throwing stubs are removed so
// no inherited dispatch name is reachable on any subclass (Aster's absence
// invariant), dropping primitive-definition 11 → 9. E3b.2a sealed the sim
// transport (→7); E3b.2b sealed the three web transports (→1); E3b.2c seals
// AxonaPeer.onRoutedMessage, the LAST primitive (→0). Every dispatch receiver in
// the program — every transport AND the peer — now reaches its raw primitive only
// through the capability channel; the onRequest/onNotification/onRoutedMessage
// names survive nowhere as reachable methods. The E3 absence invariant is complete.
summary['primitive-definition'] === 0 ? pass('primitive definitions = 0 (every receiver sealed — E3 absence invariant complete)') : fail(`primitive-definition = ${summary['primitive-definition']}, expected 0`);
summary['parameterized-registrar'] === 1 ? pass('parameterized registrar (onDirectMessage direct_*) = 1') : fail(`parameterized-registrar = ${summary['parameterized-registrar']}, expected 1`);

const args = process.argv.slice(2);
if (args.includes('--write')) {
  writeFileSync(MANIFEST, serialized.replace('"treeHash": "PINNED"', `"treeHash": ${JSON.stringify(treeHash)}`) + '\n');
  pass(`wrote ${relative(REPO, MANIFEST)} (${rows.length} rows, tree ${treeHash.slice(0, 7)})`);
} else {
  // Default = CI gate: the committed manifest must match the tree (rows only;
  // the tree-hash line is normalized out so a routine commit does not trip it).
  let committed;
  try { committed = readFileSync(MANIFEST, 'utf8'); } catch { fail('no committed manifest — run with --write first'); }
  if (committed) {
    const norm = (s) => JSON.stringify(JSON.parse(s), (k, v) => k === 'treeHash' ? 'PINNED' : v);
    norm(committed) === norm(serialized)
      ? pass('committed manifest matches the tree (no site added/removed/reclassified)')
      : fail('manifest DRIFT: a registration site changed class, appeared, or vanished vs the committed manifest — re-run with --write and review the diff');
  }
}

console.log('\nSummary:', JSON.stringify(summary));
console.log(`\nResult: ${ok ? 'PASS' : 'FAIL'}\n`);
process.exit(ok ? 0 : 1);
