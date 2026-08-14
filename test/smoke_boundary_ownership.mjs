// =====================================================================
// smoke_boundary_ownership.mjs — REF-1.1 S5: the cross-boundary ownership
// FENCE, over a COMPLETE, FAIL-CLOSED scan of every registration site in src/.
//
// Aster S5 F1 (recut-2): the scanner hard-coded three files while src/ holds 73;
// a registration placed in a fourth file was never discovered, and the exact-text
// regexes could silently miss ordinary JS syntax (double-quotes, spacing, computed
// labels, comments). NEG4 only called classify() on a synthetic endpoint — it
// never exercised file discovery. This recut:
//   1. ENUMERATES the actual src/**/*.js set (recursive walk), not an allowlist.
//   2. STRIPS comments with a string-aware state machine, so // inside a URL or a
//      commented-out registration cannot fool the scan.
//   3. Discovers registrations via the three mechanisms — on(T.X) (B1 routed),
//      <recv>.onRoutedMessage('lit') / <recv>.onNotification('lit') (both quote
//      styles, flexible spacing), and signaling.dispatch case labels (brace-matched
//      slice) — and FAILS CLOSED on any onRoutedMessage/onNotification call whose
//      argument is not a resolvable literal (computed/variable), except the single
//      wireHandlers mechanism call dht.onRoutedMessage(type,…) whose concrete wires
//      come from the on(T.X) scan.
//   4. Classifies every discovered site IN_SCOPE (→ boundary + registry wire) or
//      OUT_OF_SCOPE (documented plane); INV0 fails on anything unclassified.
//   5. NEG4 injects a synthetic FOURTH FILE into the discovery input and proves its
//      new registration reaches INV0 and fails until classified.
//
// OUT_OF_SCOPE planes (not REF-1.1 frame-contract boundaries, excluded explicitly):
// synaptome-learning gossip (reinforce/triadic_introduce/hop_cache/lateral_spread),
// membership gossip (peer-leaving), direct messaging (axona:direct/__tunneled_direct__).
// =====================================================================
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join, relative } from 'node:path';
import { buildBoundary1Registry } from '../src/pubsub/boundary1Registry.js';
import { buildBoundary2Registry } from '../src/transport/boundary2Registry.js';
import { buildBoundary3Registry } from '../src/transport/boundary3Registry.js';
import { buildBoundary4Registry } from '../src/transport/boundary4Registry.js';
import { T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };

console.log('\nREF-1.1 S5 — cross-boundary ownership fence (complete fail-closed src/ scan)\n');

const R1 = buildBoundary1Registry().wiring, R2 = buildBoundary2Registry().wiring,
      R3 = buildBoundary3Registry().wiring, R4 = buildBoundary4Registry().wiring;
const REG = { B1: new Set(R1.keys()), B2: new Set(R2.keys()), B3: new Set(R3.keys()), B4: new Set(R4.keys()) };
const WIRING = { B1: R1, B2: R2, B3: R3, B4: R4 };
const REG_PREFIX = { B1: 'pubsub:', B2: 'transport:', B3: 'mesh:', B4: 'bridge:' };

// ── string-aware comment stripper (handles ' " ` strings, // and /* */ comments) ──
function stripComments(code) {
  let out = '', i = 0; const n = code.length;
  while (i < n) {
    const c = code[i], d = code[i + 1];
    if (c === '/' && d === '/') { while (i < n && code[i] !== '\n') i++; continue; }
    if (c === '/' && d === '*') { i += 2; while (i < n && !(code[i] === '*' && code[i + 1] === '/')) i++; i += 2; continue; }
    if (c === '"' || c === "'" || c === '`') {
      const q = c; out += c; i++;
      while (i < n) { const e = code[i]; out += e; i++; if (e === '\\') { if (i < n) { out += code[i]; i++; } continue; } if (e === q) break; }
      continue;
    }
    out += c; i++;
  }
  return out;
}
// ── recursive src walk → [{ path, code }] (code comment-stripped) ──
function listJs(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name); const st = statSync(p);
    if (st.isDirectory()) out.push(...listJs(p));
    else if (name.endsWith('.js')) out.push(p);
  }
  return out;
}
// ── brace-matched `const signaling = { … }` slice → its case labels ──
function signalingCases(code) {
  const a = code.indexOf('const signaling'); if (a < 0) return [];
  const open = code.indexOf('{', a); if (open < 0) return [];
  let depth = 0, i = open, end = -1; const n = code.length;
  while (i < n) {
    const c = code[i];
    if (c === '"' || c === "'" || c === '`') { const q = c; i++; while (i < n) { const e = code[i]; i++; if (e === '\\') { i++; continue; } if (e === q) break; } continue; }
    if (c === '{') depth++; else if (c === '}') { depth--; if (depth === 0) { end = i; break; } }
    i++;
  }
  if (end < 0) return [];
  return [...code.slice(open, end).matchAll(/case\s+['"]([a-z][\w-]*)['"]\s*:/g)].map((m) => m[1]);
}

// Non-literal registration calls that are DOCUMENTED mechanisms/planes, not new
// frame-contract wires — exempted PRECISELY by (file, receiver, method, arg) so a
// NEW computed registration anywhere else still fails INV0b closed.
const MECHANISM_EXEMPT = [
  { file: 'pubsub/wireHandlers.js', recv: 'dht',       method: 'onRoutedMessage', arg: 'type',     why: 'B1 routed registration helper; concrete wires come from on(T.X)' },
  { file: 'dht/AxonaPeer.js',       recv: 'peer',      method: 'onRoutedMessage', arg: 'type',     why: 'transport-adapter delegation shim (onRoutedMessage:(type,h)=>peer.onRoutedMessage) — forwards, no new wire' },
  { file: 'web/composite.js',       recv: 't',         method: 'onNotification',  arg: 'type',     why: 'CompositeTransport fan-out; re-registers recorded handlers, concrete wires from composite.onNotification(literal)' },
  { file: 'dht/AxonaPeer.js',       recv: 'transport', method: 'onNotification',  arg: 'wireType', why: 'direct-messaging reply family (wireType=`direct_${type}`) — OUT of REF-1.1 frame-contract scope' },
];

// ── discover: parse a file list → { sites, unresolved } (fail-closed) ──
function discover(fileList) {
  const sites = [];       // { surface, wire, site }
  const unresolved = [];  // registration calls we cannot resolve to a literal (and are not documented mechanisms)
  const LIT = /^\s*['"]([\w:-]+)['"]\s*$/;
  const isMechanism = (fp, recv, method, arg) => MECHANISM_EXEMPT.some((x) => fp.endsWith(x.file) && recv === x.recv && method === x.method && arg === x.arg);
  for (const { path: fp, code } of fileList) {
    for (const m of code.matchAll(/\bon\(\s*T\.([A-Z_]+)\s*,/g)) {
      const w = T[m[1]];
      if (typeof w === 'string' && w) sites.push({ surface: 'routed', wire: w, site: `${fp} on(T.${m[1]})` });
      else unresolved.push(`${fp} on(T.${m[1]}) — T.${m[1]} does not resolve to a wire`);
    }
    for (const m of code.matchAll(/(\w+)\.(onRoutedMessage|onNotification)\s*\(\s*([^,)]+)/g)) {
      const [, recv, method, rawArg] = m;
      const arg = rawArg.trim();
      const lit = rawArg.match(LIT);
      if (lit) {
        const surface = method === 'onRoutedMessage' ? 'routed-dht' : `${recv}-notif`;
        sites.push({ surface, wire: lit[1], site: `${fp} ${recv}.${method}('${lit[1]}')` });
      } else if (isMechanism(fp, recv, method, arg)) {
        // documented mechanism/plane (see MECHANISM_EXEMPT) — introduces no new frame-contract wire
      } else {
        unresolved.push(`${fp} ${recv}.${method}(${arg}) — non-literal registration arg`);
      }
    }
    for (const w of signalingCases(code)) sites.push({ surface: 'bridge-ws', wire: w, site: `${fp} signaling.dispatch case '${w}'` });
  }
  return { sites, unresolved };
}

// ── classification: IN_SCOPE (→ boundary + registry wire) vs OUT_OF_SCOPE ──
const IN_SCOPE = new Map([
  ['bridge-ws|welcome', ['B2', 'welcome']], ['bridge-ws|turn', ['B4', 'turn']],
  ['bridge-ws|peer-list', ['B3', 'peer-list']], ['bridge-ws|peer-joined', ['B3', 'peer-joined']],
  ['bridge-ws|peer-left', ['B3', 'peer-left']], ['bridge-ws|signal', ['B3', 'signal']],
  ['bridge-ws|pong', ['B4', 'pong']], ['bridge-ws|version-gate', ['B4', 'version-gate']],
  ['bridge-notif|hello', ['B2', 'hello']], ['bridge-notif|hello-ack', ['B2', 'hello-ack']],
  ['webrtc-notif|hello', ['B3', 'hello']], ['webrtc-notif|hello-sig', ['B3', 'hello-sig']],
  ['webrtc-notif|cap-attest', ['B2', 'cap-attest']],
  ['routed-dht|mesh:signal', ['B3', 'signal']],   // DHT-relay signalling (Aster F1 recut-1)
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

// ── discover the REAL surface by walking src/ ──
const SRC = fileURLToPath(new URL('../src/', import.meta.url));
const realFiles = listJs(SRC).map((p) => ({ path: relative(SRC, p), code: stripComments(readFileSync(p, 'utf8')) }));
const { sites, unresolved } = discover(realFiles);

// ── INV0. FAIL-CLOSED: every discovered site classified; nothing unresolved ──
{
  const unclassified = sites.filter((e) => classify(e) === null);
  check('INV0. fail-closed scan of src/**/*.js: every discovered registration site is classified in-scope or documented-exclusion; no unclassified site',
    unclassified.length === 0, `\n   unclassified: ${unclassified.map((e) => e.site).join(', ')}`);
  check('INV0b. fail-closed parse: every onRoutedMessage/onNotification call resolves to a literal wire (no non-literal/computed registration silently skipped) — only the wireHandlers mechanism is exempt',
    unresolved.length === 0, `\n   unresolved: ${unresolved.join(', ')}`);
}

const L = sites.map((e) => ({ e, c: classify(e) })).filter((x) => Array.isArray(x.c))
  .map((x) => ({ key: `${x.e.surface}|${x.e.wire}`, boundary: x.c[0], regWire: x.c[1], surface: x.e.surface, wire: x.e.wire }));
const A = (surface, wire) => L.some((x) => x.surface === surface && x.wire === wire);

check(`A1. walked ${realFiles.length} src files; ${sites.length} registration sites → ${L.length} in-scope + ${sites.length - L.length} documented exclusions (routed B1=${L.filter((x) => x.boundary === 'B1').length})`,
  realFiles.length >= 60 && L.filter((x) => x.boundary === 'B1').length === 19 && L.length >= 33);

// ── INV1 forward / INV2 backward vs TABLE_ONLY ──
{
  const miss = L.filter((x) => !REG[x.boundary].has(x.regWire));
  check('INV1. forward: every live in-scope endpoint’s boundary registry contains its mapped wire',
    miss.length === 0, `\n   miss: ${miss.map((x) => x.key).join(', ')}`);
}
const TABLE_ONLY = new Set(['B4:client-hello', 'B4:ping', 'B4:turn-refresh', 'B4:peer-list-request']);
{
  const liveByB = {}; for (const x of L) (liveByB[x.boundary] ??= new Set()).add(x.regWire);
  const miss = [];
  for (const [b, wires] of Object.entries(REG)) for (const w of wires)
    if (!liveByB[b]?.has(w) && !TABLE_ONLY.has(`${b}:${w}`)) miss.push(`${b}:${w}`);
  check('INV2. backward: every registry wire is a live in-scope endpoint of that boundary OR a documented TABLE_ONLY frame',
    miss.length === 0, `\n   miss: ${miss.join(', ')}`);
}
// ── INV3 both B3 signal endpoints pinned / INV4 both hello endpoints pinned ──
check('INV3. the B3 `signal` frame has TWO endpoints pinned by surface: bridge-ws|signal AND routed-dht|mesh:signal, both → B3 wire signal',
  A('bridge-ws', 'signal') && A('routed-dht', 'mesh:signal')
  && L.find((x) => x.surface === 'routed-dht' && x.wire === 'mesh:signal')?.regWire === 'signal' && REG.B3.has('signal'));
check('INV4. hello is TWO endpoints pinned by surface: bridge-notif|hello→B2 AND webrtc-notif|hello→B3',
  L.find((x) => x.key === 'bridge-notif|hello')?.boundary === 'B2' && L.find((x) => x.key === 'webrtc-notif|hello')?.boundary === 'B3' && REG.B2.has('hello') && REG.B3.has('hello'));

// ── O1 namespace / E1-E5 edge cases ──
{
  const bad = [];
  for (const b of Object.keys(REG)) for (const [wire, info] of WIRING[b]) if (!String(info.type).startsWith(REG_PREFIX[b])) bad.push(`${b}:${wire}`);
  check('O1. every registered row type is namespaced to its boundary', bad.length === 0, `\n   ${bad.join(', ')}`);
}
check('E1. welcome → B2 only, never B3', L.find((x) => x.key === 'bridge-ws|welcome')?.boundary === 'B2' && !REG.B3.has('welcome'));
check('E2. cap-attest is the WIRE (carries write-flight-ack-v1 capability codec) → B2; no separate `write-flight-ack` wire',
  L.find((x) => x.key === 'webrtc-notif|cap-attest')?.boundary === 'B2' && REG.B2.has('cap-attest') && !REG.B2.has('write-flight-ack'));
check('E3. peer-list → B3 only, never B4', L.find((x) => x.key === 'bridge-ws|peer-list')?.boundary === 'B3' && !REG.B4.has('peer-list'));
check('E4. peer-list-request → B4 only + TABLE_ONLY', REG.B4.has('peer-list-request') && !REG.B3.has('peer-list-request') && TABLE_ONLY.has('B4:peer-list-request'));
check('E5. turn → B4; no `turn` wire in B2', L.find((x) => x.key === 'bridge-ws|turn')?.boundary === 'B4' && !REG.B2.has('turn'));

// ── NEG. teeth ──
check('NEG1. an in-scope endpoint whose registry lacks the wire FAILS INV1',
  [...L, { boundary: 'B2', regWire: 'ghost-wire' }].some((x) => !REG[x.boundary].has(x.regWire)));
check('NEG2. a wrong reassignment (pong B4→B2) FAILS INV1',
  L.map((x) => x.key === 'bridge-ws|pong' ? { ...x, boundary: 'B2' } : x).some((x) => !REG[x.boundary].has(x.regWire)));
{
  const liveByB = {}; for (const x of L) (liveByB[x.boundary] ??= new Set()).add(x.regWire);
  check('NEG3. a registered-but-not-live wire (not table-only) FAILS INV2', !liveByB.B1?.has('phantom-wire') && !TABLE_ONLY.has('B1:phantom-wire'));
}
// NEG4 (Aster): inject a synthetic FOURTH FILE into DISCOVERY; its new registration
// must be discovered AND unclassified → INV0 fails. Exercises the whole pipeline.
{
  const synthetic = { path: 'src/__neg4_synthetic__.js', code: "x.onRoutedMessage('__neg4_ghost_route__', () => {});" };
  const d = discover([...realFiles, synthetic]);
  const foundGhost = d.sites.some((e) => e.wire === '__neg4_ghost_route__' && e.surface === 'routed-dht');
  const failsInv0 = d.sites.some((e) => classify(e) === null);
  check('NEG4. a synthetic FOURTH source file with a new routed registration is DISCOVERED by the walk and FAILS INV0 until classified (proves file discovery + fail-closed, not just classify())',
    foundGhost && failsInv0);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
