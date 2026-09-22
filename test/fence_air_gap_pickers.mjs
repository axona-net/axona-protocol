// fence_air_gap_pickers.mjs — Bridge-Air-Gap-Plan v0.3 §7.1.3, WP4 row P1.
//
// The static audit. docs/TRANSIT-PICKERS.md §A lists every chooser of a next hop
// or a role holder. This fence holds that list against the tree:
//   (1) every chooser named in the table exists in source at the file it names;
//   (2) every chooser's body consults the capability (isTransit / isIntroduction /
//       introductionIds / _isIntroductionId / capabilityFor / bridgeId), or is one
//       of the two composite methods that ARE the gate;
//   (3) no function in src/dht or src/pubsub that iterates the synaptome or
//       dht.neighbors() to pick a peer is missing from the table.
// A chooser present in source and absent from the table FAILS. A listed chooser
// without the call FAILS. Missing instrumentation is a failure, not a zero.
import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

const table = readFileSync(join(root, 'docs/TRANSIT-PICKERS.md'), 'utf8');
const sectionA = table.split('## B.')[0];
// rows look like: | dht/AxonaPeer.js:4025 `_greedyNextHopToward` | ... — a row may name two functions
const rows = [];
for (const line of sectionA.split('\n')) {
  const m = line.match(/^\| ((?:dht|pubsub|transport\/web)\/[A-Za-z]+\.js)[^|]*\|/);
  if (!m) continue;
  const file = m[1];
  const names = [...line.matchAll(/:(?:\d+|~[\d–]+)\s+`([A-Za-z_]+)`/g)].map((x) => x[1]);
  const special = /route_msg/.test(line);
  const gatedElsewhere = /\(gated at/.test(line);
  rows.push({ file, names, special, gatedElsewhere, line });
}
check('table §A parsed with rows', rows.length >= 12, String(rows.length));

const CAP = /isTransit|isIntroduction|introductionIds|_isIntroductionId|capabilityFor|bridgeId\(\)|opClass/;
function fnBody(src, name) {
  const re = new RegExp(`^\\s*(?:static\\s+)?(?:async\\s+)?${name}\\s*\\([^)]*\\)\\s*\\{`, 'm');
  const m = re.exec(src);
  if (!m) return null;
  let i = m.index + m[0].length, depth = 1;
  while (i < src.length && depth > 0) { const c = src[i++]; if (c === '{') depth++; else if (c === '}') depth--; }
  return src.slice(m.index, i);
}

console.log('\n[P1] every listed chooser exists and consults the capability');
for (const r of rows) {
  const src = readFileSync(join(root, 'src', r.file), 'utf8');
  if (r.special) {
    // the route_msg receive handler is a closure inside registerFrame, not a method
    const idx = src.indexOf("registerFrame(transport, 'route_msg'");
    const body = idx >= 0 ? src.slice(idx, idx + 4000) : '';
    check(`${r.file} route_msg receive scan consults isTransit`, /isTransit\(/.test(body));
    continue;
  }
  for (const name of r.names) {
    const body = fnBody(src, name);
    check(`${r.file} \`${name}\` exists`, body !== null);
    if (body === null) continue;
    if (r.gatedElsewhere) { check(`${r.file} \`${name}\` is gated at the composite (row says so)`, true); continue; }
    check(`${r.file} \`${name}\` consults the capability`, CAP.test(body));
  }
}

console.log('\n[P1] no chooser outside the table');
// A chooser iterates the synaptome or dht.neighbors() and compares XOR distance or picks a peer.
const listed = new Set(rows.flatMap((r) => r.names.map((n) => `${r.file}:${n}`)));
const files = [];
for (const d of ['dht', 'pubsub']) for (const f of readdirSync(join(root, 'src', d))) if (f.endsWith('.js')) files.push(`${d}/${f}`);
// Iterate the synaptome or neighbours but are NOT hop or role choosers, with the reason:
const KNOWN_NON_PICKERS = new Set([
  'dht/AxonaPeer.js:_installRoutingHandlers',   // container of the route_msg closure; the closure is the special row above
  'dht/AxonaPeer.js:_lookupStep',               // discovery: queries peers (any class may be queried); returns ids to route TO
  'dht/AxonaPeer.js:findKClosest',              // discovery: same; consumers (cohort, heirs) filter what it returns
  'dht/NeuronNode.js:progressCandidates',       // XOR helper over the synaptome; its callers are the choosers and are listed
  'dht/NeuronNode.js:bestByAP',                 // XOR helper; callers: the lookahead_probe REPLY (names a peer, never a hop) and _lookupStep
  'pubsub/writeFlight.js:_flightEvict',         // evicts a write flight; picks nothing
  'dht/AxonaPeer.js:_vitality', 'dht/AxonaPeer.js:_reinforceWave', 'dht/AxonaPeer.js:_selfIntegrate',
  'dht/AxonaPeer.js:lookaheadStats', 'dht/AxonaPeer.js:health', 'dht/AxonaPeer.js:getMetrics',
  'dht/AxonaPeer.js:introductionIds', 'dht/AxonaPeer.js:_pinFor',
]);
const suspects = [];
for (const f of files) {
  const src = readFileSync(join(root, 'src', f), 'utf8');
  const re = /^\s*(?:static\s+)?(?:async\s+)?([A-Za-z_]+)\s*\([^)]*\)\s*\{/gm;
  let m;
  while ((m = re.exec(src)) !== null) {
    const name = m[1];
    if (['if', 'for', 'while', 'switch', 'catch', 'function', 'constructor'].includes(name)) continue;
    const body = fnBody(src, name);
    if (!body) continue;
    const iterates = /synaptome\.values\(\)|dht\.neighbors\(\)|neighbors\(\)\s*\|\|/.test(body);
    const picks = /\^\s*(target|tBig|topicBig|targetBig|targetKey|this\.nodeId)|bestDist|bestPeerId|nextHopId|slice\(0,\s*(K|n|this\._)|\.sort\(/.test(body);
    if (iterates && picks) {
      const key = `${f}:${name}`;
      if (!listed.has(key) && !KNOWN_NON_PICKERS.has(key)) suspects.push(key);
    }
  }
}
check('no synaptome/neighbour chooser outside the table', suspects.length === 0, suspects.join(', '));

console.log(`\nResult: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
