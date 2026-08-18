// fence_readcap_importer_freeze.mjs — REF-1.1 E3c (SEAL). Freeze the mechanism-shim
// set by MODULE IDENTITY (design decision 3; council-cleared E3 design
// "the shim set ... freezes it by module identity").
//
// readDispatchCapability is the ONE allowlisted reader of the module-private dispatch
// capability channel (the sealed transports' deposited closures). After the seal, the
// ONLY way to reach a raw dispatch primitive is to hold this reader. So the security of
// the seal reduces to: WHICH modules may import readDispatchCapability. This fence
// freezes that set. A new importer — a would-be back door around the door — fails closed.
//
// The frozen set is exactly five modules, each a reviewed entry:
//   - the DEFINER  (registerFrame.js exports it; the canonical door reads its own channel),
//   - the BARREL   (registry/index.js re-exports it),
//   - three MECHANISM SHIMS that legitimately replay/forward a sealed primitive:
//       * CompositeTransport fan-out        (transport/web/composite.js),
//       * AxonaPeer routed demux + adapter  (dht/AxonaPeer.js),
//       * registerDirectFrame direct_* home (registry/registerDirectFrame.js — E3 decision 2).
//
// Adding to this set is a reviewed change (edit FROZEN below + justify), exactly as the
// design requires. The fence is non-vacuous: every frozen entry must actually reference
// the reader, so a silent drop is caught too.
//
// Run: node test/fence_readcap_importer_freeze.mjs
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const SRC = fileURLToPath(new URL('../src', import.meta.url));

// module identity (repo-relative path under src/) -> reviewed role/justification
const FROZEN = {
  'registry/registerFrame.js':       'DEFINER — exports readDispatchCapability; the canonical door reads its own module-private channel',
  'registry/index.js':               'BARREL — re-exports readDispatchCapability from registerFrame.js',
  'transport/web/composite.js':      'MECHANISM SHIM — CompositeTransport fan-out replays a handler to each sub-transport via its capability',
  'dht/AxonaPeer.js':                'MECHANISM SHIM — AxonaPeer routed demux + default-DHT adapter reach the sealed peer routed primitive',
  'registry/registerDirectFrame.js': 'MECHANISM SHIM — the direct_${type} parameterized registrar (E3 decision 2 home)',
};

// strip // line comments and /* */ block comments (keep "://" in URLs intact)
const stripComments = (src) => src
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/(^|[^:])\/\/[^\n]*/g, '$1');

function walk(dir, out = []) {
  for (const name of readdirSync(dir)) {
    const p = `${dir}/${name}`;
    const st = statSync(p);
    if (st.isDirectory()) walk(p, out);
    else if (/\.(mjs|js)$/.test(name)) out.push(p);
  }
  return out;
}

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${++n} - ${m} ${extra}`); fail++; } };

console.log('readDispatchCapability importer freeze — the mechanism-shim set, by module identity\n');

// discover every src module that references readDispatchCapability outside comments
const referencing = new Set();
for (const abs of walk(SRC)) {
  const rel = abs.slice(SRC.length + 1);
  const code = stripComments(readFileSync(abs, 'utf8'));
  if (/\breadDispatchCapability\b/.test(code)) referencing.add(rel);
}

// 1. every referencing module is in the frozen set (no new back door around the door)
for (const rel of [...referencing].sort()) {
  ok(`1. ${rel} is a frozen readDispatchCapability holder`, rel in FROZEN,
     '— a NEW module reached the capability reader: a back door around the sealed door. Add a reviewed FROZEN entry or remove the import.');
}

// 2. non-vacuous: every frozen entry actually references the reader (no silent drop)
for (const rel of Object.keys(FROZEN).sort()) {
  ok(`2. frozen entry ${rel} still references readDispatchCapability`, referencing.has(rel),
     '— frozen entry no longer holds the reader; update FROZEN.');
}

// 3. the set matches exactly (size check, so neither list drifts unnoticed)
ok(`3. exactly ${Object.keys(FROZEN).length} modules hold readDispatchCapability`,
   referencing.size === Object.keys(FROZEN).length,
   `— found ${referencing.size}: ${[...referencing].sort().join(', ')}`);

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
