#!/usr/bin/env node
// sync-cachebust.mjs — make every `?v=` import tag equal the kernel version.
//
// WHY THIS EXISTS. The browser apps import kernel modules with a cache-busting
// query — `/src/index.js?v=4.49.0`. The string is what makes a returning browser
// re-fetch instead of serving the copy it stored last release. It was hand-edited,
// so it drifted: on 2026-08-04 the live apps ran server-side 4.59.2 while their
// import tags still said 4.49.0, and examples/minimal-pubsub-browser said 4.24.0
// — twenty-five versions stale. A cold `curl` reports the new version and a warm
// browser runs the old one, so the drift is invisible to exactly the check most
// likely to be run after a deploy.
//
// WHY BUILD TIME AND NOT RUNTIME. You cannot import KERNEL_VERSION to construct
// the URL you import KERNEL_VERSION from. The tag has to be written into the
// source before the browser sees it, so package.json is the single source and
// this script is the only writer.
//
//   node scripts/sync-cachebust.mjs          rewrite tags to package.json version
//   node scripts/sync-cachebust.mjs --check  exit 1 if any tag is stale (CI gate)
import { readFileSync, writeFileSync, readdirSync, statSync } from 'node:fs';
import { join, dirname, extname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT    = dirname(dirname(fileURLToPath(import.meta.url)));
const VERSION = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')).version;
const CHECK   = process.argv.includes('--check');

// Everything served to a browser. Add new deployed directories HERE — a surface
// missing from this list is a surface that will rot, which is how examples/ got
// to 4.24.0 while nobody was looking.
const SCAN = ['apps', 'examples'];
const EXT  = new Set(['.js', '.mjs', '.html', '.css']);
const TAG  = /(\?v=)(\d+\.\d+\.\d+)/g;

function* walk(dir) {
  let entries;
  try { entries = readdirSync(dir); } catch { return; }
  for (const e of entries) {
    if (e === 'node_modules' || e.startsWith('.')) continue;
    const p = join(dir, e);
    if (statSync(p).isDirectory()) yield* walk(p);
    else if (EXT.has(extname(p))) yield p;
  }
}

const stale = [];
let rewritten = 0;

for (const dir of SCAN) {
  for (const file of walk(join(ROOT, dir))) {
    const src = readFileSync(file, 'utf8');
    let hits = 0;
    const out = src.replace(TAG, (m, prefix, found) => {
      if (found === VERSION) return m;
      hits++;
      stale.push(`${file.slice(ROOT.length + 1)} — ?v=${found}`);
      return prefix + VERSION;
    });
    if (hits && !CHECK) { writeFileSync(file, out); rewritten += hits; }
  }
}

if (CHECK) {
  if (stale.length) {
    console.error(`cache-bust CHECK FAILED — ${stale.length} tag(s) not at ${VERSION}:`);
    for (const s of stale) console.error('  ' + s);
    console.error('\nRun: node scripts/sync-cachebust.mjs');
    process.exit(1);
  }
  console.log(`cache-bust OK — every ?v= tag is ${VERSION}`);
} else {
  console.log(rewritten
    ? `cache-bust synced — ${rewritten} tag(s) → ${VERSION}`
    : `cache-bust already at ${VERSION} — nothing to do`);
}
