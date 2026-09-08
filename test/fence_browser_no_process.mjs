// =====================================================================
// fence_browser_no_process.mjs — 4.79.0
//
// `sub()` threw in EVERY browser. The first statement of _steerColdSubscribe
// read a bare `process.env.SUB_LOOKUP_MS`, and reading an undeclared
// identifier is a ReferenceError, not undefined:
//
//     sub() → pubsubSubscribe() → _sendSubscribe() → _steerColdSubscribe()
//                                                    └── ReferenceError
//
// Measured 2026-09-08 on demo.axona.net/examples/minimal-pubsub-browser/:
// Safari and Chromium threw the identical error at the identical stack. It was
// not a Safari defect, and it was not a connectivity defect — it surfaced as an
// unhandled promise REJECTION, so it read as "no peers" for as long as it did.
//
// THE FENCE IS THE CLASS, NOT THE FOUR LINES. Any bare `process` in code a
// browser loads is the same latent ReferenceError, and it will be introduced
// again — the reads that broke this were added one at a time, each looking
// local and harmless, and one of them wore a `process.env && process.env.X`
// guard that cannot work: the `&&` never runs, because evaluating the left
// operand is itself the throw.
//
// So: (A) prove the accessor is correct with no `process` at all, and
//     (B) prove no browser-reachable module reaches for `process` directly.
// =====================================================================
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { envNum, envStr, envList, hasProcessEnv } from '../src/utils/env.js';

const SRC = fileURLToPath(new URL('../src/', import.meta.url));
let pass = 0, fail = 0;
const ok = (cond, name, detail = '') => {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fail++; console.log(`  ✗ ${name}${detail ? ` — ${detail}` : ''}`); }
};

// ── A. the accessor, in a host with no `process` whatsoever ──────────
// env.js is imported ABOVE, while `process` still exists, because the ESM
// loader itself needs it. The functions read the global lazily at CALL time,
// which is exactly when a browser would call them — so removing it now
// reproduces the browser faithfully.
console.log('A. accessor with globalThis.process removed');
const saved = globalThis.process;
delete globalThis.process;
try {
  ok(typeof process === 'undefined', 'the host really has no `process`');

  let threw = null;
  let n, s, l, h;
  try { n = envNum('SUB_LOOKUP_MS', 600); s = envStr('RUN_ID'); l = envList('FLEET_ALLOWLIST'); h = hasProcessEnv(); }
  catch (e) { threw = e; }

  ok(threw === null, 'no throw with no process', threw && threw.message);
  ok(n === 600,  'envNum returns its fallback');
  ok(s === null, 'envStr returns null');
  ok(l === null, 'envList returns null');
  ok(h === false, 'hasProcessEnv reports false');
} finally {
  globalThis.process = saved;
}

// ── the fallback must also survive a GARBAGE value, not just an absent one ──
console.log('   …and with a malformed value present');
process.env.__FENCE_BAD__ = 'not-a-number';
ok(envNum('__FENCE_BAD__', 42) === 42, 'envNum rejects NaN rather than propagating it');
process.env.__FENCE_NUM__ = '17';
ok(envNum('__FENCE_NUM__', 42) === 17, 'envNum still reads a real value under Node');
delete process.env.__FENCE_BAD__; delete process.env.__FENCE_NUM__;

// ── B. no browser-reachable module touches `process` directly ────────
// Node-only trees are exempt: they cannot be reached by a browser import.
console.log('B. static scan of browser-reachable src/');
const EXEMPT_DIRS = ['transport/node/', 'persistence/'];
const EXEMPT_FILES = ['utils/env.js'];   // the one place the guard is written

const walk = (dir) => readdirSync(dir).flatMap((e) => {
  const p = join(dir, e);
  return statSync(p).isDirectory() ? walk(p) : (p.endsWith('.js') ? [p] : []);
});

const offenders = [];
for (const file of walk(SRC)) {
  const rel = relative(SRC, file);
  if (EXEMPT_DIRS.some((d) => rel.startsWith(d)) || EXEMPT_FILES.includes(rel)) continue;
  // Strip block comments across the WHOLE file before splitting — the word
  // "process" appears constantly in this codebase's prose ("in-process",
  // "process restart"), and a per-line stripper cannot see that a line sits
  // inside a multi-line /** … */. Newlines are preserved so numbers stay true.
  const src = readFileSync(file, 'utf8')
    .replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '));
  src.split('\n').forEach((line, i) => {
    const code = line.replace(/\/\/.*$/, '');
    if (!/\bprocess\b/.test(code)) return;
    // `globalThis.process?.x` and `typeof process` are the two safe forms.
    if (/globalThis\s*\.\s*process/.test(code)) return;
    if (/typeof\s+process/.test(code)) return;
    offenders.push(`${rel}:${i + 1}: ${line.trim()}`);
  });
}
ok(offenders.length === 0,
   'no bare `process` outside node-only trees',
   offenders.length ? `\n      ${offenders.join('\n      ')}` : '');

// ── C. the exact line that broke it stays fixed ──────────────────────
console.log('C. the call site that threw');
const mgr = readFileSync(join(SRC, 'pubsub/AxonaManager.js'), 'utf8');
const steer = mgr.slice(mgr.indexOf('_steerColdSubscribe(topicBig)'));
ok(steer.length > 0, '_steerColdSubscribe still exists');
ok(!/\bprocess\s*\.\s*env/.test(steer.slice(0, 4000)),
   '_steerColdSubscribe reads no process.env');

console.log(`\n${fail === 0 ? '✓' : '✗'} ${pass}/${pass + fail}`);
process.exit(fail === 0 ? 0 : 1);
