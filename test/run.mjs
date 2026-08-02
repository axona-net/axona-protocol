#!/usr/bin/env node
// =====================================================================
// run.mjs — the test runner that can report its own completeness.
//
// WHY THIS EXISTS
// `npm test` was one `&&` chain of 109 invocations. Two consequences, both
// measured on 2026-07-29:
//
//   1. A failure at position 3 meant positions 4..109 never ran, and the
//      output was a single red line. You fixed #3, re-ran, and #7 failed.
//      One fix became an unbounded queue of surprises with no way to know
//      how much was actually broken.
//
//   2. 35 of 144 test files were in no suite the chain touched. A test that
//      is not wired in looks EXACTLY like a test that passes: both are
//      silent. There was no way to tell them apart from the output.
//
// Every gate in the refactor plan is phrased "the suite is green before this
// ships". Against the old runner that sentence meant "an unknown fraction of
// an unknown number of tests did not fail before the chain stopped". It was
// not a fact, it was a property of a shell expression. This file exists to
// make it a fact.
//
// THE CONTRACT
//   * every selected test RUNS, even after another one fails;
//   * the report states a COUNT, checked against the manifest selection —
//     `ran` must equal `selected` or the run fails on that alone;
//   * a test that cannot even be spawned is a FAILURE, never a skip;
//   * failure is reported ONCE, at the end, with every failing test named.
//
// A NOTE ON THE TIMEOUT, which is itself a lesson from building this. The
// first triage pass wrapped each test in `timeout 120 node ...` and reported
// all 35 orphans failing with exit 127 in zero seconds. That was not 35
// broken tests: macOS has no `timeout` binary, so the shell was reporting
// "command not found" 35 times and the harness was the broken thing. Hence
// the timeout here is Node's own child-process timeout — no external binary,
// no silent absence, and a distinct TIMEOUT verdict that can never be
// confused with a test's own exit code.
//
// USAGE
//   node test/run.mjs                      # the default class
//   node test/run.mjs --class default,extended
//   node test/run.mjs --class all
//   node test/run.mjs --guard              # reconcile disk <-> manifest only
//   node test/run.mjs --list               # print the selection, run nothing
//   node test/run.mjs --timeout 300        # seconds per test (default 180)
// =====================================================================

import { readFileSync, existsSync, readdirSync, statSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join, relative } from 'node:path';

const TEST_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = dirname(TEST_DIR);
const MANIFEST = join(TEST_DIR, 'manifest.json');

// A test file is anything matching these. Kept deliberately narrow: the guard
// compares this set against the manifest, so a loose pattern here would drag
// helpers and fixtures into "orphaned test" reports and train us to ignore it.
const TEST_FILE = /^(smoke|fence)_.*\.m?js$/;

// `quarantined` is a transitional class, not a resting place. It means "known
// to be unrun, and that is a debt with a number attached" — as opposed to
// `retired`, which asserts somebody DECIDED the test is obsolete and said why.
// The guard prints the quarantined count on every run precisely so it cannot
// quietly become permanent, which is how these 35 accumulated in the first place.
const CLASSES = ['default', 'extended', 'integration', 'quarantined', 'retired'];
const RUNNABLE = new Set(['default', 'extended', 'integration', 'quarantined']);

// ─────────────────────────── args ───────────────────────────

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(`--${name}`);
const opt = (name, dflt) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : dflt;
};

const TIMEOUT_MS = Number(opt('timeout', '180')) * 1000;
const JOBS = Math.max(1, Number(opt('jobs', '1')));

// ─────────────────────────── manifest ───────────────────────────

function loadManifest() {
  if (!existsSync(MANIFEST)) {
    fail(`no manifest at ${relative(ROOT, MANIFEST)} — the runner cannot know what it is supposed to run`);
  }
  let m;
  try {
    m = JSON.parse(readFileSync(MANIFEST, 'utf8'));
  } catch (e) {
    fail(`manifest is not valid JSON: ${e.message}`);
  }
  if (!Array.isArray(m.tests)) fail('manifest has no `tests` array');
  return m;
}

/**
 * Every test file on disk, relative to test/.
 *
 * Two naming conventions, because test/ also holds things that are NOT tests:
 * experiment_*, repro_*, diag_*, churn_* and the sweeps are investigation
 * scripts that measure rather than assert, and pulling them in would fill the
 * orphan report with noise until we learned to ignore it. So at the top level
 * only smoke_/fence_ counts, while inside integration/ every .mjs is a test
 * (that directory holds nothing else, and its files predate the prefix rule).
 */
function filesOnDisk() {
  const out = [];
  for (const entry of readdirSync(TEST_DIR).sort()) {
    if (statSync(join(TEST_DIR, entry)).isDirectory()) continue;
    if (TEST_FILE.test(entry)) out.push(entry);
  }
  const intDir = join(TEST_DIR, 'integration');
  if (existsSync(intDir)) {
    for (const entry of readdirSync(intDir).sort()) {
      if (entry.endsWith('.mjs')) out.push(`integration/${entry}`);
    }
  }
  return out;
}

// ─────────────────────────── guard ───────────────────────────
//
// The guard is what makes the fix STAY fixed. Without it a new test can be
// born orphaned and nothing notices, which is the exact mechanism that
// produced the 35. Reconciliation is bidirectional on purpose: a manifest
// entry with no file is just as much a lie as a file with no entry — that is
// how the 4.42.0 revert silently dropped four closed findings when their
// fences were reverted along with the code.

function runGuard(manifest) {
  const disk = new Set(filesOnDisk());
  const declared = new Map();
  const problems = [];

  for (const t of manifest.tests) {
    if (!t.file) { problems.push('manifest entry with no `file`'); continue; }
    if (declared.has(t.file)) problems.push(`declared twice: ${t.file}`);
    declared.set(t.file, t);
    if (!CLASSES.includes(t.class)) {
      problems.push(`${t.file}: class "${t.class}" is not one of ${CLASSES.join('|')}`);
    }
    // A retired or quarantined test with no reason is indistinguishable from
    // one that was dropped by accident. The reason IS the decision record.
    if ((t.class === 'retired' || t.class === 'quarantined') && !t.reason) {
      problems.push(`${t.file}: class "${t.class}" requires a \`reason\``);
    }
    // `hold` marks a quarantined fence that RUNS AND FAILS against a known,
    // unfixed defect — quarantined only so the commit gate stays green while
    // the fix is decided. It is meaningless on any other class: a default test
    // that fails already reddens the gate, and a retired test is a decision.
    if (t.hold && t.class !== 'quarantined') {
      problems.push(`${t.file}: \`hold\` is only valid on class "quarantined" (found on "${t.class}")`);
    }
  }

  const orphaned = [...disk].filter((f) => !declared.has(f));
  const phantom = [...declared.keys()].filter((f) => !disk.has(f));

  for (const f of orphaned) problems.push(`ORPHANED — on disk, in no manifest entry: ${f}`);
  for (const f of phantom) problems.push(`PHANTOM  — in manifest, not on disk: ${f}`);

  const byClass = {};
  for (const c of CLASSES) byClass[c] = manifest.tests.filter((t) => t.class === c).length;

  console.log('manifest guard');
  console.log(`  files on disk      ${disk.size}`);
  console.log(`  declared           ${declared.size}`);
  for (const c of CLASSES) console.log(`    ${c.padEnd(16)} ${byClass[c]}`);

  if (byClass.quarantined > 0) {
    // Loud on every run, by design. Quarantine is a debt; a debt you stop
    // seeing is a debt you stop paying.
    console.log(`\n  ${byClass.quarantined} test(s) QUARANTINED — unrun, awaiting triage. Not a resting state.`);
  }
  const holds = manifest.tests.filter((t) => t.hold);
  for (const t of holds) {
    console.log(`\n  ⛔ RELEASE HOLD: ${t.file} — ${t.hold}`);
  }

  if (problems.length) {
    console.log(`\n  ${problems.length} problem(s):`);
    for (const p of problems) console.log(`    ✗ ${p}`);
    return false;
  }
  console.log('\n  ✓ disk and manifest agree');
  return true;
}

// ─────────────────────────── run ───────────────────────────

function select(manifest, classes) {
  const want = classes === 'all' ? new Set(RUNNABLE) : new Set(String(classes).split(',').map((s) => s.trim()));
  for (const c of want) {
    if (!CLASSES.includes(c)) fail(`unknown class "${c}" — expected ${CLASSES.join('|')} or "all"`);
    if (!RUNNABLE.has(c)) fail(`class "${c}" is not runnable (it declares tests that deliberately do not run)`);
  }
  return manifest.tests.filter((t) => want.has(t.class));
}

function runOne(file) {
  return new Promise((resolve) => {
    const started = Date.now();
    const child = spawn(process.execPath, [join(TEST_DIR, file)], {
      cwd: ROOT,
      stdio: ['ignore', 'pipe', 'pipe'],
      // Node's own timeout — no external binary to be silently missing.
      timeout: TIMEOUT_MS,
      killSignal: 'SIGKILL',
    });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { out += d; });

    // 'error' fires when the process could not be spawned at all. That is a
    // FAILURE, not a skip: an unspawnable test proves nothing and must never
    // be able to pass by being absent.
    child.on('error', (e) => resolve({
      file, verdict: 'FAIL', ms: Date.now() - started, output: `could not spawn: ${e.message}`,
    }));

    child.on('close', (code, signal) => {
      const ms = Date.now() - started;
      const timedOut = signal === 'SIGKILL' && ms >= TIMEOUT_MS - 1000;
      resolve({
        file,
        verdict: timedOut ? 'TIMEOUT' : code === 0 ? 'PASS' : 'FAIL',
        ms,
        code,
        signal,
        output: out,
      });
    });
  });
}

async function runAll(tests) {
  const results = [];
  const queue = [...tests];
  const width = Math.max(...tests.map((t) => t.file.length)) + 2;

  const worker = async () => {
    for (;;) {
      const t = queue.shift();
      if (!t) return;
      const r = await runOne(t.file);
      r.class = t.class;
      results.push(r);
      const mark = r.verdict === 'PASS' ? '✓' : r.verdict === 'TIMEOUT' ? '⏰' : '✗';
      console.log(`  ${mark} ${t.file.padEnd(width)}${String(Math.round(r.ms / 100) / 10).padStart(6)}s  ${r.class}`);
    }
  };

  await Promise.all(Array.from({ length: Math.min(JOBS, queue.length) }, worker));
  return results;
}

function fail(msg) {
  console.error(`\nrun.mjs: ${msg}\n`);
  process.exit(2);
}

// ─────────────────────────── main ───────────────────────────

const manifest = loadManifest();

if (flag('guard')) {
  process.exit(runGuard(manifest) ? 0 : 1);
}

const tests = select(manifest, opt('class', 'default'));

if (flag('list')) {
  for (const t of tests) console.log(`${t.class.padEnd(14)} ${t.file}`);
  console.log(`\n${tests.length} selected`);
  process.exit(0);
}

if (!tests.length) fail('selection is empty — refusing to report success for running nothing');

// The guard runs BEFORE the tests, always. A green suite measured against a
// manifest that disagrees with the disk is the failure this whole file exists
// to prevent, and finding that out after 20 minutes of tests is too late.
const guardOk = runGuard(manifest);

console.log(`\nrunning ${tests.length} test(s)  [class: ${opt('class', 'default')}, timeout ${TIMEOUT_MS / 1000}s, jobs ${JOBS}]\n`);

const results = await runAll(tests);

// ─────────────────────────── report ───────────────────────────

const passed = results.filter((r) => r.verdict === 'PASS');
const failed = results.filter((r) => r.verdict === 'FAIL');
const timedOut = results.filter((r) => r.verdict === 'TIMEOUT');

if (failed.length || timedOut.length) {
  console.log('\n──────── failures ────────');
  for (const r of [...failed, ...timedOut]) {
    console.log(`\n✗ ${r.file}  (${r.verdict}${r.code != null ? `, exit ${r.code}` : ''}${r.signal ? `, ${r.signal}` : ''})`);
    const tail = r.output.trimEnd().split('\n').slice(-25);
    for (const line of tail) console.log(`    ${line}`);
  }
}

console.log('\n──────── summary ────────');
console.log(`  selected  ${tests.length}`);
console.log(`  ran       ${results.length}`);
console.log(`  passed    ${passed.length}`);
console.log(`  failed    ${failed.length}`);
console.log(`  timed out ${timedOut.length}`);

// THE completeness check. Without this the report is just a nicer-looking
// version of the old chain: a count nobody compares against anything is not
// evidence of coverage.
const complete = results.length === tests.length;
if (!complete) {
  console.log(`\n  ✗ INCOMPLETE — ran ${results.length} of ${tests.length} selected. The report cannot be trusted.`);
}
if (!guardOk) {
  console.log('\n  ✗ manifest guard failed (see above) — disk and manifest disagree');
}

// ─────────────────────────── release holds ───────────────────────────
// A held fence is quarantined-but-KNOWN-RED: it pins a live defect whose fix is
// not yet approved. The suite above staying green is deliberate (commit gate);
// what is NOT acceptable is the summary reading as releasable — "PASS 134/134"
// with a known write-loss defect behind it is the same confident-false-negative
// shape as the defects this suite exists to catch (Aster, council seq 146).
//
// So every held fence is RUN here, and its actual state is enforced:
//   still RED  → the hold is confirmed and stamped into the final line.
//   now GREEN  → the manifest is lying about the world. That FAILS the run:
//                the fence must be promoted to `default` and the hold cleared,
//                and the runner refuses to let the hold rot into a stale flag.
// Exit code stays 0 on a confirmed hold — the hold gates RELEASE, not commits —
// but the final line always carries it, so no reading of the output is clean.
const held = manifest.tests.filter((t) => t.class === 'quarantined' && t.hold);
let holdViolations = 0;
const holdLines = [];
if (held.length) {
  console.log('\n──────── release holds ────────');
  for (const t of held) {
    const r = await runOne(t.file);
    if (r.verdict === 'PASS') {
      holdViolations++;
      console.log(`  ✗ ${t.file} is GREEN but held — the defect it pins is fixed.`);
      console.log(`    Promote it to class "default" and remove \`hold\`; a stale hold is a lie in the other direction.`);
    } else {
      holdLines.push(`${t.file} still RED`);
      console.log(`  ⛔ ${t.file} still RED (${r.verdict.toLowerCase()}) — ${t.hold}`);
    }
  }
}

const ok = complete && guardOk && !failed.length && !timedOut.length && !holdViolations;
const holdSuffix = holdLines.length ? `  ·  ⛔ RELEASE HOLD: ${holdLines.join('; ')}` : '';
console.log(`\n${ok ? 'PASS' : 'FAIL'} — ${passed.length}/${tests.length} passed${holdSuffix}\n`);
process.exit(ok ? 0 : 1);
