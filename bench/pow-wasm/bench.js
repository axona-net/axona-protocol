// Bench orchestrator (main thread): rich device info → cycle a SUITE of tests in
// a Worker (fault-tolerant: a test that fails is skipped with a note, the rest
// keep going) → aggregate → render → report. See README.md.

// Bump on every bench change so a stale cached app is obvious in the UI.
const BENCH_VERSION = '0.5.0';

// Register memory-hard candidates here as they compile (drop the file in
// candidates/ implementing candidates/template.js):
const CANDIDATES = {
  'cuckoo':          './candidates/cuckoo.js',            // asymmetric, memory-bandwidth-hard (run first)
  'equihash':        './candidates/equihash.js',          // asymmetric, generalized-birthday (memory-capacity)
  'sha256-baseline': './candidates/sha256-baseline.js',   // reference (not memory-hard)
  'argon2id':        './candidates/argon2id.js',          // demoted symmetric fallback (not in suite)
};

// Per-candidate suite metadata (suiteDifficulties + label), read once on load by
// importing each module (cheap — heavy deps inside candidates are lazy).
const CANDIDATE_META = {};
async function loadMeta() {
  for (const [k, url] of Object.entries(CANDIDATES)) {
    try {
      const m = await import(url);
      CANDIDATE_META[k] = {
        name: m.name || k,
        version: m.version || '?',
        suiteDifficulties: Array.isArray(m.suiteDifficulties) ? m.suiteDifficulties : [12, 16, 18, 20],
        difficultyLabel: m.difficultyLabel || 'difficulty',
        trials: Number.isInteger(m.trials) && m.trials > 0 ? m.trials : null,   // optional per-candidate override
      };
    } catch (e) {
      CANDIDATE_META[k] = { name: k, version: 'load-error', suiteDifficulties: [12, 16, 18, 20], difficultyLabel: 'difficulty', loadError: String(e.message || e) };
    }
  }
}

// One line that proves which app + candidate files actually loaded (vs a stale
// cached copy). Each version lives in its own file, so a cached file shows its
// old number here.
function renderBuildInfo() {
  const el = $('buildinfo'); if (!el) return;
  const parts = Object.keys(CANDIDATES).map((k) => `${k} v${CANDIDATE_META[k] ? CANDIDATE_META[k].version : '?'}`);
  el.textContent = `app v${BENCH_VERSION} · ${parts.join(' · ')}`;
}

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function percentile(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

// ── device identity + rich info ─────────────────────────────────────
function deviceId() {
  try {
    let id = localStorage.getItem('powbench-device-id');
    if (!id) {
      id = (crypto.randomUUID ? crypto.randomUUID()
                              : Date.now().toString(36) + Math.random().toString(36).slice(2, 10));
      localStorage.setItem('powbench-device-id', id);
    }
    return id;
  } catch { return 'no-storage'; }
}
function deviceLabel() { try { return localStorage.getItem('powbench-device-label') || ''; } catch { return ''; } }

function shortUa(ua) {
  ua = ua || '';
  if (/iPhone/.test(ua)) return 'iPhone'; if (/iPad/.test(ua)) return 'iPad';
  if (/Android/.test(ua)) return 'Android'; if (/Macintosh/.test(ua)) return 'Mac';
  if (/Windows/.test(ua)) return 'Windows'; if (/Linux/.test(ua)) return 'Linux';
  return ua.slice(0, 16);
}

// WebGL unmasked renderer — the real GPU string where the browser allows it.
function gpuInfo() {
  try {
    const c = document.createElement('canvas');
    const gl = c.getContext('webgl') || c.getContext('experimental-webgl');
    if (!gl) return null;
    const ext = gl.getExtension('WEBGL_debug_renderer_info');
    return ext ? String(gl.getParameter(ext.UNMASKED_RENDERER_WEBGL)) : null;
  } catch { return null; }
}

let DEVICE = baseDevice();
function baseDevice() {
  return {
    benchVersion: BENCH_VERSION,                       // tag each result with the app build
    deviceId: deviceId(),
    deviceLabel: deviceLabel(),
    ua: navigator.userAgent,
    platform: navigator.platform || null,
    deviceMemoryGB: navigator.deviceMemory ?? null,    // coarse (Chrome): 0.25..8
    cores: navigator.hardwareConcurrency ?? null,
    screen: `${screen.width}x${screen.height}@${window.devicePixelRatio}`,
    crossOriginIsolated: self.crossOriginIsolated === true,
    gpu: gpuInfo(),
    ts: new Date().toISOString(),
  };
}
// Async enrichment where available: Client Hints (real model/arch on Chromium),
// network type. iOS Safari lacks userAgentData → falls back to UA gracefully.
async function enrichDevice() {
  try {
    const uad = navigator.userAgentData;
    if (uad?.getHighEntropyValues) {
      const h = await uad.getHighEntropyValues(['model', 'platformVersion', 'architecture', 'bitness']);
      DEVICE.model = h.model || null;
      DEVICE.uaPlatform = uad.platform || null;
      DEVICE.platformVersion = h.platformVersion || null;
      DEVICE.arch = h.architecture || null;
      DEVICE.bitness = h.bitness || null;
      DEVICE.mobile = uad.mobile ?? null;
    }
  } catch { /* */ }
  try { DEVICE.connection = navigator.connection?.effectiveType || null; } catch { /* */ }
  renderDevice();
}
function deviceInfo() {
  DEVICE.deviceLabel = deviceLabel();
  DEVICE.ts = new Date().toISOString();
  return { ...DEVICE };
}
function renderDevice() {
  const d = DEVICE;
  const name = d.deviceLabel || d.model || shortUa(d.ua);
  const bits = [
    name,
    d.gpu ? `GPU: ${d.gpu}` : null,
    (d.uaPlatform || d.platform) && d.platformVersion ? `${d.uaPlatform || d.platform} ${d.platformVersion}` : (d.platform || null),
    d.arch ? `${d.arch}${d.bitness ? '/' + d.bitness : ''}` : null,
    d.deviceMemoryGB != null ? `~${d.deviceMemoryGB} GB RAM` : null,
    d.cores != null ? `${d.cores} cores` : null,
    d.connection ? `net ${d.connection}` : null,
    `isolated=${d.crossOriginIsolated}`,
  ].filter(Boolean);
  if ($('devsummary')) $('devsummary').textContent = bits.join(' · ');
  if ($('dev')) $('dev').textContent = JSON.stringify(d, null, 2);
}

async function uaMemoryBytes() {
  try {
    if (self.crossOriginIsolated && performance.measureUserAgentSpecificMemory) {
      const m = await performance.measureUserAgentSpecificMemory();
      return m.bytes;
    }
  } catch { /* */ }
  return null;
}

// ── the test suite ──────────────────────────────────────────────────
// Each candidate declares its own meaningful sweep (SHA: zero-bits; argon2id:
// memory-MB; Cuckoo: edge-bits), so the suite is candidate × that candidate's
// difficulties — apples-to-apples within a candidate.
function buildSuite() {
  const specs = [];
  for (const c of Object.keys(CANDIDATES)) for (const d of (CANDIDATE_META[c]?.suiteDifficulties || [])) specs.push({ candidate: c, difficulty: d });
  return specs;
}
const testKey = (s) => `${s.candidate}@${s.difficulty}`;
const suiteState = new Map();   // testKey → { status, note, lastMint }
function stateFor(s) {
  let v = suiteState.get(testKey(s));
  if (!v) { v = { status: 'pending', note: '', lastMint: null }; suiteState.set(testKey(s), v); }
  return v;
}
function renderSuite() {
  const el = $('suite'); if (!el) return;
  el.innerHTML = '<table><tbody>' + buildSuite().map((s) => {
    const v = stateFor(s);
    const label = CANDIDATE_META[s.candidate]?.difficultyLabel || 'd';
    const icon = v.status === 'ok' ? '✓' : v.status === 'skipped' ? '⏭' : '·';
    const note = v.status === 'skipped' ? `<span style="color:#b00">skipped: ${v.note}</span>`
               : (v.lastMint != null ? `${v.lastMint} ms` : '');
    return `<tr><td>${icon}</td><td>${s.candidate} · ${label}=${s.difficulty}</td><td class="muted">${note}</td></tr>`;
  }).join('') + '</tbody></table>';
}

let lastResult = null;

// ── one benchmark run (spec optional; default = manual inputs) ───────
// Fault-tolerant: a worker OOM (worker.onerror) or a run exceeding maxMs is
// caught and returned as a result with oom/timedOut set — never throws.
async function runOnce(spec) {
  const candidateKey = spec ? spec.candidate : $('candidate').value;
  const difficulty   = spec ? spec.difficulty : parseInt($('difficulty').value, 10);
  const trials       = CANDIDATE_META[candidateKey]?.trials ?? Math.max(1, parseInt($('trials').value, 10) || 5);
  const maxMs        = Math.max(2000, parseInt($('maxms').value, 10) || 300000);   // safety ceiling, not a tight budget — a benchmark shouldn't give up early
  const pubkeyHex    = $('pubkey').value.trim() || 'aa'.repeat(32);

  $('status').textContent = `loading ${candidateKey} d=${difficulty}…`;
  const trialsData = [];
  let oom = false, error = null, timedOut = false, candidateName = candidateKey;
  const memBefore = await uaMemoryBytes();
  const worker = new Worker('./worker.js', { type: 'module' });

  await new Promise((resolve) => {
    const finish = () => { clearTimeout(timer); resolve(); };
    const timer = setTimeout(() => { timedOut = true; try { worker.terminate(); } catch { /* */ } finish(); }, maxMs);
    worker.onmessage = (e) => {
      const m = e.data;
      if (m.type === 'loaded') {
        candidateName = m.name;
        $('status').textContent = `${candidateKey} d=${difficulty}: ${trials} trials…`;
        worker.postMessage({ type: 'run', pubkeyHex, difficulty, trials });
      } else if (m.type === 'trial') {
        trialsData.push(m);
        $('status').textContent = `${candidateKey} d=${difficulty} · trial ${m.i + 1}/${trials} · ${m.mintMs.toFixed(0)}ms · ${(m.peakMemBytes / 1e6).toFixed(0)}MB`;
      } else if (m.type === 'done') {
        finish();
      } else if (m.type === 'error') {
        error = m.message; finish();
      }
    };
    worker.onerror = (ev) => { oom = true; error = ev.message || 'worker died (likely OOM)'; finish(); };
    worker.postMessage({ type: 'load', candidateUrl: CANDIDATES[candidateKey] });
  });
  try { worker.terminate(); } catch { /* */ }
  const memAfter = await uaMemoryBytes();

  const mint = trialsData.map((t) => t.mintMs);
  const result = {
    device: deviceInfo(),
    candidate: candidateName, candidateKey, difficulty, trials: trialsData.length,
    mint_ms: {
      p50: percentile(mint, 50), p90: percentile(mint, 90), p99: percentile(mint, 99),
      min: mint.length ? Math.min(...mint) : null, max: mint.length ? Math.max(...mint) : null,
    },
    verify_ms_avg: trialsData.length ? trialsData.reduce((a, t) => a + t.verifyMs, 0) / trialsData.length : null,
    peak_wasm_mem_mb: Math.max(0, ...trialsData.map((t) => t.peakMemBytes)) / 1e6,
    ua_mem_delta_mb: (memBefore != null && memAfter != null) ? (memAfter - memBefore) / 1e6 : null,
    witness_len: trialsData.length ? trialsData[trialsData.length - 1].witnessLen : null,
    all_verified: trialsData.length > 0 && trialsData.every((t) => t.ok),
    oom, timedOut, error,
  };
  lastResult = result;
  render(result);
  return result;
}

function specFailed(r)  { return !!r.oom || !!r.timedOut || !!r.error || r.trials === 0 || !r.all_verified; }
function failNote(r)    {
  if (r.oom) return 'OOM (out of memory)';
  if (r.timedOut) return 'too slow (timeout)';
  if (r.error) return String(r.error).slice(0, 40);
  if (r.trials === 0) return 'no trials';
  if (!r.all_verified) return 'verify failed';
  return 'failed';
}

function render(r) {
  const rows = [
    ['candidate', r.candidate],
    ['difficulty', r.difficulty],
    ['trials', r.trials],
    ['mint p50 / p90 / p99 (ms)', r.mint_ms.p50 != null ? `${r.mint_ms.p50.toFixed(0)} / ${r.mint_ms.p90.toFixed(0)} / ${r.mint_ms.p99.toFixed(0)}` : '—'],
    ['verify avg (ms)', r.verify_ms_avg != null ? r.verify_ms_avg.toFixed(3) : '—'],
    ['peak WASM mem (MB)', r.peak_wasm_mem_mb.toFixed(1)],
    ['witness length (chars)', r.witness_len ?? '—'],
    ['all verified', r.all_verified ? 'yes' : 'NO'],
    ['status', r.oom ? 'OOM' : r.timedOut ? 'TIMEOUT' : r.error ? r.error : 'ok'],
  ];
  $('results').innerHTML = rows.map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`).join('');
  $('json').value = JSON.stringify(r, null, 2);
}

// ── comparison report (collector → device) ──────────────────────────
function renderLeaderboard(report) {
  const el = $('compare'); if (!el) return;
  if (!lastResult) { el.textContent = 'run a benchmark to see where your device stands.'; return; }
  const myId = deviceId(), myUa = navigator.userAgent;
  const cand = lastResult.candidate, diff = lastResult.difficulty;
  const myMint = lastResult.mint_ms?.p50 != null ? Math.round(lastResult.mint_ms.p50) : null;
  const rows = (report.devices || []).filter((e) => e.c === cand && e.d === diff).sort((a, b) => a.mint - b.mint);
  if (!rows.length) {
    el.innerHTML = myMint != null ? `your mint p50: <b>${myMint} ms</b> — waiting for others on this test…` : '(no comparison data yet)';
    return;
  }
  const mints = rows.map((e) => e.mint);
  const median = mints[Math.floor((mints.length - 1) / 2)];
  const meIdx = rows.findIndex((e) => e.id === myId || e.id === 'ua:' + myUa);
  const rankTxt = meIdx >= 0 ? `rank <b>${meIdx + 1}</b> of ${rows.length}` : `not yet ranked (${rows.length} others)`;
  const head =
    `<div><b>${cand}</b> · difficulty ${diff} — ${rows.length} device(s)</div>` +
    `<div>your mint p50: <b>${myMint ?? '?'} ms</b> · ${rankTxt}</div>` +
    `<div class="muted">fastest ${mints[0]} ms · median ${median} ms · slowest ${mints[mints.length - 1]} ms</div>`;
  const list = rows.slice(0, 12).map((e, i) => {
    const me = (e.id === myId || e.id === 'ua:' + myUa);
    const name = e.label || (e.ua ? shortUa(e.ua) : String(e.id).slice(0, 10));
    return `<tr style="${me ? 'font-weight:700;background:#eef' : ''}"><td>${i + 1}</td><td>${name}${me ? ' (you)' : ''}</td><td>${e.mint} ms</td><td>${e.mem ?? '?'}MB</td><td>${e.oom ? 'OOM' : ''}</td></tr>`;
  }).join('');
  el.innerHTML = head + `<table style="margin-top:.4rem"><tbody>${list}</tbody></table>`;
}

function maybeSubmit(result) {
  const url = $('collector').value.trim();
  if (!url) return;
  fetch(url.replace(/\/$/, '') + '/submit', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(result),
  }).catch(() => { /* best-effort */ });
}

async function reportNow(result) {
  $('report').disabled = true;
  try {
    const { reportToAxona } = await import('./axona-report.js');
    await reportToAxona(result, (m) => { $('status').textContent = m; }, renderLeaderboard);
  } catch (e) {
    $('status').textContent = 'Axona report failed: ' + (e.message || e);
  } finally { $('report').disabled = false; }
}

// ── orchestration ───────────────────────────────────────────────────
async function singleRun() {
  if (continuous) return;
  $('run').disabled = true;
  try {
    const r = await runOnce();
    maybeSubmit(r);
    if ($('autoReport').checked) await reportNow(r); else $('status').textContent = 'done';
  } catch (e) { $('status').textContent = 'error: ' + (e.message || e); }
  finally { $('run').disabled = false; }
}

let continuous = false, reporter = null, iter = 0;

// Continuous mode: cycle the whole suite, repeat. A test that fails on this
// device is marked skipped (with a note) and not retried; the rest keep going.
// Failed runs are still PUBLISHED (oom/timeout is useful Stage-4 data).
async function startContinuous() {
  if (continuous) return;
  continuous = true; iter = 0;
  $('loop').textContent = 'Stop'; $('run').disabled = true;
  const gap = Math.max(0, parseInt($('gap').value, 10) || 0);
  for (const s of buildSuite()) { const v = stateFor(s); v.status = 'pending'; v.note = ''; }   // reset on start
  renderSuite();

  if ($('autoReport').checked) {
    try {
      const { createReporter } = await import('./axona-report.js');
      reporter = await createReporter((m) => { $('status').textContent = m; }, renderLeaderboard);
    } catch (e) {
      $('status').textContent = 'Axona connect failed — continuing local-only: ' + (e.message || e);
      reporter = null;
    }
  }

  while (continuous) {
    let ranAny = false;
    for (const spec of buildSuite()) {
      if (!continuous) break;
      const st = stateFor(spec);
      if (st.status === 'skipped') continue;             // a failed test stays skipped
      ranAny = true;
      iter++; $('iter').textContent = String(iter);
      let r;
      try { r = await runOnce(spec); }
      catch (e) { st.status = 'skipped'; st.note = 'threw: ' + (e.message || e); renderSuite(); continue; }

      if (specFailed(r)) {
        st.status = 'skipped'; st.note = failNote(r);
        $('status').textContent = `${spec.candidate} d=${spec.difficulty} — skipped (${st.note})`;
      } else {
        st.status = 'ok'; st.note = '';
        st.lastMint = r.mint_ms?.p50 != null ? Math.round(r.mint_ms.p50) : null;
      }
      renderSuite();

      maybeSubmit(r);                                    // publish success AND failure (failures are data)
      if (reporter) {
        try { await reporter.publish(r); }
        catch (e) { $('status').textContent = 'publish failed: ' + (e.message || e); }
      }
      if (!continuous) break;
      await sleep(gap);
    }
    if (!ranAny) { $('status').textContent = 'all tests skipped on this device — stopping.'; break; }
  }
  await stopContinuous();
}

async function stopContinuous() {
  continuous = false;
  $('loop').textContent = 'Run suite continuously';
  $('run').disabled = false;
  if (reporter) { try { await reporter.close(); } catch { /* */ } reporter = null; }
}

// ── wiring ──────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const sel = $('candidate');
  for (const k of Object.keys(CANDIDATES)) { const o = document.createElement('option'); o.value = k; o.textContent = k; sel.appendChild(o); }

  const lab = $('label');
  lab.value = deviceLabel();
  lab.addEventListener('input', () => {
    try { localStorage.setItem('powbench-device-label', lab.value.trim()); } catch { /* */ }
    renderDevice();
  });

  renderDevice();
  enrichDevice();            // async: model / GPU / arch where available
  if ($('buildinfo')) $('buildinfo').textContent = `app v${BENCH_VERSION} · loading candidates…`;
  loadMeta().then(() => { renderSuite(); renderBuildInfo(); });   // suite matrix + loaded-version line

  const shareUrl = location.origin + location.pathname;
  const link = $('shareUrl'); link.textContent = shareUrl; link.href = shareUrl;
  const qr = $('qr');
  qr.src = 'https://api.qrserver.com/v1/create-qr-code/?size=180x180&margin=8&data=' + encodeURIComponent(shareUrl);
  qr.alt = 'QR · ' + shareUrl;

  $('run').addEventListener('click', () => singleRun());
  $('loop').addEventListener('click', () => { if (continuous) stopContinuous(); else startContinuous(); });
  $('report').addEventListener('click', () => { if (lastResult) reportNow(lastResult); else $('status').textContent = 'run a benchmark first'; });
  $('copy').addEventListener('click', () => { if (lastResult) navigator.clipboard.writeText(JSON.stringify(lastResult)); });
  $('download').addEventListener('click', () => {
    if (!lastResult) return;
    const blob = new Blob([JSON.stringify(lastResult, null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `powbench-${lastResult.candidateKey}-d${lastResult.difficulty}.json`;
    a.click();
  });
});
