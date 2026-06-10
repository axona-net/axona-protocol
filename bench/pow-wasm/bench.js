// Bench orchestrator (main thread): device info → spawn worker → collect trials
// → aggregate → render → export. See README.md.
//
// Register memory-hard candidates here as they compile (drop the file in
// candidates/ implementing the contract in candidates/template.js):
const CANDIDATES = {
  'sha256-baseline': './candidates/sha256-baseline.js',
  // 'equihash':       './candidates/equihash.js',
  // 'cuckoo':         './candidates/cuckoo.js',
};

const $ = (id) => document.getElementById(id);

function percentile(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

// Stable per-device id (persisted in localStorage) so testers are distinguishable
// beyond the UA string — survives reloads; one id per browser profile.
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

function deviceInfo() {
  return {
    deviceId: deviceId(),                              // stable per-device stamp
    deviceLabel: deviceLabel(),                        // optional human name
    ua: navigator.userAgent,
    deviceMemoryGB: navigator.deviceMemory ?? null,    // coarse (Chrome): 0.25..8
    cores: navigator.hardwareConcurrency ?? null,
    screen: `${screen.width}x${screen.height}@${window.devicePixelRatio}`,
    crossOriginIsolated: self.crossOriginIsolated === true,
    ts: new Date().toISOString(),
  };
}

async function uaMemoryBytes() {
  // Accurate, cross-agent — but requires cross-origin isolation (COOP/COEP).
  try {
    if (self.crossOriginIsolated && performance.measureUserAgentSpecificMemory) {
      const m = await performance.measureUserAgentSpecificMemory();
      return m.bytes;
    }
  } catch { /* not available */ }
  return null;
}

let lastResult = null;

// Run one full benchmark (load candidate → trials in a Worker → aggregate →
// render → return the result). Pure; orchestration (single vs continuous,
// reporting) is handled by the callers below.
async function runOnce() {
  const candidateKey = $('candidate').value;
  const difficulty = parseInt($('difficulty').value, 10);
  const trials = parseInt($('trials').value, 10);
  const pubkeyHex = $('pubkey').value.trim() || 'aa'.repeat(32);

  $('status').textContent = 'loading candidate…';
  const trialsData = [];
  let oom = false, error = null, candidateName = candidateKey;

  const memBefore = await uaMemoryBytes();
  const worker = new Worker('./worker.js', { type: 'module' });

  await new Promise((resolve) => {
    worker.onmessage = (e) => {
      const m = e.data;
      if (m.type === 'loaded') {
        candidateName = m.name;
        $('status').textContent = `running ${trials} trials @ difficulty ${difficulty}…`;
        worker.postMessage({ type: 'run', pubkeyHex, difficulty, trials });
      } else if (m.type === 'trial') {
        trialsData.push(m);
        $('status').textContent =
          `trial ${m.i + 1}/${trials} · mint ${m.mintMs.toFixed(0)}ms · ` +
          `mem ${(m.peakMemBytes / 1e6).toFixed(0)}MB`;
      } else if (m.type === 'done') {
        resolve();
      } else if (m.type === 'error') {
        error = m.message; resolve();
      }
    };
    // A worker that dies mid-run (the classic OOM signature on a phone).
    worker.onerror = (ev) => { oom = true; error = ev.message || 'worker died (likely OOM)'; resolve(); };
    worker.postMessage({ type: 'load', candidateUrl: CANDIDATES[candidateKey] });
  });
  worker.terminate();

  const memAfter = await uaMemoryBytes();
  const mint = trialsData.map((t) => t.mintMs);
  const result = {
    device: deviceInfo(),
    candidate: candidateName,
    difficulty,
    trials: trialsData.length,
    mint_ms: {
      p50: percentile(mint, 50), p90: percentile(mint, 90), p99: percentile(mint, 99),
      min: mint.length ? Math.min(...mint) : null, max: mint.length ? Math.max(...mint) : null,
    },
    verify_ms_avg: trialsData.length
      ? trialsData.reduce((a, t) => a + t.verifyMs, 0) / trialsData.length : null,
    peak_wasm_mem_mb: Math.max(0, ...trialsData.map((t) => t.peakMemBytes)) / 1e6,
    ua_mem_delta_mb: (memBefore != null && memAfter != null) ? (memAfter - memBefore) / 1e6 : null,
    witness_len: trialsData.length ? trialsData[trialsData.length - 1].witnessLen : null,
    all_verified: trialsData.length > 0 && trialsData.every((t) => t.ok),
    oom, error,
  };
  lastResult = result;
  render(result);
  if (error) $('status').textContent = `done — ERROR: ${error}`;
  return result;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Single benchmark (the "Run" button): run once, then one-shot report if enabled.
async function singleRun() {
  if (continuous) return;
  $('run').disabled = true;
  try {
    const r = await runOnce();
    maybeSubmit(r);
    if ($('autoReport').checked) await reportNow(r);
    else $('status').textContent = 'done';
  } catch (e) {
    $('status').textContent = 'error: ' + (e.message || e);
  } finally {
    $('run').disabled = false;
  }
}

// Continuous mode (user-initiated via "Run continuously"): loop run → publish →
// wait, until Stop. With auto-publish on, connect to Axona ONCE and reuse it.
let continuous = false, reporter = null, iter = 0;

async function startContinuous() {
  if (continuous) return;
  continuous = true; iter = 0;
  $('loop').textContent = 'Stop'; $('run').disabled = true;
  const gap = Math.max(0, parseInt($('gap').value, 10) || 0);

  if ($('autoReport').checked) {
    try {
      const { createReporter } = await import('./axona-report.js');
      reporter = await createReporter((m) => { $('status').textContent = m; });
    } catch (e) {
      $('status').textContent = 'Axona connect failed — continuing local-only: ' + (e.message || e);
      reporter = null;
    }
  }

  while (continuous) {
    iter++; $('iter').textContent = String(iter);
    let r;
    try { r = await runOnce(); }
    catch (e) { $('status').textContent = 'run error: ' + (e.message || e); break; }
    maybeSubmit(r);
    if (reporter) {
      try { const { msgId } = await reporter.publish(r); $('status').textContent = `iter ${iter} · published ✓ (${String(msgId).slice(0, 10)}…)`; }
      catch (e) { $('status').textContent = `iter ${iter} · publish failed: ${e.message || e}`; }
    } else {
      $('status').textContent = `iter ${iter} · done (local)`;
    }
    if (!continuous) break;
    await sleep(gap);
  }
  await stopContinuous();
}

async function stopContinuous() {
  continuous = false;
  $('loop').textContent = 'Run continuously';
  $('run').disabled = false;
  if (reporter) { try { await reporter.close(); } catch { /* */ } reporter = null; }
}

// Relay the result back over the live Axona network (pub/sub) so a local node
// collects it — no HTTP collector needed, works across the internet. Lazy: the
// heavy kernel + WebRTC connect only loads when reporting is actually used.
async function reportNow(result) {
  $('report').disabled = true;
  try {
    const { reportToAxona } = await import('./axona-report.js');
    await reportToAxona(result, (m) => { $('status').textContent = m; });
  } catch (e) {
    $('status').textContent = 'Axona report failed: ' + (e.message || e);
  } finally {
    $('report').disabled = false;
  }
}

function render(r) {
  const rows = [
    ['candidate', r.candidate],
    ['difficulty', r.difficulty],
    ['trials', r.trials],
    ['mint p50 / p90 / p99 (ms)', r.mint_ms.p50 != null ? `${r.mint_ms.p50.toFixed(0)} / ${r.mint_ms.p90.toFixed(0)} / ${r.mint_ms.p99.toFixed(0)}` : '—'],
    ['mint min / max (ms)', r.mint_ms.min != null ? `${r.mint_ms.min.toFixed(0)} / ${r.mint_ms.max.toFixed(0)}` : '—'],
    ['verify avg (ms)', r.verify_ms_avg != null ? r.verify_ms_avg.toFixed(3) : '—'],
    ['peak WASM mem (MB)', r.peak_wasm_mem_mb.toFixed(1)],
    ['UA mem delta (MB)', r.ua_mem_delta_mb != null ? r.ua_mem_delta_mb.toFixed(1) : 'n/a (need cross-origin isolation)'],
    ['witness length (chars)', r.witness_len ?? '—'],
    ['all verified', r.all_verified ? 'yes' : 'NO'],
    ['OOM / error', r.oom ? 'OOM' : (r.error ? r.error : 'none')],
    ['device', `${r.device.cores ?? '?'} cores · ${r.device.deviceMemoryGB ?? '?'}GB · COI=${r.device.crossOriginIsolated}`],
  ];
  $('results').innerHTML = rows.map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`).join('');
  $('json').value = JSON.stringify(r, null, 2);
}

function maybeSubmit(result) {
  const url = $('collector').value.trim();
  if (!url) return;
  fetch(url.replace(/\/$/, '') + '/submit', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify(result),
  }).then(() => { $('status').textContent += ' · submitted'; })
    .catch((e) => { $('status').textContent += ` · submit failed: ${e.message}`; });
}

// ── wiring ──────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const sel = $('candidate');
  for (const k of Object.keys(CANDIDATES)) {
    const o = document.createElement('option'); o.value = k; o.textContent = k; sel.appendChild(o);
  }
  // Optional device label, persisted so it sticks across runs/reloads.
  const lab = $('label');
  lab.value = deviceLabel();
  lab.addEventListener('input', () => {
    try { localStorage.setItem('powbench-device-label', lab.value.trim()); } catch { /* */ }
    $('dev').textContent = JSON.stringify(deviceInfo(), null, 2);
  });

  $('dev').textContent = JSON.stringify(deviceInfo(), null, 2);

  // QR + share link so a phone can join by scanning. The QR image is the only
  // external call in the app (a public QR service) and only renders the PUBLIC
  // page URL; the link below is the always-works fallback.
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
    a.download = `powbench-${lastResult.candidate.split(' ')[0]}-d${lastResult.difficulty}.json`;
    a.click();
  });
});
