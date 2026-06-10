# PoW phone-WASM benchmark

The **go/no-go gate** for the Stage-4 memory-hard PoW function pick
(`axona-docs/architecture/Stage4-MemoryHard-PoW-v0.1.md`). It measures, on a real
device, for a candidate at a chosen memory parameter:

- **mint time** (p50 / p90 / p99 over trials),
- **verify time**,
- **peak WASM memory** + an **OOM** flag,
- whether the tab survives (the real phone constraint).

The decision rule: *at a memory parameter that fits the weakest supported phone,
does the candidate fit without OOM, give an acceptable foreground mint, and keep
verify at µs scale?* If neither Equihash nor Cuckoo clears it → lower difficulty,
go background-only, or fall back to Argon2id.

## Run it now (SHA-256 baseline — works today)

```bash
# from the repo root (axona-protocol/)
node bench/pow-wasm/collector.js          # serves the page (COOP/COEP) + collects results
# → open  http://localhost:8099/bench/pow-wasm/   (desktop)
# → open  http://<your-PC-LAN-IP>:8099/bench/pow-wasm/   on a phone
```

Pick the `sha256-baseline` candidate, set a difficulty (leading-zero bits), and
Run. The baseline is **not** memory-hard — it's the compute-bound / ~0-memory
reference the real candidates must beat at the phone floor.

> You can also serve with any static server (`python3 -m http.server` from the
> repo root), but then `crossOriginIsolated` is false, so the accurate
> `measureUserAgentSpecificMemory()` and WASM threads are unavailable. The
> included `collector.js` sets COOP/COEP so isolation is on.

## Add a memory-hard candidate

1. Compile the reference solver to **single-threaded WASM** (Emscripten for
   Tromp's Cuckoo C/C++; wasm-pack for a Rust Equihash). Size the memory
   parameter to the phone floor (~256–512 MB). Tune difficulty by **search
   effort, not memory.**
2. Write `candidates/<name>.js` implementing the contract in
   `candidates/template.js` (`mint` / `verify` / `peakMemoryBytes` / `reset`).
   `peakMemoryBytes()` should track `wasmMemory.buffer.byteLength` — it's the
   headline metric vs the floor.
3. Register it in the `CANDIDATES` map in `bench.js`. It appears in the dropdown.

## Collecting results across devices

Put the collector's URL (e.g. `http://<PC-LAN-IP>:8099`) in the page's
**collector URL** box; each run POSTs its result JSON to `results.jsonl` and
prints a live tally. Or just use **Copy / Download JSON** per device.

**Axona dogfood (optional):** forward each result to a topic and aggregate from
anywhere with the relay CLI —

```bash
tail -f results.jsonl | while read l; do \
  node ../../axona-relay/src/cli.js pub "axona/pow-bench" "$l"; done
node ../../axona-relay/src/cli.js sub "axona/pow-bench" --for 600   # aggregate
```

## Phased plan (see the Stage-4 record §6)

1. **Node baseline** — param→cost curve, no browser.
2. **Desktop browser** — catch WASM issues (memory growth, SIMD, threads); DevTools
   CPU-throttle for dev iteration (simulates CPU, *not* memory/thermal).
3. **Real phones** — the decision data (your phone + PC + a low-end Android), plus
   optionally a cloud device lab (BrowserStack / AWS Device Farm) for breadth.
   Non-negotiable: emulators don't reproduce mobile memory limits or thermal.
4. **Passive field data** — once a candidate is flagged in, the shipped
   `powCalibrate` + relay logging gather real cross-device numbers over time.

`results.jsonl` is gitignored.
