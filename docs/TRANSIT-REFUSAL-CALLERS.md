# TRANSIT-REFUSAL-CALLERS — every routed-send caller and what it does with a refused verdict

Contract: Bridge-Air-Gap-Plan v0.3 §7.2.3 as restated in v0.5 §7.2.3. Kernel baseline 4.88.0 (main c3676fa), grep of 2026-09-22.
WP4 O4 observes, per row, `NO_TRANSPORT_ROUTE` (or the row's existing failure) and no second attempt on the same edge inside the row's cadence.

Rule (plan v0.3 §7.2.3): a refused verdict `{consumed:false, terminal:true, refused:true}` ends the walk; the operation reports failure to its own caller and retries only on its own cadence, never immediately on the same edge.

Source facts (2026-09-22 grep): every `_send`/`_route` caller below is fire-and-forget — none awaits or inspects the verdict; `_route` tallies `{consumed:false}` in `_routeStats` (AxonaManager.js:376) and returns the verdict. The one caller that awaits verdicts is `_replicateRole` (repairPlane.js ~762–790, via `_syncPush` returning the dispatch promise) for the durability ledger; it records the verdict per target and retries on the next repair tick.

| Caller (file:line) | Verb | Awaits verdict? | Retry cadence today | Under refusal |
|---|---|---|---|---|
| AxonaManager.js:1171 pubsubSubscribe | SUB | no | renewal interval (RENEW_FAST_MS 5 s → RENEW_MS 60 s backoff, repairPlane ~140) | next renewal; caller sees no attach (`_upstream` empty) |
| AxonaManager.js:1223 pubsubPublish | PUB | no | pending-pub ledger retried per tick up to PENDING_PUB_MAX_TRIES (repairPlane ~200) | next tick; confirm stays false |
| AxonaManager.js:1269 unsubscribe | UNSUB | no | none (renewal lapse also drops) | dropped; lapse handles it |
| AxonaManager.js:1293 metricsOn | METRICSON | no | METRICS_PUB_MS | next cadence |
| AxonaManager.js:1351 kill | KILL | no | pending-kill ledger per tick | next tick |
| AxonaManager.js:1375 pull | PULL | no | caller's own retry (corrId timeout) | timeout to caller |
| syncEngine.js:127 _syncPull | PULLUP | no | repair tick | next tick |
| syncEngine.js:139 replayUp | REPLAYUP | no | on next PULLUP | next tick |
| syncEngine.js:178 _syncPush | REPLICATE / HANDOFF | promise returned; awaited by _replicateRole | repair tick (keepalive 5 s; full 60 s) | ledger marks target failed; next tick |
| syncEngine.js:218 handoff ack | HANDOFFACK | no | none | dropped |
| rootElection.js:64,156 beacons | ROOTBEACON | no | BEACON_MS 20 s | next beacon |
| writeFlight.js:175,198,202 | RECEIPTPROBE / INGESTACK / RECEIPTNACK | no | flight timeout | flight fails to caller |
| wireHandlers.js:341 _delegateTo | ADOPT | no | next _accept over capacity | next delegation attempt |
| wireHandlers.js:588,611 acks | INGESTACK | no | none | dropped |
| wireHandlers.js:876,897,933,936 fan-out | DELIVER | no | next publish / replay | subscriber misses; renewal re-homes |
| wireHandlers.js:1145 | PULLRESP | no | none | requester times out |
| AxonaPeer.js:3639 sendDirect fallback | __tunneled_direct__ | .catch only | caller's own | caller's own |
| AxonaPeer.js:4962 relay signal sink | mesh:signal | awaited (r) | transport falls back to the bridge `signal` path | falls back to bare `signal` via the bridge (allowed row) |

WP4 O4 obligation: for each row, a harness case observes NO_TRANSPORT_ROUTE (or the row's existing failure) and no second attempt on the same edge inside the row's cadence.
