# TRANSIT-PICKERS — every chooser of a next hop or role holder, and every send site

Contract: Bridge-Air-Gap-Plan v0.3 §7.1.3 (+ deltas through v0.7). Kernel baseline 4.88.0 (main c3676fa), grep of 2026-09-22.
WP4 P1 (static audit) greps this table against the tree: every chooser in §A must call `isTransit()` on this branch; a chooser present in source and absent here FAILS the row; a listed chooser without the call FAILS the row. Line numbers are the baseline's and are re-pinned by the audit, not by hand.

## A. Choosers (must filter by capability)

| Site | Chooses | Filter point |
|---|---|---|
| dht/AxonaPeer.js:4025 `_greedyNextHopToward` | next hop by strict XOR progress | skip non-transit synapses |
| dht/AxonaPeer.js:4066 `_findCloserInTwoHops` | probe set + adjacent first hop | probe only transit synapses; never assign a non-transit first hop |
| dht/AxonaPeer.js ~932–1010 `route_msg` receive handler | forward hop by greedy scan over `node.synaptome` | skip non-transit |
| dht/AxonaPeer.js:5053 `sendDirect` | a direct notification (direct_<type>) | (gated at composite: opClassOf direct_* = 'forward') |
| dht/AxonaPeer.js:4518 `_addByVitality` | synaptome admission/eviction victim | introduction synapses are exempt from eviction contests and never counted toward transport degree |
| dht/AxonaPeer.js:4647 `_evictAndReplace` | never replaces an introduction synapse | returns null for an introduction dead synapse |
| dht/AxonaPeer.js:4682 `_localCandidate` | replacement candidates | never returns an introduction id |
| pubsub/AxonaManager.js:855 `pickCapableAdjacent` | D0 delegate | skip non-transit |
| pubsub/rootElection.js:46 `_emitRootBeacons` | beacon basin (beaconFanout nearest neighbours) | skip non-transit |
| pubsub/rootElection.js:130 `_onRootBeacon` | layer-forward basin (beaconFanout nearest neighbours) | skip non-transit |
| pubsub/rootElection.js:170 `_bestKnownClosest` | closest-known node gating beacon acceptance | skip every introduction id |
| dht/AxonaPeer.js:2270 `_pickRecruitPeer` | a child to recruit for delivery | skip an introduction-class child |
| pubsub/repairPlane.js:832 `_isIntroductionId` | cohort want (findKClosest consumer, ~680), heirs, nearestReachable share this predicate | exclude every introduction id; unknown ids stay eligible as role holders |
| pubsub/repairPlane.js:832 `_nearestReachable` | cohort fallback | same |
| pubsub/repairPlane.js:942 `_pickHeirs` | heir + alt on leave | same |
| pubsub/rootClaim.js:178 `meshBare` | "is any non-bridge neighbour routable" | uses `bridgeId()` list |
| pubsub/rootClaim.js:195 `selfClosestReachable` | reachable-closest root test | skip non-transit |
| pubsub/wireHandlers.js:303 `_pickChild` | delegate to a child | skip an introduction-class child |
| pubsub/wireHandlers.js:319 `_promoteChild` | promote a subscriber to child | skip an introduction-class subscriber |
| transport/web/composite.js:134 `_routeFor` | sub-transport for a send | takes opClass; 'forward' requires 'transport'; no fall-through |
| transport/web/composite.js:134 `_gate` | the egress gate (class + pinned generation) | NO_TRANSPORT_ROUTE on refusal |

## B. Routed sends (all via `_route` → `dht.routeMessage`; the choosers above apply)
AxonaManager.js:333 `_send`; :358 `_route`; :1171 SUB; :1223 PUB; :1269 UNSUB; :1293 METRICSON; :1351 KILL; :1354 TOUCH; :1375 PULL.
repairPlane.js:205 KILL retry; :206 PUB retry; :876 PUB (write flight).
syncEngine.js:127 PULLUP; :139 REPLAYUP; :177 HANDOFF; :178 REPLICATE; :218 HANDOFFACK.
rootElection.js:64, :156 ROOTBEACON.
writeFlight.js:175 RECEIPTPROBE; :198 INGESTACK; :202 RECEIPTNACK.
wireHandlers.js:341 ADOPT; :588, :611 INGESTACK; :876, :897, :933, :936 DELIVER; :1145 PULLRESP.
AxonaPeer.js:3639 `__tunneled_direct__`; :4962 `mesh:signal`.

## C. Direct sends / notifications (transport-level; `_routeFor(id, opClass)` applies)
AxonaPeer.js:815, :2152 `presence` (ntf); :1499 `peer-leaving` (ntf); :3118 `axona:direct` (ntf); :3992, :4140 `lookahead_probe` (req); :4568 `reinforce` (ntf); :4584 `triadic_introduce` (ntf); :4690 `local_probe` (req); :4805 `find_closest_set` (req); :1006, :4906 `route_msg` (req); :5063 `direct_<type>` (ntf, from sendDirect); :5308 `lateral_spread` (ntf); :5338 `lookup_step` (req).
transport/web/composite.js:254 `notify`; transport/web/index.js:948 `hello-ack` (ntf to bridge).

Operation classes for `_routeFor`: `route_msg`, `__tunneled_direct__`, `direct_*`, `axona:direct` = 'forward' (transport only); `lookahead_probe`, `local_probe`, `find_closest_set`, `lookup_step` = 'discovery' (any class may be queried; result never makes the responder a hop); `presence`, `peer-leaving`, `reinforce`, `triadic_introduce`, `lateral_spread`, `hello-ack` = 'introduction'/'maintenance' (allowed to the bridge).

## D. Frames served by a kernel node (registerFrame) — the bridge inherits all of these
`__tunneled_direct__` `find_closest_set` `hop_cache` `lateral_spread` `local_probe` `lookahead_probe` `lookup_step` `mesh:signal` `peer-leaving` `presence` `reinforce` `route_msg` `triadic_introduce`

## E. Bridge-registered handlers (bridge_axona_node.js)
onRequest: `find_closest_set` `local_probe` `lookahead_probe` `lookup_step` `ping`; onNotification: `hello` `hello-ack` `hop_cache` `lateral_spread` `reinforce` `triadic_introduce`; plus `__tunneled_direct__` routed handler (bridge_engine.js:211). server.js bare frames: `axona` `peer-list-request` `ping` `signal` `turn-refresh`.
