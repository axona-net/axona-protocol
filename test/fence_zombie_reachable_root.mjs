// fence_zombie_reachable_root.mjs — a DEAD root that lingers in neighbors()
// defeats every corpse-freshness cut. Fact-finding instrument for the
// 2026-08-07 prod specimen (GH #28): an ungracefully-killed root left the
// owned `axona.bot` topic write-dead 50+ minutes while reads worked, and the
// eagle fleet showed `peer-departed-hint-ignored-live` on 13/20 relays.
//
// THE PROPERTY UNDER TEST. rootClaim.liveCloserRoot orders its evidence:
//
//   if (b.verified && fresh)        return b.root;   ← 1.5×BEACON_MS cut (4.59.0)
//   if (m._isReachableId(b.root))   return b.root;   ← NO freshness cut AT ALL
//   if (!requireReachable && fresh) return b.root;   ← 1.5×BEACON_MS cut
//
// and _isReachableId(hex) is nothing but "hex ∈ dht.neighbors()". A SIGKILLed
// process closes no WebRTC channels; until the transport evicts the corpse
// from neighbors(), the middle clause keeps naming it — past the freshness
// cut, for as long as the record lives (TTL 50 s plain / 90 s verified) — on
// the STRICT (SUB) and LOOSE (PUB/KILL) paths alike, and _topicDecision keeps
// routing writes toward it. The same neighbors() answer is why the #364-B
// departure hints get IGNORED ("still looks live"): every guard consults the
// same lying list. NOTE the measured bound: one record cannot outlive 90 s,
// so the 50-minute prod specimen additionally requires active regeneration —
// the open question this fence does NOT close.
//
// SECTIONS 1–3 PIN THE DEFECT AS IT STANDS (they are GREEN on 4.61.2 and
// document the mechanism). When Dead-Root Eviction (Dead-Root-Eviction-v0.2,
// council-ratified) ships, sections 1 and 3 FLIP to the fixed contract — a
// deliberate flip, not a regression; see the E5 task. Section 2 is the
// control (clean departure heals) and must be green forever.
//
// Run: node test/fence_zombie_reachable_root.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { BEACON_MS, T } from '../src/pubsub/constants.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG   = 0x89n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc    = (s) => String(s).toLowerCase();

// Distances to TOPIC, closest first: DEAD < SELF < NB (the corpse is the terminus).
const TOPIC = REG | 0x1000n;
const DEAD  = REG | 0x1001n;   // the ungracefully-killed root
const SELF  = REG | 0x1010n;
const NB    = REG | 0x8000n;   // live neighbour: not alone-in-the-dark

function mk({ neighbors }) {
  const clock = { t: 1_000_000 };
  const sends = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      sends.push({ type, via: [...(payload.via || [])] });
      // A via toward the corpse never reaches it; something mid-route consumes.
      return { consumed: true, hops: 3, atNode: lc(idHex(NB)) };
    },
    neighbors,
    bridgeId: () => null,
    findKClosest: async () => [],
    lookup: async () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, clock, sends };
}

// Install the beacon through the production receiver, then age it INTO the
// zombie window: past the 1.5×BEACON_MS freshness cut (30 s) but inside the
// record TTL (BEACON_TTL_MS = 50 s). In that window only the reachable clause
// can keep the corpse alive — which is exactly the clause a zombie neighbour
// satisfies. MEASURED BOUND (this fence's first run): past the TTL the record
// itself dies and the corpse loses on every clause, zombie or not — so a
// single record explains at most ~50 s (90 s verified) of strand, and the
// 50-minute prod specimen requires ACTIVE regeneration of state. That is the
// open fact-finding question; the fleet's raw logs are the evidence.
function agedCorpseBeacon(am, clock) {
  am._onRootBeacon(
    { root: lc(idHex(DEAD)), topics: [idHex(TOPIC)], beaconId: 'b-corpse-0', layer: 1 },
    { fromId: idHex(NB) },
  );
  clock.t += Math.floor(BEACON_MS * 2);   // 40 s: > 30 s freshness, < 50 s TTL
}

// ── 1. Zombie neighbour: aged beacon still wins on BOTH gates ───────────────
{
  const { am, clock } = mk({ neighbors: () => [NB, DEAD] });
  agedCorpseBeacon(am, clock);
  const strict = am._rootClaim.liveCloserRoot(TOPIC);
  const loose  = am._rootClaim.liveCloserRoot(TOPIC, { requireReachable: false });
  ok('1a CURRENT DEFECT (flips with eviction fix): in the zombie window the corpse wins the STRICT gate on reachability alone',
    strict === lc(idHex(DEAD)), `got ${strict}`);
  ok('1b CURRENT DEFECT (flips with eviction fix): same corpse wins the LOOSE gate',
    loose === lc(idHex(DEAD)), `got ${loose}`);
}

// ── 2. CONTROL — clean departure: corpse out of neighbors(), cuts work ──────
{
  const { am, clock } = mk({ neighbors: () => [NB] });   // DEAD evicted from the mesh
  agedCorpseBeacon(am, clock);
  const strict = am._rootClaim.liveCloserRoot(TOPIC);
  const loose  = am._rootClaim.liveCloserRoot(TOPIC, { requireReachable: false });
  ok('2a control (green forever): same age WITHOUT the zombie entry loses the strict gate', strict === null, `got ${strict}`);
  ok('2b control (green forever): and loses the loose gate — the 4.59.0 freshness cut works', loose === null, `got ${loose}`);
}

// ── 3. The strand at one node: the zombie steers ROUTING, not just the gate ──
// With the corpse in neighbors() it is also the closest reachable node to the
// topic, so _topicDecision keeps the PUB in normal routing ("forward") — the
// write leaves live custody toward a node that will never ingest it — and
// self, the closest LIVE node, declines to root. Both halves of the strand.
{
  const { am, clock } = mk({ neighbors: () => [NB, DEAD] });
  agedCorpseBeacon(am, clock);
  const payload = { topicId: idHex(TOPIC), json: JSON.stringify({ v: 3 }) };
  const d = am._topicDecision(payload, { fromId: idHex(NB) });
  await am._onPub(payload, { fromId: idHex(NB) });
  ok('3a CURRENT DEFECT (flips with eviction fix): routing keeps forwarding the PUB toward the corpse',
    d === 'forward', `decision=${d}`);
  ok('3b CURRENT DEFECT (flips with eviction fix): the closest LIVE node never roots — no role minted, the write is gone',
    !am.axonRoles.get(TOPIC));
}

console.log(fail === 0 ? `\nfence_zombie_reachable_root: ${n}/${n} ok` : `\nfence_zombie_reachable_root: ${fail} FAILED of ${n}`);
process.exit(fail === 0 ? 0 : 1);
