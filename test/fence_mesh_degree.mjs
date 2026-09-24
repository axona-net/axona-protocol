// fence_mesh_degree.mjs — the WebRTC mesh holds a bounded degree on a bridge,
// and is unchanged everywhere else.
//
// WHY (measured, 2026-09-24). The bridge's degree cap governs ONE side of the
// node. `nsenter … ss -tuna` inside each production bridge container:
//     east   17 inbound WebSocket (cap 15 + slack 2), 1 outbound uplink, 1 UDP
//     west    1 inbound WebSocket,                    1 outbound uplink, 7 UDP
// and each bridge's synaptome equalled the column it was rich in — east 17, west
// 7. So west was a full WebRTC mesh participant wearing bridge clothes, and
// BRIDGE_MAX_PEERS could not see it. The mesh manager's own header says it
// keeps a channel "to every other peer in the mesh", which is exactly what it
// did. David 2026-09-24: cap the WebRTC connections too, symmetric with inbound.
//
// THE DESIGN, and what is deliberately NOT done. The cap is OFF by default —
// `degree.maxPeers` absent or 0 leaves every existing caller (browser, relay,
// test) behaving precisely as before, because only a bridge has a reason to be
// a mediocre node on purpose. When configured, the manager GRADUATES rather
// than refuses, the same shape as the WebSocket side: accept, then release the
// most expendable open channel, under hysteresis, one per interval.
//
// THE ONE THING THAT IS NOT SYMMETRIC, and it is the load-bearing part: the
// WebSocket side graduates with close code 4200, which the remote kernel reads
// as "you are meshed, do not reconnect". A DataChannel close says nothing. So
// the cooldown is enforced on OUR OWN DOOR — a retired peer is refused by
// onPeerList and onPeerJoined for the cooldown — or the bridge retires and
// re-accepts the same peer for ever. That is the create-and-destroy cycle
// measured at 2.8/s on the west bridge, in a different costume.
//
// Run: node test/fence_mesh_degree.mjs
import { MeshManager } from '../src/transport/web/mesh.js';
import { selectMeshRetire } from '../src/transport/web/mesh_degree.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};

const T = 1_800_000_000_000;

/** A manager whose teardown is observed instead of performed (no real WebRTC). */
function mk(degree = null) {
  const retired = [];
  const m = new MeshManager({ sendSignal: () => {}, log: () => {}, degree });
  m._myId = 'self';
  m._retire = (id, why) => { retired.push({ id, why }); m._peers.delete(id); };
  m.getLatency = () => null;
  return { m, retired };
}
/** Seat an OPEN channel, the shape _wireDataChannel leaves behind. */
function open(m, peerId, { openedAt = Date.now() - 60_000 } = {}) {
  const st = m._newPeerState(peerId, 'offerer');
  st.state = 'open';
  st.openedAt = openedAt;
  m._peers.set(peerId, st);
  return st;
}
const region = (map) => (peerId) => map[peerId] ?? null;

console.log('bounded mesh degree — a bridge is mediocre on BOTH sides\n');

// ── 1. OFF BY DEFAULT: every existing caller is untouched ────────────────
{
  const { m, retired } = mk();                       // no degree option at all
  for (let i = 0; i < 40; i++) open(m, `p${i}`);
  m._enforceDegree();
  ok('1a. with no cap configured, 40 open channels are all kept', m._peers.size === 40);
  ok('1b. …and nothing was retired', retired.length === 0);
  ok('1c. degreeStats reports cap 0, which is how "unbounded" reads', m.degreeStats().cap === 0);
  const s = m.degreeStats();
  ok('1d. …and the counters are zero, not absent', s.retired === 0 && s.refused === 0);
}

// ── 2. HYSTERESIS: the cap alone does not fire; cap+slack does ──────────
{
  const regions = {};
  const { m, retired } = mk({ maxPeers: 15, regionOf: region(regions) });
  for (let i = 0; i < 17; i++) { regions[`p${i}`] = '89'; open(m, `p${i}`); }
  m._enforceDegree();
  ok('2a. at exactly cap+slack (17 of 15+2) nothing is retired', retired.length === 0);
  regions.p17 = '89'; open(m, 'p17');
  m._enforceDegree();
  ok('2b. one over the band retires exactly one', retired.length === 1, JSON.stringify(retired));
  ok('2c. …with the reason degree-cap', retired[0]?.why === 'degree-cap');
}

// ── 3. ONE PER INTERVAL: a burst does not thin the mesh at once ─────────
{
  const regions = {};
  const { m, retired } = mk({ maxPeers: 5, regionOf: region(regions) });
  for (let i = 0; i < 20; i++) { regions[`p${i}`] = i % 2 ? '89' : '80'; open(m, `p${i}`); }
  for (let i = 0; i < 6; i++) m._enforceDegree();    // six calls, no clock advance
  ok('3. six enforcement passes inside one interval retire ONE channel', retired.length === 1,
    `retired=${retired.length}`);
}

// ── 4. THE DOOR: a retired peer is refused while in cooldown ────────────
{
  const { m } = mk({ maxPeers: 15, cooldownMs: 60_000, regionOf: () => '89' });
  const initiated = [], accepted = [];
  m._initiateTo = (id) => initiated.push(id);
  m._acceptFrom = (id) => accepted.push(id);
  m._retiredRecently.set('gone', Date.now());
  m.onPeerList(['gone', 'fresh']);
  m.onPeerJoined('gone');
  m.onPeerJoined('other');
  ok('4a. onPeerList does NOT re-initiate to a peer in cooldown', !initiated.includes('gone'));
  ok('4b. …but still initiates to everyone else', initiated.includes('fresh'));
  ok('4c. onPeerJoined does NOT accept a peer in cooldown', !accepted.includes('gone'));
  ok('4d. …but still accepts everyone else', accepted.includes('other'));
  ok('4e. the refusals are counted', m.degreeStats().refused === 2, String(m.degreeStats().refused));
}
{
  const { m } = mk({ maxPeers: 15, cooldownMs: 1, regionOf: () => '89' });
  const initiated = [];
  m._initiateTo = (id) => initiated.push(id);
  m._retiredRecently.set('gone', Date.now() - 60_000);
  m.onPeerList(['gone']);
  ok('4f. once the cooldown lapses the peer is welcome again', initiated.includes('gone'));
  ok('4g. …and the stale cooldown entry is dropped', !m._retiredRecently.has('gone'));
}

// ── 5. WHAT IS NEVER CHOSEN ─────────────────────────────────────────────
{
  const regions = { keep: '80' };
  for (let i = 0; i < 20; i++) regions[`p${i}`] = '89';
  const { m, retired } = mk({
    maxPeers: 5, regionOf: region(regions),
    isProtected: (id) => id === 'guarded',
  });
  for (let i = 0; i < 20; i++) open(m, `p${i}`);
  open(m, 'keep');                                   // sole member of region 80
  regions.guarded = '89'; open(m, 'guarded', { openedAt: 0 + 1 });   // oldest by far
  m._enforceDegree();
  ok('5a. the sole representative of a region is never retired', retired[0]?.id !== 'keep');
  ok('5b. a PROTECTED peer is never retired, even as the oldest channel',
    retired[0]?.id !== 'guarded', JSON.stringify(retired));
  ok('5c. something in the over-represented region was chosen', retired[0]?.id?.startsWith('p'));
}
{
  const { m, retired } = mk({ maxPeers: 1, regionOf: () => '89', minUptimeMs: 30_000 });
  for (let i = 0; i < 6; i++) open(m, `young${i}`, { openedAt: Date.now() - 1_000 });
  m._enforceDegree();
  ok('5d. a channel younger than minUptime is not retired — it just formed for a reason',
    retired.length === 0);
}
{
  const { m, retired } = mk({ maxPeers: 1, regionOf: () => null });   // nobody authenticated
  for (let i = 0; i < 6; i++) open(m, `anon${i}`);
  m._enforceDegree();
  ok('5e. an unauthenticated peer has no region, so it is never retired on balance',
    retired.length === 0);
}
{
  const { m, retired } = mk({ maxPeers: 1, regionOf: () => '89' });
  for (let i = 0; i < 6; i++) {
    const st = m._newPeerState(`neg${i}`, 'offerer');   // negotiating, never opened
    m._peers.set(`neg${i}`, st);
  }
  m._enforceDegree();
  ok('5f. channels still negotiating are not counted and not retired', retired.length === 0);
}

// ── 6. THE SELECTION ITSELF (pure) ──────────────────────────────────────
{
  const cands = [
    { id: 'a', region: '89', openedAt: T - 500_000, rttMs: 10,  inCooldown: false, isProtected: false },
    { id: 'b', region: '89', openedAt: T - 100_000, rttMs: 10,  inCooldown: false, isProtected: false },
    { id: 'c', region: '80', openedAt: T - 900_000, rttMs: 900, inCooldown: false, isProtected: false },
    { id: 'd', region: '80', openedAt: T - 800_000, rttMs: 900, inCooldown: false, isProtected: false },
    { id: 'e', region: '80', openedAt: T - 700_000, rttMs: 900, inCooldown: false, isProtected: false },
  ];
  const pick = selectMeshRetire(cands, { now: T, minUptimeMs: 30_000 });
  ok('6a. the most over-represented region wins the primary axis (80 has 3, 89 has 2)',
    pick?.region === '80', JSON.stringify(pick));
  ok('6b. within it, the LONGEST-HELD channel is released', pick?.id === 'c', JSON.stringify(pick));
  ok('6c. the basis is named honestly as age, not vitality', pick?.basis === 'age');
  ok('6d. nothing eligible ⇒ null, never a throw',
    selectMeshRetire([], { now: T, minUptimeMs: 0 }) === null);
  const allProtected = cands.map((c) => ({ ...c, isProtected: true }));
  ok('6e. every candidate protected ⇒ null', selectMeshRetire(allProtected, { now: T, minUptimeMs: 0 }) === null);
}

console.log(`\nResult: ${n} passed, ${fail} failed`);
if (fail) process.exit(1);
