// smoke_pick_capable.mjs — D0's capable-delegate selector (4.62.2). The manager
// reads per-channel capability from the transport adapter (dht.isCapable, set
// from a verified CAP_ATTEST) and returns the topic-CLOSEST capable adjacent
// peer, or null → the D0 observation-only fallback. Fail-closed: an adapter with
// no isCapable (sim doubles / pre-4.62.2 transports) treats nobody as capable.
//
// Run: node test/smoke_pick_capable.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { idHex, idBig } from '../src/pubsub/ids.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

function mk(selfId, { neighbors = [], capable = new Set(), withIsCapable = true } = {}) {
  const dht = {
    verdictsSupported: true,
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: async () => ({ consumed: true }),
    neighbors: () => neighbors.map((b) => idHex(b)),
    bridgeId: () => null,
  };
  if (withIsCapable) dht.isCapable = (hex) => capable.has(String(hex).toLowerCase());
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => 1 });
  am.nodeId = selfId;
  am.setLogSink(() => {});
  return am;
}

const self  = 1n << 240n;
const topic = self ^ 0xffff0000n;
// Three neighbours at increasing XOR distance from the topic.
const near  = topic ^ 0x1n;      // closest
const mid   = topic ^ 0x100n;
const far   = topic ^ 0x10000n;

// ── 1. picks the CLOSEST capable neighbour ────────────────────────────
{
  const am = mk(self, { neighbors: [far, mid, near], capable: new Set([lcx(far), lcx(mid), lcx(near)]) });
  ok('1 all capable → returns the topic-closest', am.pickCapableAdjacent(topic) === lcx(near), am.pickCapableAdjacent(topic));
}

// ── 2. skips the closer INCAPABLE peer, picks the next capable one ─────
{
  const am = mk(self, { neighbors: [far, mid, near], capable: new Set([lcx(mid), lcx(far)]) });  // near NOT capable
  ok('2 closest is incapable → picks next-closest capable (mid)', am.pickCapableAdjacent(topic) === lcx(mid), am.pickCapableAdjacent(topic));
}

// ── 3. none capable → null (fallback) ─────────────────────────────────
{
  const am = mk(self, { neighbors: [far, mid, near], capable: new Set() });
  ok('3 no capable neighbour → null', am.pickCapableAdjacent(topic) === null);
}

// ── 4. adapter without isCapable → null, fail-closed ──────────────────
{
  const am = mk(self, { neighbors: [near], capable: new Set([lcx(near)]), withIsCapable: false });
  ok('4 adapter lacks isCapable → null (fail-closed)', am.pickCapableAdjacent(topic) === null);
}

// ── 5. self is never selected even if listed + "capable" ──────────────
{
  const am = mk(self, { neighbors: [self, near], capable: new Set([lcx(self), lcx(near)]) });
  ok('5 self is excluded → picks the real neighbour', am.pickCapableAdjacent(topic) === lcx(near), am.pickCapableAdjacent(topic));
}

// ── 6. no neighbours → null ───────────────────────────────────────────
{
  const am = mk(self, { neighbors: [], capable: new Set() });
  ok('6 no neighbours → null', am.pickCapableAdjacent(topic) === null);
}

function lcx(big) { return idHex(big).toLowerCase(); }

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
