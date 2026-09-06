// fence_cold_subscribe_reach.mjs — cold-subscribe read loss (GH #418/#397).
//
// A fresh subscriber's SUB routed GREEDY toward the bare topic id (via:[]) strands
// at a LOCAL MINIMUM on a cold/sparse synaptome — it never reaches the topic-closest
// root, so it is never seated and replays NOTHING. Established subscribers deliver
// 100%; fresh since:'all' subscribers were measured ~25-55%. The write path (PUB/
// KILL) does not show this because it retries via PENDING_PUB; reads had no
// equivalent. The v4.64.0 change made _sendSubscribe ignore the root hint on the
// unpinned path, so even the background-warmed true-root hint never steered the SUB.
//
// THE FIX (src/pubsub/AxonaManager.js _sendSubscribe): for the UNPINNED case,
//   (a) use the warm root hint (_rootHint_) if present — restored, and re-applied on
//       every renewFastMs renewal (that IS the bounded read-retry), and
//   (b) if no warm hint yet, do a BOUNDED iterative network lookup for the first SUB
//       (raced against SUB_LOOKUP_MS, greedy fallback), so the cold first attempt
//       reaches the true root instead of stranding.
//
// TOPOLOGY (deterministic, no search). Distances are XOR to the topic id T:
//   R = T^1        → globally closest = the TRUE ROOT (holds the message)
//   L = T^0xF00    → a decoy, closer to T than S, whose ONLY neighbour is S
//   S = T^0xFF000  → the fresh subscriber, farther from T than L
// Adjacency is ONLY  S—L.  R is NOT adjacent to anyone, so:
//   * greedy(S → T)  steps S→L (L closer to T), then dead-ends at L  → STRAND, L≠R.
//   * a SUB addressed to the resolved root NODE R reaches R (an exact, resolved
//     address is delivered last-mile; a bare virtual topic key is what strands).
// The mock's lookup()/findKClosest() return the GLOBAL closest (R) — the origin-
// independent oracle that escapes the greedy local minimum. The RED control removes
// lookup/findKClosest (and any beacon), forcing the pre-fix greedy path.
//
// Run: node test/fence_cold_subscribe_reach.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc = (s) => String(s).toLowerCase();
const flush = async (rounds = 24) => { for (let i = 0; i < rounds; i++) await new Promise((r) => setImmediate(r)); };

// A gappy mock network of real AxonaManager instances. `resolve` toggles whether the
// DHT can iteratively resolve the true root (fix available) or not (RED control).
class Net {
  constructor({ resolve }) { this.nodes = new Map(); this.adj = new Map(); this.clock = { t: 1_700_000_000_000 }; this.resolve = resolve; }
  link(a, b) {
    (this.adj.get(a) ?? this.adj.set(a, new Set()).get(a)).add(b);
    (this.adj.get(b) ?? this.adj.set(b, new Set()).get(b)).add(a);
  }
  _globalClosest(target) {
    let best = null, bd = null;
    for (const id of this.nodes.keys()) { const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } }
    return best;
  }
  // Greedy walk from `start` toward `target` over adjacency; stop at a local minimum.
  _greedyTerminus(start, target) {
    let cur = start, guard = 0;
    while (guard++ < 64) {
      let next = cur, bd = cur ^ target;
      for (const nb of (this.adj.get(cur) || [])) { if (!this.nodes.has(nb)) continue; const d = nb ^ target; if (d < bd) { bd = d; next = nb; } }
      if (next === cur) return cur;
      cur = next;
    }
    return cur;
  }
  add(idBig) {
    const self = this; const handlers = new Map();
    const dht = {
      verdictsSupported: true,
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      neighbors: () => [...(self.adj.get(idBig) || [])].map(idHex),
      bridgeId: () => null,
      // An EXACT live node id is a resolved address → delivered last-mile. A bare
      // VIRTUAL key (the topic id, no node sitting on it) → greedy hop-by-hop, which
      // STRANDS at a local minimum on this sparse graph. That contrast is the bug and
      // the fix in one line: a SUB toward the topic id strands; a SUB toward the
      // resolved root's node id reaches it.
      routeMessage: async (target, type, payload, meta = {}) => {
        let t; try { t = typeof target === 'bigint' ? target : BigInt('0x' + String(target)); } catch { t = null; }
        if (t === null) return { consumed: false, exhausted: true };
        const dest = self.nodes.has(t) ? t : self._greedyTerminus(idBig, t);
        if (dest === null) return { consumed: false, exhausted: true };
        const h = self.nodes.get(dest)?.handlers.get(type);
        if (!h) return { consumed: false, terminal: true };
        const r = await h(payload, { targetId: t, isTerminal: true, hopCount: 1, fromId: meta.fromId ?? idHex(idBig) });
        return r === 'consumed' ? { consumed: true, atNode: idHex(dest), hops: 1 } : { consumed: false, terminal: true };
      },
      // The iterative resolver — present only when `resolve` (fix path); returns the
      // GLOBAL closest, escaping the greedy local minimum. Absent in the RED control.
      ...(self.resolve ? {
        lookup: async (target) => { const c = self._globalClosest(target); return { path: c === null ? [] : [idHex(c)] }; },
        findKClosest: async (target, _k = 1) => { const c = self._globalClosest(target); return c === null ? [] : [idHex(c)]; },
      } : {}),
    };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => self.clock.t, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    am.nodeId = idBig; am.setLogSink(() => {});
    const rec = { id: idBig, am, handlers, got: [] };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
}

// Build one scenario: R roots a topic and holds one published message; then a FRESH
// subscriber S cold-subscribes since:'all'. Returns what S received + role state.
async function scenario({ resolve, author, desc, T }) {
  const R_ID = T ^ 0x1n;        // globally closest → true root
  const L_ID = T ^ 0xF00n;      // decoy / local-minimum dead-end
  const S_ID = T ^ 0xFF000n;    // fresh subscriber, farther from T than L

  const net = new Net({ resolve });
  const R = net.add(R_ID), L = net.add(L_ID), S = net.add(S_ID);
  net.link(S_ID, L_ID);         // the ONLY edge — R is reachable only as an exact node
  const sanity = {
    trueRoot: net._globalClosest(T) === R_ID,
    strandsAtL: net._greedyTerminus(S_ID, T) === L_ID,
  };

  // R roots the topic and holds one message published BEFORE anyone else subscribes.
  R.am.pubsubSubscribe(T); R.am._becomeRoot(T);
  await flush();
  const env = await buildEnvelope({ topic: desc, message: { hi: 'cold-replay' }, seq: 1, identity: author, ts: net.clock.t });
  R.am.pubsubPublish(T, JSON.stringify(env));
  await flush();
  const rootRole = R.am.axonRoles.get(T);
  const rootHolds = !!rootRole?.isRoot && (rootRole?.cache?.length ?? 0) > 0;

  // FRESH cold subscribe, since:'all' (reset the consumption floor to 0 so the root
  // replays the full history it already holds).
  net.clock.t += 1_000;
  S.am.pubsubResetTopicConsumption(T);
  S.am.pubsubSubscribe(T);
  await flush();

  const sHex = lc(idHex(S_ID));
  return {
    sanity, rootHolds, msgId: env.msgId,
    seatedAtRoot: !!rootRole?.subscribers?.has(sHex),
    delivered: S.got.includes(env.msgId),
    strandedAtL: !!L.am.axonRoles.get(T)?.subscribers?.has(sHex),
  };
}

async function main() {
  console.log('Axona pub/sub — cold-subscribe reach (GH #418/#397): fresh SUB must reach the true root\n');
  const author = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'cold-subscribe-reach', write: 'open' };
  const T = await deriveTopicIdBig(desc);

  // ── GREEN (fix available: DHT can resolve the true root) ──────────────
  const green = await scenario({ resolve: true, author, desc, T });
  check('sanity: R is the topic-closest node (true root)', green.sanity.trueRoot);
  check('sanity: greedy from S strands at the decoy L, not the root R', green.sanity.strandsAtL);
  check('(A) the true root holds the published message', green.rootHolds);
  check('(B) WITH the fix, the fresh SUB reaches the true root (R seats S)', green.seatedAtRoot);
  check('(B) WITH the fix, the held message is replayed/delivered to the fresh subscriber', green.delivered,
    `(delivered=${green.delivered})`);

  // ── RED control (no iterative resolver → forced greedy = pre-fix path) ─
  const red = await scenario({ resolve: false, author, desc, T });
  check('(C) RED: the true root still holds the message', red.rootHolds);
  check('(C) RED: forced greedy — the fresh SUB does NOT reach the root', !red.seatedAtRoot);
  check('(C) RED: the fresh subscriber receives NOTHING (the bug, fence bites)', !red.delivered,
    `(delivered=${red.delivered})`);
  check('(C) RED: the fresh SUB stranded at the decoy L (local minimum)', red.strandedAtL,
    `(strandedAtL=${red.strandedAtL})`);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('fence threw:', e?.stack || e); process.exit(2); });
