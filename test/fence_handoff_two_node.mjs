// fence_handoff_two_node.mjs — a departing ROOT's retry exemption must be earned.
//
// SCOPE CORRECTION (Aster, council 2026-08-01). This file previously called
// itself the "non-root handoff" fence and claimed Aster's fourth requirement. It
// does not: pair() calls leaver._becomeRoot(), so every case here drives the ROOT
// path — HANDOFF out, HANDOFFACK back. The NON-ROOT holder path is a different
// branch (repairPlane's REPLICATE leg, where _handoffAcked is set from a consumed
// dispatch promise) and nothing here touched it. Overstated coverage is worse
// than absent coverage, because it stops anyone looking.
//
// The non-root branch now has its own file: fence_handoff_nonroot.mjs. What
// follows is the ROOT path, which is real and worth pinning on its own terms.
//
// WHY THIS FILE EXISTS AT ALL. fence_q2_end_to_end asserted the same property
// from a SINGLE node and its consumed control could never fire — I measured it:
// three pubsub:handoff messages dispatched and resolved {consumed:true} while
// _handoffAcked stayed empty, because the set is populated by _onHandoffAck, a
// wire ACK sent back BY THE HEIR, and there was no heir process to send one. Its
// fail-closed halves therefore proved less than they looked like: "the set stays
// empty" is also what happens when nothing runs. A control that cannot fire is
// not a control — the same defect I found in my own kill section, where a missing
// meta.isTerminal had two checks asserting against a handler that returned early.
//
// So: two real managers, a real routing fabric between them, and a real ack.
//
// WHAT THE EXEMPTION MEANS. A departing root hands its history to an heir. The
// heir's ack is the evidence that the history survived the departure; a topic in
// _handoffAcked is exempt from further retry rounds and from the cohort spray.
// Granting that exemption without evidence is how a leaver drops the last copy
// of a topic and reports success — the #340 class.
//
// THE ACK IS HONEST (#402): it carries held vs sent, and a leaver only counts it
// when held >= sent. A heir that admits nothing must NOT earn the leaver's
// silence, which section 3 drives directly.
//
// Run: node test/fence_handoff_two_node.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const idHex = (b) => b.toString(16).padStart(66, '0');
const lc = (s) => String(s).toLowerCase();

// ── the fabric ─────────────────────────────────────────────────────────────
// Two managers, real routed delivery between them. routeMessage resolves a REAL
// verdict from the receiving handler, so the adapter honestly declares
// verdictsSupported: true — nothing here fakes a dispatch outcome.
class Net {
  constructor() { this.nodes = new Map(); this.clock = { t: 1_700_000_000_000 }; this.drop = new Set(); }

  add(idBig, { rootReplicas = 0 } = {}) {
    const self = this;
    const handlers = new Map();
    const dht = {
      verdictsSupported: true,
      getSelfId: () => idBig,
      onRoutedMessage: (type, fn) => handlers.set(type, fn),
      routeMessage: async (target, type, payload) => {
        // DROP simulates a heir that never answers — the case where the leaver
        // must NOT assume the history landed.
        if (self.drop.has(type)) return { consumed: false, exhausted: true };
        const dest = self._closest(target);
        if (dest === null) return { consumed: false, exhausted: true };
        const node = self.nodes.get(dest);
        const h = node.handlers.get(type);
        if (!h) return { consumed: false, terminal: true };
        const r = await h(payload, { targetId: dest, isTerminal: true, fromId: idHex(idBig) });
        return r === 'consumed' ? { consumed: true, atNode: idHex(dest), hops: 1 }
                                : { consumed: false, terminal: true };
      },
      findKClosest: async (t, k = 2) => self._sorted(t).filter(x => x !== idBig).slice(0, k).map(idHex),
      neighbors: () => [...self.nodes.keys()].filter(x => x !== idBig).map(idHex),
      bridgeId: () => null,
      // The handoff resolves its heir through lookup().path.
      lookup: async (t) => ({ path: self._sorted(t).filter(x => x !== idBig).map(idHex) }),
      isReachableId: () => true,
    };
    const am = new AxonaManager({ dht, now: () => self.clock.t, rootReplicas });
    am.nodeId = idBig;
    am.setLogSink(() => {});
    this.nodes.set(idBig, { am, handlers, id: idBig });
    return am;
  }

  _sorted(target) {
    let t; try { t = typeof target === 'bigint' ? target : BigInt('0x' + String(target)); } catch { t = 0n; }
    return [...this.nodes.keys()].sort((a, b) => ((a ^ t) < (b ^ t) ? -1 : 1));
  }
  _closest(target) { const s = this._sorted(target); return s.length ? s[0] : null; }
}

const DESC = (name) => ({ region: 'useast', owner: null, name, write: 'open' });

// Build a leaver holding history for `desc`, plus a live heir in the same region.
async function pair(name, { heirAdmits = true } = {}) {
  const desc = DESC(name);
  const topicId = await deriveTopicIdBig(desc);
  const region = topicId >> 256n;
  const net = new Net();
  // The LEAVER is placed closest to the topic so it is the natural root; the HEIR
  // is second-closest, which is exactly who routing re-converges on when it goes.
  const leaverId = (region << 256n) | 0x0001n;
  const heirId   = (region << 256n) | 0x0002n;
  const leaver = net.add(leaverId);
  const heir   = net.add(heirId);
  if (!heirAdmits) {
    // A heir that refuses everything: its ack must report held=0 < sent, which
    // the leaver is required to treat as NOT acked.
    heir._syncIngest = async () => {};
  }
  leaver.pubsubSubscribe(topicId);
  const role = leaver._becomeRoot(topicId);
  const author = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: author, ts: net.clock.t });
  await leaver._ingestPublish(role, JSON.stringify(env));
  return { net, leaver, heir, topicId, role, env, desc };
}

console.log('two-node handoff (ROOT path) — the exemption must be EARNED, not assumed\n');

// ── 0. THE FABRIC IS REAL ──────────────────────────────────────────────────
{
  const { leaver, heir, topicId, role } = await pair('h2-fabric');
  ok('0a. the leaver roots the topic and holds history',
    role.isRoot && role.cache.length === 1, `cache=${role.cache.length}`);
  ok('0b. a second live node exists in the topic\'s region — a real heir, not a stub',
    heir.nodeId !== leaver.nodeId && (heir.nodeId >> 256n) === (topicId >> 256n));
  // _handoffAcked is created BY pubsubLeaveHandoff, so before it runs the set
  // does not exist at all. Asserting `.size === 0` on undefined would throw and
  // look like a failure of the property rather than of the check.
  ok('0c. precondition — nothing is exempt before the handoff runs',
    (leaver._handoffAcked?.size ?? 0) === 0);
}

// ── 1. THE CONTROL FIRES — this is what a single node could not do ─────────
{
  const { leaver, heir, topicId } = await pair('h2-acked');
  await leaver.pubsubLeaveHandoff();

  ok('1a. CONTROL — the heir ACKED, so the topic IS exempt from further retry ' +
     'rounds. This assertion was unreachable from one node.',
    (leaver._handoffAcked?.has(lc(idHex(topicId))) ?? false),
    JSON.stringify([...(leaver._handoffAcked ?? [])]));
  ok('1b. …and the exemption is EARNED: the heir actually holds the history',
    (heir.axonRoles.get(topicId)?.cache?.length ?? 0) === 1,
    `heirCache=${heir.axonRoles.get(topicId)?.cache?.length ?? 0}`);
}

// ── 2. NO ACK, NO EXEMPTION ────────────────────────────────────────────────
// The heir is unreachable for HANDOFF. The leaver must not assume the history
// landed — this is the #340 class, where a leaver dropped the last copy and
// reported success.
{
  const { net, leaver, topicId } = await pair('h2-noack');
  net.drop.add('pubsub:handoff');
  await leaver.pubsubLeaveHandoff();
  ok('2a. the heir never received the handoff → NO exemption is granted',
    !(leaver._handoffAcked?.has(lc(idHex(topicId))) ?? false),
    JSON.stringify([...(leaver._handoffAcked ?? [])]));
}

// ── 3. A DISHONEST-BY-OMISSION ACK EARNS NOTHING (#402) ────────────────────
// The heir answers, but admits nothing: held=0 < sent. An ack that arrives is
// not the same as history that survived, and the leaver must tell them apart —
// otherwise "someone replied" would discharge the obligation, which is this
// whole version's defect in one more costume.
{
  const { leaver, heir, topicId } = await pair('h2-short', { heirAdmits: false });
  await leaver.pubsubLeaveHandoff();
  ok('3a. the heir ACKED but held nothing → still NO exemption (held < sent)',
    !(leaver._handoffAcked?.has(lc(idHex(topicId))) ?? false),
    JSON.stringify([...(leaver._handoffAcked ?? [])]));
  ok('3b. …and the heir genuinely holds no history, so the refusal is real',
    (heir.axonRoles.get(topicId)?.cache?.length ?? 0) === 0,
    `heirCache=${heir.axonRoles.get(topicId)?.cache?.length ?? 0}`);
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
