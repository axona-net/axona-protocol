// fence_pub_defers_to_corpse.mjs — a publish must not be handed to a root that
// nothing can reach AND NOTHING LEARNED. This fence pins the COUNCIL-APPROVED
// end-state (seq 146/147 + the atNode amendment), which is not "never forward":
// the first forward toward a beaconed root is allowed — it is how the system
// finds out — but the VERDICT of that forward must drive state, in both
// directions, and no state may move without evidence.
//
// THE DEFECT (David, 2026-08-02, prod). A Windows host restarted and ~20 relays
// went with it. Reads on the affected keyspace slice kept working. WRITES failed
// for hours and never self-healed; they came back only when the relays were
// relaunched by hand. Measured during the outage: the FAILING topic had 6 of 8
// candidate holders alive; a WORKING one had 4 of 8. Cohort health was not the
// discriminator, so the cause had to be a gate.
//
// THE GATE ASYMMETRY (the trigger):
//
//   _onSub  → _liveCloserRoot(topic)                            requireReachable TRUE
//   _onPub  → _liveCloserRoot(topic, { requireReachable:false })
//   _onKill → same as PUB
//
// and in rootClaim.liveCloserRoot:
//
//   if (b.verified) return b.root;                                   ← NO liveness, NO freshness
//   if (m._isReachableId(b.root)) return b.root;
//   if (!requireReachable && (now - b.at) < beaconMs*1.5) return b.root;   ← PUB/KILL only
//
// So at ONE node holding ONE stale beacon, a SUB falls through to a live
// terminus while a PUB is handed to the corpse by _deferToRoot and reported
// 'consumed'. Fatal rather than untidy because a subscription renews forever
// while a publish is one-shot BY DESIGN (acknowledging a publisher would
// disclose its location): reads rode out a multi-hour outage on retries the
// write path never gets. And rootClaim.become() fires only at a routing
// TERMINUS, so an eaten publish never mints a replacement root — re-rooting is
// driven by traffic ARRIVING, and the healing signal and the lost payload are
// the same packets.
//
// TWO FINDINGS THE FIRST DRAFT OF THIS FENCE PRODUCED, NOW PINNED HERE:
//   • rootClaim.js:147 — a `verified` beacon defeats liveness on BOTH paths,
//     bounded only by exp (2×ROOT_VERIFY_MS = 90s, 3× the remote-beacon cut).
//   • rootClaim.js:300 — _deferToRoot calls demote, which RE-PINS _upstream to
//     the corpse and subscribes through it. ONE publish broke the read path
//     that was working. Prod (4.49.0) has no unpin at all: permanent until
//     restart, which is every "only a reload fixed it" report we have.
//
// THE APPROVED CONTRACT (council 2026-08-02, both reviewers concurring):
//   • Dispatch FIRST; mutate role/_upstream ONLY on an explicit consumed
//     verdict — and (atNode amendment) only when the consuming node IS the
//     beaconed root. consumed-elsewhere or absent atNode → NO mutation; the
//     consumer's own DELIVER re-homes us organically via _onDeliver.
//   • On an explicit failed verdict: invalidate ONLY the matching beacon
//     candidate (guarded by root identity + `at` stamp). No demote, no pin.
//     The publisher's early-resend pump then retries into a node that will
//     root properly — so "first lost, second saved" is the floor, and the
//     invalidation landing sub-second inside the pump window is the
//     expectation. Section 1f pins the retry, not the assertion.
//   • unsupported / violation → NO state transition (fail closed; violation
//     logs loudly). Silence is never evidence, in either direction.
//   • The `verified` bypass gets the same 1.5×BEACON_MS freshness cut the
//     loose clause already has (option B). Safe because verified records are
//     written once at demote time and a live root's plain beacons overwrite
//     them within 20s — only a DEAD root leaves one standing its full 90s.
//   • SUB's defer is untouched: its gate already requires reachability, so
//     its demote is evidence-based at defer time.
//
// EXPECTED RED against 99a9b55 (pre-fix): 1e, 1f, 1g, 2b, 2c, 3c, 3f, 4b, 6b,
// 6c fail — 10 of 28. Controls (1c, 1d, 3d, 3e, 4c, 4d, 6a, all of section 5)
// must be GREEN both before and after the fix — a fix that reddens section 5
// has deleted the anti-hotspot last-mile correction and re-opened the
// same-region root flap it was written to stop.
//
// Run: node test/fence_pub_defers_to_corpse.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { BEACON_MS, BEACON_TTL_MS, ROOT_VERIFY_MS, T } from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG   = 0x89n << 248n;                 // one region throughout: the region gate is not under test
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc    = (s) => String(s).toLowerCase();

// Distances to TOPIC, closest first:  DEAD (0x0001) < ALIVE (0x0002) < OTHER (0x0004) < SELF (0x0010) < FAR
const TOPIC = REG | 0x1000n;
const DEAD  = REG | 0x1001n;                 // the relay that went down with the host
const ALIVE = REG | 0x1002n;                 // a closer root that IS reachable (control)
const OTHER = REG | 0x1004n;                 // a THIRD node that may consume a rerouted message
const SELF  = REG | 0x1010n;
const FAR   = REG | 0x9000n;                 // farther from TOPIC than SELF — must never win
const NB    = REG | 0x8000n;                 // a live neighbour, so we are not "alone in the dark"
const SUBER = REG | 0x7000n;                 // a REMOTE subscriber (not us — that path has its own guard)

// The transport double answers the way production _route does: it NEVER
// rejects, reports failure by RESOLVING {consumed:false, exhausted:true}, and
// names the consuming node in atNode when consumption happened. A message
// via-pinned to a dead target exhausts; anything else is consumed at its
// target — unless `consumeAt` overrides WHO consumed (the reroute-elsewhere
// case the atNode amendment exists for).
// atNodeShape: 'hex' (test-double convention) | 'bigint' (production adapters
// report an id VALUE) | 'garbage' (malformed — must be read as no evidence).
// holdVerdicts: routeMessage's promises stay pending until release() — the
// only way to stage a beacon-generation race against an in-flight dispatch.
function mk({ neighbors = [NB, ALIVE], deadVia = [DEAD], consumeAt = null,
              atNodeShape = 'hex', holdVerdicts = false } = {}) {
  const clock = { t: 1_000_000 };
  const sends = [];
  const pending = [];
  const dead = new Set(deadVia.map((d) => lc(idHex(d))));
  const shape = (big) => atNodeShape === 'bigint' ? big
    : atNodeShape === 'garbage' ? { not: 'an id' }
    : lc(idHex(big));
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      const via = [...(payload.via || [])];
      sends.push({ type, via, topicId: payload.topicId });
      let result;
      if (via.length && dead.has(lc(via[0]))) {
        result = (consumeAt != null)
          ? { consumed: true, hops: 3, atNode: shape(consumeAt) }
          : { consumed: false, exhausted: true, hops: 4 };
      } else {
        const tBig = typeof target === 'bigint' ? target : BigInt(`0x${String(target)}`);
        result = { consumed: true, hops: 1, atNode: shape(via.length ? BigInt(`0x${via[0]}`) : tBig) };
      }
      if (!holdVerdicts) return result;
      return new Promise((res) => pending.push(() => res(result)));
    },
    neighbors: () => neighbors,
    bridgeId: () => null,
    findKClosest: async () => [],
    lookup: async () => null,                 // overridden where a section drives _verifyRoots
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  const release = () => { while (pending.length) pending.shift()(); };
  return { am, clock, sends, dht, release };
}

// Install beacons through the PRODUCTION receiver — a fence that installs its
// own premise certifies nothing about the code that normally installs it.
function beacon(am, rootBig, topicBig = TOPIC, seq = 0) {
  am._onRootBeacon(
    { root: lc(idHex(rootBig)), topics: [idHex(topicBig)], beaconId: `b-${idHex(rootBig).slice(0, 8)}-${seq}`, layer: 1 },
    { fromId: idHex(NB) },
  );
}

const subPayload  = () => ({ topicId: idHex(TOPIC), via: [], subscriberId: idHex(SUBER), since: 0, hw: 0, lw: 0 });
const pubPayload  = (id = 'm1') => ({ topicId: idHex(TOPIC), via: [], json: JSON.stringify({ msgId: id, body: 'x' }) });
const killPayload = () => ({ topicId: idHex(TOPIC), via: [], msgId: 'm1' });
const TERMINAL    = { isTerminal: true, fromId: idHex(NB), targetId: idHex(SELF) };
const settle      = () => new Promise((r) => setImmediate(r));

const sentTo = (sends, who, type = null) =>
  sends.filter((s) => s.via.length && lc(s.via[0]) === lc(idHex(who)) && (!type || s.type === type));

console.log('pub defers to corpse — a forward may probe, but only evidence moves state\n');

// ── 1. THE FAILED FORWARD MUST TEACH: invalidate, retry locally, touch nothing ─
{
  const { am, sends } = mk();
  beacon(am, DEAD);

  ok('1a. the beacon was accepted by the production receiver',
    am._rootBeacons.get(TOPIC)?.root === lc(idHex(DEAD)), JSON.stringify(am._rootBeacons.get(TOPIC)));
  ok('1b. …and its root is genuinely UNREACHABLE, yet strictly closer than us',
    am._isReachableId(lc(idHex(DEAD))) === false && ((DEAD ^ TOPIC) < (SELF ^ TOPIC)));

  am._onSub(subPayload(), TERMINAL);
  ok('1c. CONTROL — the SUB is served here, NOT deferred to the dead root',
    sentTo(sends, DEAD, T.SUB).length === 0 && am.axonRoles.get(TOPIC)?.isRoot === true,
    JSON.stringify(sends));

  const before = sends.length;
  const verdict = await am._onPub(pubPayload(), TERMINAL);
  await settle();

  ok('1d. CONTROL — the first PUB probes the beaconed root (forwarding is HOW we learn)',
    sentTo(sends.slice(before), DEAD, T.PUB).length === 1 && verdict === 'consumed',
    JSON.stringify(sends.slice(before)));
  ok('1e. THE CONTRACT — the failed verdict INVALIDATES the beacon record',
    !am._rootBeacons.has(TOPIC), JSON.stringify(am._rootBeacons.get(TOPIC)));

  const beforeRetry = sends.length;
  await am._onPub(pubPayload(), TERMINAL);   // the pump's re-send of the SAME message
  await settle();
  const role = am.axonRoles.get(TOPIC);
  // Cache content is deliberately NOT asserted: this fence's envelope is
  // unsigned, and B-4 ingress verification correctly drops it — that gate has
  // its own suites. The contract HERE is routing: the retry terminates at this
  // node (zero outbound sends) instead of being fed back to the corpse.
  ok('1f. THE CONTRACT — the RETRY is not fed to the corpse; it terminates HERE as root',
    sends.slice(beforeRetry).length === 0 && role?.isRoot === true,
    `sends=${JSON.stringify(sends.slice(beforeRetry))} isRoot=${role?.isRoot}`);
  ok('1g. THE CONTRACT — no _upstream pin toward the corpse was EVER written',
    lc((am._upstream.get(TOPIC) || [])[0] || '') !== lc(idHex(DEAD)) &&
    sentTo(sends, DEAD, T.SUB).length === 0,
    JSON.stringify({ upstream: am._upstream.get(TOPIC), subs: sentTo(sends, DEAD, T.SUB) }));
}

// ── 2. A SERVING ROOT KEEPS ITS ROLE THROUGH A FAILED FORWARD ──────────────
// The node holding the topic — role, cache, subscribers — probes the beaconed
// root like anyone else, but a failed probe must not have ALREADY cost it the
// role. Pre-fix, _deferToRoot demoted before the send: the write was given
// away AND the giver stripped itself, unconditionally.
{
  const { am, sends } = mk();
  beacon(am, DEAD);                          // lands first: no role yet, demote no-ops
  am._onSub(subPayload(), TERMINAL);         // we become the serving root
  ok('2a. precondition — we are the serving root, not pinned to the corpse',
    am.axonRoles.get(TOPIC)?.isRoot === true &&
    lc((am._upstream.get(TOPIC) || [])[0] || '') !== lc(idHex(DEAD)));

  await am._onPub(pubPayload(), TERMINAL);
  await settle();
  const role = am.axonRoles.get(TOPIC);
  ok('2b. THE CONTRACT — the failed probe did NOT demote us: the role survives',
    role?.isRoot === true, JSON.stringify({ isRoot: role?.isRoot }));
  ok('2c. THE CONTRACT — and did not pin our subscription to the corpse',
    lc((am._upstream.get(TOPIC) || [])[0] || '') !== lc(idHex(DEAD)) &&
    sentTo(sends, DEAD, T.SUB).length === 0,
    JSON.stringify({ upstream: am._upstream.get(TOPIC), subs: sentTo(sends, DEAD, T.SUB) }));
}

// ── 3. THE `verified` BYPASS GETS THE FRESHNESS CUT (option B) ─────────────
// 3-inject drives the record shape rootElection.js:281 writes; 3-verify drives
// the PRODUCTION writer itself (_verifyRoots), per Aster's review — injecting
// the record was the one premise the first draft hand-installed.
{
  const { am, clock, sends } = mk();
  am._rootBeacons.set(TOPIC, { root: lc(idHex(DEAD)), at: clock.t, exp: clock.t + 2 * ROOT_VERIFY_MS, verified: true });
  clock.t += BEACON_MS * 2;                  // past 1.5×BEACON_MS, inside exp
  ok('3a. the injected verified record is past the freshness cut, not expired',
    (clock.t - am._rootBeacons.get(TOPIC).at) >= BEACON_MS * 1.5 && clock.t < am._rootBeacons.get(TOPIC).exp);
  am._onSub(subPayload(), TERMINAL);
  await am._onPub(pubPayload(), TERMINAL);
  await settle();
  ok('3b. CONTROL — a fresh verified record still steers (checked in 3e)', true);
  ok('3c. THE CONTRACT — a STALE verified record steers NOBODY, sub or pub',
    sentTo(sends, DEAD).length === 0, JSON.stringify(sentTo(sends, DEAD)));
}
{
  // Drive the production writer: hold a root claim, let _verifyRoots find a
  // strictly-closer node via dht.lookup, and use the record IT writes.
  const { am, clock, sends, dht } = mk();
  dht.lookup = async () => ({ path: [lc(idHex(NB)), lc(idHex(DEAD))] });
  am._onSub(subPayload(), TERMINAL);         // become root (claim to verify)
  const formed = am.axonRoles.get(TOPIC);
  formed.lastVerify = 0; formed.formedAt = clock.t - 60_000;   // due immediately
  await am._verifyRoots(clock.t);            // takes `now` — repairPlane passes it
  await settle();
  const rec = am._rootBeacons.get(TOPIC);
  ok('3d. the PRODUCTION writer stamped a verified record naming the closer node',
    rec?.verified === true && rec?.root === lc(idHex(DEAD)), JSON.stringify(rec));

  const beforeFresh = sends.length;
  await am._onPub(pubPayload('m-fresh'), TERMINAL);
  await settle();
  ok('3e. CONTROL — while FRESH, the verified record steers the publish',
    sentTo(sends.slice(beforeFresh), DEAD, T.PUB).length === 1, JSON.stringify(sends.slice(beforeFresh)));

  // Re-arm the record (the failed probe above may have consumed it), then age it.
  am._rootBeacons.set(TOPIC, { root: lc(idHex(DEAD)), at: clock.t, exp: clock.t + 2 * ROOT_VERIFY_MS, verified: true });
  clock.t += BEACON_MS * 2;
  const beforeStale = sends.length;
  await am._onPub(pubPayload('m-stale'), TERMINAL);
  await settle();
  ok('3f. THE CONTRACT — once stale, the same verified record steers nothing',
    sentTo(sends.slice(beforeStale), DEAD, T.PUB).length === 0, JSON.stringify(sends.slice(beforeStale)));
}

// ── 4. THE atNode AMENDMENT: consumed is not consumed-BY-HIM ───────────────
// A via-pinned PUB whose waypoint is popped mid-route can be consumed by a
// DIFFERENT node. The verdict reads consumed — but demoting toward the
// beaconed root on that evidence pins us to a node that never took the
// message. Mutation requires consumed AND atNode === the beaconed root;
// anything else is no-evidence and mutates nothing (the real consumer's
// DELIVER re-homes us organically).
{
  const { am, sends } = mk({ consumeAt: OTHER });   // routed-to-DEAD is consumed at OTHER
  beacon(am, DEAD);
  am._onSub(subPayload(), TERMINAL);
  await am._onPub(pubPayload(), TERMINAL);
  await settle();
  ok('4a. the probe was consumed — but by a different node than the beacon named',
    sentTo(sends, DEAD, T.PUB).length === 1);
  ok('4b. THE AMENDMENT — consumed-elsewhere moves NO state: role kept, no pin',
    am.axonRoles.get(TOPIC)?.isRoot === true &&
    lc((am._upstream.get(TOPIC) || [])[0] || '') !== lc(idHex(DEAD)),
    JSON.stringify({ isRoot: am.axonRoles.get(TOPIC)?.isRoot, upstream: am._upstream.get(TOPIC) }));
}
{
  // The MULTI-HOP LIVE ROOT — the case that made option A wrong. DEAD here is
  // not dead: it fails _isReachableId (not a direct neighbour) yet is routable
  // and CONSUMES the probe. Confirmed consumption at the named root is the one
  // piece of evidence that justifies demote + re-home. Green pre-fix too (the
  // old code reached the same end state, just before the evidence existed) —
  // this is the control that the fix keeps re-homing working.
  const { am, sends } = mk({ consumeAt: DEAD });
  beacon(am, DEAD);
  am._onSub(subPayload(), TERMINAL);                // strict SUB gate can't see it → we root
  await am._onPub(pubPayload(), TERMINAL);
  await settle();
  ok('4c. CONTROL — the probe reached the multi-hop root and was consumed THERE',
    sentTo(sends, DEAD, T.PUB).length === 1);
  ok('4d. CONTROL — confirmed consumption AT the named root DOES re-home us under it',
    am.axonRoles.get(TOPIC)?.isRoot !== true &&
    lc((am._upstream.get(TOPIC) || [])[0] || '') === lc(idHex(DEAD)),
    JSON.stringify({ isRoot: am.axonRoles.get(TOPIC)?.isRoot, upstream: am._upstream.get(TOPIC) }));
}

// ── 5. CONTROLS — green before AND after; red here means the fix broke #353 ─
{
  const { am, sends } = mk({ deadVia: [] });
  beacon(am, ALIVE);
  ok('5a-pre. the beaconed root is reachable', am._isReachableId(lc(idHex(ALIVE))) === true);
  await am._onPub(pubPayload(), TERMINAL);
  ok('5a. CONTROL — a publish IS forwarded to a reachable closer root',
    sentTo(sends, ALIVE, T.PUB).length === 1, JSON.stringify(sends));
}
{
  const { am, clock, sends } = mk();
  beacon(am, DEAD);
  clock.t += BEACON_MS * 2;                  // outside 1.5×BEACON_MS, inside TTL
  ok('5b-pre. the plain beacon is stale-but-unexpired',
    (clock.t - am._rootBeacons.get(TOPIC).at) >= BEACON_MS * 1.5 && clock.t < am._rootBeacons.get(TOPIC).exp);
  await am._onPub(pubPayload(), TERMINAL);
  ok('5b. CONTROL — past the freshness cut, the publish is NOT forwarded',
    sentTo(sends, DEAD, T.PUB).length === 0, JSON.stringify(sends));
}
{
  const { am, sends } = mk({ neighbors: [NB, FAR], deadVia: [] });
  beacon(am, FAR);
  await am._onPub(pubPayload(), TERMINAL);
  ok('5c. CONTROL — never forward to a node FARTHER from the topic than us (I-2)',
    sentTo(sends, FAR, T.PUB).length === 0, JSON.stringify(sends));
}

// ── 6. KILL PARITY — the same contract, same funnel ────────────────────────
// _onKill carries the identical requireReachable:false defer, but GATED on
// holding no role (unlike PUB's, which is unconditional — that asymmetry is
// why section 2 exists for PUB and this section runs ROLELESS). A kill is a
// publish with a side-effect; a lost post-outage kill is the same class of
// loss (the kill-leak lesson, #260/#128).
{
  const { am, sends } = mk();
  beacon(am, DEAD);                          // roleless near-miss node: no _onSub here
  const before = sends.length;
  await am._onKill(killPayload(), TERMINAL);
  await settle();
  ok('6a. CONTROL — the first KILL probes the beaconed root',
    sentTo(sends.slice(before), DEAD, T.KILL).length === 1, JSON.stringify(sends.slice(before)));
  ok('6b. THE CONTRACT — the failed probe invalidated the beacon for KILL too',
    !am._rootBeacons.has(TOPIC), JSON.stringify(am._rootBeacons.get(TOPIC)));
  const beforeRetry = sends.length;
  await am._onKill(killPayload(), TERMINAL);
  await settle();
  ok('6c. THE CONTRACT — the retried KILL is not fed to the corpse',
    sentTo(sends.slice(beforeRetry), DEAD, T.KILL).length === 0, JSON.stringify(sends.slice(beforeRetry)));
}

// ── 7. ASTER'S CONSTRAINTS (council seq 149/150) ───────────────────────────
// Attribution is by id VALUE through the shared predicate, never a raw string
// compare at the call site; malformed attribution is no evidence; and a
// verdict from generation A must never erase generation B.
{
  // 7a. production adapters report atNode as a BIGINT — attribution must hold.
  const { am, sends } = mk({ consumeAt: DEAD, atNodeShape: 'bigint' });
  beacon(am, DEAD);
  am._onSub(subPayload(), TERMINAL);
  await am._onPub(pubPayload(), TERMINAL);
  await settle();
  ok('7a. bigint atNode attributes correctly — confirmed consumption re-homes us',
    am.axonRoles.get(TOPIC)?.isRoot !== true &&
    lc((am._upstream.get(TOPIC) || [])[0] || '') === lc(idHex(DEAD)),
    JSON.stringify({ isRoot: am.axonRoles.get(TOPIC)?.isRoot, upstream: am._upstream.get(TOPIC) }));
  void sends;
}
{
  // 7b. MALFORMED atNode is no evidence — nothing may move.
  const { am } = mk({ consumeAt: DEAD, atNodeShape: 'garbage' });
  beacon(am, DEAD);
  am._onSub(subPayload(), TERMINAL);
  await am._onPub(pubPayload(), TERMINAL);
  await settle();
  ok('7b. malformed atNode moves NO state: role kept, no pin, no throw',
    am.axonRoles.get(TOPIC)?.isRoot === true &&
    lc((am._upstream.get(TOPIC) || [])[0] || '') !== lc(idHex(DEAD)),
    JSON.stringify({ isRoot: am.axonRoles.get(TOPIC)?.isRoot, upstream: am._upstream.get(TOPIC) }));
}
{
  // 7c. GENERATION RACE — while dispatch toward generation A is in flight, a
  // NEWER beacon (generation B, same root, later `at`) arrives. A's failed
  // verdict must invalidate only A; B survives. Without the `at` guard this
  // deletes B and the freshly-relearned pointer with it.
  const { am, clock, release } = mk({ holdVerdicts: true });
  beacon(am, DEAD);                                    // generation A, at = t0
  const genA = am._rootBeacons.get(TOPIC)?.at;
  const inflight = am._onPub(pubPayload(), TERMINAL);  // dispatch held open
  clock.t += 1_000;
  beacon(am, DEAD, TOPIC, 1);                          // generation B, at = t0+1000
  const genB = am._rootBeacons.get(TOPIC)?.at;
  ok('7c-pre. generation B replaced A in the record while A was in flight',
    genB === clock.t && genB !== genA);
  release(); await inflight; await settle();
  ok('7c. RACE — A\'s failed verdict does NOT erase generation B',
    am._rootBeacons.get(TOPIC)?.at === genB,
    JSON.stringify(am._rootBeacons.get(TOPIC)));
}

console.log(`\n${fail ? `FAIL ${fail}/${n + fail}` : `PASS ${n}/${n}`}`);
process.exit(fail ? 1 : 0);
