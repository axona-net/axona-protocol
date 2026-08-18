// smoke_write_flight.mjs — the write recovery flight (Dead-Root Eviction
// v0.3, phase E3). A forwarded write completes ONLY on its INGEST-ack;
// consumed-at-the-named-root is hop-local evidence and never terminal. The
// centerpiece is RESPONSIVE-NO-MUTATION (Aster seq 427, the GH #28 captured
// mode): a root that keeps answering — verdicts, even probe NACKs — while
// never producing a receipt must be evicted on a bounded schedule and the
// write promoted to a live holder. SIGKILL-silence is the easier sibling.
//
// Run: node test/smoke_write_flight.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { INGEST_ACK_MS, FLIGHT_PROBE_MS } from '../src/pubsub/writeFlight.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG    = 0x89n << 248n;
const idHex  = (b) => b.toString(16).padStart(66, '0');
const lc     = (s) => String(s).toLowerCase();

// Distances to TOPIC, closest first: FLAPPER < HEIR < SELF < NB. The flapper
// is NOT a neighbour — it is reached only through routing, so SELF is the
// routing terminus and the last-mile beacon correction is what forwards.
const TOPIC   = REG | 0x1000n;
const FLAPPER = REG | 0x1001n;   // answers everything, ingests nothing
const HEIR    = REG | 0x1002n;   // the live holder promotion should pick
const SELF    = REG | 0x1010n;
const NB      = REG | 0x8000n;

function mk({ neighbors = () => [NB, HEIR] } = {}) {
  const clock = { t: 1_000_000 };
  const sends = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      sends.push({ target, type, payload });
      // Everything is consumed AND attributed to its target — the captured
      // flapper's exact signature. Failure never fires; only acks matter.
      const t = typeof target === 'bigint' ? target : BigInt(`0x${target}`);
      const at = (payload?.via?.length) ? lc(String(payload.via[0])) : lc(idHex(t));
      return { consumed: true, hops: 1, atNode: at };
    },
    neighbors,
    bridgeId: () => null,
    findKClosest: async () => [],
    lookup: async () => null,
  };
  const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 0 });
  am.nodeId = SELF;
  const logs = [];
  am.setLogSink((lvl, evt, data) => logs.push({ lvl, evt, data }));
  return { am, clock, sends, logs };
}

function beaconFlapper(am, epoch = 4) {
  am._onRootBeacon(
    { root: lc(idHex(FLAPPER)), topics: [idHex(TOPIC)], epochs: [epoch], beaconId: `b-e3-${epoch}-${Math.floor(am._now())}`, layer: 1 },
    { fromId: idHex(NB) },
  );
}

const PUBP = (id) => ({ topicId: idHex(TOPIC), json: JSON.stringify({ msgId: id, v: 3 }) });
const sendsOf = (sends, type) => sends.filter((s) => s.type === type);

// ── 1. Happy path: ack completes the flight; no probe ever flies ────────────
{
  const { am, clock, sends } = mk();
  beaconFlapper(am);
  await am._onPub(PUBP('m-happy'), { fromId: idHex(NB), isTerminal: true });
  ok('1a the PUB forwarded toward the beaconed root opened a flight',
    am._writeFlights?.size === 1, `flights ${am._writeFlights?.size}`);
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'm-happy', epoch: 4, op: 'pub' }, { fromId: idHex(FLAPPER) });
  ok('1b the INGEST-ack — and only it — completes the flight', am._writeFlights.size === 0);
  clock.t += INGEST_ACK_MS + FLIGHT_PROBE_MS * 4;
  am._flightSweep();
  ok('1c later sweeps have nothing to do — no probe, no eviction',
    sendsOf(sends, T.RECEIPTPROBE).length === 0 && am._rootTombstones == null);
}

// ── 2. RESPONSIVE-NO-MUTATION: nacks forever → bounded eviction + promotion ─
{
  const { am, clock, sends, logs } = mk();
  beaconFlapper(am, 4);
  await am._onPub(PUBP('m-flap'), { fromId: idHex(NB), isTerminal: true });
  ok('2a consumed-and-attributed did NOT complete the write', am._writeFlights.size === 1);

  clock.t += INGEST_ACK_MS + 1;                          // ack deadline passes
  am._flightSweep();
  const probe1 = sendsOf(sends, T.RECEIPTPROBE);
  ok('2b sweep past the ack deadline sends the receipt probe to the suspect',
    probe1.length === 1 && probe1[0].payload.msgId === 'm-flap');

  am._onReceiptNack({ topicId: idHex(TOPIC), msgId: 'm-flap', op: 'pub', reason: 'not-held' });
  ok('2c the honest nack earns ONE direct retry of the write',
    sendsOf(sends, T.PUB).filter((s) => s.payload.via?.[0] === lc(idHex(FLAPPER))).length >= 2);

  clock.t += INGEST_ACK_MS + 1;                          // retry acked nothing
  am._flightSweep();                                     // probe round 2
  am._onReceiptNack({ topicId: idHex(TOPIC), msgId: 'm-flap', op: 'pub', reason: 'not-held' });
  clock.t += INGEST_ACK_MS + 1;
  am._flightSweep();                                     // rounds cap → evict

  const t = am._rootTombstones?.get(TOPIC);
  ok('2d the incarnation is tombstoned {root, epoch}', !!t && t.root === lc(idHex(FLAPPER)) && t.epoch === 4,
    JSON.stringify(t, (k, v) => typeof v === 'bigint' ? String(v) : v));
  ok('2e the corpse\'s beacon record is buried with it', !am._rootBeacons.has(TOPIC));
  ok('2f eviction is loud', logs.some((l) => l.evt === 'pubsub:root-evicted'));
  const promoted = sendsOf(sends, T.PUB).filter((s) => s.payload.via?.[0] === lc(idHex(HEIR)));
  ok('2g the write is retry-promoted to the closest LIVE holder', promoted.length === 1,
    `to-heir ${promoted.length}`);
  ok('2h the promotion opened a fresh flight against the heir (a second flapper gets the same treatment)',
    am._writeFlights.size === 1 && [...am._writeFlights.values()][0].rootHex === lc(idHex(HEIR)));
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'm-flap', epoch: 5, op: 'pub' }, { fromId: idHex(HEIR) });
  ok('2i the heir\'s ack settles everything — no flights left', am._writeFlights.size === 0);
}

// ── 3. SILENCE (SIGKILL analog): no nack, probe window expires → evict ──────
{
  const { am, clock, sends } = mk();
  beaconFlapper(am, 7);
  await am._onPub(PUBP('m-dead'), { fromId: idHex(NB), isTerminal: true });
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();       // probe 1 (silence)
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();     // silence convicts... round 2 first
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  ok('3a silence convicts within the bounded rounds — tombstone recorded',
    am._rootTombstones?.get(TOPIC)?.epoch === 7);
  ok('3b write promoted onward, not dropped',
    sendsOf(sends, T.PUB).some((s) => s.payload.via?.[0] === lc(idHex(HEIR))));
}

// ── 4. Serialization: two stranded writers, ONE flight, ONE probe ───────────
{
  const { am, clock, sends } = mk();
  beaconFlapper(am, 2);
  await am._onPub(PUBP('m-a'), { fromId: idHex(NB), isTerminal: true });
  await am._onPub(PUBP('m-b'), { fromId: idHex(NB), isTerminal: true });
  ok('4a both writes joined the ONE standing flight',
    am._writeFlights.size === 1 && [...am._writeFlights.values()][0].entries.size === 2);
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  ok('4b one probe for the flight, not one per writer', sendsOf(sends, T.RECEIPTPROBE).length === 1);
}

// ── 5. Promotion to SELF + the epoch chain (E4 seed) ────────────────────────
{
  const { am, clock, logs } = mk({ neighbors: () => [] });   // nobody else live
  beaconFlapper(am, 9);
  await am._onPub(PUBP('m-self'), { fromId: idHex(NB), isTerminal: true });
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  ok('5a with no live neighbour, SELF promotes and roots the topic',
    logs.some((l) => l.evt === 'pubsub:write-flight-promoted' && l.data?.to === 'self'));
  const role = am.axonRoles.get(TOPIC);
  ok('5b the promoted incarnation mints PAST the tombstoned epoch (9 → 10)',
    !!role && role.isRoot && role.epoch === 10, `epoch ${role && role.epoch}`);
}

// ── 6. The tombstone gates the corpse's ghost, admits its rebirth ───────────
{
  const { am, clock } = mk();
  beaconFlapper(am, 4);
  await am._onPub(PUBP('m-gate'), { fromId: idHex(NB), isTerminal: true });
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();     // evicted at epoch 4
  beaconFlapper(am, 4);                                  // the ghost re-beacons
  ok('6a a beacon from the tombstoned incarnation loses the closer gate',
    am._rootClaim.liveCloserRoot(TOPIC, { requireReachable: false }) === null);
  beaconFlapper(am, 11);                                 // legitimate rebirth: higher epoch
  ok('6b a HIGHER epoch from the same node is a rebirth and passes',
    am._rootClaim.liveCloserRoot(TOPIC, { requireReachable: false }) === lc(idHex(FLAPPER)));
}

// ── 7. KILL rides the same flight, completing separately on its own ack ─────
{
  const { am, clock, sends } = mk();
  beaconFlapper(am, 5);
  await am._onKill({ topicId: idHex(TOPIC), kill: { msgId: 'k-target' } }, { fromId: idHex(NB), isTerminal: true });
  const f = [...(am._writeFlights?.values() ?? [])][0];
  ok('7a a forwarded KILL opens a flight with op:kill bound to the TARGET msgId',
    !!f && [...f.entries.values()][0].op === 'kill' && [...f.entries.values()][0].msgId === 'k-target');
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'k-target', epoch: 5, op: 'pub' }, { fromId: idHex(FLAPPER) });
  ok('7b a PUB ack does NOT complete a KILL entry — ops are separate completions',
    am._writeFlights.size === 1);
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'k-target', epoch: 5, op: 'kill' }, { fromId: idHex(FLAPPER) });
  ok('7c the KILL ack completes it', am._writeFlights.size === 0);
  void sends; void clock;
}

// ── 8. Death immediately AFTER the ack: no stale state, next write recovers ─
{
  const { am, clock, sends } = mk();
  beaconFlapper(am, 6);
  await am._onPub(PUBP('m-first'), { fromId: idHex(NB), isTerminal: true });
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'm-first', epoch: 6, op: 'pub' }, { fromId: idHex(FLAPPER) });
  ok('8a first write settled honestly — the ack was true when given', am._writeFlights.size === 0);
  // The root dies now (silently). The NEXT write runs the whole flight fresh.
  await am._onPub(PUBP('m-second'), { fromId: idHex(NB), isTerminal: true });
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  ok('8b the corpse is convicted by the SECOND write\'s own flight — no false failure on the first',
    am._rootTombstones?.get(TOPIC)?.epoch === 6);
  ok('8c the second write promoted onward',
    sendsOf(sends, T.PUB).some((s) => s.payload.via?.[0] === lc(idHex(HEIR))));
}

// ── 9. THE ACK MUST BIND (Aster seq 439): sender + epoch, or nothing moves ──
{
  const { am, clock, sends } = mk();
  beaconFlapper(am, 4);
  await am._onPub(PUBP('m-bind'), { fromId: idHex(NB), isTerminal: true });
  ok('9a flight open against the flapper at epoch 4', am._writeFlights.size === 1);
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'm-bind', epoch: 4, op: 'pub' }, { fromId: idHex(HEIR) });
  ok('9b a valid ack from a DIFFERENT holder does not settle the suspect\'s flight',
    am._writeFlights.size === 1);
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'm-bind', epoch: 9, op: 'pub' }, { fromId: idHex(FLAPPER) });
  ok('9c the right node at the WRONG epoch does not settle it either',
    am._writeFlights.size === 1);
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  ok('9d the flight proceeded to its receipt-bound recovery regardless',
    sendsOf(sends, T.RECEIPTPROBE).length === 1);
  am._onIngestAck({ topicId: idHex(TOPIC), msgId: 'm-bind', epoch: 4, op: 'pub' }, { fromId: idHex(FLAPPER) });
  ok('9e the BOUND ack — right node, right incarnation — settles it', am._writeFlights.size === 0);
}

console.log(fail === 0 ? `\nsmoke_write_flight: ${n}/${n} ok` : `\nsmoke_write_flight: ${fail} FAILED of ${n}`);
process.exit(fail === 0 ? 0 : 1);
