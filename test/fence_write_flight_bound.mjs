// fence_write_flight_bound.mjs — 4.129.1: a write recovery flight ENDS.
//
// Before 4.129.1 a write no root would ingest circled for ever (2026-11-04,
// axona-win svc-15, Orion c2735cc0): a PUB past the publish skew is dropped
// `drop-stale` at every root with no ack, so every flight is evicted and the
// write promoted again; `_rootTombstones` keeps ONE tombstone per topic, so the
// second eviction re-admitted the first root and the chain ping-ponged between
// two neighbours; and a held node's hop-local `holdFwd` was frozen into the
// stored entry, so every re-send was dropped on sight as a foreign return.
//
//   B1  a chain of promotions stops at FLIGHT_MAX_PROMOTIONS, every target
//       distinct, ending `write-flight-promotion-cap`
//   B2  the second promotion never returns to the first evicted root
//   B3  a PUB whose envelope ts is outside the skew ends at its first eviction
//       (`write-flight-stale`); a fresh ts and a missing ts are promoted
//   B4  the stored entry, the nack retry and the promotion carry no holdFwd;
//       the first forward still carries it (the held node's own bound)
//
// Run: node test/fence_write_flight_bound.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { T } from '../src/pubsub/constants.js';
import { INGEST_ACK_MS, FLIGHT_PROBE_MS, FLIGHT_MAX_PROMOTIONS } from '../src/pubsub/writeFlight.js';
import { MAX_PUBLISH_SKEW_MS } from '../src/pubsub/envelope.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG   = 0x89n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const lc    = (s) => String(s).toLowerCase();

// Six neighbours, each closer to TOPIC than SELF, closest first.
const TOPIC = REG | 0x1000n;
const N     = [1, 2, 3, 4, 5, 6].map((i) => REG | (0x1000n + BigInt(i)));
const SELF  = REG | 0x9000n;
const hx    = (b) => lc(idHex(b));

function mk(neighbors = N) {
  const clock = { t: 10_000_000 };
  const sends = [];
  const dht = {
    verdictsSupported: true,
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: async (target, type, payload) => {
      sends.push({ target, type, payload });
      const t = typeof target === 'bigint' ? target : BigInt(`0x${target}`);
      const at = (payload?.via?.length) ? lc(String(payload.via[0])) : lc(idHex(t));
      return { consumed: true, hops: 1, atNode: at };
    },
    neighbors: () => neighbors,
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

// Silence convicts in two sweeps: past the ack deadline a probe flies, past
// the probe window the root is evicted and the write promoted.
function evictOnce(am, clock) {
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
}

const pub = (id, ts) => ({ topicId: idHex(TOPIC), json: JSON.stringify(ts === undefined ? { msgId: id, v: 3 } : { msgId: id, v: 3, ts }) });
const promotionsOf = (sends, id, from = 0) => sends.slice(from)
  .filter((s) => s.type === T.PUB && JSON.parse(s.payload.json).msgId === id)
  .map((s) => s.payload.via?.[0]);

// ── B1. the chain stops at the cap, every target distinct ───────────────────
{
  const { am, clock, sends, logs } = mk();
  am._forwardToRoot(TOPIC, T.PUB, pub('m-cap'), hx(N[0]));
  const start = sends.length;
  for (let i = 0; i < FLIGHT_MAX_PROMOTIONS + 3; i++) evictOnce(am, clock);
  const targets = promotionsOf(sends, 'm-cap', start);
  ok('B1a the write is promoted exactly FLIGHT_MAX_PROMOTIONS times', targets.length === FLIGHT_MAX_PROMOTIONS,
    `promotions ${targets.length} → ${targets.map((t) => t.slice(-4)).join(',')}`);
  ok('B1b every promotion target is a different node, none the first root',
    new Set(targets).size === targets.length && !targets.includes(hx(N[0])));
  ok('B1c the chain ends loudly: write-flight-ended why write-flight-promotion-cap',
    logs.some((l) => l.evt === 'pubsub:write-flight-ended' && l.data.why === 'write-flight-promotion-cap'));
  ok('B1d no flight is left open', am._writeFlights.size === 0, `flights ${am._writeFlights.size}`);
}

// ── B2. no ping-pong: the second promotion skips the first evicted root ─────
{
  const { am, clock, sends } = mk([N[0], N[1], N[2]]);
  am._forwardToRoot(TOPIC, T.PUB, pub('m-pp'), hx(N[0]));
  const start = sends.length;
  evictOnce(am, clock);
  evictOnce(am, clock);
  const targets = promotionsOf(sends, 'm-pp', start);
  ok('B2a first promotion goes to the next closest, N2', targets[0] === hx(N[1]), String(targets[0]).slice(-4));
  ok('B2b second promotion goes to N3, NOT back to N1', targets[1] === hx(N[2]), String(targets[1]).slice(-4));
}

// ── B3. a stale PUB ends at its first eviction; fresh and ts-less go on ─────
{
  const { am, clock, sends, logs } = mk();
  am._forwardToRoot(TOPIC, T.PUB, pub('m-old', clock.t - MAX_PUBLISH_SKEW_MS - 60_000), hx(N[0]));
  const start = sends.length;
  evictOnce(am, clock);
  ok('B3a a PUB past the publish skew is not promoted', promotionsOf(sends, 'm-old', start).length === 0);
  ok('B3b it ends loudly: write-flight-ended why write-flight-stale',
    logs.some((l) => l.evt === 'pubsub:write-flight-ended' && l.data.why === 'write-flight-stale'));
  ok('B3c no flight is left open', am._writeFlights.size === 0);
}
{
  const { am, clock, sends } = mk();
  am._forwardToRoot(TOPIC, T.PUB, pub('m-fresh', 10_000_000), hx(N[0]));
  am._forwardToRoot(TOPIC, T.PUB, pub('m-nots'), hx(N[0]));
  const start = sends.length;
  evictOnce(am, clock);
  ok('B3d control: a fresh ts is promoted', promotionsOf(sends, 'm-fresh', start).length === 1);
  ok('B3e control: a missing ts is promoted, as before (the root judges it)', promotionsOf(sends, 'm-nots', start).length === 1);
}

// ── B4. holdFwd is hop-local: never stored, never re-sent ───────────────────
{
  const { am, clock, sends } = mk();
  am._forwardToRoot(TOPIC, T.PUB, { ...pub('m-hold'), holdFwd: 'aaaabbbbccccdddd' }, hx(N[0]));
  ok('B4a the first forward still carries holdFwd (the held node\'s own re-entry bound)',
    sends[0]?.payload?.holdFwd === 'aaaabbbbccccdddd');
  const entry = [...[...am._writeFlights.values()][0].entries.values()][0];
  ok('B4b the stored entry has no holdFwd', !('holdFwd' in entry.payload));
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  const before = sends.length;
  am._onReceiptNack({ topicId: idHex(TOPIC), msgId: 'm-hold', op: 'pub', reason: 'not-held' });
  const retry = sends.slice(before).filter((s) => s.type === T.PUB);
  ok('B4c the nack retry carries no holdFwd', retry.length === 1 && !('holdFwd' in retry[0].payload));
  const before2 = sends.length;
  clock.t += INGEST_ACK_MS + 1; am._flightSweep();
  clock.t += FLIGHT_PROBE_MS + 1; am._flightSweep();
  const promo = sends.slice(before2).filter((s) => s.type === T.PUB);
  ok('B4d the promotion carries no holdFwd', promo.length === 1 && !('holdFwd' in promo[0].payload),
    `promo ${promo.length}`);
}

console.log(fail ? `\n${fail} FAILED of ${n + fail}` : `\nall ${n} passed`);
process.exit(fail ? 1 : 0);
