// =====================================================================
// smoke_synaptome_storm_feedback.mjs — isolate the Synaptome-Maintenance
// connection-attempt feedback loop that 6522f2f reverted ("connection-count
// storm + convergence wedge"), on the REAL kernel, deterministically.
//
// WHY THIS TEST EXISTS. The dht-sim mesh harness could not reproduce the storm
// because the sim transport's openConnection is free/instant/always-succeeds, so
// a near-successor is admitted on the first probe and the deficit closes. The
// real storm needs the one condition the free sim never has: a near-successor
// that the maintenance loop KEEPS trying to reach but that does NOT get admitted
// (a slow/failing WebRTC negotiation on the web transport). Then:
//
//   findKClosest(self) → near-successor in deficit → _considerCandidate →
//   openConnection PROBE → not admitted → isConn() still false → next tick the
//   SAME successor is in deficit → probe again … every tick, forever.
//   Each onPeerDied additionally fires _scheduleMaintain (AxonaPeer.js:448),
//   so churn multiplies the passes.
//
// This test drives the real AxonaPeer + real sim transport, makes a designated
// set of XOR-near successors UNBINDABLE (openConnection returns false for them,
// modelling a negotiation that never completes), and MEASURES openConnection
// attempts per tick / per injected death — maintenance OFF vs ON, and with each
// of the three council mitigations applied separately as a peer-method patch.
//
// Mitigations (as isolated patches on the real peer, standing in for the kernel
// changes they model — NO kernel edit here; this test only measures):
//   #1 zero-wire local candidates: findKClosest → already-connected members only
//   #2 decouple refill from onClose: _scheduleMaintain no-op (periodic tick only)
//   #3 hysteresis floor: _maintainSynaptome no-op while synaptome.size >= floor
//
// Run: node test/smoke_synaptome_storm_feedback.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));
const xcmp  = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

async function makePeer(net, domain, lat, lng, maintain = null) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, synaptomeMaintain: maintain });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id) };
}

// Count openConnection attempts on the test node, and optionally make a set of
// targets UNBINDABLE (probe returns false without opening — models a WebRTC
// negotiation that never completes). Returns a live counter object.
function instrumentDials(t, unbindable /* Set<bigint> */) {
  const orig = t.transport.openConnection.bind(t.transport);
  const m = { total: 0, toUnbindable: 0 };
  t.transport.openConnection = async (peerId, ...a) => {
    const big = (typeof peerId === 'bigint') ? peerId
      : (typeof peerId === 'string') ? fromHex(peerId) : null;
    m.total++;
    if (big !== null && unbindable.has(big)) { m.toUnbindable++; return false; } // never admits
    return orig(peerId, ...a);
  };
  return m;
}

// A faithful drop as seen by node t: tear t's side of the channel (so
// isConnected → false, like a heartbeat-timeout close) then fire t's onPeerDied
// — the exact path AxonaPeer.start's onPeerDied handler runs on the web transport.
function realDrop(t, victimHex) {
  const nh = t.transport._normPeerId ? t.transport._normPeerId(victimHex) : victimHex;
  t.transport._openTo.delete(nh);
  t.transport._latency.delete(nh);
  const hb = t.transport._heartbeats?.get(nh);
  if (hb) { clearInterval(hb); t.transport._heartbeats.delete(nh); }
  t.transport._fireDied(nh);
}

// Build a discoverable, reachable base mesh + a fresh test node knowing 1 sponsor.
async function scenario(maintain) {
  const net = new SimNetwork(); const domain = new AxonaDomain();
  const base = [];
  for (let i = 0; i < 12; i++) base.push(await makePeer(net, domain, (i*17)%80-40, (i*53)%360-180, null));
  for (let i = 0; i < base.length; i++)
    for (let j = i + 1; j < base.length; j++)
      await base[i].transport.openConnection(base[j].id.id);
  await wait(20);
  const KNEAR = 5;
  const t = await makePeer(net, domain, 5, 5, maintain && { kNear: KNEAR, intervalMs: 999999, maxPerTick: 3 });
  await t.transport.openConnection(base[0].id.id); // sponsor only
  await wait(10);
  // The KNEAR globally-nearest live peers to t (the successors maintenance targets).
  const nearest = base.map(b => b.big).sort((a, b) => xcmp(t.big ^ a, t.big ^ b)).slice(0, KNEAR);
  const recOf = (big) => base.find(b => b.big === big);         // big → base record (for hex + drop)
  return { net, domain, base, t, KNEAR, nearest, recOf };
}

async function main() {
  console.log('Axona synaptome-maintenance STORM/feedback smoke (isolated, real kernel)\n');

  // ── CONTROL: bindable near-successors → refill converges then is a no-op ──
  {
    const { t, KNEAR, nearest } = await scenario(true);
    const m = instrumentDials(t, new Set());               // nothing unbindable
    for (let k = 0; k < 8; k++) { await t.peer._maintainSynaptome(); await wait(15); }
    const have = nearest.filter(id => t.node.synaptome.has(id)).length;
    const dialsAtConverged = m.total;
    const r = await t.peer._maintainSynaptome();            // steady-state pass
    await wait(15);
    const extraAfterConverged = m.total - dialsAtConverged;
    check('CONTROL: bindable successors → quota fills', have === KNEAR, `(${have}/${KNEAR})`);
    check('CONTROL: converged → further pass attempts 0 (no runaway)', r === 0 && extraAfterConverged === 0, `(r=${r}, extra=${extraAfterConverged})`);
    console.log(`    [control] total dials to converge = ${dialsAtConverged}\n`);
  }

  // ── STORM: unbindable near-successors → maintenance re-probes EVERY tick ──
  let stormPerTickOn = 0;
  {
    const { t, nearest } = await scenario(true);
    const unb = new Set(nearest);                            // ALL near-successors won't bind
    const m = instrumentDials(t, unb);
    const TICKS = 10;
    for (let k = 0; k < TICKS; k++) { await t.peer._maintainSynaptome(); await wait(15); }
    stormPerTickOn = m.toUnbindable / TICKS;
    check('STORM: unbindable successors are re-probed every tick (maintenance ON)', m.toUnbindable >= TICKS, `(${m.toUnbindable} probes / ${TICKS} ticks)`);
    check('STORM: never admitted → synaptome never holds them', nearest.every(id => !t.node.synaptome.has(id)));
    console.log(`    [storm ON] ${m.toUnbindable} unbindable-probes over ${TICKS} ticks = ${stormPerTickOn.toFixed(1)}/tick\n`);
  }
  {
    const { t, nearest } = await scenario(false);           // maintenance OFF
    const m = instrumentDials(t, new Set(nearest));
    for (let k = 0; k < 10; k++) { await t.peer._maintainSynaptome().catch(()=>{}); await wait(15); }
    check('STORM baseline: maintenance OFF → zero refill probes', m.toUnbindable === 0, `(${m.toUnbindable})`);
    console.log(`    [storm OFF] unbindable-probes = ${m.total}\n`);
  }

  // ── FEEDBACK: each injected death schedules ONE debounced refill pass ──
  {
    const { t, nearest, recOf } = await scenario(true);
    instrumentDials(t, new Set());                          // converge (nothing unbindable)
    for (let k = 0; k < 8; k++) { await t.peer._maintainSynaptome(); await wait(15); }
    // Count dials caused purely by injected deaths (real onPeerDied → reschedule).
    const m = { total: 0 }; const orig = t.transport.openConnection.bind(t.transport);
    t.transport.openConnection = async (p, ...a) => { m.total++; return orig(p, ...a); };
    const DEATHS = 5;
    for (let d = 0; d < DEATHS; d++) {
      realDrop(t, recOf(nearest[d % nearest.length]).id.id);  // tear channel + fire t's onPeerDied
      await wait(300);                                        // > 250ms debounce
    }
    const perDeath = m.total / DEATHS;
    check('FEEDBACK: onPeerDied → coalesced refill, bounded (not multiplicative)', perDeath > 0 && perDeath <= 3 + 0.5, `(${perDeath.toFixed(2)} dials/death, maxPerTick=3)`);
    console.log(`    [feedback] ${m.total} dials over ${DEATHS} injected deaths = ${perDeath.toFixed(2)}/death (bounded by maxPerTick, NOT multiplicative)\n`);
  }

  // ── MITIGATION #1: zero-wire local candidates → no probe to unbindable ──
  {
    const { t, nearest } = await scenario(true);
    // Model #1: refill sources candidates from already-connected members only.
    t.peer.findKClosest = async () => [...t.node.synaptome.keys()];
    const m = instrumentDials(t, new Set(nearest));
    const TICKS = 10;
    for (let k = 0; k < TICKS; k++) { await t.peer._maintainSynaptome(); await wait(15); }
    check('MIT#1 (local candidates): unbindable-probe storm eliminated', m.toUnbindable === 0, `(${m.toUnbindable} vs ${stormPerTickOn.toFixed(1)}/tick ON)`);
    console.log(`    [mit#1] unbindable-probes over ${TICKS} ticks = ${m.toUnbindable}\n`);
  }

  // ── MITIGATION #2: decouple refill from onClose → deaths stop scheduling ──
  {
    const { t, nearest, recOf } = await scenario(true);
    instrumentDials(t, new Set());
    for (let k = 0; k < 8; k++) { await t.peer._maintainSynaptome(); await wait(15); } // converge
    t.peer._scheduleMaintain = () => {};                    // model #2: onClose no longer refills
    const m = { total: 0 }; const orig = t.transport.openConnection.bind(t.transport);
    t.transport.openConnection = async (p, ...a) => { m.total++; return orig(p, ...a); };
    for (let d = 0; d < 5; d++) { realDrop(t, recOf(nearest[d % nearest.length]).id.id); await wait(300); }
    check('MIT#2 (decouple onClose): injected deaths trigger no refill dials', m.total === 0, `(${m.total}; vs FEEDBACK >0/death)`);
    console.log(`    [mit#2] dials from 5 deaths = ${m.total}\n`);
  }

  // ── MITIGATION #3: hysteresis floor → no refill while size >= floor ──
  // One near-successor is unbindable (always in deficit); the rest bind. After
  // convergence size sits above the floor, so the floor must suppress the probe.
  {
    const { t, nearest } = await scenario(true);
    const unb = new Set([nearest[0]]);                      // one persistent deficit
    const m = instrumentDials(t, unb);
    for (let k = 0; k < 8; k++) { await t.peer._maintainSynaptome(); await wait(15); } // converge (size rises)
    const sizeConverged = t.node.synaptome.size;
    const probesNoFloor = m.toUnbindable;                   // storm baseline while converging
    const FLOOR = Math.max(2, sizeConverged - 1);           // floor below current size → should suppress
    const origMaint = t.peer._maintainSynaptome.bind(t.peer);
    t.peer._maintainSynaptome = async () => (t.node.synaptome.size >= FLOOR ? 0 : origMaint());
    const probesBefore = m.toUnbindable;
    for (let k = 0; k < 10; k++) { await t.peer._maintainSynaptome(); await wait(15); }
    const probesUnderFloor = m.toUnbindable - probesBefore;
    const nonVacuous = sizeConverged >= FLOOR && probesNoFloor > 0;   // deficit was real AND we're above floor
    check('MIT#3 (hysteresis floor): refill suppressed while size >= floor',
      nonVacuous && probesUnderFloor === 0,
      `(size=${sizeConverged} floor=${FLOOR}; probes without floor=${probesNoFloor}, under floor=${probesUnderFloor})`);
    console.log(`    [mit#3] size=${sizeConverged} >= floor=${FLOOR} → probes under floor = ${probesUnderFloor} (baseline ${probesNoFloor})\n`);
  }

  console.log(`Result: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
