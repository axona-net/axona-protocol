// =====================================================================
// smoke_direct_fence.mjs — REF-1.1 E1 direct_* admissible-type fence.
//
// The fence is a RUNTIME capability check at the single parameterized registrar
// for the computed `direct_${type}` wire (council-cleared design; David chose a
// registration-time allowlist; Vega ffdba957 hardening). It gates BOTH sides —
// sendDirect (send) and onDirectMessage (receive) — on a construction-time
// allowlist, phased like the cutover: OBSERVE at E1 (record would-refuse, allow),
// ENFORCE at E4 (throw = fail closed). Malformed types are refused in both phases.
//
// White-box: a fake node/transport captures onNotification installs + notify
// sends; the fence fires before either, so no mesh is needed. Cases mirror Vega's
// named list: omitted-vs-empty, malformed, observe, enforce, construct-time copy,
// single-registrar.
//
// Run: node test/smoke_direct_fence.mjs
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { AxonaDomain } from '../src/dht/AxonaDomain.js';
import { sealByOwnMethods } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const threw = (fn) => { try { fn(); return false; } catch { return true; } };
const threwAsync = async (fn) => { try { await fn(); return false; } catch { return true; } };

function makeTransport() {
  const installs = [];      // wires passed to onNotification
  const sends = [];         // { peerId, wire } passed to notify
  const cbs = new Map();    // wire -> the installed listener (to prove delivery)
  // E3c (SEAL): registerDirectFrame is capability-mandatory (no literal-name fallback),
  // so this transport mock deposits a notification capability that delegates to its
  // onNotification recorder.
  return sealByOwnMethods({
    installs, sends, cbs,
    onNotification(wire, cb) { installs.push(wire); cbs.set(wire, cb); return { wire, unsub() {} }; },
    async notify(peerId, wire) { sends.push({ peerId, wire }); return true; },
  });
}
function makePeer(opts = {}) {
  const tx = makeTransport();
  const node = { id: 1n, alive: true, transport: tx };
  const peer = new AxonaPeer({ node, transport: tx, domain: new AxonaDomain(), ...opts });
  const logs = [];
  peer._emitLog = (level, ev, meta) => logs.push({ level, ev, meta });   // capture would-refuse traces
  return { peer, tx, logs };
}
const wouldRefuse = (logs) => logs.filter((l) => l.ev === 'direct-fence-would-refuse');

console.log('\nREF-1.1 E1 — direct_* admissible-type fence\n');

// ── OMITTED allowlist (undefined) → dormant: well-formed types pass untouched ──
{
  const { peer, tx } = makePeer();                       // no directMessageTypes
  peer.onDirectMessage('foo', () => {});
  check('OMIT1. omitted allowlist installs a well-formed type (dormant)', tx.installs.includes('direct_foo'));
  await peer.sendDirect(2n, 'foo', {});
  check('OMIT2. omitted allowlist sends a well-formed type (dormant)', tx.sends.some((s) => s.wire === 'direct_foo'));
}

// ── MALFORMED type → fail-closed in BOTH phases, even dormant (corrupt wire) ──
{
  const { peer, tx } = makePeer();                       // dormant
  check('MAL1. non-string type throws (onDirectMessage)', threw(() => peer.onDirectMessage(123, () => {})));
  check('MAL2. empty-string type throws', threw(() => peer.onDirectMessage('', () => {})));
  check('MAL3. already direct_-prefixed type throws', threw(() => peer.onDirectMessage('direct_x', () => {})));
  check('MAL4. malformed installs NOTHING', tx.installs.length === 0);
  check('MAL5. malformed type throws (sendDirect)', await threwAsync(() => peer.sendDirect(2n, 'direct_x', {})));
  check('MAL6. malformed send sends NOTHING', tx.sends.length === 0);
}

// ── APPROVED type DELIVERS: the installed listener reaches the app handler ──
{
  const { peer, tx } = makePeer({ directMessageTypes: ['ok'] });
  let got = null;
  peer.onDirectMessage('ok', (payload, meta) => { got = { payload, meta }; });
  const cb = tx.cbs.get('direct_ok');
  cb && cb('66'.repeat(33), { hello: 1 });   // simulate transport delivering a frame (well-formed 66-hex fromId)
  check('DELIVER1. an approved type installs a listener that receives the payload', got && got.payload && got.payload.hello === 1);
}

// ── EXPLICIT allowlist + OBSERVE (default): un-admitted records + allows ──
{
  const { peer, tx, logs } = makePeer({ directMessageTypes: ['ok'] });
  peer.onDirectMessage('ok', () => {});
  check('OBS1. admitted type installs with NO would-refuse trace', tx.installs.includes('direct_ok') && wouldRefuse(logs).length === 0);
  peer.onDirectMessage('nope', () => {});
  check('OBS2. un-admitted type still INSTALLS in observe (byte-identical)', tx.installs.includes('direct_nope'));
  check('OBS3. un-admitted type RECORDS a would-refuse trace', wouldRefuse(logs).some((l) => l.meta?.type === 'nope'));
  await peer.sendDirect(2n, 'nope', {});
  check('OBS4. un-admitted send still SENDS in observe + records', tx.sends.some((s) => s.wire === 'direct_nope') && wouldRefuse(logs).some((l) => l.meta?.op === 'sendDirect'));
}

// ── EXPLICIT allowlist + ENFORCE (E4): un-admitted throws + does nothing ──
{
  const { peer, tx } = makePeer({ directMessageTypes: ['ok'], enforceDirectMessageTypes: true });
  peer.onDirectMessage('ok', () => {});
  check('ENF1. admitted type installs under enforce', tx.installs.includes('direct_ok'));
  const before = tx.installs.length;
  check('ENF2. un-admitted onDirectMessage THROWS under enforce', threw(() => peer.onDirectMessage('nope', () => {})));
  check('ENF3. enforce-refused install did NOTHING', tx.installs.length === before);
  check('ENF4. un-admitted sendDirect THROWS under enforce', await threwAsync(() => peer.sendDirect(2n, 'nope', {})));
  check('ENF5. enforce-refused send did NOTHING', tx.sends.length === 0);
}

// ── OMITTED ≠ EMPTY: explicit empty Set admits ZERO; omitted is dormant ──
{
  const empty = makePeer({ directMessageTypes: new Set(), enforceDirectMessageTypes: true });
  check('DISTINCT1. explicit empty Set + enforce REFUSES every type', threw(() => empty.peer.onDirectMessage('anything', () => {})));
  const omit = makePeer({ enforceDirectMessageTypes: true });   // omitted + enforce → dormant
  omit.peer.onDirectMessage('anything', () => {});
  check('DISTINCT2. omitted + enforce is dormant (installs) — not the same as empty', omit.tx.installs.includes('direct_anything'));
}

// ── CONSTRUCT-TIME COPY [R1]: mutating the caller's array after construct is inert ──
{
  const arr = ['ok'];
  const { peer } = makePeer({ directMessageTypes: arr, enforceDirectMessageTypes: true });
  arr.push('late');                                     // mutate AFTER construction
  check('COPY1. a type added to the caller array post-construct is NOT admitted', threw(() => peer.onDirectMessage('late', () => {})));
}

// ── SINGLE REGISTRAR: repeated onDirectMessage installs the raw listener once ──
{
  const { peer, tx } = makePeer({ directMessageTypes: ['ok'] });
  peer.onDirectMessage('ok', () => {});
  peer.onDirectMessage('ok', () => {});
  check('SINGLE1. two onDirectMessage(ok) installs direct_ok exactly once', tx.installs.filter((w) => w === 'direct_ok').length === 1);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
