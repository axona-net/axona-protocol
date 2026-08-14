// =====================================================================
// smoke_boundary4_registry.mjs — REF-1.1 S4c: the Boundary-4 (bridge
// administration) frame-contract registry TABLE + the standalone certified-
// evaluator sweep over its shadow wrap. Mirrors the ACCEPTED Boundary-2/3
// increment-1 pattern (table-first; the live wiring is S4c increment 2).
//
// The registry is DEFAULT-OFF; flag-on the wrap OBSERVES a decoder-certified
// snapshot beside each handler and emits a trace, never mutating/suppressing/
// reordering the handler or its args; flag-off the handler runs verbatim
// (byte-identical). Dispatch is NOT migrated.
//
//   T. TABLE: 7 rows over 7 wires — client-hello/version-gate (admission),
//      ping/pong (heartbeat), turn-refresh/turn (TURN), peer-list-request
//      (discovery). Deliberate decisions asserted: evidence-axis null (session
//      administration, named outcomes); ADMISSION not AUTH (client-hello carries
//      an admissionGuard = the version gate that closes 4426, authGuard n/a
//      across the boundary); ping/pong is a `t`-keyed REQUEST/RESPONSE
//      conversation on the PAYLOAD leg (pong echoes the ping timestamp); turn and
//      peer-list-request are solicited but carry NO wire correlation key so no
//      conversation is declared.
//   W. WIRING: 7 wires; no signal-style variant split (each wire → one row).
//   R. EVALUATOR SWEEP: a certified representative frame through the shadow wrap
//      of each wire → registered + schemaOk + verdict preserved; async pass/reject
//      inert 'object'; sync throw rethrown+threw; schema-invalid branded
//      schemaOk=false; the uncertified unbranded floor.
//   P. CONVERSATION IS OBSERVABLE: ping/pong conversationPresent TRUE on a
//      certified `t` (the payload-leg pairing key is observed); a no-conversation
//      row (client-hello) is conversationPresent false.
//   D. FLAG-OFF IDENTITY: flag OFF → wrap verbatim + ZERO traces, pass and throw.
//   O. OBSERVE UNIT: makeBoundary4Observers().observe side-channel — flag-off
//      zero traces + input untouched; flag-on branded + verdict UNOBSERVED +
//      scope stamped; never throws out.
// =====================================================================
import { buildBoundary4Registry, boundary4Rows, makeBoundary4Observers } from '../src/transport/boundary4Registry.js';
import { setShadowEnabled } from '../src/registry/index.js';
import { certify } from '../src/registry/snapshotMint.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const certFrame = (obj) => certify(JSON.stringify(obj));
const EMPTY = certFrame({});   // B4 rows carry no meta projection — the meta leg is always empty

// Representative VALID frames (shape only — the registry observes shape; the live
// admission/version guards enforce the real policy).
const CERT = {
  'client-hello':      { version: '2.112.0', wireVersion: '4.0', kernelVersion: '4.62.2', capabilities: ['mesh-relay'] },
  'version-gate':      { minPeerVersion: '4', serverT: 1000 },
  'ping':              { t: 1000, meshBound: 3 },
  'pong':              { t: 1000, serverT: 1001 },
  'turn-refresh':      {},
  'turn':              { turn: { urls: ['turn:t.example:3478'], username: 'u', credential: 'c' }, serverT: 1001 },
  'peer-list-request': {},
};
const WIRES = ['client-hello', 'version-gate', 'ping', 'pong', 'turn-refresh', 'turn', 'peer-list-request'];

console.log('\nREF-1.1 S4c — Boundary-4 (bridge administration) registry\n');

// ── T. TABLE ──────────────────────────────────────────────────────────
{
  const rows = boundary4Rows();
  const by = Object.fromEntries(rows.map((r) => [r.type, r]));
  const types = new Set(rows.map((r) => r.type));
  check('T1. TABLE: 7 rows over 7 types (client-hello, version-gate, ping, pong, turn-refresh, turn, peer-list-request)',
    rows.length === 7 && types.size === 7
    && ['bridge:client-hello', 'bridge:version-gate', 'bridge:ping', 'bridge:pong', 'bridge:turn-refresh', 'bridge:turn', 'bridge:peer-list-request'].every((t) => types.has(t)));

  check('T2. kinds + outcomes: client-hello ONE_WAY/CONNECTION_ADMITTED; version-gate UNSOLICITED_EVENT/VERSION_GATE_ANNOUNCED; ping/pong ONE_WAY/VITALITY_REPORTED+RTT_SAMPLED; turn-refresh/turn TURN_*; peer-list-request PEER_LIST_REQUESTED',
    by['bridge:client-hello'].kind === 'ONE_WAY' && by['bridge:client-hello'].terminalOutcome === 'CONNECTION_ADMITTED'
    && by['bridge:version-gate'].kind === 'UNSOLICITED_EVENT' && by['bridge:version-gate'].terminalOutcome === 'VERSION_GATE_ANNOUNCED'
    && by['bridge:ping'].kind === 'ONE_WAY' && by['bridge:ping'].terminalOutcome === 'VITALITY_REPORTED'
    && by['bridge:pong'].kind === 'ONE_WAY' && by['bridge:pong'].terminalOutcome === 'RTT_SAMPLED'
    && by['bridge:turn-refresh'].terminalOutcome === 'TURN_REFRESH_REQUESTED' && by['bridge:turn'].terminalOutcome === 'TURN_CREDENTIAL_APPLIED'
    && by['bridge:peer-list-request'].terminalOutcome === 'PEER_LIST_REQUESTED');

  // Decision #1: the evidence hierarchy is the pub/sub DATA plane; these session-
  // administration frames carry a named outcome, not an evidence level.
  check('T3. evidence-axis decision: every B4 row has evidence=null AND proves=null AND a named outcome+terminalOutcome',
    rows.every((r) => r.evidence === null && r.proves === null && typeof r.outcome === 'string' && typeof r.terminalOutcome === 'string'));

  // Decision #2: bridge administration is ADMISSION-gated (version + admitted-state),
  // never cryptographic — authGuard is n/a across the whole boundary; client-hello IS
  // the version gate (closes 4426); the post-admit frames are 'admitted'-gated.
  const ch = by['bridge:client-hello'];
  check('T4. ADMISSION not AUTH: client-hello admissionGuard names the version gate + 4426 + the flagDayFloor stage (F1), authGuard n/a; ping/turn-refresh/peer-list-request admissionGuard=admitted; EVERY row authGuard n/a',
    ch.authGuard === 'n/a' && /version gate/i.test(ch.admissionGuard) && /4426/.test(ch.admissionGuard) && /flagDayFloor/.test(ch.admissionGuard)
    && /admitted/.test(by['bridge:ping'].admissionGuard) && /admitted/.test(by['bridge:turn-refresh'].admissionGuard) && /admitted/.test(by['bridge:peer-list-request'].admissionGuard)
    && rows.every((r) => r.authGuard === 'n/a'));

  // Decision #3: ping/pong is a t-keyed REQUEST/RESPONSE conversation on the PAYLOAD
  // leg (pong echoes the ping timestamp for RTT); the KIND stays ONE_WAY.
  const ping = by['bridge:ping'], pong = by['bridge:pong'];
  const tLeg = (r) => r.conversation && r.conversation.pairing.length === 1 && r.conversation.pairing[0].local === 't' && r.conversation.pairing[0].remote === 't' && r.conversation.pairing[0].from === 'payload';
  check('T5. ping=REQUEST↔pong=RESPONSE, paired on t(payload); opposites cross-reference; KIND stays ONE_WAY',
    ping.conversation && ping.conversation.role === 'REQUEST' && ping.conversation.opposite === 'bridge:pong' && tLeg(ping) && ping.kind === 'ONE_WAY'
    && pong.conversation && pong.conversation.role === 'RESPONSE' && pong.conversation.opposite === 'bridge:ping' && tLeg(pong) && pong.kind === 'ONE_WAY');

  // Decision #4: turn-refresh/turn and peer-list-request are solicited but carry NO
  // wire correlation key (like B3 signal offer/answer) — so NO conversation is declared.
  check('T6. turn-refresh/turn and peer-list-request carry NO conversation AND NO correlation subject (solicited, but no wire key — matched by the socket round-trip)',
    by['bridge:turn-refresh'].conversation === null && by['bridge:turn'].conversation === null && by['bridge:peer-list-request'].conversation === null
    && by['bridge:turn-refresh'].correlation === null && by['bridge:turn'].correlation === null && by['bridge:peer-list-request'].correlation === null);

  // Retry classifications grounded in the live handlers + owningService split.
  check('T7. retry: client-hello NONE, version-gate NATURAL, ping NONE, pong NONE, turn-refresh BOUNDED_N (F2), turn NONE, peer-list-request NATURAL; services Admission/Heartbeat/Turn/Discovery',
    by['bridge:client-hello'].retry === 'NONE' && by['bridge:version-gate'].retry === 'NATURAL'
    && by['bridge:ping'].retry === 'NONE' && by['bridge:pong'].retry === 'NONE'
    && by['bridge:turn-refresh'].retry === 'BOUNDED_N' && by['bridge:turn'].retry === 'NONE' && by['bridge:peer-list-request'].retry === 'NATURAL'
    && by['bridge:client-hello'].owningService === 'BridgeAdmission' && by['bridge:ping'].owningService === 'BridgeHeartbeat'
    && by['bridge:turn'].owningService === 'BridgeTurn' && by['bridge:peer-list-request'].owningService === 'BridgeDiscovery');
}

// ── W. WIRING ─────────────────────────────────────────────────────────
{
  const reg = buildBoundary4Registry({ enabled: false });
  check('W1. WIRING: 7 rows; 7 wires; each wire maps to a single row, NO variant split (no signal-style multi-variant frame in this boundary)',
    reg.size() === 7 && reg.wiring.size === 7
    && WIRES.every((w) => reg.wiring.get(w) && !reg.wiring.get(w).variantBy && reg.wiring.get(w).type === `bridge:${w === 'client-hello' ? 'client-hello' : w}`));
}

// ── R. STANDALONE EVALUATOR SWEEP ─────────────────────────────────────
{
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary4Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  const wrapFor = (wire, handler) => reg.wrap(reg.wiring.get(wire).type, handler);

  // R1 — every wire: certified valid frame (empty meta leg) → registered + schemaOk + verdict passed.
  let ok = 0; const miss = [];
  for (const wire of WIRES) {
    tr.length = 0;
    wrapFor(wire, () => undefined).call({}, certFrame(CERT[wire]), EMPTY);
    const r = tr[0];
    if (tr.length === 1 && r.type === reg.wiring.get(wire).type && r.registered === true && r.schemaOk === true && r.verdict === 'passed' && r.faults == null) ok++;
    else miss.push(`${wire}:${JSON.stringify(r)}`);
  }
  check(`R1. standalone evaluator: all ${WIRES.length} wires observed registered + schemaOk, verdict preserved`, ok === WIRES.length, `\n   ${miss.join('\n   ')}`);

  // R2 — async handler: the returned Promise is passed through UNTOUCHED; sync verdict inert 'object'.
  tr.length = 0;
  const pPass = Promise.resolve(7);
  const retA = wrapFor('ping', () => pPass).call({}, certFrame(CERT['ping']), EMPTY);
  check('R2. async handler: returned Promise passed through by identity; inert sync verdict object', retA === pPass && tr.length === 1 && tr[0].verdict === 'object');

  // R3 — rejecting Promise: same object returned, verdict inert object, caller still owns the rejection.
  tr.length = 0;
  const pRej = Promise.reject(new Error('nack'));
  const retR = wrapFor('ping', () => pRej).call({}, certFrame(CERT['ping']), EMPTY);
  let caught = false; try { await retR; } catch { caught = true; }
  check('R3. rejecting Promise: same object returned, verdict object, caller still owns the rejection', retR === pRej && caught && tr[0].verdict === 'object');

  // R4 — synchronous throw: rethrown to the caller; verdict threw.
  tr.length = 0;
  let sthrew = false; try { wrapFor('ping', () => { throw new Error('boom'); }).call({}, certFrame(CERT['ping']), EMPTY); } catch { sthrew = true; }
  check('R4. sync throw: rethrown to caller AND verdict threw', sthrew && tr.length === 1 && tr[0].verdict === 'threw');

  // R5 — schema-invalid: a client-hello missing wireVersion → registered, schemaOk=false, schema fault, handler still ran.
  tr.length = 0;
  let ran = false;
  wrapFor('client-hello', () => { ran = true; }).call({}, certFrame({ version: '2.112.0', kernelVersion: '4.62.2' }), EMPTY);
  check('R5. schema-invalid: client-hello without wireVersion → registered, schemaOk=false, schema fault, handler ran',
    ran && tr.length === 1 && tr[0].registered === true && tr[0].schemaOk === false && (tr[0].faults || []).some((f) => f.startsWith('schema:')));

  // R6 — the unbranded floor: an uncertified LIVE frame is never reflected on.
  tr.length = 0;
  const retU = wrapFor('ping', () => 'handled').call({}, { t: 1, meshBound: 0 }, {});
  check('R6. uncertified live frame: handler verbatim + unbranded-source (no reflection)',
    retU === 'handled' && tr.length === 1 && tr[0].registered === null && (tr[0].faults || []).includes('unbranded-source') && tr[0].verdict === 'unobserved');
}

// ── P. CONVERSATION IS OBSERVABLE (payload-leg `t` pairing) ───────────
{
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary4Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  const drive = (wire, body) => { tr.length = 0; reg.wrap(reg.wiring.get(wire).type, () => undefined).call({}, certFrame(body), EMPTY); return tr[0]; };

  // The pairing key `t` is on the PAYLOAD leg (not meta as in B3) — so a certified
  // frame carrying `t` makes conversationPresent true on both heartbeat legs.
  check('P1. ping (REQUEST leg) conversationPresent TRUE on a certified payload `t`', drive('ping', CERT['ping']).conversationPresent === true);
  check('P2. pong (RESPONSE leg) conversationPresent TRUE on a certified payload `t`', drive('pong', CERT['pong']).conversationPresent === true);
  // A no-conversation row must not claim one.
  check('P3. client-hello (no conversation) → conversationPresent falsey', !drive('client-hello', CERT['client-hello']).conversationPresent);
}

// ── FR. RECUT-1 (Aster/Vega F1-F4: describe the live handlers, not the story) ──
{
  const rows = boundary4Rows();
  const by = Object.fromEntries(rows.map((r) => [r.type, r]));

  // F1 — the flagDayFloor stage is named in BOTH the client-hello guard and note.
  const ch = by['bridge:client-hello'];
  check('FR1. F1: client-hello admissionGuard + note name the flagDayFloor namespace-floor stage (MIN_KERNEL_VERSION / MIN_PEER_APP_VERSION)',
    /flagDayFloor/.test(ch.admissionGuard) && /flagDayFloor/.test(ch.note)
    && /MIN_KERNEL_VERSION/.test(ch.admissionGuard) && /MIN_PEER_APP_VERSION/.test(ch.admissionGuard));

  // F2 — turn-refresh is the bounded, non-idempotent retry; note names the bound + fresh credential.
  const tref = by['bridge:turn-refresh'];
  check('FR2. F2: turn-refresh retry=BOUNDED_N (not NATURAL/NONE); note names TURN_REFRESH_MAX_TRIES + the fresh-credential-per-attempt',
    tref.retry === 'BOUNDED_N' && /TURN_REFRESH_MAX_TRIES/.test(tref.note) && /fresh/i.test(tref.note));

  // F3 — version-gate types minPeerVersion as a string in the table…
  check('FR3. F3: version-gate schema.types.minPeerVersion === "string"',
    by['bridge:version-gate'].schema.types && by['bridge:version-gate'].schema.types.minPeerVersion === 'string');

  // …and a NUMERIC minPeerVersion is now schema-invalid (junk no longer certifies).
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary4Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  reg.wrap('bridge:version-gate', () => undefined).call({}, certFrame({ minPeerVersion: 4, serverT: 1 }), EMPTY);
  check('FR4. F3 negative: version-gate with a NUMERIC minPeerVersion → schemaOk FALSE + schema fault (not silently certified)',
    tr.length === 1 && tr[0].schemaOk === false && (tr[0].faults || []).some((f) => f.startsWith('schema:')), `\n   ${JSON.stringify(tr[0])}`);
  setShadowEnabled(false);

  // F4 — peer-list-request note describes the additive onPeerList ingest, not a full replace.
  const plr = by['bridge:peer-list-request'];
  check('FR5. F4: peer-list-request note names the ADDITIVE onPeerList ingest (never removes) and does NOT claim a "full replace"',
    /additive/i.test(plr.note) && /onPeerList/.test(plr.note) && /never removes/i.test(plr.note) && !/full replace/i.test(plr.note));
}

// ── D. FLAG-OFF IDENTITY (inert wrap) ─────────────────────────────────
{
  const tr = [];
  const reg = buildBoundary4Registry({ sink: (rec) => tr.push(rec) });   // honors the GLOBAL runtime flag
  setShadowEnabled(false);

  tr.length = 0;
  const ret = reg.wrap('bridge:pong', () => 'verbatim').call({}, certFrame(CERT['pong']), EMPTY);
  check('D1. flag-off: handler runs verbatim (return identical) AND ZERO traces emitted (inert wrap)', ret === 'verbatim' && tr.length === 0);

  tr.length = 0;
  let threw = false; try { reg.wrap('bridge:pong', () => { throw new Error('x'); }).call({}, certFrame(CERT['pong']), EMPTY); } catch { threw = true; }
  check('D2. flag-off: a throwing handler still rethrows verbatim AND ZERO traces', threw && tr.length === 0);
}

// ── O. OBSERVE UNIT (makeBoundary4Observers side-channel) ─────────────
{
  { const tr = []; const { observe } = makeBoundary4Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(false);
    observe('pong', 'c-1', Object.freeze({ ...CERT['pong'] }));   // frozen: proves observe never mutates input
    check('O1. observe() flag-off: ZERO traces + input untouched (byte-identical, no certify work)', tr.length === 0); }

  { const tr = []; const { observe } = makeBoundary4Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(true);
    observe('pong', 'c-1', CERT['pong']);
    check('O2. observe() flag-on: branded bridge:pong, schemaOk, verdict UNOBSERVED (no handler claim), scope stamped',
      tr.length === 1 && tr[0].type === 'bridge:pong' && tr[0].registered === true && tr[0].schemaOk === true && tr[0].verdict === 'unobserved' && tr[0].scope === 'c-1'); }

  { const tr = []; const { observe } = makeBoundary4Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(true);
    observe('version-gate', 'c-2', CERT['version-gate']);
    check('O3. observe() flag-on: a bridge->peer version-gate observed, verdict UNOBSERVED, scope stamped',
      tr.length === 1 && tr[0].type === 'bridge:version-gate' && tr[0].verdict === 'unobserved' && tr[0].scope === 'c-2'); }

  { const { observe } = makeBoundary4Observers({});
    setShadowEnabled(true);
    let obsThrew = false; const cyc = {}; cyc.self = cyc;   // cyclic → JSON.stringify throws INSIDE observe
    try { observe('ping', 'c-3', cyc); } catch { obsThrew = true; }
    check('O4. observe() never throws OUT — a cyclic/malformed body is swallowed (transport unaffected)', obsThrew === false); }
}

setShadowEnabled(false);
console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
