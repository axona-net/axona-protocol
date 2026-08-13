// =====================================================================
// smoke_boundary3_registry.mjs — REF-1.1 S4b: the Boundary-3 (WebRTC signalling
// + mesh-auth) frame-contract registry TABLE + the standalone certified-evaluator
// sweep over its shadow wrap. Mirrors the ACCEPTED Boundary-2 increment-1 pattern
// (smoke_boundary3 is table-first; the live wiring is S4b increment 2).
//
// The registry is DEFAULT-OFF; flag-on the wrap OBSERVES a decoder-certified
// snapshot beside each handler and emits a trace, never mutating/suppressing/
// reordering the handler or its args; flag-off the handler runs verbatim
// (byte-identical). Dispatch is NOT migrated.
//
//   T. TABLE: 8 rows over 6 wires — peer-list/peer-joined/peer-left (bridge
//      signalling discovery), signal×3 (SDP offer/answer + ICE, variant rows),
//      mesh hello + hello-sig (base auth). Deliberate decisions asserted:
//      evidence-axis null (connection plane, named outcomes); signal frames are
//      UNAUTHENTICATED here (channel identity bound downstream by hello-sig's
//      fingerprint CBV); discovery is bridge-asserted; no conversation subject.
//   W. WIRING: 6 wires; signal carries a value-gated variantBy on payload.kind
//      (sdp-offer/sdp-answer/ice); the others map to a single row.
//   R. EVALUATOR SWEEP: a certified representative frame through the shadow wrap
//      of each non-variant wire → registered + schemaOk + verdict preserved;
//      async pass/reject inert 'object'; sync throw rethrown+threw; schema-
//      invalid branded schemaOk=false; the uncertified unbranded floor.
//   V. VARIANT SELECTION: a certified signal frame selects offer/answer/candidate
//      by payload.kind; an unknown kind selects no known variant.
//   D. FLAG-OFF IDENTITY: flag OFF → wrap verbatim + ZERO traces, pass and throw.
//   O. OBSERVE UNIT: makeBoundary3Observers().observe side-channel — flag-off
//      zero traces + input untouched; flag-on branded + verdict UNOBSERVED +
//      scope stamped; signal variant via observe; never throws out.
// =====================================================================
import { buildBoundary3Registry, boundary3Rows, makeBoundary3Observers } from '../src/transport/boundary3Registry.js';
import { setShadowEnabled } from '../src/registry/index.js';
import { certify } from '../src/registry/snapshotMint.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const certFrame = (obj) => certify(JSON.stringify(obj));

// Representative VALID frames (shape only — the registry observes shape; the live
// authGuard enforces the crypto). Hex strings need not be crypto-valid here.
const NODEID = 'ab'.repeat(33);   // 66 hex
const PUBKEY = 'cd'.repeat(32);   // 64 hex
const SIG    = 'ed25519:' + 'ef'.repeat(64);
const SDP    = 'v=0\r\no=- 1 1 IN IP4 0.0.0.0\r\n';
const CAND   = { candidate: 'candidate:1 1 udp 2 1.2.3.4 5 typ host', sdpMid: '0', sdpMLineIndex: 0 };
const CERT = {
  'peer-list':   { peers: ['aa1', 'bb2'] },
  'peer-joined': { peerId: 'aa1' },
  'peer-left':   { peerId: 'aa1', nodeId: 'abcdef01' },
  'hello':       { proto: 'axona/5', nonce: 'nonceA' },
  'hello-sig':   { proto: 'axona/5', nodeId: NODEID, pubkey: PUBKEY, sig: SIG, pow: '' },
};
const SIGNAL = { offer: { kind: 'sdp-offer', sdp: SDP }, answer: { kind: 'sdp-answer', sdp: SDP }, candidate: { kind: 'ice', candidate: CAND } };
// non-variant wires drive the R sweep; signal is exercised in the V block
const WIRES = ['peer-list', 'peer-joined', 'peer-left', 'hello', 'hello-sig'];

console.log('\nREF-1.1 S4b — Boundary-3 (WebRTC signalling + mesh-auth) registry\n');

// ── T. TABLE ──────────────────────────────────────────────────────────
{
  const rows = boundary3Rows();
  const by = Object.fromEntries(rows.map((r) => [r.variant ? `${r.type}:${r.variant}` : r.type, r]));
  const types = new Set(rows.map((r) => r.type));
  check('T1. TABLE: 8 rows over 6 types (peer-list/joined/left, signal×3 offer/answer/candidate, hello, hello-sig)',
    rows.length === 8 && types.size === 6
    && ['mesh:peer-list', 'mesh:peer-joined', 'mesh:peer-left', 'mesh:signal', 'mesh:hello', 'mesh:hello-sig'].every((t) => types.has(t))
    && by['mesh:signal:offer'] && by['mesh:signal:answer'] && by['mesh:signal:candidate']);

  check('T2. kinds + outcomes: discovery UNSOLICITED_EVENT/PEER_*; signal ONE_WAY/*_APPLIED; hello ONE_WAY/NONCE_EXCHANGED; hello-sig ONE_WAY/CHANNEL_AUTHENTICATED',
    by['mesh:peer-list'].kind === 'UNSOLICITED_EVENT' && by['mesh:peer-list'].terminalOutcome === 'PEER_SET_ANNOUNCED'
    && by['mesh:peer-joined'].terminalOutcome === 'PEER_ANNOUNCED' && by['mesh:peer-left'].terminalOutcome === 'PEER_DEPARTED'
    && by['mesh:signal:offer'].kind === 'ONE_WAY' && by['mesh:signal:offer'].terminalOutcome === 'OFFER_APPLIED'
    && by['mesh:signal:answer'].terminalOutcome === 'ANSWER_APPLIED' && by['mesh:signal:candidate'].terminalOutcome === 'CANDIDATE_APPLIED'
    && by['mesh:hello'].kind === 'ONE_WAY' && by['mesh:hello'].terminalOutcome === 'NONCE_EXCHANGED'
    && by['mesh:hello-sig'].kind === 'ONE_WAY' && by['mesh:hello-sig'].terminalOutcome === 'CHANNEL_AUTHENTICATED');

  // Decision #1: evidence hierarchy is the pub/sub DATA plane; these connection-plane
  // frames carry a named outcome, not an evidence level.
  check('T3. evidence-axis decision: every B3 row has evidence=null AND proves=null AND a named outcome+terminalOutcome',
    rows.every((r) => r.evidence === null && r.proves === null && typeof r.outcome === 'string' && typeof r.terminalOutcome === 'string'));

  // Decision #2: signal frames are UNAUTHENTICATED at this boundary; the crypto
  // guard lives on mesh:hello-sig (fingerprint CBV = the MITM defense over the SDP).
  check('T4. signal is UNAUTHENTICATED here (authGuard n/a on offer/answer/candidate) AND carries NO conversation/correlation subject',
    ['offer', 'answer', 'candidate'].every((v) => by[`mesh:signal:${v}`].authGuard === 'n/a' && by[`mesh:signal:${v}`].conversation === null && by[`mesh:signal:${v}`].correlation === null));

  // Decision #3: mesh base auth completes on hello-sig — verifyAuthHello over a CBV
  // that folds the DTLS fingerprints (the MITM defense the unsigned signal relies on).
  const hs = by['mesh:hello-sig'];
  check('T5. mesh:hello-sig is the base-auth completion: CHANNEL_AUTHENTICATED, verifyAuthHello guard names the fingerprint CBV; nonce-leg hello + discovery carry no per-frame auth',
    /verifyAuthHello/.test(hs.authGuard) && /fingerprint|cbvFromFingerprints/i.test(hs.authGuard)
    && by['mesh:hello'].authGuard === 'n/a' && by['mesh:peer-list'].authGuard === 'n/a' && by['mesh:peer-joined'].authGuard === 'n/a');

  // Decision #4: signal is one frame, three payload.kind variants, value-gated.
  check('T6. signal variants: offer=sdp-offer, answer=sdp-answer, candidate=ice (capabilityRange.kind); discovery owningService MeshDiscovery, signal MeshSignalling, auth MeshAuth',
    by['mesh:signal:offer'].capabilityRange.kind === 'sdp-offer' && by['mesh:signal:answer'].capabilityRange.kind === 'sdp-answer' && by['mesh:signal:candidate'].capabilityRange.kind === 'ice'
    && by['mesh:peer-list'].owningService === 'MeshDiscovery' && by['mesh:signal:offer'].owningService === 'MeshSignalling' && by['mesh:hello'].owningService === 'MeshAuth');
}

// ── W. WIRING ─────────────────────────────────────────────────────────
{
  const reg = buildBoundary3Registry({ enabled: false });
  const sig = reg.wiring.get('signal');
  check('W1. WIRING: 8 rows; 6 wires; signal has value-gated variantBy on payload.kind (sdp-offer/sdp-answer/ice); every other wire maps to a single row, no variant split',
    reg.size() === 8 && reg.wiring.size === 6
    && WIRES.every((w) => reg.wiring.get(w) && !reg.wiring.get(w).variantBy)
    && sig && sig.type === 'mesh:signal' && sig.variantBy && sig.variantBy.path === 'kind'
    && sig.variantBy.cases['sdp-offer'] === 'offer' && sig.variantBy.cases['sdp-answer'] === 'answer' && sig.variantBy.cases['ice'] === 'candidate');
}

// ── R. STANDALONE EVALUATOR SWEEP (non-variant wires) ─────────────────
{
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary3Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  const wrapFor = (wire, handler) => reg.wrap(reg.wiring.get(wire).type, handler);

  // R1 — every non-variant wire: certified valid frame + certified meta → registered + schemaOk + verdict passed.
  let ok = 0; const miss = [];
  for (const wire of WIRES) {
    tr.length = 0;
    wrapFor(wire, () => undefined).call({}, certFrame(CERT[wire]), certFrame({ scope: 'aa1' }));
    const r = tr[0];
    if (tr.length === 1 && r.type === reg.wiring.get(wire).type && r.registered === true && r.schemaOk === true && r.verdict === 'passed' && r.faults == null) ok++;
    else miss.push(`${wire}:${JSON.stringify(r)}`);
  }
  check(`R1. standalone evaluator: all ${WIRES.length} non-variant wires observed registered + schemaOk, verdict preserved`, ok === WIRES.length, `\n   ${miss.join('\n   ')}`);

  // R2 — async handler: the returned Promise is passed through UNTOUCHED; sync verdict is inert 'object'.
  tr.length = 0;
  const pPass = Promise.resolve(7);
  const retA = wrapFor('hello-sig', () => pPass).call({}, certFrame(CERT['hello-sig']), certFrame({ scope: 'aa1' }));
  check('R2. async handler: returned Promise passed through by identity; inert sync verdict object', retA === pPass && tr.length === 1 && tr[0].verdict === 'object');

  // R3 — rejecting Promise: same object returned, verdict inert object, caller still owns the rejection.
  tr.length = 0;
  const pRej = Promise.reject(new Error('nack'));
  const retR = wrapFor('hello-sig', () => pRej).call({}, certFrame(CERT['hello-sig']), certFrame({ scope: 'aa1' }));
  let caught = false; try { await retR; } catch { caught = true; }
  check('R3. rejecting Promise: same object returned, verdict object, caller still owns the rejection', retR === pRej && caught && tr[0].verdict === 'object');

  // R4 — synchronous throw: rethrown to the caller; verdict threw.
  tr.length = 0;
  let sthrew = false; try { wrapFor('hello-sig', () => { throw new Error('boom'); }).call({}, certFrame(CERT['hello-sig']), certFrame({ scope: 'aa1' })); } catch { sthrew = true; }
  check('R4. sync throw: rethrown to caller AND verdict threw', sthrew && tr.length === 1 && tr[0].verdict === 'threw');

  // R5 — schema-invalid: a hello-sig missing sig → registered, schemaOk=false, schema fault, handler still ran.
  tr.length = 0;
  let ran = false;
  wrapFor('hello-sig', () => { ran = true; }).call({}, certFrame({ proto: 'axona/5', nodeId: NODEID, pubkey: PUBKEY, pow: '' }), certFrame({ scope: 'aa1' }));
  check('R5. schema-invalid: hello-sig without sig → registered, schemaOk=false, schema fault, handler ran',
    ran && tr.length === 1 && tr[0].registered === true && tr[0].schemaOk === false && (tr[0].faults || []).some((f) => f.startsWith('schema:')));

  // R6 — the unbranded floor: an uncertified LIVE frame is never reflected on.
  tr.length = 0;
  const retU = wrapFor('hello-sig', () => 'handled').call({}, { proto: 'axona/5', nodeId: NODEID }, {});
  check('R6. uncertified live frame: handler verbatim + unbranded-source (no reflection)',
    retU === 'handled' && tr.length === 1 && tr[0].registered === null && (tr[0].faults || []).includes('unbranded-source') && tr[0].verdict === 'unobserved');
}

// ── V. VARIANT SELECTION (signal payload.kind) ────────────────────────
{
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary3Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  const vb = reg.wiring.get('signal').variantBy;
  const wrapSignal = (body) => { tr.length = 0; reg.wrap('mesh:signal', () => undefined, { variantBy: vb }).call({}, certFrame(body), certFrame({ scope: 'aa1' })); return tr[0]; };

  const rOffer = wrapSignal(SIGNAL.offer);
  check('V1. signal kind=sdp-offer → variant offer, registered + schemaOk, verdict passed', rOffer.variant === 'offer' && rOffer.registered === true && rOffer.schemaOk === true && rOffer.verdict === 'passed');

  const rAnswer = wrapSignal(SIGNAL.answer);
  check('V2. signal kind=sdp-answer → variant answer, registered + schemaOk', rAnswer.variant === 'answer' && rAnswer.registered === true && rAnswer.schemaOk === true);

  const rCand = wrapSignal(SIGNAL.candidate);
  check('V3. signal kind=ice → variant candidate, registered + schemaOk', rCand.variant === 'candidate' && rCand.registered === true && rCand.schemaOk === true);

  const rBogus = wrapSignal({ kind: 'bogus-kind', sdp: SDP });
  check('V4. signal kind=unknown → no known variant selected (not registered as offer/answer/candidate)',
    rBogus.variant == null || rBogus.registered !== true, `\n   ${JSON.stringify(rBogus)}`);
}

// ── D. FLAG-OFF IDENTITY (inert wrap) ─────────────────────────────────
{
  const tr = [];
  const reg = buildBoundary3Registry({ sink: (rec) => tr.push(rec) });   // honors the GLOBAL runtime flag
  setShadowEnabled(false);

  tr.length = 0;
  const ret = reg.wrap('mesh:hello-sig', () => 'verbatim').call({}, certFrame(CERT['hello-sig']), certFrame({ scope: 'aa1' }));
  check('D1. flag-off: handler runs verbatim (return identical) AND ZERO traces emitted (inert wrap)', ret === 'verbatim' && tr.length === 0);

  tr.length = 0;
  let threw = false; try { reg.wrap('mesh:hello-sig', () => { throw new Error('x'); }).call({}, certFrame(CERT['hello-sig']), certFrame({ scope: 'aa1' })); } catch { threw = true; }
  check('D2. flag-off: a throwing handler still rethrows verbatim AND ZERO traces', threw && tr.length === 0);
}

// ── O. OBSERVE UNIT (makeBoundary3Observers side-channel) ─────────────
{
  { const tr = []; const { observe } = makeBoundary3Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(false);
    observe('hello-sig', 'aa1', Object.freeze({ ...CERT['hello-sig'] }));   // frozen: proves observe never mutates input
    check('O1. observe() flag-off: ZERO traces + input untouched (byte-identical, no certify work)', tr.length === 0); }

  { const tr = []; const { observe } = makeBoundary3Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(true);
    observe('hello-sig', 'aa1', CERT['hello-sig']);
    check('O2. observe() flag-on: branded mesh:hello-sig, schemaOk, verdict UNOBSERVED (no handler claim), scope stamped',
      tr.length === 1 && tr[0].type === 'mesh:hello-sig' && tr[0].registered === true && tr[0].schemaOk === true && tr[0].verdict === 'unobserved' && tr[0].scope === 'aa1'); }

  { const tr = []; const { observe } = makeBoundary3Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(true);
    observe('signal', 'aa1', SIGNAL.offer);   // the observe site passes frame.payload; kind is a top-level leaf
    check('O3. observe() signal: variant offer selected via payload.kind, verdict UNOBSERVED, scope stamped',
      tr.length === 1 && tr[0].type === 'mesh:signal' && tr[0].variant === 'offer' && tr[0].verdict === 'unobserved' && tr[0].scope === 'aa1'); }

  { const { observe } = makeBoundary3Observers({});
    setShadowEnabled(true);
    let obsThrew = false; const cyc = {}; cyc.self = cyc;   // cyclic → JSON.stringify throws INSIDE observe
    try { observe('hello', 'aa1', cyc); } catch { obsThrew = true; }
    check('O4. observe() never throws OUT — a cyclic/malformed body is swallowed (transport unaffected)', obsThrew === false); }
}

setShadowEnabled(false);
console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
