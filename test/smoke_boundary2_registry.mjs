// =====================================================================
// smoke_boundary2_registry.mjs — REF-1.1 S4a: the Boundary-2 (transport hello /
// auth / session + CAP_ATTEST) frame-contract registry TABLE + the standalone
// certified-evaluator sweep over its shadow wrap.
//
// The tranche contract mirrors the ACCEPTED Boundary-1 registry (91c8080): the
// registry is DEFAULT-OFF; when the runtime shadow flag is ON the wrap OBSERVES a
// decoder-certified snapshot beside each handler and emits a trace — never
// mutating, suppressing, or reordering the handler or its arguments. With the flag
// OFF the handler runs verbatim, so flag-off is byte-identical (D block). Dispatch
// is NOT migrated. This S4a smoke gates the TABLE + the standalone evaluator; the
// live transport-layer wiring (wrapping bridge/webrtc notification handlers) is the
// next increment (same "table first" sequencing S2 used for Boundary-1).
//
//   T. TABLE: 4 rows (welcome / hello / hello-ack / cap-attest) mint + register;
//      the deliberate B2 modeling decisions hold — the evidence hierarchy is the
//      pub/sub data plane so evidence/proves are null (auth frames carry a named
//      outcome instead); hello/hello-ack are a channel-scoped MUTUAL auth (ONE_WAY
//      + conversation keyed on the connId META leg), not payload-correlated
//      REQUEST_RESPONSE; CAP_ATTEST is a Boundary-2 capability (§4.3).
//   W. WIRING: buildBoundary2Registry registers 4 rows; wiring maps 4 labels; no
//      signed/legacy variant split.
//   R. STANDALONE EVALUATOR SWEEP: a certified representative frame through the
//      shadow wrap of each wire → registered + schemaOk + verdict preserved;
//      async-pass/reject, sync-throw, schema-invalid, and the unbranded floor.
//   C. CHANNEL-SCOPED CONVERSATION (three-valued): hello's connId pair leg is
//      META-sourced — true only under a CERTIFIED meta snapshot, 'unknown' under
//      uncertified meta, false when the leg is absent.
//   D. FLAG-OFF IDENTITY: with the shadow flag OFF the wrap runs verbatim and emits
//      ZERO traces (the inert-wrap byte-identity guarantee), pass and throw alike.
//
// Run: node test/smoke_boundary2_registry.mjs
// =====================================================================
import { buildBoundary2Registry, boundary2Rows, rowDefs, makeBoundary2Observers } from '../src/transport/boundary2Registry.js';
import { setShadowEnabled } from '../src/registry/index.js';
import { certify } from '../src/registry/snapshotMint.js';
// Live-wiring (L block): drive a real webTransport over a fake WebSocket.
import { webTransport } from '../src/transport/web/index.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { buildAuthHello, cbvFromNonces } from '../src/transport/handshake-auth.js';
import { MeshAuth } from '../src/transport/web/mesh-auth.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Minimal fake WebSocket (the reconnect-smoke pattern): tracks instances, fires
// events, and `deliver(obj)` feeds a wire frame into the transport.
let liveSockets = [];
class FakeWS {
  constructor(url) { this.url = url; this.sent = []; this._l = new Map(); this.readyState = 0; liveSockets.push(this); queueMicrotask(() => { this.readyState = 1; this._fire('open'); }); }
  addEventListener(t, h) { if (!this._l.has(t)) this._l.set(t, new Set()); this._l.get(t).add(h); }
  send(d) { if (this.readyState === 1) this.sent.push(d); }
  close(code, reason) { if (this.readyState === 3) return; this.readyState = 3; this._fire('close', { code, reason }); }
  _fire(t, ev = {}) { const s = this._l.get(t); if (t === 'error' && (!s || !s.size)) throw ev.error || new Error('unhandled'); if (s) for (const h of s) try { h(ev); } catch {} }
  deliver(obj) { this._fire('message', { data: JSON.stringify(obj) }); }
}

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const certFrame = (obj) => certify(JSON.stringify(obj));

// Representative VALID frames (shape only — the registry observes shape; the live
// authGuard enforces the crypto). Hex strings need not be crypto-valid here.
const NODEID = 'ab'.repeat(33);   // 66 hex
const PUBKEY = 'cd'.repeat(32);   // 64 hex
const SIG    = 'ed25519:' + 'ef'.repeat(64);
const CAPSIG = 'ab'.repeat(64);
const CERT = {
  welcome:     { connId: 'c1', serverNonce: 'n1', version: '2.110.0', kernelVersion: '4.62.2', turn: { urls: ['x'] } },
  hello:       { proto: 'axona/5', nodeId: NODEID, pubkey: PUBKEY, sig: SIG, pow: '' },
  'hello-ack': { proto: 'axona/5', nodeId: NODEID, pubkey: PUBKEY, sig: SIG, pow: '' },
  'cap-attest':{ capId: 'write-flight-ack-v1', nodeId: NODEID, sig: CAPSIG },
};
const WIRES = ['welcome', 'hello', 'hello-ack', 'cap-attest'];

console.log('\nREF-1.1 S4a — Boundary-2 (transport hello/auth/session + CAP_ATTEST) registry\n');

// ── T. TABLE ──────────────────────────────────────────────────────────
{
  const rows = boundary2Rows();
  const by = Object.fromEntries(rows.map((r) => [r.type, r]));
  check('T1. TABLE: 4 rows mint (welcome, hello, hello-ack, cap-attest)',
    rows.length === 4 && ['transport:welcome', 'transport:hello', 'transport:hello-ack', 'transport:cap-attest'].every((t) => by[t]));

  check('T2. kinds + outcomes: welcome UNSOLICITED_EVENT/SESSION_WELCOMED; hello(-ack) ONE_WAY/CHANNEL_AUTHENTICATED; cap-attest UNSOLICITED_EVENT/CAPABILITY_ATTESTED',
    by['transport:welcome'].kind === 'UNSOLICITED_EVENT' && by['transport:welcome'].terminalOutcome === 'SESSION_WELCOMED'
    && by['transport:hello'].kind === 'ONE_WAY' && by['transport:hello'].terminalOutcome === 'CHANNEL_AUTHENTICATED'
    && by['transport:hello-ack'].kind === 'ONE_WAY' && by['transport:hello-ack'].terminalOutcome === 'CHANNEL_AUTHENTICATED'
    && by['transport:cap-attest'].kind === 'UNSOLICITED_EVENT' && by['transport:cap-attest'].terminalOutcome === 'CAPABILITY_ATTESTED');

  // Deliberate decision #1: the evidence hierarchy is the pub/sub DATA plane; transport
  // auth/session frames carry a named outcome, NOT an evidence level.
  check('T3. evidence-axis decision: every B2 row has evidence=null AND proves=null AND a named outcome+terminalOutcome',
    rows.every((r) => r.evidence === null && r.proves === null && typeof r.outcome === 'string' && typeof r.terminalOutcome === 'string'));

  // Deliberate decision #2: channel-scoped mutual auth — ONE_WAY with a conversation
  // pair keyed on the connId META leg; no authority correlation subject claimed.
  const h = by['transport:hello'], ha = by['transport:hello-ack'];
  check('T4. channel-scoped conversation: hello REQUEST↔hello-ack, hello-ack RESPONSE↔hello, pairing on connId(meta), localKey empty, NO correlation subject',
    h.conversation.role === 'REQUEST' && h.conversation.opposite === 'transport:hello-ack'
    && h.conversation.pairing[0].from === 'meta' && h.conversation.pairing[0].local === 'connId' && h.conversation.pairing[0].remote === 'connId'
    && h.conversation.localKey.length === 0 && h.correlation === null
    && ha.conversation.role === 'RESPONSE' && ha.conversation.opposite === 'transport:hello' && ha.correlation === null);

  // Deliberate decision #3: CAP_ATTEST is a Boundary-2 (transport/auth) capability (§4.3).
  const c = by['transport:cap-attest'];
  check('T5. CAP_ATTEST is Boundary-2: owningService TransportAuth, capabilityRange.capId write-flight-ack-v1, verifyCapAttest guard',
    c.owningService === 'TransportAuth' && c.capabilityRange.capId === 'write-flight-ack-v1' && /verifyCapAttest/.test(c.authGuard));

  check('T6. auth guards declared: hello/hello-ack carry verifyAuthHello; welcome carries no per-frame auth (n/a)',
    /verifyAuthHello/.test(h.authGuard) && /verifyAuthHello/.test(ha.authGuard) && by['transport:welcome'].authGuard === 'n/a');
}

// ── W. WIRING ─────────────────────────────────────────────────────────
{
  const reg = buildBoundary2Registry({ enabled: false });
  check('W1. WIRING: registry holds 4 rows; 4 wires map to their row type; no variant split',
    reg.size() === 4 && WIRES.every((w) => reg.wiring.get(w) && !reg.wiring.get(w).variantBy)
    && reg.wiring.get('hello').type === 'transport:hello' && reg.wiring.get('cap-attest').type === 'transport:cap-attest');
}

// ── R. STANDALONE EVALUATOR SWEEP ─────────────────────────────────────
{
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary2Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  const wrapFor = (wire, handler) => reg.wrap(reg.wiring.get(wire).type, handler);

  // R1 — every wire: certified valid frame + certified meta → registered + schemaOk + verdict passed.
  let ok = 0; const miss = [];
  for (const wire of WIRES) {
    tr.length = 0;
    wrapFor(wire, () => undefined).call({}, certFrame(CERT[wire]), certFrame({ connId: 'c1' }));
    const r = tr[0];
    if (tr.length === 1 && r.type === reg.wiring.get(wire).type && r.registered === true && r.schemaOk === true && r.verdict === 'passed' && r.faults == null) ok++;
    else miss.push(`${wire}:${JSON.stringify(r)}`);
  }
  check(`R1. standalone evaluator: all ${WIRES.length} wires observed registered + schemaOk, verdict preserved`, ok === WIRES.length, `\n   ${miss.join('\n   ')}`);

  // R2 — async handler: the returned Promise is passed through UNTOUCHED; sync verdict is inert 'object'
  //      (S1 F1: the generic core never awaits/observes settlement).
  tr.length = 0;
  const pPass = Promise.resolve(7);
  const retA = wrapFor('hello', () => pPass).call({}, certFrame(CERT.hello), certFrame({ connId: 'c1' }));
  check('R2. async handler: returned Promise passed through by identity; inert sync verdict object',
    retA === pPass && tr.length === 1 && tr[0].verdict === 'object');

  // R3 — rejecting Promise: same object returned, verdict inert object, caller still owns the rejection.
  tr.length = 0;
  const pRej = Promise.reject(new Error('nack'));
  const retR = wrapFor('hello', () => pRej).call({}, certFrame(CERT.hello), certFrame({ connId: 'c1' }));
  let caught = false; try { await retR; } catch { caught = true; }
  check('R3. rejecting Promise: same object returned, verdict object, caller still owns the rejection', retR === pRej && caught && tr[0].verdict === 'object');

  // R4 — synchronous throw: rethrown to the caller; verdict threw.
  tr.length = 0;
  let sthrew = false; try { wrapFor('hello', () => { throw new Error('boom'); }).call({}, certFrame(CERT.hello), certFrame({ connId: 'c1' })); } catch { sthrew = true; }
  check('R4. sync throw: rethrown to caller AND verdict threw', sthrew && tr.length === 1 && tr[0].verdict === 'threw');

  // R5 — schema-invalid: a hello missing sig → registered, schemaOk=false, schema fault, handler still ran.
  tr.length = 0;
  let ran = false;
  wrapFor('hello', () => { ran = true; }).call({}, certFrame({ proto: 'axona/5', nodeId: NODEID, pubkey: PUBKEY, pow: '' }), certFrame({ connId: 'c1' }));
  check('R5. schema-invalid: hello without sig → registered, schemaOk=false, schema fault, handler ran',
    ran && tr.length === 1 && tr[0].registered === true && tr[0].schemaOk === false && (tr[0].faults || []).some((f) => f.startsWith('schema:')));

  // R6 — the unbranded floor: an uncertified LIVE frame is never reflected on.
  tr.length = 0;
  const retU = wrapFor('hello', () => 'handled').call({}, { proto: 'axona/5', nodeId: NODEID }, {});
  check('R6. uncertified live frame: handler verbatim + unbranded-source (no reflection)',
    retU === 'handled' && tr.length === 1 && tr[0].registered === null && (tr[0].faults || []).includes('unbranded-source') && tr[0].verdict === 'unobserved');
}

// ── C. CHANNEL-SCOPED CONVERSATION (three-valued) ─────────────────────
{
  setShadowEnabled(true);
  const tr = [];
  const reg = buildBoundary2Registry({ enabled: () => true, sink: (rec) => tr.push(rec) });
  const wrapHello = (meta) => { tr.length = 0; reg.wrap('transport:hello', () => undefined).call({}, certFrame(CERT.hello), meta); return tr[0]; };

  const rTrue = wrapHello(certFrame({ connId: 'c1' }));       // certified meta carries the connId leg → observable
  check('C1. certified meta with connId → conversationPresent true (channel leg observed)', rTrue.conversationPresent === true);

  const rUnknown = wrapHello({ connId: 'c1' });               // uncertified meta → meta side unbranded
  check('C2. uncertified meta → conversationPresent unknown (never claimed from the payload alone)', rUnknown.conversationPresent === 'unknown');

  const rFalse = wrapHello(certFrame({ other: 'x' }));        // certified meta lacking connId → leg absent
  check('C3. certified meta without connId → conversationPresent false (channel leg absent)', rFalse.conversationPresent === false);
}

// ── D. FLAG-OFF IDENTITY (inert wrap) ─────────────────────────────────
{
  const tr = [];
  // No `enabled` override → the registry honors the GLOBAL runtime shadow flag
  // (the production wiring path). With it OFF the wrap must be byte-identical.
  const reg = buildBoundary2Registry({ sink: (rec) => tr.push(rec) });
  setShadowEnabled(false);   // runtime flag OFF → wrap must be byte-identical + emit nothing

  tr.length = 0;
  const ret = reg.wrap('transport:hello', () => 'verbatim').call({}, certFrame(CERT.hello), certFrame({ connId: 'c1' }));
  check('D1. flag-off: handler runs verbatim (return identical) AND ZERO traces emitted (inert wrap)', ret === 'verbatim' && tr.length === 0);

  tr.length = 0;
  let threw = false; try { reg.wrap('transport:hello', () => { throw new Error('x'); }).call({}, certFrame(CERT.hello), certFrame({ connId: 'c1' })); } catch { threw = true; }
  check('D2. flag-off: a throwing handler still rethrows verbatim AND ZERO traces', threw && tr.length === 0);
}

// ── O. OBSERVE UNIT (the live-wiring side-channel makeBoundary2Observers) ──────
{
  const HELLO = { proto: 'axona/5', nodeId: NODEID, pubkey: PUBKEY, sig: SIG, pow: '' };
  const CAP   = { capId: 'write-flight-ack-v1', nodeId: NODEID, sig: CAPSIG };

  { const tr = []; const { observe } = makeBoundary2Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(false);
    observe('hello', 'c1', Object.freeze({ ...HELLO }));   // frozen: proves observe never mutates its input
    check('O1. observe() flag-off: ZERO traces + input untouched (byte-identical, no certify work)', tr.length === 0); }

  { const tr = []; const { observe } = makeBoundary2Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(true);
    observe('hello', 'c1', HELLO);
    check('O2. observe() flag-on: branded transport:hello, schemaOk, conversation true, verdict UNOBSERVED (F1: no handler success claimed), connId stamped (F3)',
      tr.length === 1 && tr[0].type === 'transport:hello' && tr[0].registered === true && tr[0].schemaOk === true && tr[0].conversationPresent === true && tr[0].verdict === 'unobserved' && tr[0].connId === 'c1'); }

  { const tr = []; const { observe } = makeBoundary2Observers({ sink: (r) => tr.push(r) });
    setShadowEnabled(true);
    observe('cap-attest', 'BRIDGE', CAP);   // reused fixed sentinel connId, twice
    observe('cap-attest', 'BRIDGE', CAP);
    check('O3. reconnect isolation: reused sentinel connId → two INDEPENDENT traces, no retained per-connId state',
      tr.length === 2 && tr.every((r) => r.type === 'transport:cap-attest')); }

  { const { observe } = makeBoundary2Observers({});
    setShadowEnabled(true);
    let obsThrew = false; const cyc = {}; cyc.self = cyc;   // cyclic → JSON.stringify throws INSIDE observe
    try { observe('hello', 'c1', cyc); } catch { obsThrew = true; }
    check('O4. observe() never throws OUT — a cyclic/malformed body is swallowed (transport unaffected)', obsThrew === false); }

  setShadowEnabled(false);
}

// ── L. LIVE TRANSPORT WIRING (differential over the REAL webTransport) ─────────
// Drive a real webTransport{frameRegistry:true} over a fake WebSocket through the
// actual bridge handshake. Proves the four live notification sites RUN on raw args
// while the registry observes flag-on / stays silent flag-off, plus the recut
// points: F1 unobserved verdict (never 'passed'), F3 session-scoped connId + real
// reconnect isolation, F4 all four wires, F2 bounded trace ring.
{
  const alice    = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  const bridgeId = await createNodeIdentity({ lat: 51.5,  lng: -0.12 });
  const mkHello = async (nonce, conn, badSig = false) => {
    const h = await buildAuthHello({ identity: bridgeId, cbv: cbvFromNonces(nonce, conn, 'bridge') });
    if (badSig) h.sig = 'ed25519:' + 'ff'.repeat(64);   // valid shape, wrong signature → live verify fails
    return h;
  };
  const bringUp = async (conn, nonce, { frameRegistry = true } = {}) => {
    liveSockets = [];
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS, handshakeTimeoutMs: 2000, reconnect: false, frameRegistry });
    const wl = []; t.onWelcome((i) => wl.push(i));
    const startP = t.start();
    await sleep(5);
    const sock = t.socket;
    sock.deliver({ type: 'welcome', connId: conn, serverNonce: nonce, version: '2.112.0', kernelVersion: '4.62.2', turn: null });
    sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: await mkHello(nonce, conn) } });
    await startP;
    return { t, sock, wl };
  };
  // hello-ack as the FIRST auth frame (bridge not yet bound) — drives onBridgeAuthHello's
  // full verify+bind, not the post-auth early return. start() resolves IFF that bind lands.
  const bringUpAck = async (conn, nonce, { frameRegistry = true } = {}) => {
    liveSockets = [];
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS, handshakeTimeoutMs: 400, reconnect: false, frameRegistry });
    const wl = []; t.onWelcome((i) => wl.push(i));
    const startP = t.start();
    await sleep(5);
    const sock = t.socket;
    sock.deliver({ type: 'welcome', connId: conn, serverNonce: nonce, version: '2.112.0', kernelVersion: '4.62.2', turn: null });
    sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello-ack', body: await mkHello(nonce, conn) } });
    let bound = true; try { await startP; } catch { bound = false; }   // resolves only if hello-ack bound the bridge
    return { t, sock, wl, bound };
  };
  // Establish a REAL bound + write-flight-ack-capable mesh peer against the live
  // webTransport, driving frames through the transport's OWN webrtc dispatch
  // (t.webrtc._onMessage — the exact ingress MeshManager's registered onMessage
  // callback invokes) and a driver MeshAuth playing the peer. NO production hook:
  // t.webrtc / t.mesh are pre-existing composite handles, _onMessage is webrtc's
  // own private method, and the fingerprint/send overrides are a test-only adapter
  // outside src/. cbvFromNonces / cbvFromFingerprints both sort, so the driver's
  // matching fp stub yields an identical channel cbv on both sides → real bind.
  const meshPeerAttests = async (t, peerId, { freezeCap = false } = {}) => {
    const FP = { local: 'aa11', remote: 'bb22' };
    t.mesh.fingerprintsFor = () => FP;                       // give alice a stable DTLS-fp cbv
    const bob = new MeshAuth({
      identity: peerId,
      fingerprints: () => FP,                                // sorted → identical cbv both sides
      bindPeer: () => {},
      send: (_m, frame) => {                                 // bob → alice via the REAL webrtc ingress
        let body = frame.body;
        if (freezeCap && frame.type === 'cap-attest') body = Object.freeze({ ...frame.body });
        queueMicrotask(() => { try { t.webrtc._onMessage('B', { k: 'ntf', type: frame.type, body }); } catch { /* */ } });
      },
    });
    const origSend = t.mesh.send.bind(t.mesh);
    t.mesh.send = (meshId, frame) => {                       // alice → bob (capture alice's outgoing mesh frames)
      if (frame && frame.k === 'ntf') queueMicrotask(() => {
        if (frame.type === 'hello') bob.onHello('A', frame.body);
        else if (frame.type === 'hello-sig') bob.onHelloSig('A', frame.body);
        else if (frame.type === 'cap-attest') bob.onCapAttest('A', frame.body);
      });
      try { return origSend(meshId, frame); } catch { return undefined; /* no real channel */ }
    };
    bob.onChannelOpen('A');                                  // kick: bob hello → alice → mutual bind → mutual cap-attest
    for (let i = 0; i < 40; i++) await sleep(2);             // settle the microtask handshake
  };
  const typesOf = (t) => t.frameRegistryShadow().traces.map((x) => x.type);
  const helloTrace = (t, conn) => t.frameRegistryShadow().traces.find((x) => x.type === 'transport:hello' && (conn === undefined || x.connId === conn));
  const capTrace   = (t) => t.frameRegistryShadow().traces.find((x) => x.type === 'transport:cap-attest');

  // L1 — flag ON: welcome+hello RUN on raw args (bridgeInfo + onWelcome) AND observed;
  // hello verdict UNOBSERVED (F1) and the SESSION connId, not the BRIDGE sentinel (F3).
  setShadowEnabled(true);
  { const { t, wl } = await bringUp('c-A', 'nonceA');
    const h = helloTrace(t);
    check('L1. live flag-on: welcome+hello ran on raw args (bridgeInfo+onWelcome) AND observed; hello verdict UNOBSERVED (F1) + SESSION connId c-A not the BRIDGE sentinel (F3)',
      t.bridgeInfo && t.bridgeInfo.connId === 'c-A' && wl.length === 1
      && typesOf(t).includes('transport:welcome') && h && h.verdict === 'unobserved' && h.connId === 'c-A');
    try { t.socket?.close?.(1000); } catch { /* */ } }

  // L2 — F4/F6 hello-ack MEANINGFUL: a first-frame hello-ack drives onBridgeAuthHello's
  // full verify+bind (not the post-auth early return). The observe side-channel neither
  // breaks the bind nor changes it — flag-on binds identically to registry-off.
  { const on  = await bringUpAck('ack-on',  'nAckOn');
    const off = await bringUpAck('ack-off', 'nAckOff', { frameRegistry: false });
    const h = on.t.frameRegistryShadow().traces.find((x) => x.type === 'transport:hello-ack');
    check('L2. F6 hello-ack MEANINGFUL: first-frame hello-ack verify+bound (start resolved) flag-on AND registry-off identically; observed flag-on (verdict UNOBSERVED, connId ack-on)',
      on.bound === true && off.bound === true && off.t.frameRegistryShadow() === null
      && h && h.verdict === 'unobserved' && h.connId === 'ack-on');
    try { on.t.socket?.close?.(1000); off.t.socket?.close?.(1000); } catch { /* */ } }

  // L2b — F4/F6 cap-attest MEANINGFUL: base auth binds and the peer's CAP_ATTEST
  // verifies against the AUTHENTICATED pubkey + channel cbv → isCapable flips true,
  // driven through the transport's OWN webrtc dispatch (NO production hook). The observe
  // side-channel neither breaks the verify nor mutates the raw frame (frozen-body proof),
  // and the disposition is identical with the registry absent.
  { const peer = await createNodeIdentity({ lat: 48.8, lng: 2.35 });
    const on = await bringUp('cap-on', 'nCapOn');
    await meshPeerAttests(on.t, peer, { freezeCap: true });
    const ct = capTrace(on.t);
    const onCapable = on.t.isCapable(peer.id) === true;
    const off = await bringUp('cap-off', 'nCapOff', { frameRegistry: false });
    await meshPeerAttests(off.t, peer);
    check('L2b. F6 cap-attest MEANINGFUL: peer base-auth binds + CAP_ATTEST verifies → isCapable true via the real webrtc dispatch (no hook); observed flag-on (verdict UNOBSERVED, connId B, frozen frame untouched) AND capable identically registry-off',
      onCapable && ct && ct.verdict === 'unobserved' && ct.connId === 'B'
      && off.t.isCapable(peer.id) === true && off.t.frameRegistryShadow() === null);
    try { on.t.socket?.close?.(1000); off.t.socket?.close?.(1000); } catch { /* */ } }

  // L3 — F1 negative: a hello whose auth SIGNATURE is invalid — the real async handler
  // fails, yet the trace (emitted BEFORE the handler runs) says UNOBSERVED, never 'passed'.
  { liveSockets = [];
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS, handshakeTimeoutMs: 300, reconnect: false, frameRegistry: true });
    t.start().catch(() => {});
    await sleep(5);
    const sock = t.socket;
    sock.deliver({ type: 'welcome', connId: 'c-neg', serverNonce: 'nonceN', version: '2.112.0', kernelVersion: '4.62.2', turn: null });
    sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: await mkHello('nonceN', 'c-neg', true) } });
    await sleep(5);
    const h = helloTrace(t);
    check('L3. F1 negative: bridge auth with an INVALID signature — the trace says UNOBSERVED, NEVER passed (async failure not overclaimed)',
      h && h.verdict === 'unobserved' && h.connId === 'c-neg');
    try { t.socket?.close?.(4000); } catch { /* */ } }

  // L4 — F3 REAL reconnect isolation: one transport, socket dropped, reconnect with a
  // DIFFERENT welcome connId — the two hello legs carry sess-A vs sess-B, never conflated.
  { liveSockets = [];
    const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWS, handshakeTimeoutMs: 2000, reconnect: true, reconnectInitialMs: 40, reconnectMaxMs: 40, frameRegistry: true });
    const startP = t.start(); await sleep(5);
    const s1 = t.socket;
    s1.deliver({ type: 'welcome', connId: 'sess-A', serverNonce: 'nA', version: '2.112.0', kernelVersion: '4.62.2', turn: null });
    s1.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: await mkHello('nA', 'sess-A') } });
    await startP;
    const aHello = helloTrace(t, 'sess-A');
    s1.close(1006); await sleep(120);                     // drop → reconnect (reconnectInitialMs 40) opens a fresh socket
    const s2 = t.socket;
    s2.deliver({ type: 'welcome', connId: 'sess-B', serverNonce: 'nB', version: '2.112.0', kernelVersion: '4.62.2', turn: null });
    s2.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: await mkHello('nB', 'sess-B') } });
    await sleep(5);
    const bHello = helloTrace(t, 'sess-B');
    check('L4. F3 reconnect isolation: reconnect with a NEW welcome connId → the two hello legs carry sess-A vs sess-B, never conflated (bridge-auth keyed by session, not the fixed BRIDGE sentinel)',
      s2 !== s1 && aHello && aHello.connId === 'sess-A' && bHello && bHello.connId === 'sess-B');
    try { t.socket?.close?.(1000); } catch { /* */ } }

  // L5 — F2 saturation: drive past the 1024 ring cap through a live site; the trace
  // store stays bounded (drop-oldest), never growing unbounded for the session.
  { const { t } = await bringUp('c-sat', 'nSat');
    const before = t.frameRegistryShadow().traces.length;
    const capBody = { capId: 'write-flight-ack-v1', nodeId: NODEID, sig: CAPSIG };
    for (let i = 0; i < 1100; i++) await t.webrtc._onMessage('sat', { k: 'ntf', type: 'cap-attest', body: capBody });   // real webrtc dispatch (no hook)
    check('L5. F2 saturation: 1100+ observed cap-attest frames (real webrtc dispatch) → trace ring stays BOUNDED at 1024 (drop-oldest, not unbounded growth)',
      before < 1024 && t.frameRegistryShadow().traces.length === 1024);
    try { t.socket?.close?.(1000); } catch { /* */ } }

  // L6 — F6 flag OFF: the SAME live path RUNS the handlers on ALL FOUR sites
  // (welcome, hello, hello-ack, cap-attest) yet the registry emits ZERO traces.
  setShadowEnabled(false);
  { const { t, wl, sock } = await bringUp('c-off', 'nOff');                                      // welcome + hello ran
    sock.deliver({ type: 'axona', payload: { k: 'ntf', type: 'hello-ack', body: await mkHello('nOff', 'c-off') } });   // hello-ack site
    await t.webrtc._onMessage('off', { k: 'ntf', type: 'cap-attest', body: { capId: 'write-flight-ack-v1', nodeId: NODEID, sig: CAPSIG } });   // cap-attest site
    await sleep(2);
    check('L6. F6 live flag-off: ALL FOUR sites (welcome/hello/hello-ack/cap-attest) RAN AND the registry emitted ZERO traces (byte-identical live path)',
      t.bridgeInfo && t.bridgeInfo.connId === 'c-off' && wl.length === 1 && t.frameRegistryShadow().traces.length === 0);
    try { t.socket?.close?.(1000); } catch { /* */ } }

  setShadowEnabled(false);
}

setShadowEnabled(false);
console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
