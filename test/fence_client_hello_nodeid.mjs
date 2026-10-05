// =====================================================================
// fence_client_hello_nodeid.mjs — Hold-and-Fill v0.5 (axona-docs 4334504),
// repair row 2b: the client-hello carries the hex nodeId this peer will
// authenticate as, so the bridge's same-region anchor affinity (bridge row
// 2, axona-bridge 40997fc) has a region for the newcomer at admission, which
// is BEFORE hello-ack binds the real one.
//
// What this fences:
//   1. The first frame on socket open is client-hello and its `nodeId` equals
//      identity.id exactly (66 lowercase hex), beside the existing version,
//      wireVersion and kernelVersion fields.
//   2. Reconnect re-sends it with the same nodeId (the identity does not
//      change across a socket).
//   3. The Boundary-4 registry row for client-hello projects nodeId, types it
//      as a string, and does NOT require it: a frame without nodeId (every
//      kernel at or below 4.102.0) is schema-valid; a frame with a non-string
//      nodeId is not.
//
// With the row-2b field removed from web/index.js, checks 1 and 2 fail; with
// nodeId added to the row's `require`, check 3's old-client case fails.
//
// Run: node test/fence_client_hello_nodeid.mjs
// =====================================================================
import { webTransport }       from '../src/transport/web/index.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { KERNEL_VERSION }     from '../src/transport/handshake.js';
import { rowDefs }            from '../src/transport/boundary4Registry.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const tick = async () => { await new Promise(r => queueMicrotask(r)); await new Promise(r => setTimeout(r, 0)); };

// Minimal fake WebSocket, as test/smoke_transport_web_full.js uses: opens on
// the next microtask, records every send.
class FakeWebSocket {
  constructor(url) {
    this.url = url; this.sent = []; this._listeners = new Map(); this.readyState = 0;
    queueMicrotask(() => { this.readyState = 1; this._fire('open'); });
  }
  addEventListener(type, handler) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(handler);
  }
  send(data) { if (this.readyState !== 1) throw new Error('socket not open'); this.sent.push(data); }
  close() { this.readyState = 3; this._fire('close'); }
  _fire(type, ev = {}) { const s = this._listeners.get(type); if (s) for (const h of s) { try { h(ev); } catch {} } }
}

(async () => {
  console.log('fence_client_hello_nodeid: row 2b — client-hello carries the nodeId the peer will authenticate as');
  const alice = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  check('setup: identity.id is 66 lowercase hex', /^[0-9a-f]{66}$/.test(alice.id));

  // 1. First frame.
  const t = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWebSocket, handshakeTimeoutMs: 500 });
  const startP = t.start().catch(() => {});   // the fake bridge never answers; the timeout is not the subject
  await tick();
  const first = t.socket?.sent?.[0] ? JSON.parse(t.socket.sent[0]) : null;
  check('1 first frame is client-hello', first?.type === 'client-hello', JSON.stringify(first));
  check('1 client-hello.nodeId equals identity.id', first?.nodeId === alice.id, String(first?.nodeId));
  check('1 existing fields still present', typeof first?.version === 'string' && typeof first?.wireVersion === 'string' && first?.kernelVersion === KERNEL_VERSION);

  // 2. Reconnect re-sends with the same nodeId.
  const sock1 = t.socket;
  sock1.close();
  await tick();
  // scheduleReconnect uses a backoff timer; drive it by waiting a little.
  let sock2 = null;
  for (let i = 0; i < 40 && !sock2; i++) { await new Promise(r => setTimeout(r, 50)); if (t.socket && t.socket !== sock1) sock2 = t.socket; }
  const re = sock2?.sent?.[0] ? JSON.parse(sock2.sent[0]) : null;
  check('2 reconnect re-sent client-hello', re?.type === 'client-hello', sock2 ? JSON.stringify(re) : 'no second socket');
  check('2 reconnect nodeId unchanged', re?.nodeId === alice.id);
  await t.stop().catch(() => {});
  await startP;

  // 3. The registry row: optional, typed, projected.
  const def = rowDefs().find((d) => d?.wire === 'client-hello') ?? null;
  const projection = def?.projection?.payload ?? null;
  const schema = def?.schema ?? null;
  check('3 row found', !!def && !!schema, String(def && Object.keys(def)));
  check('3 note within the registry limit', typeof def?.note === 'string' && def.note.length <= 500, String(def?.note?.length));
  check('3 nodeId projected', Array.isArray(projection) && projection.includes('nodeId'), String(projection));
  check('3 nodeId typed string', schema?.types?.nodeId === 'string', String(schema?.types && Object.keys(schema.types)));
  check('3 nodeId NOT required (old clients admit unchanged)', Array.isArray(schema?.require) && !schema.require.includes('nodeId'), String(schema?.require));

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
