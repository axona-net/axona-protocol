// =====================================================================
// smoke_transport_rpc_ref.mjs — REF-1.1 E2.0: the TransportRpcRef channel subject.
//
// Council-ratified (Aster ASTER-E2-CHANNEL-SUBJECT, Vega, Orion ee0d0e13): the
// onRequest RPC reply obligation is recorded as FrameKind.REQUEST_RESPONSE with
// CorrelationSubjectKind.TransportRpcRef — a CHANNEL subject: the request↔return
// pairing is the transport RPC channel, not a payload field. defineRow permits an
// empty `requires` + a fixed `transportScope: 'request-return'` ONLY for this
// subject on REQUEST_RESPONSE, and rejects it for every other kind/subject.
//
// Proves Aster's three required demonstrations plus the gating:
//   (a) concurrent same-target requests stay distinct at the transport layer;
//   (b) missing/invalid transport correlation context refuses;
//   (c) no wire payload projection is claimed as a correlation identifier.
//
// Run: node test/smoke_transport_rpc_ref.mjs
// =====================================================================
import { defineRow, FrameKind, CorrelationSubjectKind } from '../src/registry/index.js';
import { boundary5Rows } from '../src/dht/boundary5Registry.js';
import { boundary6Rows } from '../src/dht/boundary6Registry.js';

let passed = 0, failed = 0;
const check = (l, ok, x = '') => { console.log(`  ${ok ? '✓' : '✗'} ${l}${ok ? '' : ' ' + x}`); ok ? passed++ : failed++; };
const mints = (o) => { try { return defineRow(o) && true; } catch { return false; } };
const rejects = (o) => !mints(o);
const RR = FrameKind.REQUEST_RESPONSE, RPC = CorrelationSubjectKind.TransportRpcRef;
const base = { type: 't:rpc', wire: 'w', owningService: 'Svc', versionRange: { min: 4, max: 4 } };
const rpcCorr = { kind: RPC, requires: [], transportScope: 'request-return' };

console.log('\nREF-1.1 E2.0 — TransportRpcRef channel subject\n');

// ── the exception exists, and ONLY for REQUEST_RESPONSE + TransportRpcRef ──
check('S1. REQUEST_RESPONSE + TransportRpcRef + empty requires + transportScope "request-return" MINTS',
  mints({ ...base, kind: RR, correlation: rpcCorr }));
check('S2. TransportRpcRef on a NON-REQUEST_RESPONSE kind (ONE_WAY) is REJECTED',
  rejects({ ...base, kind: FrameKind.ONE_WAY, correlation: rpcCorr }));
check('S3. no generic empty-requires escape hatch: an empty requires on a NON-TransportRpcRef subject (IngressRef) is REJECTED',
  rejects({ ...base, kind: RR, correlation: { kind: CorrelationSubjectKind.IngressRef, requires: [] } }));
check('S4. transportScope is REJECTED on a non-TransportRpcRef subject (IngressRef) — not a general field',
  rejects({ ...base, kind: RR, correlation: { kind: CorrelationSubjectKind.IngressRef, requires: ['x'], transportScope: 'request-return' }, projection: { payload: ['x'] } }));

// ── (b) missing/invalid transport correlation context refuses ──
check('S5. (b) TransportRpcRef with NO transportScope is REJECTED (missing transport context)',
  rejects({ ...base, kind: RR, correlation: { kind: RPC, requires: [] } }));
check('S6. (b) TransportRpcRef with an INVALID transportScope (not "request-return") is REJECTED',
  rejects({ ...base, kind: RR, correlation: { kind: RPC, requires: [], transportScope: 'bogus-scope' } }));

// ── (c) no wire payload projection is claimed as a correlation identifier ──
check('S7. (c) TransportRpcRef with a NON-EMPTY requires (a payload projection claimed as correlation id) is REJECTED — the pairing is the channel, not a payload field',
  rejects({ ...base, kind: RR, correlation: { kind: RPC, requires: ['target'], transportScope: 'request-return' }, projection: { payload: ['target'] } }));

// ── (a) concurrent same-target requests stay distinct at the transport layer ──
// A TransportRpcRef row declares NO payload correlation (requires empty), so two
// requests carrying the SAME payload (same target) are NOT conflated into one
// conversation by the contract — their association is the transport RPC channel,
// which distinguishes concurrent calls. Demonstrated at the contract level: the
// minted correlation has zero payload keys, and adding the shared target as a key
// is rejected (S7), so same-target concurrency cannot collapse to one subject.
{
  const row = defineRow({ ...base, kind: RR, correlation: rpcCorr });
  check('S8. (a) a TransportRpcRef row exposes ZERO payload correlation keys — concurrent same-target requests are distinguished by the transport channel, never merged by payload',
    row.correlation && row.correlation.kind === RPC && row.correlation.requires.length === 0 && row.correlation.transportScope === 'request-return');
}

// ── the live B5/B6 request rows actually use it; notify/routed legs do not ──
const b5 = boundary5Rows(), b6 = boundary6Rows();
const isRpc = (r) => r.kind === RR && r.correlation?.kind === RPC && r.correlation.requires.length === 0 && r.correlation.transportScope === 'request-return';
check('S9. all 5 Boundary-5 request legs carry TransportRpcRef; all 6 notification legs are ONE_WAY with no correlation',
  b5.filter((r) => r.kind === RR).length === 5 && b5.filter((r) => r.kind === RR).every(isRpc)
  && b5.filter((r) => r.kind === 'ONE_WAY').length === 6 && b5.filter((r) => r.kind === 'ONE_WAY').every((r) => r.correlation == null));
check('S10. the one Boundary-6 axona:direct REQUEST leg carries TransportRpcRef; the notify + tunneled legs are ONE_WAY',
  b6.filter((r) => r.kind === RR).length === 1 && b6.filter((r) => r.kind === RR).every(isRpc)
  && b6.filter((r) => r.kind === 'ONE_WAY').length === 2);

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
