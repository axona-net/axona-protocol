// smoke_cap_attest.mjs — the authenticated capability oracle (4.62.2, R13/R15/
// R17). Pins the byte-exact CAP_ATTEST transcript, the deterministic golden
// signature, the locally-derived cbvDigest (with per-transport fixtures), and
// the two rejection properties that matter: key binding (a proof verifies only
// under the base-authenticated peer key, never a frame-supplied one — R15) and
// channel freshness (a prior-channel frame replayed after reconnect fails
// because the verifier derives a different digest — R17).
//
// Run: node test/smoke_cap_attest.mjs
import {
  WRITE_FLIGHT_ACK_V1, CAP_DOMAIN,
  cbvDigest, buildCapTranscript, signCapAttest, verifyCapAttest,
} from '../src/pubsub/capAttest.js';
import { bytesToHex } from '../src/pubsub/ackProof.js';
import { getPublicKeyAsync, signAsync } from '../src/crypto/noble-ed25519.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

// Independent SHA-256(UTF-8(s)) → hex, to check cbvDigest without trusting it.
const _enc = new TextEncoder();
async function sha256HexIndep(s) {
  const buf = await crypto.subtle.digest('SHA-256', _enc.encode(s));
  return bytesToHex(new Uint8Array(buf));
}

// ── fixed key + inputs (the golden) ───────────────────────────────────
const SECRET = new Uint8Array(32);
for (let i = 0; i < 32; i++) SECRET[i] = i + 1;               // 01 02 … 20
const peerKey = await getPublicKeyAsync(SECRET);              // base-authenticated key
const signFn  = (b) => signAsync(b, SECRET);
const nodeId  = 'a'.repeat(66);                              // the attester's authenticated nodeId
const cbv     = 'n:1111:2222:node-ws';                      // a node-WS channel binding string

// Checked-in golden: fixed SECRET (01..20) + nodeId 'aa…' + cbv 'n:1111:2222:node-ws'
// → this exact signature. An independent impl of buildCapTranscript + Ed25519
// must reproduce it (R13/R15/R17 byte-exactness).
const GOLDEN_SIG = '0ee129e2fc96c4be4228368157a732792045ed0515d4607a1723f8cae442273bc2b9aa25f635d361c6227a6ba5a3a67952474bc8a7055acf94a96a7ab4ed8a05';

// ── transcript shape + domain ─────────────────────────────────────────
const digest = await cbvDigest(cbv);
ok('cbvDigest is 32 bytes', digest.length === 32, String(digest.length));
ok('cbvDigest === SHA-256(UTF-8(cbv)) independently', bytesToHex(digest) === await sha256HexIndep(cbv));
const t = buildCapTranscript(nodeId, digest);
// DOMAIN(19) + u8(1) + capId(19) + nodeId(33) + digest(32) = 104
ok('transcript is the fixed length 104', t.length === 104, String(t.length));
ok('transcript begins with DOMAIN', bytesToHex(t.slice(0, CAP_DOMAIN.length)) === bytesToHex(CAP_DOMAIN));

// ── deterministic golden signature ────────────────────────────────────
const f1 = await signCapAttest(signFn, { nodeId, cbvString: cbv });
const f2 = await signCapAttest(signFn, { nodeId, cbvString: cbv });
ok('capId is write-flight-ack-v1', f1.capId === WRITE_FLIGHT_ACK_V1);
ok('signature is deterministic', f1.sig === f2.sig);
ok('signature matches the checked-in golden (R13/R15/R17)', f1.sig === GOLDEN_SIG, f1.sig);

// ── per-transport digest fixtures (node-WS, bridge, mesh forms) ────────
for (const [label, s] of [
  ['node-WS', 'n:aaaa:bbbb:node'],
  ['bridge',  'n:aaaa:bbbb:node|conn:7f3c'],
  ['mesh',    'n:aaaa:bbbb:node|fp:AB:CD'],
]) {
  const d = await cbvDigest(s);
  ok(`${label} CBV string digests to 32 bytes == SHA-256(UTF-8)`, d.length === 32 && bytesToHex(d) === await sha256HexIndep(s));
}

// ── round-trip verify ─────────────────────────────────────────────────
ok('honest attestation verifies under the base-auth key', (await verifyCapAttest(f1, { peerKey, expectedNodeId: nodeId, cbvString: cbv })).ok === true);

// ── R15: key binding — a different signer fails under the claimed peer key ──
const SECRET_B = new Uint8Array(32).fill(9);
const fB = await signCapAttest((b) => signAsync(b, SECRET_B), { nodeId, cbvString: cbv });
ok('R15 a claim signed by a DIFFERENT identity fails under peerKey', (await verifyCapAttest(fB, { peerKey, expectedNodeId: nodeId, cbvString: cbv })).ok === false);

// ── R15: a frame carrying an extra key field cannot substitute the verifier's key ──
const withKey = { ...f1, pubkey: bytesToHex(await getPublicKeyAsync(SECRET_B)) };
ok('R15 an extra key field on the frame is ignored (still verifies under peerKey)', (await verifyCapAttest(withKey, { peerKey, expectedNodeId: nodeId, cbvString: cbv })).ok === true);
ok('R15 verifying that same frame under the WRONG peerKey fails', (await verifyCapAttest(withKey, { peerKey: await getPublicKeyAsync(SECRET_B), expectedNodeId: nodeId, cbvString: cbv })).ok === false);

// ── R17: reconnect replay — same frame, different current channel CBV, fails ──
const replay = await verifyCapAttest(f1, { peerKey, expectedNodeId: nodeId, cbvString: 'n:3333:4444:node-ws' });
ok('R17 a prior-channel frame replayed after reconnect FAILS (digest differs)', replay.ok === false && replay.reason === 'bad_signature', replay.reason);

// ── binding + shape rejections ────────────────────────────────────────
ok('nodeId mismatch (frame is not this channel peer) → rejected', (await verifyCapAttest(f1, { peerKey, expectedNodeId: 'b'.repeat(66), cbvString: cbv })).reason === 'nodeid_mismatch');
ok('wrong capId → rejected', (await verifyCapAttest({ ...f1, capId: 'other-cap' }, { peerKey, expectedNodeId: nodeId, cbvString: cbv })).reason === 'bad_capid');
ok('missing peerKey → fail-closed', (await verifyCapAttest(f1, { expectedNodeId: nodeId, cbvString: cbv })).reason === 'no_peer_key');

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
