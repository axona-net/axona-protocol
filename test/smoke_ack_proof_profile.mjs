// smoke_ack_proof_profile.mjs — Aster fixed-slice review (kernel 4.62.2): the
// signed D1 proof must round-trip under a SHRUNK keyspace profile using the REAL
// idHex() serializer, and under an ODD canonical-width profile. This pins the
// contract Aster's blocking finding exposed: idHex() pads every id to 66 hex (33
// bytes) UNCONDITIONALLY, so proof topicId/ackTo are a fixed 33 bytes in every
// profile — the transcript builder must accept exactly that, not a HEX_CHARS/2
// profile-derived width (which rejected the real signed path under hashBits<256).
//
// Run: node test/smoke_ack_proof_profile.mjs
import { configureKeyspace, getKeyspace, HEX_CHARS } from '../src/utils/hexid.js';
import { idHex } from '../src/pubsub/ids.js';
import { PURPOSE, OP, ID_BYTES, buildTranscript, signAckProof, verifyAckProof } from '../src/pubsub/ackProof.js';
import { getPublicKeyAsync, signAsync } from '../src/crypto/noble-ed25519.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const SECRET = new Uint8Array(32); for (let i = 0; i < 32; i++) SECRET[i] = i + 1;
const rootPub = await getPublicKeyAsync(SECRET);
const signFn  = (b) => signAsync(b, SECRET);

// A full signed round-trip using idHex-produced ids — the REAL D1 serializer
// path. Returns the verify result.
async function signedRoundTrip(topicBig, ackBig) {
  const fields = {
    purpose: PURPOSE.INGEST_ACK, op: OP.pub,
    topicId: idHex(topicBig),           // <- real serializer, pads to 66 hex
    msgId: 'b'.repeat(64),
    epoch: 3,
    attemptId: '11'.repeat(16),
    ackTo: idHex(ackBig),               // <- real serializer
    flightNonce: '22'.repeat(16),
    rootPub,
  };
  const frame = await signAckProof(signFn, fields);
  return verifyAckProof(frame);
}

async function exerciseProfile(label, hashBits) {
  configureKeyspace({ hashBits });
  const ks = getKeyspace();
  console.log(`\n── ${label}: hashBits=${ks.hashBits} idBits=${ks.idBits} HEX_CHARS=${ks.hexChars} ──`);
  // idHex is fixed 66 hex / 33 bytes regardless of profile width or parity.
  ok(`${label}: idHex pads to 66 hex despite HEX_CHARS=${ks.hexChars}`, idHex(0x1234n).length === 66);
  ok(`${label}: ID_BYTES stays 33`, ID_BYTES === 33);
  // The signed D1 path round-trips through the real idHex serializer.
  const v = await signedRoundTrip((1n << 60n) ^ 0xabcn, (1n << 55n) ^ 0xdefn);
  ok(`${label}: signed proof over idHex-produced ids verifies`, v.ok === true, v.reason || '');
  // A RAW shrunk-width id (bypassing idHex) is still rejected on width.
  const raw = 'a'.repeat(Math.max(2, ks.hexChars & ~1));   // even-length, != 66
  if (raw.length !== 66) {
    const bad = await verifyAckProof({
      purpose: PURPOSE.INGEST_ACK, op: 'pub', topicId: raw, msgId: 'b'.repeat(64),
      epoch: 3, attemptId: '11'.repeat(16), ackTo: idHex(1n), flightNonce: '22'.repeat(16),
      rootPub: (await import('../src/pubsub/ackProof.js')).bytesToHex(rootPub), sig: '00'.repeat(64),
    });
    ok(`${label}: a raw ${raw.length}-hex topicId (not via idHex) is rejected`, bad.ok === false && bad.reason === 'bad_topic', bad.reason);
  }
}

// hashBits:64 → HEX_CHARS=18 (even); the exact reproduction from Aster's review.
await exerciseProfile('shrunk-even', 64);
// hashBits:66 → ID_BITS=74 → HEX_CHARS=ceil(74/4)=19 (ODD canonical width).
await exerciseProfile('shrunk-odd', 66);
// restore production default (belt-and-braces; process exits anyway).
configureKeyspace({ hashBits: 256 });
ok('restored production HEX_CHARS=66', HEX_CHARS === 66);

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
