// smoke_ack_proof.mjs — the signed INGEST-ACK proof (Write-Flight Ack Routing,
// D1 + Aster R7). Pins the byte-exact transcript, the deterministic golden
// signature, and every rejection vector the design names: NACK-relabeled-as-ACK,
// tampered field, wrong field width, wrong signing key, and the hash-portion
// authority binding (R1, refined). Pure module under test — no transport.
//
// Run: node test/smoke_ack_proof.mjs
import {
  PURPOSE, OP, DOMAIN,
  buildTranscript, signAckProof, verifyAckProof, rootPubMatchesNodeHash,
  bytesToHex, hexToBytes,
} from '../src/pubsub/ackProof.js';
import { getPublicKeyAsync, signAsync } from '../src/crypto/noble-ed25519.js';
import { computeNodeIdBigInt } from '../src/identity/nodeid.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

// ── deterministic key + inputs (the golden vector) ────────────────────
// Fixed 32-byte secret → Ed25519 is deterministic (RFC 8032), so the signature
// over a fixed transcript is a checked-in constant an independent impl must
// reproduce.
const SECRET = new Uint8Array(32);
for (let i = 0; i < 32; i++) SECRET[i] = i + 1;                 // 01 02 … 20
const rootPub = await getPublicKeyAsync(SECRET);               // 32 bytes
const signFn = (bytes) => signAsync(bytes, SECRET);

// Fixed field inputs. topicId/ackTo are 66-hex (prod 264-bit width); msgId is
// 64-hex (SHA-256); attemptId/flightNonce are 32-hex (16 bytes).
const topicId     = 'a'.repeat(66);
const msgId       = 'b'.repeat(64);
const ackTo       = 'c'.repeat(66);
const attemptId   = '11'.repeat(16);
const flightNonce = '22'.repeat(16);
const epoch       = 7;

const baseFields = {
  purpose: PURPOSE.INGEST_ACK, op: OP.pub,
  topicId, msgId, epoch, attemptId, ackTo, flightNonce, rootPub,
};

// ── transcript shape ──────────────────────────────────────────────────
const t = buildTranscript(baseFields);
// DOMAIN(25)+purpose(1)+op(1)+topic(33)+msg(32)+epoch(8)+attempt(16)+ackTo(33)+nonce(16)+pub(32)
const expectedLen = 25 + 1 + 1 + 33 + 32 + 8 + 16 + 33 + 16 + 32;
ok(`transcript is the fixed length ${expectedLen}`, t.length === expectedLen, `got ${t.length}`);
ok('transcript begins with DOMAIN', bytesToHex(t.slice(0, DOMAIN.length)) === bytesToHex(DOMAIN));

// ── golden signature (deterministic) ──────────────────────────────────
// Checked-in golden: fixed SECRET (01..20) + the fixed inputs above → this exact
// signature. An independent implementation of buildTranscript + Ed25519 must
// reproduce these bytes, or it is not conformant (Aster R7).
const GOLDEN_ROOTPUB = '79b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664';
const GOLDEN_SIG     = '12e0b03bff05b44eb85cdcb2886f1ccf48ff92f8d42f5dff80442abec78091fe52f07b4e60674dc90c8f39bfd492368d84f5b8d798e884d36af5b347b299eb0b';
ok('rootPub derives from the fixed secret', bytesToHex(rootPub) === GOLDEN_ROOTPUB, bytesToHex(rootPub));

const frame1 = await signAckProof(signFn, baseFields);
const frame2 = await signAckProof(signFn, baseFields);
ok('signature is deterministic', frame1.sig === frame2.sig);
ok('signature matches the checked-in golden (R7)', frame1.sig === GOLDEN_SIG, frame1.sig);

// ── round-trip verify ─────────────────────────────────────────────────
const v = await verifyAckProof(frame1);
ok('honest proof verifies', v.ok === true, v.reason || '');
ok('verify returns decoded fields', v.ok && v.fields.msgId === msgId && v.fields.epoch === 7);

// ── R7: a RECEIPT_NACK cannot be relabeled as an INGEST_ACK ────────────
// Same signature bytes, purpose flipped 0x01→0x02. The verifier rebuilds a
// different transcript (purpose is signed), so the signature fails.
const relabeled = { ...frame1, purpose: PURPOSE.RECEIPT_NACK };
const rv = await verifyAckProof(relabeled);
ok('relabel INGEST_ACK→RECEIPT_NACK fails (R7)', rv.ok === false && rv.reason === 'bad_signature', rv.reason);

// A genuinely NACK-purpose proof, signed as such, verifies — and is a DIFFERENT
// signature from the ACK over otherwise-identical fields.
const nackFrame = await signAckProof(signFn, { ...baseFields, purpose: PURPOSE.RECEIPT_NACK });
ok('genuine RECEIPT_NACK verifies', (await verifyAckProof(nackFrame)).ok === true);
ok('ACK and NACK signatures differ over identical fields', nackFrame.sig !== frame1.sig);

// ── tamper: flip a msgId byte ─────────────────────────────────────────
const tampered = { ...frame1, msgId: 'c' + msgId.slice(1) };
ok('tampered msgId fails', (await verifyAckProof(tampered)).ok === false);

// ── tamper: flip op pub→kill ──────────────────────────────────────────
const opFlipped = { ...frame1, op: 'kill' };
ok('flipped op fails', (await verifyAckProof(opFlipped)).ok === false);

// ── width rejection: attemptId not 16 bytes ───────────────────────────
const shortAttempt = await verifyAckProof({ ...frame1, attemptId: '1234' });
ok('short attemptId rejected on width', shortAttempt.ok === false && shortAttempt.reason === 'bad_attempt', shortAttempt.reason);
const badPub = await verifyAckProof({ ...frame1, rootPub: 'ab' });
ok('short rootPub rejected on width', badPub.ok === false && badPub.reason === 'bad_rootpub', badPub.reason);

// ── wrong key: valid signature under a DIFFERENT identity ──────────────
// Sign with secret B but claim rootPub A → the signature does not verify under A.
const SECRET_B = new Uint8Array(32).fill(9);
const signB = (bytes) => signAsync(bytes, SECRET_B);
const frameB = await signAckProof(signB, baseFields);          // rootPub field is still A
ok('proof signed by a different key fails under claimed rootPub', (await verifyAckProof(frameB)).ok === false);

// ── purpose outside the closed set ────────────────────────────────────
ok('purpose 0x07 refused by builder', (() => { try { buildTranscript({ ...baseFields, purpose: 7 }); return false; } catch (e) { return e.code === 'bad_purpose'; } })());

// ── hash-portion authority binding (R1, refined) ──────────────────────
// The nodeId of rootPub at some region must hash-match on its low bits; a
// different pubkey must not.
const rootBig = await computeNodeIdBigInt(rootPub, 37.77, -122.42);
ok('rootPub hash-binds to its own nodeId', await rootPubMatchesNodeHash(rootPub, rootBig));
const otherPub = await getPublicKeyAsync(SECRET_B);
ok('a different pubkey does NOT hash-bind', !(await rootPubMatchesNodeHash(otherPub, rootBig)));
// Region prefix is irrelevant to the binding: same pubkey, different region → still binds.
const rootBigElsewhere = await computeNodeIdBigInt(rootPub, -33.86, 151.20);
ok('binding is region-independent (hash portion only)', await rootPubMatchesNodeHash(rootPub, rootBigElsewhere));

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'} — ${n} assertions, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
