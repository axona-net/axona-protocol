// =====================================================================
// smoke_pow.js — Stage 2 proof-of-work scaffolding (E-1 placement defense /
//                publish anti-flood anchor).
//
// Verifies the mechanism works at difficulty > 0 (and is pubkey-bound), and is
// INERT (no-op) at the shipped difficulty 0 — including that the pow/signerPow
// fields travel the identity, the envelope, and the auth hello so raising
// difficulty later needs no format flag-day.
//
// Run: node test/smoke_pow.js
// =====================================================================

import { POW_DIFFICULTY, powMint, powVerify, powBits, powCalibrate } from '../src/pow/pow.js';
import { deriveIdentity }              from '../src/identity/index.js';
import { buildEnvelope, verifyEnvelope } from '../src/pubsub/envelope.js';
import { buildAuthHello, verifyAuthHello } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PK  = 'aa'.repeat(32);   // 64-hex dummy pubkeys
const PK2 = 'bb'.repeat(32);

async function run() {
  console.log('PoW scaffolding (E-1 / Stage 2)\n');

  console.log('── shipped difficulty is 0 (INERT) ──');
  check('POW_DIFFICULTY.transport === 0', POW_DIFFICULTY.transport === 0);
  check('POW_DIFFICULTY.publish === 0',   POW_DIFFICULTY.publish === 0);
  check('mint at difficulty 0 → "" (no work)',     (await powMint({ pubkeyHex: PK, role: 'transport' })) === '');
  check('verify accepts an empty nonce at 0',      (await powVerify({ pubkeyHex: PK, nonce: '', role: 'transport' })) === true);
  check('verify accepts an absent nonce at 0',     (await powVerify({ pubkeyHex: PK, role: 'transport' })) === true);
  check('verify accepts ANY nonce at 0',           (await powVerify({ pubkeyHex: PK, nonce: 'garbage', role: 'transport' })) === true);

  console.log('\n── mechanism works at difficulty > 0 ──');
  const D = 8;
  const nonce = await powMint({ pubkeyHex: PK, role: 'publish', difficulty: D });
  check('mint finds a nonce',                       typeof nonce === 'string' && nonce.length > 0);
  check('minted nonce carries ≥ D leading-zero bits', (await powBits({ pubkeyHex: PK, nonce, role: 'publish' })) >= D);
  check('verify accepts the minted nonce at D',    (await powVerify({ pubkeyHex: PK, nonce, role: 'publish', difficulty: D })) === true);
  check('verify REJECTS a wrong nonce at D',        (await powVerify({ pubkeyHex: PK, nonce: 'zzz', role: 'publish', difficulty: D })) === false);
  check('verify REJECTS an empty nonce at D',       (await powVerify({ pubkeyHex: PK, nonce: '', role: 'publish', difficulty: D })) === false);
  check('PoW is PUBKEY-BOUND (nonce invalid for another key)', (await powVerify({ pubkeyHex: PK2, nonce, role: 'publish', difficulty: D })) === false);

  console.log('\n── device calibration ──');
  const cal = await powCalibrate({ ms: 150 });
  check('calibrate reports a positive hash rate',  cal.hashesPerSec > 0);

  console.log('\n── fields travel the wire (inert at 0) ──');
  const id = await deriveIdentity({ lat: 51.5, lng: -0.12 });
  check('identity carries a pow field (=== "")',   id.pow === '');
  const env = await buildEnvelope({ topic: 'cats', message: 'hi', identity: id });
  check('signed envelope carries signerPow (=== "")', env.signerPow === '');
  check('envelope still verifies',                 (await verifyEnvelope(env))?.ok === true);
  const cbv   = 'cbv:smoke-pow';
  const hello = await buildAuthHello({ identity: id, cbv });
  check('auth hello carries a pow field (=== "")', hello.pow === '');
  check('auth hello verifies (pow no-op at 0)',    (await verifyAuthHello(hello, { cbv })).ok === true);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

run();
