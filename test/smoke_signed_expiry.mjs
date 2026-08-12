// smoke_signed_expiry.mjs — REF-1.1 S2.0c Phase 2: signed immutable expiry,
// the Option-1 V2 content-address + committed-deadline machinery
// (src/pubsub/signedExpiry.js) in ISOLATION. PURE + FENCED: this module does
// not change the default envelope wire path, so the running V1 network is
// unaffected and the full suite stays green; the flag-day cutover is separate.
//
// Assertions map to the accepted design's RED test matrix
// (REF-1.1-S2.0c-Signed-Expiry-Design-v6.md):
//   1 width contract (blocker 2) · 2 byte-exact preimage + V1/V2 distinctness +
//   tampered-exp · 3 D1/D2 lifetime + boundaries · 4 anonymous local path +
//   sim-relaxed profile · 5 D1 SAFE-integer boundary (Aster Phase-2 review).
// Golden preimage + digests are byte-exact constants captured from the module.
// The V1 id is cross-checked against the SHIPPED envelope.computeMsgId so the
// "byte-preserved legacy" claim fences the real V1, not duplicate logic.
// Run: node test/smoke_signed_expiry.mjs
import {
  MSGID_DOMAIN_V2, TTL_CEILING, CLOCK_SKEW, SignedExpiryError,
  assertV2Widths, msgIdV2Preimage, computeMsgIdV2, computeMsgIdV1,
  clampExp, validateExp, effectiveDeath, isBodyFresh,
} from '../src/pubsub/signedExpiry.js';
import { canonical } from '../src/pubsub/post.js';
import { computeMsgId as envelopeComputeMsgId } from '../src/pubsub/envelope.js';

let n = 0, fail = 0;
const ok  = (m, c, extra = '') => { if (c) console.log(`  ok ${++n} - ${m}`); else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const rej = async (m, code, fn) => { let e = null; try { await fn(); } catch (x) { e = x; } ok(m, e instanceof SignedExpiryError && e.code === code, e ? `got ${e.code}` : 'did not throw'); };

// Fixed golden vectors (captured from the module).
const PUB   = 'a'.repeat(64);              // 64 hex — production publisher width
const TOPIC = '89' + 'b'.repeat(64);       // 66 hex — region byte + 32
const MSG   = 'hello';
const TS    = 1_000_000_000_000, EXP = TS + TTL_CEILING;
const GOLD_PREIMAGE = '{"d":"axona:pubsub-msgid:v2","exp":1000086400000,"message":"hello","publisher":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","topicId":"89bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}';
const GOLD_V2   = '89a81457c066ef4c46eeff728e32fab275240769fd380b506c4407773d8e8687';
const GOLD_V1   = '312555f3d9a5c6ab8acab9ebaed23be85ad64d285f121889b9fb6a67b69e50b2';
const GOLD_TAMP = '89967d2c80134464b544e06898437abde8235366677a35f8fbd613a09846b913';
const GOLD_ANON = '698ef361d54ef169674b8ff85dd90b4f39e7b235c16169974bdfdfdc8d37a069';

console.log('REF-1.1 S2.0c Phase 2 — signed immutable expiry (pure, fenced)\n');

// ---- 2: byte-exact preimage + digest, V1/V2 distinct, tampered exp ----------
ok('2: V2 preimage is byte-exact (d, exp, message, publisher, topicId sorted)', msgIdV2Preimage({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP }) === GOLD_PREIMAGE);
ok('2: preimage carries the literal domain tag axona:pubsub-msgid:v2', GOLD_PREIMAGE.includes(`"d":"${MSGID_DOMAIN_V2}"`) && MSGID_DOMAIN_V2 === 'axona:pubsub-msgid:v2');
ok('2: V2 digest matches the golden', (await computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP })) === GOLD_V2);
ok('2: V1 digest matches the golden (byte-preserved legacy)', (await computeMsgIdV1({ publisher: PUB, message: MSG })) === GOLD_V1);
// Cross-check against the SHIPPED envelope.computeMsgId so "byte-preserved
// legacy" fences the REAL V1 implementation, not just duplicate local logic.
ok('2: computeMsgIdV1 == shipped envelope.computeMsgId (keyed)', (await computeMsgIdV1({ publisher: PUB, message: MSG })) === (await envelopeComputeMsgId({ publisher: PUB, message: MSG })));
ok('2: computeMsgIdV1 == shipped envelope.computeMsgId (anonymous null)', (await computeMsgIdV1({ publisher: null, message: MSG })) === (await envelopeComputeMsgId({ publisher: null, message: MSG })));
ok('2: V1 preimage has NO d field (legacy shape)', canonical({ publisher: PUB, message: MSG }) === `{"message":"hello","publisher":"${PUB}"}`);
ok('2: V1 and V2 ids DIFFER for the same {publisher, message}', GOLD_V1 !== GOLD_V2);
ok('2: a tampered exp yields a DIFFERENT id (exp is committed into the address)', (await computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP + 1 })) === GOLD_TAMP && GOLD_TAMP !== GOLD_V2);
ok('2: computeMsgIdV2 is deterministic', (await computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP })) === (await computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP })));

// ---- 1: width contract (blocker 2) — reject BEFORE hashing ------------------
ok('1: 64-hex publisher + 66-hex topicId accepted', typeof (await computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP })) === 'string');
await rej('1: 66-hex publisher rejected pre-hash (the exact v5 mistake)', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: '89' + 'a'.repeat(64), message: MSG, topicId: TOPIC, exp: EXP }));
await rej('1: 64-hex topicId rejected pre-hash', 'BAD_TOPICID_WIDTH', () => computeMsgIdV2({ publisher: PUB, message: MSG, topicId: 'b'.repeat(64), exp: EXP }));
await rej('1: upper-case publisher rejected', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: 'A'.repeat(64), message: MSG, topicId: TOPIC, exp: EXP }));
await rej('1: non-hex publisher rejected', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: 'g'.repeat(64), message: MSG, topicId: TOPIC, exp: EXP }));
await rej('1: upper-case topicId rejected', 'BAD_TOPICID_WIDTH', () => computeMsgIdV2({ publisher: PUB, message: MSG, topicId: '89' + 'B'.repeat(64), exp: EXP }));
await rej('1: non-hex topicId rejected pre-hash (v6 matrix vector)', 'BAD_TOPICID_WIDTH', () => computeMsgIdV2({ publisher: PUB, message: MSG, topicId: '89' + 'z'.repeat(64), exp: EXP }));
await rej('1: non-integer exp rejected', 'BAD_EXP', () => computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: 1.5 }));
ok('1: publisher width (64) and topicId width (66) are distinct in the golden preimage', PUB.length === 64 && TOPIC.length === 66);
// assertV2Widths is the pre-hash gate directly
{ let e = null; try { assertV2Widths(PUB, TOPIC); } catch (x) { e = x; } ok('1: assertV2Widths accepts the production widths', e === null); }
{ let e = null; try { assertV2Widths('89' + 'a'.repeat(64), TOPIC); } catch (x) { e = x; } ok('1: assertV2Widths rejects a 66-hex publisher', e && e.code === 'BAD_PUBLISHER_WIDTH'); }

// ---- 4: anonymous local path + sim-relaxed profile -------------------------
ok('4: null publisher (anonymous) is accepted and hashes', (await computeMsgIdV2({ publisher: null, message: MSG, topicId: TOPIC, exp: EXP })) === GOLD_ANON);
ok('4: anonymous id differs from the keyed id', GOLD_ANON !== GOLD_V2);
{ // sim-relaxed profile: EXPLICIT narrower widths, NOT a validation-off switch.
  const SIM = { pubWidth: 4, topicWidth: 2 };
  let e = null; try { await computeMsgIdV2({ publisher: '1234', message: MSG, topicId: 'ab', exp: EXP }, SIM); } catch (x) { e = x; }
  ok('4: sim-relaxed profile (explicit widths) accepts a truncated lowercase-hex id', e === null);
  await rej('4: production default still rejects truncated ids', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: '1234', message: MSG, topicId: 'ab', exp: EXP }));
  // Aster blocker 2: relaxed widths must NOT open the gate to arbitrary types /
  // non-hex / upper-case / wrong-length — only to a narrower fixed hex width.
  await rej('4: relaxed profile still rejects an OBJECT publisher', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: {}, message: MSG, topicId: 'ab', exp: EXP }, SIM));
  await rej('4: relaxed profile still rejects an ARRAY topicId', 'BAD_TOPICID_WIDTH', () => computeMsgIdV2({ publisher: '1234', message: MSG, topicId: ['a', 'b'], exp: EXP }, SIM));
  await rej('4: relaxed profile still rejects an UPPER-CASE id', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: '12AB', message: MSG, topicId: 'ab', exp: EXP }, SIM));
  await rej('4: relaxed profile still rejects a NON-HEX id', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: 'zzzz', message: MSG, topicId: 'ab', exp: EXP }, SIM));
  await rej('4: relaxed profile still rejects a WRONG-LENGTH id', 'BAD_PUBLISHER_WIDTH', () => computeMsgIdV2({ publisher: '123', message: MSG, topicId: 'ab', exp: EXP }, SIM));
  await rej('4: relaxed profile still rejects a NUMBER topicId', 'BAD_TOPICID_WIDTH', () => computeMsgIdV2({ publisher: '1234', message: MSG, topicId: 12, exp: EXP }, SIM));
}

// ---- 3: D1/D2 lifetime + boundaries ----------------------------------------
ok('3: clampExp(ts) = ts + TTL_CEILING', clampExp(TS) === TS + TTL_CEILING);
ok('3: validateExp accepts exp = ts + TTL_CEILING', validateExp(TS, TS + TTL_CEILING) === TS + TTL_CEILING);
await rej('3: validateExp rejects exp = ts (not strictly after)', 'EXP_NOT_AFTER_TS', () => validateExp(TS, TS));
await rej('3: validateExp rejects exp = ts - 1', 'EXP_NOT_AFTER_TS', () => validateExp(TS, TS - 1));
await rej('3: validateExp rejects exp over ts + TTL_CEILING', 'EXP_OVER_CEILING', () => validateExp(TS, TS + TTL_CEILING + 1));
ok('3: one effectiveDeath = exp + CLOCK_SKEW', effectiveDeath(EXP) === EXP + CLOCK_SKEW);
ok('3: body fresh AT exp', isBodyFresh(EXP, EXP) === true);
ok('3: body fresh AT effectiveDeath (exp + CLOCK_SKEW) boundary', isBodyFresh(EXP + CLOCK_SKEW, EXP) === true);
ok('3: body NOT fresh at effectiveDeath + 1', isBodyFresh(EXP + CLOCK_SKEW + 1, EXP) === false);

// ---- 5: D1 SAFE-integer boundary (Aster Phase-2 blocker 1) ------------------
// A value past Number.MAX_SAFE_INTEGER loses precision, so it must never be
// hashed into the identity or used as a deadline. isInteger() let it through;
// isSafeInteger() fails closed. MAX = 9007199254740991.
const MAX = Number.MAX_SAFE_INTEGER;
// Aster's exact repro: ts = MAX, exp = ts + 1 (an unsafe integer).
await rej('5: validateExp rejects exp = MAX_SAFE_INTEGER + 1 (unsafe)', 'BAD_EXP', () => validateExp(1_000_000_000_000, MAX + 1));
await rej('5: validateExp rejects ts = MAX_SAFE_INTEGER (any later exp is unsafe)', 'BAD_EXP', () => validateExp(MAX, MAX + 1));
await rej('5: computeMsgIdV2 rejects an unsafe exp before hashing', 'BAD_EXP', () => computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: MAX + 1 }));
// exp itself safe, but exp + CLOCK_SKEW would leave the safe range → fail closed.
await rej('5: computeMsgIdV2 rejects a safe exp whose exp+CLOCK_SKEW is unsafe', 'BAD_EXP', () => computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: MAX - 2 }));
await rej('5: clampExp rejects ts = MAX_SAFE_INTEGER (clamped exp overflows)', 'BAD_EXP', () => clampExp(MAX));
await rej('5: clampExp rejects an unsafe ts', 'BAD_EXP', () => clampExp(MAX + 5));
// The EXPORTED helpers fail closed themselves — a Phase 3 caller can reach
// effectiveDeath/isBodyFresh directly with an unvalidated envelope exp.
await rej('5: effectiveDeath fails closed on exp = MAX_SAFE_INTEGER (skew overflows)', 'BAD_EXP', () => effectiveDeath(MAX));
await rej('5: effectiveDeath fails closed on an unsafe exp (> MAX_SAFE_INTEGER)', 'BAD_EXP', () => effectiveDeath(MAX + 1));
await rej('5: isBodyFresh inherits the guard — isBodyFresh(MAX, MAX) rejects (not true)', 'BAD_EXP', () => isBodyFresh(MAX, MAX));
await rej('5: isBodyFresh rejects a safe now with an unsafe exp', 'BAD_EXP', () => isBodyFresh(0, MAX + 1));
// A validated max-safe deadline still behaves correctly at effectiveDeath/isBodyFresh.
const EXP_HI = MAX - CLOCK_SKEW - 1;                 // safe, and EXP_HI+CLOCK_SKEW = MAX-1 is safe
ok('5: effectiveDeath at the safe-max boundary stays exact', effectiveDeath(EXP_HI) === MAX - 1 && Number.isSafeInteger(effectiveDeath(EXP_HI)));
ok('5: isBodyFresh true AT the safe-max effectiveDeath', isBodyFresh(MAX - 1, EXP_HI) === true);
ok('5: isBodyFresh false one ms past the safe-max effectiveDeath', isBodyFresh(MAX, EXP_HI) === false);
ok('5: computeMsgIdV2 accepts the safe-max exp (hashes to a string)', typeof (await computeMsgIdV2({ publisher: PUB, message: MSG, topicId: TOPIC, exp: EXP_HI })) === 'string');

console.log(`\nRESULT: ${n} checks passed, ${fail} failed.`);
process.exit(fail ? 1 : 0);
