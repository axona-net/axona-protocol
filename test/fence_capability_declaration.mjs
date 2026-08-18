// fence_capability_declaration.mjs — Q2 point 6. Capability is declared at BUILD
// time, once and loudly, or the peer does not construct.
//
// WHY A THROW AND NOT A LOG. v4.58.0 first made a missing declaration a per-send
// 'violation': fail-closed, but an adapter that never declares would emit one
// ERROR per message forever while remaining silently uncreditable. That is a log
// storm standing in for a build error. Aster, council 2026-08-01: "not the
// once-loud declared-capability contract we approved."
//
// WHAT MADE THIS EXPENSIVE, AND WHY THE COST WAS THE POINT. Turning the throw on
// broke 45 of 127 test files, because their dht doubles did not declare. The
// tempting fix — blanket-declare `true` to make them construct — would convert
// every send from a NON-reporting double into a contract violation and mask the
// very regressions those tests exist to catch. That is the "make the doubles
// pass" mistake with the sign flipped, and it is the same error v4.57.0 made in
// the other direction. So all 82 doubles were audited by what routeMessage
// actually RESOLVES:
//
//   2 declare TRUE   — fence_syncpush_rejection returns {consumed:true};
//                      smoke_pubsub_republish delegates to the real
//                      AxonaPeer.routeMessage, which resolves a verdict.
//   80 declare FALSE — they return a push-count or undefined. That is the honest
//                      answer and it costs them nothing: a non-reporting adapter
//                      simply never credits a replica and never unpins a pin.
//
// This file guards the rule itself. Without it, someone re-adds an undeclared
// adapter and nothing objects until a durability decision is made on no evidence.
//
// Run: node test/fence_capability_declaration.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { sealTestDht } from './lib/testCapability.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const SELF = (0x87n << 248n) | 0x011n;
const base = () => ({
  getSelfId: () => SELF,
  onRoutedMessage: () => {},
  routeMessage: async () => ({ consumed: true }),
});
// E3b.4 (SEAL): the dht double opts into the mandatory capability through the
// explicit test-only helper, so the ONLY variable this fence exercises is
// verdictsSupported — not the (separate) capability-presence contract.
const build = (extra) => new AxonaManager({ dht: sealTestDht({ ...base(), ...extra }) });
const throws = (fn) => { try { fn(); return null; } catch (e) { return e; } };

console.log('capability declaration — declared at construction, once, or not at all\n');

// ── 1. UNDECLARED DOES NOT CONSTRUCT ───────────────────────────────────────
{
  const e = throws(() => build({}));
  ok('1a. an adapter with no verdictsSupported THROWS at construction — not a ' +
     'per-send log, not a degraded mode', !!e, String(e));
  ok('1b. …as a TypeError', e instanceof TypeError, e?.constructor?.name);
  ok('1c. …and the message says what to declare and why, so the fix does not ' +
     'require reading the kernel',
    /true/.test(e.message) && /false/.test(e.message) && /DECLARED, never inferred/.test(e.message),
    JSON.stringify(e.message));
  ok('1d. …and explicitly warns against declaring true to silence it, which is ' +
     'the failure mode that would mask real regressions',
    /Do NOT declare true to silence this/.test(e.message));
}

// ── 2. ONLY A BOOLEAN COUNTS AS A DECLARATION ──────────────────────────────
// Truthiness is not a declaration. A string, a number, or null all mean someone
// guessed at the shape, and guessing is the thing being removed.
{
  for (const [label, v] of [['a string', 'true'], ['a number', 1], ['null', null],
                            ['undefined', undefined], ['an object', {}]]) {
    const e = throws(() => build({ verdictsSupported: v }));
    ok(`2. ${label} is not a declaration — still throws`, !!e, `${label} was accepted`);
  }
}

// ── 3. EITHER BOOLEAN CONSTRUCTS ───────────────────────────────────────────
// Without this the rule could be satisfied by refusing everything, and "declare
// false" has to remain a first-class, unpunished answer — it is what every
// honest non-reporting adapter says.
{
  ok('3a. declaring TRUE constructs', !throws(() => build({ verdictsSupported: true })));
  ok('3b. declaring FALSE constructs — an honest admission is not an error',
    !throws(() => build({ verdictsSupported: false })));
}

// ── 4. THE PRODUCTION ADAPTER DECLARES ─────────────────────────────────────
// The one adapter that ships. If this ever regresses, every peer built by
// AxonaPeer stops constructing, so it is worth asserting where the audit can see
// it rather than discovering it at runtime.
{
  const src = await import('node:fs').then(m => m.readFileSync('src/dht/AxonaPeer.js', 'utf8'));
  ok('4a. AxonaPeer\'s dht adapter declares verdictsSupported: true — it resolves ' +
     '{consumed:…} from routeMessage',
    /verdictsSupported:\s*true/.test(src));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
