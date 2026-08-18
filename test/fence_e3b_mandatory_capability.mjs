// fence_e3b_mandatory_capability.mjs — REF-1.1 E3b.4 (SEAL). The source-backed
// proof Aster's boundary ruling (39012d73 / option 1) requires for re-submission:
//
//   (1) production registerFrame carries NO literal-name dispatch fallback — no
//       recv.onRequest(...) / recv.onNotification(...) / recv.onRoutedMessage(...)
//       call survives in its source. Capability presence is mandatory.
//   (2) the CompositeTransport fan-out is cap-only — no t.onRequest(...) /
//       t.onNotification(...) literal-method fallback.
//   (3) EVERY production dispatch receiver deposits a capability at construction:
//       the five transports (sim, node-WS, webrtc, bridge, composite), the
//       AxonaPeer, and the default-DHT adapter AxonaManager registers B1 on.
//   (4) it is PAIRED with the E0 primitive-definition = 0 invariant (read from the
//       committed manifest), so "no fallback + every receiver deposits" sits on top
//       of "no receiver even defines a raw primitive".
//
// This is a SOURCE fence (it reads src/), complementary to smoke_e3a_capability_boundary
// (which proves the same seal at RUNTIME on a constructed transport). Together they are
// the E3b acceptance evidence: source has no bypass, runtime confirms the absence.
//
// Run: node test/fence_e3b_mandatory_capability.mjs
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const read = (p) => readFileSync(root + p, 'utf8');

// Strip // line comments and /* */ block comments so a pattern inside prose does not
// read as a call. Deliberately simple: the kernel uses no regex/string literals that
// would confuse this in the files under test (verified by the positive controls below).
const stripComments = (src) => src
  .replace(/\/\*[\s\S]*?\*\//g, '')
  .replace(/(^|[^:])\/\/[^\n]*/g, '$1');   // keep "://" (URLs) intact

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${++n} - ${m} ${extra}`); fail++; }
};

console.log('E3b mandatory-capability — no literal-name fallback, every receiver deposits\n');

// ── (1) registerFrame source has no literal-name dispatch fallback ────────────
{
  const src = stripComments(read('src/registry/registerFrame.js'));
  for (const prim of ['onRequest', 'onNotification', 'onRoutedMessage']) {
    const re = new RegExp(`\\brecv\\.${prim}\\s*\\(`);
    ok(`1. registerFrame source has no recv.${prim}( fallback call`, !re.test(src),
       'a literal-name fallback survives in the door');
  }
  ok('1d. registerFrame refuses a receiver with no deposited capability (mandatory-capability throw present)',
     /no deposited dispatch capability/.test(src));
}

// ── (2) composite fan-out is cap-only ─────────────────────────────────────────
{
  const src = stripComments(read('src/transport/web/composite.js'));
  ok('2a. CompositeTransport fan-out has no t.onRequest( literal-method fallback',
     !/\bt\.onRequest\s*\(/.test(src));
  ok('2b. CompositeTransport fan-out has no t.onNotification( literal-method fallback',
     !/\bt\.onNotification\s*\(/.test(src));
}

// ── (3) every production dispatch receiver deposits at construction ────────────
{
  const receivers = [
    ['sim transport',        'src/transport/sim/transport.js',   /depositDispatchCapability\s*\(\s*this\b/],
    ['node WS transport',    'src/transport/node/wstransport.js',/depositDispatchCapability\s*\(\s*this\b/],
    ['webrtc transport',     'src/transport/web/webrtc.js',      /depositDispatchCapability\s*\(\s*this\b/],
    ['bridge transport',     'src/transport/web/bridge.js',      /depositDispatchCapability\s*\(\s*this\b/],
    ['composite transport',  'src/transport/web/composite.js',   /depositDispatchCapability\s*\(\s*this\b/],
    ['AxonaPeer (self)',     'src/dht/AxonaPeer.js',              /depositDispatchCapability\s*\(\s*this\b/],
    ['default-DHT adapter',  'src/dht/AxonaPeer.js',              /depositDispatchCapability\s*\(\s*dht\b/],
  ];
  for (const [label, path, re] of receivers) {
    ok(`3. ${label} deposits a dispatch capability at construction`, re.test(read(path)),
       'production receiver does not deposit — registerFrame would refuse it');
  }
}

// ── (4) paired with E0 primitive-definition = 0 ───────────────────────────────
{
  const manifest = JSON.parse(read('test/REF-1.1-E0-manifest.json'));
  ok('4. E0 manifest records primitive-definition = 0 (no receiver defines a raw primitive)',
     manifest.summary && manifest.summary['primitive-definition'] === 0,
     `primitive-definition = ${manifest.summary?.['primitive-definition']}`);
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
