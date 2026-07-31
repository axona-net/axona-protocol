// fence_pull_outcome.mjs — Q1. A read must say WHICH kind of nothing it got.
//
// requestPull collapses four distinct outcomes into a single `null`:
//
//   AxonaManager.js:798    timeout          → resolve(null)
//   wireHandlers.js:766    JSON.parse threw → parsed = null   (malformed response)
//   wireHandlers.js:774    no json in resp  → parsed ?? null   (responder holds nothing)
//   wireHandlers.js:774    good envelope    → the envelope
//
// Three different facts, one value. Every consumer above then manufactures a
// confident negative from it, and on 2026-07-31 that cost most of a day: three
// separate repairs (relay v0.97.0 mcp pull, v0.98.0 ops/cli, v0.99.0 post-script
// confirms) are all WALL-CLOCK HEURISTICS guessing from a stopwatch at what this
// function already knew and threw away. Aster: "the durable fix belongs below the
// MCP layer". This fence pins that contract so those three can be deleted rather
// than tuned.
//
// A responder's empty answer is NOT proof the network holds nothing — it is one
// node's negative. That distinction is why 'response with envelope:null' must be
// its own outcome and must not be flattened into 'timeout' or into 'absent'.
//
// DRIVES THE REAL PATH. requestPull() is called for real and the reply is fed
// through the real _onPullResp handler. Nothing is stubbed out — the previous
// fence in this class (fence_replica_ledger) replaced the function under test
// with a synthetic throw and so certified its own premise. Not again.
//
// EXPECTED RED against the current tree: 1b, 1c and 1d all resolve null and are
// therefore indistinguishable, and none carries a `kind`.
//
// Run: node test/fence_pull_outcome.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const TOPIC = REG | 0x7001n;

function mk() {
  const sends = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => { sends.push({ type, payload }); },
    neighbors: () => [idHex(REG | 0xaa0n)],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => Date.now(), rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, sends };
}

// Issue a real pull, then feed a crafted PULLRESP through the real handler.
// `patch === null` means: never answer (exercise the timeout path).
async function pullOutcome(patch, timeoutMs = 400) {
  const { am, sends } = mk();
  const p = am.requestPull(TOPIC, null, { timeoutMs });
  const sent = sends[sends.length - 1];
  const corrId = sent?.payload?.corrId;
  if (!corrId) throw new Error('no PULL was sent — fence cannot drive the real path');
  if (patch !== null) {
    am._onPullResp({ corrId, requesterId: idHex(SELF), ...patch }, { targetId: SELF });
    return await p;
  }
  // TIMEOUT CASE. requestPull unrefs its timer (AxonaManager.js:799) so a pending
  // pull cannot hold the process open — node would exit before the timeout fires.
  // That is correct for production and awkward for a test, so hold a ref'd timer
  // for the duration and release it once the pull settles.
  const keepAlive = setInterval(() => {}, 50);
  try { return await p; } finally { clearInterval(keepAlive); }
}

const ENVELOPE = { msgId: 'm1', ts: 1, message: { text: 'hi' } };

console.log('pull outcome — a read must say WHICH kind of nothing it got\n');

// AMENDED after Aster's Q1 review (council 2026-07-31). The 'empty' case was
// `{}` — an OMITTED json — which no conforming responder ever emits: _onPull
// (wireHandlers.js:779) always sets the field, `json: hit ? hit.json : null`.
// So the fence was certifying a wire shape the protocol does not produce, and
// the REAL no-hit path was untested. json:null is the genuine negative; an
// omitted field and the string 'null' are malformed messages that must not be
// allowed to impersonate one.
const got = {
  answered  : await pullOutcome({ json: JSON.stringify(ENVELOPE) }),
  empty     : await pullOutcome({ json: null }),           // THE REAL no-hit shape
  omitted   : await pullOutcome({}),                       // no conforming responder emits this
  stringNull: await pullOutcome({ json: 'null' }),         // parses to null — must NOT read as a no-hit
  timedOut  : await pullOutcome(null),                     // nobody answered
  malformed : await pullOutcome({ json: '{not json' }),    // reply arrived, unparseable
};
for (const [k, v] of Object.entries(got)) {
  console.log(`     ${k.padEnd(10)} -> ${JSON.stringify(v)?.slice(0, 90)}`);
}
console.log();

// ── 1. Each outcome is TAGGED, and tagged as itself ────────────────────────
ok('1a. a real envelope is kind:"response" and carries it',
  got.answered?.kind === 'response' && got.answered?.envelope?.msgId === 'm1',
  JSON.stringify(got.answered));

ok('1b. a responder that HOLDS NOTHING is kind:"response" with envelope:null — ' +
   'one node\'s negative, NOT proof the network is empty',
  got.empty?.kind === 'response' && got.empty?.envelope === null,
  JSON.stringify(got.empty));

ok('1c. no answer at all is kind:"timeout" — never an empty response',
  got.timedOut?.kind === 'timeout', JSON.stringify(got.timedOut));

ok('1d. an unparseable reply is kind:"invalid-response" — never an empty response',
  got.malformed?.kind === 'invalid-response', JSON.stringify(got.malformed));

// 1e/1f are Aster's finding. Both are malformed messages that LOOK like a no-hit
// if you are careless, and treating either as one hands a caller a fabricated
// authoritative negative — the whole defect Q1 exists to remove.
ok('1e. an OMITTED json is kind:"invalid-response", NOT an empty response — ' +
   'the responder at wireHandlers.js:779 always sets the field',
  got.omitted?.kind === 'invalid-response', JSON.stringify(got.omitted));

ok('1f. the STRING "null" is kind:"invalid-response" — it parses to null and ' +
   'would otherwise impersonate a genuine no-hit',
  got.stringNull?.kind === 'invalid-response', JSON.stringify(got.stringNull));

// ── 2. THE POINT: the four are mutually distinguishable ────────────────────
// This is the assertion the old contract could not satisfy at any timeout value,
// which is why raising budgets was never going to be the fix.
{
  const kinds = [got.answered?.kind, got.empty?.kind, got.timedOut?.kind, got.malformed?.kind,
                 got.omitted?.kind, got.stringNull?.kind];
  ok('2a. all four outcomes carry a kind', kinds.every(k => typeof k === 'string'),
    JSON.stringify(kinds));
  ok('2b. timeout, empty-response and invalid-response are THREE distinct kinds',
    new Set([got.timedOut?.kind, got.empty?.kind, got.malformed?.kind]).size === 3,
    JSON.stringify(kinds));
  ok('2c. no outcome is a bare null — the caller can always ask what happened',
    Object.values(got).every(v => v !== null && v !== undefined),
    JSON.stringify(Object.values(got).map(v => v === null ? 'NULL' : typeof v)));
}

// ── 3. CONTROL: the success path still delivers the full envelope ──────────
// Without this, "tag everything" could quietly stop returning data and still pass.
ok('3a. CONTROL — the envelope survives tagging intact (msgId + ts + message)',
  got.answered?.envelope?.msgId === 'm1' &&
  got.answered?.envelope?.ts === 1 &&
  got.answered?.envelope?.message?.text === 'hi',
  JSON.stringify(got.answered?.envelope));

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
