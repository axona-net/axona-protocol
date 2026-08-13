// smoke_pull_requester_gate.mjs — a read is matched by the WHOLE pair
// (corrId, requesterId), never corrId alone.
//
// DEFECT (Aster, REF-1.1 S2/S3 recut-4 review, council msgId d17ece0b):
// _onPullResp admitted any PULLRESP routed to this node's targetId (the
// destination OR-guard), then resolved _pending.get(corrId) with NO check on
// requesterId. So a certified response routed HERE carrying our corrId but a
// DIFFERENT requesterId settled — and deleted — another party's pending read.
// corrId is minted per-node (nodeHex[:8] + seq), so two nodes CAN collide on it.
//
// FIX: requestPull records the requester (this node) in the _pending entry;
// _onPullResp requires the response's requesterId to fold to it, else ignores
// the response and leaves the pending read intact for the real answer.
//
// DRIVES THE REAL PATH (same harness as fence_pull_outcome): requestPull() runs
// for real and crafted PULLRESPs feed through the real _onPullResp. Nothing
// stubbed.
//
// Run: node test/smoke_pull_requester_gate.mjs

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n;
const FOREIGN = REG | 0x022n;      // a different node — its requesterId must not match ours
const TOPIC = REG | 0x7001n;

function mk() {
  const sends = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    verdictsSupported: false,
    routeMessage: (target, type, payload) => { sends.push({ type, payload }); },
    neighbors: () => [idHex(REG | 0xaa0n)],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht, now: () => Date.now(), rootReplicas: 2 });
  am.nodeId = SELF;
  am.setLogSink(() => {});
  return { am, sends };
}

const tick = () => new Promise(r => setTimeout(r, 0));
const ENVELOPE = { msgId: 'm1', ts: 1, message: { text: 'hi' } };

console.log('pull requester gate — a foreign requesterId must not settle our read\n');

// ── 1. HIJACK ATTEMPT — foreign requesterId, our corrId, routed to us ───────
// The OR-guard passes (targetId === SELF). Before the fix this resolved the
// pending read with the attacker's envelope and deleted it.
{
  const { am, sends } = mk();
  const keepAlive = setInterval(() => {}, 50);   // hold the process while the read is pending
  let settled;                                    // stays undefined until the promise resolves
  const p = am.requestPull(TOPIC, null, { timeoutMs: 5000 }).then(v => { settled = v; });
  const corrId = sends[sends.length - 1]?.payload?.corrId;
  if (!corrId) throw new Error('no PULL was sent — smoke cannot drive the real path');

  // Attacker: same corrId, FOREIGN requesterId, a real-looking envelope.
  am._onPullResp(
    { corrId, requesterId: idHex(FOREIGN), json: JSON.stringify(ENVELOPE), publishTs: 1 },
    { targetId: SELF });
  await tick();

  ok('1a. the foreign-requester response did NOT settle the read', settled === undefined,
    JSON.stringify(settled));
  ok('1b. the pending read is still armed (not deleted by the impostor)',
    am._pending.has(corrId));

  // Now the legitimate answer arrives — our requesterId, a genuine no-hit.
  am._onPullResp({ corrId, requesterId: idHex(SELF), json: null }, { targetId: SELF });
  await p;

  ok('1c. the LEGITIMATE response settles the read', settled?.kind === 'response',
    JSON.stringify(settled));
  ok('1d. the attacker envelope did NOT leak in — the read reflects OUR responder\'s no-hit',
    settled?.envelope === null, JSON.stringify(settled));
  ok('1e. the pending entry is cleared once the real response lands', !am._pending.has(corrId));
  clearInterval(keepAlive);
}

// ── 2. CONTROL — a matching requesterId still resolves normally ─────────────
// The gate must not break legit reads: same corrId, OUR requesterId, real body.
{
  const { am, sends } = mk();
  const keepAlive = setInterval(() => {}, 50);
  let settled;
  const p = am.requestPull(TOPIC, null, { timeoutMs: 5000 }).then(v => { settled = v; });
  const corrId = sends[sends.length - 1]?.payload?.corrId;

  am._onPullResp(
    { corrId, requesterId: idHex(SELF), json: JSON.stringify(ENVELOPE), publishTs: 1 },
    { targetId: SELF });
  await p;

  ok('2a. CONTROL — a matching-requester response resolves', settled?.kind === 'response',
    JSON.stringify(settled));
  ok('2b. CONTROL — and carries the envelope intact', settled?.envelope?.msgId === 'm1',
    JSON.stringify(settled?.envelope));
  clearInterval(keepAlive);
}

// ── 3. MALFORMED requesterId — idBig throws → ignored, read stays armed ─────
{
  const { am, sends } = mk();
  const keepAlive = setInterval(() => {}, 50);
  let settled;
  const p = am.requestPull(TOPIC, null, { timeoutMs: 5000 }).then(v => { settled = v; });
  const corrId = sends[sends.length - 1]?.payload?.corrId;

  am._onPullResp({ corrId, requesterId: 'not-a-hex-id', json: null }, { targetId: SELF });
  await tick();
  ok('3a. a malformed requesterId does not settle the read', settled === undefined,
    JSON.stringify(settled));
  ok('3b. and the pending read stays armed', am._pending.has(corrId));

  am._onPullResp({ corrId, requesterId: idHex(SELF), json: null }, { targetId: SELF });
  await p;
  ok('3c. the well-formed matching response then settles it', settled?.kind === 'response');
  clearInterval(keepAlive);
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
