// fence_republish_after_kill.mjs — 4.131.0: the author republishing the SAME
// message after killing it is a new publication, not a resurrection.
//
// Seen 2026-11-05 in alert-bot localhost-twoshot: run 2 kills run 1's 2,992
// messages, then republishes fire alerts whose content is identical (event
// time from the NASA CSV, persistent 'firms' identity). The msgId is
// sha256(publisher, message) and folds in neither ts nor seq, so every
// republished fire alert had the msgId of a tombstone and _tombstoned dropped
// it: 438 topics received nothing.
//
//   Every test compares two stamps from ONE clock:
//   A  root ingress, no author kill ts on the tombstone → stays dead (as before)
//   B  root ingress, envelope ts AFTER the kill's signed ts → accepted, tombstone gone
//   B2 root ingress, envelope ts BEFORE the kill's signed ts → stays dead
//   S  a root-stamped copy (replay/fan-out): publishTs below killTs stays dead
//      even when its envelope ts is LATER (the author's clock is never compared
//      with the root's: smoke_reorder_convergence signs with the wall clock and
//      stamps with a test clock); publishTs above killTs is accepted
//   C  a different signer still revokes the tombstone (unchanged)
//   D  an envelope with no ts stays dead at ingress (fail closed)
//
// Run: node test/fence_republish_after_kill.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { sealTestDht } from './lib/testCapability.mjs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const T = 1_700_000_000_000;
const TOPIC = { region: 'useast', name: 'fire' };
const stubDht = () => ({ getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
  sendDirect: async () => true, verdictsSupported: false, routeMessage: async () => {} });
const am = new AxonaManager({ dht: sealTestDht(stubDht()), now: () => T });
const logs = []; const orig = am._log?.bind(am); am._log = (lvl, ev, ctx) => { logs.push(ev); orig?.(lvl, ev, ctx); };
const alice = await createAuthorIdentity(), bob = await createAuthorIdentity();
const killTs = T + 10_000;
const mk = async (identity, ts) => JSON.stringify(await buildEnvelope({ topic: TOPIC, message: '{"lat":1,"lng":2}', identity, ts, seq: ts }));
const authorKillTs = T + 3_000;                          // the author's clock signs the kill earlier than the root stamps it
const roleWith = (msgId, signer, authorTs) => ({ tombstones: new Map([[msgId, { exp: T + 600_000, killTs, signer: signer.toLowerCase(), seq: 7, ...(authorTs != null ? { authorTs } : {}) }]]) });

console.log('fence_republish_after_kill');
{
  const json = await mk(alice, killTs + 5_000); const id = JSON.parse(json).msgId; const role = roleWith(id, alice.pubkeyHex);
  check('A: ingress with no author kill ts stays suppressed', am._tombstoned(role, id, json) === true && role.tombstones.has(id));
}
{
  const old = await mk(alice, T), again = await mk(alice, authorKillTs + 1);
  const id = JSON.parse(old).msgId;
  check('B: the republished copy has the same msgId (content-derived)', JSON.parse(again).msgId === id);
  const role = roleWith(id, alice.pubkeyHex, authorKillTs);
  check('B: ingress copy signed after the kill\'s signed ts is accepted', am._tombstoned(role, id, again) === false);
  check('B: the tombstone is removed', !role.tombstones.has(id));
  check('B: logged tombstone-superseded', logs.includes('tombstone-superseded'));
  const role2 = roleWith(id, alice.pubkeyHex, authorKillTs);
  check('B2: ingress copy signed before the kill\'s signed ts stays dead', am._tombstoned(role2, id, old) === true && role2.tombstones.has(id));
}
{
  const late = await mk(alice, killTs + 60_000); const id = JSON.parse(late).msgId;
  const role = roleWith(id, alice.pubkeyHex, authorKillTs);
  check('S: stamped copy with publishTs below killTs stays dead despite a later envelope ts', am._tombstoned(role, id, late, killTs - 1) === true && role.tombstones.has(id));
  check('S: stamped copy with publishTs above killTs is accepted', am._tombstoned(role, id, late, killTs + 1) === false && !role.tombstones.has(id));
}
{
  const json = await mk(bob, T); const id = JSON.parse(json).msgId; const role = roleWith(id, alice.pubkeyHex);
  check('C: a different signer revokes the tombstone (unchanged)', am._tombstoned(role, id, json) === false && !role.tombstones.has(id));
}
{
  const env = JSON.parse(await mk(alice, killTs + 5_000)); delete env.ts; const json = JSON.stringify(env);
  const role = roleWith(env.msgId, alice.pubkeyHex, authorKillTs);
  check('D: no ts at ingress → suppressed, tombstone kept', am._tombstoned(role, env.msgId, json) === true && role.tombstones.has(env.msgId));
}
console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
