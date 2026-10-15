#!/usr/bin/env node
// =====================================================================
// fence_pool_profile.mjs — G4 of the two-pool packet (axona-docs ca52384;
// Aster v1.1 bc4fe433 §2, v1.2 b395fe8f G4): PROFILE FEASIBILITY.
//
// One validated, frozen profile; every infeasible combination is REFUSED
// with the field named, never trimmed or borrowed. The review rows are
// carried as examples and none is adopted: a transport built without a
// profile is not armed (fence_pool_reservation covers that side).
//
// Mutant: delete the `M + I > H` check in makePoolProfile → check 1 fails.
// Run: node test/fence_pool_profile.mjs
// =====================================================================
import { makePoolProfile, isPoolProfile, PoolProfileError, POOL_PROFILE_EXAMPLES } from '../src/transport/web/pool_profile.js';
import { webTransport } from '../src/transport/web/index.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const refuses = (opts, field) => { try { makePoolProfile(opts); return false; } catch (e) { return e instanceof PoolProfileError && e.field === field; } };

console.log('fence_pool_profile: G4 — the profile is validated at construction, refused never trimmed');

check('1 M + I > H is refused, naming H (no borrowing, no 101st allocation)', refuses({ M: 50, I: 50, H: 99 }, 'H'));
check('2 pPending > M is refused, naming pPending', refuses({ M: 4, I: 4, pPending: 5 }, 'pPending'));
check('3 cInbound > I is refused, naming cInbound', refuses({ M: 50, I: 2, cInbound: 3 }, 'cInbound'));
check('4 serveFloor > M is refused, naming serveFloor', refuses({ M: 3, I: 3, serveFloor: 4 }, 'serveFloor'));
check('5 a hostBudget below H + doorSockets is refused, naming hostBudget', refuses({ M: 50, I: 50, H: 100, hostBudget: 110, doorSockets: 15 }, 'hostBudget'));
check('6 M is required and ≥ 1; I is required and ≥ 0', refuses({ I: 5 }, 'M') && refuses({ M: 0, I: 5 }, 'M') && refuses({ M: 5 }, 'I') && refuses({ M: 5, I: -1 }, 'I'));
check('7 a non-integer is refused with its field', refuses({ M: 50.5, I: 50 }, 'M') && refuses({ M: 50, I: 50, promoteMs: 'soon' }, 'promoteMs'));

const relay = makePoolProfile(POOL_PROFILE_EXAMPLES.relay);
check('8 the relay review row is feasible: armed, frozen, H defaults to M + I when absent elsewhere', relay.armed === true && Object.isFrozen(relay) && relay.M === 50 && relay.I === 50 && relay.H === 100 && relay.pPending === 8 && relay.cInbound === 4 && relay.serveFloor === 3 && relay.promoteMs === 30_000);
const leaf = makePoolProfile(POOL_PROFILE_EXAMPLES.leaf);
check('9 the leaf review row is feasible at a fraction of the relay\'s (a leaf is not presumed capable of 100 peers)', leaf.M === 6 && leaf.I === 6 && leaf.H === 12 && leaf.pPending === 2 && leaf.cInbound === 2);
const dflt = makePoolProfile({ M: 10, I: 4 });
check('10 defaults derive from M and I and never exceed them: H = M + I, pPending = min(8, M), cInbound = min(4, I), serveFloor = min(3, M)', dflt.H === 14 && dflt.pPending === 8 && dflt.cInbound === 4 && dflt.serveFloor === 3 && makePoolProfile({ M: 2, I: 1 }).pPending === 2 && makePoolProfile({ M: 2, I: 1 }).cInbound === 1);
check('11 a feasible hostBudget passes and is carried with doorSockets', makePoolProfile({ M: 50, I: 50, H: 100, hostBudget: 115, doorSockets: 15 }).hostBudget === 115);
check('12 isPoolProfile recognises the product and nothing shaped loosely', isPoolProfile(relay) && !isPoolProfile({ M: 50, I: 50 }) && !isPoolProfile(null));

// The launcher refuses at construction, before any mesh exists.
{
  let threw = null;
  try { webTransport({ bridgeUrl: 'ws://127.0.0.1:1', identity: { id: 'ab'.repeat(33) }, meshPools: { M: 50, I: 50, H: 10 }, autoHandshake: false }); }
  catch (e) { threw = e; }
  check('13 webTransport({ meshPools }) refuses an infeasible profile before building the mesh', threw instanceof PoolProfileError && threw.field === 'H', String(threw));
  let threw2 = null;
  try { webTransport({ bridgeUrl: 'ws://127.0.0.1:1', identity: { id: 'ab'.repeat(33) }, meshPools: { M: 5, I: 5 }, meshLedger: false, autoHandshake: false }); }
  catch (e) { threw2 = e; }
  check('14 pools without the ledger are refused (the ledger carries the classes)', threw2 instanceof PoolProfileError && threw2.field === 'meshLedger', String(threw2));
}

console.log(`\nfence_pool_profile: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
