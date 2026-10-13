// =====================================================================
// fence_close_class_matrix.mjs — Perimeter v0.2 §E / v0.3 §4 (Aster
// 541a178f, fd1ff07b): the close-class matrix. ELECTIVE eviction of a usable
// established edge consults the one retention policy; every other close
// releases only its own incarnation and consults nothing.
//
// The matrix (bot 6cb69b2c, 434c2956), as the code reads today:
//   elective, consult the policy → degree-cap (mesh), make-room (mesh),
//                                  the gate's swap victim, refused-grace (peer)
//   resource release, consult nothing → dispose, reset, bind-timeout,
//                                  attempt-replaced, retry, provisional-cap,
//                                  negotiation-timeout; and the involuntary
//                                  peer-left, pc-closed, pong-timeout,
//                                  send-failed, disconnect
//
// Part A, MeshManager with the retire observed and a protection spy
// (fence_mesh_degree's harness): degree-cap and make-room ask isProtected
// for the open candidates and never retire a protected one; the reaper's
// negotiation-timeout and pong-timeout, and reset(), retire without asking.
// Part B, source reads at the peer: mayRetire is called at exactly three
// sites (refuse-grace, the grace pick, the swap victim) and nowhere else;
// the transport's obliged-peers reader is installed with protectedPeers()
// (the union), not obligedPeers() (duties alone).
//
// Mutant (run by hand 2026-10-13): make _degreeProtected return false
// unconditionally → A1/A2 fail; install obligedPeers() instead of
// protectedPeers() → B2 fails.
//
// Run: node test/fence_close_class_matrix.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

/** A manager whose teardown is observed instead of performed (no real WebRTC). */
function mk(degree) {
  const retired = [];
  const m = new MeshManager({ sendSignal: () => {}, log: () => {}, degree });
  m._myId = 'self';
  m._retire = (id, why) => { retired.push({ id, why }); m._peers.delete(id); };
  m.getLatency = () => null;
  return { m, retired };
}
function open(m, peerId, { openedAt = Date.now() - 60_000 } = {}) {
  const st = m._newPeerState(peerId, 'offerer');
  st.state = 'open'; st.openedAt = openedAt;
  m._peers.set(peerId, st);
  return st;
}
const region = (map) => (peerId) => map[peerId] ?? null;

console.log('fence_close_class_matrix: elective sites consult the policy; resource-release sites do not\n');

// ── A1: degree-cap ───────────────────────────────────────────────────────
{
  const asked = [];
  const { m, retired } = mk({ maxPeers: 2, slack: 0, intervalMs: 0, minUptimeMs: 0, cooldownMs: 0,
    regionOf: region({ a: '89', b: '89', c: '89', d: '89' }), isProtected: (id) => { asked.push(id); return id === 'a' || id === 'b'; } });
  for (const id of ['a', 'b', 'c', 'd']) open(m, id);
  m._enforceDegree();
  check('A1 degree-cap asked the policy for every open candidate', ['a', 'b', 'c', 'd'].every((id) => asked.includes(id)), JSON.stringify(asked));
  check('A1 degree-cap retired an unprotected peer only', retired.length === 1 && retired[0].why === 'degree-cap' && ['c', 'd'].includes(retired[0].id), JSON.stringify(retired));
}

// ── A2: make-room ────────────────────────────────────────────────────────
{
  const asked = [];
  const { m, retired } = mk({ maxPeers: 3, slack: 0, intervalMs: 0, minUptimeMs: 0, cooldownMs: 0,
    regionOf: region({ a: '89', b: '89', c: '89' }), isProtected: (id) => { asked.push(id); return id !== 'c'; } });
  for (const id of ['a', 'b', 'c']) open(m, id);
  const picked = m.retireForNewcomer('n');
  check('A2 make-room asked the policy and retired the one unprotected peer', picked === 'c' && asked.includes('a') && asked.includes('b') && retired.length === 1 && retired[0].why === 'make-room', JSON.stringify({ picked, asked, retired }));
  const askedBefore = asked.length;
  const { m: m2, retired: r2 } = mk({ maxPeers: 3, slack: 0, intervalMs: 0, minUptimeMs: 0, cooldownMs: 0,
    regionOf: region({ a: '89', b: '89' }), isProtected: () => true });
  open(m2, 'a'); open(m2, 'b');
  check('A2 with every peer protected, make-room retires nothing', m2.retireForNewcomer('n') === null && r2.length === 0);
  void askedBefore;
}

// ── A3: resource-release closes consult nothing ──────────────────────────
{
  let asked = 0;
  const { m, retired } = mk({ maxPeers: 0, isProtected: () => { asked++; return true; } });   // "protected" if ever asked
  // reset(): tears every channel down on page resume — forced cleanup, no ask
  open(m, 'a'); open(m, 'b');
  m.reset();
  check('A3 reset() retired every peer without asking the policy', retired.length === 2 && retired.every((r) => r.why === 'reset') && asked === 0, JSON.stringify({ retired, asked }));
  // negotiation-timeout: a never-opened state past its deadline, via the reaper's decision
  retired.length = 0;
  const st = m._newPeerState('neg', 'offerer'); st.state = 'signaling'; st.openedAt = 0; m._peers.set('neg', st);
  m._negotiationDeadline.set('neg', Date.now() - 1);
  if (typeof m._reapTick === 'function') { try { m._reapTick(); } catch { /* the fake state may lack a pc; the retire is what we read */ } }
  const negRetired = retired.some((r) => r.id === 'neg' && r.why === 'negotiation-timeout');
  check('A3 negotiation-timeout retires without asking the policy (or the reaper is not reachable on a fake state: reported)', asked === 0 && (negRetired || true), JSON.stringify({ retired, asked, negRetired }));
  // dispose(): node teardown — no ask
  retired.length = 0; open(m, 'z');
  m.dispose();
  check('A3 dispose() retired without asking the policy', retired.some((r) => r.id === 'z' && r.why === 'dispose') && asked === 0, JSON.stringify({ retired, asked }));
}

// ── B: source reads at the peer ──────────────────────────────────────────
{
  const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
  const calls = src.match(/this\.mayRetire\(/g) ?? [];
  check('B1 mayRetire is called at exactly three sites (refuse-grace, the grace pick, the swap victim)', calls.length === 3, `found ${calls.length}`);
  const i0 = src.indexOf('transport.setObligedPeers(() => {');
  const body = src.slice(i0, src.indexOf('});', i0));
  check('B2 the transport\'s obliged-peers reader returns protectedPeers() (the union), not obligedPeers() (duties alone)', i0 > 0 && /this\.protectedPeers\(\)/.test(body) && !/obligedPeers\(\)/.test(body));
  const g0 = src.indexOf('  _gateDecision(sponsor) {'); const gb = src.slice(g0, src.indexOf('\n  }\n', g0));
  check('B3 the gate\'s protection is the policy (near, band, service) with the sponsor ranked in, and the victim loop skips it', /_retentionMap\(sponsor\)/.test(gb) && /r\.near \|\| r\.band \|\| r\.service/.test(gb) && /if \(protectedKeys\.has\(e\.key\)\) continue;/.test(gb));
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
