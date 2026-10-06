// =====================================================================
// fence_hop_cache_sender.mjs — Hold-and-Fill v0.15 (axona-docs e4809d2),
// repair row 9: the hop_cache SENDER on the lookup trace.
//
// What this fences:
//   A. The sender, on a real AxonaPeer with _lookupStep stubbed to return a
//      known trace and transport.notify recorded:
//      A1 flag OFF (no synaptomeMaintain): a successful lookup sends NOTHING.
//      A2 flag ON: a successful lookup sends exactly min(LATERAL_K, hops)
//         `hop_cache` frames, payload { target, depth: 0 } with the BigInt
//         target, to distinct hops nearest the target first, never to self
//         or to the target; once per successful lookup (a second lookup
//         sends again; a failed lookup sends nothing; an empty trace sends
//         nothing); the per-peer counters and log line agree.
//      NOTE: the sender's counters are notify ATTEMPTS (Aster a8cd8f25), not
//      delivery evidence; only part B observes a received frame.
//   B. End to end on the sim network: chain A — B — C — T. With A armed,
//      A.lookup(T) routes through B and C; a hop RECEIVES hop_cache{T} from
//      A and the frame reaches _considerCandidate(T, 'hopCache') — the
//      guarded candidate path (rows 10, 11, 8), which on the web transport
//      is the only path that can dial a stranger (a forwarding hop's own
//      in-lookup install uses the bound-only _addByVitality and fails for a
//      stranger there; in the sim both succeed, so the fence observes the
//      frame, not the table). With A unarmed nothing is sent or considered.
//   C. STATIC: the sender is guarded by this._maintainCfg; the receiver
//      (hopCacheHandler) is unchanged from main.
//
// With the arm-flag check removed, A1 fails. With the sender call removed
// from lookup(), A2 and B fail.
//
// Run: node test/fence_hop_cache_sender.mjs
// =====================================================================
import { readFileSync } from 'node:fs';
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const J = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString(16).slice(0, 8) : x));

async function makePeer(net, domain, lat, lng, opts = {}) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, ...opts });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id), hex: id.id };
}
const recordNotify = (rec) => { const calls = []; const orig = rec.transport.notify.bind(rec.transport); rec.transport.notify = async (to, type, body) => { calls.push({ to, type, body }); return orig(to, type, body).catch(() => {}); }; return calls; };

(async () => {
  console.log('fence_hop_cache_sender: row 9 — the hop_cache sender on the lookup trace');
  const domain = new AxonaDomain();
  const K = domain.LATERAL_K;
  check('setup: LATERAL_K is the design\'s bound', K === 3, String(K));

  // ── A. the sender on a stubbed trace ─────────────────────────────
  console.log('\n  A. the sender');
  // trace entries carry the synapse the hop chose next (the LTP wave reads synapse.peerId)
  const mkTrace = (self, hops, target) => { const ids = [self, ...hops, target]; return ids.map((id, i) => ({ fromId: id, synapse: { peerId: ids[Math.min(i + 1, ids.length - 1)], weight: 0.5 } })); };
  const H = (self, n) => Array.from({ length: n }, (_, i) => self ^ (1n << BigInt(150 + i)));

  // A1 flag OFF
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 1, 1, {});
    const calls = recordNotify(a);
    const hops = H(a.big, 5), target = a.big ^ (1n << 200n);
    a.peer._lookupStep = async () => ({ found: true, trace: mkTrace(a.big, hops, target), path: [a.big, ...hops, target], totalTimeMs: 1 });
    const r = await a.peer.lookup(target);
    const hc = calls.filter(c => c.type === 'hop_cache');
    check('A1 flag OFF: a successful lookup sends NO hop_cache', r.found === true && hc.length === 0 && (a.peer._hopCacheAttempts ?? 0) === 0, J(hc));
    check('A1 _maintainCfg is null without synaptomeMaintain', a.peer._maintainCfg === null);
    await a.peer.stop().catch(() => {});
  }

  // A2 flag ON
  {
    const net = new SimNetwork();
    const a = await makePeer(net, domain, 2, 2, { synaptomeMaintain: true });
    // stop the maintenance timer so nothing else dials during the test
    if (a.peer._maintainTimer) { clearInterval(a.peer._maintainTimer); a.peer._maintainTimer = null; }
    const calls = recordNotify(a);
    const logs = []; const ol = a.peer._emitLog.bind(a.peer); a.peer._emitLog = (l, m, c) => { logs.push([m, c]); return ol(l, m, c); };
    const hops = H(a.big, 5), target = a.big ^ (1n << 200n);
    a.peer._lookupStep = async () => ({ found: true, trace: mkTrace(a.big, hops, target), path: [a.big, ...hops, target], totalTimeMs: 1 });
    await a.peer.lookup(target);
    const hc = calls.filter(c => c.type === 'hop_cache');
    check('A2 flag ON: exactly LATERAL_K hop_cache frames for a 5-hop trace', hc.length === K, String(hc.length));
    check('A2 payload is { target: <BigInt>, depth: 0 }', hc.every(c => typeof c.body?.target === 'bigint' && c.body.target === target && c.body.depth === 0), J(hc.map(c => c.body)));
    check('A2 recipients: distinct hops, nearest the target first, never self or target', hc.map(c => c.to).join() === [hops[4], hops[3], hops[2]].join() && !hc.some(c => c.to === a.big || c.to === target), J(hc.map(c => c.to)));
    check('A2 counters and log agree (ATTEMPTS, not deliveries)', a.peer._hopCacheAttempts === K && a.peer._hopCacheLast?.attempted === K && logs.some(([m, c]) => m === 'hop-cache-attempted' && c?.hops === K), J(a.peer._hopCacheLast));
    // once per successful lookup: a second lookup sends again; failure sends nothing; empty trace sends nothing
    await a.peer.lookup(target);
    check('A2 a second successful lookup sends again (once per lookup)', calls.filter(c => c.type === 'hop_cache').length === 2 * K);
    a.peer._lookupStep = async () => ({ found: false, trace: mkTrace(a.big, hops, target), path: [a.big], totalTimeMs: 1 });
    await a.peer.lookup(target);
    check('A2 a FAILED lookup sends nothing', calls.filter(c => c.type === 'hop_cache').length === 2 * K);
    a.peer._lookupStep = async () => ({ found: true, trace: [], path: [a.big], totalTimeMs: 1 });
    await a.peer.lookup(target);
    check('A2 an empty trace sends nothing', calls.filter(c => c.type === 'hop_cache').length === 2 * K);
    // a short trace: fewer than K hops → that many, no padding
    a.peer._lookupStep = async () => ({ found: true, trace: mkTrace(a.big, hops.slice(0, 2), target), path: [a.big, hops[0], hops[1], target], totalTimeMs: 1 });
    await a.peer.lookup(target);
    check('A2 a 2-hop trace sends 2', calls.filter(c => c.type === 'hop_cache').length === 2 * K + 2);
    await a.peer.stop().catch(() => {});
  }

  // ── B. end to end on the sim network ──────────────────────────────
  console.log('\n  B. a lookup teaches a hop about the target it found');
  async function chain(armed) {
    const net = new SimNetwork();
    const A = await makePeer(net, domain, 10, 10, armed ? { synaptomeMaintain: true } : {});
    if (A.peer._maintainTimer) { clearInterval(A.peer._maintainTimer); A.peer._maintainTimer = null; }
    const B = await makePeer(net, domain, 11, 11, {});
    const C = await makePeer(net, domain, 12, 12, {});
    const T = await makePeer(net, domain, 13, 13, {});
    // wire the chain A—B, B—C, C—T (sim: open + bind on both ends → auto-admit)
    await A.transport.openConnection(B.hex); await B.transport.openConnection(C.hex); await C.transport.openConnection(T.hex);
    await wait(30);
    // B must not already hold T
    B.node.synaptome.delete(T.big); A.node.synaptome.delete(C.big); A.node.synaptome.delete(T.big); B.node.synaptome.delete(A.big);
    // record what the hops RECEIVE and what reaches the guarded candidate path
    let received = 0; const considered = [];
    for (const X of [B, C]) {
      const hc = X.transport._ntfHandlers?.get('hop_cache');
      if (hc) X.transport._ntfHandlers.set('hop_cache', (from, payload) => { received++; return hc(from, payload); });
      const oc = X.peer._considerCandidate.bind(X.peer);
      X.peer._considerCandidate = async (id, source) => { considered.push({ who: X === B ? 'B' : 'C', id, source }); return oc(id, source); };
    }
    const r = await A.peer.lookup(T.big);
    await wait(80);
    const viaFrame = considered.filter(c => c.source === 'hopCache' && c.id === T.big);
    return { r, received, viaFrame: viaFrame.length, aSent: A.peer._hopCacheAttempts ?? 0, path: r.path.length, stop: async () => { for (const x of [A, B, C, T]) { await x.peer.stop().catch(() => {}); } } };
  }
  {
    const on = await chain(true);
    check('B armed: the lookup found T through the chain', on.r.found === true && on.path >= 3, J({ found: on.r.found, path: on.path }));
    check('B armed: A attempted hop_cache (bounded by K; attempts, not deliveries)', on.aSent >= 1 && on.aSent <= K, String(on.aSent));
    check('B armed: a hop received hop_cache from A and the frame reached _considerCandidate(T, hopCache)', on.received >= 1 && on.viaFrame >= 1, J({ received: on.received, viaFrame: on.viaFrame }));
    await on.stop();
    const off = await chain(false);
    check('B unarmed: the same lookup found T', off.r.found === true);
    check('B unarmed: nothing sent, nothing received, nothing considered via the frame', off.aSent === 0 && off.received === 0 && off.viaFrame === 0, J({ aSent: off.aSent, received: off.received, viaFrame: off.viaFrame }));
    await off.stop();
  }

  // ── C. static ─────────────────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/dht/AxonaPeer.js', import.meta.url), 'utf8');
    const s = src.indexOf('  _sendHopCache(targetKey, trace) {'); const e = src.indexOf('\n  }\n', s);
    const body = src.slice(s, e);
    check('C the sender returns before anything when _maintainCfg is unset', /if \(!this\._maintainCfg\) return 0;/.test(body));
    check('C lookup() calls the sender only on a found result', /if \(result\.found\) this\._sendHopCache\(targetKey, result\.trace\);/.test(src));
    check('C the receiver is unchanged: hop_cache and lateral_spread share hopCacheHandler → _considerCandidate(payload.target, source)', /const hopCacheHandler = async \(_fromId, payload\) => \{\n\s*const source = \(payload\.depth \?\? 0\) === 0 \? 'hopCache' : 'lateralSpread';\n\s*await this\._considerCandidate\(payload\.target, source\);/.test(src));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
