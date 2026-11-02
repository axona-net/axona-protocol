#!/usr/bin/env node
// fence_graduated_sample — "The introduction represents the mesh, not the
// socket" v0.1 (axona-docs c7b0230), P1.a, the kernel half.
//
//   A. SANITISER. Not an array → empty; entries that are not lowercase
//      66-hex are dropped; duplicates dropped, order kept; at most 64 read.
//   B. NOMINATED, NEVER DIALLED. A real AxonaPeer with the fill armed on a
//      transport that fans in peer-list: handles (`c17`) are ignored by the
//      handler, a 66-hex id is nominated as source `directory` once, the
//      peer's own id is ignored, a bound peer is ignored; the mesh's own
//      dial list is the `peers` array alone (C).
//   C. STATICS on web/index.js: `graduated` is sanitised, reaches the row-12
//      handlers, and `mesh.onPeerList` is called with `peers` only.
//
// Mutants (each run by hand; the section named fails): pass `forHandlers` to
// mesh.onPeerList → C; drop the sanitiser → A, C; nominate as 'pex' → B.
import { readFileSync } from 'node:fs';
import { AxonaPeer }            from '../src/dht/AxonaPeer.js';
import { AxonaDomain }          from '../src/dht/AxonaDomain.js';
import { NeuronNode }           from '../src/dht/NeuronNode.js';
import { Transport }            from '../src/contracts/Transport.js';
import { depositDispatchCapability } from '../src/registry/index.js';
import { createNodeIdentity }   from '../src/identity/index.js';
import { fromHex }              from '../src/utils/hexid.js';
import { sanitizeGraduatedSample, GRADUATED_SAMPLE_MAX } from '../src/transport/web/graduated_sample.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const hexN = (n) => n.toString(16).padStart(66, '0');
const ARMED = { synaptomeMaintain: { kNear: 5, maxPerTick: 3, kCache: 8, pPending: 4, directoryMs: 1000 }, attemptGuard: {}, admissionGate: { kNear: 5, sparseFloor: 2 } };

class StubTransport extends Transport {
  constructor() { super(); this.handlers = []; this.bound = new Set(); depositDispatchCapability(this, { request: () => {}, notification: () => {} }); }
  onPeerList(h) { this.handlers.push(h); return () => {}; }
  emitPeerList(list) { for (const h of this.handlers) h(list.slice()); }
  async start() {} async stop() {} getLocalNodeId() { return 0n; }
  async openConnection() { return false; } async closeConnection() {}
  isConnected(id) { return this.bound.has(id); }
  async send() { throw new Error('stub'); } async notify() {}
  onPeerDied() { return () => {}; } getLatency() { return 10; }
  boundPeers() { return [...this.bound]; } onPeerBound() { return () => {}; }
}

(async () => {
  console.log('fence_graduated_sample: P1.a kernel half — the bridge\'s graduated sample is nominated as directory and never dialled');

  // ── A. sanitiser ───────────────────────────────────────────────────────
  {
    check('A1 non-array → empty', sanitizeGraduatedSample(undefined).length === 0 && sanitizeGraduatedSample('x').length === 0 && sanitizeGraduatedSample({ 0: hexN(1) }).length === 0);
    const good = hexN(0xabc), upper = good.toUpperCase();
    const out = sanitizeGraduatedSample([good, 'c17', 'c-self-1', upper, good, good.slice(1), 42, null, hexN(0xdef)]);
    check('A2 keeps lowercase 66-hex only, drops duplicates, keeps order', out.length === 2 && out[0] === good && out[1] === hexN(0xdef));
    const many = Array.from({ length: 100 }, (_, i) => hexN(i + 1));
    check(`A3 reads at most ${GRADUATED_SAMPLE_MAX}`, sanitizeGraduatedSample(many).length === GRADUATED_SAMPLE_MAX && GRADUATED_SAMPLE_MAX === 64);
  }

  // ── B. nominated as directory, never dialled ───────────────────────────
  {
    const id = await createNodeIdentity({ lat: 38, lng: -77 });
    const node = new NeuronNode({ id: fromHex(id.id), lat: 38, lng: -77 });
    const t = new StubTransport(); node.transport = t;
    const peer = new AxonaPeer({ domain: new AxonaDomain(), node, nodeIdentity: id, transport: t, ...ARMED });
    peer._requireAxonaManager('fence');
    await peer.start();
    if (peer._maintainTimer) { clearInterval(peer._maintainTimer); peer._maintainTimer = null; }
    const strangerHex = (i) => (fromHex(id.id) ^ (1n << BigInt(40 + i))).toString(16).padStart(66, '0');
    const g1 = strangerHex(1), g2 = strangerHex(2), boundHex = strangerHex(3);
    t.bound.add(fromHex(boundHex));
    const before = peer._fillStats.nominated;
    t.emitPeerList(['c17', 'c-self-9', g1, id.id, boundHex, g2, g1]);
    const cache = peer._fillCache;
    check('B1 handles are ignored; the two stranger ids are nominated, once each', peer._fillStats.nominated - before === 2 && cache.has(fromHex(g1)) && cache.has(fromHex(g2)));
    check('B2 nominated as source `directory`', cache.get(fromHex(g1))?.src === 'directory' && cache.get(fromHex(g2))?.src === 'directory');
    check('B3 the peer\'s own id and a bound peer are never nominated', !cache.has(fromHex(id.id)) && !cache.has(fromHex(boundHex)));
    check('B4 the fill directory state records the answer', peer._fillDirectory?.state === 'answered' && peer._fillDirectory?.offered === 7);
    await peer.stop?.();
  }

  // ── C. statics on web/index.js ─────────────────────────────────────────
  {
    const src = readFileSync(new URL('../src/transport/web/index.js', import.meta.url), 'utf8');
    const i = src.indexOf("case 'peer-list': {"); const block = src.slice(i, src.indexOf("case 'peer-joined':", i));
    check('C1 the field is sanitised', /const graduated = sanitizeGraduatedSample\(frame\.graduated\)/.test(block));
    check('C2 the handlers receive peers plus the sample', /const forHandlers = graduated\.length \? peers\.concat\(graduated\) : peers;/.test(block) && /h\(forHandlers\.slice\(\)\)/.test(block));
    check('C3 the mesh dials `peers` only', /mesh\.onPeerList\(peers\)/.test(block) && !/mesh\.onPeerList\(forHandlers\)/.test(block));
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error('fence crashed:', e); process.exit(2); });
