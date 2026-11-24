#!/usr/bin/env node
// =====================================================================
// fence_loss_burst_log.mjs — "A node that lost everyone starts over" v0.6,
// trigger 1, LOG ONLY. Domain flag LOSS_BURST_LOG, default off.
//
// 2026-11-24, one Mac: a process logged 54 connections closed in two seconds,
// opened one two seconds later and went on as a topic's root with that one
// channel for nine and a half hours. David: a burst is over 50% of its
// connections inside ten seconds, AND fewer than three non-bridge channels
// left. This build only COUNTS: one log line, nothing closed, nothing reset.
//
//   A  the decision (pure): the collector's morning fires once; one closing a
//      minute never fires; exactly half does not fire; a burst that leaves
//      three non-bridge channels does not fire; a bridge left over does not
//      count as a channel left; a small node is left alone; it fires once and
//      again only after the node has been back at the floor.
//   B  the hook in the one death body: off, no line and no state; on, one
//      loss-burst line with the counts; the eviction runs either way, and a
//      throw in the count does not cost it.
// Run: node test/fence_loss_burst_log.mjs
// =====================================================================
import { LossBurst, LOSS_BURST_WINDOW_MS, LOSS_BURST_FLOOR } from '../src/dht/lossBurst.js';
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { AxonaDomain } from '../src/dht/AxonaDomain.js';

const EXPECT = 19;
let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

// Close `n` of `open` channels, `gapMs` apart, leaving `nonBridgeAfter(i)` non-bridge channels.
function run(lb, { open, n, gapMs, t0 = 1_000_000, bridges = 0 }) {
  const hits = []; let t = t0, left = open;
  for (let i = 0; i < n; i++) {
    const before = left; left--;
    const h = lb.death(t, before, Math.max(0, left - bridges));
    if (h) hits.push({ i, ...h });
    t += gapMs;
  }
  return { hits, t, left };
}

console.log('fence_loss_burst_log: a burst of channel closes is counted, and nothing more');
console.log(' A. the decision');
{
  const lb = new LossBurst();
  const r = run(lb, { open: 55, n: 54, gapMs: 37 });       // 54 of 55 in two seconds
  check('A1 the collector\'s morning (54 of 55 in 2 s) fires, once', r.hits.length === 1 && lb.fired === 1, JSON.stringify(r.hits));
  check('A2 and it fires at the death that takes the node under three, not before', r.hits[0]?.left === LOSS_BURST_FLOOR - 1 && r.hits[0]?.closed === 53 && r.hits[0]?.base === 55, JSON.stringify(r.hits[0]));
}
{
  const lb = new LossBurst();
  const r = run(lb, { open: 55, n: 54, gapMs: 60_000 });
  check('A3 one closing a minute never fires, down to the last channel', r.hits.length === 0, JSON.stringify(r.hits));
}
{
  const lb = new LossBurst();
  check('A4 exactly half (2 of 4, none left non-bridge) does not fire: MORE than half', run(lb, { open: 4, n: 2, gapMs: 10, bridges: 2 }).hits.length === 0);
  const lb2 = new LossBurst();
  check('A5 three of four does', run(lb2, { open: 4, n: 3, gapMs: 10 }).hits.length === 1);
}
{
  const lb = new LossBurst();
  const r = run(lb, { open: 20, n: 17, gapMs: 10 });       // 17 of 20 gone, three non-bridge left
  check('A6 a burst that leaves three non-bridge channels does not fire', r.hits.length === 0, JSON.stringify(r.hits));
  const lb2 = new LossBurst();
  const r2 = run(lb2, { open: 20, n: 17, gapMs: 10, bridges: 1 });   // three left, one of them the bridge
  check('A7 the bridge is not a channel left: three open, one a bridge, fires', r2.hits.length === 1 && r2.hits[0].left === 2, JSON.stringify(r2.hits));
}
{
  const lb = new LossBurst();
  check('A8 a node that only ever had three channels is left alone', run(lb, { open: 3, n: 3, gapMs: 10 }).hits.length === 0);
}
{
  const lb = new LossBurst();
  const a = run(lb, { open: 10, n: 10, gapMs: 10 });
  check('A9 it fires once however many more close', a.hits.length === 1, JSON.stringify(a.hits));
  const b = run(lb, { open: 10, n: 10, gapMs: 10, t0: a.t + 60_000, bridges: 8 });   // ten open again, eight of them bridges: never back at the floor
  check('A10 a node that stayed thin does not fire again', b.hits.length === 0, JSON.stringify(b.hits));
  lb.recovered(8);
  const c = run(lb, { open: 10, n: 10, gapMs: 10, t0: b.t + 60_000 });
  check('A11 after it has been back at the floor, a new burst fires', c.hits.length === 1, JSON.stringify(c.hits));
}
{
  const lb = new LossBurst();
  let t = 5_000_000; const hits = [];
  // eleven of twenty close: six now, five more than a window later
  for (let i = 0; i < 6; i++) { const h = lb.death(t, 20 - i, 0); if (h) hits.push(h); t += 10; }
  t += LOSS_BURST_WINDOW_MS + 1;
  for (let i = 0; i < 5; i++) { const h = lb.death(t, 14 - i, 0); if (h) hits.push(h); t += 10; }
  check('A12 the half is counted inside ONE window, not across two', hits.length === 0, JSON.stringify(hits));
  check('A13 a value that is not a number decides nothing', new LossBurst().death(NaN, 10, 0) === null && new LossBurst().death(1, undefined, 0) === null);
}

console.log(' B. the hook in the one death body');
{
  const ID = (n, region = '89') => BigInt('0x' + region + n.toString(16).padStart(64, '0'));
  const mk = (flag, { bridges = [] } = {}) => {
    const bound = new Set(); for (let i = 1; i <= 12; i++) bound.add(ID(i));
    for (const b of bridges) bound.add(b);
    const logs = []; let clock = 9_000_000; let maintained = 0;
    const dom = flag === null ? {} : { LOSS_BURST_LOG: flag };
    const peer = {
      _domain: dom,
      _node: { synaptome: new Map(), incomingSynapses: new Map(), connections: new Set(), transport: { boundPeers: () => [...bound], bridgeNodeIdBig: bridges[0] ?? null } },
      _emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }),
      _scheduleMaintain: () => { maintained++; },
      _lossBurstClock: () => clock,
      _openChannels: AxonaPeer.prototype._openChannels, _noteLossBurst: AxonaPeer.prototype._noteLossBurst,
    };
    const kill = (id) => { bound.delete(id); clock += 20; return AxonaPeer.prototype._peerDeath.call(peer, id, 'test'); };
    return { peer, logs, kill, ID, burst: () => logs.filter((l) => l.ev === 'loss-burst'), evicted: () => logs.filter((l) => l.ev === 'peer-died-evicted').length, maintained: () => maintained };
  };
  check('B0 the flag is off by default', new AxonaDomain({}).LOSS_BURST_LOG === false);
  const off = mk(false);
  for (let i = 1; i <= 12; i++) off.kill(off.ID(i));
  check('B1 off: twelve of twelve die, no loss-burst line, no state kept, twelve evictions', off.burst().length === 0 && off.peer._lossBurst === undefined && off.evicted() === 12, JSON.stringify({ b: off.burst().length, e: off.evicted() }));
  const on = mk(true, { bridges: [ID(999, 'ff')] });
  for (let i = 1; i <= 12; i++) on.kill(on.ID(i));
  const line = on.burst()[0]?.ctx;
  check('B2 on: one loss-burst line', on.burst().length === 1, String(on.burst().length));
  check('B3 it carries the counts, and the bridge left open is not counted as a channel left', !!line && line.base === 13 && line.closed === 10 && line.nonBridgeLeft === 2 && line.left === 3 && line.windowMs === LOSS_BURST_WINDOW_MS, JSON.stringify(line));
  check('B4 on: the eviction ran for every death all the same', on.evicted() === 12 && on.maintained() === 12, JSON.stringify({ e: on.evicted(), m: on.maintained() }));
  const thrower = mk(true);
  thrower.peer._noteLossBurst = () => { throw new Error('count broke'); };
  let threw = null; try { for (let i = 1; i <= 12; i++) thrower.kill(thrower.ID(i)); } catch (e) { threw = e; }
  check('B5 a throw in the count does not cost the eviction', threw === null && thrower.evicted() === 12, String(threw));
}
console.log(`\nfence_loss_burst_log: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
