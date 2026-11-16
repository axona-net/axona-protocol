// fence_route_type_tally.mjs — 4.149.0 per-type tally (ROUTE_TYPE_TALLY, default OFF).
// A diagnostic: it counts this manager's routed sends and received frames by
// type and changes no decision. Drives the three bodies on a bare object, so
// nothing else of the manager is in play.
import assert from 'node:assert/strict';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
let n = 0; const check = (name, ok, extra = '') => { assert(ok, name + ' ' + extra); n++; console.log('PASS ' + name); };
const P = AxonaManager.prototype;
const mk = (on) => { const logs = []; return { _typeTally: on ? { out: new Map(), in: new Map() } : null, _routeStats: null, _log: (l, e, d) => logs.push({ l, e, d }), dht: { _lookaheadAnswerStats: { answers: 7, skipped: 2 } }, logs }; };
const tally = (o, r, type, target = 5n) => P._tallyRoute.call(o, target, r, type);

// OFF: nothing is created, the verdict passes through, the failure counter still works.
const off = mk(false);
const v = { consumed: false, exhausted: true };
check('off: verdict returned untouched', tally(off, v, 'x:pub') === v);
check('off: no tally object appears', off._typeTally === null);
check('off: the existing failure counter still counts', off._routeStats.fail === 1 && off._routeStats.ok === 0);
const h = () => 'r'; check('off: _countIn returns the handler itself', P._countIn.call(off, 'x:pub', h) === h);
P._reportRouteOutcomes.call(off);
check('off: only the existing line is logged', off.logs.length === 1 && off.logs[0].e === 'routed-outcomes');

// ON: by type, by outcome, by reason.
const on = mk(true);
tally(on, { consumed: true }, 'x:pub');
tally(on, { consumed: false, exhausted: true }, 'x:pub');
tally(on, { consumed: false, transportError: true, error: 'boom' }, 'x:pub');
tally(on, { consumed: false, reason: 'no-route-to-target-and-more-text-than-fits' }, 'x:ack');
tally(on, { consumed: false }, 'x:ack');
tally(on, undefined, 'x:ack'); tally(on, 3, 'x:ack');
const out = on._typeTally.out;
check('on: ok and fail split by type', out.get('x:pub').ok === 1 && out.get('x:pub').fail === 2 && out.get('x:ack').fail === 2);
check('on: reasons named', out.get('x:pub').why.exhausted === 1 && out.get('x:pub').why.transport === 1 && out.get('x:ack').why.other === 1);
check('on: a long reason is cut to 24 characters', Object.keys(out.get('x:ack').why).some((k) => k.length === 24));
check('on: a send with no verdict is "none", not a failure', out.get('x:ack').none === 2);
check('on: the existing counter is unchanged by the tally', on._routeStats.ok === 1 && on._routeStats.fail === 4);
check('on: verdict still returned untouched', tally(on, v, 'x:pub') === v);

// Arrivals.
let called = 0; const w = P._countIn.call(on, 'x:deliver', (a, b) => { called++; return a + b; });
check('on: the wrapper calls through with arguments and result', w(2, 3) === 5 && called === 1);
w(1, 1); check('on: arrivals counted', on._typeTally.in.get('x:deliver') === 2);
const thrower = P._countIn.call(on, 'x:bad', () => { throw new Error('handler'); });
let threw = false; try { thrower(); } catch { threw = true; }
check('on: a throwing handler still throws and is still counted', threw && on._typeTally.in.get('x:bad') === 1);

// Bounds.
const big = mk(true);
for (let i = 0; i < 200; i++) { tally(big, { consumed: false, reason: 'r' + i }, 't' + i); P._countIn.call(big, 'i' + i, () => {})(); }
check('bounded: at most 64 types each way', big._typeTally.out.size === 64 && big._typeTally.in.size === 64);
const one = mk(true); for (let i = 0; i < 50; i++) tally(one, { consumed: false, reason: 'r' + i }, 'same');
check('bounded: at most 6 reasons per type, all failures still counted', Object.keys(one._typeTally.out.get('same').why).length === 6 && one._typeTally.out.get('same').fail === 50);
check('hostile type values do not throw', (tally(on, { consumed: true }, undefined), tally(on, { consumed: true }, { toString() { return 'obj'; } }), tally(on, { consumed: true }, 'x'.repeat(500)), true) && [...on._typeTally.out.keys()].every((k) => k.length <= 40));
const broken = mk(true); broken._typeTally.out = { get() { throw new Error('map'); } };
check('a broken tally does not break the send', tally(broken, v, 'x:pub') === v && broken._routeStats.fail === 1);

// Report: one line, drained.
P._reportRouteOutcomes.call(on);
const line = on.logs.find((x) => x.e === 'route-type-tally');
check('report: the tally line carries out, in and the lookahead count', !!line && line.d.out['x:pub'].fail === 3 && line.d.in['x:deliver'] === 2 && line.d.la.answers === 7);
check('report: drained', on._typeTally.out.size === 0 && on._typeTally.in.size === 0);
const before = on.logs.length; P._reportRouteOutcomes.call(on);
check('report: nothing counted, nothing logged', on.logs.length === before);
const quiet = mk(true); P._countIn.call(quiet, 'x:in', () => {})(); P._reportRouteOutcomes.call(quiet);
check('report: arrivals alone are reported, with no failure line', quiet.logs.length === 1 && quiet.logs[0].e === 'route-type-tally');
console.log(`fence_route_type_tally: ${n} passed, 0 failed`);
