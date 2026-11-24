// =====================================================================
// fence_return_route_health.mjs — health() carries the return-route counters.
//
// WHY THIS EXISTS. The kernel has counted return routes since Closer-Born
// v0.6: recorded, used, delivered, fell back on the pub/sub side; honoured,
// unavailable, originated, delivered, fell back on the routing side. Nothing
// outside the process could read them. On 2026-11-24 the question "did this
// subscriber's way back expire?" had no reading, only an argument from
// RETURN_ROUTE_TTL_MS and RENEW_MS being the same sixty seconds.
//
// This is the seam test for the accessor and for health() carrying it. Like
// fence_health_role_projection.mjs it calls the methods with a minimal `this`:
// both are pure projections.
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let pass = 0, fail = 0;
const ok = (cond, name, got) => {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fail++; console.log(`  ✗ ${name}${got !== undefined ? ` — got ${JSON.stringify(got)}` : ''}`); }
};
const stats = (self) => AxonaPeer.prototype.returnRouteStats.call(self);

const basePeer = () => ({
  _node: { id: 'n1' },
  _engine: null,
  _transport: null,
  _started: true,
  _subscriptions: new Map(),
  _errorHandlers: new Set(),
  _logHandlers: new Set(),
  _nodeIdHex: () => 'ff00aabb',
  _installTransportLogHook: () => {},
  peers: () => [],
  lookaheadStats: () => null,
  returnRouteStats() { return AxonaPeer.prototype.returnRouteStats.call(this); },
});

console.log('A. the pub/sub counters are carried, with armed and live');
{
  const am = { _returnRoute: true, _retRoutes: new Map([['a', 1], ['b', 2]]),
               _returnRouteStats: { recorded: 7, used: 5, delivered: 3, fellBack: 2 } };
  const r = stats({ _axonaManager: am });
  ok(r.pubsub.armed === true, 'armed is true when the option is on', r.pubsub);
  ok(r.pubsub.live === 2, 'live is the number of breadcrumbs held', r.pubsub.live);
  ok(r.pubsub.recorded === 7 && r.pubsub.used === 5 && r.pubsub.delivered === 3 && r.pubsub.fellBack === 2,
     'recorded, used, delivered, fellBack pass through unchanged', r.pubsub);
}

console.log('B. an option that is off reads as off, not as absent');
{
  const am = { _returnRoute: false, _retRoutes: new Map(), _returnRouteStats: { recorded: 0, used: 0, delivered: 0, fellBack: 0 } };
  const r = stats({ _axonaManager: am });
  ok(r.pubsub !== null && r.pubsub.armed === false, 'armed is false', r.pubsub);
  ok(r.pubsub.live === 0 && r.pubsub.used === 0, 'the counters are zero, present', r.pubsub);
}

console.log('C. no manager is NOT the same fact as zero');
{
  const r = stats({});
  ok(r.pubsub === null, 'pubsub is null when there is no manager', r.pubsub);
  ok(r.routing && r.routing.honoured === 0 && r.routing.unavailable === 0,
     'the routing side still reports, at zero', r.routing);
}

console.log('D. the routing counters are carried');
{
  const r = stats({ _returnRouteStats: { honoured: 9, unavailable: 4, originated: 2, delivered: 1, fellBack: 1 } });
  ok(r.routing.honoured === 9 && r.routing.unavailable === 4 && r.routing.originated === 2
     && r.routing.delivered === 1 && r.routing.fellBack === 1, 'all five pass through', r.routing);
}

console.log('E. a partial stats object does not lose a field');
{
  const am = { _returnRoute: true, _retRoutes: new Map(), _returnRouteStats: { recorded: 1 } };
  const r = stats({ _axonaManager: am });
  ok(r.pubsub.recorded === 1 && r.pubsub.fellBack === 0 && r.pubsub.delivered === 0,
     'missing counters read 0, never undefined', r.pubsub);
}

console.log('F. health() carries it, and survives it throwing');
{
  const am = { inspectRoles: () => [], _returnRoute: true, _retRoutes: new Map([['a', 1]]),
               _returnRouteStats: { recorded: 3, used: 2, delivered: 2, fellBack: 0 } };
  const h = AxonaPeer.prototype.health.call({ ...basePeer(), _axonaManager: am });
  ok(h.returnRoute && h.returnRoute.pubsub && h.returnRoute.pubsub.recorded === 3 && h.returnRoute.pubsub.live === 1,
     'health().returnRoute.pubsub holds the counters', h.returnRoute);
  let threw = null, h2;
  try { h2 = AxonaPeer.prototype.health.call({ ...basePeer(), _axonaManager: am, returnRouteStats() { throw new Error('boom'); } }); }
  catch (e) { threw = e; }
  ok(threw === null, 'a throwing accessor does not take health() down', threw && threw.message);
  ok(h2 && h2.returnRoute === null, 'and reads null, not a clean zero', h2 && h2.returnRoute);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
