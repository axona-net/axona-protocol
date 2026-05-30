// =====================================================================
// smoke_pubsub_replay.js — replay idempotency on (re)subscribe.
//
// Regression for the "earlier messages keep reappearing every ~10 s" bug:
// a node that is both a root/axon AND a subscriber to a topic re-issues
// subscribe-k to itself on every refreshTick.  The self-replay branch of
// _maybeSendReplay used to re-fire the entire replay cache to the local
// app callback with no dedup, so cached publishes were re-delivered on
// every refresh.  The fix funnels ALL delivery through _deliverToApp,
// which delivers each publishId to the app exactly once — distinct from
// the network-level _seenPublishes set (which is marked even when the app
// never subscribed, so reusing it would instead DROP legitimate backlog).
//
// Run: node test/smoke_pubsub_replay.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const hex = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);
const big = (h) => BigInt('0x' + h);

const SELF   = hex(0x01);
const OTHER  = hex(0x02);
const TOPIC  = hex(0x05);

function makeManager() {
  const dht = {
    getSelfId:       () => big(SELF),
    onRoutedMessage: () => {},
    onDirectMessage: () => {},
    routeMessage:    () => {},
    sendDirect:      async () => true,
    findKClosest:    undefined,
  };
  const am = new AxonaManager({ dht });
  const delivered = [];                         // publishIds delivered to the app
  am._deliveryCallback = (_topicId, _json, publishId) => delivered.push(publishId);
  return { am, delivered };
}

// Simulate a publish arriving at this node (as a root) from `fromId`.
async function deliverPublish(am, publishId, ts, fromId = OTHER) {
  await am._onPublishDirect(
    { topicId: TOPIC, json: JSON.stringify({ m: publishId }), publishId, publishTs: ts,
      publisher: OTHER },
    { fromId });
}
// Simulate a (re)subscribe-k to self — what refreshTick does every interval.
async function selfResubscribe(am) {
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SELF }, { fromId: SELF });
}

async function testNoReDeliveryOnRefresh() {
  console.log('\n── self-subscribed root does NOT re-deliver on every resubscribe ──');
  const { am, delivered } = makeManager();
  await selfResubscribe(am);                    // become root + self-subscribe
  await deliverPublish(am, 'p1', 101);
  await deliverPublish(am, 'p2', 102);
  await deliverPublish(am, 'p3', 103);
  check('each publish delivered once on arrival', delivered.length === 3);

  // Simulate several refresh-driven self-resubscribes.
  await selfResubscribe(am);
  await selfResubscribe(am);
  await selfResubscribe(am);
  check('no re-delivery across 3 resubscribes (still 3 total)', delivered.length === 3);
  check('delivered set is exactly p1,p2,p3',
    delivered.slice().sort().join(',') === 'p1,p2,p3');

  // A genuinely new publish still gets through after the refreshes.
  await deliverPublish(am, 'p4', 104);
  check('a new publish after refreshes still delivers', delivered.length === 4 && delivered.includes('p4'));
}

async function testLazyAxonCatchUpStillWorks() {
  console.log('\n── lazy-axon backlog is still delivered exactly once on subscribe ──');
  const { am, delivered } = makeManager();
  // The node relays publishes as a root BEFORE its local app subscribes.
  // (self is not yet a child, so these are cached but not delivered.)
  await deliverPublish(am, 'b1', 201);
  await deliverPublish(am, 'b2', 202);
  check('backlog not delivered before local subscribe', delivered.length === 0);

  // Now the local app subscribes — it should catch up on the backlog ONCE.
  await selfResubscribe(am);
  check('backlog delivered once on first subscribe', delivered.length === 2);

  // Subsequent refreshes must not re-deliver the caught-up backlog.
  await selfResubscribe(am);
  await selfResubscribe(am);
  check('no re-delivery of backlog on later refreshes', delivered.length === 2);
}

async function main() {
  console.log('Axona pub/sub replay-idempotency smoke');
  await testNoReDeliveryOnRefresh();
  await testLazyAxonCatchUpStillWorks();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
