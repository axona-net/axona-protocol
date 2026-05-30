// =====================================================================
// smoke_pubsub_authz.js — pub/sub subscribe authorization (finding C4).
//
// A direct subscribe (`pubsub:subscribe-k`) registers the subscriber as a
// child the axon relays the topic's full feed to — directly, by nodeId.
// If the axon trusted the payload's `subscriberId`, an attacker could name
// a victim and turn the axon into a reflection/amplification (DRDoS) source.
//
// axona/4 makes `meta.fromId` the *proven* channel peer, so the axon now
// requires `subscriberId === meta.fromId` on subscribe-k / unsubscribe-k.
// These tests drive the real AxonaManager handlers directly.
//
// Run: node test/smoke_pubsub_authz.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const hex   = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);  // 66-char
const big   = (h) => BigInt('0x' + h);

const ROOT      = hex(0x01);
const SUBSCRIBER= hex(0x02);
const ATTACKER  = hex(0x03);
const VICTIM    = hex(0x04);
const TOPIC     = hex(0x05);

function makeManager() {
  const sent = [];
  const dht = {
    getSelfId:        () => big(ROOT),
    onRoutedMessage:  () => {},
    onDirectMessage:  () => {},
    routeMessage:     () => {},
    sendDirect:       async (to, type, body) => { sent.push({ to, type, body }); return true; },
    findKClosest:     undefined,
  };
  const am = new AxonaManager({ dht });
  return { am, sent };
}

function childIds(am, topicHex) {
  const role = am.axonRoles.get(big(topicHex));
  if (!role) return null;
  return new Set([...role.children.keys()].map(k => k.toString()));
}

async function testLegitSubscribeAdmitted() {
  console.log('\n── legit subscribe (subscriberId === fromId) is admitted ──');
  const { am } = makeManager();
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER },
    { fromId: SUBSCRIBER });
  const kids = childIds(am, TOPIC);
  check('role created for topic', kids !== null);
  check('authenticated subscriber registered as child',
    kids && kids.has(big(SUBSCRIBER).toString()));
}

async function testSpoofedSubscribeDropped() {
  console.log('\n── spoofed subscribe (attacker names a victim) is dropped ──');
  const { am, sent } = makeManager();
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: VICTIM },   // claims the victim…
    { fromId: ATTACKER });                       // …but the proven sender is the attacker
  check('no role/children allocated for spoofed subscribe', childIds(am, TOPIC) === null);
  check('no replay/deliver fired at the victim',
    !sent.some(s => s.to === big(VICTIM)));
}

async function testNoFromIdBackCompat() {
  console.log('\n── missing fromId (local/sim context) keeps working ──');
  const { am } = makeManager();
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER },
    {});                                          // no fromId → cannot verify → admit
  const kids = childIds(am, TOPIC);
  check('subscriber admitted when fromId absent', kids && kids.has(big(SUBSCRIBER).toString()));
}

async function testUnsubscribeAuthz() {
  console.log('\n── unsubscribe requires the authenticated subscriber ──');
  const { am } = makeManager();
  // Seed a legit subscription first.
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });

  // Attacker tries to unsubscribe the victim's... actually the subscriber.
  am._onUnsubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: ATTACKER });
  check('spoofed unsubscribe does NOT remove the subscription',
    childIds(am, TOPIC).has(big(SUBSCRIBER).toString()));

  // The real subscriber can unsubscribe itself.
  am._onUnsubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });
  check('authenticated unsubscribe removes the subscription',
    !childIds(am, TOPIC).has(big(SUBSCRIBER).toString()));
}

async function main() {
  console.log('Axona pub/sub subscribe authorization (C4) smoke');
  await testLegitSubscribeAdmitted();
  await testSpoofedSubscribeDropped();
  await testNoFromIdBackCompat();
  await testUnsubscribeAuthz();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
