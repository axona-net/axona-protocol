// testCapability.mjs — REF-1.1 E3b (Aster ASTER-E3B-HOLD): the EXPLICIT TEST-ONLY
// deposit helper.
//
// After E3b, production registerFrame has NO generic literal-name fallback: a
// receiver can register a frame ONLY if it has deposited a dispatch capability. Real
// transports and the AxonaPeer deposit in their constructors. A test that needs a
// bare object (or a hand-rolled double) to act as a sealed frame receiver deposits a
// capability HERE, explicitly — the production path gains no test affordance, and the
// "capability presence is mandatory" invariant holds identically in tests and prod.
//
// The deposited closures store into standard per-kind Maps on the receiver
// (_reqHandlers / _ntfHandlers / _routedHandlers), so a test can both register a frame
// through registerFrame and assert on the stored handler.

import { depositDispatchCapability } from '../../src/registry/index.js';

/**
 * Deposit a dispatch capability onto an existing receiver object, making it a valid
 * sealed frame receiver for registerFrame. Creates the standard handler Maps if absent.
 * @param {object} recv
 * @param {Array<'request'|'notification'|'routed'>} [kinds]
 * @returns {object} recv
 */
export function depositTestCapability(recv, kinds = ['request', 'notification', 'routed']) {
  if (!recv._reqHandlers)    recv._reqHandlers    = new Map();
  if (!recv._ntfHandlers)    recv._ntfHandlers    = new Map();
  if (!recv._routedHandlers) recv._routedHandlers = new Map();
  const caps = {};
  if (kinds.includes('request'))      caps.request      = (type, h) => recv._reqHandlers.set(type, h);
  if (kinds.includes('notification')) caps.notification = (type, h) => recv._ntfHandlers.set(type, h);
  if (kinds.includes('routed'))       caps.routed       = (type, h) => recv._routedHandlers.set(type, h);
  depositDispatchCapability(recv, caps);
  return recv;
}

/** A fresh bare sealed frame receiver with a deposited capability. */
export function makeSealedReceiver(kinds) {
  return depositTestCapability({}, kinds);
}

/**
 * Seal a bare test receiver that carries raw primitive METHODS (onRequest /
 * onNotification / onRoutedMessage), depositing a capability for each method it
 * actually defines, each delegating to that method. This is the general test-only
 * seal for the door/boundary smokes that hand-roll a receiver and call registerFrame
 * directly (production registerFrame now MANDATES a deposited capability — no
 * literal-name fallback). A receiver missing a given primitive gets no cap for that
 * leg, so registerFrame still refuses that leg — preserving the primitive-selection
 * semantics those tests assert, now expressed through the capability channel.
 * Mutates and returns recv.
 * @param {object} recv
 * @returns {object} recv
 */
export function sealByOwnMethods(recv) {
  const caps = {};
  if (typeof recv.onRequest === 'function')       caps.request      = (t, h) => recv.onRequest(t, h);
  if (typeof recv.onNotification === 'function')  caps.notification = (t, h) => recv.onNotification(t, h);
  if (typeof recv.onRoutedMessage === 'function') caps.routed       = (t, h) => recv.onRoutedMessage(t, h);
  depositDispatchCapability(recv, caps);
  return recv;
}

/**
 * Seal a TEST DOUBLE of the DHT adapter (the receiver AxonaManager registers its
 * B1 pub/sub frames on) so registerFrame reaches its routed primitive through the
 * capability channel — production registerFrame has no fallback. The double keeps
 * its own onRoutedMessage closure (test-only; the fence scans src/ not test/); this
 * helper deposits a routed capability that delegates to it, storing handlers exactly
 * where the double already stores them. Mutates and returns `dht`.
 *
 * This IS the "explicit test-only construction/deposit helper" Aster's E3b HOLD
 * sanctions — never used in production, which requires every receiver to deposit.
 * @param {object} dht
 * @returns {object} dht
 */
export function sealTestDht(dht) {
  if (dht && typeof dht.onRoutedMessage === 'function') {
    depositDispatchCapability(dht, { routed: (type, h) => dht.onRoutedMessage(type, h) });
  }
  return dht;
}

export default depositTestCapability;
