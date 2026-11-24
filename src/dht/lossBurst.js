// =====================================================================
// lossBurst.js — "A node that lost everyone starts over" v0.6, trigger 1.
//
// A BURST: more than half of the node's open channels close inside a short
// window, AND fewer than three channels to nodes that are not a bridge are
// left. David, 2026-11-24: "over 50% of its connections", "burst window of ten
// seconds"; and, on Orion's LSO-3, "Let's add the second condition".
//
// The case it is cut to: a process on a Mac logged 54 connections closed in
// two seconds, opened one two seconds later, and went on as the root of a
// topic with that one channel for nine and a half hours.
//
// This file DECIDES. It closes nothing and resets nothing. It is pure: the
// caller hands it the clock and the counts.
//
// How the half is measured. The window holds the deaths of the last windowMs.
// The baseline is the number of channels open just before the OLDEST death in
// the window. The burst fires when deaths-in-window is MORE than half that
// baseline and nonBridgeLeft is under the floor. One closing a minute never
// has two deaths in one window against a baseline of any size worth the name.
//
// It fires ONCE per burst: after firing it is spent until the node has been
// back at the floor or above, so a node that stays thin does not fire again on
// every further death.
// =====================================================================
export const LOSS_BURST_WINDOW_MS = 10_000;
export const LOSS_BURST_SHARE     = 0.5;
export const LOSS_BURST_FLOOR     = 3;
// A node that never had this many channels has nothing to lose "most" of: two
// channels closing on a three-node testnet is not the event this is cut to.
export const LOSS_BURST_MIN_BASE  = 4;

export class LossBurst {
  constructor({ windowMs = LOSS_BURST_WINDOW_MS, share = LOSS_BURST_SHARE, floor = LOSS_BURST_FLOOR, minBase = LOSS_BURST_MIN_BASE } = {}) {
    this.windowMs = windowMs; this.share = share; this.floor = floor; this.minBase = minBase;
    this._deaths = [];        // [{ at, openBefore }]
    this._spent = false;
    this.fired = 0;
  }
  /**
   * One channel closed.
   * @param {number} now            ms
   * @param {number} openBefore     channels open just before this one closed (it included)
   * @param {number} nonBridgeLeft  channels to non-bridge nodes open now
   * @returns {null | {closed:number, base:number, left:number, windowMs:number}} the burst, once
   */
  death(now, openBefore, nonBridgeLeft) {
    if (!Number.isFinite(now) || !Number.isFinite(openBefore) || !Number.isFinite(nonBridgeLeft)) return null;
    if (nonBridgeLeft >= this.floor) this._spent = false;
    this._deaths.push({ at: now, openBefore });
    while (this._deaths.length && now - this._deaths[0].at > this.windowMs) this._deaths.shift();
    const base = this._deaths[0].openBefore, closed = this._deaths.length;
    if (this._spent || base < this.minBase) return null;
    if (closed > base * this.share && nonBridgeLeft < this.floor) {
      this._spent = true; this.fired++;
      return { closed, base, left: nonBridgeLeft, windowMs: this.windowMs };
    }
    return null;
  }
  /** The node is seen at or above the floor again: a later burst may fire. */
  recovered(nonBridgeLeft) { if (nonBridgeLeft >= this.floor) this._spent = false; }
}
