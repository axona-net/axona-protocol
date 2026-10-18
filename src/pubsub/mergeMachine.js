// =====================================================================
// mergeMachine.js — AxonaManager mixin: DUAL-ROOT RECONCILIATION, Part 4(d)
// of the design packet (axona-docs/architecture/Axona-Dual-Root-
// Reconciliation-v0.1.md, header v0.15, SHA 14a6bd8; David "build it"
// 2026-10-17). Everything here is behind `mergeCapable` (OFF by default);
// an unarmed node never sets a gate, never cuts, never forwards.
//
// THE CUT AND THE GATE (stage d2). When the loser L binds a merge round it
// CUTS and GATES in one synchronous run: `role.gen` increments and
// `role.merge.gate = {winner, winnerEpoch, corr, at}` is set. While the gate
// is set L stamps nothing. The cut is defined AT THE COMMIT, not at the
// handler's entry: every commit path captures {role0, gen0} once before its
// first await (`_cutCapture`) and each mutation point (`_cachePush`,
// `_applyKill`) re-checks `role === role0 && role.gen === gen0`
// synchronously, immediately before the write (`_cutHolds`). The gate alone
// is not the test: a cut followed by a demotion clears the gate, and a check
// against the gate, or against a `gen` re-read after the await, would pass a
// write admitted before the cut. A cut increments `gen`; nothing decrements
// it.
//
// THE GATE'S FORWARD is its own contract (packet 4(d), "Placement" to
// "Unreachable or refusing W"):
//   · `_mergeGateIntercept` runs in _onPub / _onKill directly after
//     _holdIntercept, for bare writes only (a via-pinned write is somebody's
//     forward, possibly ours to W). `_reroute` is never called for a gated
//     write: in a split its topic-id walk can return to L.
//   · The write goes to W BY NAME through `_forwardToWinner`, the sibling of
//     `_forwardToRoot`: same flight, same ack routing, two differences. It
//     demotes on NO verdict (only step 5 moves L's claim), and it opens the
//     flight with an EXPLICIT epoch, the bound `winnerEpoch`, never the beacon
//     record and never 0, so W's ack at any other epoch discharges nothing.
//   · Re-entry: the forward carries a `gateFwd` nonce in a bounded ledger of
//     the hold's shape; a bare write carrying L's own nonce that returns is
//     undeliverable `merge-gate-return`, never forwarded again, never stamped.
//   · A `failed` verdict, or a flight that passes INGEST_ACK_MS without W's
//     ack, ends the round: the gate clears, L keeps root under its epoch.
//   · W re-minted (an ack from W at a different epoch) aborts the round and
//     records the new epoch on the candidate for the next OFFER (d3).
//   · MERGE_TIMEOUT_MS bounds one gate interval from the bind (`_mergeSweep`).
//
// Stage d3 adds the exchange (OFFER / PULLUP with corr / header / REPLAYUP
// cursor / REPLAYACK / MERGE_CLOSED) and the release token at step 5; the
// bind in d3 calls `_mergeCut` below.
// =====================================================================
import { T, MERGE_TIMEOUT_MS, MERGE_GATE_FWD_MAX, MERGE_GATE_REENTRY_MS } from './constants.js';
import { idHex, idBig, lc } from './ids.js';
import { makeMergeState } from './rootClaim.js';
import { dispatchVerdict } from './dispatch.js';

const randomNonce = () => {
  const b = new Uint8Array(8); globalThis.crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
};

export const mergeMachineMethods = {

  // ── the cut ────────────────────────────────────────────────────────────
  /** Entry-time capture for a commit path: taken ONCE, synchronously, before the first await. */
  _cutCapture(role) {
    return role ? { role0: role, gen0: role.gen | 0 } : null;
  },
  /**
   * The check at the mutation point. True when the write may commit: the
   * role is the one captured, its generation has not moved, and no gate is
   * set (the cheap third refusal). A null `cut` (a caller with no capture)
   * is checked against the gate alone.
   */
  _cutHolds(role, cut) {
    if (!role) return false;
    if (role.merge?.gate) return false;
    if (!cut) return true;
    return role === cut.role0 && (role.gen | 0) === cut.gen0;
  },
  /**
   * CUT + GATE, one synchronous run, no await between. Called at the bind
   * (d3) and by the fences. Returns the gate record. A gate already set for
   * another corr is replaced only by the caller's explicit choice (d3's
   * supersession rule); here a second cut on a gated role is refused.
   */
  _mergeCut(topicBig, { winner, winnerEpoch, corr }) {
    const role = this.axonRoles.get(topicBig);
    if (!role || !role.isRoot) return null;
    if (!role.merge) role.merge = makeMergeState();
    if (role.merge.gate) return null;
    role.gen = (role.gen | 0) + 1;
    role.merge.gate = { winner: lc(winner), winnerEpoch: winnerEpoch | 0, corr: String(corr), at: this._now() };
    role.merge.corr = String(corr);
    this._log('info', 'merge-cut', { topic: idHex(topicBig).slice(0, 12), winner: lc(winner).slice(0, 10), winnerEpoch: winnerEpoch | 0, corr: String(corr).slice(0, 8), gen: role.gen, cache: role.cache.length });
    return role.merge.gate;
  },
  /** Clear the gate (the round ended, by completion or abort). `gen` is never decremented. */
  _mergeGateClear(role, why, extra = {}) {
    const g = role?.merge?.gate;
    if (!g) return false;
    role.merge.gate = null;
    role.merge.corr = null;
    this._log('info', 'merge-gate-clear', { topic: idHex(role.topicId).slice(0, 12), why, corr: g.corr.slice(0, 8), heldMs: this._now() - g.at, ...extra });
    return true;
  },
  /**
   * ABORT a round at L: the gate clears, L keeps root under its own epoch,
   * forwarded writes the winner never acked are the publisher's to re-send.
   * The next trigger issues a new OFFER with a new corr (d3).
   */
  _mergeAbort(role, why, extra = {}) {
    if (!role?.merge?.gate) return false;
    this._log('warn', 'merge-abort', { topic: idHex(role.topicId).slice(0, 12), why, corr: role.merge.gate.corr.slice(0, 8), ...extra });
    return this._mergeGateClear(role, why, extra);
  },
  /** MERGE_TIMEOUT_MS bounds ONE gate interval, measured from the bind. Driven from refreshTick. */
  _mergeSweep(now) {
    for (const role of this.axonRoles.values()) {
      const g = role.merge?.gate;
      if (!g) continue;
      if (now - g.at >= MERGE_TIMEOUT_MS) this._mergeAbort(role, 'timeout', { boundMs: MERGE_TIMEOUT_MS });
    }
  },

  // ── the gate's forward ─────────────────────────────────────────────────
  /**
   * In _onPub / _onKill directly after _holdIntercept. Returns true when the
   * write was handled here (forwarded, or marked undeliverable) and the
   * handler must return 'consumed' without stamping.
   */
  _mergeGateIntercept(topicBig, type, payload) {
    if (Array.isArray(payload.via) && payload.via.length) return false;   // a forward, never intercepted
    const role = this.axonRoles.get(topicBig);
    const gate = role?.merge?.gate || null;
    const fwd = role?.merge?.gateFwd || null;
    // RE-ENTRY: a bare write carrying a gateFwd token. Ours → consumed from the
    // ledger, undeliverable, never forwarded again, never stamped, whatever the
    // state of the gate (the publisher's flight recovers it). Unknown while
    // gated → fail closed. Unknown and no gate → nobody's forward that we can
    // tell; it is an ordinary bare write here.
    if (typeof payload.gateFwd === 'string') {
      const ours = !!(fwd && fwd.has(payload.gateFwd) && fwd.get(payload.gateFwd).type === type);
      if (ours) fwd.delete(payload.gateFwd);
      if (ours || gate) {
        this._log('info', 'merge-gate-reentry', { topic: idHex(topicBig).slice(0, 12), type, ours, gated: !!gate });
        this._undeliverable(type, topicBig, ours ? 'merge-gate-return' : 'merge-gate-return-unknown');
        return true;
      }
      return false;
    }
    if (!gate) return false;
    return this._mergeGateForward(topicBig, type, payload, role, gate);
  },
  _mergeGateForward(topicBig, type, payload, role, gate) {
    const now = this._now();
    const fwd = (role.merge.gateFwd ??= new Map());
    for (const [k, e] of fwd) { if (now - e.at >= MERGE_GATE_REENTRY_MS) fwd.delete(k); }
    if (fwd.size >= MERGE_GATE_FWD_MAX) { this._undeliverable(type, topicBig, 'merge-gate-saturated'); return true; }
    const nonce = randomNonce();
    fwd.set(nonce, { at: now, type, corr: gate.corr });
    this._log('info', 'merge-gate-forward', { topic: idHex(topicBig).slice(0, 12), type, to: gate.winner.slice(0, 10), corr: gate.corr.slice(0, 8) });
    this._forwardToWinner(topicBig, type, { ...payload, gateFwd: nonce }, gate);
    return true;
  },
  /**
   * The sibling of _forwardToRoot (AxonaManager.js): same dispatch, same
   * flight and ack routing, with (1) NO demote on any verdict and (2) the
   * flight opened under the EXPLICIT bound winnerEpoch. A `failed` verdict
   * ends the round. A `consumed` verdict is hop-local evidence and moves
   * nothing. The write is discharged at L only by W's ingest ack arriving at
   * _onIngestAck for this flight, bound to W and winnerEpoch.
   */
  _forwardToWinner(topicBig, type, payload, gate) {
    const declares = this.dht?.verdictsSupported;
    const w = gate.winner;
    let ackRoute = null;
    if (type === T.PUB || type === T.KILL) {
      ackRoute = this._flightOpen(topicBig, w, type, payload, payload?.attemptId ?? null, gate.winnerEpoch);
      const f = this._writeFlights?.get(`${topicBig.toString(16)}|${w}|${gate.winnerEpoch}`);
      if (f) f.merge = { corr: gate.corr };
    }
    const out = ackRoute
      ? { ...payload, via: [w], ackTo: ackRoute.ackTo, flightNonce: ackRoute.flightNonce, attemptId: ackRoute.attemptId }
      : { ...payload, via: [w] };
    const sent = this._send(type, out);
    Promise.resolve(sent).then((r) => {
      const v = dispatchVerdict(r, declares);
      if (v === 'violation') {
        this._log('error', 'pubsub:dispatch-contract-violation', { topic: idHex(topicBig).slice(0, 12), peer: w.slice(0, 12), detail: `adapter declares verdictsSupported but returned no verdict on ${type}` });
        return;
      }
      if (v !== 'failed') return;                              // consumed / unsupported: nothing moves
      const role = this.axonRoles.get(topicBig);
      if (role?.merge?.gate?.corr === gate.corr) this._mergeAbort(role, 'winner-unreachable', { type });
    });
  },

  /** Where the flight learns the round it belongs to is still open. */
  _mergeFlightLive(f) {
    if (!f?.merge) return false;
    const role = this.axonRoles.get(f.topicBig);
    return !!role?.merge?.gate && role.merge.gate.corr === f.merge.corr;
  },
  /** A merge flight's ack at the wrong epoch from the right node: W re-minted after the bind. */
  _mergeWinnerReminted(f, gotEpoch) {
    const role = this.axonRoles.get(f.topicBig);
    if (!role?.merge?.gate || role.merge.gate.corr !== f.merge.corr) return;
    const claim = { id: f.rootHex, epoch: gotEpoch | 0, source: 'reminted', at: this._now() };
    role.merge.candidate = claim;                          // the next OFFER carries the new epoch
    this._mergeAbort(role, 'winner-reminted', { got: gotEpoch | 0, want: f.epoch });
  },
  /** A merge flight passed INGEST_ACK_MS with no ack from W: the round ends. */
  _mergeFlightSilent(f) {
    const role = this.axonRoles.get(f.topicBig);
    if (!role?.merge?.gate || role.merge.gate.corr !== f.merge.corr) return;
    this._mergeAbort(role, 'winner-no-ack', { winner: f.rootHex.slice(0, 10) });
  },
};

export default mergeMachineMethods;
