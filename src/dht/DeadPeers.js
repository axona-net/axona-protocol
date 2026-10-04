/**
 * DeadPeers — the dead-peer mark table.
 *
 * Hold-and-Fill v0.5 (axona-docs 4334504), repair row 1: a mark carries a
 * reason. Until this change `node._deadPeers` was a bare Set<bigint>; a peer
 * was either dead or not, and nothing recorded why or when. The design's
 * later rows (expiry, the retry token, the policy set) all key off the
 * reason, so the reason goes in first, on its own, with NO behaviour change:
 *
 *   - has / delete / size / clear are Map's and behave as the Set's did, so
 *     every reader (greedy routing, _localCandidate, findk probes, recruit,
 *     lookup) sees exactly what it saw before.
 *   - add(id) is kept as a Set-compatible writer, because the bridge
 *     (bridge_axona_node.js, 2.145.0) writes `node._deadPeers.add(deadId)`
 *     against the kernel tag it pins. A mark written that way has cause
 *     'unknown', which is what the kernel's own log line already says for a
 *     transport that supplies no reason (4.76.3).
 *
 * A mark is { kind, cause, at }. `kind` is from the design's closed set and
 * is 'loss' for everything the kernel writes today; 'policy' arrives with
 * row 10. `cause` is the transport-level close cause threaded through mesh
 * _retire → onPeerLost → onPeerDied, or 'unknown'. `at` is Date.now().
 */
export class DeadPeers extends Map {
  /**
   * Write a mark. Replaces an existing mark for the same id, so the table
   * records the LATEST loss, which is what expiry (row 10) will time from.
   * @param {bigint} id
   * @param {{ kind?: string, cause?: string, at?: number }} [mark]
   * @returns {this}
   */
  mark(id, mark = {}) {
    const kind  = (typeof mark.kind === 'string' && mark.kind) ? mark.kind : 'loss';
    const cause = (typeof mark.cause === 'string' && mark.cause) ? mark.cause : 'unknown';
    const at    = Number.isFinite(mark.at) ? mark.at : Date.now();
    this.set(id, { kind, cause, at });
    return this;
  }

  /**
   * Set-compatible writer for consumers that predate the mark (the bridge).
   * MEMBERSHIP ONLY: an id already marked keeps its mark. The bridge's
   * handler and the kernel's fire for the SAME death, in either order; the
   * kernel's carries the cause and the bridge's carries nothing, so a
   * no-information add must never overwrite a known cause or move `at`
   * (Aster fb79c09e). A mark persists until the peer re-binds, so there is
   * no second death to record under the same mark.
   * @param {bigint} id
   * @returns {this}
   */
  add(id) {
    return this.has(id) ? this : this.mark(id);
  }
}
