// =====================================================================
// channel_ledger.js — the CHANNEL and PEER records of Hold-and-Fill
// (axona-docs 4334504 v0.5, 95c2ff4 v0.7, "Channels, peers and transitions"),
// repair row 3.
//
// WHAT IT IS. One record per RTCPeerConnection, keyed by a token `t` minted
// at allocation and never reused in the process (the mesh's per-PC
// incarnation tag, MeshManager._attachPc), with a state from
// ALLOCATED → NEGOTIATING → OPEN → CLOSING → GONE; and one record per bound
// identity (hex nodeId), pointing at its current channel token or none.
// Counts over those records are the bounds the design names:
//
//   chan(ALLOCATED ∪ NEGOTIATING ∪ OPEN ∪ CLOSING)   ≤ C_phys
//   chan(ALLOCATED ∪ NEGOTIATING, inbound, unbound)  ≤ C_inbound
//   chan(ALLOCATED ∪ NEGOTIATING, outbound)          ≤ P_pending   (the attempt bound)
//
// WHAT IT IS NOT, in this row. It is BOOKKEEPING AND A PREDICATE. It issues
// no dial, closes nothing, and by default REFUSES NOTHING: `mayAllocate()`
// answers, the mesh asks before every PC, and with `enforce: false` (the
// default) a refusal is counted (`wouldRefuse`) and the PC is built as
// today. That is rollout step 2 of the design: measure the open-channel
// counts on the running fleet before any bound is believed. `enforce: true`
// turns the same predicate into a refusal; nothing else changes. The
// accounting release decides the default; this row does not.
//
// The mesh's existing negotiation deadline (NEGOTIATION_DEADLINE_MS from
// state creation, mesh.js _armReaper) already bounds a channel that never
// sends a first frame, so ALLOCATED has no separate timer here; the record
// just says how long it sat there.
//
// GONE is confirmed by the transport (connectionState 'closed'). A PC that
// was closed locally and never reports 'closed' is escalated after
// `closeEscalateMs`: the record is released and `closeEscalated` counts it,
// so capacity cannot leak on an implementation that stays silent after
// close(). The design's `escalate(t)` row.
// =====================================================================

export const CHAN = Object.freeze({
  ALLOCATED: 'ALLOCATED', NEGOTIATING: 'NEGOTIATING', OPEN: 'OPEN', CLOSING: 'CLOSING', GONE: 'GONE',
});

const LIVE = new Set([CHAN.ALLOCATED, CHAN.NEGOTIATING, CHAN.OPEN, CHAN.CLOSING]);
const PRE_OPEN = new Set([CHAN.ALLOCATED, CHAN.NEGOTIATING]);

export const LEDGER_DEFAULTS = Object.freeze({
  // C_phys_req(cap) = cap + P_pending + C_inbound + 4 with cap 50 (the
  // parameter table of the design): 50 + 8 + 4 + 4.
  cPhys: 66,
  cInbound: 4,
  pPending: 8,
  closeEscalateMs: 10_000,
  enforce: false,
});

export class ChannelLedger {
  /**
   * @param {object} [opts]
   * @param {number}  [opts.cPhys]
   * @param {number}  [opts.cInbound]
   * @param {number}  [opts.pPending]
   * @param {number}  [opts.closeEscalateMs]
   * @param {boolean} [opts.enforce]   false: count would-be refusals; true: refuse
   * @param {() => number} [opts.now]
   * @param {(ev:string, data:object) => void} [opts.log]
   * @param {typeof setTimeout} [opts.setTimeout]  injectable for tests
   * @param {typeof clearTimeout} [opts.clearTimeout]
   */
  constructor(opts = {}) {
    const o = { ...LEDGER_DEFAULTS, ...(opts || {}) };
    this.cPhys    = Math.max(1, Number(o.cPhys)    || LEDGER_DEFAULTS.cPhys);
    this.cInbound = Math.max(1, Number(o.cInbound) || LEDGER_DEFAULTS.cInbound);
    this.pPending = Math.max(1, Number(o.pPending) || LEDGER_DEFAULTS.pPending);
    this.closeEscalateMs = Math.max(0, Number(o.closeEscalateMs) || LEDGER_DEFAULTS.closeEscalateMs);
    this.enforce  = o.enforce === true;
    this._now     = typeof o.now === 'function' ? o.now : Date.now;
    this._log     = typeof o.log === 'function' ? o.log : () => {};
    this._setTimeout   = typeof o.setTimeout === 'function' ? o.setTimeout : setTimeout;
    this._clearTimeout = typeof o.clearTimeout === 'function' ? o.clearTimeout : clearTimeout;

    /** @type {Map<string, {t:string, meshId:string, dir:'in'|'out', state:string, since:number, negotiatingAt:number, openedAt:number, closingAt:number, goneAt:number, reason:string|null, nodeId:string|null, escalateTimer:any}>} */
    this._chan = new Map();
    /** meshId → current token (the live channel for that signalling id). */
    this._tByMeshId = new Map();
    /** nodeId hex → { nodeId, t: string|null, boundAt: number } */
    this._peer = new Map();
    this._seq = 0;
    this._stats = {
      allocated: 0, refusedOut: 0, refusedIn: 0, wouldRefuseOut: 0, wouldRefuseIn: 0,
      closeEscalated: 0, staleEvent: 0, goneTotal: 0,
    };
  }

  // ── counts ─────────────────────────────────────────────────────────

  /** Live channel records (every state but GONE). */
  chanAll() { let n = 0; for (const c of this._chan.values()) if (LIVE.has(c.state)) n++; return n; }
  /** Inbound channels before OPEN with no bound identity. */
  chanInboundUnbound() {
    let n = 0;
    for (const c of this._chan.values()) if (c.dir === 'in' && PRE_OPEN.has(c.state) && c.nodeId == null) n++;
    return n;
  }
  /** Outbound channels before OPEN: the attempt count. */
  chanOutboundPending() {
    let n = 0;
    for (const c of this._chan.values()) if (c.dir === 'out' && PRE_OPEN.has(c.state)) n++;
    return n;
  }

  // ── the predicate ──────────────────────────────────────────────────

  /**
   * May a channel in direction `dir` be allocated now? Evaluated BEFORE the
   * PeerConnection is constructed; the increment happens in allocate(), in
   * the same synchronous step, so two callers in one macrotask see each
   * other's reservation. With `enforce` false the answer is always ok and
   * the refusal is counted.
   * @param {'in'|'out'} dir
   * @returns {{ ok: boolean, why: string|null }}
   */
  mayAllocate(dir) {
    let why = null;
    if (this.chanAll() >= this.cPhys) why = 'phys';
    else if (dir === 'in' && this.chanInboundUnbound() >= this.cInbound) why = 'inbound';
    else if (dir === 'out' && this.chanOutboundPending() >= this.pPending) why = 'pending';
    if (why == null) return { ok: true, why: null };
    if (this.enforce) {
      if (dir === 'in') this._stats.refusedIn++; else this._stats.refusedOut++;
      this._log('alloc-refused', { dir, why, all: this.chanAll(), inboundUnbound: this.chanInboundUnbound(), outboundPending: this.chanOutboundPending() });
      return { ok: false, why };
    }
    if (dir === 'in') this._stats.wouldRefuseIn++; else this._stats.wouldRefuseOut++;
    return { ok: true, why };   // counted, not refused
  }

  // ── transitions ────────────────────────────────────────────────────

  /**
   * ALLOCATED. Called with the mesh's per-PC incarnation tag as the token, so
   * the ledger and the mesh log lines join on one value.
   * @param {string} t   unique per PC in this process
   * @param {string} meshId
   * @param {'in'|'out'} dir
   */
  allocate(t, meshId, dir) {
    if (this._chan.has(t)) { this._stats.staleEvent++; return this._chan.get(t); }
    const rec = {
      t, meshId, dir: dir === 'in' ? 'in' : 'out', state: CHAN.ALLOCATED,
      since: this._now(), negotiatingAt: 0, openedAt: 0, closingAt: 0, goneAt: 0,
      reason: null, nodeId: null, escalateTimer: null, seq: ++this._seq,
    };
    this._chan.set(t, rec);
    this._tByMeshId.set(meshId, t);
    this._stats.allocated++;
    return rec;
  }

  /** NEGOTIATING: the first frame (offer sent, or offer received) on `t`. */
  negotiating(t) {
    const c = this._chan.get(t);
    if (!c || c.state !== CHAN.ALLOCATED) { if (!c || c.state === CHAN.GONE) this._stats.staleEvent++; return c ?? null; }
    c.state = CHAN.NEGOTIATING; c.negotiatingAt = this._now();
    return c;
  }

  /** OPEN: the data channel opened on `t`. */
  open(t) {
    const c = this._chan.get(t);
    if (!c || !PRE_OPEN.has(c.state)) { if (!c || c.state === CHAN.GONE) this._stats.staleEvent++; return c ?? null; }
    c.state = CHAN.OPEN; c.openedAt = this._now();
    return c;
  }

  /**
   * The handshake bound `nodeId` on the channel that currently serves
   * `meshId`. The peer record points at that channel; an older channel the
   * same identity pointed at is left to its own close (the duplicate case
   * is the kernel's bindPeer dedup, which closes the loser by meshId).
   * @param {string} meshId
   * @param {string} nodeId hex
   */
  bind(meshId, nodeId) {
    const t = this._tByMeshId.get(meshId);
    const c = t ? this._chan.get(t) : null;
    if (c && LIVE.has(c.state)) c.nodeId = nodeId;
    const p = this._peer.get(nodeId) ?? { nodeId, t: null, boundAt: 0 };
    p.t = c && LIVE.has(c.state) ? t : p.t;
    p.boundAt = this._now();
    this._peer.set(nodeId, p);
    return p;
  }

  /** The binding for `meshId` was dropped (unbindPeer). The peer record keeps
   *  pointing at another channel if one serves the identity, else at none. */
  unbind(meshId) {
    const t = this._tByMeshId.get(meshId);
    const c = t ? this._chan.get(t) : null;
    const nodeId = c?.nodeId ?? null;
    if (c) c.nodeId = null;
    if (nodeId != null) {
      const p = this._peer.get(nodeId);
      if (p && p.t === t) {
        // another live channel for this identity?
        let other = null;
        for (const o of this._chan.values()) if (o !== c && o.nodeId === nodeId && LIVE.has(o.state)) { other = o.t; break; }
        if (other) p.t = other; else this._peer.delete(nodeId);
      }
    }
  }

  /** CLOSING: a close was issued on `t` (the mesh's _retire). Capacity is
   *  NOT released here; it waits for gone(t) or the escalation. */
  closing(t, reason = 'close') {
    const c = this._chan.get(t);
    if (!c || c.state === CHAN.CLOSING || c.state === CHAN.GONE) { if (!c || c.state === CHAN.GONE) this._stats.staleEvent++; return c ?? null; }
    c.state = CHAN.CLOSING; c.closingAt = this._now(); c.reason = reason;
    if (this._tByMeshId.get(c.meshId) === t) this._tByMeshId.delete(c.meshId);
    if (c.nodeId != null) {
      const p = this._peer.get(c.nodeId);
      if (p && p.t === t) p.t = null;   // the pointer is cleared in the same step as CLOSING
    }
    if (this.closeEscalateMs > 0) {
      c.escalateTimer = this._setTimeout(() => {
        c.escalateTimer = null;
        if (c.state === CHAN.CLOSING) { this._stats.closeEscalated++; this._log('close-escalated', { t, meshId: c.meshId, reason: c.reason }); this.gone(t); }
      }, this.closeEscalateMs);
      try { c.escalateTimer?.unref?.(); } catch { /* browser timers have no unref */ }
    }
    return c;
  }

  /**
   * GONE: the transport confirmed the close (connectionState 'closed'), or
   * the escalation fired. Releases the record's capacity. An unprompted
   * close (no closing() before it) is the involuntary-loss row: the record
   * goes straight to GONE and `prompted` is false in the return.
   */
  gone(t) {
    const c = this._chan.get(t);
    if (!c) { this._stats.staleEvent++; return null; }
    if (c.state === CHAN.GONE) { this._stats.staleEvent++; return c; }
    const prompted = c.state === CHAN.CLOSING;
    if (c.escalateTimer) { try { this._clearTimeout(c.escalateTimer); } catch {} c.escalateTimer = null; }
    c.state = CHAN.GONE; c.goneAt = this._now();
    if (this._tByMeshId.get(c.meshId) === t) this._tByMeshId.delete(c.meshId);
    if (c.nodeId != null) {
      const p = this._peer.get(c.nodeId);
      if (p && p.t === t) p.t = null;
    }
    this._stats.goneTotal++;
    this._chan.delete(t);   // GONE records are not retained; the token is never reused
    c.prompted = prompted;
    return c;
  }

  /** Token of the live channel serving a signalling id, or null. */
  tokenFor(meshId) { return this._tByMeshId.get(meshId) ?? null; }

  /** Snapshot for status surfaces and the step-2 measurement. */
  stats() {
    const byState = { ALLOCATED: 0, NEGOTIATING: 0, OPEN: 0, CLOSING: 0 };
    let unboundOpen = 0, oldestPreOpenMs = 0, oldestClosingMs = 0;
    const now = this._now();
    for (const c of this._chan.values()) {
      if (byState[c.state] != null) byState[c.state]++;
      if (c.state === CHAN.OPEN && c.nodeId == null) unboundOpen++;
      if (PRE_OPEN.has(c.state)) oldestPreOpenMs = Math.max(oldestPreOpenMs, now - c.since);
      if (c.state === CHAN.CLOSING) oldestClosingMs = Math.max(oldestClosingMs, now - c.closingAt);
    }
    let peersPointing = 0;
    for (const p of this._peer.values()) if (p.t != null) peersPointing++;
    return {
      bounds: { cPhys: this.cPhys, cInbound: this.cInbound, pPending: this.pPending, enforce: this.enforce },
      all: this.chanAll(), byState,
      inboundUnbound: this.chanInboundUnbound(), outboundPending: this.chanOutboundPending(),
      unboundOpen, boundPeers: this._peer.size, peersPointing,
      oldestPreOpenMs, oldestClosingMs,
      ...this._stats,
    };
  }

  /** For tests: the record for a token, or null. */
  record(t) { return this._chan.get(t) ?? null; }
  /** For tests: the peer record for a nodeId hex, or null. */
  peer(nodeId) { return this._peer.get(nodeId) ?? null; }

  dispose() {
    for (const c of this._chan.values()) if (c.escalateTimer) { try { this._clearTimeout(c.escalateTimer); } catch {} c.escalateTimer = null; }
  }
}
