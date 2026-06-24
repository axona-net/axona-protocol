// =====================================================================
// AxonaManager.js — Axona pub/sub: the routing-only axonic tree.
//
// Design: axona-docs/architecture/Pubsub-Axon-Tree-v0.1.md
//
// CLEAN BREAK (kernel v3.12.0). This replaces the K-closest / root-set /
// `sendDirect` implementation wholesale. The one rule:
//
//     Axona pub/sub uses ONLY DHT message routing. There are no direct
//     connections. Every interaction is a routed message delivered, hop by
//     hop, to the single live node closest to a 264-bit target.
//
// A message published to a topic is ROUTED toward the topic id; the closest
// live node is the (emergent, never-elected) ROOT. The root assigns a single
// monotonic timestamp — the serialization point that gives the topic a total
// order — caches it, and fans it out to its subscribers by routing a deliver
// to each. Subscribers renew toward the topic id every minute; that renewal
// is at once the keepalive, the failure detector, the self-heal, and (with a
// `since` hint) the gap-recovery. A subscriber carries an ordered `via`
// waypoint list so it can be pinned to a specific relay yet always fall back
// to the topic id if that waypoint is gone.
//
// THIS FILE IS PHASE 1: the routed core with a SINGLE root (no tree). Overload
// delegation (the tree), migration handoff, and stamped-replay-up durability
// are Phases 2–3. Side functions (kill/unpub/touch/pull/metrics/host) are kept
// thin and routing-only here — they are reworked after the core is proven, per
// the standing decision. Markers: TODO(Phase 2/3/4).
//
// What is GONE vs the old manager: sendDirect, findKClosest, K-closest fan-out
// (`*-k`), root sets, sub-axon recruitment, adopt/promote/dissolve, msgsync /
// kill-sync anti-entropy. None of it. The node never assumes a direct channel
// to a peer it discovered; it only ever calls dht.routeMessage(target, …).
// =====================================================================

import { verifyEnvelope, checkFreshness } from './envelope.js';
import { deriveTopicIdBig }               from './post.js';

// ── Inbound caps (D-1: bound attacker-controlled payloads) ──────────────
// Re-exported unchanged from the pre-clean-break manager — AxonaPeer and
// std/chunk import these as the publish-size contract; they are independent
// of the pub/sub mechanism, so the routing rewrite leaves them as-is.
export const MAX_PUBLISH_BYTES = 256 * 1024;         // absolute hard ceiling (chars)
// RELIABLE-delivery ceiling (finding O-5): the only size guaranteed receivable
// by every conformant WebRTC stack across arbitrary hops is the ~16 KiB interop
// floor. peer.pub rejects above this so oversize fails LOUD at the publisher
// instead of vanishing en route; larger payloads go through std/chunk. Set just
// under 16 KiB to leave headroom for the outer deliver frame.
export const MAX_RELIABLE_PUBLISH_BYTES = 15 * 1024;

// ── Tunable constants (design §Appendix) ────────────────────────────────
const RENEW_MS        = 60_000;          // re-subscribe cadence
const DROP_MS         = 180_000;         // evict a subscriber after 3 missed renewals
const CACHE_MAX       = 1024;            // messages cached per relay
const CACHE_BYTES     = 16 * 1024 * 1024;// byte ceiling on a relay's cache
const MAX_VIA         = 8;               // ordered-waypoint list length cap (wire sanity)
const VIA_HOP_BUDGET  = 8;               // hops per via leg (enforced kernel-side in Phase 2)
const TTL_MS          = 48 * 60 * 60 * 1000;   // 48h message hold, keyed on the ROOT timestamp
const APP_DEDUP_MAX   = 8192;            // exactly-once app-delivery LRU
const REPLAY_CHUNK_BYTES = 96 * 1024;    // byte budget per replay deliver batch

// ── Wire message types (all ROUTED) ─────────────────────────────────────
const T = {
  SUB:      'pubsub:sub',       // subscribe — routed toward topic id (or a via waypoint)
  UNSUB:    'pubsub:unsub',     // explicit unsubscribe (prompt removal; renewal-lapse also drops)
  PUB:      'pubsub:pub',       // publish — routed toward topic id; NO timestamp (root stamps)
  DELIVER:  'pubsub:deliver',   // one-or-more stamped messages — routed toward a subscriber id
  KILL:     'pubsub:kill',      // retract a message (thin; TODO Phase 4)
  UNPUB:    'pubsub:unpub',     // retract a topic's feed (thin; TODO Phase 4)
  TOUCH:    'pubsub:touch',     // extend TTL (thin; TODO Phase 4)
  PULL:     'pubsub:pull',      // on-demand fetch request — routed toward topic id
  PULLRESP: 'pubsub:pullresp',  // pull response — routed back toward the requester id
};

// ── id helpers (264-bit ids ⇄ 66-char hex) ──────────────────────────────
const idHex = (big) => big.toString(16).padStart(66, '0');
const idBig = (hex) => (typeof hex === 'bigint' ? hex : BigInt('0x' + String(hex)));
const lc    = (s) => String(s ?? '').toLowerCase();

/**
 * A relay's per-topic state. A node holds one Role for each topic it hosts —
 * as the root (Phase 1: always) or, from Phase 2, as a non-root relay that
 * renews up to a parent.
 */
function makeRole(topicId) {
  return {
    topicId,                         // bigint
    isRoot: true,                    // Phase 1: every host is the root
    subscribers: new Map(),          // subHex -> { since, via:[hex], lastRenewed }
    cache: [],                       // [{ msgId, publishTs, json, bytes }] asc by publishTs
    cacheIds: new Set(),             // msgId set for O(1) root-side dedup
    cacheBytes: 0,
    lastTs: 0,                       // highest stamp emitted (monotonic, root authority)
    tombstones: new Map(),           // msgId -> expireTs (kill; thin)
  };
}

export class AxonaManager {
  /**
   * @param {object} o
   * @param {object} o.dht  adapter: { getSelfId(), routeMessage(target,type,payload,opts?),
   *                         onRoutedMessage(type, handler) }. sendDirect/findKClosest are
   *                         NO LONGER USED — present-but-ignored for a drop-in adapter.
   * Legacy tunables (maxDirectSubs, rootSetSize, …) are accepted and ignored so
   * construction stays drop-in; the active knobs are renewMs/dropMs/replayCacheSize.
   */
  constructor({
    dht,
    now = () => Date.now(),
    emitLog = null,
    renewMs = RENEW_MS,
    dropMs = DROP_MS,
    refreshIntervalMs = 10_000,
    replayCacheSize = CACHE_MAX,
    replayCacheBytes = CACHE_BYTES,
    // accepted-and-ignored (clean break): pickRelayPeer, pickRecruitPeer,
    // shouldRecruitSubAxon, maxDirectSubs, minDirectSubs, rootSetSize,
    // crossFragmentRoots, maxSubscriptionAgeMs, rootGraceMs …
    ..._legacy
  } = {}) {
    if (!dht || typeof dht.routeMessage !== 'function' || typeof dht.getSelfId !== 'function'
        || typeof dht.onRoutedMessage !== 'function') {
      throw new TypeError('AxonaManager: dht with routeMessage + getSelfId + onRoutedMessage required');
    }
    this.dht    = dht;
    this.nodeId = dht.getSelfId();          // bigint, 264-bit
    this._now   = now;
    this._logSink = (typeof emitLog === 'function') ? emitLog : null;

    this.renewMs = renewMs;
    this.dropMs  = dropMs;
    this.refreshIntervalMs = refreshIntervalMs;
    this._cacheMax   = replayCacheSize || CACHE_MAX;
    this._cacheBytes = replayCacheBytes || CACHE_BYTES;

    // Public/inspectable state (contract surface).
    this.axonRoles      = new Map();   // topicIdBig -> Role  (topics I host)
    this.mySubscriptions = new Map();  // topicIdBig -> { since, via:[hex], lastRenewSent, host:false }
    this._hostedTopics  = new Set();   // topicIdBig hosted without app consumption
    this._lastSeenTsByTopic = new Map(); // topicIdBig -> ts  (AxonaPeer seeds `since` here)

    // Internal.
    this._appDelivered    = new Map(); // "topicHex:msgId" -> true  (exactly-once LRU)
    this._deliveryCallback = null;
    this._hostKeyspace    = false;
    this._pending         = new Map(); // pull corrId -> { resolve, timer }
    this._pullSeq         = 0;
    this._timer           = null;

    this._registerHandlers();
  }

  // ── handler registration ──────────────────────────────────────────────
  _registerHandlers() {
    const on = (type, fn) => this.dht.onRoutedMessage(type, (p, m) => fn.call(this, p, m));
    on(T.SUB,      this._onSub);
    on(T.UNSUB,    this._onUnsub);
    on(T.PUB,      this._onPub);
    on(T.DELIVER,  this._onDeliver);
    on(T.KILL,     this._onKill);
    on(T.UNPUB,    this._onUnpub);
    on(T.TOUCH,    this._onTouch);
    on(T.PULL,     this._onPull);
    on(T.PULLRESP, this._onPullResp);
  }

  // ── routing core ────────────────────────────────────────────────────
  //
  // `via` is an ordered waypoint list. We route toward via[0] (a specific node
  // id) if present, else toward the topic id. The terminus reconciles against
  // the AUTHORITATIVE topic id: a dead waypoint is simply popped and routing
  // continues toward the next target. A message is never orphaned by a stale
  // via. (Per-via hop budgeting is enforced kernel-side in Phase 2; today the
  // global MAX_HOPS backstop bounds the journey, and via lists are length ≤1.)
  _send(type, payload) {
    const via = Array.isArray(payload.via) ? payload.via : [];
    const target = via.length ? idBig(via[0]) : idBig(payload.topicId);
    // fromId stamps the routed message's originId = us.
    this.dht.routeMessage(target, type, payload, { fromId: idHex(this.nodeId), viaHopBudget: VIA_HOP_BUDGET });
  }

  // Pop the dead/served waypoint and route on toward the next target.
  _reroute(type, payload) {
    payload.via = (Array.isArray(payload.via) ? payload.via : []).slice(1);
    this._send(type, payload);
  }

  // Decide what a topic-targeted message (SUB/PUB) should do at this node.
  //   'host'    — I already host this topic → handle locally
  //   'root'    — I am the routing terminus for the topic id → become root + handle
  //   'reroute' — the current via waypoint is gone (I'm merely closest to it) → pop + route on
  //   'forward' — keep routing (return falsy so the kernel forwards)
  _topicDecision(payload, meta) {
    const topicBig = idBig(payload.topicId);
    const via = Array.isArray(payload.via) ? payload.via : [];
    if (this.axonRoles.has(topicBig)) return 'host';
    if (via.length && idBig(via[0]) === this.nodeId) return 'reroute'; // named via[0] but I don't host it
    if (meta.isTerminal) return via.length ? 'reroute' : 'root';        // closest-to-via[0] (dead) vs closest-to-topic
    return 'forward';
  }

  // ── SUBSCRIBE ────────────────────────────────────────────────────────
  _onSub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;                       // keep routing
    if (d === 'reroute') { this._reroute(T.SUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    let role = this.axonRoles.get(topicBig);
    if (!role) role = this._becomeRoot(topicBig);      // 'root': I am the closest node

    // subscriberId is self-asserted (meta.fromId is the previous hop, not the
    // origin). A forged id only makes the root route delivers to a node that
    // ignores them — harmless. Cryptographic origin-binding is a Phase 2 item.
    const subHex  = lc(payload.subscriberId);
    if (!/^[0-9a-f]{1,66}$/.test(subHex)) return 'consumed';
    const sinceTs = Number.isFinite(payload.since) ? payload.since : 0;
    role.subscribers.set(subHex, {
      since: sinceTs,
      via: Array.isArray(payload.via) ? payload.via.slice(0, MAX_VIA) : [],
      lastRenewed: this._now(),
    });
    // Replay the cache delta (since the subscriber's hint) to the new/renewing
    // subscriber. On a renewal with an up-to-date `since` this is empty — the
    // periodic re-subscribe doubles as gap recovery without re-flooding.
    this._replayTo(role, subHex, sinceTs);
    return 'consumed';
  }

  _onUnsub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.UNSUB, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (role) role.subscribers.delete(lc(payload.subscriberId));
    return 'consumed';
  }

  // ── PUBLISH ──────────────────────────────────────────────────────────
  async _onPub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.PUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    let role = this.axonRoles.get(topicBig);
    if (!role) role = this._becomeRoot(topicBig);
    // TODO(Phase 2): a non-root RELAY must forward a publish UP toward the root,
    // not stamp it. In Phase 1 every host is the root, so isRoot is always true.
    if (!role.isRoot) { this._reroute(T.PUB, { ...payload, via: [] }); return 'consumed'; }

    await this._ingestPublish(role, payload.json);
    return 'consumed';
  }

  // Root ingress: authenticate, enforce write policy, stamp, cache, fan out.
  async _ingestPublish(role, json) {
    let env;
    try { env = JSON.parse(json); } catch { this._log('warn', 'drop-unparseable'); return; }

    // B-4: verify the signature + content-derived msgId at ingress.
    const v = await verifyEnvelope(env);
    if (!v.ok) { this._log('warn', 'drop-bad-envelope', { reason: v.reason }); return; }

    // C-2: freshness — reject a stale/replayed LIVE publish (the signed ts, not
    // the wire publishTs). The replay path serves older cache deliberately; this
    // is the live-ingress gate only.
    const fr = checkFreshness(env, { now: this._now() });
    if (!fr.ok) { this._log('warn', 'drop-stale', { reason: fr.reason }); return; }

    // Write policy: recompute the topic id from the SIGNED descriptor; it must
    // match the topic this root serves, and an owner-only topic admits only the
    // owner's signature. (Defense in depth — peer.pub pre-checks too.)
    const desc = env.topic;
    let tid;
    try { tid = await deriveTopicIdBig({ region: desc.region, owner: desc.owner, name: desc.name, write: desc.write }); }
    catch { this._log('warn', 'drop-bad-descriptor'); return; }
    if (tid !== role.topicId) { this._log('warn', 'drop-topic-mismatch'); return; }
    if (desc.write === 'owner') {
      if (!env.signerPubkey || lc(env.signerPubkey) !== lc(desc.owner)) {
        this._log('warn', 'drop-write-policy', { topic: desc.name }); return;
      }
    }

    // Root-side idempotency: a message already cached (re-published or looped) is
    // not re-stamped or re-fanned.
    if (role.cacheIds.has(env.msgId)) return;

    // STAMP — the root is the topic's single serialization point. Monotonic:
    // strictly greater than its previous stamp, floored at local time. This ts
    // orders the queue and starts the 48h clock.
    const ts = Math.max(role.lastTs + 1, this._now());
    role.lastTs = ts;

    this._cachePush(role, { msgId: env.msgId, publishTs: ts, json });
    this._fanout(role, { json, publishTs: ts, msgId: env.msgId });
  }

  // ── DELIVER (root/relay → subscriber) ────────────────────────────────
  _onDeliver(payload, meta) {
    // A deliver is routed to a specific subscriber id. Consume only at the
    // target; intermediate hops forward (return falsy).
    if (meta.targetId !== this.nodeId) return;
    const topicBig = idBig(payload.topicId);

    // Remember the relay we received from, as the head of our renewal `via`
    // chain — this is what pins us to our relay (and migrates with it).
    if (payload.from) this._setVia(topicBig, lc(payload.from));

    for (const m of (Array.isArray(payload.msgs) ? payload.msgs : [])) {
      // TODO(Phase 2): if I am a non-root RELAY for topicBig, re-fan `m` to my
      // own subscribers before/while delivering locally.
      if (m && m.del) this._deliverDelete(topicBig, m);
      else if (m)     this._deliverToApp(topicBig, m.json, m.msgId, m.publishTs);
    }
    return 'consumed';
  }

  // Fan a stamped message out to every subscriber by ROUTING a deliver to each
  // (self-loopback delivers locally). Phase 1: the root is the only fan-out point.
  _fanout(role, msg) {
    const base = { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [msg] };
    for (const subHex of role.subscribers.keys()) {
      const subBig = idBig(subHex);
      if (subBig === this.nodeId) {                  // loopback
        if (msg.del) this._deliverDelete(role.topicId, msg);
        else         this._deliverToApp(role.topicId, msg.json, msg.msgId, msg.publishTs);
        continue;
      }
      this.dht.routeMessage(subBig, T.DELIVER, { ...base }, { fromId: idHex(this.nodeId) });
    }
  }

  // Replay the cache delta (publishTs > since) to one subscriber, chunked by
  // bytes so a deliver never blows the transport frame cap.
  _replayTo(role, subHex, sinceTs) {
    const subBig = idBig(subHex);
    const isSelf = subBig === this.nodeId;
    let batch = [], bytes = 0;
    const flush = () => {
      if (!batch.length) return;
      if (isSelf) for (const m of batch) this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs);
      else this.dht.routeMessage(subBig, T.DELIVER,
        { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: batch }, { fromId: idHex(this.nodeId) });
      batch = []; bytes = 0;
    };
    for (const c of role.cache) {
      if (c.publishTs <= sinceTs) continue;
      if (bytes + c.bytes > REPLAY_CHUNK_BYTES) flush();
      batch.push({ json: c.json, publishTs: c.publishTs, msgId: c.msgId });
      bytes += c.bytes;
    }
    flush();
  }

  // ── cache ────────────────────────────────────────────────────────────
  _cachePush(role, entry) {
    entry.bytes = (entry.json ? entry.json.length : 0) + 80;
    role.cache.push(entry);
    role.cacheIds.add(entry.msgId);
    role.cacheBytes += entry.bytes;
    while (role.cache.length > this._cacheMax || role.cacheBytes > this._cacheBytes) {
      const old = role.cache.shift();
      if (!old) break;
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
    }
  }

  _expireCache(role, now) {
    while (role.cache.length && (now - role.cache[0].publishTs) > TTL_MS) {
      const old = role.cache.shift();
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
    }
  }

  // ── app delivery (exactly-once) ──────────────────────────────────────
  _deliverToApp(topicBig, json, msgId, publishTs) {
    // Only the app of a node that actually SUBSCRIBED hears deliveries; a pure
    // host/relay stores+forwards without consuming.
    if (!this.mySubscriptions.has(topicBig)) return;
    const key = topicBig.toString(16) + ':' + msgId;
    if (this._appDelivered.has(key)) return;             // exactly-once
    this._appDelivered.set(key, true);
    if (this._appDelivered.size > APP_DEDUP_MAX) {
      this._appDelivered.delete(this._appDelivered.keys().next().value);
    }
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (publishTs > prev) this._lastSeenTsByTopic.set(topicBig, publishTs);
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, json, msgId, publishTs); }
      catch (e) { this._log('warn', 'delivery-callback-threw', { err: e?.message }); }
    }
  }

  // A delete marker (kill) is delivered on the same app path but must bypass the
  // content dedup (it shares the killed message's msgId).
  _deliverDelete(topicBig, m) {
    if (!this.mySubscriptions.has(topicBig)) return;
    if (this._deliveryCallback) {
      try {
        this._deliveryCallback(topicBig,
          JSON.stringify({ deleted: true, msgId: m.msgId, topic: m.topic ?? null }),
          m.msgId, m.publishTs ?? this._now());
      } catch (e) { this._log('warn', 'delete-callback-threw', { err: e?.message }); }
    }
  }

  // ── becoming a root / via tracking ───────────────────────────────────
  _becomeRoot(topicBig) {
    const role = makeRole(topicBig);
    this.axonRoles.set(topicBig, role);
    this._log('info', 'root-formed', { topic: idHex(topicBig).slice(0, 12) });
    return role;
  }

  _setVia(topicBig, relayHex) {
    const s = this.mySubscriptions.get(topicBig);
    if (s) s.via = [relayHex];                 // single waypoint in Phase 1
  }

  // ── public API (contract surface) ────────────────────────────────────

  /** Publish: route an UN-stamped message toward the topic; the root stamps it. */
  pubsubPublish(topicId, json, meta = {}) {
    this._send(T.PUB, { topicId: idHex(topicId), via: [], json });
    return meta.postHash || '';
  }

  /** Subscribe: route a subscribe toward the topic (pinned by our remembered via). */
  pubsubSubscribe(topicId) {
    const seeded = this._lastSeenTsByTopic.get(topicId);
    const since  = Number.isFinite(seeded) ? seeded : this._now();
    const prev   = this.mySubscriptions.get(topicId);
    const via    = prev?.via || [];
    this.mySubscriptions.set(topicId, { since, via, lastRenewSent: this._now(), host: false });
    this._sendSubscribe(topicId, since, via);
  }

  _sendSubscribe(topicId, since, via) {
    this._send(T.SUB, {
      topicId: idHex(topicId),
      via: (Array.isArray(via) ? via : []).slice(0, MAX_VIA),
      subscriberId: idHex(this.nodeId),
      since,
    });
  }

  /** Unsubscribe: stop renewing (natural DROP_MS drop) + prompt explicit removal. */
  pubsubUnsubscribe(topicId) {
    const s = this.mySubscriptions.get(topicId);
    this.mySubscriptions.delete(topicId);
    if (s) this._send(T.UNSUB, { topicId: idHex(topicId), via: s.via || [], subscriberId: idHex(this.nodeId) });
    this.pubsubResetTopicConsumption(topicId);
  }

  /** Forget per-topic consumption (lastSeen + app dedup) so a re-sub re-reads. */
  pubsubResetTopicConsumption(topicId) {
    this._lastSeenTsByTopic.delete(topicId);
    const prefix = topicId.toString(16) + ':';
    for (const k of this._appDelivered.keys()) if (k.startsWith(prefix)) this._appDelivered.delete(k);
  }

  /** Host a topic without consuming it — be a durable participant/root if closest. */
  pubsubHost(topicId) {
    this._hostedTopics.add(topicId);
    // Pre-warm: a subscribe toward the topic makes us its root if we are closest
    // (we self-assert our own id; _deliverToApp suppresses app delivery since the
    // topic is not in mySubscriptions). TODO(Phase 2): proper host-as-relay.
    this._send(T.SUB, { topicId: idHex(topicId), via: [], subscriberId: idHex(this.nodeId), since: this._now() });
  }

  pubsubUnhost(topicId) {
    this._hostedTopics.delete(topicId);
    const role = this.axonRoles.get(topicId);
    if (role) role.subscribers.delete(lc(idHex(this.nodeId)));
  }

  /** Volunteer to host topics near this node's id. TODO(Phase 2): organic in the routed model. */
  pubsubHostKeyspace(on = true) { this._hostKeyspace = !!on; }

  /** Retract one message (thin; TODO Phase 4). Routes to the root; root drops it + delete-markers subs. */
  pubsubKill(topicId, kill) { this._send(T.KILL, { topicId: idHex(topicId), via: [], kill }); }

  /** Retract a topic feed (thin; TODO Phase 4). */
  pubsubUnpub(topicId, unpub) { this._send(T.UNPUB, { topicId: idHex(topicId), via: [], unpub }); }

  /** Extend hold time (thin; TODO Phase 4). */
  pubsubTouch(topicId, touch) { this._send(T.TOUCH, { topicId: idHex(topicId), via: [], touch }); }

  /** Pull: routed on-demand fetch of the latest (or a specific) cached message. */
  requestPull(topicId, postHash = null, { timeoutMs = 1000 } = {}) {
    const corrId = idHex(this.nodeId).slice(0, 8) + ':' + (++this._pullSeq);
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this._pending.delete(corrId); resolve(null); }, timeoutMs);
      if (typeof timer.unref === 'function') timer.unref();
      this._pending.set(corrId, { resolve, timer });
      this._send(T.PULL, {
        topicId: idHex(topicId), via: [], corrId, postHash: postHash || null,
        requesterId: idHex(this.nodeId),
      });
    });
  }

  /** Metrics: deferred to Phase 4 (per the standing decision). Benign empty shape. */
  requestMetrics() { return Promise.resolve({ accumulated: [] }); }

  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
  setLogSink(fn) { this._logSink = (typeof fn === 'function') ? fn : null; }
  invalidateKClosestCache() { /* no K-closest cache in the routed model — no-op */ }

  resetState() {
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._hostedTopics.clear();
    this._lastSeenTsByTopic.clear();
    this._appDelivered.clear();
    for (const p of this._pending.values()) clearTimeout(p.timer);
    this._pending.clear();
  }

  // ── side-function handlers (thin; TODO Phase 4) ──────────────────────
  _onKill(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.KILL, payload); return 'consumed'; }
    const topicBig = idBig(payload.topicId);
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig);
    const msgId = payload.kill?.msgId;
    if (msgId && role.cacheIds.has(msgId)) {
      const i = role.cache.findIndex(c => c.msgId === msgId);
      if (i >= 0) { role.cacheBytes -= role.cache[i].bytes; role.cache.splice(i, 1); }
      role.cacheIds.delete(msgId);
      role.tombstones.set(msgId, this._now() + TTL_MS);
      this._fanout(role, { del: true, msgId, topic: payload.kill?.topic ?? null, publishTs: this._now() });
    }
    return 'consumed';
  }

  _onUnpub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.UNPUB, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (role) { role.cache = []; role.cacheIds.clear(); role.cacheBytes = 0; }
    return 'consumed';
  }

  _onTouch(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.TOUCH, payload); return 'consumed'; }
    return 'consumed';   // TODO(Phase 4): per-message TTL extension
  }

  _onPull(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.PULL, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    let hit = null;
    if (role) {
      hit = payload.postHash
        ? role.cache.find(c => c.msgId === payload.postHash)
        : role.cache[role.cache.length - 1];
    }
    const reqBig = idBig(payload.requesterId);
    const resp = {
      corrId: payload.corrId, json: hit ? hit.json : null,
      publishTs: hit ? hit.publishTs : null, requesterId: payload.requesterId,
    };
    if (reqBig === this.nodeId) this._onPullResp(resp, { targetId: this.nodeId });
    else this.dht.routeMessage(reqBig, T.PULLRESP, resp, { fromId: idHex(this.nodeId) });
    return 'consumed';
  }

  _onPullResp(payload, meta) {
    if (meta.targetId !== this.nodeId && idBig(payload.requesterId) !== this.nodeId) return;
    const p = this._pending.get(payload.corrId);
    if (!p) return 'consumed';
    clearTimeout(p.timer);
    this._pending.delete(payload.corrId);
    let parsed = null;
    if (payload.json) { try { parsed = JSON.parse(payload.json); } catch { parsed = null; } }
    p.resolve(parsed ? (parsed.message ?? parsed) : null);
    return 'consumed';
  }

  // ── lifecycle: the renewal + eviction + TTL sweep ────────────────────
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => { this.refreshTick().catch(() => {}); }, this.refreshIntervalMs);
    if (typeof this._timer.unref === 'function') this._timer.unref();
  }

  stop() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
  }

  async refreshTick() {
    const now = this._now();

    // 1. Renew my subscriptions (and hosted topics) toward the topic id. This is
    //    keepalive + self-heal + (via the `since` hint) gap recovery.
    for (const [topicBig, s] of this.mySubscriptions) {
      if (now - s.lastRenewSent >= this.renewMs) {
        s.lastRenewSent = now;
        const since = this._lastSeenTsByTopic.get(topicBig);
        this._sendSubscribe(topicBig, Number.isFinite(since) ? since : s.since, s.via);
      }
    }
    for (const topicBig of this._hostedTopics) {
      this._send(T.SUB, { topicId: idHex(topicBig), via: [], subscriberId: idHex(this.nodeId), since: now });
    }

    // 2. Evict subscribers that stopped renewing; expire cache; tear down a role
    //    that is empty and not locally needed.
    for (const [topicBig, role] of this.axonRoles) {
      for (const [subHex, sub] of role.subscribers) {
        if (now - sub.lastRenewed > this.dropMs) role.subscribers.delete(subHex);
      }
      for (const [msgId, exp] of role.tombstones) if (exp <= now) role.tombstones.delete(msgId);
      this._expireCache(role, now);
      if (role.subscribers.size === 0 && !this.mySubscriptions.has(topicBig) && !this._hostedTopics.has(topicBig)) {
        this.axonRoles.delete(topicBig);
      }
    }
  }

  _log(level, event, ctx) {
    if (this._logSink) { try { this._logSink(level, 'pubsub:' + event, ctx); } catch { /* sink threw */ } }
  }
}

export default AxonaManager;
