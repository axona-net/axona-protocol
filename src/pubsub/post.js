// =====================================================================
// post.js — Axona post structure: hashing, topic-id derivation, refs.
//
// A "post" is the unit of pub/sub traffic. It carries provenance via a
// publisher identifier and a content hash; the protocol layer treats
// the `content` field as opaque bytes (end-to-end argument — any
// encryption / structure lives inside the application).
//
// For the simulator we use nodeId strings in place of public keys.
// The hashing scheme is identical to what real-key deployment would
// use, so swapping nodeId → pubkey-bytes later is a one-line change.
//
// Topic identity is self-authenticating:
//
//     topic_id = sha256(publisher || ':' || topic_name)
//
// This means anyone querying metrics for a topic must sign with the key
// that hashes to the topic_id; relays can verify by recomputation alone,
// no central registry. (Signature verification itself is stubbed for
// the simulator and added in a later PR.)
//
// Post identity is content-addressed:
//
//     post_hash = sha256(canonical({
//       publisher, topic_id, topic_name, timestamp, content, references
//     }))
//
// where canonical() is JSON.stringify with sorted keys. The same post
// always hashes to the same value, regardless of platform or runtime.
// =====================================================================

import { createHash } from 'node:crypto';

/** Stable JSON encoding: object keys are sorted at every level so two
 *  semantically-identical posts hash to the same value across runs. */
function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return '[' + value.map(canonical).join(',') + ']';
  }
  const keys = Object.keys(value).sort();
  return '{' + keys.map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}

function sha256Hex(s) {
  return createHash('sha256').update(s).digest('hex');
}

/**
 * Derive a topic identifier from a publisher and a topic name.
 *
 * @param {string} publisher  Publisher identifier (nodeId in simulator;
 *                            public key bytes / fingerprint in production).
 * @param {string} topicName  Human-readable name. Two publishers with
 *                            the same topic_name produce DIFFERENT
 *                            topic_ids — names are scoped to publishers.
 * @returns {string}          Hex-encoded sha256 (64 chars).
 */
export function deriveTopicId(publisher, topicName) {
  return sha256Hex(publisher + ':' + topicName);
}

/**
 * @typedef {Object} PostRef
 * @property {string} topic_id   The topic the referenced post lives in
 * @property {string} post_hash  Content hash of the referenced post
 */

/**
 * @typedef {Object} SignedPost
 * @property {string}    post_hash
 * @property {string}    publisher
 * @property {string}    topic_id
 * @property {string}    topic_name
 * @property {number}    timestamp     Unix ms
 * @property {any}       content       Application-defined payload
 * @property {PostRef[]} references    Empty for original posts; non-empty
 *                                     for reshares (the reposted PostRef
 *                                     is the first element)
 * @property {string}    signature     Placeholder in the simulator
 */

/**
 * Construct a `SignedPost`, compute its hash, and stub a signature.
 *
 * The simulator's "signature" is a deterministic placeholder string —
 * sufficient for protocol-level testing of routing and counters; real
 * Ed25519 signatures are dropped in alongside identity keys in a later
 * PR. Receivers can already recompute post_hash and reject mismatches,
 * which catches relay-tampering without crypto.
 *
 * @param {Object}   args
 * @param {string}   args.publisher    NodeId (or pubkey fingerprint).
 * @param {string}   args.topicName    Topic name owned by `publisher`.
 * @param {any}      args.content
 * @param {PostRef[]} [args.references]
 * @param {number}   [args.timestamp]  Defaults to Date.now().
 * @returns {SignedPost}
 */
export function makePost({ publisher, topicName, content, references = [], timestamp }) {
  const topic_id = deriveTopicId(publisher, topicName);
  const ts = timestamp ?? Date.now();
  const draft = {
    publisher,
    topic_id,
    topic_name: topicName,
    timestamp:  ts,
    content,
    references,
  };
  const post_hash = sha256Hex(canonical(draft));
  return {
    post_hash,
    ...draft,
    signature: 'stub:' + publisher,    // replaced by Ed25519 in a later PR
  };
}

/**
 * Recompute a post's hash and verify it matches the stored field.
 *
 * Failed verification means the relay layer mangled (or forged) the
 * post — every conforming receiver drops it silently, no error
 * surfaced to the user. This is the §3.5 spec rule applied at the
 * receiving edge.
 */
export function verifyPostHash(post) {
  if (!post || typeof post !== 'object') return false;
  const recomputed = sha256Hex(canonical({
    publisher:  post.publisher,
    topic_id:   post.topic_id,
    topic_name: post.topic_name,
    timestamp:  post.timestamp,
    content:    post.content,
    references: post.references,
  }));
  return recomputed === post.post_hash;
}

/** Verify topic_id was derived from this publisher + topic_name. The
 *  cheap half of authenticity that needs no crypto. */
export function verifyTopicOwnership(post) {
  return post.topic_id === deriveTopicId(post.publisher, post.topic_name);
}
