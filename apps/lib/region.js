// region.js — resolve the anchor region for an Axona app instance.
//
// THE FIX for the hidden us-east dependency: an app must derive BOTH the peer's
// own identity AND its topic anchor from the SAME region, so a co-located
// deployment is a self-contained keyspace — local nodes root local topics and
// the bridge is only the WebSocket rendezvous, never a data root. Hardcoding
// us-east for both (the old behavior) put every participant + every topic in one
// region's keyspace, so anyone elsewhere could never be K-closest to anything.
//
// Precedence:  ?region=<name|hex>  ›  `fallback`  ›  DEFAULT_REGION
// The token is a region name ("uswest") or an 8-bit hex code ("0x80").
//
// CONVERGENCE RULE: all participants of a topic MUST resolve to the SAME region,
// or they compute different topic-ids and never find each other. For a regional
// deployment everyone shares the same URL (so the same ?region=); for a shared
// channel, put the region in the link so a joiner adopts it (see buildShareUrl).

import { geoCellId, geoCellCenter }        from '/src/utils/s2.js';
import { resolveRegion, regionName }       from '/src/utils/region-names.js';

// Backward-compatible default: the existing live network + the bench collector
// live in us-east, so an app with no ?region= keeps converging with them.
export const DEFAULT_REGION = 'useast';

/**
 * @param {object} [opts]
 * @param {string} [opts.search]    location.search to read ?region= from.
 * @param {string} [opts.fallback]  region used when no ?region= is present.
 * @returns {{ token, code, name, center:{lat,lng}, publisher }}
 *          `center` → deriveIdentity({lat,lng}); `publisher` → the topic anchor.
 */
export function resolveAnchor({ search = (typeof location !== 'undefined' ? location.search : ''),
                                fallback = DEFAULT_REGION } = {}) {
  const param = new URLSearchParams(search).get('region');
  const token = (param || fallback || DEFAULT_REGION);
  let code = resolveRegion(token);
  if (code == null) code = resolveRegion(DEFAULT_REGION);   // unknown token → safe default
  const center    = geoCellCenter(code);
  const prefixHex = code.toString(16).padStart(2, '0');
  return {
    token, code, name: regionName(code), center,
    publisher: prefixHex + '0'.repeat(64),                 // synthetic-publisher anchor (prefix + 64 zeros)
  };
}

/**
 * A share/QR URL that carries the region, so whoever opens it resolves to the
 * SAME keyspace as the creator (and thus the same topic-ids). Preserves any
 * existing query params and merges in region (+ optional extras like ?join=).
 */
export function buildShareUrl(regionToken, extraParams = {}) {
  const url = new URL(location.origin + location.pathname);
  const p = new URLSearchParams(location.search);
  p.set('region', regionToken);
  for (const [k, v] of Object.entries(extraParams)) p.set(k, v);
  url.search = p.toString();
  return url.toString();
}
