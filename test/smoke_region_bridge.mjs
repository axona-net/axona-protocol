// smoke_region_bridge.mjs — kernel 4.88.0: the SYSTEM region 0xFF 'bridge' (PLAN-v0.3 §6, PLAN-v0.4 §2.1 NARROW).
//   • resolveRegion accepts 'bridge' / 0xff / '0xff' and returns 0xff unfolded; 254 and 192 stay null.
//   • no coordinate produces 0xff; regionCenter('bridge') is null.
//   • resolveTopic accepts region 'bridge' ONLY for the open bridge-directory topic; any other name, an owned
//     directory, and a 0xFF node's region-omitted publish are refused at the mint.
//   • createNodeIdentity({ region: 'bridge' }) mints an id whose top byte is 0xff and whose suffix is still
//     bound to the pubkey; an unresolvable override is refused; a geo override is canonicalised.
//   • The ingest path re-derives from the signed descriptor: a SIGNED non-directory or owned-directory 0xFF
//     envelope through the real _ingestPublish is refused as 'bad-descriptor'; the open directory is accepted.
//   • dumpIdentity/loadIdentity round-trip an override identity (code/name validated, id derived from the code);
//     legacy geo-only envelopes unchanged; tampered override metadata refused (Aster CP 22a7023e).
import assert from 'node:assert/strict';
import { resolveRegion, regionName, regionCenter, canonicalRegion, CANONICAL_REGIONS, regionNameForLatLng } from '../src/utils/region-names.js';
import { geoCellId, isSystemRegion, SYSTEM_REGION_BRIDGE, S2_CELL_COUNT } from '../src/utils/s2.js';
import { resolveTopic, deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { BRIDGE_DIRECTORY_TOPIC } from '../src/bridgeDirectory.js';
import { createNodeIdentity, createAuthorIdentity, dumpIdentity, loadIdentity } from '../src/identity/index.js';
import { IdentityError, ErrorCodes } from '../src/errors.js';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { sealTestDht } from './lib/testCapability.mjs';
import { computeNodeIdBigInt } from '../src/identity/nodeid.js';
import { pubkeyMatchesNodeId } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
const check = (name, ok) => { if (ok) { passed++; console.log('  ✓ ' + name); } else { failed++; console.log('  ✗ ' + name); } };
const throwsWith = async (fn, re) => { try { await fn(); return false; } catch (e) { return re.test(String(e?.message || e)); } };

console.log('── system region 0xFF resolves, never folds, never from a coordinate ──');
check('SYSTEM_REGION_BRIDGE === 0xff', SYSTEM_REGION_BRIDGE === 0xff);
check("resolveRegion('bridge') === 0xff", resolveRegion('bridge') === 0xff);
check("resolveRegion('BRIDGE') === 0xff (case-insensitive)", resolveRegion('BRIDGE') === 0xff);
check('resolveRegion(0xff) === 0xff', resolveRegion(0xff) === 0xff);
check("resolveRegion('255') === 0xff", resolveRegion('255') === 0xff);
check('canonicalRegion(0xff) === 0xff (no fold basin)', canonicalRegion(0xff) === 0xff);
check("regionName(0xff) === 'bridge'", regionName(0xff) === 'bridge');
check('isSystemRegion(0xff) && !isSystemRegion(254) && !isSystemRegion(192) && !isSystemRegion(0x89)', isSystemRegion(0xff) && !isSystemRegion(254) && !isSystemRegion(192) && !isSystemRegion(0x89));
check('resolveRegion(254) === null, resolveRegion(192) === null (reserved band stays reserved)', resolveRegion(254) === null && resolveRegion(192) === null);
check("regionCenter('bridge') === null", regionCenter('bridge') === null);
check('CANONICAL_REGIONS.length === 84 (the system region is not a major)', CANONICAL_REGIONS.length === 84);
{ let hit = false; for (let la = -89; la <= 89; la += 11) for (let ln = -179; ln <= 179; ln += 13) { const c = geoCellId(la, ln, 8); if (c >= S2_CELL_COUNT) hit = true; }
  check('geoCellId over a lat/lng grid never returns a reserved id', !hit); }
check("regionNameForLatLng never says 'bridge'", regionNameForLatLng(38, -77) !== 'bridge' && regionNameForLatLng(0, 0) !== 'bridge');

console.log('── the directory is the only topic of the system region ──');
const dir = await resolveTopic({ region: 'bridge', name: BRIDGE_DIRECTORY_TOPIC });
check("resolveTopic({region:'bridge', name: directory}) → id starts with 'ff'", dir.topicId.startsWith('ff') && dir.region === 0xff && dir.write === 'open');
const dirUseast = await resolveTopic({ region: 'useast', name: BRIDGE_DIRECTORY_TOPIC });
check("the useast copy is a different id starting with '89'", dirUseast.topicId.startsWith('89') && dirUseast.topicId !== dir.topicId);
check("resolveTopic({region:'bridge', name:'app/anything'}) throws", await throwsWith(() => resolveTopic({ region: 'bridge', name: 'app/anything' }), /system region that holds only/));
check("resolveTopic({region: 0xff, name:'chat/general'}) throws", await throwsWith(() => resolveTopic({ region: 0xff, name: 'chat/general' }), /system region/));
check('an OWNED directory in the system region throws', await throwsWith(() => resolveTopic({ region: 'bridge', name: BRIDGE_DIRECTORY_TOPIC, owner: 'ab'.repeat(32) }), /system region/));
check("a 0xFF node's region-omitted app publish (selfRegion 255) throws", await throwsWith(() => resolveTopic({ name: 'app/x' }, 255), /system region/));
check("a 0xFF node's region-omitted DIRECTORY publish (selfRegion 255) is allowed", (await resolveTopic({ name: BRIDGE_DIRECTORY_TOPIC }, 255)).topicId.startsWith('ff'));
check('deriveTopicId / deriveTopicIdBig refuse the same descriptor (the ingest re-derivation path)', (await throwsWith(() => deriveTopicId({ region: 'bridge', name: 'app/x' }), /system region/)) && (await throwsWith(() => deriveTopicIdBig({ region: 'bridge', name: 'app/x' }), /system region/)));
check('ordinary regions are unaffected', (await resolveTopic({ region: 'eagle', name: 'app/x' })).topicId.startsWith('89'));

console.log('── identity override ──');
const geo = await createNodeIdentity({ lat: 38, lng: -77 });
const br = await createNodeIdentity({ lat: 38, lng: -77, region: 'bridge' });
check("createNodeIdentity({lat,lng}) keeps the geo byte (0x89 for 38,-77)", geo.id.startsWith('89') && geo.region.code === undefined);
check("createNodeIdentity({lat,lng, region:'bridge'}) mints an 'ff' id", br.id.startsWith('ff'));
check("…and records region.code 0xff / region.name 'bridge' beside lat/lng", br.region.code === 0xff && br.region.name === 'bridge' && br.region.lat === 38 && br.region.lng === -77);
check('the 256-bit suffix is still bound to the pubkey (handshake-auth)', (await pubkeyMatchesNodeId(br.pubkey, br.id)) === true);
check('…and a different pubkey does not match it', (await pubkeyMatchesNodeId(geo.pubkey, br.id)) === false);
{ const a = BigInt('0x' + br.id), b = await computeNodeIdBigInt(br.pubkey, 38, -77, { regionCode: 0xff }); check('computeNodeIdBigInt with regionCode 0xff reproduces the id', a === b); }
check('a geo override is canonicalised like the geo path (0x89 for the eagle code)', (await createNodeIdentity({ lat: 0, lng: 0, region: 'eagle' })).id.startsWith('89'));
check("an unresolvable override is refused, never silently geo", await throwsWith(() => createNodeIdentity({ lat: 38, lng: -77, region: 'nowhere' }), /does not resolve/));
check('a reserved non-system code is refused', await throwsWith(() => computeNodeIdBigInt(br.pubkey, 38, -77, { regionCode: 254 }), /neither a geo cell nor a system region/));


// ── persistence round-trip (Aster CP 22a7023e #1: loadIdentity derived from lat/lng only and refused every
//    override identity as IDENTITY_INVALID_FORMAT; it now validates and honours the persisted code/name) ──
console.log('── dump / load round-trip with a region override ──');
{
  const env1 = await dumpIdentity(br);
  check('dumpIdentity keeps region.code/name beside lat/lng', env1.region.code === 0xff && env1.region.name === 'bridge' && env1.region.lat === 38);
  const re1 = await loadIdentity(JSON.parse(JSON.stringify(env1)));
  check("loadIdentity accepts the 'bridge' identity and returns the same id", re1.id === br.id && re1.pubkeyHex === br.pubkeyHex);
  check('…with region.code/name preserved', re1.region.code === 0xff && re1.region.name === 'bridge' && re1.region.lat === 38 && re1.region.lng === -77);
  const env2 = await dumpIdentity(re1);
  const re2 = await loadIdentity(env2);
  check('a second dump/load round-trip is stable (envelope region identical, id identical)', JSON.stringify(env2.region) === JSON.stringify(env1.region) && re2.id === br.id);
  const msg = new TextEncoder().encode('after reload'); const sig = await re2.sign(msg);
  check('the reloaded override identity still signs and verifies', (await re2.verify(msg, sig)) === true);
  // Aster's second case: a GEO override at coordinates that fold elsewhere (0,0 is not eagle).
  const eagleAt0 = await createNodeIdentity({ lat: 0, lng: 0, region: 'eagle' });
  const reEagle = await loadIdentity(await dumpIdentity(eagleAt0));
  check("region:'eagle' at (0,0) round-trips (code 0x89 recorded and derived from)", reEagle.id === eagleAt0.id && reEagle.region.code === 0x89 && reEagle.region.name === 'eagle');
  // Legacy geo-only envelope: no code, no name — the lat/lng derivation exactly as before.
  const envGeo = await dumpIdentity(geo);
  check('a legacy geo-only envelope carries no code/name', envGeo.region.code === undefined && envGeo.region.name === undefined);
  const reGeo = await loadIdentity(envGeo);
  check('…and still loads by the lat/lng derivation, without code/name', reGeo.id === geo.id && reGeo.region.code === undefined);
  // Tampered / malformed override metadata — every one refused, none loaded as something else.
  const fmt = (e) => e instanceof IdentityError && e.code === ErrorCodes.IDENTITY_INVALID_FORMAT;
  const refuses = async (envelope, re) => { try { await loadIdentity(envelope); return false; } catch (e) { return fmt(e) && re.test(e.message); } };
  const strip = ({ code, name, ...rest }) => rest;
  check('code+name removed from an override envelope → id mismatch (not silently a geo identity)', await refuses({ ...env1, region: strip(env1.region) }, /does not match derived id/));
  check('code swapped to 0x89 on an 0xff id → id mismatch', await refuses({ ...env1, region: { ...env1.region, code: 0x89, name: 'eagle' } }, /does not match derived id/));
  check("name 'eagle' with code 0xff → refused (name/code disagree)", await refuses({ ...env1, region: { ...env1.region, name: 'eagle' } }, /is not the name of region code/));
  check("code as the string 'bridge' → refused (code must be an integer)", await refuses({ ...env1, region: { ...env1.region, code: 'bridge' } }, /region.code must be/));
  check('reserved non-system code 254 → refused', await refuses({ ...env1, region: { ...env1.region, code: 254, name: undefined } }, /region.code must be/));
  { const nc = [...Array(192).keys()].find((c) => canonicalRegion(c) !== c);
    check(`a non-canonical geo code (fold-basin cell ${nc}) → refused`, await refuses({ ...env1, region: { ...env1.region, code: nc, name: undefined } }, /region.code must be/)); }
  check('name without code → refused', await refuses({ ...env1, region: { lat: 38, lng: -77, name: 'bridge' } }, /region.code must be/));
  check('the keypair correspondence check still holds under an override (privkey from another identity)', await refuses({ ...env1, privkey: (await dumpIdentity(geo)).privkey }, /does not correspond|correspondence/));
  check('the id corruption check still holds under an override', await refuses({ ...env1, id: 'ff' + '0'.repeat(64) }, /does not match derived id/));
}

// ── the WIRE path (Aster CP 22a7023e #2): signed envelopes through the real root ingress ──
//    _ingestPublish runs verifyEnvelope + checkFreshness + deriveTopicIdBig + the write-policy check. A signed
//    non-directory or owned-directory descriptor in region 0xFF is refused there as 'bad-descriptor' (the
//    mint throws → wireHandlers drop-bad-descriptor); the open directory is the control that is accepted; a
//    derivable-but-foreign descriptor is the OTHER class ('topic-mismatch'), so the two are told apart.
console.log('── signed ingest: only the open directory lands in a 0xFF root ──');
{
  const idHexOf = (b) => b.toString(16).padStart(66, '0');
  function mk(selfId, logs) {
    const clock = { t: 1_700_000_000_000 };
    const dht = {
      verdictsSupported: true, getSelfId: () => selfId, onRoutedMessage: () => {},
      routeMessage: async () => ({ consumed: true, hops: 2 }),
      findKClosest: async () => [idHexOf(selfId ^ 0x11n), idHexOf(selfId ^ 0x22n)],
      neighbors: () => [idHexOf(selfId ^ 0x11n), idHexOf(selfId ^ 0x22n)],
      bridgeId: () => null,
      lookup: async () => ({ path: [idHexOf(selfId ^ 0x11n), idHexOf(selfId ^ 0x22n)] }),
    };
    const am = new AxonaManager({ dht: sealTestDht(dht), now: () => clock.t, rootReplicas: 2 });
    am.nodeId = selfId; am.setLogSink((lvl, ev, d) => logs.push([lvl, ev, d]));
    return { am, clock };
  }
  async function rootFor(desc, logs) {
    const topicId = await deriveTopicIdBig(desc);
    const selfId = ((topicId >> 256n) << 256n) | 0x5eedn;      // a node in the topic's own region
    const { am, clock } = mk(selfId, logs);
    am.pubsubSubscribe(topicId);
    const role = am._becomeRoot(topicId);
    return { am, clock, topicId, role };
  }
  const author = await createAuthorIdentity();
  const DIR = { region: 'bridge', owner: null, name: BRIDGE_DIRECTORY_TOPIC, write: 'open' };
  const logs = [];
  const { am, clock, topicId, role } = await rootFor(DIR, logs);
  check("a root for the 'bridge' directory exists in region 0xff", role?.topicId === topicId && (topicId >> 256n) === 0xffn);
  const ingest = async (desc) => {
    // a DISTINCT message per case: msgId = H(publisher, message), so a repeated body would hit the cache dedup, not the descriptor check
    const env = await buildEnvelope({ topic: desc, message: { k: desc.name, owner: desc.owner, region: desc.region }, seq: 1, identity: author, ts: clock.t });
    const r = await am._ingestPublish(role, JSON.stringify(env));
    return { env, r };
  };
  const ctl = await ingest(DIR);
  check('control: the signed open-directory publish is ACCEPTED by the real ingress (cached under its msgId)', ctl.r?.ok === true && role.cacheIds.has(ctl.env.msgId));
  const app = await ingest({ region: 'bridge', owner: null, name: 'app/anything', write: 'open' });
  check("a signed app-topic publish in region 'bridge' is refused as bad-descriptor", app.r?.ok === false && app.r.reason === 'bad-descriptor' && !role.cacheIds.has(app.env.msgId));
  check('…and the root logged drop-bad-descriptor', logs.some(([, ev]) => ev === 'pubsub:drop-bad-descriptor'));
  const owned = await ingest({ region: 'bridge', owner: author.authorId ?? author.pubkeyHex, name: BRIDGE_DIRECTORY_TOPIC, write: 'owner' });
  check('a signed OWNED directory publish in region 0xff is refused as bad-descriptor (signer IS the owner, so only the region rule refuses it)', owned.r?.ok === false && owned.r.reason === 'bad-descriptor' && !role.cacheIds.has(owned.env.msgId));
  const numeric = await ingest({ region: 255, owner: null, name: 'chat/general', write: 'open' });
  check('the numeric spelling (region 255) is refused the same way', numeric.r?.ok === false && numeric.r.reason === 'bad-descriptor');
  const foreign = await ingest({ region: 'eagle', owner: null, name: 'app/anything', write: 'open' });
  check("a derivable descriptor for another topic is the OTHER class: topic-mismatch, not bad-descriptor", foreign.r?.ok === false && foreign.r.reason === 'topic-mismatch');
  const logs2 = []; const EAGLE = { region: 'eagle', owner: null, name: 'app/anything', write: 'open' };
  const ordinary = await rootFor(EAGLE, logs2);
  const env2 = await buildEnvelope({ topic: EAGLE, message: { k: 2 }, seq: 1, identity: author, ts: ordinary.clock.t });
  const r2 = await ordinary.am._ingestPublish(ordinary.role, JSON.stringify(env2));
  check('control: the same app descriptor in an ordinary region is accepted by its own root', r2?.ok === true && ordinary.role.cacheIds.has(env2.msgId));
  check('the directory root holds exactly the one accepted message', role.cache.length === 1);
}

console.log(`\nsmoke_region_bridge: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
