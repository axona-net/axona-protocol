// smoke_region_bridge.mjs — kernel 4.88.0: the SYSTEM region 0xFF 'bridge' (PLAN-v0.3 §6, PLAN-v0.4 §2.1 NARROW).
//   • resolveRegion accepts 'bridge' / 0xff / '0xff' and returns 0xff unfolded; 254 and 192 stay null.
//   • no coordinate produces 0xff; regionCenter('bridge') is null.
//   • resolveTopic accepts region 'bridge' ONLY for the open bridge-directory topic; any other name, an owned
//     directory, and a 0xFF node's region-omitted publish are refused at the mint.
//   • createNodeIdentity({ region: 'bridge' }) mints an id whose top byte is 0xff and whose suffix is still
//     bound to the pubkey; an unresolvable override is refused; a geo override is canonicalised.
//   • The ingest path re-derives from the signed descriptor: a non-directory 0xFF descriptor cannot be derived
//     there either (deriveTopicId throws → wireHandlers drop-bad-descriptor).
import assert from 'node:assert/strict';
import { resolveRegion, regionName, regionCenter, canonicalRegion, CANONICAL_REGIONS, regionNameForLatLng } from '../src/utils/region-names.js';
import { geoCellId, isSystemRegion, SYSTEM_REGION_BRIDGE, S2_CELL_COUNT } from '../src/utils/s2.js';
import { resolveTopic, deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { BRIDGE_DIRECTORY_TOPIC } from '../src/bridgeDirectory.js';
import { createNodeIdentity } from '../src/identity/index.js';
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

console.log(`\nsmoke_region_bridge: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
