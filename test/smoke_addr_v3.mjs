import assert from 'node:assert';
import { deriveTopicId } from '../src/pubsub/post.js';
import { resolveRegion, keyDerivedRegion } from '../src/utils/region-names.js';
const OWNER = 'a'.repeat(64);
let n=0; const ok=(m)=>console.log(`  ok ${++n} - ${m}`);

const t1 = await deriveTopicId({ region:'useast', name:'lobby' });
assert.equal(t1.length, 66);
assert.equal(t1.slice(0,2), resolveRegion('useast').toString(16).padStart(2,'0'), 'prefix == useast byte');
ok('open topic (region,name) → region-prefixed 66-hex');

assert.equal(t1, await deriveTopicId({ region:'useast', name:'lobby' }), 'deterministic');
assert.equal(t1, await deriveTopicId({ region:0x89, name:'lobby' }), 'region code == name');
ok('deterministic + region code/name equivalence');

const owned = await deriveTopicId({ region:'useast', owner:OWNER, name:'feed', write:'owner' });
const open  = await deriveTopicId({ region:'useast', owner:OWNER, name:'feed', write:'open' });
assert.notEqual(owned, open, 'write policy changes the topic id');
ok('owner-only vs open are distinct topics');

const kd = await deriveTopicId({ owner:OWNER, name:'profile', write:'owner' });
assert.equal(kd.slice(0,2), (await keyDerivedRegion(OWNER)).toString(16).padStart(2,'0'), 'key-derived prefix');
ok('key-derived placement (region omitted + owner)');

let threw=false; try { await deriveTopicId({ name:'lobby' }); } catch { threw=true; }
assert.ok(threw, 'open topic without region throws'); ok('no global region: open topic requires a region');

threw=false; try { await deriveTopicId({ region:'useast', name:'x', write:'owner' }); } catch { threw=true; }
assert.ok(threw); ok("write:'owner' requires an owner");

console.log(`\nsmoke_addressing v3: ${n} checks passed`);
