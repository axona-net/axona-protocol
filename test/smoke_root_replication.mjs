// smoke_root_replication.mjs — singleton-root replication to N nearest backups.
//   1. a singleton root re-pushes its full cache to its N nearest reachable
//      neighbours (excl self + bridge); farther nodes + bridge are NOT targeted
//   2. a root WITH a sub-axon tree (children) does NOT replicate (relays hold cache)
//   3. receiving a REPLICATE makes a node a passive BACKUP (backupOf set, not root)
//   4. a backup whose root stopped replicating (stale) promotes to root iff closest
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
let n=0,fail=0; const ok=(m,c)=>{ if(c){console.log(`  ok ${++n} - ${m}`)}else{console.log(`  ✗  ${m}`);fail++} };
const REG=0x87n<<248n, idHex=(b)=>b.toString(16).padStart(66,'0');
const SELF=REG|0x011n, TOPIC=REG|0xabcn;
const NEAR1=REG|0xab0n, NEAR2=REG|0xabfn, FAR=REG|0xf00n, BRIDGE=REG|0x002n;
function mk({neighbors=[],bridge=null,replicas=2}={}){
  const sends=[]; const clock={t:1_000_000};
  const dht={ getSelfId:()=>SELF, onRoutedMessage:()=>{}, routeMessage:(target,type,payload)=>sends.push({target,type,payload}),
    neighbors:()=>neighbors, bridgeId:()=>bridge };
  const am=new AxonaManager({dht, now:()=>clock.t, rootReplicas:replicas}); am.nodeId=SELF;
  return {am,sends,clock};
}
const repl=(sends)=>sends.filter(s=>s.type==='pubsub:replicate');

// 1. singleton root → replicate to 2 nearest (NEAR2,NEAR1), not FAR/BRIDGE
{
  const {am,sends}=mk({neighbors:[FAR,NEAR1,NEAR2,BRIDGE],bridge:BRIDGE});
  const role=am._becomeRoot(TOPIC);
  role.cache.push({msgId:'m1',publishTs:100,json:'{}',seq:1,bytes:80}); role.cacheIds.add('m1');
  sends.length=0; am._replicateRoots();
  const r=repl(sends); const tgts=new Set(r.map(s=>s.target));
  ok('replicates to exactly the 2 nearest neighbours', r.length===2 && tgts.has(NEAR1) && tgts.has(NEAR2));
  ok('does NOT replicate to the farther node', !tgts.has(FAR));
  ok('does NOT replicate to the bridge', !tgts.has(BRIDGE));
  ok('replica push carries the full cache', r[0]?.payload?.msgs?.length===1 && r[0].payload.msgs[0].msgId==='m1');
}
// 2. root WITH children STILL replicates to its cohort — co-hosting roots must converge
//    regardless of any down-tree (the down-tree and the K-closest cohort are disjoint sets).
{
  const {am,sends}=mk({neighbors:[NEAR1,NEAR2]});
  const role=am._becomeRoot(TOPIC);
  role.cache.push({msgId:'m1',publishTs:100,json:'{}',seq:1,bytes:80}); role.cacheIds.add('m1');
  role.children.add('cc'); sends.length=0; am._replicateRoots();
  ok('root with a sub-axon tree STILL replicates to the cohort', repl(sends).length===2);
}
// 3. receiving REPLICATE → passive backup
{
  const {am}=mk({neighbors:[NEAR1,NEAR2]});
  await am._onReplicate({topicId:idHex(TOPIC), from:idHex(NEAR1), msgs:[], dels:[]}, {targetId:SELF});
  const role=am.axonRoles.get(TOPIC);
  ok('REPLICATE makes a passive BACKUP (backupOf set, not root)', !!role && role.backupOf===idHex(NEAR1).toLowerCase() && role.isRoot===false);
}
// 4. stale backup with no closer reachable neighbour → promotes to root
{
  const {am,clock}=mk({neighbors:[]});   // self is the only/closest reachable node
  const role=makeRoleBackup(am,TOPIC,idHex(NEAR1));
  clock.t += 100_000;                     // past REPLICA_STALE_MS (65s)
  await am.refreshTick();
  ok('stale backup (root gone) promotes to root when closest', am.axonRoles.get(TOPIC)?.isRoot===true);
}
// 5. KILL at a root with established replicas re-pushes the tombstone SYNCHRONOUSLY
//    (the kill-leak race: a backup that already holds the body must get the tombstone
//    before it can promote, not on the next tick).
{
  const {am,sends}=mk({neighbors:[NEAR1,NEAR2],bridge:BRIDGE});
  const role=am._becomeRoot(TOPIC);
  role.cache.push({msgId:'m1',publishTs:100,json:'{}',seq:1,bytes:80}); role.cacheIds.add('m1');
  am._replicateRoots();                                   // establishes NEAR1/NEAR2 as replicas
  ok('precondition: replicas established', role.replicas.size===2);
  sends.length=0;
  am._applyKill(role, TOPIC, {msgId:'m1', killTs:200, signer:'aa', seq:2});
  const r=repl(sends);
  const carriesTomb = r.length>0 && r.every(s => (s.payload?.dels||[]).some(d=>d.msgId==='m1'));
  ok('kill at a root synchronously re-pushes to replicas', r.length===2);
  ok('the synchronous push carries the new tombstone (no leak window)', carriesTomb);
  ok('the killed message is gone from the replicated cache', r[0]?.payload?.msgs?.every(m=>m.msgId!=='m1'));
}
function makeRoleBackup(am,topicBig,fromHex){
  // mimic _onReplicate having established a backup (without needing signed envelopes)
  const role=am.axonRoles.get(topicBig);
  if(role) return role;
  const r={topicId:topicBig,isRoot:false,subscribers:new Map(),children:new Set(),cache:[{msgId:'m1',publishTs:100,json:'{}',seq:1,bytes:80}],cacheIds:new Set(['m1']),cacheBytes:80,lastTs:100,seq:1,tombstones:new Map(),replicas:new Map(),backupOf:fromHex.toLowerCase(),lastReplicaAt:am._now()};
  am.axonRoles.set(topicBig,r); return r;
}
console.log(`\n${fail?'✗':'✓'} smoke_root_replication: ${n} passed, ${fail} failed`);
process.exit(fail?1:0);
