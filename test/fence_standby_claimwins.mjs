// fence_standby_claimwins.mjs — 4.130.0 (Orion 1f81ed10, verified by axona.bot e3636c1e):
// two live roots on one topic no longer churn their backups, and an aborted merge
// round is retried without cutover.
//
// Seen 2026-11-04: Orion's 4.128.0 seat and armed win svc-04 both rooted
// 89c699443362 / 89c37667cd05. Every backup re-principaled to whichever root's
// REPLICATE arrived last (7,356 flips on one relay, strictly alternating), because
// the cross-root grant rule checked only the issue-stamp skew. And the seat never
// re-offered: _mergeAbort kept role.merge.candidate unless cutover was armed, and
// _mergeCandidate (the only caller of _mergeOffer) refuses the same winner again.
//
//   A1  inside a live lease, a FARTHER root's grant is refused (outranked)
//   A2  an alternating two-root storm leaves the principal fixed: no re-principal
//   A3  a HIGHER epoch outranks the current principal and re-principals
//   A4  a CLOSER root at the same epoch outranks and re-principals
//   A5  once the lease lapses, a surviving root (even a farther one) appoints
//   B   cutover OFF: after an abort the same winner is offered again
//
// Run: node test/fence_standby_claimwins.mjs
import { manager, seed, replicate, gen, idHex, lc, TP, NODE, REG, T0 } from './lib/lifetimeFixture.mjs';
import { STANDBY_LEASE_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { const b = !!ok; console.log(`  ${b ? '✓' : '✗'} ${label}${b ? '' : ' ' + String(extra)}`); b ? passed++ : failed++; };
const NEAR = NODE(3), FAR = NODE(4);                         // NODE(3) is XOR-closer to every TP than NODE(4)
const principal = (role) => role.backupOf;
const flips = (ctx) => ctx.log('role-nature').filter((r) => r.ctx.why === 're-principaled').length;

console.log('fence_standby_claimwins');

console.log('\n[A1] inside a live lease a farther root does not re-principal the backup');
{
  const ctx = manager();
  ctx.clock.t = T0;
  await replicate(ctx, TP(1), NEAR, { gen: gen(NEAR, 1), issuedAt: ctx.clock.t });
  const role = ctx.role(TP(1));
  ctx.clock.t += 3_000;
  await replicate(ctx, TP(1), FAR, { gen: gen(FAR, 1), issuedAt: ctx.clock.t });
  check('principal stays the closer root', principal(role) === lc(idHex(NEAR)), principal(role));
  check('the farther grant is refused as outranked', ctx.log('grant-refused').some((r) => r.ctx.why === 'outranked'));
}

console.log('\n[A2] an alternating two-root storm (the 2026-11-04 shape) causes no flips');
{
  const ctx = manager();
  ctx.clock.t = T0;
  await replicate(ctx, TP(2), NEAR, { gen: gen(NEAR, 1), issuedAt: ctx.clock.t });
  const role = ctx.role(TP(2));
  for (let i = 0; i < 10; i++) {
    ctx.clock.t += 1_500; await replicate(ctx, TP(2), FAR, { gen: gen(FAR, 1), issuedAt: ctx.clock.t });
    ctx.clock.t += 1_500; await replicate(ctx, TP(2), NEAR, { gen: gen(NEAR, 1), issuedAt: ctx.clock.t });
  }
  check('principal never moved off the closer root', principal(role) === lc(idHex(NEAR)));
  check('zero re-principaled lines over 20 alternating grants', flips(ctx) === 0, `flips ${flips(ctx)}`);
  check('the closer root still renews the lease', role.lastReplicaAt === ctx.clock.t);
}

console.log('\n[A3] a higher epoch outranks the current principal');
{
  const ctx = manager();
  ctx.clock.t = T0;
  await replicate(ctx, TP(3), NEAR, { gen: gen(NEAR, 1), issuedAt: ctx.clock.t });
  const role = ctx.role(TP(3));
  ctx.clock.t += 3_000;
  await replicate(ctx, TP(3), FAR, { gen: gen(FAR, 2), issuedAt: ctx.clock.t });
  check('the epoch-2 root re-principals the backup', principal(role) === lc(idHex(FAR)) && role.standbyGen.epoch === 2);
}

console.log('\n[A4] a closer root at the same epoch outranks the current principal');
{
  const ctx = manager();
  ctx.clock.t = T0;
  await replicate(ctx, TP(4), FAR, { gen: gen(FAR, 1), issuedAt: ctx.clock.t });
  const role = ctx.role(TP(4));
  ctx.clock.t += 3_000;
  await replicate(ctx, TP(4), NEAR, { gen: gen(NEAR, 1), issuedAt: ctx.clock.t });
  check('the closer root re-principals the backup', principal(role) === lc(idHex(NEAR)));
}

console.log('\n[A5] after the lease lapses, a surviving farther root may appoint');
{
  const ctx = manager();
  ctx.clock.t = T0;
  await replicate(ctx, TP(5), NEAR, { gen: gen(NEAR, 1), issuedAt: ctx.clock.t });
  const role = ctx.role(TP(5));
  ctx.clock.t += STANDBY_LEASE_MS + 1;
  await replicate(ctx, TP(5), FAR, { gen: gen(FAR, 1), issuedAt: ctx.clock.t });
  check('the lapsed appointment yields to the surviving root', principal(role) === lc(idHex(FAR)), principal(role));
}

console.log('\n[B] cutover OFF: an aborted round is offered again to the same winner');
{
  const ctx = manager();
  const am = ctx.am;
  check('cutover is off in this manager', am._cutover === false);
  const T = TP(6);
  seed(ctx, T, { isRoot: true, cache: 1 });
  const role = ctx.role(T);
  const W = lc(idHex(REG | 0x0800n));                       // closer than SELF to every TP
  let offers = 0;
  am._mergeOffer = () => { offers++; };
  am._mergeCandidate(T, W, 5, 'beacon');
  check('first nomination offers', offers === 1, `offers ${offers}`);
  am._mergeCandidate(T, W, 5, 'beacon');
  check('a repeat nomination while the round stands does not re-offer', offers === 1, `offers ${offers}`);
  role.merge.gate = { corr: 'c0ffee00c0ffee00', at: ctx.clock.t };
  role.merge.corr = 'c0ffee00c0ffee00';
  am._mergeAbort(role, 'timeout');
  check('the abort clears the stale candidate', role.merge.candidate == null);
  am._mergeCandidate(T, W, 5, 'beacon');
  check('the next nomination of the same winner offers again', offers === 2, `offers ${offers}`);
}

console.log(failed ? `\n${failed} FAILED of ${passed + failed}` : `\nall ${passed} passed`);
process.exit(failed ? 1 : 0);
