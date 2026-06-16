// =====================================================================
// smoke_std_publisher.mjs — @axona/protocol/std/publisher.
//
//   1. createPublisher mints an S2-free base + unique, monotonic publishIds
//   2. persistence: a restart (new instance, same store/key) CONTINUES the
//      sequence — no reset-to-zero, so publishIds never collide across restarts
//   3. multiple publishers (distinct keys) are independent
//   4. ephemeral (no store) still yields unique ids and distinct bases
//
//   node test/smoke_std_publisher.mjs
// =====================================================================
import { createPublisher, persistentPublisher } from '../std/publisher.js';

let passed = 0, failed = 0;
const check = (l, c) => { if (c) { console.log(`  ✓ ${l}`); passed++; } else { console.log(`  ✗ ${l}`); failed++; } };

// in-memory sync store standing in for localStorage
function memStore() { const m = new Map(); return { get: (k) => (m.has(k) ? m.get(k) : null), set: (k, v) => m.set(k, v), _m: m }; }

function main() {
  console.log('@axona/protocol/std/publisher');

  // ── 1. unique, monotonic, S2-free ──
  {
    const p = createPublisher();
    const a = p.next(), b = p.next(), c = p.next();
    check('1a. publishIds unique + monotonic', a !== b && b !== c && a.endsWith(':1') && c.endsWith(':3'));
    check('1b. base id is S2-free (no 66-hex nodeId / region prefix)', /^pub_[0-9a-f]+$/.test(p.id));
  }

  // ── 2. persistence continues across a "restart" ──
  {
    const store = memStore();
    const p1 = persistentPublisher('sightings', { store });
    const id1 = p1.next();           // pub_x:1
    p1.next(); p1.next();            // :2, :3  (seq now 3)
    // simulate restart: brand-new instance, same store+key
    const p2 = persistentPublisher('sightings', { store });
    check('2a. restart restores the SAME base id', p2.id === p1.id);
    const idNext = p2.next();        // must be :4, NOT :1
    check('2b. restart CONTINUES the sequence (no reset-to-zero collision)', idNext === `${p1.id}:4`);
    check('2c. no overlap between pre- and post-restart ids', idNext !== id1);
  }

  // ── 3. multiple publishers are independent ──
  {
    const store = memStore();
    const a = persistentPublisher('chan-A', { store });
    const b = persistentPublisher('chan-B', { store });
    check('3. distinct keys → distinct bases', a.id !== b.id && a.next().startsWith(a.id) && b.next().startsWith(b.id));
  }

  // ── 4. ephemeral still works ──
  {
    const a = createPublisher(), b = createPublisher();
    check('4. two ephemeral publishers get distinct bases + unique ids', a.id !== b.id && a.next() !== b.next());
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main();
