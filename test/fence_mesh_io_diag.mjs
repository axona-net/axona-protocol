// =====================================================================
// fence_mesh_io_diag.mjs — the mesh I/O diagnostic (4.157.1, MESH_IO_DIAG=1,
// default off) counts what crosses a data channel and changes nothing.
//
// WHY (David 2026-11-20; council 55d6f2d6, Orion ad6663f5). The testnet bridge
// held ~1.9 GB of native memory with a 40 MB JS heap. dc.send() copies each
// frame into libdatachannel's unbounded queue and mesh.send reads no
// bufferedAmount. Before any remedy, measure: bytes by frame kind each way,
// the largest frame, the highest bufferedAmount at a send, and send throws.
//
// What this pins:
//   1. Unarmed: send() makes exactly one dc.send with the same string; no
//      counters exist.
//   2. Armed: the same string is sent; counters record kind, bytes, max frame,
//      bufferedAmount high-water (read before and after the send).
//   3. Armed: a throwing dc.send is counted by reason and still throws.
//   4. Armed: _ioFlush logs one 'mesh-io' row and resets the window.
// Offline: a fake data channel; no WebRTC.
// =====================================================================
import { MeshManager } from '../src/transport/web/mesh.js';

let pass = 0, fail = 0;
const ok = (c, name, extra = '') => { if (c) { pass++; console.log(`  ✓ ${name}`); } else { fail++; console.log(`  ✗ ${name} ${extra}`); } };

function mk(armed) {
  const prev = process.env.MESH_IO_DIAG;
  if (armed) process.env.MESH_IO_DIAG = '1'; else delete process.env.MESH_IO_DIAG;
  const m = Object.create(MeshManager.prototype);
  // Only the fields send()/_ioFlush touch; the constructor needs a browser.
  m._peers = new Map(); m._logs = []; m._log = (ev, d) => m._logs.push([ev, d]);
  m._ioDiag = process.env.MESH_IO_DIAG === '1'; m._io = null; if (m._ioDiag) m._ioReset();
  if (prev === undefined) delete process.env.MESH_IO_DIAG; else process.env.MESH_IO_DIAG = prev;
  const sent = []; let buffered = 0, boom = null;
  const dc = { readyState: 'open', get bufferedAmount() { return buffered; },
               send(s) { if (boom) throw new Error(boom); sent.push(s); buffered += s.length; } };
  m._peers.set('p1', { dc });
  return { m, sent, setBuffered: (n) => { buffered = n; }, setBoom: (e) => { boom = e; } };
}

const frame = { k: 'ntf', type: 'routed', body: { type: 'pubsub:replicate', x: 'y'.repeat(1000) } };

console.log('\n[1] unarmed: one dc.send, same bytes, no counters');
{
  const { m, sent } = mk(false);
  m.send('p1', frame);
  ok(sent.length === 1 && sent[0] === JSON.stringify(frame), 'one send with the same string');
  ok(m._io === null, 'no counters exist');
}

console.log('\n[2] armed: same bytes sent; counters record the send');
{
  const { m, sent, setBuffered } = mk(true);
  setBuffered(5000);
  m.send('p1', frame);
  const n = JSON.stringify(frame).length;
  ok(sent.length === 1 && sent[0] === JSON.stringify(frame), 'one send with the same string');
  const row = m._io.out.get('ntf:routed/pubsub:replicate');
  ok(row && row.n === 1 && row.bytes === n && row.max === n, 'kind, count and bytes recorded', JSON.stringify([...m._io.out]));
  ok(m._io.maxOut === n && m._io.maxOutKind === 'ntf:routed/pubsub:replicate', 'largest outbound frame recorded');
  ok(m._io.bufMax === 5000 + n, 'bufferedAmount high-water is the value after the send', String(m._io.bufMax));
}

console.log('\n[3] armed: a throwing send is counted and still throws');
{
  const { m, setBoom } = mk(true);
  setBoom('libdatachannel error while sending data channel message: Message size exceeds limit');
  let threw = false; try { m.send('p1', frame); } catch { threw = true; }
  ok(threw, 'the error still propagates');
  ok(m._io.throws['Message size exceeds limit'] === 1, 'counted by reason', JSON.stringify(m._io.throws));
}

console.log('\n[4] armed: flush logs one row and resets');
{
  const { m } = mk(true);
  m.send('p1', frame); m.send('p1', { type: 'ping' });
  m._ioFlush();
  const rows = m._logs.filter(([ev]) => ev === 'mesh-io');
  ok(rows.length === 1, 'one mesh-io row');
  const d = rows[0][1];
  ok(d.out.n === 2 && d.topOut[0].k === 'ntf:routed/pubsub:replicate' && d.open === 1, 'row carries totals, top kinds and open channels', JSON.stringify(d).slice(0, 200));
  ok(typeof d.rssMB === 'number' && typeof d.externalMB === 'number', 'row carries process memory');
  ok(m._io.out.size === 0 && m._io.maxOut === 0, 'the window is reset');
}

console.log(`\n${fail ? '✗' : '✓'} fence_mesh_io_diag: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
