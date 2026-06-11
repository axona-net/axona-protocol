// Axona-share — share images over Axona pub/sub. Proof of concept.
// Channels are pub/sub topics; images are compressed to <1MB then sent as a set
// of chunk-messages (file-transport.js) and reassembled on every subscriber.
import { connectAxona } from './axona.js';
import { chunkBytes, createReassembler, compressImage } from '../lib/file-transport.js';

const APP_VERSION = '0.1.0';
const DEFAULT_CHANNEL = { id: 'axona-share/public-images', name: 'Public Images' };
const MAX_IMAGE_BYTES = 1_000_000;
const $ = (id) => document.getElementById(id);

// ── state ───────────────────────────────────────────────────────────
let axona = null;
let channels = loadChannels();
let activeId = channels[0].id;
const feeds = new Map();          // channelId → [{ id, url, mime, caption, ts }]
const seen  = new Map();          // channelId → Set(fileId)   (dedup replay + own echo)
const reasm = new Map();          // channelId → reassembler
let pendingFile = null;           // composer: chosen File/Blob awaiting Share

function loadChannels() {
  try {
    const saved = JSON.parse(localStorage.getItem('axonashare-channels') || '[]');
    const byId = new Map(saved.map((c) => [c.id, c]));
    byId.set(DEFAULT_CHANNEL.id, byId.get(DEFAULT_CHANNEL.id) || DEFAULT_CHANNEL);   // always present
    return [DEFAULT_CHANNEL, ...[...byId.values()].filter((c) => c.id !== DEFAULT_CHANNEL.id)];
  } catch { return [DEFAULT_CHANNEL]; }
}
function saveChannels() {
  localStorage.setItem('axonashare-channels', JSON.stringify(channels.filter((c) => c.id !== DEFAULT_CHANNEL.id)));
}
const feedOf = (id) => { if (!feeds.has(id)) feeds.set(id, []); return feeds.get(id); };
const seenOf = (id) => { if (!seen.has(id)) seen.set(id, new Set()); return seen.get(id); };

// ── incoming: a fully reassembled image for a channel ───────────────
function onImage(channelId, { id, mime, bytes, meta }) {
  const s = seenOf(channelId);
  if (s.has(id)) return;
  s.add(id);
  const url = URL.createObjectURL(new Blob([bytes], { type: mime || 'image/jpeg' }));
  feedOf(channelId).push({ id, url, mime, caption: (meta && meta.caption) || '', ts: (meta && meta.ts) || Date.now() });
  if (channelId === activeId) renderFeed();
}

async function subscribeChannel(ch) {
  if (reasm.has(ch.id)) return;
  const r = createReassembler((file) => onImage(ch.id, file));
  reasm.set(ch.id, r);
  try { await axona.sub(ch.id, (msg) => r.accept(msg)); }
  catch (e) { setStatus('subscribe failed: ' + (e.message || e)); }
}

// ── publish an image to the active channel ──────────────────────────
async function shareImage(file, caption) {
  if (!axona) { setStatus('not connected yet'); return; }
  setStatus('compressing…');
  let blob;
  try { blob = await compressImage(file, { maxBytes: MAX_IMAGE_BYTES }); }
  catch (e) { setStatus('image error: ' + (e.message || e)); return; }
  const bytes = new Uint8Array(await blob.arrayBuffer());
  const meta = { caption: caption || '', ts: Date.now() };
  const msgs = chunkBytes(bytes, { name: file.name || 'image.jpg', mime: 'image/jpeg', meta });
  const fileId = msgs[0].id;
  // optimistic local card (own publishes may not echo back; seen-set dedups if they do)
  onImage(activeId, { id: fileId, mime: 'image/jpeg', bytes, meta });
  const topic = activeId;
  setStatus(`sending ${(bytes.length / 1024).toFixed(0)} KB in ${msgs.length} piece(s)…`);
  try {
    for (let i = 0; i < msgs.length; i++) { await axona.pub(topic, msgs[i]); setStatus(`sent ${i + 1}/${msgs.length}…`); }
    setStatus('shared ✓');
  } catch (e) { setStatus('share failed: ' + (e.message || e)); }
}

// ── channels ────────────────────────────────────────────────────────
async function addChannel(ch, activate = true) {
  if (!channels.find((c) => c.id === ch.id)) { channels.push(ch); saveChannels(); renderChannels(); }
  if (axona) await subscribeChannel(ch);
  if (activate) setActive(ch.id);
}
function createChannel() {
  const name = prompt('Name your new channel:');
  if (!name) return;
  const id = 'axona-share/c/' + Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 6);
  addChannel({ id, name: name.trim() });
}
function joinChannel() {
  const id = prompt('Paste the channel ID to join:');
  if (!id || !id.trim()) return;
  const clean = id.trim();
  addChannel({ id: clean, name: clean.startsWith('axona-share/') ? clean.split('/').pop() : clean });
}
function setActive(id) {
  activeId = id;
  $('activeName').textContent = (channels.find((c) => c.id === id) || {}).name || id;
  renderChannels(); renderFeed();
  closeSidebarMobile();
}

// ── rendering ───────────────────────────────────────────────────────
function renderChannels() {
  $('channels').innerHTML = channels.map((c) => `
    <div class="chan ${c.id === activeId ? 'active' : ''}" data-id="${c.id}">
      <span class="chan-name" title="${esc(c.id)}">${esc(c.name)}</span>
      <button class="copy" data-copy="${esc(c.id)}" title="Copy channel ID to share">⧉</button>
    </div>`).join('');
}
function renderFeed() {
  const items = [...feedOf(activeId)].sort((a, b) => b.ts - a.ts);   // newest first
  $('feed').innerHTML = items.length ? items.map((it) => `
    <div class="card">
      <img src="${it.url}" alt="">
      ${it.caption ? `<div class="cap">${esc(it.caption)}</div>` : ''}
    </div>`).join('') : '<div class="empty">No images yet. Share one above — it goes to everyone in this channel.</div>';
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
function setStatus(m) { $('status').textContent = m; }

// composer preview
function setPending(file) {
  pendingFile = file;
  const url = URL.createObjectURL(file);
  $('preview').src = url; $('preview').style.display = 'block';
  $('shareBtn').disabled = false;
  $('composerHint').textContent = file.name || 'image ready';
}
function clearComposer() {
  pendingFile = null;
  $('preview').src = ''; $('preview').style.display = 'none';
  $('caption').value = ''; $('shareBtn').disabled = true;
  $('composerHint').textContent = 'Choose, snap, or drag an image here';
  $('fileInput').value = ''; $('camInput').value = '';
}

// ── mobile sidebar ──────────────────────────────────────────────────
const openSidebar  = () => { $('sidebar').classList.add('open'); $('overlay').classList.add('show'); };
const closeSidebar = () => { $('sidebar').classList.remove('open'); $('overlay').classList.remove('show'); };
const closeSidebarMobile = () => { if (window.matchMedia('(max-width:760px)').matches) closeSidebar(); };

// ── wiring ──────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', async () => {
  $('ver').textContent = 'v' + APP_VERSION;
  renderChannels(); setActive(activeId); clearComposer();

  $('addChannel').addEventListener('click', () => {
    const j = confirm('OK = create a new channel\nCancel = join an existing one by ID');
    if (j) createChannel(); else joinChannel();
  });
  $('channels').addEventListener('click', (e) => {
    const copy = e.target.closest('[data-copy]');
    if (copy) { navigator.clipboard?.writeText(copy.dataset.copy); setStatus('channel ID copied — send it to a friend'); return; }
    const chan = e.target.closest('.chan'); if (chan) setActive(chan.dataset.id);
  });

  $('pickBtn').addEventListener('click', () => $('fileInput').click());
  $('camBtn').addEventListener('click', () => $('camInput').click());
  $('fileInput').addEventListener('change', (e) => { if (e.target.files[0]) setPending(e.target.files[0]); });
  $('camInput').addEventListener('change', (e) => { if (e.target.files[0]) setPending(e.target.files[0]); });
  $('shareBtn').addEventListener('click', async () => {
    if (!pendingFile) return;
    const f = pendingFile, cap = $('caption').value;
    $('shareBtn').disabled = true;
    await shareImage(f, cap);
    clearComposer();
  });

  const main = $('main');
  ['dragover', 'dragenter'].forEach((ev) => main.addEventListener(ev, (e) => { e.preventDefault(); main.classList.add('drag'); }));
  ['dragleave', 'drop'].forEach((ev) => main.addEventListener(ev, (e) => { e.preventDefault(); main.classList.remove('drag'); }));
  main.addEventListener('drop', (e) => {
    const f = [...(e.dataTransfer?.files || [])].find((x) => x.type.startsWith('image/'));
    if (f) setPending(f);
  });

  $('menuBtn').addEventListener('click', () => ($('sidebar').classList.contains('open') ? closeSidebar() : openSidebar()));
  $('overlay').addEventListener('click', closeSidebar);

  // connect + subscribe everything
  try {
    axona = await connectAxona(setStatus);
    for (const c of channels) await subscribeChannel(c);
    setStatus('connected — ' + channels.length + ' channel(s)');
  } catch (e) { setStatus('Axona connect failed: ' + (e.message || e)); }
});
