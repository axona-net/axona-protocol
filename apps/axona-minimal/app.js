// Axona Minimal — the smallest useful Axona app: publish to a topic, subscribe
// to a topic, show what arrives. ~70 lines, no framework. This is the artifact
// the programmer-intro talk builds.

import { AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, dumpIdentity, loadIdentity, deriveTopicId, KERNEL_VERSION } from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';
import { regionName }   from '/src/utils/region-names.js';
import { resolveAnchor } from '../lib/region.js';

const $ = (id) => document.getElementById(id);
const status = (t) => { $('status').textContent = t; };

// The TOPIC is rooted at a fixed region (us-east), so every participant derives
// the SAME topic-id no matter where they are. The user's OWN identity, by
// contrast, is rooted at their REAL location (see whereAmI below) — so a message
// shows where its sender actually sits, while the topic stays one shared keyspace.
const APP_VERSION = '0.3.0';
// Bridge selection (same as axona-share): ?bridge=<wss url> → ?net=testnet|prod
// shortcut → default by hostname. Lets one build run against either network.
const KNOWN_BRIDGES = { prod: 'wss://bridge.axona.net', testnet: 'wss://testnet.axona.net' };
const _params = new URLSearchParams(location.search);
function resolveBridge() {
  const explicit = (_params.get('bridge') || '').trim();
  if (/^wss?:\/\//.test(explicit)) return explicit;
  const net = (_params.get('net') || '').trim().toLowerCase();
  if (KNOWN_BRIDGES[net]) return KNOWN_BRIDGES[net];
  return location.hostname.includes('testnet') ? KNOWN_BRIDGES.testnet : KNOWN_BRIDGES.prod;
}
const BRIDGE = resolveBridge();
const NETWORK = BRIDGE === KNOWN_BRIDGES.testnet ? 'testnet' : BRIDGE === KNOWN_BRIDGES.prod ? 'prod' : 'custom';
const ANCHOR = resolveAnchor({ search: '', fallback: 'useast' });   // topic anchor — pinned to us-east

let peer, identity, publishIdentity, currentTopic = null, currentSub = null;

// Persistent PUBLISH identity (key separation): the transport `identity` authenticates
// the connection and is ephemeral; this signs your posts and is persisted so authorship
// is stable across reloads. The kernel requires a publish identity to sign a publish.
async function loadOrCreatePublishIdentity(lat, lng) {
  const key = 'axona-minimal:publish';
  try { const s = localStorage.getItem(key); if (s) return await loadIdentity(JSON.parse(s)); } catch {}
  const id = await deriveIdentity({ lat, lng });
  try { localStorage.setItem(key, JSON.stringify(await dumpIdentity(id))); } catch {}
  return id;
}
const seen = new Set();                                  // msgIds we've already shown (dedup own echo)

// "region:userID" read off a 264-bit publish ID (the node-id): top byte = the
// sender's S2 region cell, the rest = SHA-256(pubkey).
//
// NOTE: the protocol does NOT reveal a publisher's location — a signed envelope
// carries only signerPubkey (who signed), never the node-id's S2 region (where
// they are), and the region can't be derived from the key. That's a deliberate
// publisher-privacy property. THIS APP opts in: it chooses to share the sender's
// region by putting its own node-id in the message payload below. That's an
// application-layer example of voluntary location disclosure, not a protocol
// feature. (Anonymous / sign:false posts carry nothing → 'anon'.)
const idLabel = (pubId) =>
  (typeof pubId === 'string' && pubId.length >= 10)
    ? `${regionName(parseInt(pubId.slice(0, 2), 16))}:${pubId.slice(2, 10)}`
    : 'anon';

// Real geolocation → the user's actual S2 cell. Denied / unavailable → us-east.
function whereAmI() {
  const fallback = { lat: ANCHOR.center.lat, lng: ANCHOR.center.lng };
  if (!navigator.geolocation) return Promise.resolve(fallback);
  return new Promise((resolve) => {
    navigator.geolocation.getCurrentPosition(
      (p) => resolve({ lat: p.coords.latitude, lng: p.coords.longitude }),
      ()  => resolve(fallback),                          // permission denied → default region
      { timeout: 8000, maximumAge: 600000 },
    );
  });
}

// Connect: locate the user (real S2 cell → their node-id), open the web
// transport, build the peer, wait briefly for the mesh to form.
async function connect() {
  status('locating…');
  const here = await whereAmI();
  status('connecting…');
  identity        = await deriveIdentity({ lat: here.lat, lng: here.lng });          // ephemeral transport
  publishIdentity = await loadOrCreatePublishIdentity(here.lat, here.lng);            // persistent author
  const transport = webTransport({ bridgeUrl: BRIDGE, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: here.lat, lng: here.lng });
  node.transport  = transport;
  peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, identity, transport, publishIdentity });

  await transport.start(identity.id);
  await peer.start();
  const until = Date.now() + 30000;
  while (Date.now() < until && (node.synaptome?.size ?? 0) < 3) {
    status(`forming mesh… (${node.synaptome?.size ?? 0})`);
    await new Promise((r) => setTimeout(r, 600));
  }
  status('connected');
  $('ver').textContent = `app v${APP_VERSION} · kernel v${KERNEL_VERSION} · ${NETWORK} · you ${idLabel(publishIdentity.id)}`;
  $('send').disabled = false;
}

// Subscribe to a topic. Re-subscribing to a new topic drops the old one.
async function ensureSubscribed(topic) {
  if (topic === currentTopic) return;
  if (currentSub) { try { await currentSub.stop(); } catch {} }
  currentTopic = topic;
  currentSub = await peer.sub(topic, (env) => {
    if (!env || env.deleted || seen.has(env.msgId)) return;   // skip our own already-shown echo
    seen.add(env.msgId);
    const m = env.message;
    const text = (m && typeof m === 'object') ? (m.text ?? JSON.stringify(m)) : m;
    const pub  = (m && typeof m === 'object') ? m.pub : null;   // the location WE chose to share
    render(text, idLabel(pub), false, topic);
  }, { publisher: ANCHOR.publisher, since: 'all' });
}

// Publish to the current topic. The protocol signs the post with our key but
// does NOT carry our location; THIS app voluntarily includes its publish ID
// (node-id, which encodes our S2 region) in the payload so subscribers can show
// where the message came from. Own publishes may not echo back, so we render
// optimistically and let the seen-set dedup if they do.
async function send() {
  const topic = $('topic').value.trim(), text = $('message').value;
  if (!topic || !text) return;
  await ensureSubscribed(topic);
  // Signed by publishIdentity (the peer's default publish key). We voluntarily share
  // the publish id so subscribers can show the author's region (idLabel) — app choice.
  const msgId = await peer.pub(topic, { text, pub: publishIdentity.id }, { publisher: ANCHOR.publisher });
  seen.add(msgId);
  render(text, idLabel(identity.id), true, topic);
  $('message').value = '';
}

function render(text, who, self, topic) {
  const out = $('out');
  if (out.querySelector('.empty')) out.innerHTML = '';
  const el = document.createElement('div');
  el.className = 'msg' + (self ? ' self' : '');
  el.innerHTML = `<div class="text"><span class="topic"></span><span class="body"></span></div><div class="meta"></div>`;
  el.querySelector('.topic').textContent = topic ? `${topic}: ` : '';
  el.querySelector('.body').textContent = text;
  el.querySelector('.meta').textContent = `${who} · ${new Date().toLocaleTimeString()}`;
  out.appendChild(el);
  out.scrollTop = out.scrollHeight;
}

// Show the topic-id beneath the field, prefixed with the topic's region (us-east,
// hardcoded here). deriveTopicId is pure, so this works before we even connect.
async function showTopicId(topic) {
  $('topicId').textContent = topic ? `${ANCHOR.name} : ${await deriveTopicId(ANCHOR.publisher, topic)}` : '';
}

$('send').addEventListener('click', () => send().catch((e) => status('send failed: ' + (e.message || e))));
$('message').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('send').click(); });
$('topic').addEventListener('input',  () => showTopicId($('topic').value.trim()).catch(() => {}));
$('topic').addEventListener('change', () => ensureSubscribed($('topic').value.trim()).catch(() => {}));

showTopicId($('topic').value.trim()).catch(() => {});
connect()
  .then(() => ensureSubscribed($('topic').value.trim()))
  .catch((e) => status('connect failed: ' + (e.message || e)));
