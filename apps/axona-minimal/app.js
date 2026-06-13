// Axona Minimal — the smallest useful Axona app: publish to a topic, subscribe
// to a topic, show what arrives. ~70 lines, no framework. This is the artifact
// the programmer-intro talk builds.

import { AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, deriveTopicId, KERNEL_VERSION } from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';
import { regionName }   from '/src/utils/region-names.js';
import { resolveAnchor } from '../lib/region.js';

const $ = (id) => document.getElementById(id);
const status = (t) => { $('status').textContent = t; };

// The TOPIC is rooted at a fixed region (us-east), so every participant derives
// the SAME topic-id no matter where they are. The user's OWN identity, by
// contrast, is rooted at their REAL location (see whereAmI below) — so a message
// shows where its sender actually sits, while the topic stays one shared keyspace.
const BRIDGE = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net' : 'wss://bridge.axona.net');
const ANCHOR = resolveAnchor({ search: '', fallback: 'useast' });   // topic anchor — pinned to us-east

let peer, identity, currentTopic = null, currentSub = null;
const seen = new Set();                                  // msgIds we've already shown (dedup own echo)

// "region:userID" read straight off a 264-bit publish ID (the node-id): the top
// byte is the sender's S2 region cell, the rest is SHA-256(pubkey). Every signed
// publish carries it as env.publisherNodeId — the kernel puts it there, so we
// never embed it ourselves. (Anonymous, sign:false posts have none → 'anon'.)
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
  identity        = await deriveIdentity({ lat: here.lat, lng: here.lng });
  const transport = webTransport({ bridgeUrl: BRIDGE, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: here.lat, lng: here.lng });
  node.transport  = transport;
  peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, identity, transport });

  await transport.start(identity.id);
  await peer.start();
  const until = Date.now() + 30000;
  while (Date.now() < until && (node.synaptome?.size ?? 0) < 3) {
    status(`forming mesh… (${node.synaptome?.size ?? 0})`);
    await new Promise((r) => setTimeout(r, 600));
  }
  status('connected');
  $('ver').textContent = `kernel v${KERNEL_VERSION} · you ${idLabel(identity.id)}`;
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
    render(textOf(env), idLabel(env.publisherNodeId), false, topic);
  }, { publisher: ANCHOR.publisher, since: 'all' });
}

// The message is the plain text we published. (Tolerate the odd object-shaped
// legacy post so replayed history still renders.)
const textOf = (env) =>
  (env.message && typeof env.message === 'object') ? (env.message.text ?? JSON.stringify(env.message))
                                                   : env.message;

// Publish to the current topic. The kernel attaches our publish ID to the
// signed envelope, so we send just the text. Own publishes may not echo back,
// so we render optimistically and let the seen-set dedup if they do.
async function send() {
  const topic = $('topic').value.trim(), text = $('message').value;
  if (!topic || !text) return;
  await ensureSubscribed(topic);
  const msgId = await peer.pub(topic, text, { publisher: ANCHOR.publisher });
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
