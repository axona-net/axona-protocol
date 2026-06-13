// Axona Minimal — the smallest useful Axona app: publish to a topic, subscribe
// to a topic, show what arrives. ~60 lines, no framework. This is the artifact
// the programmer-intro talk builds.

import { AxonaPeer, AxonaDomain, NeuronNode, deriveIdentity, KERNEL_VERSION } from '/src/index.js';
import { webTransport } from '/src/transport/web/index.js';
import { resolveAnchor } from '../lib/region.js';

const $ = (id) => document.getElementById(id);
const status = (t) => { $('status').textContent = t; };

// 1. Pick the bridge + region. The region fixes both this peer's node-id and the
//    topic anchor, so everyone on ?region=<r> shares one keyspace.
const BRIDGE = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net' : 'wss://bridge.axona.net');
const ANCHOR = resolveAnchor();                          // { name, center:{lat,lng}, publisher }

let peer, currentTopic = null, currentSub = null;
const seen = new Set();                                  // msgIds we've already shown (dedup own echo)

// 2. Connect: derive a keypair identity, open the web transport to the bridge,
//    build the peer, wait briefly for the mesh to form.
async function connect() {
  status(`connecting · ${ANCHOR.name}…`);
  const identity  = await deriveIdentity({ lat: ANCHOR.center.lat, lng: ANCHOR.center.lng });
  const transport = webTransport({ bridgeUrl: BRIDGE, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: ANCHOR.center.lat, lng: ANCHOR.center.lng });
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
  $('ver').textContent = `kernel v${KERNEL_VERSION} · region ${ANCHOR.name} · ${identity.id.slice(0, 10)}…`;
  $('send').disabled = false;
}

// 3. Subscribe to a topic. Re-subscribing to a new topic drops the old one.
async function ensureSubscribed(topic) {
  if (topic === currentTopic) return;
  if (currentSub) { try { await currentSub.stop(); } catch {} }
  currentTopic = topic;
  currentSub = await peer.sub(topic, (env) => {
    if (!env || env.deleted || seen.has(env.msgId)) return;   // skip our own already-shown echo
    seen.add(env.msgId);
    render(env.message, env.signerPubkey, false);
  }, { publisher: ANCHOR.publisher, since: 'all' });
}

// 4. Publish to the current topic. Own publishes may not echo back, so we render
//    optimistically and let the seen-set dedup if they do.
async function send() {
  const topic = $('topic').value.trim(), text = $('message').value;
  if (!topic || !text) return;
  await ensureSubscribed(topic);
  const msgId = await peer.pub(topic, text, { publisher: ANCHOR.publisher });
  seen.add(msgId);
  render(text, null, true);
  $('message').value = '';
}

function render(text, signer, self) {
  const out = $('out');
  if (out.querySelector('.empty')) out.innerHTML = '';
  const el = document.createElement('div');
  el.className = 'msg' + (self ? ' self' : '');
  const who = self ? 'you' : (signer ? signer.slice(0, 8) + '…' : 'peer');
  el.innerHTML = `<div class="text"></div><div class="meta"></div>`;
  el.querySelector('.text').textContent = text;
  el.querySelector('.meta').textContent = `${who} · ${new Date().toLocaleTimeString()}`;
  out.appendChild(el);
  out.scrollTop = out.scrollHeight;
}

$('send').addEventListener('click', () => send().catch((e) => status('send failed: ' + (e.message || e))));
$('message').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('send').click(); });
$('topic').addEventListener('change', () => ensureSubscribed($('topic').value.trim()).catch(() => {}));

connect()
  .then(() => ensureSubscribed($('topic').value.trim()))
  .catch((e) => status('connect failed: ' + (e.message || e)));
