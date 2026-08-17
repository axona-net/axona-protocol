// =====================================================================
// e0Allowlist.mjs — REF-1.1: the ONE reviewed allowlist of non-migration
// raw-dispatch references, shared by the E0 manifest fence and the E1
// baseline/identifier gate.
//
// Design (REF-1.1-Enforcement-Cutover-Design-v5.md) exit criterion 2: "The shim
// set is enumerated in one place with a one-line justification each, and adding
// to it is a reviewed change." This module IS that place. Every entry is a
// (file, recv, method, arg) key with a class and a justification; a raw-dispatch
// reference that matches none of these is a migration-target (E0/E2) or, if it
// is aliased/computed/a loose method-name literal, a fail-closed finding.
//
// class:
//   canonical-door         — the one registerFrame door (exit criterion 1); the
//                            allowlisted holder that reaches the primitives by name.
//   mechanism-shim         — a low-level demux/fan-out that is not a frame and has
//                            no row (CompositeTransport fan-out; the DHT-adapter
//                            passthrough).
//   parameterized-registrar— onDirectMessage's direct_${type} family (own E1 fence).
//
// (E2.1: the B1 on() helper — formerly class `registration-helper` — is DELETED;
// its 19 wires migrated to the canonical door. Its allowlist row is removed, so a
// re-introduced raw dht.onRoutedMessage(type) in wireHandlers now fails closed.)
// =====================================================================

export const SEALED = Object.freeze(new Set(['onRequest', 'onNotification', 'onRoutedMessage']));

export const RAW_DISPATCH_ALLOWLIST = Object.freeze([
  { file: 'dht/AxonaPeer.js',       recv: 'peer',      method: 'onRoutedMessage', arg: 'type',     class: 'mechanism-shim',         why: 'default-DHT adapter passthrough (AxonaPeer.js:3089)' },
  { file: 'web/composite.js',       recv: 't',         method: 'onRequest',       arg: 'type',     class: 'mechanism-shim',         why: 'CompositeTransport request fan-out (E3b.2b: cap-first via readDispatchCapability; this literal-method call is the TRANSITIONAL fallback for an unsealed sub, removed in E3b.2c)' },
  { file: 'web/composite.js',       recv: 't',         method: 'onNotification',  arg: 'type',     class: 'mechanism-shim',         why: 'CompositeTransport notification fan-out (E3b.2b: cap-first via readDispatchCapability; this literal-method call is the TRANSITIONAL fallback for an unsealed sub, removed in E3b.2c)' },
  { file: 'registry/registerDirectFrame.js', recv: 'recv', method: 'onNotification', arg: 'wire', class: 'parameterized-registrar', why: 'registerDirectFrame direct_${type} family — the ONE named direct registrar (E3 decision 2); transitional named fallback for an unsealed transport, removed when E3b seals every transport' },
  // REF-1.1 E1: the canonical registerFrame door — the allowlisted holder of the
  // raw primitives (exit criterion 2, keyed by module identity). Reaches them by
  // NAME with the `wire` param; the door itself, not a migration-target.
  { file: 'registry/registerFrame.js', recv: 'recv', method: 'onRoutedMessage', arg: 'wire', class: 'canonical-door', why: 'registerFrame routed dispatch (named access)' },
  { file: 'registry/registerFrame.js', recv: 'recv', method: 'onNotification',  arg: 'wire', class: 'canonical-door', why: 'registerFrame notification dispatch (named access)' },
  { file: 'registry/registerFrame.js', recv: 'recv', method: 'onRequest',       arg: 'wire', class: 'canonical-door', why: 'registerFrame request dispatch (named access)' },
]);

export default RAW_DISPATCH_ALLOWLIST;
