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

// E3c (SEAL — close the E0 instrumentation): the allowlist is now EMPTY. After the
// E3 seal, NO source file names a raw dispatch primitive: the canonical door
// (registerFrame) and every mechanism shim (CompositeTransport fan-out, the
// default-DHT routed passthrough, the registerDirectFrame direct_* registrar) reach
// the primitive ONLY through the deposited capability closure (readDispatchCapability),
// never by a literal `recv.onX(...)` call. There is nothing left to allowlist — a
// re-introduced raw named-primitive call anywhere now matches no entry and fails
// closed. The surviving shims are instead frozen by MODULE IDENTITY (which modules may
// import readDispatchCapability) in fence_readcap_importer_freeze.mjs. The prior
// entries (2 composite fan-out fallbacks, 3 registerFrame door legs, 1 registerDirectFrame
// fallback) were all removed across E3b.4 + E3c as each transitional fallback was dropped.
export const RAW_DISPATCH_ALLOWLIST = Object.freeze([]);

export default RAW_DISPATCH_ALLOWLIST;
