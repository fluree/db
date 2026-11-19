/**
 * The seam assertion.
 *
 * `@fluree/react` declares the engine's surface structurally
 * (`src/peer/peerEngine.ts`) so it can carry no dependency on
 * `@fluree/db-wasm`. That is a second description of a contract owned
 * elsewhere, and a description that silently drifts from the thing it
 * describes is worse than none: peer mode would keep compiling and start
 * failing at runtime, in a browser, after the engine had already shipped.
 *
 * So this file imports the REAL types from `fluree-db-wasm/js/src` — the
 * single source of truth, imported, never copied — and asserts assignability
 * in both directions where it matters. The assertions are types, so `tsc`
 * IS the check: a drift in the engine's cycle shape, subscribe signature, or
 * lifecycle states fails the `react-sdk` CI job's type-check step, not a
 * later browser run.
 *
 * It lives in `test/` deliberately: `src/` must stay free of cross-package
 * imports so the published `dist/` never references the wasm package.
 */

import { describe, expect, it } from "vitest";
import type {
  EngineState,
  LiveCycle,
  LiveSubscription,
  Peer,
  connect as wasmConnect,
} from "../../fluree-db-wasm/js/src/index.js";
import type { CycleOutcomeEvent, SubscribeRequest } from "../../fluree-db-wasm/js/src/protocol.js";
import type {
  PeerConnect,
  PeerCycle,
  PeerEngine,
  PeerEngineState,
  PeerSubscription,
} from "../src/peer/peerEngine.js";
import type { CycleUpdate } from "../src/core/transport.js";
import type { QueryKind } from "../src/core/types.js";

/** Compile-time assertion: `T` must be assignable to `Expected`. */
type Assignable<Expected, T extends Expected> = [T];

// The engine's real objects must satisfy every interface this package
// projects. If `Peer` loses `onCycle`, or `onCycle`'s payload shape changes,
// or `connect`'s options no longer accept what we pass, these stop
// compiling.
type _PeerSatisfiesEngine = Assignable<PeerEngine, Peer>;
type _ConnectSatisfiesFactory = Assignable<PeerConnect, typeof wasmConnect>;
type _CycleSatisfiesProjection = Assignable<PeerCycle, LiveCycle>;
type _SubscriptionSatisfiesProjection = Assignable<PeerSubscription, LiveSubscription>;
type _EngineStateMatches = Assignable<PeerEngineState, EngineState>;
// ...and no state may exist in our projection that the engine never emits.
type _EngineStateExhaustive = Assignable<EngineState, PeerEngineState>;

// The query languages must agree: the engine infers the language from the
// argument type, so a `kind` this package can produce and the engine cannot
// serve would be a runtime-only failure.
type _QueryKindMatches = Assignable<QueryKind, SubscribeRequest["kind"]>;
type _QueryKindExhaustive = Assignable<SubscribeRequest["kind"], QueryKind>;

// The batch shape this package consumes must stay a faithful narrowing of
// what the worker actually sends.
type _WireCycleLedger = Assignable<CycleUpdate["ledger"], CycleOutcomeEvent["ledger"]>;
type _WireCycleUnchanged = Assignable<number[], CycleOutcomeEvent["unchanged"]>;

// Reference the aliases so `noUnusedLocals` (if ever enabled) and readers
// both see they are load-bearing.
export type ProtocolAssertions = [
  _PeerSatisfiesEngine,
  _ConnectSatisfiesFactory,
  _CycleSatisfiesProjection,
  _SubscriptionSatisfiesProjection,
  _EngineStateMatches,
  _EngineStateExhaustive,
  _QueryKindMatches,
  _QueryKindExhaustive,
  _WireCycleLedger,
  _WireCycleUnchanged,
];

describe("protocol compatibility with @fluree/db-wasm", () => {
  it("is asserted at compile time, not here", () => {
    // The assertions above are the test. This case exists so the suite
    // reports a ran-marker rather than an empty file that could be deleted
    // without anyone noticing the check went away.
    expect(true).toBe(true);
  });

  it("keeps the peer transport's watermark contract wider than the engine's", () => {
    // The engine always knows `t` (its cycle runs against a frozen
    // snapshot) except for `-1`; remote mode often does not. `CycleUpdate.t`
    // is therefore optional, and the engine's always-present number has to
    // remain assignable to it.
    const fromEngine: CycleOutcomeEvent["t"] = 7;
    const asCycle: CycleUpdate["t"] = fromEngine;
    expect(asCycle).toBe(7);
  });
});
