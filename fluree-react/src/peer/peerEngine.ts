/**
 * The slice of `@fluree/db-wasm` the peer transport uses, declared
 * STRUCTURALLY.
 *
 * `@fluree/react` deliberately does not depend on `@fluree/db-wasm`: remote
 * mode must install and bundle without pulling in a wasm engine, and a hard
 * import would make the wasm package a dependency of every consumer. So the
 * application passes `connect` in (see `PeerTransportOptions.connect`) and
 * these interfaces describe what it must satisfy.
 *
 * That leaves one obligation: these types are a second description of a
 * contract owned elsewhere, and a copy that drifts is worse than no copy at
 * all. `test/protocolCompat.test.ts` closes that by type-checking the REAL
 * `@fluree/db-wasm` exports against these interfaces — if the engine's cycle
 * shape, subscribe signature, or lifecycle states change, that test fails to
 * compile. It is the single source of truth; this file is the projection,
 * and CI proves the projection is still faithful.
 */

/** One advance-cycle in batch form (`Peer.onCycle`). */
export interface PeerCycle {
  ledger: string;
  /** The cycle's frozen watermark; `undefined` when the engine had no
   * consistent view (the protocol's `-1`). */
  t: number | undefined;
  /** Subscriptions whose results moved, with decoded payloads. */
  changed: { subId: number; data: unknown }[];
  /** Subscriptions that re-ran at `t` and produced identical results. */
  unchanged: number[];
  errored: { subId: number; error: PeerError }[];
}

/** What the engine throws and reports. `FlureeError` satisfies this. */
export interface PeerError {
  message: string;
  code?: string;
  status?: number;
}

/**
 * Engine lifecycle. A crash recycle discards every subscription, so the
 * transport re-registers them on `"ready"`; `"terminal"` means the engine is
 * not coming back.
 */
export type PeerEngineState = "recycling" | "ready" | "terminal";

export interface PeerSubscription {
  readonly subId: number;
  unsubscribe(): Promise<void>;
}

export interface PeerLedger {
  query(query: string | object): Promise<unknown>;
}

export interface PeerEngine {
  ledger(id: string): Promise<PeerLedger>;
  /** Registers a live query. The engine auto-primes it: the first result
   * arrives as a cycle. The callback is unused here — delivery goes through
   * `onCycle`, which is the only surface carrying the whole batch. */
  subscribe(
    ledger: string,
    query: string | object,
    onUpdate: () => void,
  ): Promise<PeerSubscription>;
  onCycle(listener: (cycle: PeerCycle) => void): () => void;
  onEngineState(listener: (state: PeerEngineState) => void): () => void;
  close(): void;
}

/** Options this package passes to `connect`; a subset of `ConnectOptions`. */
export interface PeerConnectOptions {
  getToken?: (reason: "connect" | "reconnect") => string | Promise<string>;
  subscribe?: string[];
  workerUrl?: string | URL;
  wasmUrl?: string | URL;
  maxMemoryBytes?: number | null;
}

/** `connect` as exported by `@fluree/db-wasm`. */
export type PeerConnect = (
  url: string,
  options?: PeerConnectOptions,
) => Promise<PeerEngine>;
