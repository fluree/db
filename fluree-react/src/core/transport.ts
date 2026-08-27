/**
 * The transport seam: everything below the query cache is a `LiveTransport`.
 *
 * A transport owns query execution and change detection for its mode and
 * reports through ONE batched message per advance-cycle (`CycleUpdate`) —
 * never one message per subscription. Two implementations are planned:
 *
 * - `RemoteTransport` (this package, shipped): queries over the server HTTP
 *   API; head events from the server's SSE `/v1/fluree/events` endpoint
 *   trigger a re-fetch cycle; results are diffed client-side before a
 *   subscription is reported as changed.
 * - the browser-peer transport (`@fluree/db-wasm`, upcoming): the wasm
 *   engine in a worker runs the same cycle natively (frozen snapshot,
 *   re-run, worker-side hash gate) and posts the same batch shape across
 *   the worker boundary.
 *
 * The cache reconstructs referential stability on receipt (structural
 * sharing against the previous result), so a transport is free to deliver
 * freshly-parsed objects — `postMessage` clones anyway.
 */

import type {
  ConnectionState,
  QueryError,
  ResolvedSpec,
} from "./types.js";

/** A live subscription registered with a transport. */
export interface SubscriptionSpec extends ResolvedSpec {
  /** Cache-assigned id, unique per client instance. */
  subId: number;
}

export interface CycleChange {
  subId: number;
  /** The full formatted result (parsed JSON, or a string for text formats). */
  payload: unknown;
}

export interface CycleErrored {
  subId: number;
  error: QueryError;
}

/**
 * One advance-cycle batch. All subscriptions named in one cycle moved to
 * watermark `t` together: the cache applies every entry before notifying a
 * single observer, so sibling components can never disagree about the data.
 *
 * `unchanged` lists subscriptions that were re-evaluated at `t` and produced
 * identical results (the transport's change gate suppressed the payload);
 * subscriptions absent from all three lists were not part of this cycle
 * (other ledgers, or t-anchored).
 */
export interface CycleUpdate {
  ledger: string;
  t: number;
  changed: CycleChange[];
  unchanged: number[];
  errored: CycleErrored[];
}

/** Callbacks a transport reports into; wired once by the client. */
export interface TransportSink {
  onCycle(cycle: CycleUpdate): void;
  onConnection(state: ConnectionState): void;
}

export interface LiveTransport {
  /** Wire the sink. Called exactly once, before any subscribe. */
  start(sink: TransportSink): void;
  /**
   * Register a live subscription. The transport must deliver an initial
   * result (or error) for the new `subId` via a cycle, then include the
   * subscription in every future cycle for its ledger. A spec with `at` set
   * is fetched once and never re-evaluated.
   */
  subscribe(spec: SubscriptionSpec): void;
  unsubscribe(subId: number): void;
  /** One-shot query outside the subscription system. */
  fetchOnce(spec: ResolvedSpec): Promise<unknown>;
  connectionState(): ConnectionState;
  close(): void;
}
