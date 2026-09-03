/**
 * Message protocol between the main-thread proxy (`index.ts`) and the engine
 * worker (`worker.ts`).
 *
 * Two message families cross the boundary:
 *
 * - **Requests/responses**: every request carries a client-assigned `id`;
 *   the worker answers exactly once with the same `id`.
 * - **Events** (`EventMessage`, versioned): unsolicited worker→main pushes —
 *   head-change fan-out, and the worker's token requests (answered by the
 *   fire-and-forget `tokenResponse` request; the *event* is worker→main, the
 *   *answer* rides the normal main→worker direction with no reply expected).
 *   The two families are disjoint by shape: a response has `ok`, an event
 *   has `event`.
 *
 * Boundary values are plain JSON except query results, which travel as UTF-8
 * JSON bytes in a transferred `ArrayBuffer` by default (see
 * `ResultTransport`). Only buffered results exist on this protocol — the
 * engine's streaming query entry is deliberately not exposed (adversarial
 * review F6: streamed rows cannot participate in peer mode's fetch-and-re-run
 * loop, and the playground keeps the same shape). Keep this file
 * dependency-free: it is imported by both sides and by the smoke test.
 */

/** Which mode a worker was initialized in: in-memory read-write playground,
 * or the read-only remote peer (`fluree-db-browser`'s `BrowserPeer`). */
export type EngineMode = "playground" | "peer";

/** How query-result bytes cross the worker boundary.
 *
 * - `"transfer"`: the worker posts the UTF-8 JSON `Uint8Array` with its
 *   buffer in the transfer list — zero-copy, cost independent of size — and
 *   the main thread decodes + parses. Default.
 * - `"clone"`: the worker decodes to a JS string and posts it; the main
 *   thread parses. Structured-cloning a string is a memcpy, cheap for small
 *   results but linear in size. Kept selectable so the trade-off stays
 *   measurable (`demo/index.html` has a side-by-side timer).
 */
export type ResultTransport = "transfer" | "clone";

export type QueryKind = "sparql" | "jsonld";

/** What a query runs against: a ledger id (the worker freezes the current
 * head for the duration of that one call) or an explicit snapshot handle
 * (frozen until released — F6's "hand the query a frozen view" rule). */
export type QueryTarget = { ledger: string } | { snapshot: number };

export interface InitRequest {
  id: number;
  op: "init";
  mode: EngineMode;
  /** Override for the `.wasm` location (defaults to next to the worker's glue). */
  wasmUrl?: string;
  /** The one memory ceiling (F4): per-query budget in playground mode; in
   * peer mode it additionally derives the whole browser-io budget split.
   * Omit for the engine defaults. */
  maxMemoryBytes?: number;
  /** Peer mode: the server's versioned API base (`https://host/v1/fluree`).
   * NEVER a token — init is replayed verbatim on worker recycle, so
   * credentials go through the `tokenRequest` event instead. */
  url?: string;
  /** Peer mode: ledgers to subscribe head tracking to at init (empty array =
   * everything the token may see; omit = no tracking until requested). */
  subscribe?: string[];
  /** Set by the proxy when this init is a recycle replay — surfaces as the
   * token request's `reason: "reconnect"`. */
  reinit?: boolean;
}

/** Answer to a `tokenRequest` event. Fire-and-forget: no response comes
 * back — the worker's pending connect continues (or fails typed) with it. */
export interface TokenResponseRequest {
  id: number;
  op: "tokenResponse";
  /** Echo of the event's `requestId`. */
  requestId: number;
  /** The bearer token; omitted when the main thread could not produce one. */
  token?: string;
  /** Why no token, when `token` is absent. */
  error?: string;
}

export interface LedgerRequest {
  id: number;
  op: "createLedger" | "ledgerInfo";
  ledger: string;
}

export interface SnapshotRequest {
  id: number;
  op: "snapshot";
  ledger: string;
}

export interface ReleaseRequest {
  id: number;
  op: "release";
  snapshot: number;
}

export interface TransactRequest {
  id: number;
  op: "insert" | "upsert" | "update" | "sparqlUpdate";
  ledger: string;
  /** JSON text (insert/upsert/update) or a SPARQL Update string. */
  body: string;
}

export interface QueryRequest {
  id: number;
  op: "query";
  target: QueryTarget;
  kind: QueryKind;
  /** SPARQL text, or the JSON text of a JSON-LD query object. */
  text: string;
  transport: ResultTransport;
  /** Wall-clock budget in ms (F3). Undefined = no timeout. */
  timeoutMs?: number;
}

/** Register a live subscription (A4). The engine auto-primes it: the first
 * result arrives as a `cycleOutcome` event at the current head. */
export interface SubscribeRequest {
  id: number;
  op: "subscribe";
  ledger: string;
  kind: QueryKind;
  /** SPARQL text, or the JSON text of a JSON-LD query object. */
  text: string;
}

export interface UnsubscribeRequest {
  id: number;
  op: "unsubscribe";
  subId: number;
}

/** Test-only: deliberately trap the wasm instance to exercise the
 * crash/recycle path (`Playground._debugCrash`). */
/** Replace the bearer token for all subsequent engine I/O (peer mode only) —
 * mid-session refresh for long-lived tabs whose connect-time token would
 * otherwise expire. Complements the `getToken` pull, which runs only at
 * connect and crash-recycle. */
export interface SetTokenRequest {
  id: number;
  op: "setToken";
  token: string;
}

export interface DebugCrashRequest {
  id: number;
  op: "debugCrash";
}

export type Request =
  | InitRequest
  | TokenResponseRequest
  | LedgerRequest
  | SnapshotRequest
  | ReleaseRequest
  | TransactRequest
  | QueryRequest
  | SubscribeRequest
  | UnsubscribeRequest
  | SetTokenRequest
  | DebugCrashRequest;

// ---------------------------------------------------------------------------
// Events: unsolicited worker→main messages.
// ---------------------------------------------------------------------------

/** A ledger head advanced (peer mode, SSE head tracking). The engine has
 * already absorbed it: the next snapshot of that ledger sees the new head;
 * existing snapshots stay frozen. */
export interface HeadChangeEvent {
  kind: "headChange";
  ledger: string;
  t: number;
  indexT: number;
}

/** The worker needs a bearer token (peer connect, including the re-connect
 * inside a crash recycle — init carries no credentials by design). The main
 * thread answers with a `tokenResponse` request echoing `requestId`. */
export interface TokenRequestEvent {
  kind: "tokenRequest";
  requestId: number;
  reason: "connect" | "reconnect";
}

/** One live-query advance-cycle completed (A4; H §2's batch shape). The
 * changed subscriptions' payloads ride the enclosing `EventMessage`'s
 * `payloads`, aligned with `changed`'s order, as transferred buffers —
 * UTF-8 JSON bytes in each subscription's language-matched format.
 * Unchanged subscriptions ship no payload (the driver's hash gate); per-sub
 * errors repeat each cycle and never block other subscriptions. `t` is the
 * cycle's frozen watermark; `-1` = no consistent view (all subs errored).
 * A newly subscribed query's first outcome (the auto-prime) always reports
 * it changed. */
export interface CycleOutcomeEvent {
  kind: "cycleOutcome";
  ledger: string;
  t: number;
  changed: { subId: number }[];
  unchanged: number[];
  errored: { subId: number; error: string }[];
}

export type EventBody = HeadChangeEvent | TokenRequestEvent | CycleOutcomeEvent;

/** Envelope for events. `v` is the event-protocol version: a consumer must
 * ignore event kinds — and versions — it does not know, so new kinds can
 * ship without breaking older main-thread bundles against newer workers. */
export interface EventMessage {
  v: 1;
  event: EventBody;
  /** `cycleOutcome` only: transferred payload buffers, aligned with
   * `event.changed`. */
  payloads?: ArrayBuffer[];
}

export interface ErrorShape {
  code: string;
  status: number;
  message: string;
  /** The wasm instance is poisoned (trap/OOM abort); the proxy recycles the
   * worker when it sees this. Absent on ordinary, recoverable errors. */
  fatal?: boolean;
}

export interface OkResponse {
  id: number;
  ok: true;
  /** Parsed JSON (init/ledger/snapshot/transact responses). */
  result?: unknown;
  /** Query result as UTF-8 JSON bytes (transport = "transfer"). */
  bytes?: Uint8Array;
  /** Query result as JSON text (transport = "clone"). */
  text?: string;
}

export interface ErrResponse {
  id: number;
  ok: false;
  error: ErrorShape;
}

export type Response = OkResponse | ErrResponse;

export interface InitResult {
  version: string;
  mode: EngineMode;
  /** The budget actually applied (echo of the request, for introspection). */
  maxMemoryBytes?: number;
}

export interface LedgerInfo {
  /** Normalized `name:branch` id. */
  id: string;
  /** Commit watermark (0 for a fresh ledger). */
  t: number;
  /** Index watermark; always 0 in playground mode (indexing disabled). */
  indexT: number;
}

export interface SnapshotInfo {
  /** Engine-side handle for the frozen view. */
  handle: number;
  /** Normalized `name:branch` id. */
  id: string;
  /** The frozen commit watermark. */
  t: number;
}

export interface CommitReceipt {
  /** Transaction time of the new commit. */
  t: number;
  /** CIDv1 of the commit object. */
  commit: string;
  /** Number of flakes (asserted + retracted) in the commit. */
  flakes: number;
}
