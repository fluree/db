/**
 * Message protocol between the main-thread proxy (`index.ts`) and the engine
 * worker (`worker.ts`). Every request carries a client-assigned `id`; the
 * worker answers exactly once with the same `id`.
 *
 * Boundary values are plain JSON except query results, which travel as UTF-8
 * JSON bytes in a transferred `ArrayBuffer` by default (see
 * `ResultTransport`). Only buffered results exist on this protocol — the
 * engine's streaming query entry is deliberately not exposed (adversarial
 * review F6: streamed rows cannot participate in peer mode's fetch-and-re-run
 * loop, and the playground keeps the same shape). Keep this file
 * dependency-free: it is imported by both sides and by the smoke test.
 */

/** Which mode a worker was initialized in. Peer mode is reserved for the
 * remote-cache transport (`fluree-db-browser`) and is not implemented here. */
export type EngineMode = "playground";

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
  /** Per-query memory budget in bytes (F4). Omit for the engine default. */
  maxMemoryBytes?: number;
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
}

export type Request =
  | InitRequest
  | LedgerRequest
  | SnapshotRequest
  | ReleaseRequest
  | TransactRequest
  | QueryRequest;

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
