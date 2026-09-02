/**
 * Core types for `@fluree/react`. This module (and everything under
 * `src/core/`) is framework-agnostic: zero React imports. The React adapter
 * in `src/react/` is a thin subscription layer over these.
 */

/** Query language. Inferred from the query argument when not given:
 * a string is SPARQL, an object is a JSON-LD (FlureeQL) query. */
export type QueryKind = "sparql" | "jsonld";

/**
 * Result format at the hook boundary. Defaults are language-matched — a
 * SPARQL query returns SPARQL JSON results, a JSON-LD query returns the
 * engine's formatted JSON-LD — so components see exactly what the HTTP API
 * returns for that query language, and switching client modes (remote vs
 * peer) can never change a component's data shape. Overridable per query via
 * `opts.format`; unknown strings are passed through to the transport (the
 * remote transport sends a MIME-looking value as the `Accept` header).
 */
export type QueryFormat = "sparql-json" | "jsonld" | (string & {});

export type QueryStatus = "loading" | "ready" | "error";

/** Error shape surfaced on a query subscription. */
export interface QueryError {
  code: string;
  message: string;
  /** HTTP-ish status when the transport has one. */
  status?: number;
}

/**
 * The snapshot object a query subscription exposes. Referentially stable:
 * the SAME object is returned until this query's results (or status)
 * actually change, which is what lets `useSyncExternalStore` skip
 * re-renders. On error the last good `data` is kept (`status` flips to
 * "error" and `error` is populated); the next successful cycle clears it.
 */
export interface QueryResult<T = unknown> {
  /** Last good result. `undefined` until the first result arrives. */
  data: T | undefined;
  status: QueryStatus;
  error: QueryError | undefined;
  /**
   * The commit watermark at which `data` was last (re)computed. Note: when a
   * cycle re-runs this query at a newer head and finds the results
   * byte-identical, the snapshot (including `t`) is deliberately NOT
   * replaced — referential stability wins, and the data is still exactly
   * what the newer head would return. Use `LiveClient.ledgerHead()` for the
   * ledger's latest observed watermark.
   */
  t: number | undefined;
}

/** Per-query options accepted by `useQuery` / `LiveClient.watch`. */
export interface QueryOptions {
  /** Override query-kind inference. */
  kind?: QueryKind;
  /** Override the language-matched result format. */
  format?: QueryFormat;
  /**
   * Anchor the query at a fixed commit watermark (time travel). A t-anchored
   * subscription is fetched once and never invalidated by new commits.
   */
  at?: number;
  /**
   * Grace period (ms) before an unobserved query is garbage-collected and
   * its live subscription released. Applied when the query's cache entry is
   * first created. Defaults to the client-level `gcTime` (30s).
   */
  gcTime?: number;
}

/**
 * Client connection lifecycle, as exposed by `useConnectionState`.
 *
 * `idle` means there is nothing to connect: no query is subscribed, or every
 * subscribed query is time-anchored (`opts.at`) and so needs no live stream.
 * It is distinct from `connecting` (a stream is being established) so a
 * settled page does not show a perpetual spinner, and from `live` so a page
 * that has dropped its last subscription does not show a stale "connected".
 */
export type ConnectionState =
  | "idle"
  | "connecting"
  | "live"
  | "reconnecting"
  | "closed";

/** Fully-resolved subscription parameters (post inference + defaults). */
export interface ResolvedSpec {
  ledger: string;
  kind: QueryKind;
  /** Raw query text (SPARQL string, or the JSON text of a JSON-LD query). */
  text: string;
  format: QueryFormat;
  at?: number;
}

/**
 * Cache key for a resolved spec. v1 keys on raw text (plus ledger, kind,
 * format, and time anchor) — two textually different but semantically
 * identical queries get separate subscriptions, which costs dedup, never
 * correctness. IR-normalized keying is a later upgrade.
 */
export function specKey(spec: ResolvedSpec): string {
  return [spec.kind, spec.ledger, spec.format, spec.at ?? "", spec.text].join(
    "\0",
  );
}

/** Resolve the user-facing (ledger, query, opts) triple into a spec. */
export function resolveSpec(
  ledger: string,
  query: string | Record<string, unknown>,
  opts?: QueryOptions,
): ResolvedSpec {
  const kind: QueryKind =
    opts?.kind ?? (typeof query === "string" ? "sparql" : "jsonld");
  const text = typeof query === "string" ? query : JSON.stringify(query);
  const format: QueryFormat =
    opts?.format ?? (kind === "sparql" ? "sparql-json" : "jsonld");
  const spec: ResolvedSpec = { ledger, kind, text, format };
  if (opts?.at !== undefined) spec.at = opts.at;
  return spec;
}
