/**
 * `useQuery`: one live query per call, on `useSyncExternalStore`.
 *
 * The stability contracts the hook honors:
 * - `getSnapshot` returns the handle's cached state object, which the core
 *   replaces only when this query's results actually change — same
 *   reference means no re-render, and no infinite loop;
 * - `subscribe`/`getSnapshot` identities come from the shared store facade
 *   (bound once per cache entry), memoized here on the query KEY, so a new
 *   inline query object with identical content re-uses the same
 *   subscription;
 * - `getServerSnapshot` returns a module-level constant "loading" snapshot:
 *   the wasm/worker/SSE machinery is browser-only, so SSR renders the
 *   loading state and the client takes over after hydration.
 */

import { useMemo, useSyncExternalStore } from "react";
import type { QueryOptions, QueryResult } from "../core/types.js";
import { useFlureeClient } from "./context.js";

const SERVER_SNAPSHOT: QueryResult = Object.freeze({
  data: undefined,
  status: "loading",
  error: undefined,
  t: undefined,
});

function getServerSnapshot(): QueryResult {
  return SERVER_SNAPSHOT;
}

export function useQuery<T = unknown>(
  ledger: string,
  query: string | Record<string, unknown>,
  opts?: QueryOptions,
): QueryResult<T> {
  const client = useFlureeClient();
  const key = client.keyFor(ledger, query, opts);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- key covers every
  // spec-affecting input (ledger, query text, kind, format, at).
  const store = useMemo(() => client.watch(ledger, query, opts), [client, key]);
  return useSyncExternalStore(
    store.subscribe,
    store.getSnapshot,
    getServerSnapshot,
  ) as QueryResult<T>;
}
