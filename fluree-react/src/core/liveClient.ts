/**
 * `LiveClient`: the object the provider injects. Owns the transport and the
 * query cache; exposes the watch/one-shot/connection surface the React
 * adapter (or any other framework adapter) builds on.
 */

import { QueryCache } from "./queryCache.js";
import type { QueryStore } from "./queryCache.js";
import type { LiveTransport } from "./transport.js";
import type { ConnectionState, QueryOptions, ResolvedSpec } from "./types.js";
import { resolveSpec, specKey } from "./types.js";

/** Default grace period before an unobserved query's subscription is
 * released. Long enough that unmount/remount (tab switches, list
 * virtualization, StrictMode double-mounts) never cold-starts; short enough
 * that abandoned queries stop consuming re-fetch cycles promptly. */
const DEFAULT_GC_TIME_MS = 30_000;

export interface ClientOptions {
  /** Client-level default for the per-query GC grace period (ms). */
  gcTime?: number;
}

export class LiveClient {
  private readonly cache: QueryCache;
  private readonly transport: LiveTransport;
  private readonly connListeners = new Set<(state: ConnectionState) => void>();
  private closed = false;

  constructor(transport: LiveTransport, options: ClientOptions = {}) {
    this.transport = transport;
    this.cache = new QueryCache(
      transport,
      options.gcTime ?? DEFAULT_GC_TIME_MS,
    );
    transport.start({
      onCycle: (cycle) => this.cache.applyCycle(cycle),
      onConnection: (state) => {
        for (const listener of [...this.connListeners]) listener(state);
      },
    });
  }

  /** The cache key for a query — exported so the adapter can use it as a
   * memo dependency without touching the cache. */
  keyFor(
    ledger: string,
    query: string | Record<string, unknown>,
    opts?: QueryOptions,
  ): string {
    return specKey(resolveSpec(ledger, query, opts));
  }

  /**
   * The shared store for a query: `{subscribe, getSnapshot}` with stable
   * function identities, deduped by key across all callers. Obtaining the
   * store is side-effect-light (a cache entry, janitored if never
   * observed); the live subscription starts on the first `subscribe`.
   */
  watch(
    ledger: string,
    query: string | Record<string, unknown>,
    opts?: QueryOptions,
  ): QueryStore {
    const spec: ResolvedSpec = resolveSpec(ledger, query, opts);
    return this.cache.handleFor(spec, opts?.gcTime).store;
  }

  /** One-shot query outside the subscription system. */
  query(
    ledger: string,
    query: string | Record<string, unknown>,
    opts?: QueryOptions,
  ): Promise<unknown> {
    return this.transport.fetchOnce(resolveSpec(ledger, query, opts));
  }

  connectionState(): ConnectionState {
    return this.transport.connectionState();
  }

  onConnectionChange(listener: (state: ConnectionState) => void): () => void {
    this.connListeners.add(listener);
    return () => {
      this.connListeners.delete(listener);
    };
  }

  /** Latest commit watermark observed for a ledger, if any cycle ran. */
  ledgerHead(ledger: string): number | undefined {
    return this.cache.ledgerHead(ledger);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.cache.close();
    this.transport.close();
    this.connListeners.clear();
  }
}
