/**
 * The main-thread query cache: one `QueryHandle` per distinct query key,
 * shared by every component observing that query.
 *
 * Lifecycle (React Query's observer model, minus the fetch brain):
 * - handles are created on first watch and deduped by key;
 * - the transport subscription starts when the FIRST observer attaches and
 *   is released only by garbage collection;
 * - when the last observer detaches, the handle enters a gcTime grace
 *   period — unmount/remount within it is instant and keeps live updates
 *   flowing; the timer is cancelled by any new observer;
 * - a handle created during render but never observed (interrupted render,
 *   suspended tree) is janitored by the same timer.
 *
 * Referential stability: `handle.state` is THE snapshot object handed to
 * `useSyncExternalStore`. It is replaced only when this query's results,
 * status, or error actually change — structural sharing against the
 * previous data means a byte-identical re-delivery keeps the exact same
 * object, and a changed result reuses every unchanged row's identity.
 *
 * Version coherence: `applyCycle` swaps EVERY affected handle's state
 * before notifying ANY observer, so two components rendering in the same
 * pass can never see different watermarks' data.
 */

import { replaceEqualDeep } from "./structuralShare.js";
import type { LiveTransport, CycleUpdate } from "./transport.js";
import type { QueryResult, ResolvedSpec } from "./types.js";
import { specKey } from "./types.js";

/** What the React adapter needs: a subscribable, snapshottable store. */
export interface QueryStore {
  subscribe(onChange: () => void): () => void;
  getSnapshot(): QueryResult;
}

const INITIAL_STATE: QueryResult = Object.freeze({
  data: undefined,
  status: "loading",
  error: undefined,
  t: undefined,
});

export class QueryHandle {
  state: QueryResult = INITIAL_STATE;
  readonly store: QueryStore;
  readonly key: string;
  readonly spec: ResolvedSpec;
  /** Transport subscription id. Reassigned if the handle is resurrected
   * after collection (see `QueryCache.ensureSubscribed`). */
  subId: number;
  private readonly listeners = new Set<() => void>();
  private observerCount = 0;
  private gcTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly gcTime: number;
  private readonly cache: QueryCache;

  constructor(
    cache: QueryCache,
    spec: ResolvedSpec,
    subId: number,
    gcTime: number,
  ) {
    this.cache = cache;
    this.spec = spec;
    this.key = specKey(spec);
    this.subId = subId;
    this.gcTime = gcTime;
    // Bound once: `useSyncExternalStore` requires stable function
    // identities, and components share this store across renders.
    this.store = {
      subscribe: (onChange: () => void) => this.addObserver(onChange),
      getSnapshot: () => this.state,
    };
    // Janitor for never-observed handles (see module docs).
    this.scheduleGc();
  }

  private addObserver(onChange: () => void): () => void {
    this.listeners.add(onChange);
    this.observerCount++;
    this.cancelGc();
    if (this.observerCount === 1) {
      this.cache.ensureSubscribed(this);
    }
    let removed = false;
    return () => {
      if (removed) return; // React may call cleanup defensively
      removed = true;
      this.listeners.delete(onChange);
      this.observerCount--;
      if (this.observerCount === 0) this.scheduleGc();
    };
  }

  notify(): void {
    for (const listener of [...this.listeners]) listener();
  }

  hasObservers(): boolean {
    return this.observerCount > 0;
  }

  private scheduleGc(): void {
    this.cancelGc();
    this.gcTimer = setTimeout(() => this.cache.collect(this), this.gcTime);
  }

  private cancelGc(): void {
    if (this.gcTimer !== undefined) {
      clearTimeout(this.gcTimer);
      this.gcTimer = undefined;
    }
  }

  /** Called only by the cache on GC / client close. */
  dispose(): void {
    this.cancelGc();
    this.listeners.clear();
  }
}

export class QueryCache {
  private readonly byKey = new Map<string, QueryHandle>();
  private readonly bySubId = new Map<number, QueryHandle>();
  private readonly heads = new Map<string, number>();
  private readonly subscribed = new Set<number>();
  private nextSubId = 1;
  private closed = false;

  constructor(
    private readonly transport: LiveTransport,
    private readonly defaultGcTime: number,
  ) {}

  /** Get-or-create the shared handle for a spec. Pure lookup on the hot
   * path: safe to call during React render. */
  handleFor(spec: ResolvedSpec, gcTime?: number): QueryHandle {
    const key = specKey(spec);
    const existing = this.byKey.get(key);
    if (existing) return existing;
    const handle = new QueryHandle(
      this,
      spec,
      this.nextSubId++,
      gcTime ?? this.defaultGcTime,
    );
    this.byKey.set(key, handle);
    this.bySubId.set(handle.subId, handle);
    return handle;
  }

  /** First observer attached: open the live subscription (idempotent — a
   * remount within the grace period finds it already open). */
  ensureSubscribed(handle: QueryHandle): void {
    if (this.closed) return;
    if (this.bySubId.get(handle.subId) !== handle) {
      // Resurrection. A component can hold this handle's memoized store
      // across a collection — React runs subscribe cleanup on trees that
      // stay mounted (Activity/Offscreen, and any future re-subscribe) —
      // and would then own a store the cache had forgotten, so every cycle
      // for it would be dropped and the component would freeze on stale
      // data with no error. Re-adopt it under a FRESH id, which also means
      // a late cycle addressed to the released subscription cannot land.
      handle.subId = this.nextSubId++;
      this.bySubId.set(handle.subId, handle);
      // Only claim the key if nothing else has taken it meanwhile; a rival
      // handle for the same key stays the one new callers dedup onto.
      if (!this.byKey.has(handle.key)) this.byKey.set(handle.key, handle);
    } else if (this.subscribed.has(handle.subId)) {
      return;
    }
    this.subscribed.add(handle.subId);
    this.transport.subscribe({ ...handle.spec, subId: handle.subId });
  }

  /** GC timer fired: release the subscription and drop the handle. */
  collect(handle: QueryHandle): void {
    if (this.bySubId.get(handle.subId) !== handle) return; // already collected
    if (handle.hasObservers()) return; // re-observed; the detach reschedules
    if (this.byKey.get(handle.key) === handle) this.byKey.delete(handle.key);
    this.bySubId.delete(handle.subId);
    if (this.subscribed.delete(handle.subId) && !this.closed) {
      this.transport.unsubscribe(handle.subId);
    }
    handle.dispose();
  }

  /** Latest observed commit watermark for a ledger (from cycles). */
  ledgerHead(ledger: string): number | undefined {
    return this.heads.get(ledger);
  }

  /**
   * Apply one advance-cycle batch. Two phases: swap every affected handle's
   * state, THEN notify — the coherence guarantee lives here.
   */
  applyCycle(cycle: CycleUpdate): void {
    const dirty: QueryHandle[] = [];

    for (const { subId, payload } of cycle.changed) {
      const handle = this.bySubId.get(subId);
      if (!handle) continue; // unsubscribed while the cycle was in flight
      const shared = replaceEqualDeep(handle.state.data, payload);
      if (shared === handle.state.data && handle.state.status === "ready") {
        // Transport said "changed" but the parsed trees are deep-equal
        // (e.g. a peer transport whose gate hashes bytes, not JSON).
        // Referential stability wins: keep the snapshot.
        continue;
      }
      handle.state = {
        data: shared,
        status: "ready",
        error: undefined,
        t: cycle.t,
      };
      dirty.push(handle);
    }

    for (const subId of cycle.unchanged) {
      const handle = this.bySubId.get(subId);
      if (!handle) continue;
      if (handle.state.status === "error") {
        // Recovery: the re-run succeeded and matched the last good data.
        handle.state = {
          data: handle.state.data,
          status: "ready",
          error: undefined,
          t: cycle.t,
        };
        dirty.push(handle);
      }
      // Otherwise: unchanged results keep the exact same snapshot object —
      // no state swap, no notification, no re-render.
    }

    for (const { subId, error } of cycle.errored) {
      const handle = this.bySubId.get(subId);
      if (!handle) continue;
      // Keep-last-good-data: `data` and its watermark survive the error.
      handle.state = {
        data: handle.state.data,
        status: "error",
        error,
        t: handle.state.t,
      };
      dirty.push(handle);
    }

    const head = this.heads.get(cycle.ledger);
    if (head === undefined || cycle.t > head) {
      this.heads.set(cycle.ledger, cycle.t);
    }

    for (const handle of dirty) handle.notify();
  }

  close(): void {
    this.closed = true;
    for (const handle of this.byKey.values()) handle.dispose();
    this.byKey.clear();
    this.bySubId.clear();
    this.subscribed.clear();
  }
}
