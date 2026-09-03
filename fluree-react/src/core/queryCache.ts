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
 * A store is addressed by KEY, not by the handle that minted it: a component
 * memoizes the store it first obtained and can outlive that handle (React
 * runs subscribe cleanup on trees that stay mounted — Activity/Offscreen —
 * so a still-mounted component's query can be collected and then re-observed).
 * Every `subscribe`/`getSnapshot` therefore re-resolves whichever handle owns
 * the key now, which is what keeps `byKey` authoritative: one key can never
 * hold two live handles on two timelines.
 *
 * Referential stability: `handle.state` is THE snapshot object handed to
 * `useSyncExternalStore`. It is replaced only when this query's results,
 * status, or error actually change — structural sharing against the
 * previous data means a byte-identical re-delivery keeps the exact same
 * object, and a changed result reuses every unchanged row's identity.
 *
 * Version coherence: `applyCycle` swaps EVERY affected handle's state
 * before notifying ANY observer, so two components rendering in the same
 * pass can never see different watermarks' data. That holds within a cycle;
 * ACROSS cycles it is `applyCycle`'s staleness skip that keeps a handle from
 * moving backwards when a transport delivers out of order.
 */

import { replaceEqualDeep } from "./structuralShare.js";
import type { LiveTransport, CycleUpdate } from "./transport.js";
import type { QueryError, QueryResult, ResolvedSpec } from "./types.js";
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

/**
 * Snapshot for a query first observed AFTER the client was closed. A frozen
 * singleton so `getSnapshot` stays referentially stable (a fresh object each
 * call would loop `useSyncExternalStore`). Distinct from `INITIAL_STATE`: a
 * closed cache never observes or fetches, so a new query would otherwise sit
 * in `loading` forever with nothing to move it — the same silent-freeze shape
 * the transport-contract guard elsewhere goes out of its way to make loud.
 */
const CLOSED_STATE: QueryResult = Object.freeze({
  data: undefined,
  status: "error" as const,
  error: Object.freeze({
    code: "client-closed",
    message: "the Fluree client was closed before this query was observed",
  }),
  t: undefined,
});

/**
 * The store handed to a query first observed AFTER `close()`. A frozen
 * singleton with stable `subscribe`/`getSnapshot` identities — a real handle
 * would be minted into `byKey` (even when closed) and its `INITIAL_STATE`
 * would win in `snapshotFor` before the closed-check, so the component would
 * spin on `loading` forever. `subscribe` is a no-op (nothing will ever fire)
 * and `getSnapshot` always returns the same `CLOSED_STATE`, so
 * `useSyncExternalStore` neither loops nor waits.
 */
const CLOSED_STORE: QueryStore = Object.freeze({
  subscribe: () => () => {},
  getSnapshot: () => CLOSED_STATE,
});

function sameError(a: QueryError | undefined, b: QueryError): boolean {
  return (
    a !== undefined &&
    a.code === b.code &&
    a.message === b.message &&
    a.status === b.status
  );
}

export class QueryHandle {
  state: QueryResult = INITIAL_STATE;
  readonly store: QueryStore;
  readonly key: string;
  readonly spec: ResolvedSpec;
  /** Transport subscription id. Fixed for the life of the handle. */
  readonly subId: number;
  /** Grace period this handle was created with; inherited by the handle
   * that replaces it if a store outlives a collection. */
  readonly gcTime: number;
  /** Whether this subscription has ever produced a result. `status` cannot
   * answer that — a handle reaches `error` from `loading` without one — and
   * two branches below need to distinguish "no news" from "no data yet". */
  hasResult = false;
  private readonly listeners = new Set<() => void>();
  private observerCount = 0;
  private gcTimer: ReturnType<typeof setTimeout> | undefined;
  private disposed = false;
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
    // identities, and components share this store across renders. Both
    // members go through the cache BY KEY (see the module docs) so a store
    // whose handle was collected transparently re-binds to the live one.
    this.store = {
      subscribe: (onChange: () => void) =>
        this.cache.observe(this.key, this.spec, this.gcTime, onChange),
      getSnapshot: () =>
        this.cache.snapshotFor(this.key, this.spec, this.gcTime),
    };
  }

  /** Called only by the cache, and only on the handle that owns the key. */
  addObserver(onChange: () => void): () => void {
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

  /** Janitor for a handle nobody ever observes. Armed by the cache on
   * creation, so a closed cache cannot arm one. */
  armJanitor(): void {
    this.scheduleGc();
  }

  private scheduleGc(): void {
    this.cancelGc();
    // A disposed handle is off the books: re-arming here is how a detach
    // that arrives after `close()` used to leave a live timer behind.
    if (this.disposed) return;
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
    this.disposed = true;
    this.cancelGc();
    this.listeners.clear();
  }
}

export class QueryCache {
  private readonly byKey = new Map<string, QueryHandle>();
  private readonly bySubId = new Map<number, QueryHandle>();
  private readonly heads = new Map<string, number>();
  private readonly headListeners = new Map<string, Set<() => void>>();
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
    return this.liveHandleFor(
      specKey(spec),
      spec,
      gcTime ?? this.defaultGcTime,
    );
  }

  /** The store `useQuery` binds to. On a closed cache, a shared closed store
   * (typed `client-closed` error) rather than a freshly-minted `loading`
   * handle that nothing can ever resolve. */
  storeFor(spec: ResolvedSpec, gcTime?: number): QueryStore {
    if (this.closed) return CLOSED_STORE;
    return this.handleFor(spec, gcTime).store;
  }

  /** Attach an observer to whichever handle owns `key` right now. */
  observe(
    key: string,
    spec: ResolvedSpec,
    gcTime: number,
    onChange: () => void,
  ): () => void {
    if (this.closed) return () => {};
    return this.liveHandleFor(key, spec, gcTime).addObserver(onChange);
  }

  /** The snapshot of whichever handle owns `key` right now. */
  snapshotFor(key: string, spec: ResolvedSpec, gcTime: number): QueryResult {
    const existing = this.byKey.get(key);
    if (existing) return existing.state;
    // A closed cache mints nothing: handles survive `close()` precisely so
    // a still-mounted component keeps rendering what it last had (the
    // `existing` branch above). A query first observed AFTER close has no
    // handle and never will — surface a typed error rather than a permanent
    // `loading` that nothing can ever resolve.
    if (this.closed) return CLOSED_STATE;
    return this.liveHandleFor(key, spec, gcTime).state;
  }

  private liveHandleFor(
    key: string,
    spec: ResolvedSpec,
    gcTime: number,
  ): QueryHandle {
    const existing = this.byKey.get(key);
    if (existing) return existing;
    const handle = new QueryHandle(this, spec, this.nextSubId++, gcTime);
    // A closed cache mints nothing durable. `storeFor`/`observe`/`snapshotFor`
    // all short-circuit before reaching here, so this branch is only hit by a
    // caller that goes straight to the public `handleFor` on a closed cache.
    // Tracking the handle in `byKey`/`bySubId` here would strand it forever:
    // `observe` no-ops on a closed cache (so it can never gain an observer to
    // later trigger `collect`) and the janitor below never arms — an
    // uncollectable leak. Hand back a standalone handle instead: usable by
    // the caller, but untracked, so there is nothing left for the cache to
    // hold onto.
    if (this.closed) return handle;
    this.byKey.set(key, handle);
    this.bySubId.set(handle.subId, handle);
    handle.armJanitor();
    return handle;
  }

  /** First observer attached: open the live subscription (idempotent — a
   * remount within the grace period finds it already open). */
  ensureSubscribed(handle: QueryHandle): void {
    if (this.closed) return;
    // Stores resolve their handle by key on every call, so anything that
    // reaches here is the handle `byKey` holds. The check is the assertion,
    // not a fallback: a handle that lost the key must never open a second
    // subscription on it.
    if (this.byKey.get(handle.key) !== handle) return;
    if (this.subscribed.has(handle.subId)) return;
    this.subscribed.add(handle.subId);
    this.transport.subscribe({ ...handle.spec, subId: handle.subId });
  }

  /** GC timer fired: release the subscription and drop the handle. */
  collect(handle: QueryHandle): void {
    if (handle.hasObservers()) return; // re-observed; the detach reschedules
    if (this.byKey.get(handle.key) !== handle) return; // already collected
    this.byKey.delete(handle.key);
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
   * Watch a ledger's head. The listener fires when the watermark actually
   * MOVES — a cycle that re-runs everything and finds nothing new does not
   * call it — which is what makes the head renderable without a timer.
   */
  onLedgerHead(ledger: string, listener: () => void): () => void {
    let set = this.headListeners.get(ledger);
    if (!set) {
      set = new Set();
      this.headListeners.set(ledger, set);
    }
    set.add(listener);
    return () => {
      set.delete(listener);
      // Only drop the ledger's entry if THIS set is still the registered one.
      // A later re-subscribe may have replaced it with a fresh set under the
      // same ledger; deleting then would evict that other component's
      // listeners (they would silently stop updating). This identity check
      // also makes a defensive double-detach harmless — the second call finds
      // the entry either gone or replaced — so unlike `addObserver` (which
      // must guard an observer COUNT against double-decrement) this path needs
      // no separate `removed` flag; the delete is idempotent by construction.
      if (set.size === 0 && this.headListeners.get(ledger) === set) {
        this.headListeners.delete(ledger);
      }
    };
  }

  /**
   * True when `cycle` is older than what this handle already carries.
   *
   * A transport is meant to deliver a subscription's cycles in watermark
   * order, but the shipped two pay for that separately (`RemoteTransport`
   * with per-subscription tickets, the peer engine with its coalescer) and a
   * consumer-supplied transport may not pay for it at all. A late cycle
   * landing on top of a newer one pins that component below its siblings —
   * and because the transport's change gate then holds the STALE result as
   * its baseline, a later commit restoring that result is reported
   * `unchanged` with no payload and the component never recovers. Dropping
   * the stale entry is the only recovery the cache can perform by itself.
   *
   * Per handle rather than per ledger: a t-anchored subscription's `t` is
   * its anchor, not a delivery watermark, so it is exempt.
   */
  private stale(handle: QueryHandle, cycle: CycleUpdate): boolean {
    return (
      cycle.t !== undefined &&
      handle.spec.at === undefined &&
      handle.state.t !== undefined &&
      cycle.t < handle.state.t
    );
  }

  /**
   * Apply one advance-cycle batch. Two phases: swap every affected handle's
   * state, THEN notify — the coherence guarantee lives here.
   */
  applyCycle(cycle: CycleUpdate): void {
    if (this.closed) return;
    const dirty: QueryHandle[] = [];

    for (const { subId, payload } of cycle.changed) {
      const handle = this.bySubId.get(subId);
      if (!handle) continue; // unsubscribed while the cycle was in flight
      if (this.stale(handle, cycle)) continue;
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
      handle.hasResult = true;
      dirty.push(handle);
    }

    for (const subId of cycle.unchanged) {
      const handle = this.bySubId.get(subId);
      if (!handle) continue;
      if (this.stale(handle, cycle)) continue;
      if (!handle.hasResult) {
        // Contract violation. "Unchanged" means "re-evaluated at `t` and
        // identical to what this subscription last produced" — a handle that
        // has never produced anything has nothing for that to be true
        // against, so a transport saying it here has lost the initial
        // delivery. Doing nothing strands the component in `loading` for
        // good with nothing on screen to explain it, which is precisely the
        // silent freeze this package exists to remove; say it out loud
        // instead. Unreachable through either shipped transport, but
        // `LiveTransport` is exported, and this is where the next
        // implementor finds out. A transport that repeats the violation
        // every cycle renders it once, not once per commit.
        const error: QueryError = {
          code: "transport-contract",
          message:
            `transport reported subscription ${subId} unchanged before it ` +
            "ever delivered a result",
        };
        if (handle.state.status === "error" && sameError(handle.state.error, error)) {
          continue;
        }
        handle.state = { data: undefined, status: "error", error, t: undefined };
        dirty.push(handle);
      } else if (handle.state.status === "error") {
        // Recovery: the re-run succeeded and matched the last good data.
        // Guarded on `hasResult` above, so there IS last good data — without
        // it this branch could flip a never-delivered handle to `ready` with
        // nothing in it.
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
      if (this.stale(handle, cycle)) continue;
      if (handle.state.status === "error" && sameError(handle.state.error, error)) {
        // A persistent failure is re-reported every cycle by design. Minting
        // an identical error state would re-render this component (and every
        // memo boundary under it) on every commit, for a message that did
        // not change — worst on exactly the busy, failing ledger where the
        // page is already under stress.
        continue;
      }
      // Keep-last-good-data: `data` and its watermark survive the error.
      handle.state = {
        data: handle.state.data,
        status: "error",
        error,
        t: handle.state.t,
      };
      dirty.push(handle);
    }

    let headMoved = false;
    if (cycle.t !== undefined && cycle.t >= 0) {
      const head = this.heads.get(cycle.ledger);
      if (head === undefined || cycle.t > head) {
        this.heads.set(cycle.ledger, cycle.t);
        headMoved = true;
      }
    }

    for (const handle of dirty) handle.notify();
    // Announced in the notify phase like everything else, so a watcher that
    // reads a query's `t` on the way past sees it already at this head — a
    // header and a row cannot disagree inside one React pass.
    if (headMoved) {
      const listeners = this.headListeners.get(cycle.ledger);
      if (listeners) for (const listener of [...listeners]) listener();
    }
  }

  close(): void {
    this.closed = true;
    // Every handle, not just the keyed ones — and the maps are KEPT, so a
    // component still mounted when the client closes keeps rendering its
    // last snapshot instead of flipping back to `loading`.
    for (const handle of this.bySubId.values()) handle.dispose();
    this.subscribed.clear();
    this.headListeners.clear();
  }
}
