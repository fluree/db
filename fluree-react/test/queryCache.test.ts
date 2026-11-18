/**
 * The query cache is the framework-agnostic half of the package. These tests
 * pin the four properties the React adapter is allowed to assume:
 *
 * 1. identity/dedup — one handle per query key, shared by every observer;
 * 2. refcounting + grace-period GC — the transport subscription opens on the
 *    first observer and is released only after the last observer has been
 *    gone for `gcTime`;
 * 3. version coherence — every handle in one advance-cycle carries the new
 *    watermark BEFORE any observer is notified, so siblings cannot disagree;
 * 4. snapshot stability — the snapshot object is replaced only when this
 *    query's results, status, or error actually change.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryCache } from "../src/core/queryCache.js";
import type { QueryHandle } from "../src/core/queryCache.js";
import { resolveSpec } from "../src/core/types.js";
import { FakeTransport } from "./helpers.js";

const GC = 30_000;

function setup(gcTime = GC) {
  const transport = new FakeTransport();
  const cache = new QueryCache(transport, gcTime);
  transport.start({
    onCycle: (cycle) => cache.applyCycle(cycle),
    onConnection: () => {},
  });
  return { transport, cache };
}

const spec = (text: string, ledger = "my/ledger") =>
  resolveSpec(ledger, text);

/** Attach an observer and return both the detach fn and its call count. */
function observe(handle: QueryHandle) {
  const calls: number[] = [];
  const detach = handle.store.subscribe(() => calls.push(calls.length));
  return { detach, calls };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("identity and dedup", () => {
  it("returns the same handle and store for the same query key", () => {
    const { cache } = setup();
    const a = cache.handleFor(spec("SELECT * WHERE { ?s ?p ?o }"));
    const b = cache.handleFor(spec("SELECT * WHERE { ?s ?p ?o }"));
    expect(b).toBe(a);
    // `useSyncExternalStore` requires stable function identities.
    expect(b.store.subscribe).toBe(a.store.subscribe);
    expect(b.store.getSnapshot).toBe(a.store.getSnapshot);
  });

  it("separates queries that differ in text, ledger, format, or anchor", () => {
    const { cache } = setup();
    const base = cache.handleFor(spec("SELECT * WHERE { ?s ?p ?o }"));
    const others = [
      cache.handleFor(spec("SELECT ?s WHERE { ?s ?p ?o }")),
      cache.handleFor(spec("SELECT * WHERE { ?s ?p ?o }", "other/ledger")),
      cache.handleFor(
        resolveSpec("my/ledger", "SELECT * WHERE { ?s ?p ?o }", {
          format: "jsonld",
        }),
      ),
      cache.handleFor(
        resolveSpec("my/ledger", "SELECT * WHERE { ?s ?p ?o }", { at: 7 }),
      ),
    ];
    const ids = new Set([base.subId, ...others.map((h) => h.subId)]);
    expect(ids.size).toBe(5);
  });

  it("starts every handle in the loading state with no data", () => {
    const { cache } = setup();
    const h = cache.handleFor(spec("q"));
    expect(h.store.getSnapshot()).toEqual({
      data: undefined,
      status: "loading",
      error: undefined,
      t: undefined,
    });
  });
});

describe("refcounting and grace-period GC", () => {
  it("opens the transport subscription on the first observer only", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    expect(transport.subscribes).toHaveLength(0);

    const first = observe(h);
    expect(transport.subscribes).toHaveLength(1);
    expect(transport.subscribes[0]).toMatchObject({ subId: h.subId, text: "q" });

    observe(h);
    expect(transport.subscribes).toHaveLength(1);
    first.detach();
    expect(transport.unsubscribes).toHaveLength(0);
  });

  it("holds the subscription open for gcTime after the last observer leaves", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h).detach();

    vi.advanceTimersByTime(GC - 1);
    expect(transport.unsubscribes).toHaveLength(0);
    vi.advanceTimersByTime(1);
    expect(transport.unsubscribes).toEqual([h.subId]);
  });

  it("cancels GC when a new observer arrives inside the grace period", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h).detach();

    vi.advanceTimersByTime(GC - 1);
    observe(h); // remount
    vi.advanceTimersByTime(GC * 2);

    expect(transport.unsubscribes).toHaveLength(0);
    // The remount reused the live subscription — no re-subscribe, and the
    // data it already had is still there.
    expect(transport.subscribes).toHaveLength(1);
    expect(cache.handleFor(spec("q"))).toBe(h);
  });

  it("keeps live updates flowing during the grace period", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h).detach();

    vi.advanceTimersByTime(GC / 2);
    transport.emit({
      ledger: "my/ledger",
      t: 5,
      changed: [{ subId: h.subId, payload: { rows: [1] } }],
      unchanged: [],
      errored: [],
    });

    // A remount inside the grace period is instant: the data is already here.
    expect(h.store.getSnapshot().data).toEqual({ rows: [1] });
    expect(h.store.getSnapshot().status).toBe("ready");
  });

  it("janitors a handle that was created during render but never observed", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q")); // interrupted render
    vi.advanceTimersByTime(GC);

    expect(transport.unsubscribes).toHaveLength(0); // never subscribed
    expect(cache.handleFor(spec("q"))).not.toBe(h); // dropped from the cache
  });

  it("honours a per-query gcTime override", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"), 100);
    observe(h).detach();
    vi.advanceTimersByTime(100);
    expect(transport.unsubscribes).toEqual([h.subId]);
  });

  it("re-subscribes through a memoized store observed again after collection", () => {
    // A component can hold a memoized store across a collection (React can
    // run subscribe cleanup on a still-mounted tree). If the cache forgot
    // the handle, its cycles would be silently dropped and that component
    // would freeze forever with stale data. The store is addressed by KEY,
    // so observing it again resolves whichever handle owns the key now.
    const { cache, transport } = setup(100);
    const h = cache.handleFor(spec("q"));
    const firstId = h.subId;
    observe(h).detach();
    vi.advanceTimersByTime(100);
    expect(transport.unsubscribes).toEqual([firstId]);

    observe(h); // same store object, re-subscribed
    expect(transport.subscribes).toHaveLength(2);
    const secondId = transport.subId(1);
    expect(secondId).not.toBe(firstId);
    // Exactly one handle owns the key, and the memoized store speaks for it.
    expect(cache.handleFor(spec("q")).subId).toBe(secondId);

    transport.emit({
      ledger: "my/ledger",
      t: 9,
      changed: [{ subId: secondId, payload: { rows: [1] } }],
      unchanged: [],
      errored: [],
    });
    expect(h.store.getSnapshot().data).toEqual({ rows: [1] });

    // A late cycle addressed to the released subscription must not land.
    const before = h.store.getSnapshot();
    transport.emit({
      ledger: "my/ledger",
      t: 10,
      changed: [{ subId: firstId, payload: { rows: [99] } }],
      unchanged: [],
      errored: [],
    });
    expect(h.store.getSnapshot()).toBe(before);
  });

  it("re-binds a re-shown component onto the handle that now owns its key", () => {
    // The Activity/Offscreen case. X mounts; X is hidden and its effects are
    // torn down; the grace period elapses and X's handle is collected; Y
    // mounts the SAME query and takes the key; X is shown again and React
    // re-subscribes X's memoized store. X must join Y's handle. Re-adopting
    // X's own handle instead leaves one query key with two live
    // subscriptions on two timelines — permanently — so every commit runs
    // the query twice and a cycle naming only Y leaves X untouched.
    const { cache, transport } = setup(100);
    const x = cache.handleFor(spec("q"));
    const xStore = x.store;
    observe(x).detach();
    vi.advanceTimersByTime(100);
    expect(transport.unsubscribes).toEqual([x.subId]);

    const y = cache.handleFor(spec("q"));
    expect(y).not.toBe(x);
    observe(y);
    expect(transport.subscribes).toHaveLength(2);

    const detachX = xStore.subscribe(() => {});
    // No third subscription: X re-bound rather than resurrecting.
    expect(transport.subscribes).toHaveLength(2);
    expect(transport.subId(1)).toBe(y.subId);

    transport.emit({
      ledger: "my/ledger",
      t: 4,
      changed: [{ subId: y.subId, payload: { rows: ["v"] } }],
      unchanged: [],
      errored: [],
    });

    // One handle, one snapshot object: X and Y cannot disagree.
    expect(xStore.getSnapshot()).toBe(y.store.getSnapshot());
    expect(xStore.getSnapshot().data).toEqual({ rows: ["v"] });
    detachX();
  });
});

describe("applying an advance-cycle", () => {
  it("swaps the snapshot and notifies when results change", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    const before = h.store.getSnapshot();

    transport.emit({
      ledger: "my/ledger",
      t: 3,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });

    const after = h.store.getSnapshot();
    expect(after).not.toBe(before);
    expect(after).toEqual({
      data: { rows: ["a"] },
      status: "ready",
      error: undefined,
      t: 3,
    });
    expect(obs.calls).toHaveLength(1);
  });

  it("keeps the exact same snapshot object for an unchanged subscription", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });
    const snapshot = h.store.getSnapshot();

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [],
      unchanged: [h.subId],
      errored: [],
    });

    // Not merely deep-equal: the SAME object, so `useSyncExternalStore`
    // does not re-render. `t` deliberately stays at the watermark the data
    // was computed at — see the note on QueryResult.t.
    expect(h.store.getSnapshot()).toBe(snapshot);
    expect(obs.calls).toHaveLength(1);
  });

  it("keeps the snapshot when a transport reports 'changed' but the tree is equal", () => {
    // The peer transport's gate hashes formatted BYTES; a re-serialization
    // difference can report a change the parsed trees do not have.
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });
    const snapshot = h.store.getSnapshot();

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });

    expect(h.store.getSnapshot()).toBe(snapshot);
    expect(obs.calls).toHaveLength(1);
  });

  it("preserves row identity for unchanged rows across an advance", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [
        {
          subId: h.subId,
          payload: { rows: [{ id: 1, n: "ada" }, { id: 2, n: "grace" }] },
        },
      ],
      unchanged: [],
      errored: [],
    });
    const first = h.store.getSnapshot().data as { rows: unknown[] };

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [
        {
          subId: h.subId,
          payload: { rows: [{ id: 1, n: "ada" }, { id: 2, n: "hopper" }] },
        },
      ],
      unchanged: [],
      errored: [],
    });
    const second = h.store.getSnapshot().data as { rows: unknown[] };

    expect(second).not.toBe(first);
    expect(second.rows[0]).toBe(first.rows[0]); // memoized row: no re-render
    expect(second.rows[1]).not.toBe(first.rows[1]);
  });

  it("ignores cycle entries for subscriptions the cache no longer knows", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 4,
      changed: [{ subId: 9999, payload: { rows: [] } }],
      unchanged: [8888],
      errored: [{ subId: 7777, error: { code: "x", message: "x" } }],
    });
    expect(obs.calls).toHaveLength(0);
    expect(h.store.getSnapshot().status).toBe("loading");
  });

  it("tracks the ledger head and never moves it backwards", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    expect(cache.ledgerHead("my/ledger")).toBeUndefined();

    const cycle = (t: number) =>
      transport.emit({
        ledger: "my/ledger",
        t,
        changed: [],
        unchanged: [h.subId],
        errored: [],
      });
    cycle(5);
    expect(cache.ledgerHead("my/ledger")).toBe(5);
    cycle(3);
    expect(cache.ledgerHead("my/ledger")).toBe(5);
    cycle(8);
    expect(cache.ledgerHead("my/ledger")).toBe(8);
    expect(cache.ledgerHead("unknown/ledger")).toBeUndefined();

    // A watermark below zero is not a ledger state; it must not become one.
    transport.emit({
      ledger: "other/ledger",
      t: -1,
      changed: [],
      unchanged: [],
      errored: [],
    });
    expect(cache.ledgerHead("other/ledger")).toBeUndefined();
  });

  it("tells head watchers when the head moves, and only then", () => {
    // Rendering the ledger head is the one thing a query subscription does
    // not cover, and polling it on a timer is precisely what this package
    // exists to delete from an app's read path.
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    const seen: (number | undefined)[] = [];
    const detach = cache.onLedgerHead("my/ledger", () =>
      seen.push(cache.ledgerHead("my/ledger")),
    );
    const other: number[] = [];
    cache.onLedgerHead("some/other", () => other.push(1));

    const cycle = (t: number) =>
      transport.emit({
        ledger: "my/ledger",
        t,
        changed: [],
        unchanged: [h.subId],
        errored: [],
      });
    cycle(5);
    cycle(5); // same watermark: nothing moved
    cycle(3); // older: nothing moved
    cycle(8);
    expect(seen).toEqual([5, 8]);
    expect(other).toEqual([]); // a different ledger's watchers stay quiet

    detach();
    cycle(9);
    expect(seen).toEqual([5, 8]);
  });

  it("a stale head detach does not evict a re-subscribed watcher", () => {
    // Component A watches a ledger, then unmounts — emptying and removing the
    // ledger's listener set. Component B then mounts and watches the SAME
    // ledger, getting a fresh set under that key. If A's cleanup runs again
    // (React calls cleanup defensively, and a late unmount is ordinary), the
    // stale closure must not delete the ledger entry and evict B — B would
    // silently stop updating. This is the demo's own HeadBadge.
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);

    const detachA = cache.onLedgerHead("my/ledger", () => {});
    detachA(); // A unmounts: set empties, ledger entry removed.

    const seenB: (number | undefined)[] = [];
    cache.onLedgerHead("my/ledger", () =>
      seenB.push(cache.ledgerHead("my/ledger")),
    );
    detachA(); // A's cleanup fires again — must be a no-op for B.

    transport.emit({
      ledger: "my/ledger",
      t: 7,
      changed: [],
      unchanged: [h.subId],
      errored: [],
    });
    expect(seenB).toEqual([7]);
  });

  it("has every query already at the new head when it announces one", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    const observedT: (number | undefined)[] = [];
    cache.onLedgerHead("my/ledger", () => observedT.push(h.store.getSnapshot().t));

    transport.emit({
      ledger: "my/ledger",
      t: 4,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });

    // Not `undefined`: a header that renders the head and a row that renders
    // its query must not disagree inside one React pass.
    expect(observedT).toEqual([4]);
  });
});

describe("delivery order: a handle never moves backwards", () => {
  // A transport is meant to deliver a subscription's cycles in watermark
  // order, but the guarantee is paid for differently by each one and not at
  // all by a consumer-supplied transport. These pin what the cache does on
  // its own when a cycle lands late.

  it("drops a stale changed entry while its siblings stay at the newer head", () => {
    const { cache, transport } = setup();
    const a = cache.handleFor(spec("qa"));
    const b = cache.handleFor(spec("qb"));
    const obsA = observe(a);
    observe(b);

    // The fast cycle finishes first and both queries advance together.
    transport.emit({
      ledger: "my/ledger",
      t: 6,
      changed: [
        { subId: a.subId, payload: { rows: ["a6"] } },
        { subId: b.subId, payload: { rows: ["b6"] } },
      ],
      unchanged: [],
      errored: [],
    });
    const a6 = a.store.getSnapshot();
    expect(a6.t).toBe(6);

    // The slow cycle — opened BEFORE the commit, still in flight when the
    // faster one finished — lands afterwards carrying pre-commit data.
    transport.emit({
      ledger: "my/ledger",
      t: 5,
      changed: [{ subId: a.subId, payload: { rows: ["a5"] } }],
      unchanged: [],
      errored: [],
    });

    // Same object, so not even a re-render: A is not pinned below B.
    expect(a.store.getSnapshot()).toBe(a6);
    expect(a.store.getSnapshot().data).toEqual({ rows: ["a6"] });
    expect(obsA.calls).toHaveLength(1);
    expect(b.store.getSnapshot().t).toBe(6);
    expect(cache.ledgerHead("my/ledger")).toBe(6);

    // Dropping the stale cycle must not wedge the handle: the next real one
    // lands normally.
    transport.emit({
      ledger: "my/ledger",
      t: 7,
      changed: [{ subId: a.subId, payload: { rows: ["a7"] } }],
      unchanged: [],
      errored: [],
    });
    expect(a.store.getSnapshot().data).toEqual({ rows: ["a7"] });
    expect(a.store.getSnapshot().t).toBe(7);
    expect(obsA.calls).toHaveLength(2);
  });

  it("drops a stale error rather than failing a handle that already advanced", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 6,
      changed: [{ subId: h.subId, payload: { rows: ["a6"] } }],
      unchanged: [],
      errored: [],
    });
    const at6 = h.store.getSnapshot();

    transport.emit({
      ledger: "my/ledger",
      t: 5,
      changed: [],
      unchanged: [],
      errored: [{ subId: h.subId, error: { code: "http", message: "boom" } }],
    });

    // The failure is about a watermark this query has already left behind.
    expect(h.store.getSnapshot()).toBe(at6);
    expect(h.store.getSnapshot().status).toBe("ready");
    expect(obs.calls).toHaveLength(1);
  });

  it("delivers a t-anchored query at its anchor even when the ledger is ahead", () => {
    const { cache, transport } = setup();
    const live = cache.handleFor(spec("q"));
    observe(live);
    transport.emit({
      ledger: "my/ledger",
      t: 9,
      changed: [{ subId: live.subId, payload: { rows: ["now"] } }],
      unchanged: [],
      errored: [],
    });
    expect(cache.ledgerHead("my/ledger")).toBe(9);

    // Time travel: this subscription is pinned to t=3 and its cycle carries
    // that watermark, below the ledger head. A staleness guard written per
    // LEDGER rather than per handle would drop this delivery and leave the
    // component in `loading` forever.
    const past = cache.handleFor(resolveSpec("my/ledger", "q", { at: 3 }));
    observe(past);
    transport.emit({
      ledger: "my/ledger",
      t: 3,
      changed: [{ subId: past.subId, payload: { rows: ["then"] } }],
      unchanged: [],
      errored: [],
    });

    expect(past.store.getSnapshot().data).toEqual({ rows: ["then"] });
    expect(past.store.getSnapshot().t).toBe(3);
    // ...and the anchored cycle did not drag the ledger head back with it.
    expect(cache.ledgerHead("my/ledger")).toBe(9);
  });
});

describe("errors: keep-last-good-data", () => {
  it("keeps data and its watermark, flips status, and populates error", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });
    const good = h.store.getSnapshot().data;

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [],
      unchanged: [],
      errored: [
        { subId: h.subId, error: { code: "http", message: "boom", status: 503 } },
      ],
    });

    const snap = h.store.getSnapshot();
    expect(snap.status).toBe("error");
    expect(snap.error).toEqual({ code: "http", message: "boom", status: 503 });
    // The data the component is rendering survives the failure, identity and
    // all — the UI does not blank out on a transient server error.
    expect(snap.data).toBe(good);
    expect(snap.t).toBe(1);
    expect(obs.calls).toHaveLength(2);
  });

  it("re-renders once for a persistent identical error, not once per cycle", () => {
    // Errors repeat every cycle by design. A server 503ing on a ledger
    // committing ~10×/s would otherwise re-render this component (and every
    // memo boundary under it) ten times a second to show a string that never
    // changed — the exact churn this package exists to prevent, in the case
    // where the page is already under stress.
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    for (let t = 1; t <= 5; t++) {
      transport.emit({
        ledger: "my/ledger",
        t,
        changed: [],
        unchanged: [],
        errored: [
          {
            subId: h.subId,
            // A fresh object each cycle: equality is by value, as a real
            // transport re-minting its error shape would produce.
            error: { code: "http", message: "service unavailable", status: 503 },
          },
        ],
      });
    }
    expect(h.store.getSnapshot().status).toBe("error");
    expect(obs.calls).toHaveLength(1);
    const stuck = h.store.getSnapshot();

    // A DIFFERENT failure is still news and must get through.
    transport.emit({
      ledger: "my/ledger",
      t: 6,
      changed: [],
      unchanged: [],
      errored: [
        {
          subId: h.subId,
          error: { code: "http", message: "gateway timeout", status: 504 },
        },
      ],
    });
    expect(obs.calls).toHaveLength(2);
    expect(h.store.getSnapshot()).not.toBe(stuck);
    expect(h.store.getSnapshot().error).toEqual({
      code: "http",
      message: "gateway timeout",
      status: 504,
    });
  });

  it("errors a subscription reported unchanged before it ever delivered", () => {
    // A contract violation, not a data state: "unchanged" means "identical
    // to what this subscription last produced", and this one has produced
    // nothing. Unreachable through either shipped transport — but
    // `LiveTransport` is exported, and the alternative is a component that
    // spins forever with nothing on screen to explain it.
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);

    const violate = (t: number) =>
      transport.emit({
        ledger: "my/ledger",
        t,
        changed: [],
        unchanged: [h.subId],
        errored: [],
      });
    violate(3);

    const snap = h.store.getSnapshot();
    expect(snap.status).toBe("error");
    expect(snap.error?.code).toBe("transport-contract");
    expect(snap.error?.message).toContain(String(h.subId));
    expect(snap.data).toBeUndefined();
    expect(obs.calls).toHaveLength(1);

    // A transport that keeps doing it re-renders once, not once per commit —
    // and must NOT then be "recovered" into `ready` with nothing in it.
    violate(4);
    violate(5);
    expect(h.store.getSnapshot()).toBe(snap);
    expect(obs.calls).toHaveLength(1);

    // A real result still rescues it.
    transport.emit({
      ledger: "my/ledger",
      t: 6,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });
    expect(h.store.getSnapshot()).toMatchObject({
      status: "ready",
      data: { rows: ["a"] },
      t: 6,
    });
  });

  it("does not recover a handle that errored before its first result", () => {
    // The `unchanged` recovery branch keeps the last good data. A handle
    // whose very first fetch failed has none, so recovering it would report
    // `ready` with `data: undefined` — a component rendering an empty
    // success it never received.
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [],
      unchanged: [],
      errored: [{ subId: h.subId, error: { code: "http", message: "boom" } }],
    });
    expect(h.store.getSnapshot().status).toBe("error");

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [],
      unchanged: [h.subId],
      errored: [],
    });
    const snap = h.store.getSnapshot();
    expect(snap.status).toBe("error");
    expect(snap.data).toBeUndefined();
  });

  it("recovers to ready when a later cycle reports the query unchanged", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });
    const good = h.store.getSnapshot().data;
    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [],
      unchanged: [],
      errored: [{ subId: h.subId, error: { code: "http", message: "boom" } }],
    });
    transport.emit({
      ledger: "my/ledger",
      t: 3,
      changed: [],
      unchanged: [h.subId],
      errored: [],
    });

    const snap = h.store.getSnapshot();
    expect(snap.status).toBe("ready");
    expect(snap.error).toBeUndefined();
    expect(snap.data).toBe(good);
    expect(snap.t).toBe(3);
  });

  it("recovers to ready when a later cycle delivers new results", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [],
      unchanged: [],
      errored: [{ subId: h.subId, error: { code: "http", message: "boom" } }],
    });
    expect(h.store.getSnapshot()).toEqual({
      data: undefined,
      status: "error",
      error: { code: "http", message: "boom" },
      t: undefined,
    });

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [{ subId: h.subId, payload: { rows: ["a"] } }],
      unchanged: [],
      errored: [],
    });
    expect(h.store.getSnapshot()).toEqual({
      data: { rows: ["a"] },
      status: "ready",
      error: undefined,
      t: 2,
    });
  });
});

describe("version coherence", () => {
  it("swaps every handle in the cycle before notifying any observer", () => {
    const { cache, transport } = setup();
    const a = cache.handleFor(spec("qa"));
    const b = cache.handleFor(spec("qb"));
    const c = cache.handleFor(spec("qc"));
    for (const h of [a, b, c]) {
      h.store.subscribe(() => {});
    }
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [
        { subId: a.subId, payload: { v: "a1" } },
        { subId: b.subId, payload: { v: "b1" } },
        { subId: c.subId, payload: { v: "c1" } },
      ],
      unchanged: [],
      errored: [],
    });

    // Every observer records what the WHOLE cache looked like at the moment
    // it was told to re-read. If `applyCycle` notified inline while walking
    // the batch, an early observer would see a sibling still at t=1.
    const observations: Array<Array<number | undefined>> = [];
    for (const h of [a, b, c]) {
      h.store.subscribe(() =>
        observations.push([
          a.store.getSnapshot().t,
          b.store.getSnapshot().t,
          c.store.getSnapshot().t,
        ]),
      );
    }

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [
        { subId: a.subId, payload: { v: "a2" } },
        { subId: c.subId, payload: { v: "c2" } },
      ],
      // b re-ran at t=2 and matched: it keeps t=1 by design (its data is
      // still exactly what t=2 would return), and it is NOT notified.
      unchanged: [b.subId],
      errored: [],
    });

    expect(observations).toHaveLength(2); // a and c only; b did not change
    for (const seen of observations) {
      expect(seen).toEqual([2, 1, 2]);
    }
  });

  it("swaps a recovering handle before notifying a sibling that changed", () => {
    const { cache, transport } = setup();
    const a = cache.handleFor(spec("qa"));
    const b = cache.handleFor(spec("qb"));
    for (const h of [a, b]) h.store.subscribe(() => {});
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [
        { subId: a.subId, payload: { v: "a1" } },
        { subId: b.subId, payload: { v: "b1" } },
      ],
      unchanged: [],
      errored: [],
    });
    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [],
      unchanged: [],
      errored: [{ subId: b.subId, error: { code: "http", message: "boom" } }],
    });

    const seen: string[] = [];
    a.store.subscribe(() => seen.push(`a sees b=${b.store.getSnapshot().status}`));
    b.store.subscribe(() => seen.push(`b sees a=${a.store.getSnapshot().t}`));

    transport.emit({
      ledger: "my/ledger",
      t: 3,
      changed: [{ subId: a.subId, payload: { v: "a3" } }],
      unchanged: [b.subId], // b's re-run succeeded and matched: recovery
      errored: [],
    });

    expect(seen).toEqual(["a sees b=ready", "b sees a=3"]);
  });
});

describe("close", () => {
  it("stops notifying and applies nothing, but keeps the last snapshot", () => {
    const { cache, transport } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    transport.emit({
      ledger: "my/ledger",
      t: 1,
      changed: [{ subId: h.subId, payload: { rows: ["before"] } }],
      unchanged: [],
      errored: [],
    });
    const last = h.store.getSnapshot();
    cache.close();

    transport.emit({
      ledger: "my/ledger",
      t: 2,
      changed: [{ subId: h.subId, payload: { rows: ["after"] } }],
      unchanged: [],
      errored: [],
    });
    expect(obs.calls).toHaveLength(1); // the pre-close cycle only
    // A component still mounted when the client closed keeps rendering what
    // it last had rather than flipping back to `loading`.
    expect(h.store.getSnapshot()).toBe(last);
    expect(cache.handleFor(spec("q"))).toBe(h);
  });

  it("arms no new GC timer for a detach that arrives after close", () => {
    const { cache } = setup();
    const h = cache.handleFor(spec("q"));
    const obs = observe(h);
    cache.close();
    expect(vi.getTimerCount()).toBe(0);
    obs.detach(); // React unmounting the tree after the client closed
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not open new transport subscriptions after close", () => {
    const { cache, transport } = setup();
    cache.close();
    const h = cache.handleFor(spec("q"));
    observe(h);
    expect(transport.subscribes).toHaveLength(0);
    // ...and creating that handle armed no janitor either: a cache entry
    // minted by a render that outlived `close()` must not hold a 30s timer
    // and, in a server process, the event loop with it.
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not strand an uncollectable entry when the public handleFor is called directly on a closed cache", () => {
    // `storeFor`/`observe`/`snapshotFor` all guard `closed` before minting
    // anything, but `handleFor` is public and a caller can reach it directly
    // (every test in this file does). Two calls for a NEW key after close
    // must not return the same instance — same instance would mean the first
    // call got inserted into `byKey`, where nothing can ever observe it
    // (`observe` no-ops when closed) or janitor it (armed only pre-close) —
    // i.e. a cache entry that lives forever.
    const { cache } = setup();
    cache.close();
    const a = cache.handleFor(spec("new-key"));
    const b = cache.handleFor(spec("new-key"));
    expect(a).not.toBe(b);
    expect(vi.getTimerCount()).toBe(0);
  });
});
