/**
 * The React adapter. What is actually at stake here is that a subscription
 * that did not change causes NO work: no snapshot replacement, no
 * notification, no re-render, and no identity churn in the data the
 * component already rendered. These tests drive that through real React
 * (react-dom + jsdom), not through the store API.
 *
 * They also pin the `useSyncExternalStore` contracts. `getSnapshot`
 * returning a fresh object per call is the canonical infinite-render bug
 * with this hook, and it is invisible to any test that only compares values.
 */

import { act, render, screen } from "@testing-library/react";
import { StrictMode, memo, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LiveClient } from "../src/core/liveClient.js";
import { FlureeProvider, useFlureeClient } from "../src/react/context.js";
import { useConnectionState } from "../src/react/useConnectionState.js";
import { useQuery } from "../src/react/useQuery.js";
import { FakeTransport } from "./helpers.js";

const LEDGER = "my/ledger";
const Q = "SELECT ?s WHERE { ?s ?p ?o }";

function setup(gcTime = 30_000) {
  const transport = new FakeTransport();
  const client = new LiveClient(transport, { gcTime });
  const wrap = (ui: React.ReactNode) => (
    <FlureeProvider client={client}>{ui}</FlureeProvider>
  );
  return { transport, client, wrap };
}

type Rows = { rows: Array<{ id: number; name: string }> };

const rows = (...names: string[]): Rows => ({
  rows: names.map((name, id) => ({ id, name })),
});

/** Emit one advance-cycle inside `act` so React flushes it. */
function cycle(
  transport: FakeTransport,
  t: number,
  parts: {
    changed?: Array<{ subId: number; payload: unknown }>;
    unchanged?: number[];
    errored?: Array<{ subId: number; error: { code: string; message: string } }>;
    ledger?: string;
  },
) {
  act(() => {
    transport.emit({
      ledger: parts.ledger ?? LEDGER,
      t,
      changed: parts.changed ?? [],
      unchanged: parts.unchanged ?? [],
      errored: parts.errored ?? [],
    });
  });
}

describe("FlureeProvider", () => {
  it("throws a directive error when a hook is used outside the provider", () => {
    const Bare = () => <>{String(useQuery(LEDGER, Q).status)}</>;
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(() => render(<Bare />)).toThrow(/No FlureeProvider found/);
    } finally {
      spy.mockRestore();
    }
  });

  it("hands every consumer the same client instance", () => {
    const { client, wrap } = setup();
    let seen: LiveClient | undefined;
    const Probe = () => {
      seen = useFlureeClient();
      return null;
    };
    render(wrap(<Probe />));
    expect(seen).toBe(client);
  });
});

describe("useQuery lifecycle", () => {
  it("renders loading, then the first result", () => {
    const { transport, wrap } = setup();
    const View = () => {
      const { data, status, t } = useQuery<Rows>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:{t ?? "-"}:{data?.rows.map((r) => r.name).join(",") ?? "-"}
        </div>
      );
    };
    render(wrap(<View />));
    expect(screen.getByTestId("v").textContent).toBe("loading:-:-");

    cycle(transport, 7, {
      changed: [{ subId: transport.subId(0), payload: rows("ada", "grace") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("ready:7:ada,grace");
  });

  it("opens exactly one subscription for two components sharing a query", () => {
    const { transport, wrap } = setup();
    const View = () => <>{useQuery<Rows>(LEDGER, Q).status}</>;
    render(
      wrap(
        <>
          <View />
          <View />
        </>,
      ),
    );
    expect(transport.subscribes).toHaveLength(1);
  });

  it("dedups an inline object query that is a fresh literal every render", () => {
    const { transport, wrap } = setup();
    const View = () => <>{useQuery(LEDGER, { select: ["?s"] }).status}</>;
    const { rerender } = render(wrap(<View />));
    rerender(wrap(<View />));
    rerender(wrap(<View />));
    expect(transport.subscribes).toHaveLength(1);
  });

  it("switches subscriptions when the query changes", () => {
    const { transport, wrap } = setup();
    const View = ({ q }: { q: string }) => {
      const { data } = useQuery<Rows>(LEDGER, q);
      return <div data-testid="v">{data?.rows[0]?.name ?? "-"}</div>;
    };
    const { rerender } = render(wrap(<View q="qa" />));
    cycle(transport, 1, {
      changed: [{ subId: transport.subId(0), payload: rows("ada") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("ada");

    rerender(wrap(<View q="qb" />));
    expect(transport.subscribes).toHaveLength(2);
    expect(screen.getByTestId("v").textContent).toBe("-"); // new query, loading
    cycle(transport, 2, {
      changed: [{ subId: transport.subId(1), payload: rows("grace") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("grace");
  });

  it("survives a StrictMode double-mount with one live subscription", () => {
    const { transport, wrap } = setup();
    const View = () => {
      const { data } = useQuery<Rows>(LEDGER, Q);
      return <div data-testid="v">{data?.rows[0]?.name ?? "-"}</div>;
    };
    render(<StrictMode>{wrap(<View />)}</StrictMode>);

    // StrictMode mounts, unmounts, and remounts effects. The grace period is
    // what makes that free — no re-subscribe, and no dropped subscription.
    expect(transport.subscribes).toHaveLength(1);
    expect(transport.unsubscribes).toHaveLength(0);
    cycle(transport, 1, {
      changed: [{ subId: transport.subId(0), payload: rows("ada") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("ada");
  });
});

describe("re-render discipline", () => {
  it("does not re-render when the cycle reports this query unchanged", () => {
    const { transport, wrap } = setup();
    let renders = 0;
    const View = () => {
      renders++;
      const { data } = useQuery<Rows>(LEDGER, Q);
      return <div data-testid="v">{data?.rows[0]?.name ?? "-"}</div>;
    };
    render(wrap(<View />));
    cycle(transport, 1, {
      changed: [{ subId: transport.subId(0), payload: rows("ada") }],
    });
    const after = renders;

    cycle(transport, 2, { unchanged: [transport.subId(0)] });
    cycle(transport, 3, { unchanged: [transport.subId(0)] });
    cycle(transport, 4, { unchanged: [transport.subId(0)] });

    expect(renders).toBe(after);
    expect(screen.getByTestId("v").textContent).toBe("ada");
  });

  it("does not re-render when another query on the same ledger changes", () => {
    const { transport, wrap } = setup();
    let aRenders = 0;
    let bRenders = 0;
    const A = () => {
      aRenders++;
      return <>{useQuery(LEDGER, "qa").status}</>;
    };
    const B = () => {
      bRenders++;
      return <>{useQuery(LEDGER, "qb").status}</>;
    };
    render(
      wrap(
        <>
          <A />
          <B />
        </>,
      ),
    );
    cycle(transport, 1, {
      changed: [
        { subId: transport.subId(0), payload: rows("ada") },
        { subId: transport.subId(1), payload: rows("grace") },
      ],
    });
    const [a0, b0] = [aRenders, bRenders];

    cycle(transport, 2, {
      changed: [{ subId: transport.subId(0), payload: rows("hopper") }],
      unchanged: [transport.subId(1)],
    });

    expect(aRenders).toBeGreaterThan(a0);
    expect(bRenders).toBe(b0);
  });

  it("keeps unchanged rows memoized across an advance", () => {
    const { transport, wrap } = setup();
    const rowRenders = new Map<number, number>();
    const Row = memo(({ row }: { row: { id: number; name: string } }) => {
      rowRenders.set(row.id, (rowRenders.get(row.id) ?? 0) + 1);
      return <li>{row.name}</li>;
    });
    const View = () => {
      const { data } = useQuery<Rows>(LEDGER, Q);
      return (
        <ul>
          {data?.rows.map((r) => <Row key={r.id} row={r} />)}
        </ul>
      );
    };
    render(wrap(<View />));
    cycle(transport, 1, {
      changed: [
        { subId: transport.subId(0), payload: rows("ada", "grace", "alan") },
      ],
    });
    expect([...rowRenders.values()]).toEqual([1, 1, 1]);

    cycle(transport, 2, {
      changed: [
        { subId: transport.subId(0), payload: rows("ada", "hopper", "alan") },
      ],
    });

    // Only the row whose data actually changed re-rendered. This is the
    // whole point of structural sharing: without it, `postMessage`-cloned
    // (or re-parsed) payloads give every row a new object identity and
    // React.memo stops working for the entire list.
    expect(rowRenders.get(0)).toBe(1);
    expect(rowRenders.get(1)).toBe(2);
    expect(rowRenders.get(2)).toBe(1);
  });

  it("keeps the snapshot and data identity across unrelated parent re-renders", () => {
    const { transport, wrap } = setup();
    const snapshots: unknown[] = [];
    const datas: unknown[] = [];
    let bump: (n: number) => void = () => {};
    const View = () => {
      const [, setN] = useState(0);
      bump = setN;
      const result = useQuery<Rows>(LEDGER, Q);
      snapshots.push(result);
      datas.push(result.data);
      return <div>{result.status}</div>;
    };
    render(wrap(<View />));
    cycle(transport, 1, {
      changed: [{ subId: transport.subId(0), payload: rows("ada") }],
    });
    const n = snapshots.length;

    // Force re-renders that have nothing to do with the query. If
    // `getSnapshot` minted a fresh object per call, React would either warn
    // and re-render forever or hand the component a new `data` every pass,
    // silently breaking every downstream memo.
    for (let i = 1; i <= 3; i++) act(() => bump(i));

    expect(snapshots.length).toBe(n + 3);
    for (const snap of snapshots.slice(n)) {
      expect(snap).toBe(snapshots[n - 1]);
    }
    for (const data of datas.slice(n)) {
      expect(data).toBe(datas[n - 1]);
    }
  });

  it("returns the identical snapshot object from repeated getSnapshot calls", () => {
    const { client } = setup();
    const store = client.watch(LEDGER, Q);
    expect(store.getSnapshot()).toBe(store.getSnapshot());
    store.subscribe(() => {});
    expect(store.getSnapshot()).toBe(store.getSnapshot());
  });
});

describe("version coherence in the tree", () => {
  // The two-phase invariant itself (every handle swapped before any observer
  // is notified) is pinned at the cache level in queryCache.test.ts, where a
  // violation is directly observable. What React can observe is the
  // consequence: one batched commit per cycle, with every sibling landing on
  // the same watermark in that commit — never a cascade of one render per
  // changed subscription.
  it("advances two siblings in lock-step, one render each per cycle", () => {
    const { transport, wrap } = setup();
    const log = { a: [] as Array<number | undefined>, b: [] as Array<number | undefined> };
    const A = () => {
      log.a.push(useQuery(LEDGER, "qa").t);
      return null;
    };
    const B = () => {
      log.b.push(useQuery(LEDGER, "qb").t);
      return null;
    };
    render(
      wrap(
        <>
          <A />
          <B />
        </>,
      ),
    );
    expect(log.a).toEqual([undefined]);
    expect(log.b).toEqual([undefined]);

    for (const t of [5, 6]) {
      cycle(transport, t, {
        changed: [
          { subId: transport.subId(0), payload: rows(`a${t}`) },
          { subId: transport.subId(1), payload: rows(`b${t}`) },
        ],
      });
    }

    expect(log.a).toEqual([undefined, 5, 6]);
    expect(log.b).toEqual([undefined, 5, 6]);
  });

  it("does not re-render a sibling whose results were unchanged in the cycle", () => {
    const { transport, wrap } = setup();
    const log = { a: [] as Array<number | undefined>, b: [] as Array<number | undefined> };
    const A = () => {
      log.a.push(useQuery(LEDGER, "qa").t);
      return null;
    };
    const B = () => {
      log.b.push(useQuery(LEDGER, "qb").t);
      return null;
    };
    render(
      wrap(
        <>
          <A />
          <B />
        </>,
      ),
    );
    cycle(transport, 5, {
      changed: [
        { subId: transport.subId(0), payload: rows("a5") },
        { subId: transport.subId(1), payload: rows("b5") },
      ],
    });
    cycle(transport, 6, {
      changed: [{ subId: transport.subId(0), payload: rows("a6") }],
      unchanged: [transport.subId(1)],
    });

    expect(log.a).toEqual([undefined, 5, 6]);
    // B re-ran at t=6 and matched, so it neither re-rendered nor moved its
    // watermark — its data is still exactly what t=6 would return.
    expect(log.b).toEqual([undefined, 5]);
  });
});

describe("errors", () => {
  it("keeps the last good data on screen and reports the error", () => {
    const { transport, wrap } = setup();
    const View = () => {
      const { data, status, error } = useQuery<Rows>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:{data?.rows[0]?.name ?? "-"}:{error?.message ?? "-"}
        </div>
      );
    };
    render(wrap(<View />));
    cycle(transport, 1, {
      changed: [{ subId: transport.subId(0), payload: rows("ada") }],
    });
    cycle(transport, 2, {
      errored: [
        { subId: transport.subId(0), error: { code: "http", message: "boom" } },
      ],
    });
    expect(screen.getByTestId("v").textContent).toBe("error:ada:boom");

    cycle(transport, 3, { unchanged: [transport.subId(0)] });
    expect(screen.getByTestId("v").textContent).toBe("ready:ada:-");
  });
});

describe("unmount and GC", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("releases the subscription only after the grace period", () => {
    const { transport, wrap } = setup(5_000);
    const View = () => <>{useQuery(LEDGER, Q).status}</>;
    const { unmount } = render(wrap(<View />));
    expect(transport.subscribes).toHaveLength(1);

    unmount();
    act(() => {
      vi.advanceTimersByTime(4_999);
    });
    expect(transport.unsubscribes).toHaveLength(0);

    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(transport.unsubscribes).toEqual([transport.subId(0)]);
  });

  it("remounts inside the grace period with the data already there", () => {
    const { transport, wrap } = setup(5_000);
    const View = () => {
      const { data } = useQuery<Rows>(LEDGER, Q);
      return <div data-testid="v">{data?.rows[0]?.name ?? "-"}</div>;
    };
    const first = render(wrap(<View />));
    cycle(transport, 1, {
      changed: [{ subId: transport.subId(0), payload: rows("ada") }],
    });
    first.unmount();
    act(() => {
      vi.advanceTimersByTime(1_000);
    });

    render(wrap(<View />));
    // No loading flash, no refetch: the live subscription was still open.
    expect(screen.getByTestId("v").textContent).toBe("ada");
    expect(transport.subscribes).toHaveLength(1);
  });
});

describe("useConnectionState", () => {
  it("tracks the transport's connection lifecycle", () => {
    const { transport, wrap } = setup();
    const View = () => <div data-testid="c">{useConnectionState()}</div>;
    render(wrap(<View />));
    expect(screen.getByTestId("c").textContent).toBe("connecting");

    act(() => transport.setConnection("live"));
    expect(screen.getByTestId("c").textContent).toBe("live");
    act(() => transport.setConnection("reconnecting"));
    expect(screen.getByTestId("c").textContent).toBe("reconnecting");
  });

  it("does not re-render a query component when the connection changes", () => {
    const { transport, wrap } = setup();
    let renders = 0;
    const View = () => {
      renders++;
      return <>{useQuery(LEDGER, Q).status}</>;
    };
    const Conn = () => <>{useConnectionState()}</>;
    render(
      wrap(
        <>
          <View />
          <Conn />
        </>,
      ),
    );
    const before = renders;
    act(() => transport.setConnection("live"));
    expect(renders).toBe(before);
  });
});
