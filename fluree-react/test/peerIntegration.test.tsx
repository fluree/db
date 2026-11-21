/**
 * Peer mode through the real hooks. Same components, same API as remote
 * mode — the point of the whole two-mode design is that switching modes
 * cannot change what a component sees.
 *
 * The claim under test that matters most here: `postMessage` CLONES, so
 * every cycle hands the main thread a brand-new object graph even when
 * nothing changed inside it. Referential stability is therefore entirely
 * reconstructed on this side. If that reconstruction were missing, peer mode
 * would still be "correct" and every memoized row in every list would
 * re-render on every commit — the regression this package exists to prevent,
 * and one that no deep-equality assertion would catch.
 */

import { act, render, screen } from "@testing-library/react";
import { memo } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createClient, FlureeProvider, useConnectionState, useQuery } from "../src/index.js";
import type { PeerCycle } from "../src/peer/peerEngine.js";
import { FakePeerEngine } from "./helpers.js";

const LEDGER = "my/ledger";
const Q = "SELECT ?name WHERE { ?s <name> ?name }";

const flush = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};

type Row = { id: number; name: string };
type Results = { rows: Row[] };

/** A fresh object graph every call — exactly what structured cloning across
 * the worker boundary produces, even for an identical result. */
const results = (...names: string[]): Results => ({
  rows: names.map((name, id) => ({ id, name })),
});

function setup() {
  const engine = new FakePeerEngine();
  const client = createClient({
    peer: { connect: async () => engine, url: "https://srv/v1/fluree" },
  });
  const wrap = (ui: React.ReactNode) => (
    <FlureeProvider client={client}>{ui}</FlureeProvider>
  );
  return { engine, client, wrap };
}

/** Settle the engine connect and any pending registrations. */
async function settle() {
  await act(async () => {
    await flush();
  });
}

async function emit(engine: FakePeerEngine, cycle: Partial<PeerCycle>) {
  await act(async () => {
    engine.emitCycle({
      ledger: LEDGER,
      t: 1,
      changed: [],
      unchanged: [],
      errored: [],
      ...cycle,
    });
    await flush();
  });
}

afterEach(() => vi.restoreAllMocks());

describe("peer mode through the hooks", () => {
  it("renders loading, then the engine's first result", async () => {
    const { engine, wrap } = setup();
    const View = () => {
      const { data, status, t } = useQuery<Results>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:{t ?? "-"}:{data?.rows.map((r) => r.name).join(",") ?? "-"}
        </div>
      );
    };
    render(wrap(<View />));
    expect(screen.getByTestId("v").textContent).toBe("loading:-:-");

    await settle();
    await emit(engine, {
      t: 4,
      changed: [{ subId: engine.subId(0), data: results("ada", "grace") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("ready:4:ada,grace");
  });

  it("keeps memoized rows from re-rendering when a clone arrives unchanged", async () => {
    const { engine, wrap } = setup();
    const rowRenders = new Map<number, number>();
    const RowView = memo(({ row }: { row: Row }) => {
      rowRenders.set(row.id, (rowRenders.get(row.id) ?? 0) + 1);
      return <li>{row.name}</li>;
    });
    const View = () => {
      const { data } = useQuery<Results>(LEDGER, Q);
      return <ul>{data?.rows.map((r) => <RowView key={r.id} row={r} />)}</ul>;
    };
    render(wrap(<View />));
    await settle();

    await emit(engine, {
      t: 1,
      changed: [{ subId: engine.subId(0), data: results("ada", "grace", "alan") }],
    });
    expect([...rowRenders.values()]).toEqual([1, 1, 1]);

    // A commit changes ONE row. The worker's hash gate said "changed", so a
    // whole freshly-cloned graph crosses the boundary — every row object is
    // new. Only the row whose data actually moved may re-render.
    await emit(engine, {
      t: 2,
      changed: [{ subId: engine.subId(0), data: results("ada", "hopper", "alan") }],
    });
    expect(rowRenders.get(0)).toBe(1);
    expect(rowRenders.get(1)).toBe(2);
    expect(rowRenders.get(2)).toBe(1);
  });

  it("costs zero renders when the engine reports the query unchanged", async () => {
    const { engine, wrap } = setup();
    let renders = 0;
    const View = () => {
      renders++;
      return <>{useQuery(LEDGER, Q).status}</>;
    };
    render(wrap(<View />));
    await settle();
    await emit(engine, {
      t: 1,
      changed: [{ subId: engine.subId(0), data: results("ada") }],
    });
    const before = renders;

    // The worker-side hash gate decided nothing moved: zero payload, and
    // now zero work on this side too.
    await emit(engine, { t: 2, unchanged: [engine.subId(0)] });
    await emit(engine, { t: 3, unchanged: [engine.subId(0)] });
    expect(renders).toBe(before);
  });

  it("advances siblings in lock-step from one engine cycle", async () => {
    const { engine, wrap } = setup();
    const log = { a: [] as unknown[], b: [] as unknown[] };
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
    await settle();

    await emit(engine, {
      t: 5,
      changed: [
        { subId: engine.subId(0), data: results("a5") },
        { subId: engine.subId(1), data: results("b5") },
      ],
    });
    expect(log.a).toEqual([undefined, 5]);
    expect(log.b).toEqual([undefined, 5]);
  });
});

describe("engine crash recovery, as the UI sees it", () => {
  it("keeps rendering data through a recycle and picks updates back up", async () => {
    const { engine, wrap } = setup();
    const View = () => {
      const { data, status } = useQuery<Results>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:{data?.rows[0]?.name ?? "-"}:{useConnectionState()}
        </div>
      );
    };
    render(wrap(<View />));
    await settle();
    await emit(engine, {
      t: 1,
      changed: [{ subId: engine.subId(0), data: results("ada") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("ready:ada:live");

    await act(async () => {
      engine.emitState("recycling");
      await flush();
    });
    // The data on screen survives the crash; only the connection says so.
    expect(screen.getByTestId("v").textContent).toBe("ready:ada:reconnecting");

    await act(async () => {
      engine.emitState("ready");
      await flush();
    });
    expect(screen.getByTestId("v").textContent).toBe("ready:ada:live");

    // The subscription was re-registered against the fresh engine, so
    // updates flow again under its new id.
    await emit(engine, {
      t: 9,
      changed: [{ subId: engine.subId(1), data: results("grace") }],
    });
    expect(screen.getByTestId("v").textContent).toBe("ready:grace:live");
  });

  it("says so loudly when the engine is not coming back", async () => {
    const { engine, wrap } = setup();
    const View = () => {
      const { data, status, error } = useQuery<Results>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:{data?.rows[0]?.name ?? "-"}:{error?.code ?? "-"}
        </div>
      );
    };
    render(wrap(<View />));
    await settle();
    await emit(engine, {
      t: 1,
      changed: [{ subId: engine.subId(0), data: results("ada") }],
    });

    await act(async () => {
      engine.emitState("terminal");
      await flush();
    });
    // Keep-last-good still applies — but the component can now tell the user
    // this data has stopped updating, instead of showing it forever.
    expect(screen.getByTestId("v").textContent).toBe("error:ada:engine_unavailable");
  });
});

describe("mode differences are visible, not silent", () => {
  it("reports an error for a time-anchored query", async () => {
    const { wrap } = setup();
    const View = () => {
      const { status, error } = useQuery(LEDGER, Q, { at: 3 });
      return <div data-testid="v">{status}:{error?.code ?? "-"}</div>;
    };
    render(wrap(<View />));
    await settle();
    expect(screen.getByTestId("v").textContent).toBe("error:unsupported");
  });
});
