/**
 * Peer mode against a scripted engine double.
 *
 * The engine does the expensive half (frozen snapshot, re-run, worker-side
 * hash gate, one batched cycle), so what is actually at risk here is the
 * translation and the lifecycle: id mapping, the async-registration races the
 * worker boundary produces, and crash recycles — which discard every
 * subscription and would otherwise freeze every component silently.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PeerTransport } from "../src/peer/peerTransport.js";
import type { PeerCycle } from "../src/peer/peerEngine.js";
import type { SubscriptionSpec } from "../src/core/transport.js";
import { FakePeerEngine, recordingSink } from "./helpers.js";

const LEDGER = "my/ledger";
const flush = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};

let nextId = 1;
function spec(over: Partial<SubscriptionSpec> = {}): SubscriptionSpec {
  return {
    subId: nextId++,
    ledger: LEDGER,
    kind: "sparql",
    text: "SELECT ?s WHERE { ?s ?p ?o }",
    format: "sparql-json",
    ...over,
  };
}

function setup(engine = new FakePeerEngine()) {
  const sink = recordingSink();
  const connect = vi.fn(async () => engine);
  const transport = new PeerTransport({
    connect,
    url: "https://srv/v1/fluree",
  });
  transport.start(sink);
  return { engine, sink, transport, connect };
}

function cycle(over: Partial<PeerCycle> = {}): PeerCycle {
  return { ledger: LEDGER, t: 1, changed: [], unchanged: [], errored: [], ...over };
}

beforeEach(() => {
  nextId = 1;
});
afterEach(() => vi.restoreAllMocks());

describe("connecting", () => {
  it("always starts head tracking, since without it nothing is live", async () => {
    const { connect } = setup();
    await flush();
    expect(connect).toHaveBeenCalledWith(
      "https://srv/v1/fluree",
      expect.objectContaining({ subscribe: [] }),
    );
  });

  it("passes through a narrowed watch list, the token supplier, and the budget", async () => {
    const engine = new FakePeerEngine();
    const connect = vi.fn(async () => engine);
    const getToken = async () => "tok";
    new PeerTransport({
      connect,
      url: "https://srv/v1/fluree",
      watch: ["a/b"],
      getToken,
      maxMemoryBytes: 256,
    }).start(recordingSink());
    await flush();
    expect(connect).toHaveBeenCalledWith("https://srv/v1/fluree", {
      subscribe: ["a/b"],
      getToken,
      maxMemoryBytes: 256,
    });
  });

  it("reports live once the engine is up", async () => {
    const { sink, transport } = setup();
    expect(transport.connectionState()).toBe("connecting");
    await flush();
    expect(sink.states).toEqual(["live"]);
    expect(transport.connectionState()).toBe("live");
  });

  it("registers subscriptions that arrived before the engine was ready", async () => {
    const { engine, transport } = setup();
    transport.subscribe(spec());
    transport.subscribe(spec({ text: "qb" }));
    expect(engine.subscribes).toHaveLength(0);
    await flush();
    expect(engine.subscribes.map((s) => s.query)).toEqual([
      "SELECT ?s WHERE { ?s ?p ?o }",
      "qb",
    ]);
  });

  it("errors every subscription when the engine never connects", async () => {
    const engine = new FakePeerEngine();
    const sink = recordingSink();
    const transport = new PeerTransport({
      connect: async () => {
        throw { code: "unauthorized", status: 401, message: "no token" };
      },
      url: "https://srv/v1/fluree",
    });
    transport.start(sink);
    const a = spec();
    const b = spec({ ledger: "other/ledger" });
    transport.subscribe(a);
    transport.subscribe(b);
    await flush();

    expect(transport.connectionState()).toBe("closed");
    // Grouped per ledger so each delivery is still one coherent cycle.
    expect(sink.cycles).toHaveLength(2);
    expect(sink.cycles[0]!.errored[0]!.error).toEqual({
      code: "unauthorized",
      message: "no token",
      status: 401,
    });
    void engine;
  });
});

describe("registering a query", () => {
  it("sends a SPARQL query as text and a JSON-LD query as an object", async () => {
    const { engine, transport } = setup();
    await flush();
    transport.subscribe(spec());
    transport.subscribe(
      spec({ kind: "jsonld", format: "jsonld", text: '{"select":["?s"]}' }),
    );
    await flush();

    // The engine infers the language from the argument's TYPE, so a JSON-LD
    // query handed over as its text would be parsed as SPARQL.
    expect(engine.subscribes[0]!.query).toBe("SELECT ?s WHERE { ?s ?p ?o }");
    expect(engine.subscribes[1]!.query).toEqual({ select: ["?s"] });
  });

  it("refuses a time-anchored query instead of silently serving current data", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const s = spec({ at: 12 });
    transport.subscribe(s);
    await flush();

    expect(engine.subscribes).toHaveLength(0);
    expect(sink.cycles[0]!.errored).toEqual([
      {
        subId: s.subId,
        error: {
          code: "unsupported",
          message: expect.stringContaining("time-anchored"),
        },
      },
    ]);
  });

  it("refuses a format the engine cannot produce", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const s = spec({ format: "text/turtle" });
    transport.subscribe(s);
    await flush();

    expect(engine.subscribes).toHaveLength(0);
    expect(sink.cycles[0]!.errored[0]!.error.code).toBe("unsupported");
    expect(sink.cycles[0]!.errored[0]!.error.message).toContain("text/turtle");
  });

  it("accepts the language-matched formats", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    transport.subscribe(spec({ format: "sparql-json" }));
    transport.subscribe(spec({ kind: "jsonld", format: "jsonld", text: "{}" }));
    await flush();
    expect(engine.subscribes).toHaveLength(2);
    expect(sink.cycles).toHaveLength(0);
  });

  it("surfaces a registration failure as an error for that subscription", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    engine.failNextSubscribe = { code: "not_found", status: 404, message: "no ledger" };
    const s = spec();
    transport.subscribe(s);
    await flush();
    expect(sink.cycles[0]!.errored).toEqual([
      { subId: s.subId, error: { code: "not_found", message: "no ledger", status: 404 } },
    ]);
  });
});

describe("cycle translation", () => {
  it("maps engine ids to cache ids and preserves the batch", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    const c = spec({ text: "qc" });
    for (const s of [a, b, c]) transport.subscribe(s);
    await flush();

    engine.emitCycle(
      cycle({
        t: 7,
        changed: [{ subId: engine.subId(0), data: { rows: [1] } }],
        unchanged: [engine.subId(1)],
        errored: [{ subId: engine.subId(2), error: { message: "boom" } }],
      }),
    );

    // ONE engine cycle becomes ONE sink cycle: the batch is what lets the
    // cache move every subscription to the same watermark before notifying.
    expect(sink.cycles).toHaveLength(1);
    expect(sink.cycles[0]).toEqual({
      ledger: LEDGER,
      t: 7,
      changed: [{ subId: a.subId, payload: { rows: [1] } }],
      unchanged: [b.subId],
      errored: [{ subId: c.subId, error: { code: "peer", message: "boom" } }],
    });
  });

  it("carries an absent watermark through rather than inventing one", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const s = spec();
    transport.subscribe(s);
    await flush();
    // The engine reports `-1` (no consistent view) as `undefined`.
    engine.emitCycle(
      cycle({ t: undefined, errored: [{ subId: engine.subId(0), error: { message: "x" } }] }),
    );
    expect(sink.cycles[0]!.t).toBeUndefined();
  });

  it("drops entries for engine subscriptions it does not own", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    transport.subscribe(spec());
    await flush();

    // Another consumer sharing this peer has its own subscriptions.
    engine.emitCycle(cycle({ changed: [{ subId: 9999, data: {} }], unchanged: [8888] }));
    expect(sink.cycles).toHaveLength(0);

    engine.emitCycle(
      cycle({
        changed: [
          { subId: 9999, data: { theirs: true } },
          { subId: engine.subId(0), data: { ours: true } },
        ],
      }),
    );
    expect(sink.cycles).toHaveLength(1);
    expect(sink.cycles[0]!.changed).toEqual([
      { subId: 1, payload: { ours: true } },
    ]);
  });

  it("preserves the engine's error code and status when it has them", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    transport.subscribe(spec());
    await flush();
    engine.emitCycle(
      cycle({
        errored: [
          {
            subId: engine.subId(0),
            error: { code: "out_of_memory", status: 507, message: "budget" },
          },
        ],
      }),
    );
    expect(sink.cycles[0]!.errored[0]!.error).toEqual({
      code: "out_of_memory",
      status: 507,
      message: "budget",
    });
  });
});

describe("unsubscribe", () => {
  it("releases the engine subscription and stops delivering", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const s = spec();
    transport.subscribe(s);
    await flush();
    transport.unsubscribe(s.subId);
    await flush();

    expect(engine.unsubscribes).toEqual([engine.subId(0)]);
    engine.emitCycle(cycle({ changed: [{ subId: engine.subId(0), data: {} }] }));
    expect(sink.cycles).toHaveLength(0);
  });

  it("cancels a registration that has not completed yet", async () => {
    const engine = new FakePeerEngine();
    engine.holdRegistrations = true;
    const { sink, transport } = setup(engine);
    await flush();
    const s = spec();
    transport.subscribe(s);
    await flush();
    // The engine accepted the request but has not answered yet.
    expect(engine.subscribes).toHaveLength(1);

    transport.unsubscribe(s.subId);
    engine.releaseAll();
    await flush();

    // The late registration must be released, not left running in the
    // engine with nothing on the main thread listening for it.
    expect(engine.unsubscribes).toEqual([engine.subId(0)]);
    engine.emitCycle(cycle({ changed: [{ subId: engine.subId(0), data: {} }] }));
    expect(sink.cycles).toHaveLength(0);
  });
});

describe("crash recycle", () => {
  it("re-registers every subscription when the engine comes back", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    transport.subscribe(a);
    transport.subscribe(b);
    await flush();
    expect(engine.subscribes).toHaveLength(2);

    engine.emitState("recycling");
    expect(sink.states).toEqual(["live", "reconnecting"]);

    engine.emitState("ready");
    await flush();

    // A recycled engine has NO subscriptions. Without re-registration no
    // cycle would ever arrive again and every component would sit on stale
    // data forever, with no error to show.
    expect(engine.subscribes).toHaveLength(4);
    expect(sink.states).toEqual(["live", "reconnecting", "live"]);

    // Deliveries follow the NEW engine ids.
    engine.emitCycle(cycle({ t: 9, changed: [{ subId: engine.subId(2), data: { v: 1 } }] }));
    expect(sink.cycles.at(-1)!.changed).toEqual([{ subId: a.subId, payload: { v: 1 } }]);

    // ...and the dead ids no longer resolve to anything.
    engine.emitCycle(cycle({ changed: [{ subId: engine.subId(0), data: { stale: true } }] }));
    expect(sink.cycles.at(-1)!.changed).toEqual([{ subId: a.subId, payload: { v: 1 } }]);
  });

  it("errors every subscription when the engine gives up for good", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    transport.subscribe(a);
    transport.subscribe(b);
    await flush();

    engine.emitState("terminal");
    expect(sink.states.at(-1)).toBe("closed");
    // Loud, not silent: the components can render a failure instead of
    // showing data that will never update again.
    const errored = sink.cycles.at(-1)!.errored;
    expect(errored.map((e) => e.subId).sort()).toEqual([a.subId, b.subId].sort());
    expect(errored[0]!.error.code).toBe("engine_unavailable");
  });
});

describe("one-shot queries", () => {
  it("runs through the engine's ledger surface", async () => {
    const { engine, transport } = setup();
    await flush();
    engine.queryResult = { rows: [1] };
    await expect(
      transport.fetchOnce({
        ledger: LEDGER,
        kind: "jsonld",
        text: '{"select":["?s"]}',
        format: "jsonld",
      }),
    ).resolves.toEqual({ rows: [1] });
    expect(engine.queries).toEqual([
      { ledger: LEDGER, query: { select: ["?s"] } },
    ]);
  });

  it("waits for the engine rather than failing when called immediately", async () => {
    const { engine, transport } = setup();
    engine.queryResult = { ok: 1 };
    await expect(
      transport.fetchOnce({
        ledger: LEDGER,
        kind: "sparql",
        text: "ASK {}",
        format: "sparql-json",
      }),
    ).resolves.toEqual({ ok: 1 });
  });

  it("rejects a time-anchored one-shot", async () => {
    const { transport } = setup();
    await flush();
    await expect(
      transport.fetchOnce({
        ledger: LEDGER,
        kind: "sparql",
        text: "ASK {}",
        format: "sparql-json",
        at: 3,
      }),
    ).rejects.toMatchObject({ code: "unsupported" });
  });
});

describe("close", () => {
  it("closes the engine and stops delivering", async () => {
    const { engine, sink, transport } = setup();
    await flush();
    transport.subscribe(spec());
    await flush();
    transport.close();

    expect(engine.closed).toBe(true);
    expect(sink.states.at(-1)).toBe("closed");
    engine.emitCycle(cycle({ changed: [{ subId: engine.subId(0), data: {} }] }));
    expect(sink.cycles).toHaveLength(0);
  });

  it("closes an engine that lands after close was called", async () => {
    const engine = new FakePeerEngine();
    let release: (() => void) | undefined;
    const transport = new PeerTransport({
      connect: () =>
        new Promise((resolve) => {
          release = () => resolve(engine);
        }),
      url: "https://srv/v1/fluree",
    });
    transport.start(recordingSink());
    transport.close();
    release!();
    await flush();
    // Otherwise the worker (and its wasm heap) leaks for the page's lifetime.
    expect(engine.closed).toBe(true);
  });

  it("ignores a subscribe after close", async () => {
    const { engine, transport } = setup();
    await flush();
    transport.close();
    transport.subscribe(spec());
    await flush();
    expect(engine.subscribes).toHaveLength(0);
  });
});
