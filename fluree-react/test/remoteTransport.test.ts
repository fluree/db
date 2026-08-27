/**
 * Remote mode end-to-end against a mock server: the HTTP request shape, the
 * SSE-triggered advance-cycle, and the client-side change gate that decides
 * `changed` vs `unchanged` (remote mode has no worker-side hash gate, so the
 * diff happens here).
 *
 * Every request is mocked. Nothing here proves the request shape matches a
 * real Fluree server's routes — see the verification-status section of the
 * README for what that does and does not cover.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RemoteTransport } from "../src/remote/remoteTransport.js";
import type { RemoteTransportOptions } from "../src/remote/remoteTransport.js";
import type { SubscriptionSpec } from "../src/core/transport.js";
import { MockServer, recordingSink } from "./helpers.js";

/** Drain the microtask queue. Generous on purpose: a bounded cycle fan-out
 * is several sequential rounds deep, each costing a handful of turns. */
const flush = async () => {
  for (let i = 0; i < 400; i++) await Promise.resolve();
};

const DEBOUNCE = 5;
const LEDGER = "my/ledger";

function setup(opts: Partial<RemoteTransportOptions> = {}) {
  const server = new MockServer();
  const sink = recordingSink();
  const transport = new RemoteTransport({
    url: "https://srv",
    fetchImpl: server.fetchImpl,
    sseRefreshDebounceMs: DEBOUNCE,
    backoffBaseMs: 10,
    ...opts,
  });
  transport.start(sink);
  return { server, sink, transport };
}

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

/** Settle the initial fetch and let the debounced SSE connect land. */
async function settle() {
  await flush();
  await vi.advanceTimersByTimeAsync(DEBOUNCE);
  await flush();
}

const bindings = (...names: string[]) => ({
  head: { vars: ["s"] },
  results: {
    bindings: names.map((name) => ({ s: { type: "literal", value: name } })),
  },
});

/** Announce a new commit watermark on the mock SSE stream. */
async function head(server: MockServer, t: number, ledger = LEDGER) {
  server.stream.frame("ns-record", {
    kind: "ledger",
    resource_id: ledger,
    record: { commit_t: t },
  });
  await flush();
}

beforeEach(() => {
  nextId = 1;
  vi.useFakeTimers();
});
afterEach(() => vi.useRealTimers());

describe("query request shape", () => {
  it("POSTs a SPARQL query with the SPARQL content type and Accept", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec());
    await settle();

    expect(server.queries).toHaveLength(1);
    expect(server.queries[0]).toMatchObject({
      url: "https://srv/v1/fluree/query/my/ledger",
      method: "POST",
      body: "SELECT ?s WHERE { ?s ?p ?o }",
    });
    expect(server.queries[0]!.headers).toMatchObject({
      "content-type": "application/sparql-query",
      accept: "application/sparql-results+json",
    });
  });

  it("POSTs a JSON-LD query as JSON", async () => {
    const { server, transport } = setup();
    transport.subscribe(
      spec({ kind: "jsonld", format: "jsonld", text: '{"select":["?s"]}' }),
    );
    await settle();
    expect(server.queries[0]!.headers).toMatchObject({
      "content-type": "application/json",
      accept: "application/json",
    });
    expect(server.queries[0]!.body).toBe('{"select":["?s"]}');
  });

  it("passes a MIME-looking format straight through as Accept", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec({ format: "text/turtle" }));
    await settle();
    expect(server.queries[0]!.headers.accept).toBe("text/turtle");
  });

  it("sends a bearer token, re-read for every request", async () => {
    let issued = 0;
    const { server, transport } = setup({
      getToken: () => `tok-${++issued}`,
    });
    transport.subscribe(spec());
    await settle();
    await head(server, 2);

    expect(server.queries.map((q) => q.headers.authorization)).toEqual([
      "Bearer tok-1",
      "Bearer tok-3",
    ]);
    // ...including the SSE connect, which is why rotation works.
    expect(server.eventConnects[0]!.headers.authorization).toBe("Bearer tok-2");
  });

  it("keeps slashes and colons literal in the ledger path segment", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec({ ledger: "org/repo:branch" }));
    await settle();
    expect(server.queries[0]!.url).toBe(
      "https://srv/v1/fluree/query/org/repo:branch",
    );
  });

  it("percent-encodes characters that would break the path", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec({ ledger: "my ledger?x#y" }));
    await settle();
    expect(server.queries[0]!.url).toBe(
      "https://srv/v1/fluree/query/my%20ledger%3Fx%23y",
    );
  });

  it("trims trailing slashes from the configured base URL", async () => {
    const { server, transport } = setup({ url: "https://srv///" });
    transport.subscribe(spec());
    await settle();
    expect(server.queries[0]!.url).toBe("https://srv/v1/fluree/query/my/ledger");
  });

  it("returns a non-JSON body as text", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({
      contentType: "text/turtle",
      body: "<a> <b> <c> .",
    });
    transport.subscribe(spec({ format: "text/turtle" }));
    await settle();
    expect(sink.cycles[0]!.changed[0]!.payload).toBe("<a> <b> <c> .");
  });
});

describe("initial delivery", () => {
  it("delivers the first result as a single-entry cycle", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({ body: bindings("ada") });
    const s = spec();
    transport.subscribe(s);
    await settle();

    expect(sink.cycles).toHaveLength(1);
    expect(sink.cycles[0]).toEqual({
      ledger: LEDGER,
      t: undefined,
      changed: [{ subId: s.subId, payload: bindings("ada") }],
      unchanged: [],
      errored: [],
    });
  });

  it("reports the latest known head for a subscription that arrives later", async () => {
    const { server, sink, transport } = setup();
    transport.subscribe(spec());
    await settle();
    await head(server, 9);

    const late = spec();
    transport.subscribe(late);
    await flush();
    expect(sink.cycles.at(-1)!.t).toBe(9);
  });

  it("connects SSE once per newly watched ledger, with sorted params", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec());
    transport.subscribe(spec({ text: "other" }));
    await settle();
    expect(server.eventConnects).toHaveLength(1);

    transport.subscribe(spec({ ledger: "a/nother" }));
    await settle();
    expect(server.eventConnects).toHaveLength(2);
    expect(server.eventConnects[1]!.url).toBe(
      "https://srv/v1/fluree/events?ledger=a%2Fnother&ledger=my%2Fledger",
    );
  });

  it("forwards the SSE connection state", async () => {
    const { sink, transport } = setup();
    transport.subscribe(spec());
    expect(transport.connectionState()).toBe("connecting");
    await settle();
    expect(sink.states).toEqual(["live"]);
    expect(transport.connectionState()).toBe("live");
  });
});

describe("the advance-cycle", () => {
  it("re-queries on a head event and reports a real change", async () => {
    const { server, sink, transport } = setup();
    server.respond = (_c, n) => ({
      body: bindings(...["ada", "grace"].slice(0, n + 1)),
    });
    const s = spec();
    transport.subscribe(s);
    await settle();
    await head(server, 5);

    expect(server.queries).toHaveLength(2);
    expect(sink.cycles).toHaveLength(2);
    expect(sink.cycles[1]).toMatchObject({
      ledger: LEDGER,
      t: 5,
      unchanged: [],
      errored: [],
    });
    expect(sink.cycles[1]!.changed).toEqual([
      { subId: s.subId, payload: bindings("ada", "grace") },
    ]);
  });

  it("reports a byte-identical re-query as unchanged, with no payload", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({ body: bindings("ada") });
    const s = spec();
    transport.subscribe(s);
    await settle();
    await head(server, 5);

    expect(server.queries).toHaveLength(2); // the query really did re-run
    expect(sink.cycles[1]).toEqual({
      ledger: LEDGER,
      t: 5,
      changed: [],
      unchanged: [s.subId],
      errored: [],
    });
  });

  it("preserves row identity across a change so the cache can share it", async () => {
    const { server, sink, transport } = setup();
    server.respond = (_c, n) => ({
      body: bindings("ada", n === 0 ? "grace" : "hopper"),
    });
    transport.subscribe(spec());
    await settle();
    const first = sink.cycles[0]!.changed[0]!.payload as ReturnType<
      typeof bindings
    >;
    await head(server, 5);
    const second = sink.cycles[1]!.changed[0]!.payload as ReturnType<
      typeof bindings
    >;

    expect(second).not.toBe(first);
    expect(second.results.bindings[0]).toBe(first.results.bindings[0]);
    expect(second.results.bindings[1]).not.toBe(first.results.bindings[1]);
  });

  it("ignores a head event that does not move the watermark forward", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec());
    await settle();
    await head(server, 5);
    expect(server.queries).toHaveLength(2);
    await head(server, 5);
    await head(server, 4);
    expect(server.queries).toHaveLength(2);
  });

  it("ignores head events for ledgers nothing is watching", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec());
    await settle();
    await head(server, 5, "some/other");
    expect(server.queries).toHaveLength(1);
  });

  it("ignores malformed and non-ledger SSE frames", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec());
    await settle();
    server.stream.push("event: ns-record\ndata: not json\n\n");
    server.stream.frame("ns-record", { kind: "space", resource_id: LEDGER });
    server.stream.frame("ns-record", { kind: "ledger", resource_id: LEDGER });
    server.stream.frame("something-else", {
      kind: "ledger",
      resource_id: LEDGER,
      record: { commit_t: 9 },
    });
    await flush();
    expect(server.queries).toHaveLength(1);
  });

  it("coalesces head events arriving while a cycle is in flight", async () => {
    const { server, sink, transport } = setup();
    let release: (() => void) | undefined;
    server.respond = (_c, n) => {
      if (n !== 1) return { body: bindings(`v${n}`) };
      return new Promise((resolve) => {
        release = () => resolve({ body: bindings(`v${n}`) });
      });
    };
    transport.subscribe(spec());
    await settle();

    await head(server, 5); // starts the cycle that is now blocked
    await head(server, 6);
    await head(server, 7);
    expect(server.queries).toHaveLength(2); // still just the blocked one

    release!();
    await flush();

    // t=6 and t=7 folded into ONE follow-up cycle at the newest watermark.
    expect(server.queries).toHaveLength(3);
    expect(sink.cycles.map((c) => c.t)).toEqual([undefined, 5, 7]);
  });

  it("does not let a slow initial fetch land on top of a newer cycle result", async () => {
    // Subscribe, then have a commit land before the first fetch comes back.
    // If the late arrival were delivered, the component would jump BACKWARDS
    // to pre-commit data and stay there until the next commit.
    const { server, sink, transport } = setup();
    let release: (() => void) | undefined;
    server.respond = (_c, n) =>
      n === 0
        ? new Promise((resolve) => {
            release = () => resolve({ body: bindings("old") });
          })
        : { body: bindings("new") };
    const s = spec();
    transport.subscribe(s);
    await settle(); // SSE is live; the initial fetch is still in flight

    await head(server, 5);
    expect(sink.cycles).toHaveLength(1);
    expect(sink.cycles[0]).toMatchObject({
      t: 5,
      changed: [{ subId: s.subId, payload: bindings("new") }],
    });

    release!();
    await flush();
    expect(sink.cycles).toHaveLength(1);

    // ...and the stale payload must not have become the diff baseline
    // either, or the next identical result would be reported as a change.
    server.respond = () => ({ body: bindings("new") });
    await head(server, 6);
    expect(sink.cycles.at(-1)).toEqual({
      ledger: LEDGER,
      t: 6,
      changed: [],
      unchanged: [s.subId],
      errored: [],
    });
  });

  it("re-runs every live subscription on the ledger in one batch", async () => {
    const { server, sink, transport } = setup();
    server.respond = (call) => ({ body: bindings(call.body) });
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    transport.subscribe(a);
    transport.subscribe(b);
    await settle();
    server.respond = (call) => ({ body: bindings(`${call.body}!`) });
    await head(server, 5);

    const cycle = sink.cycles.at(-1)!;
    expect(cycle.t).toBe(5);
    expect(cycle.changed.map((c) => c.subId).sort()).toEqual(
      [a.subId, b.subId].sort(),
    );
  });

  it("does not re-run subscriptions on a different ledger", async () => {
    const { server, sink, transport } = setup();
    transport.subscribe(spec());
    transport.subscribe(spec({ ledger: "a/nother" }));
    await settle();
    await head(server, 5, "a/nother");

    expect(server.queries).toHaveLength(3); // 2 initial + 1 re-run
    expect(server.queries[2]!.url).toContain("/query/a/nother");
    expect(sink.cycles.at(-1)!.ledger).toBe("a/nother");
  });
});

describe("time-anchored subscriptions", () => {
  it("queries the anchored ledger path and reports the anchor as t", async () => {
    const { server, sink, transport } = setup();
    transport.subscribe(spec({ at: 12 }));
    await flush();
    expect(server.queries[0]!.url).toBe(
      "https://srv/v1/fluree/query/my/ledger@t:12",
    );
    expect(sink.cycles[0]!.t).toBe(12);
  });

  it("never re-runs and never opens an SSE stream by itself", async () => {
    const { server, transport } = setup();
    transport.subscribe(spec({ at: 12 }));
    await settle();
    // Nothing to watch: a t-anchored query cannot be invalidated.
    expect(server.eventConnects).toHaveLength(0);

    transport.subscribe(spec());
    await settle();
    await head(server, 99);
    // The live subscription re-ran; the anchored one did not.
    expect(server.queries.map((q) => q.url)).toEqual([
      "https://srv/v1/fluree/query/my/ledger@t:12",
      "https://srv/v1/fluree/query/my/ledger",
      "https://srv/v1/fluree/query/my/ledger",
    ]);
  });
});

describe("errors", () => {
  it("surfaces the server's error code, message, and status", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({
      status: 400,
      body: { error: "db/invalid-query", message: "unbound variable ?x" },
    });
    const s = spec();
    transport.subscribe(s);
    await settle();

    expect(sink.cycles[0]!.errored).toEqual([
      {
        subId: s.subId,
        error: {
          code: "db/invalid-query",
          message: "unbound variable ?x",
          status: 400,
        },
      },
    ]);
    expect(sink.cycles[0]!.changed).toEqual([]);
  });

  it("falls back to the status line for a non-JSON error body", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({
      status: 502,
      contentType: "text/html",
      body: "<html>bad gateway</html>",
    });
    transport.subscribe(spec());
    await settle();
    expect(sink.cycles[0]!.errored[0]!.error).toEqual({
      code: "http",
      message: "HTTP 502",
      status: 502,
    });
  });

  it("wraps a thrown network failure", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => {
      throw new TypeError("Failed to fetch");
    };
    transport.subscribe(spec());
    await settle();
    expect(sink.cycles[0]!.errored[0]!.error).toEqual({
      code: "transport",
      message: "TypeError: Failed to fetch",
    });
  });

  it("errors only the failing subscription in a mixed cycle", async () => {
    const { server, sink, transport } = setup();
    server.respond = (call) => ({ body: bindings(call.body) });
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    transport.subscribe(a);
    transport.subscribe(b);
    await settle();

    server.respond = (call) =>
      call.body === "qa"
        ? { status: 500, body: { error: "db/boom", message: "boom" } }
        : { body: bindings("qb-new") };
    await head(server, 5);

    const cycle = sink.cycles.at(-1)!;
    expect(cycle.errored.map((e) => e.subId)).toEqual([a.subId]);
    expect(cycle.changed.map((c) => c.subId)).toEqual([b.subId]);
  });

  it("recovers to unchanged when the query succeeds again with the same result", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({ body: bindings("ada") });
    const s = spec();
    transport.subscribe(s);
    await settle();

    server.respond = () => ({ status: 503, body: "down" });
    await head(server, 5);
    expect(sink.cycles.at(-1)!.errored).toHaveLength(1);

    server.respond = () => ({ body: bindings("ada") });
    await head(server, 6);
    // The diff baseline survived the failure, so the recovery cycle can
    // still say "nothing changed" and cost zero re-renders.
    expect(sink.cycles.at(-1)).toEqual({
      ledger: LEDGER,
      t: 6,
      changed: [],
      unchanged: [s.subId],
      errored: [],
    });
  });

  it("errors every live subscription when the ledger is retracted", async () => {
    const { server, sink, transport } = setup();
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    transport.subscribe(a);
    transport.subscribe(b);
    await settle();
    await head(server, 5);

    server.stream.frame("ns-retracted", {
      kind: "ledger",
      resource_id: LEDGER,
    });
    await flush();

    const cycle = sink.cycles.at(-1)!;
    expect(cycle.errored.map((e) => e.subId).sort()).toEqual(
      [a.subId, b.subId].sort(),
    );
    expect(cycle.errored[0]!.error.code).toBe("ledger-retracted");
  });
});

describe("unsubscribe and close", () => {
  it("drops the subscription from later cycles", async () => {
    const { server, sink, transport } = setup();
    // Every response differs, so a subscription that ran WOULD be reported
    // as changed — an absent id therefore means it was not re-run.
    server.respond = (call, n) => ({ body: bindings(`${call.body}-${n}`) });
    const a = spec({ text: "qa" });
    const b = spec({ text: "qb" });
    transport.subscribe(a);
    transport.subscribe(b);
    await settle();
    transport.unsubscribe(a.subId);
    await head(server, 5);

    expect(sink.cycles.at(-1)!.changed.map((c) => c.subId)).toEqual([b.subId]);
    expect(server.queries).toHaveLength(3); // 2 initial + 1 re-run for b only
  });

  it("stops watching a ledger once its last subscription goes", async () => {
    const { server, transport } = setup();
    const s = spec();
    transport.subscribe(s);
    await settle();
    expect(server.eventConnects).toHaveLength(1);

    transport.unsubscribe(s.subId);
    await settle();
    await head(server, 5);
    // The refresh found nothing to watch, so no new stream was opened and
    // the old one's events are ignored.
    expect(server.eventConnects).toHaveLength(1);
    expect(server.queries).toHaveLength(1);
  });

  it("suppresses the delivery of an in-flight fetch for a dropped subscription", async () => {
    const { server, sink, transport } = setup();
    let release: (() => void) | undefined;
    server.respond = () =>
      new Promise((resolve) => {
        release = () => resolve({ body: bindings("late") });
      });
    const s = spec();
    transport.subscribe(s);
    await flush();
    transport.unsubscribe(s.subId);
    release!();
    await flush();
    expect(sink.cycles).toHaveLength(0);
  });

  it("delivers nothing after close", async () => {
    const { server, sink, transport } = setup();
    transport.subscribe(spec());
    await settle();
    const delivered = sink.cycles.length;
    transport.close();
    await head(server, 5);
    await settle();
    expect(sink.cycles).toHaveLength(delivered);
    expect(sink.states.at(-1)).toBe("closed");
  });

  it("ignores a subscribe that arrives after close", async () => {
    const { server, transport } = setup();
    transport.close();
    transport.subscribe(spec());
    await settle();
    expect(server.queries).toHaveLength(0);
  });
});

describe("cancellation and fan-out", () => {
  it("aborts the in-flight request when a subscription goes away", async () => {
    const { server, sink, transport } = setup();
    let release: (() => void) | undefined;
    server.respond = () =>
      new Promise((resolve) => {
        release = () => resolve({ body: bindings("late") });
      });
    const s = spec();
    transport.subscribe(s);
    await flush();
    expect(server.signals[0]!.aborted).toBe(false);

    transport.unsubscribe(s.subId);
    // Not merely ignored on arrival: actually cancelled, so the server stops
    // computing an answer nobody will read.
    expect(server.signals[0]!.aborted).toBe(true);
    release?.();
    await flush();
    expect(sink.cycles).toHaveLength(0);
  });

  it("aborts a superseded request rather than racing it", async () => {
    const { server, sink, transport } = setup();
    const releases: Array<() => void> = [];
    server.respond = (_c, n) =>
      new Promise((resolve) => {
        releases.push(() => resolve({ body: bindings(`v${n}`) }));
      });
    const s = spec();
    transport.subscribe(s);
    await settle();
    // A commit arrives while the first fetch is still open.
    await head(server, 5);

    expect(server.queries).toHaveLength(2);
    expect(server.signals[0]!.aborted).toBe(true);
    expect(server.signals[1]!.aborted).toBe(false);

    // The abort of the first must not surface as a query error.
    releases[1]?.();
    await flush();
    expect(sink.cycles.at(-1)!.errored).toEqual([]);
    expect(sink.cycles.at(-1)!.changed[0]!.payload).toEqual(bindings("v1"));
  });

  it("aborts everything still in flight on close", async () => {
    const { server, transport } = setup();
    server.respond = () => new Promise(() => {});
    transport.subscribe(spec());
    transport.subscribe(spec({ text: "qb" }));
    await flush();
    transport.close();
    expect(server.signals.map((s) => s!.aborted)).toEqual([true, true]);
  });

  it("bounds how many of a cycle's queries run at once", async () => {
    const server = new MockServer();
    const sink = recordingSink();
    const transport = new RemoteTransport({
      url: "https://srv",
      fetchImpl: server.fetchImpl,
      sseRefreshDebounceMs: DEBOUNCE,
      maxConcurrency: 2,
    });
    transport.start(sink);
    // Ten components on one ledger. Remote mode re-runs every one of them
    // per commit; unbounded, that is ten simultaneous HTTP requests.
    const specs = Array.from({ length: 10 }, (_, i) => spec({ text: `q${i}` }));
    for (const s of specs) transport.subscribe(s);
    await settle();
    server.peakInFlight = 0;

    server.respond = (call) => ({ body: bindings(`${call.body}!`) });
    await head(server, 5);

    expect(server.peakInFlight).toBeLessThanOrEqual(2);
    // ...and every subscription still got its answer in ONE cycle.
    expect(sink.cycles.at(-1)!.changed).toHaveLength(10);
    expect(sink.cycles.at(-1)!.t).toBe(5);
  });

  it("still runs a cycle to completion when one query fails mid-fan-out", async () => {
    const server = new MockServer();
    const sink = recordingSink();
    const transport = new RemoteTransport({
      url: "https://srv",
      fetchImpl: server.fetchImpl,
      sseRefreshDebounceMs: DEBOUNCE,
      maxConcurrency: 2,
    });
    transport.start(sink);
    const specs = Array.from({ length: 5 }, (_, i) => spec({ text: `q${i}` }));
    for (const s of specs) transport.subscribe(s);
    await settle();

    server.respond = (call) =>
      call.body === "q2"
        ? { status: 500, body: { error: "db/boom", message: "boom" } }
        : { body: bindings(`${call.body}!`) };
    await head(server, 5);

    const cycle = sink.cycles.at(-1)!;
    expect(cycle.errored).toHaveLength(1);
    expect(cycle.changed).toHaveLength(4);
  });
});

describe("one-shot queries", () => {
  it("executes without registering a subscription or a stream", async () => {
    const { server, sink, transport } = setup();
    server.respond = () => ({ body: bindings("ada") });
    await expect(
      transport.fetchOnce({
        ledger: LEDGER,
        kind: "sparql",
        text: "ASK {}",
        format: "sparql-json",
      }),
    ).resolves.toEqual(bindings("ada"));
    await settle();
    expect(server.queries).toHaveLength(1);
    expect(server.eventConnects).toHaveLength(0);
    expect(sink.cycles).toHaveLength(0);
  });

  it("rejects with the structured error", async () => {
    const { server, transport } = setup();
    server.respond = () => ({
      status: 404,
      body: { error: "db/no-ledger", message: "not found" },
    });
    await expect(
      transport.fetchOnce({
        ledger: LEDGER,
        kind: "sparql",
        text: "ASK {}",
        format: "sparql-json",
      }),
    ).rejects.toEqual({
      code: "db/no-ledger",
      message: "not found",
      status: 404,
    });
  });
});
