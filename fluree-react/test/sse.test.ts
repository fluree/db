/**
 * The SSE source. `EventSource` cannot carry an `Authorization` header, so
 * this package parses `text/event-stream` off a fetch body itself — which
 * means the frame parser, the reconnect loop, and the debounced re-resolve
 * of the watched-ledger URL are all ours to get right.
 *
 * Everything here runs against a mock `fetch` returning a pushable stream.
 * No network is touched.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SseSource } from "../src/remote/sse.js";
import type { SseMessage, SseState } from "../src/remote/sse.js";
import { MockServer } from "./helpers.js";

/** Drain the microtask queue so stream reads and awaited fetches settle. */
const flush = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};

const DEBOUNCE = 5;
const BACKOFF = 10;

function setup(url: string | null = "https://srv/v1/fluree/events?ledger=l") {
  const server = new MockServer();
  const messages: SseMessage[] = [];
  const states: SseState[] = [];
  const current = { url };
  const source = new SseSource({
    url: () => current.url,
    headers: async () => ({ authorization: "Bearer tok" }),
    onMessage: (m) => messages.push(m),
    onState: (s) => states.push(s),
    fetchImpl: server.fetchImpl,
    refreshDebounceMs: DEBOUNCE,
    backoffBaseMs: BACKOFF,
    backoffMaxMs: BACKOFF * 4,
  });
  /** Kick a refresh and settle the connect. */
  const connect = async () => {
    source.refresh();
    await vi.advanceTimersByTimeAsync(DEBOUNCE);
    await flush();
  };
  return { server, messages, states, source, current, connect };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("connecting", () => {
  it("connects after the debounce with the resolved URL and headers", async () => {
    const { server, states, connect } = setup();
    await connect();

    expect(server.eventConnects).toHaveLength(1);
    expect(server.eventConnects[0]!.url).toBe(
      "https://srv/v1/fluree/events?ledger=l",
    );
    expect(server.eventConnects[0]!.headers).toMatchObject({
      accept: "text/event-stream",
      authorization: "Bearer tok",
    });
    expect(states).toEqual(["live"]);
  });

  it("does not connect when there is nothing to watch", async () => {
    const { server, states, connect } = setup(null);
    await connect();
    expect(server.eventConnects).toHaveLength(0);
    expect(states).toEqual([]);
  });

  it("coalesces a burst of refreshes into one connection", async () => {
    const { server, source, connect } = setup();
    await connect();
    // A page mounting twenty components hits refresh once per new ledger.
    for (let i = 0; i < 20; i++) source.refresh();
    await vi.advanceTimersByTimeAsync(DEBOUNCE);
    await flush();
    expect(server.eventConnects).toHaveLength(2);
  });

  it("re-resolves the URL on refresh when the watched set changes", async () => {
    const { server, current, source, connect } = setup();
    await connect();
    current.url = "https://srv/v1/fluree/events?ledger=l&ledger=l2";
    source.refresh();
    await vi.advanceTimersByTimeAsync(DEBOUNCE);
    await flush();
    expect(server.eventConnects.map((c) => c.url)).toEqual([
      "https://srv/v1/fluree/events?ledger=l",
      "https://srv/v1/fluree/events?ledger=l&ledger=l2",
    ]);
  });
});

describe("frame parsing", () => {
  it("parses event/data frames and dispatches on the blank line", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push('event: ns-record\ndata: {"t":1}\n\n');
    await flush();
    expect(messages).toEqual([{ event: "ns-record", data: '{"t":1}' }]);
  });

  it("does not dispatch until the frame is terminated", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push("event: ns-record\ndata: {\"t\":1}\n");
    await flush();
    expect(messages).toEqual([]);
    server.stream.push("\n");
    await flush();
    expect(messages).toHaveLength(1);
  });

  it("reassembles a frame split across chunk boundaries", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push("event: ns-re");
    await flush();
    server.stream.push('cord\ndata: {"t"');
    await flush();
    server.stream.push(":1}\n\n");
    await flush();
    expect(messages).toEqual([{ event: "ns-record", data: '{"t":1}' }]);
  });

  it("joins multi-line data with newlines", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push("data: line one\ndata: line two\n\n");
    await flush();
    expect(messages).toEqual([{ event: "message", data: "line one\nline two" }]);
  });

  it("ignores comment keep-alives and unknown fields", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push(": keep-alive\n\n");
    server.stream.push("retry: 5000\n\n");
    server.stream.push(": ping\ndata: real\n\n");
    await flush();
    expect(messages).toEqual([{ event: "message", data: "real" }]);
  });

  it("handles CRLF line endings and a missing space after the colon", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push("event:ns-record\r\ndata:tight\r\n\r\n");
    await flush();
    expect(messages).toEqual([{ event: "ns-record", data: "tight" }]);
  });

  it("treats a field line with no colon as an empty value", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push("data\n\n");
    await flush();
    expect(messages).toEqual([{ event: "message", data: "" }]);
  });

  it("resets event and id between frames", async () => {
    const { server, messages, connect } = setup();
    await connect();
    server.stream.push("event: a\nid: 1\ndata: first\n\n");
    server.stream.push("data: second\n\n");
    await flush();
    expect(messages).toEqual([
      { event: "a", data: "first", id: "1" },
      { event: "message", data: "second" },
    ]);
  });
});

describe("reconnect", () => {
  it("replays Last-Event-ID after the stream ends", async () => {
    const { server, states, connect } = setup();
    await connect();
    server.stream.frame("ns-record", { t: 1 }, "evt-7");
    await flush();

    server.stream.end();
    await flush();
    expect(states).toEqual(["live", "reconnecting"]);

    await vi.advanceTimersByTimeAsync(BACKOFF * 2);
    await flush();
    expect(server.eventConnects).toHaveLength(2);
    expect(server.eventConnects[1]!.headers["last-event-id"]).toBe("evt-7");
    expect(states).toEqual(["live", "reconnecting", "live"]);
  });

  it("retries with growing backoff when the connect itself fails", async () => {
    const { server, states, connect } = setup();
    server.failEvents = 2;
    await connect();
    expect(states).toEqual(["reconnecting"]);
    expect(server.eventConnects).toHaveLength(1);

    // First retry lands within one base interval (jitter is 50-100%).
    await vi.advanceTimersByTimeAsync(BACKOFF);
    await flush();
    expect(server.eventConnects).toHaveLength(2);

    // Second retry waits up to twice as long: nothing before its floor
    // (jitter cannot schedule it sooner than half of 2x base)...
    await vi.advanceTimersByTimeAsync(BACKOFF - 1);
    await flush();
    expect(server.eventConnects).toHaveLength(2);
    // ...and it has certainly fired by its ceiling.
    await vi.advanceTimersByTimeAsync(BACKOFF * 2);
    await flush();
    expect(server.eventConnects).toHaveLength(3);
    expect(states.at(-1)).toBe("live");
  });

  it("re-resolves auth headers on every reconnect", async () => {
    const server = new MockServer();
    let issued = 0;
    const source = new SseSource({
      url: () => "https://srv/v1/fluree/events?ledger=l",
      headers: async () => ({ authorization: `Bearer tok-${++issued}` }),
      onMessage: () => {},
      onState: () => {},
      fetchImpl: server.fetchImpl,
      refreshDebounceMs: DEBOUNCE,
      backoffBaseMs: BACKOFF,
    });
    source.refresh();
    await vi.advanceTimersByTimeAsync(DEBOUNCE);
    await flush();
    server.stream.end();
    await flush();
    await vi.advanceTimersByTimeAsync(BACKOFF * 2);
    await flush();

    // Token rotation is why `headers` is a supplier and not a fixed object.
    expect(server.eventConnects.map((c) => c.headers.authorization)).toEqual([
      "Bearer tok-1",
      "Bearer tok-2",
    ]);
  });
});

describe("close", () => {
  it("reports closed and never reconnects", async () => {
    const { server, states, source, connect } = setup();
    await connect();
    source.close();
    expect(states.at(-1)).toBe("closed");

    await vi.advanceTimersByTimeAsync(BACKOFF * 10);
    await flush();
    expect(server.eventConnects).toHaveLength(1);
  });

  it("drops a pending refresh and is idempotent", async () => {
    const { server, states, source } = setup();
    source.refresh();
    source.close();
    source.close();
    await vi.advanceTimersByTimeAsync(DEBOUNCE * 10);
    await flush();
    expect(server.eventConnects).toHaveLength(0);
    expect(states).toEqual(["closed"]);
  });

  it("stops delivering messages from a stream that is still open", async () => {
    const { server, messages, source, connect } = setup();
    await connect();
    source.close();
    server.stream.push("data: late\n\n");
    await flush();
    expect(messages).toEqual([]);
  });
});
