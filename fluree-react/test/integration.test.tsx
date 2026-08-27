/**
 * The whole package composed the way an application uses it: `createClient`
 * with a server URL, a provider, and components calling `useQuery` — driven
 * by a mock Fluree server (HTTP query responses + a pushable SSE stream).
 *
 * This is the only suite that exercises `createClient` and the public
 * barrel, and the only one where a real component re-renders because a
 * commit was announced over SSE. It is still entirely mocked: no Fluree
 * server, no wasm peer, no network.
 */

import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as pkg from "../src/index.js";
import { createClient, FlureeProvider, useQuery } from "../src/index.js";
import { MockServer } from "./helpers.js";

const DEBOUNCE = 5;
const LEDGER = "my/ledger";
const Q = "SELECT ?name WHERE { ?s <name> ?name }";

const results = (...names: string[]) => ({
  head: { vars: ["name"] },
  results: {
    bindings: names.map((name) => ({ name: { type: "literal", value: name } })),
  },
});

type Results = ReturnType<typeof results>;

function setup() {
  const server = new MockServer();
  server.respond = () => ({ body: results("ada") });
  const client = createClient({
    url: "https://srv",
    fetchImpl: server.fetchImpl,
    sseRefreshDebounceMs: DEBOUNCE,
    backoffBaseMs: 10,
    gcTime: 5_000,
  });
  return { server, client };
}

/** Let the debounced SSE connect, the HTTP fetches, and React all settle. */
async function settle() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(DEBOUNCE);
    for (let i = 0; i < 20; i++) await Promise.resolve();
  });
}

async function commit(server: MockServer, t: number) {
  await act(async () => {
    server.stream.frame("ns-record", {
      kind: "ledger",
      resource_id: LEDGER,
      record: { commit_t: t },
    });
    for (let i = 0; i < 20; i++) await Promise.resolve();
  });
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("public API surface", () => {
  it("exports the client, the hooks, and the transport seam", () => {
    for (const name of [
      "createClient",
      "LiveClient",
      "FlureeProvider",
      "useFlureeClient",
      "useQuery",
      "useConnectionState",
      "RemoteTransport",
      "replaceEqualDeep",
    ]) {
      expect(pkg, name).toHaveProperty(name);
    }
  });

  it("accepts a custom transport in place of a server URL", () => {
    const calls: string[] = [];
    const client = pkg.createClient({
      transport: {
        start: () => calls.push("start"),
        subscribe: () => calls.push("subscribe"),
        unsubscribe: () => calls.push("unsubscribe"),
        fetchOnce: async () => ({}),
        connectionState: () => "live" as const,
        close: () => calls.push("close"),
      },
    });
    // This is the seam the wasm browser-peer transport drops into: the
    // client wires the sink at construction and never looks at the mode.
    expect(calls).toEqual(["start"]);
    client.watch(LEDGER, Q).subscribe(() => {});
    expect(calls).toEqual(["start", "subscribe"]);
    expect(client.connectionState()).toBe("live");
  });
});

describe("remote mode, end to end", () => {
  it("renders server results and re-renders when a commit changes them", async () => {
    const { server, client } = setup();
    let renders = 0;
    const List = () => {
      renders++;
      const { data, status } = useQuery<Results>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:
          {data?.results.bindings.map((b) => b.name.value).join(",") ?? "-"}
        </div>
      );
    };
    render(
      <FlureeProvider client={client}>
        <List />
      </FlureeProvider>,
    );
    expect(screen.getByTestId("v").textContent).toBe("loading:-");

    await settle();
    expect(screen.getByTestId("v").textContent).toBe("ready:ada");
    expect(server.queries).toHaveLength(1);

    // A commit lands. The app contains no polling code: the SSE head event
    // is what triggers the re-query.
    server.respond = () => ({ body: results("ada", "grace") });
    await commit(server, 2);
    expect(screen.getByTestId("v").textContent).toBe("ready:ada,grace");
    expect(server.queries).toHaveLength(2);

    // A commit that does not affect this query re-runs it but costs no
    // render, because the results came back identical.
    const before = renders;
    await commit(server, 3);
    expect(server.queries).toHaveLength(3);
    expect(renders).toBe(before);
    expect(client.ledgerHead(LEDGER)).toBe(3);

    client.close();
  });

  it("shares one subscription and one HTTP request across components", async () => {
    const { server, client } = setup();
    const View = () => <>{useQuery(LEDGER, Q).status}</>;
    render(
      <FlureeProvider client={client}>
        <View />
        <View />
        <View />
      </FlureeProvider>,
    );
    await settle();
    expect(server.queries).toHaveLength(1);
    expect(server.eventConnects).toHaveLength(1);
    client.close();
  });

  it("keeps rendering the last good data when the server starts failing", async () => {
    const { server, client } = setup();
    const View = () => {
      const { data, status, error } = useQuery<Results>(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}:{data?.results.bindings[0]?.name.value ?? "-"}:
          {error?.status ?? "-"}
        </div>
      );
    };
    render(
      <FlureeProvider client={client}>
        <View />
      </FlureeProvider>,
    );
    await settle();
    expect(screen.getByTestId("v").textContent).toBe("ready:ada:-");

    server.respond = () => ({ status: 503, body: { error: "db/unavailable" } });
    await commit(server, 2);
    expect(screen.getByTestId("v").textContent).toBe("error:ada:503");

    server.respond = () => ({ body: results("ada") });
    await commit(server, 3);
    expect(screen.getByTestId("v").textContent).toBe("ready:ada:-");
    client.close();
  });

  it("releases the HTTP subscription and the SSE stream when the tree unmounts", async () => {
    const { server, client } = setup();
    const View = () => <>{useQuery(LEDGER, Q).status}</>;
    const { unmount } = render(
      <FlureeProvider client={client}>
        <View />
      </FlureeProvider>,
    );
    await settle();
    expect(server.eventConnects).toHaveLength(1);

    unmount();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
      for (let i = 0; i < 20; i++) await Promise.resolve();
    });
    await settle();

    // Nothing is watched any more, so a commit announced on the old stream
    // triggers no query.
    await commit(server, 9);
    expect(server.queries).toHaveLength(1);
    client.close();
  });

  it("surfaces the connection lifecycle to the UI", async () => {
    const { server, client } = setup();
    const View = () => {
      const { status } = useQuery(LEDGER, Q);
      return (
        <div data-testid="v">
          {status}/{pkg.useConnectionState()}
        </div>
      );
    };
    render(
      <FlureeProvider client={client}>
        <View />
      </FlureeProvider>,
    );
    expect(screen.getByTestId("v").textContent).toBe("loading/connecting");
    await settle();
    expect(screen.getByTestId("v").textContent).toBe("ready/live");

    await act(async () => {
      server.stream.end();
      for (let i = 0; i < 20; i++) await Promise.resolve();
    });
    expect(screen.getByTestId("v").textContent).toBe("ready/reconnecting");

    // The reconnect replays the server's snapshot; the data was never lost.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(20);
      for (let i = 0; i < 20; i++) await Promise.resolve();
    });
    expect(screen.getByTestId("v").textContent).toBe("ready/live");
    // Closing pushes a final "closed" state into the mounted tree.
    act(() => client.close());
    expect(screen.getByTestId("v").textContent).toBe("ready/closed");
  });
});
