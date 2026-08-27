/**
 * Server rendering. The live machinery (fetch subscriptions, SSE, and in
 * peer mode a worker) is browser-only, so a server render must produce the
 * loading state rather than throwing or hanging — that is the whole job of
 * the `getServerSnapshot` argument to `useSyncExternalStore`.
 *
 * This is remote mode's SSR/Next.js story from the design, executed.
 */

import { renderToString } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient, FlureeProvider, useConnectionState, useQuery } from "../src/index.js";
import { LiveClient } from "../src/core/liveClient.js";
import { MockServer } from "./helpers.js";
import { FakeTransport } from "./helpers.js";

const LEDGER = "my/ledger";
const Q = "SELECT ?s WHERE { ?s ?p ?o }";

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("server rendering", () => {
  it("renders the loading snapshot without touching the transport", () => {
    const transport = new FakeTransport();
    const client = new LiveClient(transport);
    const View = () => {
      const { data, status, error, t } = useQuery(LEDGER, Q);
      // One interpolation: React inserts `<!-- -->` separators between
      // adjacent text children, which would only obscure the assertion.
      return <p>{`${status}/${data}/${error}/${t}`}</p>;
    };
    const html = renderToString(
      <FlureeProvider client={client}>
        <View />
      </FlureeProvider>,
    );

    expect(html).toBe("<p>loading/undefined/undefined/undefined</p>");
    // No subscription is opened: `useSyncExternalStore` never calls
    // `subscribe` during a server render, and nothing else may either.
    expect(transport.subscribes).toHaveLength(0);
  });

  it("reports the connection as connecting on the server", () => {
    const client = new LiveClient(new FakeTransport());
    const View = () => <p>{useConnectionState()}</p>;
    expect(
      renderToString(
        <FlureeProvider client={client}>
          <View />
        </FlureeProvider>,
      ),
    ).toBe("<p>connecting</p>");
  });

  it("issues no HTTP request when a remote client is server-rendered", async () => {
    const server = new MockServer();
    const client = createClient({
      url: "https://srv",
      fetchImpl: server.fetchImpl,
      sseRefreshDebounceMs: 5,
    });
    const View = () => <p>{useQuery(LEDGER, Q).status}</p>;
    renderToString(
      <FlureeProvider client={client}>
        <View />
      </FlureeProvider>,
    );
    await vi.advanceTimersByTimeAsync(50);

    expect(server.queries).toHaveLength(0);
    expect(server.eventConnects).toHaveLength(0);
    client.close();
  });

  it("collects the cache entry a server render leaves behind", async () => {
    const transport = new FakeTransport();
    const client = new LiveClient(transport, { gcTime: 1_000 });
    const View = () => <p>{useQuery(LEDGER, Q).status}</p>;
    renderToString(
      <FlureeProvider client={client}>
        <View />
      </FlureeProvider>,
    );

    // A render creates the handle (and its janitor timer) even when nothing
    // ever observes it — on a server that is once per request, so it has to
    // clean itself up.
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(vi.getTimerCount()).toBe(0);
    expect(transport.subscribes).toHaveLength(0);
    expect(transport.unsubscribes).toHaveLength(0);
  });
});
