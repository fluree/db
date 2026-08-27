/**
 * `LiveClient` is the object the provider injects: transport ownership,
 * spec resolution (query-kind inference and the language-matched format
 * defaults), and the watch/one-shot/connection surface.
 */

import { describe, expect, it, vi } from "vitest";
import { LiveClient } from "../src/core/liveClient.js";
import { resolveSpec, specKey } from "../src/core/types.js";
import { FakeTransport } from "./helpers.js";

function setup(gcTime?: number) {
  const transport = new FakeTransport();
  const client = new LiveClient(
    transport,
    gcTime === undefined ? {} : { gcTime },
  );
  return { transport, client };
}

describe("spec resolution", () => {
  it("infers SPARQL for a string query and JSON-LD for an object query", () => {
    expect(resolveSpec("l", "SELECT * WHERE { ?s ?p ?o }")).toEqual({
      ledger: "l",
      kind: "sparql",
      text: "SELECT * WHERE { ?s ?p ?o }",
      format: "sparql-json",
    });
    expect(resolveSpec("l", { select: { "?s": ["*"] } })).toEqual({
      ledger: "l",
      kind: "jsonld",
      text: '{"select":{"?s":["*"]}}',
      format: "jsonld",
    });
  });

  it("lets opts override the inferred kind and the language-matched format", () => {
    const spec = resolveSpec("l", "SELECT *", {
      kind: "jsonld",
      format: "application/n-triples",
      at: 12,
    });
    expect(spec).toEqual({
      ledger: "l",
      kind: "jsonld",
      text: "SELECT *",
      format: "application/n-triples",
      at: 12,
    });
  });

  it("keys distinct specs distinctly and identical specs identically", () => {
    const key = (l: string, q: string | Record<string, unknown>, o?: object) =>
      specKey(resolveSpec(l, q, o));
    expect(key("l", "q")).toBe(key("l", "q"));
    expect(key("l", "q")).not.toBe(key("l2", "q"));
    expect(key("l", "q")).not.toBe(key("l", "q2"));
    expect(key("l", "q")).not.toBe(key("l", "q", { format: "jsonld" }));
    expect(key("l", "q")).not.toBe(key("l", "q", { at: 1 }));
    expect(key("l", "q", { at: 1 })).not.toBe(key("l", "q", { at: 2 }));
  });

  it("keys an object query by its serialized text, so a fresh literal dedups", () => {
    const { client } = setup();
    expect(client.keyFor("l", { select: ["?s"] })).toBe(
      client.keyFor("l", { select: ["?s"] }),
    );
  });
});

describe("watch", () => {
  it("returns the same store for equal queries and different stores otherwise", () => {
    const { client } = setup();
    const a = client.watch("l", "q");
    const b = client.watch("l", "q");
    const c = client.watch("l", "q2");
    expect(b).toBe(a);
    expect(c).not.toBe(a);
  });

  it("does not touch the transport until something subscribes", () => {
    const { client, transport } = setup();
    client.watch("l", "q");
    expect(transport.subscribes).toHaveLength(0);
    client.watch("l", "q").subscribe(() => {});
    expect(transport.subscribes).toHaveLength(1);
  });

  it("passes the resolved spec, including the time anchor, to the transport", () => {
    const { client, transport } = setup();
    client.watch("l", "q", { at: 42 }).subscribe(() => {});
    expect(transport.subscribes[0]).toMatchObject({
      ledger: "l",
      kind: "sparql",
      text: "q",
      format: "sparql-json",
      at: 42,
    });
  });
});

describe("one-shot query", () => {
  it("goes straight to the transport with the resolved spec", async () => {
    const { client, transport } = setup();
    transport.oneShotResult = { rows: [1] };
    await expect(client.query("l", { select: ["?s"] })).resolves.toEqual({
      rows: [1],
    });
    expect(transport.oneShots).toEqual([
      { ledger: "l", kind: "jsonld", text: '{"select":["?s"]}', format: "jsonld" },
    ]);
    // A one-shot must not create a subscription or a cache entry.
    expect(transport.subscribes).toHaveLength(0);
  });
});

describe("connection state", () => {
  it("delegates the current state and fans out changes to listeners", () => {
    const { client, transport } = setup();
    const seen: string[] = [];
    const off = client.onConnectionChange((s) => seen.push(s));
    expect(client.connectionState()).toBe("connecting");

    transport.setConnection("live");
    expect(client.connectionState()).toBe("live");
    transport.setConnection("reconnecting");
    off();
    transport.setConnection("live");

    expect(seen).toEqual(["live", "reconnecting"]);
  });

  it("survives a listener unsubscribing during the fan-out", () => {
    const { client, transport } = setup();
    const seen: string[] = [];
    const off = client.onConnectionChange(() => off());
    client.onConnectionChange((s) => seen.push(s));
    transport.setConnection("live");
    expect(seen).toEqual(["live"]);
  });
});

describe("ledger head", () => {
  it("reports the latest watermark any cycle carried for a ledger", () => {
    const { client, transport } = setup();
    client.watch("l", "q").subscribe(() => {});
    expect(client.ledgerHead("l")).toBeUndefined();
    transport.emit({
      ledger: "l",
      t: 11,
      changed: [{ subId: transport.subId(0), payload: {} }],
      unchanged: [],
      errored: [],
    });
    expect(client.ledgerHead("l")).toBe(11);
  });
});

describe("close", () => {
  it("closes the transport, drops listeners, and is idempotent", () => {
    const { client, transport } = setup();
    const seen: string[] = [];
    client.onConnectionChange((s) => seen.push(s));
    client.close();
    client.close();

    expect(transport.closed).toBe(true);
    transport.setConnection("live");
    expect(seen).toEqual([]);
  });

  it("releases GC timers so a closed client cannot keep the process alive", () => {
    vi.useFakeTimers();
    try {
      const { client } = setup(50_000);
      client.watch("l", "q");
      client.close();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
