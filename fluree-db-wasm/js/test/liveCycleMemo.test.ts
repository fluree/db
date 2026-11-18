/**
 * Pins the `toLiveCycle` memo (index.ts): the channel dispatches the SAME
 * `cycleOutcome` event object to every internal listener — the per-sub
 * fan-out (`LiveRegistry`, wired by `subscribe()`) and every `onCycle`
 * listener — and `toLiveCycle` must decode it exactly once per event,
 * handing every listener the identical decoded `LiveCycle`/payload objects
 * rather than re-parsing per listener. Ported from the round-2 prototype
 * (`internal-reviews/round2-shell-memo.test.mjs`, node:test), which proved
 * this is testable with a `globalThis.Worker` stub and zero production
 * changes.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect } from "../src/index.js";
import { installStubWorker, StubWorker } from "./helpers.js";

let restore: () => void;
beforeEach(() => {
  restore = installStubWorker();
});
afterEach(() => restore());

const enc = new TextEncoder();

function cycleEvent(t: number, subId: number) {
  return {
    kind: "cycleOutcome" as const,
    ledger: "demo:main",
    t,
    changed: [{ subId }],
    unchanged: [],
    errored: [],
  };
}

function payloadFor(obj: unknown): ArrayBuffer {
  return enc.encode(JSON.stringify(obj)).buffer;
}

describe("toLiveCycle memo", () => {
  it("decodes each cycle once, shared across both listeners", async () => {
    const peer = await connect("https://example/v1/fluree", {
      workerUrl: "stub://worker",
      maxMemoryBytes: null,
    });
    const stub = StubWorker.instances.at(-1)!;

    // Two independent decoders on the SAME event dispatch: the per-sub
    // fan-out (LiveRegistry, wired by subscribe) and the batch surface
    // (onCycle).
    const subData: unknown[] = [];
    const cycleData: unknown[] = [];
    await peer.subscribe("demo:main", "SELECT * WHERE {}", (u) => subData.push(u.data));
    peer.onCycle((c) => cycleData.push(c.changed[0]?.data));

    // Count payload decodes during ONE event dispatch.
    const realParse = JSON.parse;
    let parses = 0;
    JSON.parse = ((...a: Parameters<typeof JSON.parse>) => {
      parses++;
      return realParse(...a);
    }) as typeof JSON.parse;
    try {
      const body = {
        head: { vars: ["s"] },
        results: { bindings: [{ s: { type: "uri", value: "urn:a" } }] },
      };
      stub.emit({ v: 1, event: cycleEvent(5, 1), payloads: [payloadFor(body)] });
      await Promise.resolve();

      expect(parses).toBe(1);
      expect(subData.at(-1)).toEqual(body);
      expect(cycleData.at(-1)).toEqual(body);
      expect(subData.at(-1)).toBe(cycleData.at(-1)); // same memoized object

      // A DISTINCT event object must NOT reuse the prior memo entry.
      parses = 0;
      const body2 = { head: { vars: ["s"] }, results: { bindings: [] } };
      stub.emit({ v: 1, event: cycleEvent(6, 1), payloads: [payloadFor(body2)] });
      await Promise.resolve();
      expect(parses).toBe(1);
      expect(cycleData.at(-1)).toEqual(body2);
    } finally {
      JSON.parse = realParse;
    }
    peer.close();
  });
});
