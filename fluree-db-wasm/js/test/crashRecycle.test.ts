/**
 * Pins the crash-recycle ladder in `Channel` (index.ts): a FATAL reply
 * recycles the worker and schedules a backed-off respawn; the respawn
 * re-initializes the fresh worker in the background, and ONLY a non-fatal
 * re-init failure re-enters the ladder
 * (`if (!res.error.fatal) this.recycle(res.error);`). A FATAL re-init reply
 * is already recycled synchronously by the `onmessage` fatal check a few
 * lines up (`if (!msg.ok && msg.error.fatal) this.recycle(msg.error);`) —
 * re-entering it again from the `.then` would double-fire the ladder for a
 * single crash: one extra "recycling" transition and one extra unit of
 * respawn budget burned that a real second crash never happened to earn.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { connect } from "../src/index.js";
import type { EngineState } from "../src/index.js";
import { installStubWorker, StubWorker } from "./helpers.js";

let restore: () => void;
beforeEach(() => {
  restore = installStubWorker();
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
  restore();
});

/** Drain the real microtask queue on top of the fake-timer clock — the
 * respawn ladder chains a queued worker reply -> onmessage -> a `.then` a
 * couple of microtask hops deep. */
async function flush(n = 50) {
  for (let i = 0; i < n; i++) await Promise.resolve();
}

describe("crash-recycle ladder (F1)", () => {
  it("a fatal re-init reply causes exactly one further respawn, not two", async () => {
    const peer = await connect("https://example/v1/fluree", {
      workerUrl: "stub://worker",
      maxMemoryBytes: null,
    });
    const first = StubWorker.instances.at(-1)!;

    const states: EngineState[] = [];
    peer.onEngineState((s) => states.push(s));

    // Crash #1: any in-flight call answered with a fatal error recycles the
    // worker. `ledgerInfo` isn't auto-answered by the stub, so it stays
    // pending until we reply to it explicitly.
    const crashed = peer.ledger("demo").catch(() => {});
    first.replyToLatest("ledgerInfo", {
      ok: false,
      error: { code: "engine_crashed", status: 500, message: "boom", fatal: true },
    });
    await crashed;
    await flush();

    expect(states).toEqual(["recycling"]);
    expect(first.terminated).toBe(true);
    expect(StubWorker.instances).toHaveLength(1);

    // Script the RESPAWNED worker's re-init to fail FATAL — the case the
    // `.then` guard exists for.
    StubWorker.scriptNext((msg, worker) => {
      if (msg.op === "init") {
        worker.reply(msg.id, {
          ok: false,
          error: {
            code: "engine_crashed",
            status: 500,
            message: "second boot poisoned",
            fatal: true,
          },
        });
        return true;
      }
      return false;
    });

    // RESPAWN_BACKOFF_MS * 2**0 = 250ms.
    await flush();
    await vi.advanceTimersByTimeAsync(250);
    await flush();

    expect(StubWorker.instances).toHaveLength(2);
    expect(StubWorker.instances[1]!.terminated).toBe(true);
    // Exactly ONE further "recycling" for this one crash — not two.
    expect(states).toEqual(["recycling", "recycling"]);

    // The next respawn (250 * 2**1 = 500ms later) is unscripted, so it gets
    // the default ok auto-reply and should bring the engine back to "ready"
    // exactly once.
    await vi.advanceTimersByTimeAsync(500);
    await flush();

    expect(states).toEqual(["recycling", "recycling", "ready"]);
    expect(StubWorker.instances).toHaveLength(3);
    expect(StubWorker.instances[2]!.terminated).toBe(false);

    // The recovered worker is not replaced afterward.
    await vi.advanceTimersByTimeAsync(10_000);
    await flush();
    expect(StubWorker.instances).toHaveLength(3);
    expect(states).toEqual(["recycling", "recycling", "ready"]);

    peer.close();
  });
});
