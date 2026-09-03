/**
 * Pins connect()'s failure cleanup (index.ts): a NON-fatal init failure (bad
 * token, unsupported mode) leaves the spawned worker — and, in peer mode,
 * its already-instantiated wasm — alive, because `onmessage`'s fatal check
 * only recycles fatal replies. connect() never hands the caller a `Channel`
 * to close when init itself rejects, so it must terminate the worker itself
 * (`catch (e) { channel.close(); throw e; }`) or every failed/retried
 * connect leaks a worker.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect } from "../src/index.js";
import { installStubWorker, StubWorker } from "./helpers.js";

let restore: () => void;
beforeEach(() => {
  restore = installStubWorker();
});
afterEach(() => restore());

describe("connect() non-fatal init failure (F2)", () => {
  it("rejects typed and terminates the worker", async () => {
    StubWorker.scriptNext((msg, worker) => {
      if (msg.op === "init") {
        worker.reply(msg.id, {
          ok: false,
          error: { code: "unauthorized", status: 401, message: "bad token", fatal: false },
        });
        return true;
      }
      return false;
    });

    await expect(
      connect("https://example/v1/fluree", { workerUrl: "stub://worker", maxMemoryBytes: null }),
    ).rejects.toMatchObject({ name: "FlureeError", code: "unauthorized", status: 401, fatal: false });

    const worker = StubWorker.instances.at(-1)!;
    expect(worker.terminated).toBe(true);
  });
});
