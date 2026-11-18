/**
 * Pins Channel's INIT_TIMEOUT_MS (index.ts, 30s): an init round-trip that
 * never gets a reply — a hung `getToken`, a wasm fetch that never settles —
 * must not wedge connect()/playground() (and everything behind it) forever;
 * it rejects typed instead. A later, normal connect is unaffected by an
 * earlier one's timeout.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { connect } from "../src/index.js";
import { installStubWorker, StubWorker } from "./helpers.js";

const INIT_TIMEOUT_MS = 30_000;

let restore: () => void;
beforeEach(() => {
  restore = installStubWorker();
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
  restore();
});

describe("init timeout (F4)", () => {
  it("rejects typed after 30s when the worker never replies, and a normal init is unaffected", async () => {
    // Swallow the init request — the stub never replies to it.
    StubWorker.scriptNext((msg) => msg.op === "init");

    const hung = connect("https://example/v1/fluree", {
      workerUrl: "stub://worker",
      maxMemoryBytes: null,
    });
    const assertion = expect(hung).rejects.toMatchObject({ code: "timeout", status: 504 });

    await vi.advanceTimersByTimeAsync(INIT_TIMEOUT_MS);
    await assertion;

    // connect()'s catch closes the channel on any init failure, timeout
    // included.
    const hungWorker = StubWorker.instances.at(-1)!;
    expect(hungWorker.terminated).toBe(true);

    // A fresh connect — ordinary init, answered right away — is unaffected
    // by the previous one's timeout/timer.
    const peer = await connect("https://example/v1/fluree", {
      workerUrl: "stub://worker",
      maxMemoryBytes: null,
    });
    expect(peer.version).toBe("test");
    peer.close();
  });
});
