/**
 * Pins `Peer.setToken` passthrough (mid-session token refresh): the method
 * puts `{op: "setToken", token}` on the wire verbatim and resolves on the
 * worker's ok reply. The worker-side mode gate (peer-only; playground
 * answers typed `unsupported`) lives in worker.ts, which this harness stubs —
 * the wire contract is what is pinnable here.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect } from "../src/index.js";
import { findLatest, installStubWorker, StubWorker } from "./helpers.js";

let restore: () => void;
beforeEach(() => {
  restore = installStubWorker();
});
afterEach(() => restore());

describe("Peer.setToken passthrough", () => {
  it("sends the setToken op with the token verbatim and resolves on ok", async () => {
    const peerPromise = connect("http://stub.example/v1/fluree", {
      workerUrl: "stub://worker",
      getToken: async () => "initial-token",
    });
    const worker = StubWorker.instances.at(-1)!;
    const peer = await peerPromise;

    const refreshed = peer.setToken("rotated-token");
    const onWire = findLatest(worker.posted, (m) => m.op === "setToken");
    expect(onWire).toBeDefined();
    expect(onWire!.token).toBe("rotated-token");

    worker.replyToLatest("setToken", { ok: true });
    await expect(refreshed).resolves.toBeUndefined();
  });
});
