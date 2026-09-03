/**
 * Pins per-query `timeoutMs` passthrough (F3): `Ledger.query` and
 * `Snapshot.query` (index.ts) put `options.timeoutMs` straight onto the wire
 * `QueryRequest` (protocol.ts) unchanged — an explicit value rides through,
 * and omitting it must stay `undefined` (not default to some other value —
 * the worker keys "no timeout" off `undefined`).
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { playground } from "../src/index.js";
import type { Playground, Ledger } from "../src/index.js";
import { findLatest, installStubWorker, StubWorker } from "./helpers.js";

let restore: () => void;
beforeEach(() => {
  restore = installStubWorker();
});
afterEach(() => restore());

async function setup(): Promise<{ pg: Playground; worker: StubWorker; ledger: Ledger }> {
  const pgPromise = playground({ workerUrl: "stub://worker", maxMemoryBytes: null });
  const worker = StubWorker.instances.at(-1)!;
  const pg = await pgPromise;

  const ledgerPromise = pg.createLedger("demo");
  worker.replyToLatest("createLedger", { ok: true, result: { id: "demo:main", t: 0, indexT: 0 } });
  const ledger = await ledgerPromise;

  return { pg, worker, ledger };
}

describe("QueryRequest.timeoutMs passthrough", () => {
  it("Ledger.query puts timeoutMs on the wire when given, and omits it otherwise", async () => {
    const { worker, ledger } = await setup();

    void ledger.query("SELECT * WHERE { ?s ?p ?o }", { timeoutMs: 5000 }).catch(() => {});
    const withTimeout = findLatest(worker.posted, (m) => m.op === "query");
    expect(withTimeout.timeoutMs).toBe(5000);

    void ledger.query("SELECT * WHERE { ?s ?p ?o }").catch(() => {});
    const withoutTimeout = findLatest(worker.posted, (m) => m.op === "query");
    expect(withoutTimeout.timeoutMs).toBeUndefined();
  });

  it("Snapshot.query puts timeoutMs on the wire when given, and omits it otherwise", async () => {
    const { worker, ledger } = await setup();

    const snapshotPromise = ledger.snapshot();
    worker.replyToLatest("snapshot", { ok: true, result: { handle: 1, id: "demo:main", t: 0 } });
    const snapshot = await snapshotPromise;

    void snapshot.query("SELECT * WHERE { ?s ?p ?o }", { timeoutMs: 2500 }).catch(() => {});
    const withTimeout = findLatest(worker.posted, (m) => m.op === "query");
    expect(withTimeout.timeoutMs).toBe(2500);

    void snapshot.query("SELECT * WHERE { ?s ?p ?o }").catch(() => {});
    const withoutTimeout = findLatest(worker.posted, (m) => m.op === "query");
    expect(withoutTimeout.timeoutMs).toBeUndefined();
  });
});
