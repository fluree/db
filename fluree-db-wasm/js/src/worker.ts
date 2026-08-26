/**
 * `@fluree/db-wasm` — engine worker.
 *
 * Dedicated worker hosting exactly one wasm instance. It instantiates the
 * module on the first `init` request (streaming compile of the `.wasm`), then
 * services requests from `index.ts`. Each request is handled as an
 * independent promise, so several queries can be in flight; the engine
 * interleaves them on this worker's event loop (single-threaded — CPU-bound
 * work does not overlap, but a query awaiting nothing still yields between
 * operator batches).
 *
 * Crash model (F4): a Rust panic or allocator failure traps the wasm
 * instance — it is poisoned, not recoverable. This worker survives the trap
 * (the exception surfaces through the rejected call), marks itself poisoned,
 * and answers everything with a `fatal` error; the main-thread proxy recycles
 * the whole worker when it sees `fatal`.
 *
 * Query results come back from wasm as `Uint8Array` JSON bytes already
 * copied out of linear memory (wasm-bindgen's `Vec<u8>` return), so their
 * buffer is safe to transfer to the main thread.
 */

import init, { Playground, version } from "../pkg/fluree_db_wasm.js";
import type { ErrorShape, QueryTarget, Request, Response } from "./protocol.js";

let engine: Playground | null = null;
let poisoned: ErrorShape | null = null;
const decoder = new TextDecoder();

function fatalShape(err: unknown): ErrorShape {
  const message = String((err as { message?: unknown })?.message ?? err);
  const oom = /memory|allocat|oom/i.test(message);
  return {
    code: oom ? "out_of_memory" : "engine_crashed",
    status: oom ? 507 : 500,
    message: oom
      ? `engine out of memory (wasm trap): ${message}`
      : `engine crashed (wasm trap): ${message}`,
    fatal: true,
  };
}

function toErrorShape(err: unknown): ErrorShape {
  if (err && typeof err === "object") {
    const e = err as { code?: unknown; status?: unknown; message?: unknown; name?: unknown };
    if (typeof e.code === "string") {
      return {
        code: e.code,
        status: typeof e.status === "number" ? e.status : 500,
        message: typeof e.message === "string" ? e.message : String(err),
      };
    }
    // No `code` ⇒ the error did not come from the binding's typed path. A
    // RuntimeError is a wasm trap (Rust panic / OOM abort — the panic hook
    // already logged the details); a RangeError here is memory.grow failing.
    if (e.name === "RuntimeError" || e.name === "RangeError") {
      poisoned = fatalShape(err);
      return poisoned;
    }
    if (typeof e.message === "string") return { code: "internal", status: 500, message: e.message };
  }
  return { code: "internal", status: 500, message: String(err) };
}

function reply(res: Response, transfer?: Transferable[]): void {
  if (transfer) {
    self.postMessage(res, transfer);
  } else {
    self.postMessage(res);
  }
}

function requireEngine(): Playground {
  if (!engine) {
    const e = new Error("engine not initialized") as Error & { code: string; status: number };
    e.code = "not_initialized";
    e.status = 500;
    throw e;
  }
  return engine;
}

/** Resolve a query target to a snapshot handle plus whether we own it. */
async function resolveTarget(target: QueryTarget): Promise<{ handle: number; owned: boolean }> {
  if ("snapshot" in target) return { handle: target.snapshot, owned: false };
  const info = JSON.parse(await requireEngine().snapshot(target.ledger)) as { handle: number };
  return { handle: info.handle, owned: true };
}

async function handle(req: Request): Promise<void> {
  const { id } = req;
  if (poisoned) {
    reply({ id, ok: false, error: poisoned });
    return;
  }
  try {
    switch (req.op) {
      case "init": {
        if (req.mode !== "playground") {
          reply({
            id,
            ok: false,
            error: {
              code: "unsupported",
              status: 501,
              message: `engine mode "${String(req.mode)}" is not available in this build`,
            },
          });
          return;
        }
        if (!engine) {
          await init(req.wasmUrl === undefined ? undefined : { module_or_path: req.wasmUrl });
          engine = new Playground(req.maxMemoryBytes);
        }
        reply({
          id,
          ok: true,
          result: { version: version(), mode: "playground", maxMemoryBytes: req.maxMemoryBytes },
        });
        return;
      }
      case "createLedger":
        reply({ id, ok: true, result: JSON.parse(await requireEngine().createLedger(req.ledger)) });
        return;
      case "ledgerInfo":
        reply({ id, ok: true, result: JSON.parse(await requireEngine().ledgerInfo(req.ledger)) });
        return;
      case "snapshot":
        reply({ id, ok: true, result: JSON.parse(await requireEngine().snapshot(req.ledger)) });
        return;
      case "release":
        reply({ id, ok: true, result: requireEngine().release(req.snapshot) });
        return;
      case "insert":
        reply({ id, ok: true, result: JSON.parse(await requireEngine().insert(req.ledger, req.body)) });
        return;
      case "upsert":
        reply({ id, ok: true, result: JSON.parse(await requireEngine().upsert(req.ledger, req.body)) });
        return;
      case "update":
        reply({ id, ok: true, result: JSON.parse(await requireEngine().update(req.ledger, req.body)) });
        return;
      case "sparqlUpdate":
        reply({
          id,
          ok: true,
          result: JSON.parse(await requireEngine().sparqlUpdate(req.ledger, req.body)),
        });
        return;
      case "query": {
        const eng = requireEngine();
        const { handle: snap, owned } = await resolveTarget(req.target);
        let bytes: Uint8Array;
        try {
          bytes =
            req.kind === "sparql"
              ? await eng.querySparql(snap, req.text)
              : await eng.queryJsonld(snap, req.text);
        } finally {
          // Release only ephemeral (per-call) snapshots — and not on a trap,
          // where the instance is gone anyway.
          if (owned && !poisoned) eng.release(snap);
        }
        if (req.transport === "clone") {
          reply({ id, ok: true, text: decoder.decode(bytes) });
        } else {
          reply({ id, ok: true, bytes }, [bytes.buffer]);
        }
        return;
      }
      default: {
        const op = String((req as { op?: unknown }).op);
        reply({
          id,
          ok: false,
          error: { code: "invalid_input", status: 400, message: `unknown op "${op}"` },
        });
      }
    }
  } catch (err) {
    reply({ id, ok: false, error: toErrorShape(err) });
  }
}

self.onmessage = (ev: MessageEvent<Request>) => {
  void handle(ev.data);
};
