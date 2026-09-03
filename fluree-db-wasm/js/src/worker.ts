/**
 * `@fluree/db-wasm` — engine worker.
 *
 * Dedicated worker hosting exactly one wasm instance in one of two modes:
 * the in-memory read-write **playground**, or the read-only remote **peer**
 * (`connectPeer` → `fluree-db-browser`'s driver, CID-verified fetches,
 * IndexedDB cache, SSE head tracking). It instantiates the module on the
 * first `init` request (streaming compile of the `.wasm`), then services
 * requests from `index.ts`. Each request is handled as an independent
 * promise, so several queries can be in flight; the engine interleaves them
 * on this worker's event loop. Requests that arrive while an init is still
 * compiling wait for it instead of failing `not_initialized`; if init
 * failed, they get the init's actual error, not a generic one.
 *
 * Credentials (peer mode): `init` deliberately carries NO token — the proxy
 * replays init verbatim on a crash recycle, and a replayed message must
 * never be a credential replay. Instead the worker emits a `tokenRequest`
 * EVENT and waits for the main thread's `tokenResponse` before connecting;
 * a recycle therefore re-asks, and the main-thread `getToken` callback is
 * the single source of credentials.
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

import init, { connectPeer, Peer, Playground, version } from "../pkg/fluree_db_wasm.js";
import type {
  EngineMode,
  ErrorShape,
  EventBody,
  QueryTarget,
  Request,
  Response,
} from "./protocol.js";

let engine: Playground | Peer | null = null;
let mode: EngineMode | null = null;
let poisoned: ErrorShape | null = null;
/** In-flight init, so requests racing the wasm compile wait instead of
 * failing; resolved to `null` on success, the failure shape on error. */
let initing: Promise<ErrorShape | null> | null = null;
/** Why init failed, preserved so later requests report the real cause. */
let initError: ErrorShape | null = null;
const decoder = new TextDecoder();

/** Pending token requests, keyed by the event's requestId. */
const tokenWaiters = new Map<
  number,
  (answer: { token?: string; error?: string }) => void
>();
let nextTokenRequestId = 1;

function fatalShape(err: unknown): ErrorShape {
  const message = String((err as { message?: unknown })?.message ?? err);
  // "memory access out of bounds" is the wasm STACK-overflow signature (see
  // build.rs), not heap exhaustion — classifying it as out_of_memory would
  // send users tuning maxMemoryBytes for a stack bug.
  const oom = /memory|allocat|oom/i.test(message) && !/out of bounds/i.test(message);
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

/** Push an unsolicited event to the main thread (see protocol.ts). */
function emit(event: EventBody): void {
  self.postMessage({ v: 1, event });
}

/** Bridge the engine's live-query cycle outcomes (A4): ONE message per
 * advance-cycle; changed payloads posted as TRANSFERRED buffers aligned
 * with `event.changed` (zero-copy handoff, per the transport tiering). */
function wireCycleOutcomes(eng: Playground | Peer): void {
  eng.onCycleOutcome((metaJson: string, payloads: Uint8Array[]) => {
    const meta = JSON.parse(metaJson) as Omit<
      Extract<EventBody, { kind: "cycleOutcome" }>,
      "kind"
    >;
    const buffers = payloads.map((p) => p.buffer);
    self.postMessage(
      { v: 1, event: { kind: "cycleOutcome", ...meta }, payloads: buffers },
      buffers,
    );
  });
}

/** Ask the main thread for a bearer token; resolves on its `tokenResponse`.
 * No timeout: the proxy always answers, even when no `getToken` was
 * configured (with an error, which fails the connect typed). */
function requestToken(reason: "connect" | "reconnect"): Promise<string> {
  const requestId = nextTokenRequestId++;
  return new Promise((resolve, reject) => {
    tokenWaiters.set(requestId, ({ token, error }) => {
      if (typeof token === "string" && token.length > 0) {
        resolve(token);
      } else {
        const e = new Error(
          error ?? "no bearer token available (is a getToken callback configured?)",
        ) as Error & { code: string; status: number };
        e.code = "unauthorized";
        e.status = 401;
        reject(e);
      }
    });
    emit({ kind: "tokenRequest", requestId, reason });
  });
}

function requireEngine(): Playground | Peer {
  if (!engine) {
    const cause = initError ?? poisoned;
    const e = new Error(
      cause ? `engine failed to initialize: ${cause.message}` : "engine not initialized",
    ) as Error & { code: string; status: number };
    e.code = cause ? cause.code : "not_initialized";
    e.status = cause ? cause.status : 500;
    throw e;
  }
  return engine;
}

/** Playground-only surface (transacts, createLedger, the crash hook). */
function requirePlayground(opName: string): Playground {
  const eng = requireEngine();
  if (!(eng instanceof Playground)) {
    const e = new Error(
      `"${opName}" is not available in peer mode — the peer is read-only ` +
        `(commits are ordered by the origin server's write authority)`,
    ) as Error & { code: string; status: number };
    e.code = "unsupported";
    e.status = 501;
    throw e;
  }
  return eng;
}

/** Peer-only surface (token refresh). */
function requirePeer(opName: string): Peer {
  const eng = requireEngine();
  if (!(eng instanceof Peer)) {
    const e = new Error(
      `"${opName}" is not available in playground mode — memory ledgers ` +
        `carry no bearer token`,
    ) as Error & { code: string; status: number };
    e.code = "unsupported";
    e.status = 501;
    throw e;
  }
  return eng;
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
  // Token answers are fire-and-forget and must never wait behind init —
  // init is exactly what they unblock.
  if (req.op === "tokenResponse") {
    const waiter = tokenWaiters.get(req.requestId);
    if (waiter) {
      tokenWaiters.delete(req.requestId);
      waiter({ token: req.token, error: req.error });
    }
    return;
  }
  // Any non-init request racing an in-flight init waits for it; the init op
  // itself replies from its own frame below.
  if (req.op !== "init" && initing) {
    await initing;
  }
  try {
    switch (req.op) {
      case "init": {
        if (req.mode !== "playground" && req.mode !== "peer") {
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
          initing ??= (async (): Promise<ErrorShape | null> => {
            try {
              await init(req.wasmUrl === undefined ? undefined : { module_or_path: req.wasmUrl });
              if (req.mode === "peer") {
                if (!req.url) {
                  throw Object.assign(new Error("peer mode requires a server url"), {
                    code: "invalid_input",
                    status: 400,
                  });
                }
                const token = await requestToken(req.reinit ? "reconnect" : "connect");
                const peer = connectPeer(req.url, token, req.maxMemoryBytes);
                peer.onHeadChange((json: string) => {
                  const change = JSON.parse(json) as {
                    ledger: string;
                    t: number;
                    indexT: number;
                  };
                  emit({ kind: "headChange", ...change });
                });
                if (req.subscribe) peer.startHeadTracking(req.subscribe);
                engine = peer;
              } else {
                engine = new Playground(req.maxMemoryBytes);
              }
              wireCycleOutcomes(engine);
              mode = req.mode;
              initError = null;
              return null;
            } catch (err) {
              initError = toErrorShape(err);
              return initError;
            } finally {
              initing = null;
            }
          })();
          const failure = await initing;
          if (failure) {
            reply({ id, ok: false, error: failure });
            return;
          }
        }
        reply({
          id,
          ok: true,
          result: { version: version(), mode, maxMemoryBytes: req.maxMemoryBytes },
        });
        return;
      }
      case "createLedger":
        reply({
          id,
          ok: true,
          result: JSON.parse(await requirePlayground("createLedger").createLedger(req.ledger)),
        });
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
        reply({
          id,
          ok: true,
          result: JSON.parse(await requirePlayground("insert").insert(req.ledger, req.body)),
        });
        return;
      case "upsert":
        reply({
          id,
          ok: true,
          result: JSON.parse(await requirePlayground("upsert").upsert(req.ledger, req.body)),
        });
        return;
      case "update":
        reply({
          id,
          ok: true,
          result: JSON.parse(await requirePlayground("update").update(req.ledger, req.body)),
        });
        return;
      case "sparqlUpdate":
        reply({
          id,
          ok: true,
          result: JSON.parse(
            await requirePlayground("sparqlUpdate").sparqlUpdate(req.ledger, req.body),
          ),
        });
        return;
      case "subscribe":
        reply({
          id,
          ok: true,
          result: { subId: requireEngine().subscribe(req.ledger, req.kind, req.text) },
        });
        return;
      case "unsubscribe":
        reply({ id, ok: true, result: requireEngine().unsubscribe(req.subId) });
        return;
      case "setToken":
        requirePeer("setToken").setToken(req.token);
        reply({ id, ok: true });
        return;
      case "debugCrash":
        // Test hook (see Playground._debugCrash): deliberately trap the
        // instance to exercise the crash/recycle path.
        requirePlayground("debugCrash").debugCrash();
        reply({ id, ok: true });
        return;
      case "query": {
        const eng = requireEngine();
        const { handle: snap, owned } = await resolveTarget(req.target);
        let bytes: Uint8Array;
        try {
          bytes =
            req.kind === "sparql"
              ? await eng.querySparql(snap, req.text, req.timeoutMs)
              : await eng.queryJsonld(snap, req.text, req.timeoutMs);
        } finally {
          // Release only ephemeral (per-call) snapshots. This `finally` runs
          // BEFORE the outer catch sets `poisoned`, so a trap in the query
          // must not surface a second trap from release() and mask the
          // original — swallow release's own failure.
          if (owned) {
            try {
              eng.release(snap);
            } catch {
              // A poisoned instance can't release; the recycle discards it.
            }
          }
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

self.onmessageerror = (ev) => {
  // A request that failed structured deserialization carries no usable id —
  // nothing to reply to; the proxy fails its pending calls on its own error
  // handler. Log for diagnosis.
  console.error("fluree-db-wasm worker: message deserialization failed", ev);
};
