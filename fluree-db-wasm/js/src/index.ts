/**
 * `@fluree/db-wasm` — main-thread API.
 *
 * This module contains **no WebAssembly**. It spawns the engine worker
 * (`./worker.js`, which loads the `.wasm` lazily via streaming instantiation)
 * and speaks the request/response protocol in `protocol.ts` over
 * `postMessage`. The page never blocks: every call is a promise resolved by a
 * worker reply.
 *
 * Crash handling (adversarial review F4): a wasm trap — Rust panic or
 * out-of-memory abort — poisons the engine instance. The proxy detects the
 * `fatal` error (or a worker `error` event), rejects every in-flight call
 * with the typed error, terminates the worker, and — with capped, backed-off
 * attempts — spawns + re-initializes a fresh one, so the `Playground` object
 * stays usable. Engine state (ledgers, snapshots) does NOT survive a recycle:
 * every `Ledger`/`Snapshot` object is generation-stamped and calls on
 * pre-crash handles reject with a typed `not_found` (never silently answer
 * from a different engine). If the worker can never come up (bad URL, wasm
 * fetch failure) or crashes repeatedly, the channel goes terminal with
 * `engine_unavailable` instead of respawn-looping forever.
 *
 * Hosting requirements: none beyond serving static files over HTTPS (or
 * localhost). No COOP/COEP, no SharedArrayBuffer, no special headers — the
 * engine is single-threaded inside its worker.
 */

import type {
  CommitReceipt,
  ErrorShape,
  InitRequest,
  InitResult,
  LedgerInfo,
  QueryKind,
  Request,
  Response,
  ResultTransport,
  SnapshotInfo,
} from "./protocol.js";

export type {
  CommitReceipt,
  ErrorShape,
  LedgerInfo,
  QueryKind,
  ResultTransport,
  SnapshotInfo,
} from "./protocol.js";

/** Error raised for engine failures. `code` is stable; `status` mirrors the
 * HTTP status the same failure would carry on the Fluree server. `fatal`
 * means the engine worker was recycled (in-memory state lost). */
export class FlureeError extends Error {
  readonly code: string;
  readonly status: number;
  readonly fatal: boolean;

  constructor(shape: ErrorShape) {
    super(shape.message);
    this.name = "FlureeError";
    this.code = shape.code;
    this.status = shape.status;
    this.fatal = shape.fatal === true;
  }
}

export interface PlaygroundOptions {
  /** Location of the worker module. When omitted, the worker is spawned with
   * the literal `new Worker(new URL("./worker.js", import.meta.url), ...)`
   * expression bundlers statically detect. Passing a URL here opts OUT of
   * bundler detection — you own serving that asset. */
  workerUrl?: string | URL;
  /** Location of `fluree_db_wasm_bg.wasm`. Defaults to the copy next to the
   * worker's wasm-bindgen glue (`pkg/`). Set this when serving the `.wasm`
   * from a CDN or a non-default asset path. */
  wasmUrl?: string | URL;
  /** How query results cross the worker boundary. Default `"transfer"`. */
  resultTransport?: ResultTransport;
  /** Per-query memory budget in bytes (F4). A query whose retained working
   * set crosses this fails with a typed `out_of_memory` error instead of
   * growing wasm memory until the allocator kills the worker. Transact paths
   * get only a coarse input-size pre-gate from this — see the README. Default:
   * a conservative fraction of `navigator.deviceMemory` when the browser
   * exposes it (¼ of device memory, clamped to [256 MiB, 2 GiB]), else
   * 512 MiB. Pass `null` for the engine default (1 GiB, no device probe). */
  maxMemoryBytes?: number | null;
}

export interface QueryOptions {
  /** Per-call override of the result transport. */
  transport?: ResultTransport;
}

function defaultMaxMemoryBytes(): number {
  const MiB = 1024 * 1024;
  const dev = (navigator as Navigator & { deviceMemory?: number }).deviceMemory;
  if (typeof dev === "number" && dev > 0) {
    return Math.min(Math.max(Math.round((dev * 1024 * MiB) / 4), 256 * MiB), 2048 * MiB);
  }
  return 512 * MiB;
}

interface Pending {
  resolve: (r: Response) => void;
  reject: (e: Error) => void;
}

/** `Omit` applied per union member — plain `Omit<Request, "id">` would
 * collapse the union to its common keys and reject every op-specific field. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

type RequestBody = DistributiveOmit<Request, "id">;

/** Consecutive failed respawns tolerated before the channel goes terminal. */
const MAX_RESPAWN_ATTEMPTS = 3;
/** Base backoff before a respawn; doubles per consecutive failure. */
const RESPAWN_BACKOFF_MS = 250;

/** Request/response multiplexer over a worker, with crash-recycling. */
class Channel {
  private worker: Worker | null;
  private readonly pending = new Map<number, Pending>();
  private nextId = 1;
  private closed = false;
  /** Replayed into every fresh worker so a recycled engine comes back ready. */
  private initMsg: Omit<InitRequest, "id"> | null = null;
  /** Bumped on every recycle; `Ledger`/`Snapshot` objects stamp it at
   * creation so pre-crash handles can never alias a fresh engine's state
   * (review H-4). */
  private currentGeneration = 0;
  /** Set once `playground()` has seen a successful init: a worker that never
   * booted is never respawned (review H-5). */
  private everInitialized = false;
  private respawnAttempts = 0;
  private respawnTimer: ReturnType<typeof setTimeout> | null = null;
  private terminal: FlureeError | null = null;

  constructor(private readonly spawn: () => Worker) {
    this.worker = spawn();
    this.attach(this.worker);
  }

  get generation(): number {
    return this.currentGeneration;
  }

  /** Called after a successful engine init (first boot and re-inits). */
  markInitialized(): void {
    this.everInitialized = true;
    this.respawnAttempts = 0;
  }

  private attach(worker: Worker): void {
    worker.onmessage = (ev: MessageEvent<Response>) => {
      const msg = ev.data;
      const p = this.pending.get(msg.id);
      if (p) {
        this.pending.delete(msg.id);
        p.resolve(msg);
      }
      // A fatal reply means the wasm instance is poisoned: the caller above
      // already got the typed error; everything else is recycled (F4).
      if (!msg.ok && msg.error.fatal) this.recycle(msg.error);
    };
    worker.onerror = (ev) => {
      this.recycle({
        code: "engine_crashed",
        status: 500,
        message: ev.message || "engine worker error",
        fatal: true,
      });
    };
    worker.onmessageerror = () => {
      // A reply that could not be deserialized carries no id, so the specific
      // pending entry is unknowable — fail them all rather than strand one.
      const err = new FlureeError({
        code: "internal",
        status: 500,
        message: "worker reply could not be deserialized",
      });
      for (const p of this.pending.values()) p.reject(err);
      this.pending.clear();
    };
  }

  /** Kill the poisoned worker, fail everything in flight, and — within the
   * respawn budget — start and re-initialize a fresh one. */
  private recycle(error: ErrorShape): void {
    const err = new FlureeError(error);
    for (const p of this.pending.values()) p.reject(err);
    this.pending.clear();
    if (this.worker) {
      this.worker.onmessage = null;
      this.worker.onerror = null;
      this.worker.onmessageerror = null;
      this.worker.terminate();
      this.worker = null;
    }
    if (this.closed || this.terminal) return;
    this.currentGeneration++;
    // H-5: a worker that never completed an init (bad workerUrl, wasm fetch
    // failure) would fail identically forever — don't spawn-loop on it. Same
    // once the consecutive-failure budget is spent.
    if (!this.everInitialized || this.respawnAttempts >= MAX_RESPAWN_ATTEMPTS) {
      this.terminal = new FlureeError({
        code: "engine_unavailable",
        status: 503,
        message: this.everInitialized
          ? `engine worker failed ${this.respawnAttempts + 1} times in a row; giving up (last error: ${error.message})`
          : `engine worker never became ready: ${error.message}`,
        fatal: true,
      });
      return;
    }
    const delay = RESPAWN_BACKOFF_MS * 2 ** this.respawnAttempts;
    this.respawnAttempts++;
    this.respawnTimer = setTimeout(() => {
      this.respawnTimer = null;
      if (this.closed || this.terminal) return;
      this.worker = this.spawn();
      this.attach(this.worker);
      if (this.initMsg) {
        // Re-init in the background. Success resets the failure budget; a
        // fatal failure re-enters recycle() via the normal reply path, so the
        // cause is never swallowed — it becomes the next terminal message.
        void this.call(this.initMsg)
          .then((res) => {
            if (res.ok) this.markInitialized();
          })
          .catch(() => {});
      }
    }, delay);
  }

  call(req: RequestBody): Promise<Response> {
    if (this.closed) {
      return Promise.reject(
        new FlureeError({ code: "closed", status: 499, message: "playground is closed" }),
      );
    }
    if (this.terminal) {
      return Promise.reject(this.terminal);
    }
    if (this.worker === null) {
      return Promise.reject(
        new FlureeError({
          code: "engine_restarting",
          status: 503,
          message: "engine is restarting after a crash; retry shortly",
        }),
      );
    }
    if (req.op === "init") this.initMsg = req as Omit<InitRequest, "id">;
    const id = this.nextId++;
    const worker = this.worker;
    return new Promise<Response>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      worker.postMessage({ ...req, id });
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.respawnTimer !== null) {
      clearTimeout(this.respawnTimer);
      this.respawnTimer = null;
    }
    this.worker?.terminate();
    this.worker = null;
    const err = new FlureeError({ code: "closed", status: 499, message: "playground is closed" });
    for (const p of this.pending.values()) p.reject(err);
    this.pending.clear();
  }
}

function unwrap<T>(res: Response): T {
  if (!res.ok) throw new FlureeError(res.error);
  return res.result as T;
}

const decoder = new TextDecoder();

function decodeResult(res: Response): unknown {
  if (!res.ok) throw new FlureeError(res.error);
  if (res.bytes !== undefined) return JSON.parse(decoder.decode(res.bytes));
  if (res.text !== undefined) return JSON.parse(res.text);
  throw new FlureeError({ code: "internal", status: 500, message: "empty query reply" });
}

function asJsonText(data: unknown): string {
  return typeof data === "string" ? data : JSON.stringify(data);
}

function queryKindOf(query: string | object): QueryKind {
  return typeof query === "string" ? "sparql" : "jsonld";
}

function recycledError(what: string): FlureeError {
  return new FlureeError({
    code: "not_found",
    status: 404,
    message: `${what} belongs to an engine that was recycled after a crash — in-memory playground state was lost`,
  });
}

/** A frozen, immutable view of a ledger at one commit watermark. Queries on
 * it never see later commits. Hold it for multi-query consistency; `release()`
 * it when done (each live snapshot pins engine memory). */
export class Snapshot {
  /** The frozen commit watermark. */
  readonly t: number;
  /** Normalized `name:branch` id of the ledger this snapshot came from. */
  readonly ledgerId: string;
  private released = false;
  private readonly generation: number;

  constructor(
    private readonly channel: Channel,
    private readonly info: SnapshotInfo,
    private readonly transport: ResultTransport,
  ) {
    this.t = info.t;
    this.ledgerId = info.id;
    this.generation = channel.generation;
  }

  /** Query this frozen view (string = SPARQL, object = JSON-LD). */
  async query(query: string | object, options: QueryOptions = {}): Promise<unknown> {
    // H-4: a fresh engine mints the same small handle numbers, so a stale
    // handle must be refused here, not sent — it would silently address a
    // different frozen view.
    if (this.channel.generation !== this.generation) {
      throw recycledError(`snapshot of ${this.ledgerId} at t=${this.t}`);
    }
    const res = await this.channel.call({
      op: "query",
      target: { snapshot: this.info.handle },
      kind: queryKindOf(query),
      text: asJsonText(query),
      transport: options.transport ?? this.transport,
    });
    return decodeResult(res);
  }

  /** Free the engine-side view. Idempotent. A snapshot that did not survive
   * an engine recycle is already gone — releasing it is a no-op, and the
   * stale handle is never posted (it could name a fresh snapshot). */
  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    if (this.channel.generation !== this.generation) return;
    await this.channel.call({ op: "release", snapshot: this.info.handle }).then(unwrap);
  }
}

/** A ledger handle. Cheap value object: holds the id and the channel. */
export class Ledger {
  /** Normalized `name:branch` id. */
  readonly id: string;
  private readonly generation: number;

  constructor(
    private readonly channel: Channel,
    info: LedgerInfo,
    private readonly transport: ResultTransport,
  ) {
    this.id = info.id;
    this.generation = channel.generation;
  }

  /** Current watermarks (`t`, `indexT`). */
  async info(): Promise<LedgerInfo> {
    this.guard();
    return unwrap<LedgerInfo>(await this.channel.call({ op: "ledgerInfo", ledger: this.id }));
  }

  /** Insert JSON-LD: a node, an array of nodes, or `{ "@context", "@graph" }`.
   * Accepts an object or pre-serialized JSON text. */
  insert(data: unknown): Promise<CommitReceipt> {
    return this.transact("insert", data);
  }

  /** Upsert JSON-LD (replaces existing single-cardinality values). */
  upsert(data: unknown): Promise<CommitReceipt> {
    return this.transact("upsert", data);
  }

  /** JSON-LD update: `{ where, delete, insert }`. */
  update(data: unknown): Promise<CommitReceipt> {
    return this.transact("update", data);
  }

  /** SPARQL 1.1 Update. */
  sparqlUpdate(sparql: string): Promise<CommitReceipt> {
    return this.transact("sparqlUpdate", sparql);
  }

  /** Freeze the current head for multi-query consistency (F6). */
  async snapshot(): Promise<Snapshot> {
    this.guard();
    const info = unwrap<SnapshotInfo>(await this.channel.call({ op: "snapshot", ledger: this.id }));
    return new Snapshot(this.channel, info, this.transport);
  }

  /**
   * Query the ledger. The head is frozen for the duration of this one call
   * (worker-side ephemeral snapshot), so a concurrent commit can never move
   * the view mid-query; results are always fully buffered — there is no
   * streaming surface.
   *
   * - A string is SPARQL → W3C SPARQL Results JSON (SELECT/ASK) or JSON-LD
   *   (CONSTRUCT/DESCRIBE).
   * - An object is a JSON-LD query → Fluree JSON-LD results.
   */
  async query(query: string | object, options: QueryOptions = {}): Promise<unknown> {
    this.guard();
    const res = await this.channel.call({
      op: "query",
      target: { ledger: this.id },
      kind: queryKindOf(query),
      text: asJsonText(query),
      transport: options.transport ?? this.transport,
    });
    return decodeResult(res);
  }

  /** H-4: after a recycle the ledger this object described no longer exists;
   * a same-named ledger the app re-creates is a different ledger. Throws
   * (surfacing as a rejection — every caller is async). */
  private guard(): void {
    if (this.channel.generation !== this.generation) {
      throw recycledError(`ledger ${this.id}`);
    }
  }

  private async transact(
    op: "insert" | "upsert" | "update" | "sparqlUpdate",
    data: unknown,
  ): Promise<CommitReceipt> {
    this.guard();
    // asJsonText inside the async frame: a non-serializable input (circular
    // object) rejects instead of throwing synchronously from a Promise API.
    return unwrap<CommitReceipt>(
      await this.channel.call({ op, ledger: this.id, body: asJsonText(data) }),
    );
  }
}

/** An in-memory Fluree engine running in its own worker. */
export class Playground {
  /** Engine (crate) version. */
  readonly version: string;

  constructor(
    private readonly channel: Channel,
    init: InitResult,
    private readonly transport: ResultTransport,
  ) {
    this.version = init.version;
  }

  /** Create a ledger (`"demo"` → `"demo:main"`). Rejects with `conflict` if
   * it already exists. */
  async createLedger(id: string): Promise<Ledger> {
    const info = unwrap<LedgerInfo>(await this.channel.call({ op: "createLedger", ledger: id }));
    return new Ledger(this.channel, info, this.transport);
  }

  /** Open an existing ledger. Rejects with `not_found`. */
  async ledger(id: string): Promise<Ledger> {
    const info = unwrap<LedgerInfo>(await this.channel.call({ op: "ledgerInfo", ledger: id }));
    return new Ledger(this.channel, info, this.transport);
  }

  /** Terminate the worker. Everything in it (all ledgers) is discarded. */
  close(): void {
    this.channel.close();
  }

  /** @internal Test hook: trap the wasm instance on purpose to exercise the
   * crash/recycle path (used by the browser smoke test). Rejects with the
   * fatal `engine_crashed` error the crash produces. Not part of the public
   * API surface; do not call outside tests. */
  _debugCrash(): Promise<void> {
    return this.channel.call({ op: "debugCrash" }).then((res) => {
      unwrap(res);
    });
  }
}

/**
 * Start an in-memory Fluree engine in a dedicated worker.
 *
 * Nothing is fetched until this is called; the `.wasm` is then streamed and
 * compiled inside the worker. Resolves once the engine is ready.
 */
export async function playground(options: PlaygroundOptions = {}): Promise<Playground> {
  // H-6: the default spawn is the LITERAL `new Worker(new URL(...))` shape —
  // Vite/webpack 5 static worker detection only recognizes the inline
  // expression, never a URL passed through a variable. The override arm opts
  // out of bundler detection by design.
  const override = options.workerUrl;
  const channel = new Channel(
    override === undefined
      ? () =>
          new Worker(new URL("./worker.js", import.meta.url), {
            type: "module",
            name: "fluree-db-wasm",
          })
      : () => new Worker(override, { type: "module", name: "fluree-db-wasm" }),
  );
  const transport = options.resultTransport ?? "transfer";
  const maxMemoryBytes =
    options.maxMemoryBytes === null ? undefined : (options.maxMemoryBytes ?? defaultMaxMemoryBytes());
  const init = unwrap<InitResult>(
    await channel.call({
      op: "init",
      mode: "playground",
      wasmUrl: options.wasmUrl === undefined ? undefined : String(options.wasmUrl),
      maxMemoryBytes,
    }),
  );
  channel.markInitialized();
  return new Playground(channel, init, transport);
}
