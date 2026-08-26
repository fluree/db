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
 * with the typed error, terminates the worker, and spawns + re-initializes a
 * fresh one, so the `Playground` object stays usable. Engine state (ledgers,
 * snapshots) does NOT survive a recycle — in-memory playground data is gone;
 * subsequent calls on stale handles reject with `not_found`.
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
  /** Location of the worker module. Defaults to `./worker.js` next to this
   * module, which bundlers (Vite, webpack 5, Rollup) resolve statically from
   * the `new URL(..., import.meta.url)` pattern. */
  workerUrl?: string | URL;
  /** Location of `fluree_db_wasm_bg.wasm`. Defaults to the copy next to the
   * worker's wasm-bindgen glue (`pkg/`). Set this when serving the `.wasm`
   * from a CDN or a non-default asset path. */
  wasmUrl?: string | URL;
  /** How query results cross the worker boundary. Default `"transfer"`. */
  resultTransport?: ResultTransport;
  /** Per-query memory budget in bytes (F4). A query whose retained working
   * set crosses this fails with a typed `out_of_memory` error instead of
   * growing wasm memory until the allocator kills the worker. Default: a
   * conservative fraction of `navigator.deviceMemory` when the browser
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

/** Request/response multiplexer over a worker, with crash-recycling. */
class Channel {
  private worker: Worker;
  private readonly pending = new Map<number, Pending>();
  private nextId = 1;
  private closed = false;
  /** Replayed into every fresh worker so a recycled engine comes back ready. */
  private initMsg: Omit<InitRequest, "id"> | null = null;

  constructor(private readonly spawn: () => Worker) {
    this.worker = spawn();
    this.attach(this.worker);
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
  }

  /** Kill the poisoned worker, fail everything in flight, start a fresh one. */
  private recycle(error: ErrorShape): void {
    const err = new FlureeError(error);
    for (const p of this.pending.values()) p.reject(err);
    this.pending.clear();
    this.worker.onmessage = null;
    this.worker.onerror = null;
    this.worker.terminate();
    if (this.closed) return;
    this.worker = this.spawn();
    this.attach(this.worker);
    // Fire-and-forget re-init; if it fails the next call surfaces the error.
    if (this.initMsg) void this.call(this.initMsg).catch(() => {});
  }

  call(req: RequestBody): Promise<Response> {
    if (this.closed) {
      return Promise.reject(
        new FlureeError({ code: "closed", status: 499, message: "playground is closed" }),
      );
    }
    if (req.op === "init") this.initMsg = req as Omit<InitRequest, "id">;
    const id = this.nextId++;
    return new Promise<Response>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ ...req, id });
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.worker.terminate();
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

/** A frozen, immutable view of a ledger at one commit watermark. Queries on
 * it never see later commits. Hold it for multi-query consistency; `release()`
 * it when done (each live snapshot pins engine memory). */
export class Snapshot {
  /** The frozen commit watermark. */
  readonly t: number;
  /** Normalized `name:branch` id of the ledger this snapshot came from. */
  readonly ledgerId: string;
  private released = false;

  constructor(
    private readonly channel: Channel,
    private readonly info: SnapshotInfo,
    private readonly transport: ResultTransport,
  ) {
    this.t = info.t;
    this.ledgerId = info.id;
  }

  /** Query this frozen view (string = SPARQL, object = JSON-LD). */
  query(query: string | object, options: QueryOptions = {}): Promise<unknown> {
    return this.channel
      .call({
        op: "query",
        target: { snapshot: this.info.handle },
        kind: queryKindOf(query),
        text: asJsonText(query),
        transport: options.transport ?? this.transport,
      })
      .then(decodeResult);
  }

  /** Free the engine-side view. Idempotent. */
  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    await this.channel.call({ op: "release", snapshot: this.info.handle }).then(unwrap);
  }
}

/** A ledger handle. Cheap value object: holds the id and the channel. */
export class Ledger {
  /** Normalized `name:branch` id. */
  readonly id: string;

  constructor(
    private readonly channel: Channel,
    info: LedgerInfo,
    private readonly transport: ResultTransport,
  ) {
    this.id = info.id;
  }

  /** Current watermarks (`t`, `indexT`). */
  info(): Promise<LedgerInfo> {
    return this.channel.call({ op: "ledgerInfo", ledger: this.id }).then(unwrap<LedgerInfo>);
  }

  /** Insert JSON-LD: a node, an array of nodes, or `{ "@context", "@graph" }`.
   * Accepts an object or pre-serialized JSON text. */
  insert(data: unknown): Promise<CommitReceipt> {
    return this.transact("insert", asJsonText(data));
  }

  /** Upsert JSON-LD (replaces existing single-cardinality values). */
  upsert(data: unknown): Promise<CommitReceipt> {
    return this.transact("upsert", asJsonText(data));
  }

  /** JSON-LD update: `{ where, delete, insert }`. */
  update(data: unknown): Promise<CommitReceipt> {
    return this.transact("update", asJsonText(data));
  }

  /** SPARQL 1.1 Update. */
  sparqlUpdate(sparql: string): Promise<CommitReceipt> {
    return this.transact("sparqlUpdate", sparql);
  }

  /** Freeze the current head for multi-query consistency (F6). */
  async snapshot(): Promise<Snapshot> {
    const info = unwrap<SnapshotInfo>(
      await this.channel.call({ op: "snapshot", ledger: this.id }),
    );
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
  query(query: string | object, options: QueryOptions = {}): Promise<unknown> {
    return this.channel
      .call({
        op: "query",
        target: { ledger: this.id },
        kind: queryKindOf(query),
        text: asJsonText(query),
        transport: options.transport ?? this.transport,
      })
      .then(decodeResult);
  }

  private transact(
    op: "insert" | "upsert" | "update" | "sparqlUpdate",
    body: string,
  ): Promise<CommitReceipt> {
    return this.channel.call({ op, ledger: this.id, body }).then(unwrap<CommitReceipt>);
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
}

/**
 * Start an in-memory Fluree engine in a dedicated worker.
 *
 * Nothing is fetched until this is called; the `.wasm` is then streamed and
 * compiled inside the worker. Resolves once the engine is ready.
 */
export async function playground(options: PlaygroundOptions = {}): Promise<Playground> {
  const workerUrl = options.workerUrl ?? new URL("./worker.js", import.meta.url);
  const channel = new Channel(
    () => new Worker(workerUrl, { type: "module", name: "fluree-db-wasm" }),
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
  return new Playground(channel, init, transport);
}
