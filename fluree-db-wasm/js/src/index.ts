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
  CycleOutcomeEvent,
  ErrorShape,
  EventBody,
  EventMessage,
  HeadChangeEvent,
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
  CycleOutcomeEvent,
  ErrorShape,
  EventBody,
  HeadChangeEvent,
  LedgerInfo,
  QueryKind,
  ResultTransport,
  SnapshotInfo,
} from "./protocol.js";

/** One update delivered to a live subscription's callback: either fresh
 * `data` (parsed JSON in the query's language-matched format) or an `error`
 * for this cycle. Unchanged cycles deliver nothing. Previous data is NOT
 * retained here — keep-last-good composition belongs to the caller (the
 * React layer's contract, H §4). */
export interface LiveUpdate {
  ledger: string;
  /** The cycle's frozen watermark (`-1`: no consistent view). */
  t: number;
  data?: unknown;
  error?: FlureeError;
}

/**
 * One advance-cycle in its BATCH form, as delivered to `onCycle` — before
 * it is fanned out to per-subscription callbacks.
 *
 * `subscribe()`'s per-sub callbacks are the ergonomic surface, but they lose
 * two things a caching consumer needs. Version coherence: applying every
 * entry of a cycle before notifying anyone is what makes two sibling
 * components unable to disagree about `t`, and that is only possible if the
 * whole batch arrives together. And `unchanged`: "this query re-ran at `t`
 * and its results did not move" is information (it retires an error, and it
 * is the signal NOT to touch a cached snapshot), which per-sub callbacks
 * cannot express because they deliver nothing at all.
 *
 * TREAT THIS OBJECT AS READ-ONLY. One decoded cycle is memoized and handed —
 * the same instance, arrays and `FlureeError`s included — to every internal
 * listener (the per-sub fan-out and every `onCycle` callback) so a commit is
 * decoded once, not once per listener. Mutating it (`cycle.changed.sort()`,
 * reassigning a field) is therefore visible to the other listeners and is
 * cached for anyone who reads the same cycle again; copy before you mutate.
 */
export interface LiveCycle {
  ledger: string;
  /** The cycle's frozen watermark, or `undefined` when the engine had no
   * consistent view for it (the protocol's `-1`). */
  t: number | undefined;
  /** Subscriptions whose results moved, with their decoded payloads. */
  changed: { subId: number; data: unknown }[];
  /** Subscriptions that re-ran at `t` and produced identical results. */
  unchanged: number[];
  /** Per-subscription failures. These repeat every cycle and never block
   * the other subscriptions. */
  errored: { subId: number; error: FlureeError }[];
}

/**
 * Engine lifecycle, for consumers that must react to a crash recycle.
 * Subscriptions and in-memory state do NOT survive one, so a consumer holding
 * live subscriptions has to re-register them when the engine comes back.
 *
 * - `"recycling"`: the worker was poisoned and torn down; a respawn is
 *   scheduled. Everything in flight has already been rejected.
 * - `"ready"`: a fresh worker finished re-initializing. Re-subscribe here.
 * - `"terminal"`: the respawn budget is spent (or the engine never booted).
 *   Nothing will come back; every later call rejects.
 */
export type EngineState = "recycling" | "ready" | "terminal";

/** A registered live subscription; `unsubscribe()` stops updates. */
export class LiveSubscription {
  constructor(
    readonly subId: number,
    private readonly stop: () => Promise<void>,
  ) {}

  unsubscribe(): Promise<void> {
    return this.stop();
  }
}

/** A head-change notification as delivered to `Peer.on("headChange", …)`. */
export interface HeadChange {
  /** Normalized `name:branch` ledger id. */
  ledger: string;
  /** New commit watermark. */
  t: number;
  /** New index watermark. */
  indexT: number;
}

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
  /** Wall-clock budget for this query in milliseconds (F3). When it elapses,
   * the query is cancelled and the call rejects with a typed `timeout` error
   * (HTTP 408) instead of pegging the worker indefinitely. Omit for no
   * timeout. The engine cancels cooperatively at its checkpoints, so an
   * I/O-bound query (the peer's residency fetches) aborts promptly; a purely
   * compute-bound stretch is bounded by reaching the next checkpoint. */
  timeoutMs?: number;
}

export interface ConnectOptions
  extends Pick<PlaygroundOptions, "workerUrl" | "wasmUrl" | "resultTransport" | "maxMemoryBytes"> {
  /**
   * Produce a bearer token with `fluree.storage.*` scope. Called on connect
   * AND again whenever the worker reconnects after a crash recycle — the
   * init message replayed to a fresh worker never carries credentials, so
   * this callback is the single source of them. Omitting it makes every
   * connect fail typed `unauthorized` (public/anonymous serving is a future
   * server tier).
   */
  getToken?: (reason: "connect" | "reconnect") => string | Promise<string>;
  /** Ledgers to start SSE head tracking for at connect (`[]` = everything
   * the token may see). Omit for no tracking; `Peer.on("headChange")` then
   * never fires until a future subscribe surface is used. */
  subscribe?: string[];
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
// Upper bound on a single init round-trip. Init awaits the wasm fetch +
// instantiate and, in peer mode, a `tokenRequest` answered by the app's
// getToken; a getToken that never settles (a hung fetch, a forgotten await)
// would otherwise wedge `initing` — and every op behind it — forever with no
// error. On expiry the init call rejects typed and the caller (connect/
// playground) closes the channel.
const INIT_TIMEOUT_MS = 30_000;
const MAX_RESPAWN_ATTEMPTS = 3;
/** Base backoff before a respawn; doubles per consecutive failure. */
const RESPAWN_BACKOFF_MS = 250;

/** Request/response multiplexer over a worker, with crash-recycling and
 * event dispatch (unsolicited worker→main messages: head changes, token
 * requests). */
class Channel {
  private worker: Worker | null;
  private readonly pending = new Map<number, Pending>();
  private nextId = 1;
  private closed = false;
  /** Replayed into every fresh worker so a recycled engine comes back ready. */
  private initMsg: Omit<InitRequest, "id"> | null = null;
  /** Answers the worker's `tokenRequest` events (peer mode). */
  getToken: ((reason: "connect" | "reconnect") => string | Promise<string>) | null = null;
  private readonly eventListeners = new Set<
    (event: EventBody, payloads?: ArrayBuffer[]) => void
  >();
  private readonly stateListeners = new Set<(state: EngineState) => void>();
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

  /** Subscribe to worker events; returns the unsubscriber. Payloads (the
   * `cycleOutcome` transfer buffers) ride alongside the event body. */
  addEventListener(listener: (event: EventBody, payloads?: ArrayBuffer[]) => void): () => void {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
  }

  /** Subscribe to engine lifecycle transitions (crash recycle); returns the
   * unsubscriber. These originate on THIS side, not in the worker, so they
   * are not `EventBody` kinds. */
  addStateListener(listener: (state: EngineState) => void): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  private emitState(state: EngineState): void {
    for (const listener of [...this.stateListeners]) {
      try {
        listener(state);
      } catch {
        // A listener's throw must not break dispatch to the others.
      }
    }
  }

  private attach(worker: Worker): void {
    worker.onmessage = (ev: MessageEvent<Response | EventMessage>) => {
      const msg = ev.data;
      // The two message families are disjoint by shape (protocol.ts): an
      // event carries `event`, a response carries `ok`.
      if ("event" in msg) {
        this.handleEvent(msg);
        return;
      }
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

  /** Route one worker event: token requests are answered here (the version
   * gate ignores unknown kinds/versions so newer workers stay compatible);
   * everything else fans out to listeners. */
  private handleEvent(msg: EventMessage): void {
    if (msg.v !== 1) return;
    const event = msg.event;
    if (event.kind === "tokenRequest") {
      const answer = (token?: string, error?: string) =>
        this.post({ op: "tokenResponse", requestId: event.requestId, token, error });
      const getToken = this.getToken;
      if (!getToken) {
        answer(undefined, "no getToken callback configured on connect()");
        return;
      }
      void Promise.resolve()
        .then(() => getToken(event.reason))
        .then(
          (token) => answer(token),
          (err) => answer(undefined, `getToken failed: ${String(err)}`),
        );
      return;
    }
    for (const listener of this.eventListeners) {
      try {
        listener(event, msg.payloads);
      } catch {
        // A listener's throw must not break dispatch to the others.
      }
    }
  }

  /** Post a request without registering a pending entry — for
   * fire-and-forget answers the worker never replies to. */
  private post(req: RequestBody): void {
    if (this.closed || this.worker === null) return;
    this.worker.postMessage({ ...req, id: this.nextId++ });
  }

  /** Kill the poisoned worker, fail everything in flight, and — within the
   * respawn budget — start and re-initialize a fresh one. */
  private recycle(error: ErrorShape): void {
    // Re-entrancy guard: a single fatal reply can reach recycle() twice —
    // once from `onmessage` (line ~300) and once from the reinit `.then`
    // below — and without this each would arm its own respawn timer, leaving
    // the first to fire an orphan worker (a live wasm instance + SSE stream)
    // after the second already recovered. A later recycle supersedes any
    // pending respawn rather than stacking a second timer.
    if (this.respawnTimer) {
      clearTimeout(this.respawnTimer);
      this.respawnTimer = null;
    }
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
      this.emitState("terminal");
      return;
    }
    this.emitState("recycling");
    const delay = RESPAWN_BACKOFF_MS * 2 ** this.respawnAttempts;
    this.respawnAttempts++;
    this.respawnTimer = setTimeout(() => {
      this.respawnTimer = null;
      if (this.closed || this.terminal) return;
      this.worker = this.spawn();
      this.attach(this.worker);
      if (this.initMsg) {
        // Re-init in the background, marked as a replay: peer mode turns
        // that into a `tokenRequest` with reason "reconnect" (init itself
        // never carries credentials). Success resets the failure budget; a
        // fatal failure re-enters recycle() via the normal reply path, so the
        // cause is never swallowed — it becomes the next terminal message.
        void this.call({ ...this.initMsg, reinit: true })
          .then((res) => {
            if (res.ok) {
              this.markInitialized();
              // Subscriptions did NOT survive: consumers re-register here.
              this.emitState("ready");
              return;
            }
            // A NON-fatal re-init failure lands here and used to stop: not
            // initialized, not terminal, no respawn scheduled. That is the
            // one state a consumer cannot act on — it saw the crash and
            // then silence, forever. It is reachable on the ordinary path:
            // a `getToken` rejection mints `unauthorized`/401 (worker.ts),
            // and only RuntimeError/RangeError are fatalized. Re-enter the
            // ladder so this either recovers or spends the budget and
            // becomes a terminal error the consumer is told about.
            //
            // ONLY the non-fatal subset: a FATAL re-init reply is already
            // recycled synchronously by `onmessage` (line ~300), so
            // recycling it again here double-fires the ladder. (The
            // re-entrancy guard in recycle() is the backstop; this is the
            // correct predicate.)
            if (!res.error.fatal) this.recycle(res.error);
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
      // Clear the init deadline (if any) on whichever way the call settles,
      // so a normal reply never leaves a late timer to fire.
      let timer: ReturnType<typeof setTimeout> | undefined;
      const settle = {
        resolve: (r: Response) => {
          if (timer !== undefined) clearTimeout(timer);
          resolve(r);
        },
        reject: (e: unknown) => {
          if (timer !== undefined) clearTimeout(timer);
          reject(e);
        },
      };
      this.pending.set(id, settle);
      if (req.op === "init") {
        timer = setTimeout(() => {
          // Only if still unsettled (onmessage may have resolved it already).
          if (this.pending.delete(id)) {
            settle.reject(
              new FlureeError({
                code: "timeout",
                status: 504,
                message: `engine init did not complete within ${INIT_TIMEOUT_MS}ms (a hung getToken or wasm fetch?)`,
              }),
            );
          }
        }, INIT_TIMEOUT_MS);
      }
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

/**
 * Decode one wire `cycleOutcome` into the batch form. The ONE place payload
 * buffers are decoded and the `-1` no-consistent-view watermark is
 * normalized, so the per-sub fan-out below and `Peer.onCycle` can never
 * disagree about what a cycle meant.
 */
// Memoize the decoded cycle by its source event. The channel dispatches the
// SAME event object to every listener, and PeerTransport registers two that
// both decode it — one for per-subscription fan-out (LiveRegistry), one for
// delivery (Peer.onCycle). Without this, every changed payload is
// UTF-8-decoded and JSON.parsed twice per commit on the main thread, in a
// package whose whole point is paying only when the answer changes. The event
// is unreferenced after dispatch, so entries are collected with it (no leak).
const liveCycleCache = new WeakMap<CycleOutcomeEvent, LiveCycle>();

function toLiveCycle(event: CycleOutcomeEvent, payloads?: ArrayBuffer[]): LiveCycle {
  const cached = liveCycleCache.get(event);
  if (cached !== undefined) return cached;
  const cycle: LiveCycle = {
    ledger: event.ledger,
    t: event.t === -1 ? undefined : event.t,
    changed: [],
    unchanged: [...event.unchanged],
    errored: event.errored.map(({ subId, error }) => ({
      subId,
      error: new FlureeError({ code: "internal", status: 500, message: error }),
    })),
  };
  event.changed.forEach((c, i) => {
    const buf = payloads?.[i];
    if (buf === undefined) {
      // The protocol requires a payload per changed entry; a missing one is
      // a broken cycle, not an empty result.
      cycle.errored.push({
        subId: c.subId,
        error: new FlureeError({
          code: "internal",
          status: 500,
          message: "live cycle reported a change with no payload",
        }),
      });
      return;
    }
    try {
      cycle.changed.push({ subId: c.subId, data: JSON.parse(decoder.decode(buf)) });
    } catch (err) {
      cycle.errored.push({
        subId: c.subId,
        error: new FlureeError({
          code: "internal",
          status: 500,
          message: `live result decode failed: ${String(err)}`,
        }),
      });
    }
  });
  liveCycleCache.set(event, cycle);
  return cycle;
}

/** Routes batched `cycleOutcome` events to per-subscription callbacks —
 * ONE worker message per advance-cycle in, per-sub `LiveUpdate`s out. */
class LiveRegistry {
  private subs = new Map<number, (update: LiveUpdate) => void>();
  private wired = false;
  /** The engine generation `subs` belongs to. Subscription ids are minted
   * by `LiveQuerySet::next_id`, which restarts at 1 in a fresh engine, so
   * ids from before a crash recycle ALIAS the new engine's ids — the same
   * hazard `Ledger`/`Snapshot` stamp against (review H-4). */
  private generation: number;

  constructor(private readonly channel: Channel) {
    this.generation = channel.generation;
  }

  /** Drop everything registered against a previous engine. Subscriptions do
   * not survive a recycle, and keeping their callbacks would hand the next
   * engine's subId 1 to the previous engine's subscriber. */
  private syncGeneration(): void {
    if (this.channel.generation !== this.generation) {
      this.generation = this.channel.generation;
      this.subs = new Map();
    }
  }

  async subscribe(
    ledger: string,
    query: string | object,
    onUpdate: (update: LiveUpdate) => void,
  ): Promise<LiveSubscription> {
    this.wire();
    const res = unwrap<{ subId: number }>(
      await this.channel.call({
        op: "subscribe",
        ledger,
        kind: queryKindOf(query),
        text: asJsonText(query),
      }),
    );
    // AFTER the await: a recycle during the round trip must not leave this
    // registration in a map belonging to the dead engine.
    this.syncGeneration();
    const generation = this.channel.generation;
    this.subs.set(res.subId, onUpdate);
    return new LiveSubscription(res.subId, async () => {
      // Tidying a stale handle from before a recycle must be a NO-OP. The
      // docs tell consumers to re-subscribe after a crash and then release
      // the old handle; without this check that release deletes the NEW
      // subscription's callback and posts `unsubscribe` for its id, killing
      // a live subscription silently.
      if (this.channel.generation !== generation) return;
      this.subs.delete(res.subId);
      await this.channel.call({ op: "unsubscribe", subId: res.subId }).then(unwrap);
    });
  }

  private wire(): void {
    if (this.wired) return;
    this.wired = true;
    this.channel.addEventListener((event, payloads) => {
      if (event.kind !== "cycleOutcome") return;
      // Events only ever come from the CURRENT worker, so a generation
      // change means everything still in `subs` is from a dead engine.
      this.syncGeneration();
      const cycle = toLiveCycle(event, payloads);
      // `t: -1` is normalized to `undefined` in the batch form; this per-sub
      // surface has always reported the raw watermark, so it keeps doing so.
      const t = cycle.t ?? -1;
      for (const { subId, data } of cycle.changed) {
        this.subs.get(subId)?.({ ledger: cycle.ledger, t, data });
      }
      for (const { subId, error } of cycle.errored) {
        this.subs.get(subId)?.({ ledger: cycle.ledger, t, error });
      }
    });
  }
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
      timeoutMs: options.timeoutMs,
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
      timeoutMs: options.timeoutMs,
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
  private readonly live: LiveRegistry;

  constructor(
    private readonly channel: Channel,
    init: InitResult,
    private readonly transport: ResultTransport,
  ) {
    this.version = init.version;
    this.live = new LiveRegistry(channel);
  }

  /**
   * Live query (A4): `onUpdate` fires with the first result right away (the
   * auto-prime) and again after every local commit that changes the result —
   * unchanged commits are hash-gated engine-side and deliver nothing.
   * Subscriptions do not survive a crash recycle; re-subscribe after a
   * `fatal` error.
   */
  subscribe(
    ledger: string,
    query: string | object,
    onUpdate: (update: LiveUpdate) => void,
  ): Promise<LiveSubscription> {
    return this.live.subscribe(ledger, query, onUpdate);
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
  // Close the spawned worker if init throws: a NON-fatal init failure (bad
  // wasmUrl, unsupported mode) leaves the worker alive but `onmessage` does
  // not recycle it, and the caller never receives the Channel, so nobody can
  // call close() — it would leak (a wasm instance too, for peer mode) on
  // every failed/retried connect. A fatal failure self-cleans via recycle.
  let init: InitResult;
  try {
    init = unwrap<InitResult>(
      await channel.call({
        op: "init",
        mode: "playground",
        wasmUrl: options.wasmUrl === undefined ? undefined : String(options.wasmUrl),
        maxMemoryBytes,
      }),
    );
  } catch (e) {
    channel.close();
    throw e;
  }
  channel.markInitialized();
  return new Playground(channel, init, transport);
}

/**
 * A read-only peer engine over a remote Fluree server: heads resolve through
 * the server's nameservice, blocks arrive CID-verified and cached
 * (IndexedDB), queries run locally. Transacting through a peer rejects with
 * a typed `unsupported` — commits are ordered by the origin server.
 */
export class Peer {
  /** Engine (crate) version. */
  readonly version: string;
  private readonly live: LiveRegistry;

  constructor(
    private readonly channel: Channel,
    init: InitResult,
    private readonly transport: ResultTransport,
  ) {
    this.version = init.version;
    this.live = new LiveRegistry(channel);
  }

  /** Open a remote ledger (resolves its head; rejects `not_found` for
   * unknown or unauthorized ledgers — the server answers both identically).
   * The returned `Ledger`'s transact methods reject `unsupported` in peer
   * mode; `query`/`snapshot`/`info` work exactly as in the playground. */
  async ledger(id: string): Promise<Ledger> {
    const info = unwrap<LedgerInfo>(await this.channel.call({ op: "ledgerInfo", ledger: id }));
    return new Ledger(this.channel, info, this.transport);
  }

  /**
   * Live query (A4): `onUpdate` fires with the first result right away (the
   * auto-prime) and again after every server commit that changes the result
   * (SSE head change → one engine advance-cycle; unchanged results are
   * hash-gated engine-side and deliver nothing). Requires head tracking —
   * the `subscribe` connect option. Subscriptions do not survive a crash
   * recycle; re-subscribe after a `fatal` error.
   */
  subscribe(
    ledger: string,
    query: string | object,
    onUpdate: (update: LiveUpdate) => void,
  ): Promise<LiveSubscription> {
    return this.live.subscribe(ledger, query, onUpdate);
  }

  /**
   * Subscribe to ledger head changes (SSE head tracking must be active —
   * the `subscribe` connect option). The engine has already absorbed the
   * advance when the callback runs: a new `snapshot()` sees the new head;
   * existing snapshots stay frozen. Returns the unsubscriber.
   *
   * This is the fan-out primitive the live-query tier builds on; it fires
   * per ledger advance, not per query.
   */
  on(kind: "headChange", listener: (change: HeadChange) => void): () => void {
    void kind;
    return this.channel.addEventListener((event) => {
      if (event.kind === "headChange") {
        listener({ ledger: event.ledger, t: event.t, indexT: event.indexT });
      }
    });
  }

  /**
   * Every advance-cycle in its BATCH form — one call per cycle, covering all
   * of this peer's subscriptions on that ledger, including the `unchanged`
   * list that the per-subscription callbacks cannot express.
   *
   * Use this instead of `subscribe`'s callback when results feed a cache
   * that must stay version-coherent: apply the whole batch, then notify.
   * `subscribe()` is still what REGISTERS a query — pass a no-op callback if
   * this listener is doing the delivery. Returns the unsubscriber.
   */
  onCycle(listener: (cycle: LiveCycle) => void): () => void {
    return this.channel.addEventListener((event, payloads) => {
      if (event.kind === "cycleOutcome") listener(toLiveCycle(event, payloads));
    });
  }

  /**
   * Engine lifecycle. A crash recycle discards every subscription, so a
   * consumer holding live queries must re-register them on `"ready"`.
   * Returns the unsubscriber.
   */
  onEngineState(listener: (state: EngineState) => void): () => void {
    return this.channel.addStateListener(listener);
  }

  /** Terminate the worker (driver, SSE, caches' in-memory tier included).
   * The IndexedDB block cache persists for the next session. */
  close(): void {
    this.channel.close();
  }
}

/**
 * Connect to a remote Fluree server as a local read-only peer.
 *
 * `url` is the versioned API base (`https://host/v1/fluree`). The worker is
 * spawned and the `.wasm` compiled lazily, then the token is requested from
 * `options.getToken` over the event channel (never embedded in the init
 * message — a crash recycle replays init and re-asks instead of replaying a
 * credential). Resolves once the engine is ready; no network I/O has
 * happened yet beyond the wasm fetch — heads resolve on first
 * `ledger()`/`snapshot()`.
 */
export async function connect(url: string, options: ConnectOptions = {}): Promise<Peer> {
  // Same literal-worker-expression rule as playground() (review H-6).
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
  channel.getToken = options.getToken ?? null;
  const transport = options.resultTransport ?? "transfer";
  const maxMemoryBytes =
    options.maxMemoryBytes === null ? undefined : (options.maxMemoryBytes ?? defaultMaxMemoryBytes());
  // Close the spawned worker if init throws (see playground()): the common
  // peer failures — bad url, a rejecting/empty getToken (401), unsupported
  // mode — are all non-fatal, so the worker + its already-instantiated wasm
  // stay alive and unreachable unless we close here.
  let init: InitResult;
  try {
    init = unwrap<InitResult>(
      await channel.call({
        op: "init",
        mode: "peer",
        url,
        subscribe: options.subscribe,
        wasmUrl: options.wasmUrl === undefined ? undefined : String(options.wasmUrl),
        maxMemoryBytes,
      }),
    );
  } catch (e) {
    channel.close();
    throw e;
  }
  channel.markInitialized();
  return new Peer(channel, init, transport);
}
