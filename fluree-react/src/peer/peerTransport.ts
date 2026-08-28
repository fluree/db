/**
 * Peer mode: the `LiveTransport` backed by the wasm engine in a web worker.
 *
 * The engine already does the expensive half. It hears commits over SSE,
 * applies them locally, re-runs the affected subscriptions against ONE frozen
 * snapshot, hashes the formatted bytes worker-side, and emits exactly one
 * batched cycle — unchanged subscriptions carrying zero payload. This
 * transport is therefore mostly translation: engine subscription ids to cache
 * subscription ids, and the engine's cycle to ours.
 *
 * Two things it owns beyond translation:
 *
 * - **Async registration against a synchronous seam.** `subscribe` is
 *   fire-and-forget here but `Peer.subscribe` is a promise, and the engine
 *   itself may not have connected yet. Registrations queue and drain; an
 *   unsubscribe that arrives first cancels the pending registration rather
 *   than leaking a live query into the engine.
 * - **Crash recycles.** A recycled engine has no subscriptions. Nothing
 *   errors and no cycle ever arrives again, so without re-registration every
 *   component would silently freeze on stale data — the exact failure mode
 *   this package exists to prevent. The transport re-subscribes on `"ready"`
 *   and, when the engine goes `"terminal"`, errors every live subscription so
 *   the UI can say so.
 */

import type {
  CycleChange,
  CycleErrored,
  CycleUpdate,
  LiveTransport,
  SubscriptionSpec,
  TransportSink,
} from "../core/transport.js";
import type { ConnectionState, QueryError, ResolvedSpec } from "../core/types.js";
import type {
  PeerConnect,
  PeerEngine,
  PeerError,
  PeerSubscription,
} from "./peerEngine.js";

export interface PeerTransportOptions {
  /**
   * `connect` from `@fluree/db-wasm`, passed in rather than imported so this
   * package carries no dependency on the wasm engine (see `peerEngine.ts`).
   */
  connect: PeerConnect;
  /** The server's versioned API base, e.g. `https://host/v1/fluree`. */
  url: string;
  /** Bearer-token supplier. The worker asks for one on connect and again
   * after a crash recycle; the token is never embedded in the worker's init
   * message. */
  getToken?: (reason: "connect" | "reconnect") => string | Promise<string>;
  /**
   * Ledgers to start SSE head tracking for. Defaults to `[]`, which means
   * "everything this token may see" — the engine only delivers cycles for
   * tracked ledgers, so a narrower list silently makes untracked queries
   * non-live. Narrow it only when you know every ledger the app will query.
   */
  watch?: string[];
  workerUrl?: string | URL;
  wasmUrl?: string | URL;
  maxMemoryBytes?: number | null;
  /** Injectable for tests: use an already-built engine instead of connecting. */
  engine?: Promise<PeerEngine>;
}

interface Registration {
  spec: SubscriptionSpec;
  /** Set once the engine has accepted it. */
  sub?: PeerSubscription;
  /** Set when unsubscribe arrived before registration completed. */
  cancelled: boolean;
}

/** The format a query language produces natively. The engine has no format
 * parameter — a subscription's results are always its language's — so a
 * different `opts.format` cannot be honoured in peer mode and must fail
 * loudly rather than hand a component the wrong shape. */
function nativeFormat(kind: string): string {
  return kind === "sparql" ? "sparql-json" : "jsonld";
}

function toQueryError(err: unknown): QueryError {
  if (typeof err === "object" && err !== null && "message" in err) {
    const e = err as PeerError;
    const out: QueryError = {
      code: typeof e.code === "string" ? e.code : "peer",
      message: String(e.message),
    };
    if (typeof e.status === "number") out.status = e.status;
    return out;
  }
  return { code: "peer", message: String(err) };
}

export class PeerTransport implements LiveTransport {
  private readonly options: PeerTransportOptions;
  private sink: TransportSink | undefined;
  private engine: PeerEngine | undefined;
  private enginePromise: Promise<PeerEngine> | undefined;
  private connState: ConnectionState = "connecting";
  private closed = false;
  /**
   * The failure that took the engine out, if any. Remembered because a
   * subscribe can arrive AFTER it — an expired token fails the connect, the
   * first route's queries correctly show `unauthorized`, the user navigates,
   * and the new route mounts fresh `useQuery`s. Without a replay those are
   * stored and never acted on: `loading` forever, no error, no cycle, which
   * is the silent freeze this transport exists to prevent.
   */
  private failure: QueryError | undefined;

  /** Cache subId -> registration. */
  private readonly subs = new Map<number, Registration>();
  /** Engine subId -> cache subId. */
  private readonly byEngineId = new Map<number, number>();
  private detachCycle: (() => void) | undefined;
  private detachState: (() => void) | undefined;

  constructor(options: PeerTransportOptions) {
    this.options = options;
  }

  start(sink: TransportSink): void {
    this.sink = sink;
    const pending =
      this.options.engine ??
      this.options.connect(this.options.url, {
        ...(this.options.getToken ? { getToken: this.options.getToken } : {}),
        // Always pass a tracking list: without one the engine never starts
        // head tracking and no subscription is ever live.
        subscribe: this.options.watch ?? [],
        ...(this.options.workerUrl !== undefined
          ? { workerUrl: this.options.workerUrl }
          : {}),
        ...(this.options.wasmUrl !== undefined ? { wasmUrl: this.options.wasmUrl } : {}),
        ...(this.options.maxMemoryBytes !== undefined
          ? { maxMemoryBytes: this.options.maxMemoryBytes }
          : {}),
      });
    this.enginePromise = pending;
    void pending.then(
      (engine) => this.onEngineReady(engine),
      (err) => this.onEngineFailed(err),
    );
  }

  private onEngineReady(engine: PeerEngine): void {
    if (this.closed) {
      engine.close();
      return;
    }
    this.engine = engine;
    this.detachCycle = engine.onCycle((cycle) => this.applyCycle(cycle));
    this.detachState = engine.onEngineState((state) => {
      if (state === "recycling") {
        // Every engine id died with the worker. Drop the mapping outright:
        // a fresh worker restarts its id counter, so a stale entry could
        // otherwise route another query's results to this subscription.
        this.byEngineId.clear();
        for (const reg of this.subs.values()) reg.sub = undefined;
        this.setConnection("reconnecting");
      } else if (state === "ready") {
        // A recycled engine has no subscriptions: re-register everything
        // this transport still holds, or every component freezes silently.
        this.failure = undefined; // the engine came back
        this.setConnection("live");
        for (const [subId, reg] of this.subs) {
          reg.sub = undefined;
          void this.register(subId, reg);
        }
      } else {
        this.setConnection("closed");
        this.failure = {
          code: "engine_unavailable",
          message: "the peer engine stopped and could not be restarted",
          status: 503,
        };
        this.failAll(this.failure);
      }
    });
    this.setConnection("live");
    for (const [subId, reg] of this.subs) {
      if (!reg.sub) void this.register(subId, reg);
    }
  }

  private onEngineFailed(err: unknown): void {
    if (this.closed) return;
    this.setConnection("closed");
    this.failure = toQueryError(err);
    this.failAll(this.failure);
  }

  /** Report an error for every live subscription, grouped per ledger so each
   * delivery is still one coherent cycle. */
  private failAll(error: QueryError): void {
    const byLedger = new Map<string, CycleErrored[]>();
    for (const [subId, reg] of this.subs) {
      const list = byLedger.get(reg.spec.ledger) ?? [];
      list.push({ subId, error });
      byLedger.set(reg.spec.ledger, list);
    }
    for (const [ledger, errored] of byLedger) {
      this.sink?.onCycle({ ledger, changed: [], unchanged: [], errored });
    }
  }

  private setConnection(state: ConnectionState): void {
    if (this.connState === state) return;
    this.connState = state;
    this.sink?.onConnection(state);
  }

  /**
   * The two things peer mode cannot answer, checked in one place because
   * they must hold on EVERY public path. Both refusals exist so switching
   * client modes can never change a component's data shape; enforcing one on
   * a path and not the other is worse than having neither, because the half
   * that is missing fails silently.
   */
  private unsupported(spec: ResolvedSpec): QueryError | undefined {
    // The engine has no notion of a past watermark: `snapshot` freezes the
    // CURRENT head, and a subscription is always against the live ledger.
    // Silently serving current data for a time-travel query would be a
    // correctness bug, so say so.
    if (spec.at !== undefined) {
      return {
        code: "unsupported",
        message:
          `peer mode cannot serve a time-anchored query (at: ${spec.at}) — ` +
          "the in-browser engine has no historical view. Use remote mode for time travel.",
      };
    }
    // The engine has no format parameter — a query's results are always its
    // language's — so a different `opts.format` cannot be honoured here.
    if (spec.format !== nativeFormat(spec.kind)) {
      return {
        code: "unsupported",
        message:
          `peer mode cannot serve format "${spec.format}" for a ${spec.kind} query — ` +
          `the engine always produces "${nativeFormat(spec.kind)}". Use remote mode for other formats.`,
      };
    }
    return undefined;
  }

  subscribe(spec: SubscriptionSpec): void {
    if (this.closed) return;

    const refused = this.unsupported(spec);
    if (refused) {
      this.reject(spec, refused);
      return;
    }
    if (this.failure) {
      // The engine is already gone. Storing this registration would leave
      // the component loading forever; nothing will ever drain it.
      this.reject(spec, this.failure);
      return;
    }

    const reg: Registration = { spec, cancelled: false };
    this.subs.set(spec.subId, reg);
    if (this.engine) void this.register(spec.subId, reg);
  }

  /** Deliver a rejection for a spec that was never registered. */
  private reject(spec: SubscriptionSpec, error: QueryError): void {
    // Asynchronously, so a subscribe that fails synchronously still reaches
    // the cache after the caller has finished attaching observers.
    queueMicrotask(() => {
      if (this.closed) return;
      this.sink?.onCycle({
        ledger: spec.ledger,
        changed: [],
        unchanged: [],
        errored: [{ subId: spec.subId, error }],
      });
    });
  }

  private async register(subId: number, reg: Registration): Promise<void> {
    const engine = this.engine;
    if (!engine || reg.cancelled) return;
    try {
      const sub = await engine.subscribe(
        reg.spec.ledger,
        // The engine infers the query language from the argument's type, so
        // a JSON-LD query must go over as an object, not as its text.
        reg.spec.kind === "sparql" ? reg.spec.text : JSON.parse(reg.spec.text),
        () => {},
      );
      if (this.closed || reg.cancelled || this.subs.get(subId) !== reg) {
        void sub.unsubscribe().catch(() => {});
        return;
      }
      reg.sub = sub;
      this.byEngineId.set(sub.subId, subId);
    } catch (err) {
      if (this.closed || reg.cancelled) return;
      this.sink?.onCycle({
        ledger: reg.spec.ledger,
        changed: [],
        unchanged: [],
        errored: [{ subId, error: toQueryError(err) }],
      });
    }
  }

  unsubscribe(subId: number): void {
    const reg = this.subs.get(subId);
    if (!reg) return;
    this.subs.delete(subId);
    reg.cancelled = true;
    if (reg.sub) {
      this.byEngineId.delete(reg.sub.subId);
      void reg.sub.unsubscribe().catch(() => {});
    }
  }

  /** Translate one engine cycle into ours, dropping entries for engine
   * subscriptions this transport does not own (another consumer may share
   * the same peer). */
  private applyCycle(cycle: {
    ledger: string;
    t: number | undefined;
    changed: { subId: number; data: unknown }[];
    unchanged: number[];
    errored: { subId: number; error: PeerError }[];
  }): void {
    if (this.closed) return;
    const changed: CycleChange[] = [];
    const unchanged: number[] = [];
    const errored: CycleErrored[] = [];
    // The engine names ledgers canonically (`demo/board:main`); the app
    // subscribed with whatever it passed to `useQuery` (usually the bare
    // name). Report cycles under the APP's spelling, or everything keyed by
    // ledger downstream — `client.ledgerHead(ledger)` most visibly — is
    // filed under a name the app never uses and silently reads as unknown.
    let ledger: string | undefined;
    const claim = (engineId: number): number | undefined => {
      const subId = this.byEngineId.get(engineId);
      if (subId === undefined) return undefined;
      ledger ??= this.subs.get(subId)?.spec.ledger;
      return subId;
    };

    for (const entry of cycle.changed) {
      const subId = claim(entry.subId);
      if (subId !== undefined) changed.push({ subId, payload: entry.data });
    }
    for (const engineId of cycle.unchanged) {
      const subId = claim(engineId);
      if (subId !== undefined) unchanged.push(subId);
    }
    for (const entry of cycle.errored) {
      const subId = claim(entry.subId);
      if (subId !== undefined) {
        errored.push({ subId, error: toQueryError(entry.error) });
      }
    }
    if (ledger === undefined) {
      return; // nothing in this cycle belongs to us
    }

    const update: CycleUpdate = { ledger, changed, unchanged, errored };
    if (cycle.t !== undefined) update.t = cycle.t;
    this.sink?.onCycle(update);
  }

  async fetchOnce(spec: ResolvedSpec): Promise<unknown> {
    // `LiveClient.query()` routes straight here with the caller's `opts`, so
    // this is a public path and both refusals have to hold on it.
    const refused = this.unsupported(spec);
    if (refused) throw refused;
    if (this.failure) throw this.failure;
    const engine = this.engine ?? (await this.enginePromise);
    if (!engine) throw { code: "closed", message: "peer engine is not available" };
    const ledger = await engine.ledger(spec.ledger);
    return ledger.query(
      spec.kind === "sparql" ? spec.text : JSON.parse(spec.text),
    );
  }

  connectionState(): ConnectionState {
    return this.connState;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.detachCycle?.();
    this.detachState?.();
    this.subs.clear();
    this.byEngineId.clear();
    this.setConnection("closed");
    // The engine may still be connecting; close it whenever it lands.
    if (this.engine) this.engine.close();
    else void this.enginePromise?.then((e) => e.close()).catch(() => {});
  }
}
