/**
 * Remote mode: the `LiveTransport` that needs no wasm. Queries run on the
 * Fluree server over HTTP (`POST /v1/fluree/query/{ledger}`); the server's
 * SSE endpoint (`GET /v1/fluree/events?ledger=...`) announces new commit
 * watermarks per ledger; each announcement triggers an advance-cycle with
 * the same semantics the browser-peer transport implements natively:
 *
 * - coalesce: head events arriving while a cycle is in flight fold into ONE
 *   follow-up cycle at the latest watermark;
 * - re-evaluate every live subscription on that ledger;
 * - diff each result against the previous one CLIENT-side (remote mode has
 *   no worker-side hash gate) via structural sharing — deep-equal results
 *   are reported `unchanged` and carry no payload;
 * - emit ONE `CycleUpdate` for the whole batch.
 *
 * The SSE snapshot the server replays on every (re)connect feeds the same
 * path, so reconnect catch-up is just a cycle whose diff usually reports
 * everything unchanged.
 */

import { replaceEqualDeep } from "../core/structuralShare.js";
import type {
  CycleChange,
  CycleErrored,
  LiveTransport,
  SubscriptionSpec,
  TransportSink,
} from "../core/transport.js";
import type {
  ConnectionState,
  QueryError,
  ResolvedSpec,
} from "../core/types.js";
import { SseSource } from "./sse.js";

export interface RemoteTransportOptions {
  /** Server base URL, e.g. `https://data.example.com` (no trailing slash
   * needed; `/v1/fluree/...` is appended). */
  url: string;
  /** Bearer-token supplier, called per request and per SSE (re)connect —
   * returning a fresh token supports rotation. */
  getToken?: () => string | undefined | Promise<string | undefined>;
  /** Injectable for tests / non-browser runtimes. */
  fetchImpl?: typeof fetch;
  sseRefreshDebounceMs?: number;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
}

/** Keep `/`, `:` and `@` literal in the greedy `*ledger` path segment. */
function encodeLedgerPath(ledger: string): string {
  return encodeURIComponent(ledger)
    .replace(/%2F/gi, "/")
    .replace(/%3A/gi, ":")
    .replace(/%40/gi, "@");
}

function acceptFor(format: string, kind: string): string | undefined {
  if (format === "sparql-json") return "application/sparql-results+json";
  if (format === "jsonld") return "application/json";
  if (format.includes("/")) return format; // raw MIME passthrough
  return kind === "sparql" ? "application/sparql-results+json" : undefined;
}

function toQueryError(err: unknown): QueryError {
  if (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    "message" in err
  ) {
    const e = err as { code: unknown; message: unknown; status?: unknown };
    const out: QueryError = {
      code: String(e.code),
      message: String(e.message),
    };
    if (typeof e.status === "number") out.status = e.status;
    return out;
  }
  return { code: "transport", message: String(err) };
}

export class RemoteTransport implements LiveTransport {
  private readonly base: string;
  private readonly fetchImpl: typeof fetch;
  private readonly getToken: RemoteTransportOptions["getToken"];
  private sink: TransportSink | undefined;
  private sse: SseSource | undefined;
  private readonly sseOptions: Pick<
    RemoteTransportOptions,
    "sseRefreshDebounceMs" | "backoffBaseMs" | "backoffMaxMs"
  >;
  private connState: ConnectionState = "connecting";

  private readonly subs = new Map<number, SubscriptionSpec>();
  /** Live (non-anchored) subscriptions grouped by ledger. */
  private readonly byLedger = new Map<string, Set<number>>();
  /** Previous delivered payload per subscription (the diff baseline). */
  private readonly prev = new Map<number, unknown>();
  /** Latest commit watermark seen per subscribed ledger. */
  private readonly heads = new Map<string, number>();
  /** Coalescing: target watermark for the next cycle per ledger. */
  private readonly pending = new Map<string, number>();
  private readonly running = new Set<string>();
  private closed = false;

  constructor(options: RemoteTransportOptions) {
    this.base = options.url.replace(/\/+$/, "");
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.getToken = options.getToken;
    this.sseOptions = {
      sseRefreshDebounceMs: options.sseRefreshDebounceMs,
      backoffBaseMs: options.backoffBaseMs,
      backoffMaxMs: options.backoffMaxMs,
    };
  }

  start(sink: TransportSink): void {
    this.sink = sink;
    const sseOpts: ConstructorParameters<typeof SseSource>[0] = {
      url: () => this.eventsUrl(),
      headers: async () => {
        const token = await this.getToken?.();
        const headers: Record<string, string> = {};
        if (token) headers.authorization = `Bearer ${token}`;
        return headers;
      },
      onMessage: (msg) => this.handleSse(msg.event, msg.data),
      onState: (state) => {
        this.connState = state;
        sink.onConnection(state);
      },
      fetchImpl: this.fetchImpl,
    };
    if (this.sseOptions.sseRefreshDebounceMs !== undefined) {
      sseOpts.refreshDebounceMs = this.sseOptions.sseRefreshDebounceMs;
    }
    if (this.sseOptions.backoffBaseMs !== undefined) {
      sseOpts.backoffBaseMs = this.sseOptions.backoffBaseMs;
    }
    if (this.sseOptions.backoffMaxMs !== undefined) {
      sseOpts.backoffMaxMs = this.sseOptions.backoffMaxMs;
    }
    this.sse = new SseSource(sseOpts);
  }

  subscribe(spec: SubscriptionSpec): void {
    if (this.closed) return;
    this.subs.set(spec.subId, spec);
    if (spec.at === undefined) {
      let set = this.byLedger.get(spec.ledger);
      if (!set) {
        set = new Set();
        this.byLedger.set(spec.ledger, set);
        this.sse?.refresh(); // watched-ledger set grew
      }
      set.add(spec.subId);
    }
    void this.initialFetch(spec);
  }

  unsubscribe(subId: number): void {
    const spec = this.subs.get(subId);
    if (!spec) return;
    this.subs.delete(subId);
    this.prev.delete(subId);
    if (spec.at === undefined) {
      const set = this.byLedger.get(spec.ledger);
      set?.delete(subId);
      if (set && set.size === 0) {
        this.byLedger.delete(spec.ledger);
        this.heads.delete(spec.ledger);
        this.sse?.refresh(); // watched-ledger set shrank
      }
    }
  }

  async fetchOnce(spec: ResolvedSpec): Promise<unknown> {
    return this.execute(spec);
  }

  connectionState(): ConnectionState {
    return this.connState;
  }

  close(): void {
    this.closed = true;
    this.sse?.close();
    this.subs.clear();
    this.byLedger.clear();
    this.prev.clear();
    this.pending.clear();
  }

  private eventsUrl(): string | null {
    const ledgers = [...this.byLedger.keys()].sort();
    if (ledgers.length === 0) return null;
    const params = ledgers
      .map((l) => `ledger=${encodeURIComponent(l)}`)
      .join("&");
    return `${this.base}/v1/fluree/events?${params}`;
  }

  /** Deliver a new subscription's first result as its own single-entry
   * cycle — one delivery path for everything. */
  private async initialFetch(spec: SubscriptionSpec): Promise<void> {
    try {
      const payload = await this.execute(spec);
      if (!this.subs.has(spec.subId) || this.closed) return;
      this.prev.set(spec.subId, payload);
      this.sink?.onCycle({
        ledger: spec.ledger,
        t: spec.at ?? this.heads.get(spec.ledger) ?? 0,
        changed: [{ subId: spec.subId, payload }],
        unchanged: [],
        errored: [],
      });
    } catch (err) {
      if (!this.subs.has(spec.subId) || this.closed) return;
      this.sink?.onCycle({
        ledger: spec.ledger,
        t: spec.at ?? this.heads.get(spec.ledger) ?? 0,
        changed: [],
        unchanged: [],
        errored: [{ subId: spec.subId, error: toQueryError(err) }],
      });
    }
  }

  private handleSse(event: string, dataText: string): void {
    let data: unknown;
    try {
      data = JSON.parse(dataText);
    } catch {
      return;
    }
    if (typeof data !== "object" || data === null) return;
    const d = data as {
      kind?: unknown;
      resource_id?: unknown;
      record?: { commit_t?: unknown };
    };
    if (d.kind !== "ledger" || typeof d.resource_id !== "string") return;
    const ledger = d.resource_id;
    const set = this.byLedger.get(ledger);
    if (!set || set.size === 0) return;

    if (event === "ns-retracted") {
      // The ledger is gone: error every live subscription on it (data is
      // kept by the cache per keep-last-good-data).
      const error: QueryError = {
        code: "ledger-retracted",
        message: `ledger ${ledger} was retracted`,
      };
      this.sink?.onCycle({
        ledger,
        t: this.heads.get(ledger) ?? 0,
        changed: [],
        unchanged: [],
        errored: [...set].map((subId) => ({ subId, error })),
      });
      return;
    }
    if (event !== "ns-record") return;

    const commitT = d.record?.commit_t;
    if (typeof commitT !== "number") return;
    const known = this.heads.get(ledger);
    if (known !== undefined && commitT <= known) return;
    this.heads.set(ledger, commitT);
    this.scheduleCycle(ledger, commitT);
  }

  private scheduleCycle(ledger: string, t: number): void {
    const existing = this.pending.get(ledger);
    this.pending.set(ledger, existing === undefined ? t : Math.max(existing, t));
    if (this.running.has(ledger)) return; // coalesce into the follow-up
    void this.drainCycles(ledger);
  }

  private async drainCycles(ledger: string): Promise<void> {
    this.running.add(ledger);
    try {
      for (;;) {
        const target = this.pending.get(ledger);
        if (target === undefined || this.closed) return;
        this.pending.delete(ledger);
        await this.runCycle(ledger, target);
      }
    } finally {
      this.running.delete(ledger);
    }
  }

  private async runCycle(ledger: string, t: number): Promise<void> {
    const ids = [...(this.byLedger.get(ledger) ?? [])];
    const specs = ids
      .map((id) => this.subs.get(id))
      .filter((s): s is SubscriptionSpec => s !== undefined);
    if (specs.length === 0) return;

    const settled = await Promise.allSettled(
      specs.map((spec) => this.execute(spec)),
    );
    if (this.closed) return;

    const changed: CycleChange[] = [];
    const unchanged: number[] = [];
    const errored: CycleErrored[] = [];
    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i];
      const outcome = settled[i];
      if (!spec || !outcome || !this.subs.has(spec.subId)) continue;
      if (outcome.status === "fulfilled") {
        const shared = replaceEqualDeep(this.prev.get(spec.subId), outcome.value);
        if (this.prev.has(spec.subId) && shared === this.prev.get(spec.subId)) {
          unchanged.push(spec.subId);
        } else {
          this.prev.set(spec.subId, shared);
          changed.push({ subId: spec.subId, payload: shared });
        }
      } else {
        // Keep the previous payload as the diff baseline so recovery after
        // an error can still report "unchanged".
        errored.push({ subId: spec.subId, error: toQueryError(outcome.reason) });
      }
    }

    this.sink?.onCycle({ ledger, t, changed, unchanged, errored });
  }

  private async execute(spec: ResolvedSpec): Promise<unknown> {
    const ledgerPath =
      spec.at !== undefined ? `${spec.ledger}@t:${spec.at}` : spec.ledger;
    const url = `${this.base}/v1/fluree/query/${encodeLedgerPath(ledgerPath)}`;
    const headers: Record<string, string> = {
      "content-type":
        spec.kind === "sparql" ? "application/sparql-query" : "application/json",
    };
    const accept = acceptFor(spec.format, spec.kind);
    if (accept !== undefined) headers.accept = accept;
    const token = await this.getToken?.();
    if (token) headers.authorization = `Bearer ${token}`;

    const res = await this.fetchImpl(url, {
      method: "POST",
      headers,
      body: spec.text,
    });
    if (!res.ok) {
      let message = `HTTP ${res.status}`;
      let code = "http";
      try {
        const body = (await res.json()) as {
          error?: unknown;
          message?: unknown;
        };
        if (typeof body.message === "string") message = body.message;
        else if (typeof body.error === "string") message = body.error;
        if (typeof body.error === "string") code = body.error;
      } catch {
        // non-JSON error body: keep the status line
      }
      const error: QueryError = { code, message, status: res.status };
      throw error;
    }
    const contentType = res.headers.get("content-type") ?? "";
    return contentType.includes("json") ? res.json() : res.text();
  }
}
