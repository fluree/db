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
  /**
   * How many of a cycle's queries may be in flight at once.
   *
   * Remote mode re-runs EVERY live subscription on a ledger per commit (the
   * v1 invalidation ladder), which on the peer is local CPU but here is one
   * HTTP round-trip each. Firing thirty at once on a busy ledger buries the
   * server and blocks the browser's connection pool for everything else on
   * the page, so the fan-out is bounded. Default 6 — the classic per-host
   * connection limit; raise it on HTTP/2, where requests multiplex.
   */
  maxConcurrency?: number;
}

/** Default cycle fan-out width. See `maxConcurrency`. */
const DEFAULT_MAX_CONCURRENCY = 6;

/**
 * `Promise.allSettled` with a concurrency ceiling. Results stay aligned with
 * `items` by index — the caller pairs them back up with their specs.
 */
async function settleWithLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<PromiseSettledResult<R>[]> {
  const out: PromiseSettledResult<R>[] = new Array(items.length);
  let next = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const i = next++;
      const item = items[i];
      if (i >= items.length || item === undefined) return;
      try {
        out[i] = { status: "fulfilled", value: await fn(item, i) };
      } catch (reason) {
        out[i] = { status: "rejected", reason };
      }
    }
  };
  const width = Math.max(1, Math.min(limit, items.length));
  await Promise.all(Array.from({ length: width }, worker));
  return out;
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
  /** The server's canonical `name:branch` alias per watched ledger. */
  private readonly canonicalAlias = new Map<string, string>();
  private readonly resolvingAlias = new Set<string>();
  /** Coalescing: target watermark for the next cycle per ledger. */
  private readonly pending = new Map<string, number>();
  private readonly running = new Set<string>();
  /**
   * Per-subscription delivery ordering. A subscription's requests can
   * overlap — a commit can land while its very first query is still in
   * flight — and HTTP responses can come back out of order. Every request
   * takes a monotonically increasing ticket, and a response older than what
   * has already been delivered for that subscription is dropped, so a
   * subscription's results can only move forward. Without this, a slow
   * first fetch lands on top of a newer cycle and the component sits on
   * pre-commit data until the next commit happens to arrive.
   */
  private nextTicket = 1;
  private readonly delivered = new Map<number, number>();
  /**
   * The in-flight request per subscription. A superseded or unsubscribed
   * query is aborted rather than left to run to completion server-side with
   * its answer thrown away — otherwise a burst of commits leaves the server
   * computing results nobody will ever read, and holds browser connections
   * while it does.
   */
  private readonly inflight = new Map<number, AbortController>();
  private readonly maxConcurrency: number;
  private closed = false;

  constructor(options: RemoteTransportOptions) {
    this.base = options.url.replace(/\/+$/, "");
    // Bound to the global on purpose. `fetch` is a WebIDL operation on
    // Window: stored as an instance property and called as
    // `this.fetchImpl(...)`, it receives the transport as its receiver and
    // every browser throws "Illegal invocation". Node's fetch does not care,
    // so this is invisible to tests that inject `fetchImpl` — it fails only
    // in a real browser, on every request.
    this.fetchImpl = options.fetchImpl ?? fetch.bind(globalThis);
    this.getToken = options.getToken;
    this.maxConcurrency = options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY;
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
        void this.resolveAlias(spec.ledger); // refreshes again when known
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
    this.delivered.delete(subId);
    this.inflight.get(subId)?.abort();
    this.inflight.delete(subId);
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
    for (const ctrl of this.inflight.values()) ctrl.abort();
    this.inflight.clear();
    this.subs.clear();
    this.byLedger.clear();
    this.prev.clear();
    this.pending.clear();
    this.heads.clear();
    this.delivered.clear();
  }

  /** Claim the in-flight slot for a subscription, aborting whatever request
   * was already occupying it. */
  private beginRequest(subId: number): AbortController {
    this.inflight.get(subId)?.abort();
    const ctrl = new AbortController();
    this.inflight.set(subId, ctrl);
    return ctrl;
  }

  private endRequest(subId: number, ctrl: AbortController): void {
    if (this.inflight.get(subId) === ctrl) this.inflight.delete(subId);
  }

  /** True if a newer response for this subscription has already landed. */
  private outranked(subId: number, ticket: number): boolean {
    const last = this.delivered.get(subId);
    return last !== undefined && last > ticket;
  }

  private markDelivered(subId: number, ticket: number): void {
    const last = this.delivered.get(subId);
    if (last === undefined || ticket > last) this.delivered.set(subId, ticket);
  }

  private eventsUrl(): string | null {
    // The events filter matches the CANONICAL `name:branch` alias, but an
    // app subscribes with whatever it passed to `useQuery` — usually the
    // bare name. Send both: the canonical form once resolved, and the raw
    // form regardless, so a failed or pending resolution degrades to the
    // old behaviour instead of to silence.
    const aliases = new Set<string>();
    for (const ledger of this.byLedger.keys()) {
      aliases.add(ledger);
      const canonical = this.canonicalAlias.get(ledger);
      if (canonical !== undefined) aliases.add(canonical);
    }
    if (aliases.size === 0) return null;
    const params = [...aliases]
      .sort()
      .map((l) => `ledger=${encodeURIComponent(l)}`)
      .join("&");
    return `${this.base}/v1/fluree/events?${params}`;
  }

  /**
   * Ask the server for a ledger's canonical alias.
   *
   * `useQuery("demo/board", …)` queries fine, but the server announces
   * commits for `demo/board:main`, and its events filter compares aliases
   * exactly — so subscribing under the bare name yields a stream that
   * connects, stays open, and never delivers anything. Rather than assuming
   * a default branch name, ask: `/info/{ledger}` reports the alias. On
   * failure we keep the raw name and lose nothing we had.
   */
  private async resolveAlias(ledger: string): Promise<void> {
    if (this.canonicalAlias.has(ledger) || this.resolvingAlias.has(ledger)) return;
    this.resolvingAlias.add(ledger);
    try {
      const headers: Record<string, string> = { accept: "application/json" };
      const token = await this.getToken?.();
      if (token) headers.authorization = `Bearer ${token}`;
      const res = await this.fetchImpl(
        `${this.base}/v1/fluree/info/${encodeLedgerPath(ledger)}`,
        { headers },
      );
      if (res.ok) {
        const body = (await res.json()) as { ledger?: { alias?: unknown } };
        const alias = body.ledger?.alias;
        if (typeof alias === "string" && alias !== "") {
          this.canonicalAlias.set(ledger, alias);
        }
      }
    } catch {
      // Keep the raw alias; the stream still opens under it.
    } finally {
      this.resolvingAlias.delete(ledger);
      if (!this.closed) this.sse?.refresh();
    }
  }

  /** Deliver a new subscription's first result as its own single-entry
   * cycle — one delivery path for everything. */
  private async initialFetch(spec: SubscriptionSpec): Promise<void> {
    // A t-anchored query is pinned to its anchor; otherwise the best we can
    // honestly say is the newest head SSE has announced for this ledger,
    // which is `undefined` until the first head event arrives.
    const t = spec.at ?? this.heads.get(spec.ledger);
    const ticket = this.nextTicket++;
    const ctrl = this.beginRequest(spec.subId);
    try {
      const payload = await this.execute(spec, ctrl.signal);
      this.endRequest(spec.subId, ctrl);
      if (!this.subs.has(spec.subId) || this.closed) return;
      if (this.outranked(spec.subId, ticket)) return;
      this.markDelivered(spec.subId, ticket);
      this.prev.set(spec.subId, payload);
      this.sink?.onCycle({
        ledger: spec.ledger,
        t,
        changed: [{ subId: spec.subId, payload }],
        unchanged: [],
        errored: [],
      });
    } catch (err) {
      this.endRequest(spec.subId, ctrl);
      // An abort is this transport's own decision, not a failure to report.
      if (ctrl.signal.aborted) return;
      if (!this.subs.has(spec.subId) || this.closed) return;
      if (this.outranked(spec.subId, ticket)) return;
      this.markDelivered(spec.subId, ticket);
      this.sink?.onCycle({
        ledger: spec.ledger,
        t,
        changed: [],
        unchanged: [],
        errored: [{ subId: spec.subId, error: toQueryError(err) }],
      });
    }
  }

  /**
   * Resolve an event's `resource_id` to EVERY ledger key this transport is
   * watching under.
   *
   * The server announces the NORMALIZED id — `name:branch`, e.g.
   * `demo/board:main` — but an application subscribes with whatever string
   * it passed to `useQuery`, which is usually the bare name whose branch the
   * server filled in. Comparing them directly silently drops every head
   * event: the subscription stays open, nothing errors, and no query ever
   * updates. So try the id as given, AND the id with its branch suffix
   * removed. (Deriving the default branch name instead would be a guess;
   * this needs none.)
   *
   * Both, not the first match: one page can perfectly well hold
   * `useQuery("demo/board", …)` and `useQuery("demo/board:main", …)`, and
   * returning only the exact hit leaves the bare-name subscription open,
   * reporting `live`, erroring nothing, and never updating again — the same
   * silence this mapping exists to remove, one case over.
   */
  private watchedLedgersFor(resourceId: string): string[] {
    const out: string[] = [];
    if (this.byLedger.has(resourceId)) out.push(resourceId);
    const cut = resourceId.lastIndexOf(":");
    if (cut > 0) {
      const base = resourceId.slice(0, cut);
      if (this.byLedger.has(base)) out.push(base);
    }
    return out;
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
    const ledgers = this.watchedLedgersFor(d.resource_id);
    if (ledgers.length === 0) return;

    if (event === "ns-retracted") {
      // The ledger is gone: error every live subscription on it (data is
      // kept by the cache per keep-last-good-data).
      for (const ledger of ledgers) {
        const set = this.byLedger.get(ledger);
        if (!set || set.size === 0) continue;
        const error: QueryError = {
          code: "ledger-retracted",
          message: `ledger ${ledger} was retracted`,
        };
        const retractedAt = this.heads.get(ledger);
        this.sink?.onCycle({
          ledger,
          t: retractedAt,
          changed: [],
          unchanged: [],
          errored: [...set].map((subId) => ({ subId, error })),
        });
      }
      return;
    }
    if (event !== "ns-record") return;

    const commitT = d.record?.commit_t;
    if (typeof commitT !== "number") return;
    for (const ledger of ledgers) {
      const set = this.byLedger.get(ledger);
      if (!set || set.size === 0) continue;
      const known = this.heads.get(ledger);
      if (known !== undefined && commitT <= known) continue;
      this.heads.set(ledger, commitT);
      this.scheduleCycle(ledger, commitT);
    }
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

    const tickets = specs.map(() => this.nextTicket++);
    const ctrls = specs.map((spec) => this.beginRequest(spec.subId));
    const settled = await settleWithLimit(specs, this.maxConcurrency, (spec, i) =>
      this.execute(spec, ctrls[i]?.signal),
    );
    specs.forEach((spec, i) => {
      const ctrl = ctrls[i];
      if (ctrl) this.endRequest(spec.subId, ctrl);
    });
    if (this.closed) return;

    const changed: CycleChange[] = [];
    const unchanged: number[] = [];
    const errored: CycleErrored[] = [];
    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i];
      const outcome = settled[i];
      const ticket = tickets[i];
      if (!spec || !outcome || ticket === undefined) continue;
      if (ctrls[i]?.signal.aborted) continue; // superseded on purpose
      if (!this.subs.has(spec.subId)) continue;
      if (this.outranked(spec.subId, ticket)) continue;
      this.markDelivered(spec.subId, ticket);
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

  private async execute(spec: ResolvedSpec, signal?: AbortSignal): Promise<unknown> {
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
      ...(signal ? { signal } : {}),
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
