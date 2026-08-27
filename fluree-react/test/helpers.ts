/**
 * Shared test doubles: a fully scriptable `LiveTransport` (for core and
 * adapter tests), a recording `TransportSink` (for transport tests), and a
 * mock HTTP/SSE server (for the remote transport and the SSE parser).
 */

import type {
  CycleUpdate,
  LiveTransport,
  SubscriptionSpec,
  TransportSink,
} from "../src/core/transport.js";
import type { ConnectionState, ResolvedSpec } from "../src/core/types.js";

export class FakeTransport implements LiveTransport {
  sink: TransportSink | undefined;
  readonly subscribes: SubscriptionSpec[] = [];
  readonly unsubscribes: number[] = [];
  readonly oneShots: ResolvedSpec[] = [];
  oneShotResult: unknown = { ok: true };
  private state: ConnectionState = "connecting";
  closed = false;

  start(sink: TransportSink): void {
    this.sink = sink;
  }

  subscribe(spec: SubscriptionSpec): void {
    this.subscribes.push(spec);
  }

  unsubscribe(subId: number): void {
    this.unsubscribes.push(subId);
  }

  async fetchOnce(spec: ResolvedSpec): Promise<unknown> {
    this.oneShots.push(spec);
    return this.oneShotResult;
  }

  connectionState(): ConnectionState {
    return this.state;
  }

  close(): void {
    this.closed = true;
  }

  emit(cycle: CycleUpdate): void {
    this.sink?.onCycle(cycle);
  }

  setConnection(state: ConnectionState): void {
    this.state = state;
    this.sink?.onConnection(state);
  }

  /** subId assigned to the nth transport subscription (creation order). */
  subId(n: number): number {
    const spec = this.subscribes[n];
    if (!spec) throw new Error(`no subscription #${n}`);
    return spec.subId;
  }
}

export interface RecordingSink extends TransportSink {
  cycles: CycleUpdate[];
  states: ConnectionState[];
}

export function recordingSink(): RecordingSink {
  const sink: RecordingSink = {
    cycles: [],
    states: [],
    onCycle(cycle) {
      sink.cycles.push(cycle);
    },
    onConnection(state) {
      sink.states.push(state);
    },
  };
  return sink;
}

// ---------------------------------------------------------------------------
// HTTP / SSE doubles
// ---------------------------------------------------------------------------

/** A pushable `text/event-stream` body. Aborting the request errors the
 * stream, the way a real `fetch` does — otherwise a reader would hang. */
export function sseStream(signal?: AbortSignal) {
  let ctrl!: ReadableStreamDefaultController<Uint8Array>;
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      ctrl = c;
    },
  });
  const encoder = new TextEncoder();
  let done = false;
  const finish = (fn: () => void) => {
    if (done) return;
    done = true;
    fn();
  };
  signal?.addEventListener("abort", () =>
    finish(() => ctrl.error(new Error("aborted"))),
  );
  return {
    stream,
    push(text: string) {
      if (!done) ctrl.enqueue(encoder.encode(text));
    },
    /** Push a well-formed SSE frame. */
    frame(event: string, data: unknown, id?: string) {
      const lines = [`event: ${event}`, `data: ${JSON.stringify(data)}`];
      if (id !== undefined) lines.push(`id: ${id}`);
      this.push(`${lines.join("\n")}\n\n`);
    },
    end: () => finish(() => ctrl.close()),
    fail: (err: unknown) => finish(() => ctrl.error(err)),
  };
}

export type SseStream = ReturnType<typeof sseStream>;

export interface QueryCall {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string;
}

export interface HttpResult {
  status?: number;
  contentType?: string;
  /** Body: an object/array is JSON-encoded, a string is sent verbatim. */
  body?: unknown;
}

function makeResponse(result: HttpResult): Response {
  const status = result.status ?? 200;
  const contentType = result.contentType ?? "application/json";
  const text =
    typeof result.body === "string"
      ? result.body
      : JSON.stringify(result.body ?? {});
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers({ "content-type": contentType }),
    json: async () => JSON.parse(text) as unknown,
    text: async () => text,
  } as unknown as Response;
}

/**
 * Mock Fluree server: routes `POST /v1/fluree/query/*` to a scripted
 * responder and `GET /v1/fluree/events` to a pushable SSE stream.
 */
export class MockServer {
  readonly queries: QueryCall[] = [];
  readonly eventConnects: Array<{ url: string; headers: Record<string, string> }> =
    [];
  /** Replaceable per test; receives the recorded call. */
  respond: (call: QueryCall, n: number) => HttpResult | Promise<HttpResult> = () => ({
    body: { head: { vars: [] }, results: { bindings: [] } },
  });
  /** Set to fail the NEXT events connect (drives reconnect tests). */
  failEvents = 0;
  streams: SseStream[] = [];

  get stream(): SseStream {
    const s = this.streams.at(-1);
    if (!s) throw new Error("no SSE stream connected yet");
    return s;
  }

  readonly fetchImpl: typeof fetch = async (input, init) => {
    const url = String(input);
    const headers = Object.fromEntries(
      Object.entries((init?.headers ?? {}) as Record<string, string>).map(
        ([k, v]) => [k.toLowerCase(), v],
      ),
    );
    if (url.includes("/v1/fluree/events")) {
      this.eventConnects.push({ url, headers });
      if (this.failEvents > 0) {
        this.failEvents--;
        return makeResponse({ status: 503, body: "unavailable" });
      }
      const s = sseStream(init?.signal ?? undefined);
      this.streams.push(s);
      return {
        ok: true,
        status: 200,
        headers: new Headers({ "content-type": "text/event-stream" }),
        body: s.stream,
      } as unknown as Response;
    }
    const call: QueryCall = {
      url,
      method: init?.method ?? "GET",
      headers,
      body: String(init?.body ?? ""),
    };
    const n = this.queries.length;
    this.queries.push(call);
    return makeResponse(await this.respond(call, n));
  };
}

// ---------------------------------------------------------------------------
// Peer-engine double
// ---------------------------------------------------------------------------

import type {
  PeerCycle,
  PeerEngine,
  PeerEngineState,
  PeerLedger,
  PeerSubscription,
} from "../src/peer/peerEngine.js";

export interface FakeSubscribeCall {
  ledger: string;
  query: string | object;
  subId: number;
}

/**
 * Scriptable stand-in for `@fluree/db-wasm`'s `Peer`. Registration can be
 * held open (`holdRegistrations`) to drive the async-registration races the
 * real worker boundary produces.
 */
export class FakePeerEngine implements PeerEngine {
  readonly subscribes: FakeSubscribeCall[] = [];
  readonly unsubscribes: number[] = [];
  readonly queries: Array<{ ledger: string; query: string | object }> = [];
  queryResult: unknown = { ok: true };
  closed = false;
  /** When true, `subscribe` promises stay pending until `releaseAll()`. */
  holdRegistrations = false;
  /** Set to make the next `subscribe` reject. */
  failNextSubscribe: unknown;

  private nextSubId = 100;
  private readonly cycleListeners = new Set<(c: PeerCycle) => void>();
  private readonly stateListeners = new Set<(s: PeerEngineState) => void>();
  private readonly held: Array<() => void> = [];

  async ledger(id: string): Promise<PeerLedger> {
    return {
      query: async (query: string | object) => {
        this.queries.push({ ledger: id, query });
        return this.queryResult;
      },
    };
  }

  subscribe(
    ledger: string,
    query: string | object,
    _onUpdate: () => void,
  ): Promise<PeerSubscription> {
    if (this.failNextSubscribe !== undefined) {
      const err = this.failNextSubscribe;
      this.failNextSubscribe = undefined;
      return Promise.reject(err);
    }
    const subId = this.nextSubId++;
    this.subscribes.push({ ledger, query, subId });
    const sub: PeerSubscription = {
      subId,
      unsubscribe: async () => {
        this.unsubscribes.push(subId);
      },
    };
    if (!this.holdRegistrations) return Promise.resolve(sub);
    return new Promise((resolve) => this.held.push(() => resolve(sub)));
  }

  /** Resolve every held registration. */
  releaseAll(): void {
    const pending = this.held.splice(0);
    for (const release of pending) release();
  }

  onCycle(listener: (cycle: PeerCycle) => void): () => void {
    this.cycleListeners.add(listener);
    return () => this.cycleListeners.delete(listener);
  }

  onEngineState(listener: (state: PeerEngineState) => void): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  close(): void {
    this.closed = true;
  }

  emitCycle(cycle: PeerCycle): void {
    for (const listener of [...this.cycleListeners]) listener(cycle);
  }

  emitState(state: PeerEngineState): void {
    for (const listener of [...this.stateListeners]) listener(state);
  }

  /** Engine subId assigned to the nth subscribe (registration order). */
  subId(n: number): number {
    const call = this.subscribes[n];
    if (!call) throw new Error(`no engine subscription #${n}`);
    return call.subId;
  }
}
