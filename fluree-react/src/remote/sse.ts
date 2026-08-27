/**
 * Minimal fetch-based SSE source. `EventSource` cannot send an
 * `Authorization` header (auth via `getToken` is part of the v1 contract),
 * so this reads `text/event-stream` off a fetch body and parses frames per
 * the SSE spec: `event:`/`data:`/`id:` fields, multi-line data joined with
 * newlines, dispatch on blank line, `:` comment lines (the server's
 * keep-alives) ignored.
 *
 * Reconnects with capped exponential backoff + jitter, sending
 * `Last-Event-ID` (CORS-allowed by the server). The server replays a full
 * snapshot of subscribed records on every connect, which doubles as
 * catch-up after a drop. `refresh()` re-resolves the URL (the subscribed
 * ledger set changed) — debounced so a burst of mounting components
 * coalesces into one connection.
 */

export interface SseMessage {
  event: string;
  data: string;
  id?: string;
}

export type SseState = "connecting" | "live" | "reconnecting" | "closed";

export interface SseSourceOptions {
  /** Resolve the current stream URL; `null` means nothing to watch. */
  url: () => string | null;
  /** Extra request headers (auth) — re-resolved on every (re)connect. */
  headers: () => Promise<Record<string, string>>;
  onMessage: (msg: SseMessage) => void;
  onState: (state: SseState) => void;
  fetchImpl?: typeof fetch;
  refreshDebounceMs?: number;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class SseSource {
  private readonly opts: SseSourceOptions;
  private readonly fetchImpl: typeof fetch;
  private readonly refreshDebounceMs: number;
  private readonly backoffBaseMs: number;
  private readonly backoffMaxMs: number;
  private generation = 0;
  private ctrl: AbortController | undefined;
  private debounce: ReturnType<typeof setTimeout> | undefined;
  private lastEventId: string | undefined;
  private closed = false;

  constructor(opts: SseSourceOptions) {
    this.opts = opts;
    this.fetchImpl = opts.fetchImpl ?? fetch;
    this.refreshDebounceMs = opts.refreshDebounceMs ?? 20;
    this.backoffBaseMs = opts.backoffBaseMs ?? 1000;
    this.backoffMaxMs = opts.backoffMaxMs ?? 15_000;
  }

  /** The watched set (or auth) changed: reconnect with a fresh URL. */
  refresh(): void {
    if (this.closed) return;
    if (this.debounce !== undefined) clearTimeout(this.debounce);
    this.debounce = setTimeout(() => {
      this.debounce = undefined;
      if (this.closed) return;
      this.generation++;
      this.ctrl?.abort();
      const url = this.opts.url();
      if (url === null) return; // idle: nothing subscribed
      void this.runLoop(this.generation, url);
    }, this.refreshDebounceMs);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.debounce !== undefined) clearTimeout(this.debounce);
    this.generation++;
    this.ctrl?.abort();
    this.opts.onState("closed");
  }

  private stale(gen: number): boolean {
    return this.closed || gen !== this.generation;
  }

  private async runLoop(gen: number, url: string): Promise<void> {
    let attempt = 0;
    while (!this.stale(gen)) {
      try {
        const ctrl = new AbortController();
        this.ctrl = ctrl;
        const headers: Record<string, string> = {
          accept: "text/event-stream",
          ...(await this.opts.headers()),
        };
        if (this.lastEventId !== undefined) {
          headers["last-event-id"] = this.lastEventId;
        }
        const res = await this.fetchImpl(url, {
          headers,
          signal: ctrl.signal,
        });
        if (!res.ok || !res.body) {
          throw new Error(`SSE connect failed: HTTP ${res.status}`);
        }
        attempt = 0;
        if (!this.stale(gen)) this.opts.onState("live");
        await this.readStream(res.body, gen);
        // Server closed the stream: fall through to reconnect.
        throw new Error("SSE stream ended");
      } catch {
        if (this.stale(gen)) return;
        this.opts.onState("reconnecting");
        attempt++;
        const cap = Math.min(
          this.backoffMaxMs,
          this.backoffBaseMs * 2 ** (attempt - 1),
        );
        await sleep(cap * (0.5 + Math.random() * 0.5));
      }
    }
  }

  private async readStream(
    body: ReadableStream<Uint8Array>,
    gen: number,
  ): Promise<void> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let event = "";
    let data: string[] = [];
    let id: string | undefined;

    const dispatch = () => {
      if (data.length > 0) {
        if (id !== undefined) this.lastEventId = id;
        const msg: SseMessage = {
          event: event || "message",
          data: data.join("\n"),
        };
        if (id !== undefined) msg.id = id;
        this.opts.onMessage(msg);
      }
      event = "";
      data = [];
      id = undefined;
    };

    for (;;) {
      const { done, value } = await reader.read();
      if (done || this.stale(gen)) return;
      buffer += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        let line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);
        if (line === "") {
          dispatch();
        } else if (line.startsWith(":")) {
          // keep-alive comment
        } else {
          const colon = line.indexOf(":");
          const field = colon < 0 ? line : line.slice(0, colon);
          let value = colon < 0 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          if (field === "event") event = value;
          else if (field === "data") data.push(value);
          else if (field === "id") id = value;
          // "retry" and unknown fields ignored
        }
      }
    }
  }
}
