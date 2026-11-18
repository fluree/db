/**
 * A scriptable stand-in for `Worker`, installed as `globalThis.Worker` so
 * `connect()`/`playground()` (index.ts) spawn it instead of a real dedicated
 * worker. Ported from the round-2 adversarial-review prototype
 * (`internal-reviews/round2-shell-memo.test.mjs`), which proved index.ts is
 * unit-testable this way with zero production changes — `workerUrl` in the
 * options bypasses the bundler-detected `new Worker(new URL(...))` literal,
 * and index.ts imports nothing from the generated `pkg/` glue, so none of
 * this needs a wasm build.
 *
 * Default auto-replies (queued as a microtask, mimicking a real worker's
 * async postMessage) cover only "init"/"subscribe"/"unsubscribe" with an ok
 * reply — the ops index.ts always needs answered just to make progress.
 * Every other op (query, transact, ledgerInfo, createLedger, snapshot,
 * release, debugCrash, unsubscribe) is left pending for the test to answer
 * explicitly via `reply()`/`replyToLatest()`, and any op — including init —
 * can be scripted per-instance via `scriptNext()` before it spawns.
 */
type MessageListener = ((ev: { data: any }) => void) | null;
type ErrorListener = ((ev: { message?: string }) => void) | null;
type Responder = (msg: any, worker: StubWorker) => boolean;

export class StubWorker {
  static instances: StubWorker[] = [];
  private static nextOverride: Responder | null = null;
  private static nextSubId = 1;

  readonly url: string | URL;
  readonly opts: unknown;
  onmessage: MessageListener = null;
  onerror: ErrorListener = null;
  onmessageerror: (() => void) | null = null;
  readonly posted: any[] = [];
  terminated = false;
  private readonly respondOverride: Responder | null;

  constructor(url: string | URL, opts?: unknown) {
    this.url = url;
    this.opts = opts;
    this.respondOverride = StubWorker.nextOverride;
    StubWorker.nextOverride = null;
    StubWorker.instances.push(this);
  }

  /** Script the response for the very next `new Worker(...)` — set this
   * before whatever triggers that spawn (a `connect()`/`playground()` call,
   * or advancing the fake-timer respawn delay past a scheduled one).
   * Consumed once. Return `true` from `fn` to mean "handled, skip the
   * default auto-reply"; `false` falls through to it. */
  static scriptNext(fn: Responder): void {
    StubWorker.nextOverride = fn;
  }

  static reset(): void {
    StubWorker.instances = [];
    StubWorker.nextOverride = null;
    StubWorker.nextSubId = 1;
  }

  postMessage(msg: any): void {
    this.posted.push(msg);
    queueMicrotask(() => {
      if (this.respondOverride?.(msg, this)) return;
      this.autoRespond(msg);
    });
  }

  terminate(): void {
    this.terminated = true;
  }

  /** Reply to one pending request by id. */
  reply(id: number, res: { ok: true; result?: unknown } | { ok: false; error: unknown }): void {
    this.onmessage?.({ data: { id, ...res } });
  }

  /** Reply to the most recently posted message with the given op. Throws if
   * none is pending — answering a message that was never sent is a test
   * bug, not a silent no-op. */
  replyToLatest(op: string, res: { ok: true; result?: unknown } | { ok: false; error: unknown }): any {
    const msg = findLatest(this.posted, (m) => m.op === op);
    if (!msg) throw new Error(`StubWorker: no posted "${op}" message to reply to`);
    this.reply(msg.id, res);
    return msg;
  }

  /** Push an unsolicited event (the `EventMessage` envelope). */
  emit(eventMsg: { v: 1; event: unknown; payloads?: ArrayBuffer[] }): void {
    this.onmessage?.({ data: eventMsg });
  }

  /** Fire a worker-level `error` event (distinct from a fatal reply). */
  fail(message: string): void {
    this.onerror?.({ message });
  }

  private autoRespond(msg: any): void {
    if (msg.op === "init") {
      this.reply(msg.id, { ok: true, result: { version: "test", mode: msg.mode } });
    } else if (msg.op === "subscribe") {
      this.reply(msg.id, { ok: true, result: { subId: StubWorker.nextSubId++ } });
    } else if (msg.op === "unsubscribe") {
      this.reply(msg.id, { ok: true, result: true });
    }
    // Everything else is intentionally left unanswered by default.
  }
}

/** Install the stub as `globalThis.Worker` (and reset its instance registry)
 * for one test, and hand back a restorer. Call from `beforeEach`/`afterEach`
 * so tests never leak a stub — or a stray instance — across files. */
export function installStubWorker(): () => void {
  const prior = (globalThis as { Worker?: unknown }).Worker;
  StubWorker.reset();
  (globalThis as { Worker: unknown }).Worker = StubWorker;
  return () => {
    (globalThis as { Worker?: unknown }).Worker = prior;
  };
}

/** Last array element matching `pred` — a `findLast` that doesn't need the
 * ES2023 lib (this package's tsconfig targets ES2022). */
export function findLatest<T>(arr: readonly T[], pred: (t: T) => boolean): T | undefined {
  for (let i = arr.length - 1; i >= 0; i--) {
    const item = arr[i] as T;
    if (pred(item)) return item;
  }
  return undefined;
}
