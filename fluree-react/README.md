# @fluree/react

React hooks for live Fluree queries. A component subscribes to one query and
re-renders only when **that query's results actually change** — no polling,
no cache-invalidation keys, no manual refetching. The database's own commit
stream is the invalidation signal.

```tsx
function People() {
  const { data, status } = useQuery(
    "my/ledger",
    "SELECT ?name WHERE { ?s <schema:name> ?name }",
  );
  if (status === "loading") return <Spinner />;
  return <ul>{data.results.bindings.map((b) => <Row key={b.name.value} b={b} />)}</ul>;
}
```

When someone commits to `my/ledger`, this list updates. There is no other
code. If the commit did not change what this query returns, the component
does not re-render at all — and when it does, the rows that did not change
keep their object identity, so `React.memo` down the tree keeps working.

> **Status:** pre-release (`0.0.0-dev`). Both modes are implemented and
> tested against scripted doubles. See
> [Verification status](#verification-status) — it is deliberately blunt
> about what has and has not been run for real.

## Two modes, one API

The hooks are identical in both. Only `createClient` differs — in the demo
app, that is literally the only line that changes.

**Remote mode** — no wasm. Queries execute on a Fluree server over HTTP; the
server's SSE endpoint announces new commit watermarks; each announcement
triggers a re-query cycle. Works against any Fluree server with query +
events, renders on the server (SSR/Next.js), and adds nothing to your bundle
beyond this package.

```ts
import { createClient } from "@fluree/react";

const client = createClient({
  url: "https://data.example.com",
  getToken: () => auth.accessToken, // optional; called per request, so rotation works
});
```

**Peer mode** — the headline. A wasm engine in a web worker hears the same
commit stream, applies it locally, and re-runs the affected queries against
a frozen snapshot at memory speed, hashing results worker-side so unchanged
subscriptions cross the boundary with no payload at all.

```ts
import { connect } from "@fluree/db-wasm";
import { createClient } from "@fluree/react";

const client = createClient({
  peer: {
    connect,                              // passed in, not imported — see below
    url: "https://data.example.com/v1/fluree",
    getToken: () => auth.accessToken,
  },
});
```

`connect` is a parameter rather than an import so that **`@fluree/react` has
no dependency on `@fluree/db-wasm`**: remote mode installs, bundles, and
server-renders without a wasm engine anywhere near it, and peer mode is
whatever your bundler does with one dynamic import.

Remote mode is the on-ramp: an app written against these hooks today gains
peer mode by changing that one call, because the transport contract and the
result shapes are identical in both.

### What differs between the modes

Two things the in-browser engine genuinely cannot do. Both **fail loudly**
— the query reports `status: "error"` with code `unsupported` — rather than
quietly returning something of the wrong shape or the wrong age.

| | remote | peer |
| --- | --- | --- |
| `opts.at` (time travel) | yes | **no** — the engine has no historical view |
| `opts.format` other than the query language's own | yes | **no** — the engine always produces its language's format |
| writes | your app's own HTTP calls | same (the peer is read-only) |
| `t` before the first commit event | `undefined` | known from the first cycle |

Everything else — result shapes, identity stability, error semantics,
dedup, GC — is identical by construction.

## API

### `createClient(config)`

| Field | Mode | Meaning |
| --- | --- | --- |
| `url` | remote | Server base URL. `/v1/fluree/...` is appended. |
| `getToken` | remote | `() => string \| undefined \| Promise<...>`, called per request and per SSE reconnect. |
| `fetchImpl` | remote | Override `fetch` (tests, non-browser runtimes). |
| `sseRefreshDebounceMs`, `backoffBaseMs`, `backoffMaxMs` | remote | SSE reconnect tuning. |
| `maxConcurrency` | remote | How many of a cycle's queries may be in flight at once. Default 6. |
| `peer.connect` | peer | `connect` from `@fluree/db-wasm`. |
| `peer.url` | peer | The versioned API base, e.g. `https://host/v1/fluree`. |
| `peer.getToken` | peer | `(reason) => string \| Promise<string>`; asked on connect and after a crash recycle. |
| `peer.watch` | peer | Ledgers to head-track. Default `[]` = everything the token may see. A narrower list silently makes untracked queries non-live, so narrow it only when you know every ledger the app queries. |
| `peer.workerUrl`, `peer.wasmUrl`, `peer.maxMemoryBytes` | peer | Passed through to the engine. |
| `transport` | custom | A raw `LiveTransport`. |
| `gcTime` | both | Grace period (ms) before an unobserved query's subscription is released. Default 30 000. |

Create it once, outside your component tree, and keep it stable.

### `<FlureeProvider client={client}>`

Injects the client. Context carries the **client only** — data never flows
through context, so the provider itself never causes a re-render.

### `useQuery(ledger, query, opts?)`

```ts
const { data, status, error, t } = useQuery<T>(ledger, query, opts);
```

- `query` — a **string** is treated as SPARQL, an **object** as a JSON-LD
  (FlureeQL) query. An inline object literal is fine: queries are keyed by
  their serialized text, so a fresh literal each render reuses the same
  subscription.
- `status` — `"loading" | "ready" | "error"`.
- `data` — the last good result, `undefined` until the first one arrives.
  **Kept on error** (see below).
- `error` — `{ code, message, status? }` when `status === "error"`.
- `t` — the commit watermark `data` was computed at. See
  [Watermarks](#watermarks-t) for the two cases where it is `undefined`.

`opts`:

| Option | Meaning |
| --- | --- |
| `kind` | `"sparql" \| "jsonld"`, overriding inference from the query's type. |
| `format` | Result format. Defaults are **language-matched**: a SPARQL query returns SPARQL JSON results, a JSON-LD query returns formatted JSON-LD — exactly what the HTTP API returns for that language. A MIME-looking string (`"text/turtle"`) is passed through as `Accept`. |
| `at` | Anchor at a fixed watermark (time travel). A `t`-anchored query is fetched once and **never invalidated** by new commits. |
| `gcTime` | Per-query override of the client's grace period. Applied when the query's cache entry is first created. |

### `useConnectionState()`

`"connecting" | "live" | "reconnecting" | "closed"`. Useful for an
offline/stale badge. It is a separate subscription, so connection churn
never re-renders your query components.

### `client.query(ledger, query, opts?)`

A one-shot `Promise`, outside the subscription system — for imperative
code (event handlers, loaders). No cache entry, no subscription.

### `client.ledgerHead(ledger)` / `client.close()`

The newest watermark any cycle has reported for a ledger, and a full
teardown (releases every subscription and the SSE stream).

## How it behaves

**Deduplication.** Every component asking the same question shares one
subscription and one in-flight request. "Same" means same ledger, query
text, kind, format, and time anchor.

**Unmount is cheap, remount is instant.** When the last component observing
a query unmounts, the subscription is held open for `gcTime` (30s). A
remount inside that window renders the current data immediately with no
refetch and no loading flash — which is also what makes React StrictMode's
double-mount and list virtualization free.

**Errors keep the last good data.** On failure, `status` flips to `"error"`
and `error` is populated, but `data` and its watermark survive — identity
and all. Your UI does not blank out because one poll hit a 503. The next
successful cycle clears the error, and if the results came back identical,
that recovery costs zero re-renders.

**Version coherence.** One commit produces one advance-cycle. Every
subscription in that cycle is updated *before any component is notified*, so
two sibling components can never render data from different watermarks.

**Structural sharing.** Results arrive as freshly-parsed JSON — over HTTP in
remote mode, and in peer mode because `postMessage` clones. Referential
stability is therefore reconstructed on receipt: the new result is merged
against the previous one so unchanged branches keep their exact objects. If
*nothing* changed, the previous result object is returned unchanged and no
re-render happens at all. This is what makes `React.memo` and `useMemo`
dependencies work on rows.

**Superseded work is cancelled (remote).** A commit that lands while a
query's request is still open aborts that request rather than letting the
server finish an answer nobody will read. And because remote mode re-runs
*every* live subscription on a ledger per commit, the fan-out is bounded
(`maxConcurrency`, default 6) so a page with thirty subscriptions does not
fire thirty simultaneous requests and starve everything else in the browser's
connection pool.

**Engine crashes are visible (peer).** A wasm engine that traps is recycled,
and a recycled engine has no subscriptions — nothing errors, and no cycle
ever arrives again. The peer transport re-registers every subscription when
the engine comes back (`useConnectionState()` reports `reconnecting` then
`live` meanwhile), and if the engine gives up for good, every query flips to
`status: "error"` so the UI can say the data has stopped updating instead of
showing it forever.

### Watermarks (`t`)

`t` is the watermark `data` was computed at, and it is deliberately *not*
bumped when a cycle re-runs the query at a newer head and finds the results
identical — replacing the snapshot just to change a number would defeat the
whole point. The data is still exactly what the newer head returns. Use
`client.ledgerHead(ledger)` for the ledger's latest observed watermark.

In remote mode `t` is `undefined` until the first SSE head event arrives,
because a query response does not tell us which watermark it ran at. Peer
mode always knows it. Do not render `t` as "the current state of the
database"; render it as "the version of this answer".

## Server rendering

A server render produces the loading snapshot: no subscribe, no HTTP
request, no SSE connect. The client takes over after hydration. A server
render does create a cache entry, which collects itself after `gcTime` — so
prefer a per-request client, or a short `gcTime`, in a server process.

## Verification status

Written out plainly, because "the tests pass" and "this works against
Fluree" are different claims and only the first is true today.

**Executed** — 171 tests (`npx vitest run`), all running real package code:

| Suite | Tests | What it actually proves |
| --- | --- | --- |
| `structuralShare` | 11 | Unchanged rows keep **object identity** across an advance; an all-equal result returns the previous object itself. Every assertion is `toBe`, not `toEqual` — a deep-equality test here would pass against an implementation that shares nothing. |
| `queryCache` | 23 | Dedup by key; the transport subscription opens on the first observer and is released only after `gcTime` with none; the janitor collects never-observed handles; **version coherence** (observers verify the whole cache already carries the new watermark at notify time); keep-last-good-data and both recovery paths; an unchanged cycle produces the identical snapshot object and no notification. |
| `liveClient` | 13 | Query-kind inference and the language-matched format defaults; cache keying; one-shot queries; connection fan-out; teardown releases timers. |
| `useQuery` (react-dom + jsdom) | 19 | Real components: a memoized row does **not** re-render when a sibling row changes; an unchanged cycle costs zero renders; `getSnapshot` is referentially stable across unrelated parent re-renders (a fresh object per call is the canonical infinite-loop bug with `useSyncExternalStore`); StrictMode double-mount keeps one subscription; siblings advance in lock-step. |
| `sse` | 19 | The hand-rolled frame parser against a real `ReadableStream`: frames split across chunks, multi-line `data`, keep-alive comments, CRLF, `Last-Event-ID` replay, debounced re-resolve of the watched-ledger URL, jittered exponential backoff (jitter pinned, so the doubling is asserted exactly), and per-reconnect auth re-resolution. |
| `remoteTransport` | 42 | Request construction; the SSE-triggered cycle; the client-side change gate; coalescing of head events arriving mid-cycle; ledger-path encoding; time anchors; structured errors; per-subscription delivery ordering; cancellation of superseded requests; the bounded fan-out (mutation-checked — removing the bound fails the test). |
| `peerTransport` | 24 | Engine-id mapping; the batch preserved as one cycle; async-registration races (including an unsubscribe that beats its own registration); the two refusals (`at`, non-native `format`); and crash recycles — re-registration on `"ready"`, stale-id invalidation, and loud failure on `"terminal"`. |
| `peerIntegration` (react-dom) | 7 | Peer mode through the real hooks: memoized rows survive `postMessage` cloning, an unchanged cycle costs zero renders, and a crash recycle keeps data on screen and then picks updates back up under the fresh engine's ids. |
| `protocolCompat` | 2 | The seam. Imports the **real** `@fluree/db-wasm` types and asserts assignability both ways, so `tsc` fails on drift. Mutation-checked against three drifts: a changed cycle shape, a removed method, and a new lifecycle state. |
| `integration` | 7 | The public barrel and `createClient` composed as an app uses them: an SSE head event drives a re-query that re-renders a component. |
| `ssr` | 4 | `renderToString` yields the loading snapshot and performs no I/O. |

**Also executed:** the demo app builds through a real bundler in CI (`vite
build`), which is the only thing that exercises this package as a *package*
rather than as source files a test imported.

**Mocked.** Every HTTP response, every SSE frame, and the whole wasm engine.
The request shapes this package *sends* —
`POST /v1/fluree/query/{ledger}`, `GET /v1/fluree/events?ledger=…`, the
`ns-record` / `ns-retracted` event names and their
`{kind, resource_id, record.commit_t}` payload, the `{ledger}@t:{n}`
time-travel path — have been **read against the server's source and found
correct**, but no test here checks them against a running server: if a route
or field name changed tomorrow, all 171 tests would still pass.

**Not verified:**

- **No live-server run in peer mode.** The engine double implements the
  contract `protocolCompat` pins, but a scripted double cannot prove the real
  worker delivers cycles the way its types promise.
- **No browser run.** jsdom only. The demo has been driven against a real
  server (see below), but not as an automated test.
- **No packaging smoke.** `npm run build` emits `dist/`, and the demo bundles
  the package through Vite, but nothing installs the published tarball and
  checks its export map.
- **Nothing measured.** No benchmark of cycle cost, structural-sharing cost,
  or behaviour at high subscription counts.
- **The remote fan-out is bounded, not fixed.** Remote mode still issues one
  HTTP request per live subscription per commit; `maxConcurrency` limits how
  many run at once, it does not reduce how many run. There is no multi-query
  endpoint on the server to fix this properly today.

## The demo

`demo/` is a two-tab live board: open it twice, write in one tab, watch the
other update — with no polling code in the app. Its per-component
`rendered N×` counters make the invisible property visible: voting on one
note re-renders exactly that row, and a second panel subscribed to a
different query on the same ledger re-runs on every commit but does not
re-render, because its answer did not move. See [demo/README.md](demo/README.md).

## Development

```sh
npm ci
npm run typecheck   # tsc --noEmit
npm test            # vitest run
npm run build       # emit dist/

cd demo && npm ci && npm run dev   # the two-tab demo
```

CI runs the same steps, plus a demo install and bundle, on any change under
`fluree-react/**` (the `react-sdk` job). This package is a plain npm package
and deliberately **not** a Cargo workspace member, so no Rust job ever
rebuilds because of it.

## License

BUSL-1.1
