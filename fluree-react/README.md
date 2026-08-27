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

> **Status:** pre-release (`0.0.0-dev`). Remote mode is implemented and
> tested against a mock server; it has not yet been run against a live
> Fluree server. Peer mode is not implemented yet. See
> [Verification status](#verification-status) — it is deliberately blunt.

## Two modes, one API

The hooks are identical in both. Only `createClient` differs.

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

**Peer mode** — the headline, and *not yet shipped*. A wasm engine in a web
worker hears the same commit stream, applies it locally, and re-runs the
affected queries at memory speed. It plugs into the same client through the
`LiveTransport` seam:

```ts
const client = createClient({ transport: await createPeerTransport(...) }); // not yet available
```

Remote mode is the on-ramp: an app written against these hooks today gains
peer mode by changing one line, because the transport contract and the
result shapes are identical in both.

## API

### `createClient(config)`

| Field | Mode | Meaning |
| --- | --- | --- |
| `url` | remote | Server base URL. `/v1/fluree/...` is appended. |
| `getToken` | remote | `() => string \| undefined \| Promise<...>`, called per request and per SSE reconnect. |
| `fetchImpl` | remote | Override `fetch` (tests, non-browser runtimes). |
| `sseRefreshDebounceMs`, `backoffBaseMs`, `backoffMaxMs` | remote | SSE reconnect tuning. |
| `transport` | custom | A `LiveTransport` — how peer mode will be injected. |
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

**Executed** — 133 tests (`npx vitest run`), all running real package code:

| Suite | Tests | What it actually proves |
| --- | --- | --- |
| `structuralShare` | 11 | Unchanged rows keep **object identity** across an advance; an all-equal result returns the previous object itself. Every assertion is `toBe`, not `toEqual` — a deep-equality test here would pass against an implementation that shares nothing. |
| `queryCache` | 23 | Dedup by key; the transport subscription opens on the first observer and is released only after `gcTime` with none; the janitor collects never-observed handles; **version coherence** (observers verify the whole cache already carries the new watermark at notify time); keep-last-good-data and both recovery paths; an unchanged cycle produces the identical snapshot object and no notification. |
| `liveClient` | 13 | Query-kind inference and the language-matched format defaults; cache keying; one-shot queries; connection fan-out; teardown releases timers. |
| `useQuery` (react-dom + jsdom) | 19 | Real components: a memoized row does **not** re-render when a sibling row changes; an unchanged cycle costs zero renders; `getSnapshot` is referentially stable across unrelated parent re-renders (a fresh object per call is the canonical infinite-loop bug with `useSyncExternalStore`); StrictMode double-mount keeps one subscription; siblings advance in lock-step. |
| `sse` | 19 | The hand-rolled frame parser against a real `ReadableStream`: frames split across chunks, multi-line `data`, keep-alive comments, CRLF, `Last-Event-ID` replay, debounced re-resolve of the watched-ledger URL, jittered exponential backoff (jitter pinned, so the doubling is asserted exactly), and per-reconnect auth re-resolution. |
| `remoteTransport` | 37 | Request construction; the SSE-triggered cycle; the client-side change gate (`changed` vs `unchanged`); coalescing of head events arriving mid-cycle; ledger-path encoding; time anchors; structured errors; per-subscription delivery ordering. |
| `integration` | 7 | The public barrel and `createClient` composed as an app uses them: an SSE head event drives a re-query that re-renders a component. |
| `ssr` | 4 | `renderToString` yields the loading snapshot and performs no I/O. |

**Mocked.** Every HTTP response and every SSE frame. The tests assert the
request shapes this package *sends* —
`POST /v1/fluree/query/{ledger}`, `GET /v1/fluree/events?ledger=…`, the
`ns-record` / `ns-retracted` event names and their
`{kind, resource_id, record.commit_t}` payload, the `{ledger}@t:{n}`
time-travel path — but **nothing here checks those against a running Fluree
server.** If a route, event name, or field name is wrong, all 133 tests
still pass. That check is the next thing this package needs.

**Not verified at all:**

- **No run against a live Fluree server**, in any mode.
- **Peer mode does not exist yet.** `LiveTransport` is shaped for it and
  `CycleUpdate` mirrors the engine's batch-event shape, but no peer
  transport is implemented and there is no compile-time link between this
  package's types and `@fluree/db-wasm`'s protocol types — that shared
  protocol file is still to be built, and until it is, the seam is held
  together by convention.
- **No browser run.** jsdom only.
- **No packaging smoke.** `npm run build` type-checks and emits, but the
  tests import from `src/`, so the published `dist/` shape and the export
  map are unexercised.
- **Nothing measured.** No benchmark of cycle cost, structural-sharing
  cost, or behaviour at high subscription counts. Remote mode re-runs
  *every* live subscription on a ledger per commit; that is correct by
  construction and untested for scale.

## Development

```sh
npm ci
npm run typecheck   # tsc --noEmit
npm test            # vitest run
npm run build       # emit dist/
```

CI runs the same three on any change under `fluree-react/**` (the
`react-sdk` job). This package is a plain npm package and deliberately
**not** a Cargo workspace member, so no Rust job ever rebuilds because of
it.

## License

BUSL-1.1
