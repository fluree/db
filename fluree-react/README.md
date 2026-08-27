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

> **Status:** pre-release (`0.0.0-dev`), and it does **not** work against a
> released Fluree server yet — remote mode needs an SSE fix that is still an
> open PR. Read [What you need on the server](#what-you-need-on-the-server)
> before your first run, and
> [What's proven / what isn't](#whats-proven--what-isnt) before believing
> anything else in this file.

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

### Peer mode is not a speed-up on first paint

Worth stating plainly, because the opposite is the natural assumption. Peer
mode pays a cold-start cost remote mode does not have: fetching and compiling
a ~9 MB wasm engine, then discovering and fetching index blocks. A measured
cold open on a 200-row ledger is ~3.4 s, and the dominant term is not I/O —
about 2.7 s of it is idle across eight sequential discovery rounds (~25 ms of
object fetches, ~15 ms of preflights). For comparison, remote mode reached
first rendered data in **141 ms** in the demo on loopback.

What peer mode buys is everything after that first paint: subsequent queries
and every update re-run locally against a frozen snapshot, with no round trip
and no server work. Reducing the round count is the known path to fixing cold
open; until then, choose peer mode for update latency and offline-ish
resilience, not for time-to-first-paint.

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

## What you need on the server

Neither mode works against a stock, released `fluree-server` today. If you
install this package, point it at one, and see a subscription that connects
and then never updates, this section — not your code — is the reason.

### Remote mode

| Requirement | Why |
| --- | --- |
| **A server built with the `?ledger=` SSE fix** (PR **#1730**, `fix/events-per-ledger-subscription`). Not in any release. | Without it `GET /v1/fluree/events?ledger=…` fails deserialization outright — axum's `Query` extractor cannot build a `Vec` from repeated query keys — so **even a single ledger** 400s and the stream never opens. The symptom is a query stuck in `loading` with `useConnectionState()` flapping `reconnecting`. |
| Nothing else. | Query + events is the whole surface. No flags, no tokens unless your server enforces them. |

### Peer mode

Everything above, plus:

| Requirement | Why |
| --- | --- |
| `--storage-proxy-enabled` | The in-browser engine reads ledger heads and index blocks through `/v1/fluree/storage/*`. Off by default; without it every request 401s with *"Token lacks storage proxy permissions"*. |
| `--storage-proxy-trusted-issuer <did:key>` | The proxy trusts only issuers named here (it falls back to `--events-auth-trusted-issuer`). |
| A bearer token carrying **`fluree.storage.all`** or **`fluree.storage.ledgers`** | The storage proxy requires `fluree.storage.*` claims specifically. **`fluree.events.*` is not sufficient**, and the shipped `fluree-events-token` CLI mints only the events half — so it cannot produce a working peer token. `fluree-db-wasm/js/scripts/fluree-server.mjs` exports `createIdentity()` and `mintStorageProxyToken()` that do. |
| `@fluree/db-wasm`'s generated glue | `cd fluree-db-wasm/js && npm install && node scripts/build.mjs` writes `js/pkg/`. |
| A bundler allowed to read it | In a monorepo, Vite's dev server refuses paths outside the project root: set `server.fs.allow`. The symptom is `engine_crashed` with an **empty page console**, because the failure is inside the worker. |

If your server enforces events auth (`--events-auth-mode` is not `none`,
the default), a peer token needs **both** claim families.

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

> **The easiest way to lose all of this** is to derive new objects from
> `data` during render — `data.results.bindings.map(b => ({…}))` gives every
> row a fresh identity and every memoized child re-renders on every commit.
> Nothing warns you. Pass result rows down as they come and convert at the
> leaf, or memoize the derivation per row. The demo does the former, after
> initially getting it wrong.

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

## What's proven / what isn't

Written out plainly, because "the tests pass" and "this works against Fluree"
are different claims.

**In one paragraph:** both modes have run in a real browser against a real
`fluree-server` and done the thing this package exists to do — a commit in
one tab re-rendering exactly the affected row in another, with no polling
code in the app. Remote mode is reliable there; peer mode is reliable only
with a warm block cache, and stalls from cold for reasons that live in the
engine, below this package. Everything in the automated suite is mocked:
178 tests, no network, no wasm. Two known blockers are listed at the end.

### Proven in a real browser, against a real server

| | remote | peer |
| --- | --- | --- |
| Two tabs, live cross-tab update | yes | yes |
| Only the changed row re-rendered (`1×`→`2×`, siblings still `1×`) | yes | yes |
| A second query on the same ledger re-ran and did **not** re-render | yes | yes |
| Watermark advanced | `t` 5→8 | `t` 1→2 |
| Reliable from a cold start | yes | **no** — stalls |

Remote mode reached first rendered data in **141 ms** on loopback. Peer mode
is slower to first paint by design (see
[Peer mode is not a speed-up on first paint](#peer-mode-is-not-a-speed-up-on-first-paint));
no honest peer cold-open number was obtainable here, because the only clean
cold runs available were the ones that stall.

Also executed for real: the demo builds through a real bundler in CI (`vite
build`), which is the only thing exercising this as a *package* rather than
as source files a test imported.

### Proven by executed tests — 178 (`npx vitest run`), all real package code

| Suite | Tests | What it actually proves |
| --- | --- | --- |
| `structuralShare` | 11 | Unchanged rows keep **object identity** across an advance; an all-equal result returns the previous object itself. Every assertion is `toBe`, not `toEqual` — a deep-equality test here would pass against an implementation that shares nothing. |
| `queryCache` | 23 | Dedup by key; the transport subscription opens on the first observer and is released only after `gcTime` with none; the janitor collects never-observed handles; **version coherence** (observers verify the whole cache already carries the new watermark at notify time); keep-last-good-data and both recovery paths; an unchanged cycle produces the identical snapshot object and no notification. |
| `liveClient` | 13 | Query-kind inference and the language-matched format defaults; cache keying; one-shot queries; connection fan-out; teardown releases timers. |
| `useQuery` (react-dom + jsdom) | 19 | Real components: a memoized row does **not** re-render when a sibling row changes; an unchanged cycle costs zero renders; `getSnapshot` is referentially stable across unrelated parent re-renders (a fresh object per call is the canonical infinite-loop bug with `useSyncExternalStore`); StrictMode double-mount keeps one subscription; siblings advance in lock-step. |
| `sse` | 19 | The hand-rolled frame parser against a real `ReadableStream`: frames split across chunks, multi-line `data`, keep-alive comments, CRLF, `Last-Event-ID` replay, debounced re-resolve of the watched-ledger URL, jittered exponential backoff (jitter pinned, so the doubling is asserted exactly), and per-reconnect auth re-resolution. |
| `remoteTransport` | 48 | Request construction; the SSE-triggered cycle; the client-side change gate; coalescing of head events arriving mid-cycle; ledger-path encoding; time anchors; structured errors; per-subscription delivery ordering; cancellation of superseded requests; the bounded fan-out (mutation-checked — removing the bound fails the test). |
| `peerTransport` | 25 | Engine-id mapping; the batch preserved as one cycle; async-registration races (including an unsubscribe that beats its own registration); the two refusals (`at`, non-native `format`); and crash recycles — re-registration on `"ready"`, stale-id invalidation, and loud failure on `"terminal"`. |
| `peerIntegration` (react-dom) | 7 | Peer mode through the real hooks: memoized rows survive `postMessage` cloning, an unchanged cycle costs zero renders, and a crash recycle keeps data on screen and then picks updates back up under the fresh engine's ids. |
| `protocolCompat` | 2 | The seam. Imports the **real** `@fluree/db-wasm` types and asserts assignability both ways, so `tsc` fails on drift. Mutation-checked against three drifts: a changed cycle shape, a removed method, and a new lifecycle state. |
| `integration` | 7 | The public barrel and `createClient` composed as an app uses them: an SSE head event drives a re-query that re-renders a component. |
| `ssr` | 4 | `renderToString` yields the loading snapshot and performs no I/O. |

### What the live runs found that 178 green tests could not

Every one of these was invisible to the mocked suite, and the first three
share a failure mode: **silence**. That is the argument for making the live
run a standing gate rather than a one-off.

1. `fetch` held as an instance property and called as `this.fetchImpl(...)`
   passes the transport as the receiver, and **every browser** rejects that
   as an illegal invocation. Node's fetch does not care, and every transport
   test injected its own `fetchImpl`, so nothing could see it. Every request
   in a real browser failed.
2. The server announces commits for the canonical `name:branch` id
   (`demo/board:main`), but an app subscribes with the bare name — so every
   head event was discarded as unwatched. Subscription open, nothing
   errored, no query ever updating.
3. The server's events filter compares aliases exactly, so subscribing under
   the bare name produced a stream that connects, stays open, and delivers
   nothing. The canonical alias is now *resolved* from `/info/{ledger}`
   rather than guessed from a default branch name.
4. (Server-side; now PR **#1730**) the events endpoint's documented
   `?ledger=` filter never worked at all — axum's `Query` extractor cannot
   build a `Vec` from repeated query keys, so even a single `?ledger=x`
   failed deserialization and `?all=true` was the only usable form.
5. The demo — the artifact whose *entire purpose* is showing that unchanged
   rows do not re-render — mapped results into fresh row objects each render,
   discarding the identity the package had just preserved. Everything worked;
   every row simply re-rendered on every commit. Only the render counters
   showed it. This is the easiest way for a user to lose the whole benefit.

### Mocked

Every HTTP response, every SSE frame, and the whole wasm engine, in the
automated suite. The live runs were manual and are **not reproducible in CI
today** — that gap is why defects 1-4 survived a green suite.

### Unverified

- **No automated browser run.** jsdom only.
- **No packaging smoke.** `npm run build` emits `dist/` and the demo bundles
  the package through Vite, but nothing installs the published tarball and
  checks its export map.
- **Nothing measured** beyond the two first-paint numbers above: no benchmark
  of cycle cost, structural-sharing cost, or behaviour at high subscription
  counts.
- **`useSuspenseQuery`, optimistic writes, and the differential results tier**
  are not built; the API was shaped not to preclude them.

### Known blockers

1. **Remote mode needs PR #1730.** Not in any release. Until it lands, the
   SSE subscription 400s and no query ever updates. See
   [What you need on the server](#what-you-need-on-the-server).
2. **Peer mode stalls from a cold block cache.** `init` succeeds, the token
   handshake completes, the connection reports `live` — then no query ever
   answers: no error, no panic, and **no `/v1/fluree/storage/*` request ever
   issued**, so the engine stalls before its first block fetch. Reproduces
   through the raw worker protocol with head tracking off, i.e. entirely
   below this package; remote mode against the same server is unaffected.
   Engine-side, owned elsewhere. With a warm cache, peer mode works.

Neither blocker is in this package's code.

### Not a blocker, but know it

**The remote fan-out is bounded, not reduced.** Remote mode issues one HTTP
request per live subscription per commit; `maxConcurrency` (default 6) limits
how many run at once, not how many run. There is no multi-query endpoint on
the server to fix this properly today, so a page with many subscriptions on a
busy ledger is the case to watch. Peer mode does not have this shape at all —
its re-runs are local.

## The demo

`demo/` is a two-tab live board: open it twice, write in one tab, watch the
other update — with no polling code in the app. Its per-component
`rendered N×` counters make the invisible property visible: voting on one
note re-renders exactly that row, and a second panel subscribed to a
different query on the same ledger re-runs on every commit but does not
re-render, because its answer did not move.

- [demo/WALKTHROUGH.md](demo/WALKTHROUGH.md) — the presentation script: setup
  commands, what to click in what order, what to point at on screen, and the
  questions you will get.
- [demo/README.md](demo/README.md) — how it is built and why.

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
