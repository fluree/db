# @fluree/db-wasm

> Package name is a placeholder pending the npm org decision.

Fluree in the browser: the graph database engine (`fluree-db-api`) compiled to WebAssembly and hosted in a dedicated Web Worker. Two modes share one API surface:

- **Playground** (`playground()`): an in-memory Fluree you can create ledgers in, transact JSON-LD into, and query with SPARQL or JSON-LD — no server.
- **Peer** (`connect(url, { getToken, ... })`): a read-only local engine over a remote Fluree server — heads resolve through the server's nameservice, index/commit blocks arrive CID-verified and cache in IndexedDB, queries execute locally, and SSE head tracking pushes ledger advances to `peer.on("headChange", …)`. Transacting through a peer rejects with a typed `unsupported`; commits are ordered by the origin server.

```js
import { connect } from "@fluree/db-wasm";

const peer = await connect("https://data.example.com/v1/fluree", {
  getToken: () => fetchTokenFromMyBackend(),   // fluree.storage.* scope
  subscribe: ["books:main"],                   // SSE head tracking ([] = all visible)
});
const books = await peer.ledger("books:main");
const rows = await books.query("SELECT ?s WHERE { ?s ?p ?o } LIMIT 10");
const off = peer.on("headChange", ({ ledger, t }) => rerender(ledger, t));
peer.close();
```

**Peer credentials:** `getToken` is the single source — the token is requested from the main thread over the worker's event channel at connect AND at every post-crash reconnect; it is never embedded in a replayable init message. There is no mid-session re-auth op on this package's surface: when a token expires, requests fail typed and you reconnect (`close()` + `connect()`) with a fresh token, which costs every subscription, snapshot, and the warm residency tier.

The engine below this package can already do better — `fluree-db-browser`'s `BrowserPeer::set_token` swaps the bearer through a shared cell that every I/O surface reads, with no teardown, and it is natively tested. It is deliberately NOT exposed here yet, because wiring it needs a decision this package has not made: which side owns the token once both paths exist. Today `getToken` is asked on connect and re-asked on every recycle, so it is the single source of truth; a pushed `setToken` would be silently superseded by the next recycle's `getToken` unless the two are reconciled. Expect a `setToken` op once that is settled.

## Install & use

```js
import { playground } from "@fluree/db-wasm";

const fluree = await playground();          // spawns the worker, streams the .wasm
const ledger = await fluree.createLedger("demo");

await ledger.insert({
  "@context": { ex: "http://example.org/ns/" },
  "@graph": [
    { "@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:knows": { "@id": "ex:bob" } },
    { "@id": "ex:bob",   "@type": "ex:Person", "ex:name": "Bob" },
  ],
});

// SPARQL string → W3C SPARQL Results JSON
const rows = await ledger.query(`
  PREFIX ex: <http://example.org/ns/>
  SELECT ?name WHERE { ?p a ex:Person ; ex:name ?name }`);

// JSON-LD query object → JSON-LD results
const alice = await ledger.query({
  "@context": { ex: "http://example.org/ns/" },
  select: { "?p": ["*"] },
  where: { "@id": "?p", "ex:knows": { "@id": "ex:bob" } },
});

await ledger.update({ /* where / delete / insert */ });
await ledger.sparqlUpdate(`PREFIX ex: <http://example.org/ns/> INSERT DATA { ex:carol a ex:Person }`);
fluree.close();                             // terminates the worker; ledgers are gone
```

### API

| | |
|---|---|
| `playground(options?)` → `Promise<Playground>` | Start an engine worker. `options.workerUrl`, `options.wasmUrl` override asset locations; `options.resultTransport` is `"transfer"` (default) or `"clone"`; `options.maxMemoryBytes` caps each query's memory budget (see below). |
| `connect(url, options?)` → `Promise<Peer>` | Read-only peer over a remote server (`url` = versioned API base). Adds `options.getToken` (credential source — see above) and `options.subscribe` (SSE head tracking). In peer mode `maxMemoryBytes` is the ONE ceiling: it derives the whole browser-io budget split (residency tier, write-behind, fetch width) and the per-query budget (¼ of it). |
| `Peer.ledger(id)` → `Promise<Ledger>`, `Peer.on("headChange", cb)` → unsubscriber, `Peer.version`, `.close()` | Same `Ledger` surface as the playground minus writes (transacts reject `unsupported`). Head-change callbacks fire after the engine absorbed the advance: the next `snapshot()` sees the new head; frozen snapshots never move. |
| `Playground.subscribe(ledger, query, onUpdate)` / `Peer.subscribe(...)` → `Promise<LiveSubscription>` | Live query: `onUpdate` fires with the first result immediately (auto-prime) and again after every commit that CHANGES the result — unchanged results are hash-gated engine-side and cost no serialization or delivery. One batched engine cycle per head advance (peer: SSE-driven; playground: after each local transact); changed payloads cross the worker boundary as transferred buffers. `update` is `{ ledger, t, data }` or `{ ledger, t, error }` (per-cycle; keep-last-good is the caller's layer). Subscriptions do not survive a crash recycle — re-subscribe on `fatal`. `LiveSubscription.unsubscribe()` stops updates. |
| `Playground.createLedger(id)` / `.ledger(id)` → `Promise<Ledger>` | `"demo"` normalizes to `"demo:main"`. `conflict` / `not_found` errors respectively. |
| `Playground.version`, `.close()` | Engine version; terminate the worker. |
| `Ledger.insert(data)` / `.upsert(data)` / `.update(doc)` / `.sparqlUpdate(text)` → `Promise<CommitReceipt>` | JSON-LD in (object or JSON text); `{ t, commit, flakes }` out. |
| `Ledger.query(sparqlOrJsonLd, { transport? })` → `Promise<unknown>` | String = SPARQL, object = JSON-LD query. Results are the same shapes the Fluree HTTP `/query` route returns, always fully buffered. The head is frozen for the duration of each call — a concurrent commit never moves the view mid-query. |
| `Ledger.snapshot()` → `Promise<Snapshot>` | Freeze the current head. `Snapshot.query(...)` runs any number of queries against the same immutable view (`.t` tells you which); `Snapshot.release()` frees it. |
| `Ledger.info()` → `Promise<LedgerInfo>` | `{ id, t, indexT }`. |
| `FlureeError` | `{ code, status, message, fatal }`. Codes: `invalid_input`, `not_found`, `conflict`, `cancelled`, `out_of_memory`, `unsupported`, `internal`, `engine_crashed`, `engine_restarting` (a call raced the post-crash respawn window — retry shortly), `engine_unavailable` (terminal: the worker never booted or crashed repeatedly), `not_initialized` (internal: a request reached the worker before any init), `closed`. |

Every method returns a promise: calls cross to the worker as messages, so the page thread never blocks, even during a long query. There is deliberately **no streaming-results API** — results arrive complete or not at all, which is also what keeps the future peer mode's fetch-and-retry execution transparent.

### Memory, crashes, and recycling

The engine lives in wasm linear memory (hard 4 GiB ceiling; memory grows but never shrinks). Two guard rails keep that from taking pages down:

- **A per-query memory budget** (`maxMemoryBytes`). A query whose retained working set (sorts, group-bys, join builds) crosses the budget fails with a typed `out_of_memory` error — the engine survives and other work is untouched. Default: ¼ of `navigator.deviceMemory` clamped to [256 MiB, 2 GiB] where the browser exposes it (Chromium), else 512 MiB. Pass a number to override, or `null` for the engine's built-in default (1 GiB). **The budget instruments query execution only.** The transact paths (`insert`/`upsert`/`update`/`sparqlUpdate`) have no engine-side budget yet; they get a coarse input-size pre-gate (bodies over ¼ of the budget are refused typed) — a transaction that passes the gate but stages an enormous graph can still trap the allocator and cost you the playground via recycle. A transact-side budget is engine work, tracked; until then, batch very large loads.
- **Worker recycling.** If the wasm instance traps anyway (a Rust panic, or an allocation the browser refuses), the instance is unrecoverable. The proxy detects it, rejects every in-flight call with a `fatal` `FlureeError` (`engine_crashed`, or `out_of_memory` for allocation traps), terminates the worker, and — after a short backoff — starts + re-initializes a fresh one. The `Playground` object stays usable — but playground data is in-memory, so **ledgers and snapshots do not survive a recycle**: every `Ledger`/`Snapshot` object is generation-stamped, and calls on pre-crash objects reject with a typed `not_found` (they can never silently read the fresh engine's state, even though the fresh engine reuses small snapshot-handle numbers). Calls that race the respawn window get `engine_restarting`. Respawns are capped (3 consecutive failures) with exponential backoff; past the cap — or if the worker never booted at all (bad `workerUrl`, wasm fetch failure) — the channel goes terminal and everything rejects `engine_unavailable`. Treat `fatal: true` as "re-create my data or reload".

## Hosting requirements

**None beyond static files over HTTPS (or `localhost`).** Specifically:

- **No COOP/COEP headers, no cross-origin isolation, no SharedArrayBuffer.** The engine is single-threaded inside its worker and talks to the page with `postMessage`. Your OAuth popups, third-party embeds, and analytics keep working; GitHub Pages, S3/CloudFront, Netlify, Vercel, Cloudflare Pages all work with zero configuration.
- Serve `fluree_db_wasm_bg.wasm` with `Content-Type: application/wasm` so the browser can compile it while it streams (every mainstream host does; the fallback path still works if not, just slower). Enable Brotli or gzip on it — see sizes below.
- The worker is a **module worker** (`new Worker(url, { type: "module" })`): Chrome 80+, Firefox 114+, Safari 15+.
- Bundlers: the default spawn is the literal `new Worker(new URL("./worker.js", import.meta.url), { type: "module" })` expression, which is the shape bundlers' *static* worker detection requires (a URL passed through a variable is not detected — that is why `workerUrl` overrides opt out of bundling). Verified: unbundled (the demo + CI smoke run the package as plain static files) and a zero-config `vite build` + headless run of the built app via `node scripts/vite-repro.mjs` (dev-box script; run it yourself to re-verify). webpack 5/Rollup/Parcel document the same detection pattern but are **not** covered by our checks — no bundler runs in CI yet. Without a bundler, serve the package directory as-is and import `dist/index.js`; pass `workerUrl`/`wasmUrl` only when hosting the assets somewhere else (CDN).
- Memory: everything lives in the worker's wasm linear memory (4 GiB ceiling, never shrinks). `close()` and start a new playground to release it.

## Building from source

Requires the pinned Rust toolchain plus the `wasm32-unknown-unknown` target, the `wasm-bindgen` CLI at the exact version in `Cargo.lock`, `wasm-opt` (binaryen), Node ≥ 20, and the repo's wasm C toolchain setup (see `.cargo/config.toml` — a wasm-capable clang for `zstd-sys`).

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version "$(grep -A1 'name = "wasm-bindgen"$' ../../Cargo.lock | tail -1 | cut -d'"' -f2)" --locked
cd fluree-db-wasm/js && npm install
node scripts/build.mjs            # → pkg/ (wasm + glue) and dist/ (TS output); prints sizes
node scripts/serve.mjs            # http://127.0.0.1:8787/demo/
node scripts/smoke-browser.mjs    # headless-Chrome end-to-end check (positive ran-marker)
node scripts/smoke-peer-server.mjs  # the same, against a REAL fluree-db-server
```

`smoke-peer-server.mjs` launches `fluree-server` itself (building it first if
`target/{debug,release}/fluree-server` is absent, or set `FLUREE_SERVER_BIN`),
mints a storage-proxy token, creates and populates a ledger over HTTP, then
drives `connect()` in headless Chrome from a *different origin* through a
recording reverse proxy — so it asserts the query's rows, the CAS objects the
browser actually fetched, and the SSE live loop after an external commit.

The Rust-level browser tests run with `wasm-pack test --headless --chrome ..` from the crate directory (needs a `chromedriver` matching your Chrome; set `CHROMEDRIVER=` to point at one).

### Build profile and size

The `.wasm` builds with the workspace's `wasm-release` cargo profile (fat LTO, one codegen unit, stripped, `opt-level = "s"`) followed by `wasm-opt -Os`. `scripts/build.mjs` prints raw / gzip / brotli sizes and writes `pkg/size-report.json`; the current numbers are in the crate README. Playground and peer mode share one binary; a feature-sliced slim build (no R2RML, reasoner, full-text, Cypher) is the lever if size ever matters more than surface.

### Peer-mode verification status (honest)

Verified in CI's browser smoke without a server: the token event round-trip (init carries no credential; `getToken` answers the worker's request), peer init/close, and typed non-fatal failure against an unreachable remote. Verified natively in `fluree-db-browser`'s mock-driver suites: head resolution through the proxy nameservice, SSE head tracking → callback dispatch, block fetch/verify/cache.

Verified in CI against a **real `fluree-db-server`** by `scripts/smoke-peer-server.mjs`: the server runs with the storage proxy on and the smoke's own did:key as its only trusted issuer (the production token path, not `--storage-proxy-insecure`); a ledger is created, given `f:serveBlocks true`, and populated over HTTP; the page is served from a different origin than the API, so the request really is cross-origin; `connect()` then opens the ledger, and the query's rows are asserted alongside the traffic that produced them — successful `GET /storage/objects/{cid}` fetches, head resolutions, an open SSE stream, and **zero** calls to `/query` (local compute, not query-shipping). An external HTTP commit then has to reach the page as an SSE head change, be absorbed by the engine, and show up both in a re-query and in a live subscription's update.

Still not covered: token expiry/refresh against a live server, IndexedDB reuse across a page reload, ranged leaf reads, and anything on the public/anonymous tier (not implemented server-side).

### Result transport

Query results leave the engine as UTF-8 JSON bytes. By default the worker posts that `Uint8Array` with its buffer in the transfer list (zero-copy handoff, constant cost) and the page decodes and parses it. `resultTransport: "clone"` instead decodes in the worker and structured-clones a string (a memcpy — fine for small results, linear in size). `demo/index.html` has a side-by-side timer; the shapes and numbers are in the crate README.
