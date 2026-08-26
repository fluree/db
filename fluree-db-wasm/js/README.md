# @fluree/db-wasm

> Package name is a placeholder pending the npm org decision.

Fluree in the browser: the graph database engine (`fluree-db-api`) compiled to WebAssembly and hosted in a dedicated Web Worker. This release ships **playground mode** — an in-memory Fluree you can create ledgers in, transact JSON-LD into, and query with SPARQL or JSON-LD, with no server. The **peer mode** (query a Fluree server's ledgers locally from CID-verified, cached index blocks) uses the same API surface via a second constructor and lands in a later release.

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
| `Playground.createLedger(id)` / `.ledger(id)` → `Promise<Ledger>` | `"demo"` normalizes to `"demo:main"`. `conflict` / `not_found` errors respectively. |
| `Playground.version`, `.close()` | Engine version; terminate the worker. |
| `Ledger.insert(data)` / `.upsert(data)` / `.update(doc)` / `.sparqlUpdate(text)` → `Promise<CommitReceipt>` | JSON-LD in (object or JSON text); `{ t, commit, flakes }` out. |
| `Ledger.query(sparqlOrJsonLd, { transport? })` → `Promise<unknown>` | String = SPARQL, object = JSON-LD query. Results are the same shapes the Fluree HTTP `/query` route returns, always fully buffered. The head is frozen for the duration of each call — a concurrent commit never moves the view mid-query. |
| `Ledger.snapshot()` → `Promise<Snapshot>` | Freeze the current head. `Snapshot.query(...)` runs any number of queries against the same immutable view (`.t` tells you which); `Snapshot.release()` frees it. |
| `Ledger.info()` → `Promise<LedgerInfo>` | `{ id, t, indexT }`. |
| `FlureeError` | `{ code, status, message, fatal }`. Codes: `invalid_input`, `not_found`, `conflict`, `cancelled`, `out_of_memory`, `unsupported`, `internal`, `engine_crashed`, `closed`. |

Every method returns a promise: calls cross to the worker as messages, so the page thread never blocks, even during a long query. There is deliberately **no streaming-results API** — results arrive complete or not at all, which is also what keeps the future peer mode's fetch-and-retry execution transparent.

### Memory, crashes, and recycling

The engine lives in wasm linear memory (hard 4 GiB ceiling; memory grows but never shrinks). Two guard rails keep that from taking pages down:

- **A per-query memory budget** (`maxMemoryBytes`). A query whose retained working set (sorts, group-bys, join builds) crosses the budget fails with a typed `out_of_memory` error — the engine survives and other work is untouched. Default: ¼ of `navigator.deviceMemory` clamped to [256 MiB, 2 GiB] where the browser exposes it (Chromium), else 512 MiB. Pass a number to override, or `null` for the engine's built-in default (1 GiB).
- **Worker recycling.** If the wasm instance traps anyway (a Rust panic, or an allocation the browser refuses), the instance is unrecoverable. The proxy detects it, rejects every in-flight call with a `fatal` `FlureeError` (`engine_crashed`, or `out_of_memory` for allocation traps), terminates the worker, and starts + re-initializes a fresh one. The `Playground` object stays usable — but playground data is in-memory, so **ledgers and snapshots do not survive a recycle**; stale handles reject with `not_found`. Treat `fatal: true` as "re-create my data or reload".

## Hosting requirements

**None beyond static files over HTTPS (or `localhost`).** Specifically:

- **No COOP/COEP headers, no cross-origin isolation, no SharedArrayBuffer.** The engine is single-threaded inside its worker and talks to the page with `postMessage`. Your OAuth popups, third-party embeds, and analytics keep working; GitHub Pages, S3/CloudFront, Netlify, Vercel, Cloudflare Pages all work with zero configuration.
- Serve `fluree_db_wasm_bg.wasm` with `Content-Type: application/wasm` so the browser can compile it while it streams (every mainstream host does; the fallback path still works if not, just slower). Enable Brotli or gzip on it — see sizes below.
- The worker is a **module worker** (`new Worker(url, { type: "module" })`): Chrome 80+, Firefox 114+, Safari 15+.
- Bundlers: the package resolves its worker and `.wasm` with `new URL(..., import.meta.url)`, which Vite, webpack 5, Rollup, and Parcel handle statically. Without a bundler, serve the package directory as-is and import `dist/index.js`; pass `workerUrl`/`wasmUrl` only when hosting the assets somewhere else (CDN).
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
```

The Rust-level browser tests run with `wasm-pack test --headless --chrome ..` from the crate directory (needs a `chromedriver` matching your Chrome; set `CHROMEDRIVER=` to point at one).

### Build profile and size

The `.wasm` builds with the workspace's `wasm-release` cargo profile (fat LTO, one codegen unit, stripped, `opt-level = "s"`) followed by `wasm-opt -Os`. `scripts/build.mjs` prints raw / gzip / brotli sizes and writes `pkg/size-report.json`; the current numbers are in the crate README. Playground and future peer mode share one binary; a feature-sliced slim build (no R2RML, reasoner, full-text, Cypher) is the lever if size ever matters more than surface.

### Result transport

Query results leave the engine as UTF-8 JSON bytes. By default the worker posts that `Uint8Array` with its buffer in the transfer list (zero-copy handoff, constant cost) and the page decodes and parses it. `resultTransport: "clone"` instead decodes in the worker and structured-clones a string (a memcpy — fine for small results, linear in size). `demo/index.html` has a side-by-side timer; the shapes and numbers are in the crate README.
