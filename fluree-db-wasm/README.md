# fluree-db-wasm

The `wasm-bindgen` binding layer for the browser build of Fluree, plus the npm package (`js/`) that hosts it in a dedicated Web Worker. User-facing docs, API, and hosting requirements: [`js/README.md`](js/README.md).

## Layout

- `src/lib.rs` — `#[wasm_bindgen]` exports: `Playground` (in-memory engine; create / insert / upsert / update / sparqlUpdate / snapshot / release / querySparql / queryJsonld / ledgerInfo) and `version()`. Wraps `fluree-db-api` with `default-features = false` — the exact dependency shape the CI `wasm32` check enforces. Queries take a **snapshot handle** (a frozen `GraphDb`), never a live alias — review F6 — and the constructor takes the single memory-budget setting applied to every query via `QueryCancellation::set_memory_limit` — review F4. The streaming query entry is deliberately not bound.
- `src/error.rs` — `ApiError` → JS `Error` with stable `code` + HTTP-style `status`.
- `tests/playground.rs` — `wasm-bindgen-test` suite, run **in a dedicated worker** in headless Chrome.
- `js/src/index.ts` — main-thread proxy (no wasm); `js/src/worker.ts` — worker entry; `js/src/protocol.ts` — the message protocol.
- `js/scripts/build.mjs` — cargo (`--profile wasm-release`) → `wasm-bindgen --target web` → `wasm-opt` → `tsc` → size report. `js/scripts/smoke-browser.mjs` — headless-Chrome end-to-end check of the shipped package over CDP. `js/scripts/serve.mjs` — static server with the right MIME types and *no* isolation headers.

## Runtime model (why there is no tokio here)

The engine runs single-threaded on the worker's event loop. No tokio runtime is constructed: wasm-bindgen turns each `async fn` export into a `Promise`, and the engine's internal detached spawns go through `fluree-db-api`'s `spawn_detached` seam to `wasm_bindgen_futures::spawn_local`. rayon call sites run sequentially under rayon-core's wasm fallback. Clocks go through `fluree_db_core::clock` (`web-time`). Nothing in this crate blocks the thread.

## Verification

```sh
cargo check --target wasm32-unknown-unknown -p fluree-db-wasm --all-targets
cargo clippy -p fluree-db-wasm --all-targets --no-deps
wasm-pack test --headless --chrome fluree-db-wasm          # Rust-level, in a worker
cd fluree-db-wasm/js && node scripts/build.mjs && node scripts/smoke-browser.mjs   # shipped package
```

CI runs the last two in the `wasm-smoke` job (`.github/workflows/ci.yml`) next to the `wasm32` compile check.
