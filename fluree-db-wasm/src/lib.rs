//! Browser binding layer for Fluree: a `wasm-bindgen` surface over
//! `fluree-db-api` compiled for `wasm32-unknown-unknown`.
//!
//! Design (see `js/README.md` for the user-facing story):
//!
//! - **Single-threaded, event-loop driven.** No tokio runtime is ever
//!   constructed. Every async export is turned into a JS `Promise` by
//!   wasm-bindgen and polled by the browser's microtask queue; the engine's
//!   own detached spawns route to `wasm_bindgen_futures::spawn_local` through
//!   `fluree-db-api`'s `spawn_detached` seam.
//! - **Worker-hosted.** This module is instantiated inside a dedicated Web
//!   Worker by `js/src/worker.ts`; page code talks to the TypeScript proxy in
//!   `js/src/index.ts`, never to these exports directly. Nothing here assumes
//!   a worker, though — the wasm-bindgen tests run it in one, and a page could
//!   instantiate it on the main thread at the cost of blocking during queries.
//! - **Queries run against frozen snapshots** (adversarial review F6): a
//!   snapshot pins a `GraphDb` view under an integer handle and every query
//!   names a handle, so a head advance — SSE-driven in peer mode — can never
//!   move the view mid-query. Only buffered results are exposed; the engine's
//!   streaming entry (`run_stream_query`) is deliberately not bound, because
//!   rows emitted before completion cannot participate in the peer's
//!   fetch-and-re-run loop.
//! - **One memory setting** (adversarial review F4): the byte ceiling becomes
//!   each query's memory budget (`QueryCancellation::set_memory_limit`, typed
//!   `out_of_memory` on breach), and in peer mode additionally derives every
//!   browser-io knob via `BrowserIoConfig::from_max_memory` — the single
//!   governor the F4 review asked for.
//! - **JSON in, JSON out.** Inputs arrive as JSON text (SPARQL is passed as
//!   the query string itself). Query results leave as UTF-8 JSON *bytes*
//!   (`Vec<u8>` → `Uint8Array`) so the worker can hand the buffer to the main
//!   thread as a transferable with zero copies at the boundary; small
//!   metadata (receipts, ledger/snapshot info) leaves as a JSON string.
//! - **Two engine modes, one surface.** [`Playground`] wraps
//!   `FlureeBuilder::memory().build_memory()`: in-process memory ledgers,
//!   read-write, no server. [`peer::Peer`] (wasm-only module) wraps
//!   `fluree-db-browser`'s `BrowserPeer`: remote ledgers read locally from
//!   CID-verified blocks, read-only, with SSE head tracking. Both expose the
//!   identical snapshot/query methods via [`engine::EngineCore`].

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

mod engine;
mod error;
mod live;
#[cfg(target_arch = "wasm32")]
mod peer;

use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
pub use peer::{connect_peer, Peer};

use fluree_db_api::{FlureeBuilder, TransactResultRef};
use serde_json::json;

use crate::engine::{ledger_info_json, make_exec_options, parse_json, sanitize_bytes, EngineCore};
use crate::error::{api_error, js_error};
use crate::live::LiveBridge;
use fluree_db_browser::LiveQuerySet;

/// Runs once per module instantiation: route Rust panics to `console.error`
/// with a message instead of an opaque `RuntimeError: unreachable`. A panic
/// still aborts the instance (wasm32-unknown-unknown has no unwinding); the
/// hook only makes the post-mortem legible. The JS shell recycles the worker
/// on such a trap.
#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

/// Crate version, surfaced to JS for the `playground().version` field.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// An in-memory Fluree instance hosting any number of memory ledgers.
///
/// Every method is `async` and resolves on the browser event loop. Methods
/// take `&self`, so concurrent calls on one handle are allowed (wasm-bindgen
/// takes a shared borrow for the life of each future); the engine serializes
/// commits per ledger internally. Interior state (the snapshot slab) is
/// borrowed only between awaits, never across one.
#[wasm_bindgen]
pub struct Playground {
    core: EngineCore,
    /// The full ceiling (the per-query budget equals it in playground mode);
    /// kept separately for the transact pre-gate.
    max_memory_bytes: Option<usize>,
    /// Live subscriptions (A4). The playground drives `advance` after each
    /// local commit, so live queries work with no server.
    live: LiveBridge,
}

impl Default for Playground {
    fn default() -> Self {
        Self::new(None)
    }
}

#[wasm_bindgen]
impl Playground {
    /// Build the in-memory engine. Cheap: no I/O, no ledgers yet.
    ///
    /// `max_memory_bytes` caps each query's retained-memory budget; queries
    /// that cross it fail with a typed `out_of_memory` error instead of
    /// trapping the whole instance. Omitted/0 → engine default. The budget
    /// instruments QUERY execution only — the transact paths have no
    /// engine-side budget yet and get a coarse input-size pre-gate instead
    /// (see [`Self::insert`] and `js/README.md`).
    #[wasm_bindgen(constructor)]
    pub fn new(max_memory_bytes: Option<f64>) -> Playground {
        let max = sanitize_bytes(max_memory_bytes);
        let fluree = FlureeBuilder::memory().build_memory();
        let live = LiveBridge::new(LiveQuerySet::with_execution_options(
            fluree.clone(),
            None,
            make_exec_options(max),
        ));
        Playground {
            core: EngineCore::new(fluree, max),
            max_memory_bytes: max,
            live,
        }
    }

    /// Create a ledger. `ledger_id` is normalized to `name:branch`
    /// (`"demo"` → `"demo:main"`). Rejects with `conflict` if it exists.
    /// Resolves to `{"id","t","indexT"}` as a JSON string.
    #[wasm_bindgen(js_name = createLedger)]
    pub async fn create_ledger(&self, ledger_id: String) -> Result<String, JsValue> {
        let state = self
            .core
            .fluree()
            .create_ledger(&ledger_id)
            .await
            .map_err(api_error)?;
        Ok(ledger_info_json(
            state.ledger_id(),
            state.t(),
            state.index_t(),
        ))
    }

    /// Current watermarks of an existing ledger as `{"id","t","indexT"}`.
    /// Rejects with `not_found` for unknown ledgers.
    #[wasm_bindgen(js_name = ledgerInfo)]
    pub async fn ledger_info(&self, ledger_id: String) -> Result<String, JsValue> {
        self.core.ledger_info(&ledger_id).await
    }

    /// Freeze the ledger's current head as a queryable snapshot. Resolves to
    /// `{"handle","id","t"}`; the handle stays valid — and the view immutable,
    /// later commits notwithstanding — until [`Self::release`].
    pub async fn snapshot(&self, ledger_id: String) -> Result<String, JsValue> {
        self.core.snapshot(&ledger_id).await
    }

    /// Drop a snapshot. Returns whether the handle existed. Never errors —
    /// releasing twice is a no-op.
    pub fn release(&self, snapshot: u32) -> bool {
        self.core.release(snapshot)
    }

    /// Insert JSON-LD (`data` is the JSON text of a node, node array, or
    /// `{"@context", "@graph"}` document). Resolves to a commit receipt.
    pub async fn insert(&self, ledger_id: String, data: String) -> Result<String, JsValue> {
        self.transact_pregate("insert body", data.len())?;
        let data = parse_json("transaction body", &data)?;
        let out = self
            .core
            .fluree()
            .graph(&ledger_id)
            .transact()
            .insert(&data)
            .commit()
            .await
            .map_err(api_error)?;
        self.advance_live(&ledger_id);
        Ok(receipt_json(&out))
    }

    /// Upsert JSON-LD: like `insert`, but existing values of single-cardinality
    /// properties on the same subject are replaced instead of accumulated.
    pub async fn upsert(&self, ledger_id: String, data: String) -> Result<String, JsValue> {
        self.transact_pregate("upsert body", data.len())?;
        let data = parse_json("transaction body", &data)?;
        let out = self
            .core
            .fluree()
            .graph(&ledger_id)
            .transact()
            .upsert(&data)
            .commit()
            .await
            .map_err(api_error)?;
        self.advance_live(&ledger_id);
        Ok(receipt_json(&out))
    }

    /// JSON-LD update: a `{"where", "delete", "insert"}` document (JSON text).
    pub async fn update(&self, ledger_id: String, data: String) -> Result<String, JsValue> {
        self.transact_pregate("update document", data.len())?;
        let data = parse_json("update document", &data)?;
        let out = self
            .core
            .fluree()
            .graph(&ledger_id)
            .transact()
            .update(&data)
            .commit()
            .await
            .map_err(api_error)?;
        self.advance_live(&ledger_id);
        Ok(receipt_json(&out))
    }

    /// SPARQL 1.1 Update (`INSERT DATA`, `DELETE/INSERT WHERE`, …).
    #[wasm_bindgen(js_name = sparqlUpdate)]
    pub async fn sparql_update(
        &self,
        ledger_id: String,
        sparql: String,
    ) -> Result<String, JsValue> {
        self.transact_pregate("SPARQL update", sparql.len())?;
        let out = self
            .core
            .fluree()
            .graph(&ledger_id)
            .transact()
            .sparql_update(&sparql)
            .commit()
            .await
            .map_err(api_error)?;
        self.advance_live(&ledger_id);
        Ok(receipt_json(&out))
    }

    /// Run a SPARQL query against a snapshot handle. Resolves to UTF-8 JSON
    /// bytes: W3C SPARQL Results JSON for SELECT/ASK, JSON-LD for
    /// CONSTRUCT/DESCRIBE — the same shapes the HTTP `/query` route returns.
    #[wasm_bindgen(js_name = querySparql)]
    pub async fn query_sparql(
        &self,
        snapshot: u32,
        sparql: String,
        timeout_ms: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        self.core.query_sparql(snapshot, &sparql, timeout_ms).await
    }

    /// Run a JSON-LD query (`query` is the JSON text of the query object)
    /// against a snapshot handle. Resolves to UTF-8 JSON bytes in Fluree's
    /// JSON-LD result format.
    #[wasm_bindgen(js_name = queryJsonld)]
    pub async fn query_jsonld(
        &self,
        snapshot: u32,
        query: String,
        timeout_ms: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        self.core.query_jsonld(snapshot, &query, timeout_ms).await
    }

    /// Test hook: deliberately panic — i.e. trap the wasm instance — so the
    /// JS shell's crash/recycle path can be exercised end to end (a wasm
    /// panic aborts the instance; there is no gentler way to produce the real
    /// poisoned state). Hidden: not part of the supported API surface.
    #[doc(hidden)]
    #[wasm_bindgen(js_name = debugCrash)]
    #[allow(clippy::panic, clippy::unused_self)]
    pub fn debug_crash(&self) {
        panic!("debugCrash: deliberate trap requested by the test harness");
    }
}

/// Live-query verbs (A4) — see `src/live.rs` for the delivery contract.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl Playground {
    /// Register a live subscription (auto-primed: its first result arrives
    /// as a `cycleOutcome` event at the current head). Returns the sub id.
    #[wasm_bindgen(js_name = subscribe)]
    pub fn subscribe_live(
        &self,
        ledger: String,
        kind: String,
        text: String,
    ) -> Result<f64, JsValue> {
        self.live.subscribe(&ledger, &kind, &text)
    }

    /// Remove a live subscription. Idempotent.
    #[wasm_bindgen(js_name = unsubscribe)]
    pub fn unsubscribe_live(&self, sub_id: f64) -> bool {
        self.live.unsubscribe(sub_id)
    }

    /// Register the cycle-outcome fan-out callback
    /// `(metaJson: string, payloads: Uint8Array[])`.
    #[wasm_bindgen(js_name = onCycleOutcome)]
    pub fn on_cycle_outcome(&self, callback: js_sys::Function) {
        self.live.on_outcome_js(callback);
    }
}

impl Playground {
    /// After a successful local commit: run one live advance-cycle for the
    /// ledger (detached; the driver coalesces). No-op off-wasm — the native
    /// build has no event loop to drive detached work.
    fn advance_live(&self, ledger_id: &str) {
        #[cfg(target_arch = "wasm32")]
        self.live.advance_detached(ledger_id);
        #[cfg(not(target_arch = "wasm32"))]
        let _ = ledger_id;
    }

    /// Coarse input-size pre-gate for the transact paths (PR-1715 review):
    /// the memory budget instruments only query execution — staging a
    /// transaction has no engine-side budget yet, and a large JSON body
    /// expands several-fold into parsed values, staged flakes, and novelty.
    /// Refuse inputs whose size alone makes an allocator trap plausible:
    /// ¼ of the budget approximates "input × expansion ≥ budget". Coarse by
    /// design; the real fix is a transact-side budget in the engine.
    fn transact_pregate(&self, what: &str, len: usize) -> Result<(), JsValue> {
        let Some(budget) = self.max_memory_bytes else {
            return Ok(());
        };
        let cap = budget / 4;
        if len > cap {
            return Err(js_error(
                error::code::OUT_OF_MEMORY,
                507,
                &format!(
                    "{what} is {len} bytes; inputs over {cap} bytes (¼ of the \
                     {budget}-byte memory budget) are refused to avoid trapping \
                     the engine mid-transact — raise maxMemoryBytes or split the \
                     transaction"
                ),
            ));
        }
        Ok(())
    }
}

fn receipt_json(out: &TransactResultRef) -> String {
    json!({
        "t": out.receipt.t,
        "commit": out.receipt.commit_id.to_string(),
        "flakes": out.receipt.flake_count,
    })
    .to_string()
}
