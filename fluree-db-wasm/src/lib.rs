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
//!   [`Playground::snapshot`] pins a `GraphDb` view under an integer handle
//!   and every query names a handle, so a head advance — or, in the future
//!   peer mode, the fetch-and-re-run miss loop — can never see the view move
//!   mid-query. Only buffered results are exposed; the engine's streaming
//!   entry (`run_stream_query`) is deliberately not bound, because rows
//!   emitted before completion cannot participate in that re-run loop.
//! - **One memory setting** (adversarial review F4): the constructor takes an
//!   optional byte ceiling that is applied to every query as its memory
//!   budget (`QueryCancellation::set_memory_limit`), so an oversized query
//!   fails with a typed `out_of_memory` (HTTP-style 507) instead of growing
//!   linear memory until the allocator traps and kills the worker. When the
//!   browser-io crate lands its engine-wide memory governor, this becomes the
//!   single number handed to it.
//! - **JSON in, JSON out.** Inputs arrive as JSON text (SPARQL is passed as
//!   the query string itself). Query results leave as UTF-8 JSON *bytes*
//!   (`Vec<u8>` → `Uint8Array`) so the worker can hand the buffer to the main
//!   thread as a transferable with zero copies at the boundary; small
//!   metadata (receipts, ledger/snapshot info) leaves as a JSON string.
//! - **Playground = memory ledgers.** [`Playground`] wraps
//!   `FlureeBuilder::memory().build_memory()`: the in-process, novelty-only
//!   engine with `IndexingMode::Disabled`. Everything lives in linear memory
//!   for the life of the worker. The peer/cache mode (remote CID-verified
//!   blocks, IndexedDB/OPFS cache) is a separate constructor on the same JS
//!   surface, built in `fluree-db-browser`, and is not wired here yet.

#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

mod error;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_api::{
    Fluree, FlureeBuilder, GraphDb, GraphSnapshotQueryBuilder, QueryExecutionOptions,
    TransactResultRef,
};
use serde_json::{json, Value as JsonValue};
use wasm_bindgen::prelude::*;

use crate::error::{api_error, invalid_json, js_error, serialize_failed};

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
    fluree: Fluree,
    /// Per-query memory budget in bytes; `None` = the engine's process
    /// default (1 GiB on wasm32). See F4 note in the module docs.
    max_memory_bytes: Option<usize>,
    /// Frozen `GraphDb` views, keyed by the handle handed to JS.
    snapshots: RefCell<HashMap<u32, Arc<GraphDb>>>,
    next_handle: Cell<u32>,
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
        Playground {
            fluree: FlureeBuilder::memory().build_memory(),
            max_memory_bytes: max_memory_bytes
                .filter(|b| b.is_finite() && *b >= 1.0)
                .map(|b| b as usize),
            snapshots: RefCell::new(HashMap::new()),
            next_handle: Cell::new(1),
        }
    }

    /// Create a ledger. `ledger_id` is normalized to `name:branch`
    /// (`"demo"` → `"demo:main"`). Rejects with `conflict` if it exists.
    /// Resolves to `{"id","t","indexT"}` as a JSON string.
    #[wasm_bindgen(js_name = createLedger)]
    pub async fn create_ledger(&self, ledger_id: String) -> Result<String, JsValue> {
        let state = self
            .fluree
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
        let state = self.fluree.ledger(&ledger_id).await.map_err(api_error)?;
        Ok(ledger_info_json(
            state.ledger_id(),
            state.t(),
            state.index_t(),
        ))
    }

    /// Freeze the ledger's current head as a queryable snapshot. Resolves to
    /// `{"handle","id","t"}`; the handle stays valid — and the view immutable,
    /// later commits notwithstanding — until [`Self::release`].
    pub async fn snapshot(&self, ledger_id: String) -> Result<String, JsValue> {
        let view = self.fluree.db(&ledger_id).await.map_err(api_error)?;
        let handle = self.next_handle.get();
        self.next_handle.set(handle.wrapping_add(1));
        let info = json!({ "handle": handle, "id": view.ledger_id.as_ref(), "t": view.t });
        self.snapshots.borrow_mut().insert(handle, Arc::new(view));
        Ok(info.to_string())
    }

    /// Drop a snapshot. Returns whether the handle existed. Never errors —
    /// releasing twice is a no-op.
    pub fn release(&self, snapshot: u32) -> bool {
        self.snapshots.borrow_mut().remove(&snapshot).is_some()
    }

    /// Insert JSON-LD (`data` is the JSON text of a node, node array, or
    /// `{"@context", "@graph"}` document). Resolves to a commit receipt.
    pub async fn insert(&self, ledger_id: String, data: String) -> Result<String, JsValue> {
        self.transact_pregate("insert body", data.len())?;
        let data = parse_json("transaction body", &data)?;
        let out = self
            .fluree
            .graph(&ledger_id)
            .transact()
            .insert(&data)
            .commit()
            .await
            .map_err(api_error)?;
        Ok(receipt_json(&out))
    }

    /// Upsert JSON-LD: like `insert`, but existing values of single-cardinality
    /// properties on the same subject are replaced instead of accumulated.
    pub async fn upsert(&self, ledger_id: String, data: String) -> Result<String, JsValue> {
        self.transact_pregate("upsert body", data.len())?;
        let data = parse_json("transaction body", &data)?;
        let out = self
            .fluree
            .graph(&ledger_id)
            .transact()
            .upsert(&data)
            .commit()
            .await
            .map_err(api_error)?;
        Ok(receipt_json(&out))
    }

    /// JSON-LD update: a `{"where", "delete", "insert"}` document (JSON text).
    pub async fn update(&self, ledger_id: String, data: String) -> Result<String, JsValue> {
        self.transact_pregate("update document", data.len())?;
        let data = parse_json("update document", &data)?;
        let out = self
            .fluree
            .graph(&ledger_id)
            .transact()
            .update(&data)
            .commit()
            .await
            .map_err(api_error)?;
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
            .fluree
            .graph(&ledger_id)
            .transact()
            .sparql_update(&sparql)
            .commit()
            .await
            .map_err(api_error)?;
        Ok(receipt_json(&out))
    }

    /// Run a SPARQL query against a snapshot handle. Resolves to UTF-8 JSON
    /// bytes: W3C SPARQL Results JSON for SELECT/ASK, JSON-LD for
    /// CONSTRUCT/DESCRIBE — the same shapes the HTTP `/query` route returns.
    #[wasm_bindgen(js_name = querySparql)]
    pub async fn query_sparql(&self, snapshot: u32, sparql: String) -> Result<Vec<u8>, JsValue> {
        let view = self.view(snapshot)?;
        let result = GraphSnapshotQueryBuilder::new_from_parts(&self.fluree, &view)
            .sparql(&sparql)
            .execution_options(self.exec_options())
            .execute_formatted()
            .await
            .map_err(api_error)?;
        to_bytes(&result)
    }

    /// Run a JSON-LD query (`query` is the JSON text of the query object)
    /// against a snapshot handle. Resolves to UTF-8 JSON bytes in Fluree's
    /// JSON-LD result format.
    #[wasm_bindgen(js_name = queryJsonld)]
    pub async fn query_jsonld(&self, snapshot: u32, query: String) -> Result<Vec<u8>, JsValue> {
        let query = parse_json("query", &query)?;
        let view = self.view(snapshot)?;
        let result = GraphSnapshotQueryBuilder::new_from_parts(&self.fluree, &view)
            .jsonld(&query)
            .execution_options(self.exec_options())
            .execute_formatted()
            .await
            .map_err(api_error)?;
        to_bytes(&result)
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

impl Playground {
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

    /// Clone the pinned view out of the slab; the borrow ends before any await.
    fn view(&self, handle: u32) -> Result<Arc<GraphDb>, JsValue> {
        self.snapshots
            .borrow()
            .get(&handle)
            .cloned()
            .ok_or_else(|| {
                js_error(
                    error::code::NOT_FOUND,
                    404,
                    &format!("snapshot handle {handle} was released or never existed"),
                )
            })
    }

    fn exec_options(&self) -> QueryExecutionOptions {
        let mut opts = QueryExecutionOptions::default();
        if let Some(limit) = self.max_memory_bytes {
            let cancellation = fluree_db_core::QueryCancellation::new();
            cancellation.set_memory_limit(limit);
            opts.cancellation = Some(cancellation);
        }
        opts
    }
}

fn parse_json(what: &str, text: &str) -> Result<JsonValue, JsValue> {
    serde_json::from_str(text).map_err(|e| invalid_json(what, e))
}

fn to_bytes(value: &JsonValue) -> Result<Vec<u8>, JsValue> {
    serde_json::to_vec(value).map_err(serialize_failed)
}

fn ledger_info_json(id: &str, t: i64, index_t: i64) -> String {
    json!({ "id": id, "t": t, "indexT": index_t }).to_string()
}

fn receipt_json(out: &TransactResultRef) -> String {
    json!({
        "t": out.receipt.t,
        "commit": out.receipt.commit_id.to_string(),
        "flakes": out.receipt.flake_count,
    })
    .to_string()
}
