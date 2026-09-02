//! The mode-agnostic engine surface: a `Fluree` plus the frozen-snapshot
//! slab and the per-query memory budget, shared by [`crate::Playground`]
//! (memory ledgers, read-write) and the peer mode (remote ledgers,
//! read-only). Everything here is plain Rust — the wasm-bindgen classes
//! delegate to it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

use fluree_db_api::{Fluree, GraphDb, GraphSnapshotQueryBuilder, QueryExecutionOptions};
use serde_json::{json, Value as JsonValue};
use wasm_bindgen::JsValue;

use crate::error::{self, api_error, invalid_json, js_error, serialize_failed};

pub(crate) struct EngineCore {
    fluree: Fluree,
    /// Per-query memory budget in bytes; `None` = the engine's process
    /// default (1 GiB on wasm32). See the F4 note in the crate docs.
    query_budget_bytes: Option<usize>,
    /// Frozen `GraphDb` views, keyed by the handle handed to JS.
    snapshots: RefCell<HashMap<u32, Arc<GraphDb>>>,
    next_handle: Cell<u32>,
}

impl EngineCore {
    pub(crate) fn new(fluree: Fluree, query_budget_bytes: Option<usize>) -> Self {
        Self {
            fluree,
            query_budget_bytes,
            snapshots: RefCell::new(HashMap::new()),
            next_handle: Cell::new(1),
        }
    }

    pub(crate) fn fluree(&self) -> &Fluree {
        &self.fluree
    }

    pub(crate) async fn ledger_info(&self, ledger_id: &str) -> Result<String, JsValue> {
        let state = self.fluree.ledger(ledger_id).await.map_err(api_error)?;
        Ok(ledger_info_json(
            state.ledger_id(),
            state.t(),
            state.index_t(),
        ))
    }

    pub(crate) async fn snapshot(&self, ledger_id: &str) -> Result<String, JsValue> {
        let view = self.fluree.db(ledger_id).await.map_err(api_error)?;
        let handle = self.next_handle.get();
        self.next_handle.set(handle.wrapping_add(1));
        let info = json!({ "handle": handle, "id": view.ledger_id.as_ref(), "t": view.t });
        self.snapshots.borrow_mut().insert(handle, Arc::new(view));
        Ok(info.to_string())
    }

    pub(crate) fn release(&self, snapshot: u32) -> bool {
        self.snapshots.borrow_mut().remove(&snapshot).is_some()
    }

    pub(crate) async fn query_sparql(
        &self,
        snapshot: u32,
        sparql: &str,
        timeout_ms: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        let view = self.view(snapshot)?;
        let (opts, cancel) = self.exec_options_with_cancel(timeout_ms.is_some());
        let fut = GraphSnapshotQueryBuilder::new_from_parts(&self.fluree, &view)
            .sparql(sparql)
            .execution_options(opts)
            .execute_formatted();
        let result = run_query_with_timeout(fut, cancel, timeout_ms)
            .await
            .map_err(api_error)?;
        to_bytes(&result)
    }

    pub(crate) async fn query_jsonld(
        &self,
        snapshot: u32,
        query_text: &str,
        timeout_ms: Option<f64>,
    ) -> Result<Vec<u8>, JsValue> {
        let query = parse_json("query", query_text)?;
        let view = self.view(snapshot)?;
        let (opts, cancel) = self.exec_options_with_cancel(timeout_ms.is_some());
        let fut = GraphSnapshotQueryBuilder::new_from_parts(&self.fluree, &view)
            .jsonld(&query)
            .execution_options(opts)
            .execute_formatted();
        let result = run_query_with_timeout(fut, cancel, timeout_ms)
            .await
            .map_err(api_error)?;
        to_bytes(&result)
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

    /// Build execution options for an ad-hoc query, returning the cancellation
    /// handle when one was created so the caller can trip it on timeout (F3). A
    /// handle exists when a memory budget is set OR a timeout was requested;
    /// with neither, the query runs with default options and no handle — the
    /// zero-overhead default path, byte-for-byte the pre-F3 behavior.
    fn exec_options_with_cancel(
        &self,
        want_timeout: bool,
    ) -> (QueryExecutionOptions, Option<fluree_db_core::QueryCancellation>) {
        if self.query_budget_bytes.is_none() && !want_timeout {
            return (QueryExecutionOptions::default(), None);
        }
        let cancellation = fluree_db_core::QueryCancellation::new();
        if let Some(limit) = self.query_budget_bytes {
            cancellation.set_memory_limit(limit);
        }
        let mut opts = QueryExecutionOptions::default();
        opts.cancellation = Some(cancellation.clone());
        (opts, Some(cancellation))
    }
}

/// Execution options carrying the per-query memory budget (F4): a fresh
/// cancellation handle with `set_memory_limit`, or defaults when unbudgeted.
pub(crate) fn make_exec_options(budget_bytes: Option<usize>) -> QueryExecutionOptions {
    let mut opts = QueryExecutionOptions::default();
    if let Some(limit) = budget_bytes {
        let cancellation = fluree_db_core::QueryCancellation::new();
        cancellation.set_memory_limit(limit);
        opts.cancellation = Some(cancellation);
    }
    opts
}

/// Race an ad-hoc query against a wall-clock timeout (F3). When `timeout_ms`
/// elapses first, the shared [`QueryCancellation`](fluree_db_core::QueryCancellation)
/// is tripped with `Timeout` and the query future is awaited to unwind — it
/// returns `QueryError::Cancelled { reason: Timeout }` at its next cooperative
/// checkpoint, which `error::code_for` maps to the JS `timeout` code. The timer
/// lives on the worker's event loop, which the query's own `.await`s (residency
/// fetches from IndexedDB / network in peer mode) yield to, so an I/O-bound
/// query is aborted promptly; a purely CPU-bound stretch with no awaits is
/// bounded only by reaching the next checkpoint (hard-preempting that needs a
/// cross-thread `SharedArrayBuffer` signal — a separate, larger change). With
/// no timeout (or no handle) this is a straight `fut.await` with zero added
/// cost, so the default query path is unchanged.
#[cfg_attr(not(target_arch = "wasm32"), allow(unused_variables))]
async fn run_query_with_timeout<F>(
    fut: F,
    cancellation: Option<fluree_db_core::QueryCancellation>,
    timeout_ms: Option<f64>,
) -> F::Output
where
    F: std::future::Future,
{
    #[cfg(target_arch = "wasm32")]
    {
        if let (Some(ms), Some(cancel)) = (
            timeout_ms.filter(|m| m.is_finite() && *m >= 0.0),
            cancellation.as_ref(),
        ) {
            use futures::future::{select, Either};
            futures::pin_mut!(fut);
            let timer = gloo_timers::future::TimeoutFuture::new(clamp_timeout_millis(ms));
            futures::pin_mut!(timer);
            return match select(fut, timer).await {
                // Query finished first; dropping `timer` clears its setTimeout.
                Either::Left((out, _timer)) => out,
                // Timer fired: trip the shared handle, then let the query unwind
                // at its next checkpoint into the typed Timeout cancellation.
                Either::Right(((), fut)) => {
                    cancel.cancel_with(fluree_db_core::QueryCancellationReason::Timeout);
                    fut.await
                }
            };
        }
    }
    fut.await
}

/// Clamp a JS millisecond timeout so `setTimeout` cannot silently treat it as
/// "fire immediately": browsers store the delay as a signed 32-bit int, so a
/// value past `i32::MAX` wraps negative and fires on the next tick (the browser
/// timer-overflow trap). A ~24.8-day ceiling reads as "effectively no timeout"
/// for a browser query — the right degradation for an absurd input. `ms` is
/// pre-filtered finite and `>= 0.0`, so the cast is exact within range.
#[cfg(target_arch = "wasm32")]
fn clamp_timeout_millis(ms: f64) -> u32 {
    ms.min(f64::from(i32::MAX)) as u32
}

pub(crate) fn parse_json(what: &str, text: &str) -> Result<JsonValue, JsValue> {
    serde_json::from_str(text).map_err(|e| invalid_json(what, e))
}

pub(crate) fn to_bytes(value: &JsonValue) -> Result<Vec<u8>, JsValue> {
    serde_json::to_vec(value).map_err(serialize_failed)
}

pub(crate) fn ledger_info_json(id: &str, t: i64, index_t: i64) -> String {
    json!({ "id": id, "t": t, "indexT": index_t }).to_string()
}

/// Sanitize the JS-provided byte ceiling (`Option<f64>` across the boundary).
pub(crate) fn sanitize_bytes(bytes: Option<f64>) -> Option<usize> {
    bytes
        .filter(|b| b.is_finite() && *b >= 1.0)
        .map(|b| b as usize)
}
