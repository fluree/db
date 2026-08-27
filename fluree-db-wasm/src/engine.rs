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
    ) -> Result<Vec<u8>, JsValue> {
        let view = self.view(snapshot)?;
        let result = GraphSnapshotQueryBuilder::new_from_parts(&self.fluree, &view)
            .sparql(sparql)
            .execution_options(self.exec_options())
            .execute_formatted()
            .await
            .map_err(api_error)?;
        to_bytes(&result)
    }

    pub(crate) async fn query_jsonld(
        &self,
        snapshot: u32,
        query_text: &str,
    ) -> Result<Vec<u8>, JsValue> {
        let query = parse_json("query", query_text)?;
        let view = self.view(snapshot)?;
        let result = GraphSnapshotQueryBuilder::new_from_parts(&self.fluree, &view)
            .jsonld(&query)
            .execution_options(self.exec_options())
            .execute_formatted()
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

    fn exec_options(&self) -> QueryExecutionOptions {
        let mut opts = QueryExecutionOptions::default();
        if let Some(limit) = self.query_budget_bytes {
            let cancellation = fluree_db_core::QueryCancellation::new();
            cancellation.set_memory_limit(limit);
            opts.cancellation = Some(cancellation);
        }
        opts
    }
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
