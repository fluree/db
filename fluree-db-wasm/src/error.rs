//! `ApiError` → JS `Error` with a stable `code` and HTTP-style `status`.
//!
//! The main-thread proxy re-raises these as `FlureeError { code, status }`;
//! the codes are the JS-facing taxonomy, deliberately coarser than
//! `ApiError`'s variants so the peer-mode transport can reuse them
//! (`unauthorized`, `network`) without the playground ever producing them.

use fluree_db_api::ApiError;
use fluree_db_core::QueryCancellationReason;
use fluree_db_query::QueryError;
use wasm_bindgen::JsValue;

/// Stable, machine-readable error codes crossing the worker boundary.
pub(crate) mod code {
    pub const NOT_FOUND: &str = "not_found";
    pub const CONFLICT: &str = "conflict";
    pub const INVALID_INPUT: &str = "invalid_input";
    pub const CANCELLED: &str = "cancelled";
    /// A query passed its wall-clock timeout and was aborted *typed* (F3) —
    /// the engine survives. `api::error` already maps a cancelled query to
    /// HTTP 408, so only the code distinguishes a timeout from a caller cancel.
    pub const TIMEOUT: &str = "timeout";
    /// A query crossed its memory budget and was aborted *typed* — the engine
    /// survives. The JS shell uses the same code (with `fatal: true`) when the
    /// allocator traps instead.
    pub const OUT_OF_MEMORY: &str = "out_of_memory";
    pub const UNSUPPORTED: &str = "unsupported";
    pub const INTERNAL: &str = "internal";
}

fn code_for(err: &ApiError) -> &'static str {
    if err.is_not_found() {
        return code::NOT_FOUND;
    }
    match err {
        ApiError::LedgerExists(_) => code::CONFLICT,
        ApiError::Parse(_)
        | ApiError::Sparql { .. }
        | ApiError::SparqlLower(_)
        | ApiError::Cypher { .. }
        | ApiError::CypherLower(_)
        | ApiError::CypherUpdateLower(_)
        | ApiError::Turtle(_)
        | ApiError::Builder(_) => code::INVALID_INPUT,
        ApiError::Query(QueryError::Cancelled {
            reason: QueryCancellationReason::Timeout,
        }) => code::TIMEOUT,
        ApiError::Query(QueryError::Cancelled { .. }) => code::CANCELLED,
        ApiError::Query(QueryError::MemoryBudgetExceeded { .. }) => code::OUT_OF_MEMORY,
        _ => match err.status_code() {
            400..=499 => code::INVALID_INPUT,
            501 => code::UNSUPPORTED,
            _ => code::INTERNAL,
        },
    }
}

/// Build a JS `Error` carrying `code` and `status` own-properties.
pub(crate) fn js_error(code: &str, status: u16, message: &str) -> JsValue {
    let e = js_sys::Error::new(message);
    // Reflect::set only fails on non-objects; `e` is always an object.
    let _ = js_sys::Reflect::set(&e, &JsValue::from_str("code"), &JsValue::from_str(code));
    let _ = js_sys::Reflect::set(
        &e,
        &JsValue::from_str("status"),
        &JsValue::from_f64(f64::from(status)),
    );
    e.into()
}

pub(crate) fn api_error(err: ApiError) -> JsValue {
    js_error(code_for(&err), err.status_code(), &err.to_string())
}

/// Malformed JSON handed in from JS (query object or transaction body).
pub(crate) fn invalid_json(what: &str, err: serde_json::Error) -> JsValue {
    js_error(code::INVALID_INPUT, 400, &format!("{what}: {err}"))
}

/// Serializing an engine-produced result — cannot fail for `serde_json::Value`
/// input, mapped anyway rather than unwrapped.
pub(crate) fn serialize_failed(err: serde_json::Error) -> JsValue {
    js_error(code::INTERNAL, 500, &format!("result serialization: {err}"))
}

#[cfg(test)]
mod tests {
    use super::{code, code_for};
    use fluree_db_api::ApiError;
    use fluree_db_core::QueryCancellationReason;
    use fluree_db_query::QueryError;

    fn cancelled(reason: QueryCancellationReason) -> ApiError {
        ApiError::Query(QueryError::Cancelled { reason })
    }

    #[test]
    fn timeout_cancellation_maps_to_the_timeout_code_not_cancelled() {
        // F3: a query aborted by its wall-clock timeout must surface a distinct
        // `timeout` code so the JS surface can tell it apart from a caller
        // cancel or a disconnect (all three are one QueryError::Cancelled).
        assert_eq!(code_for(&cancelled(QueryCancellationReason::Timeout)), code::TIMEOUT);
        assert_eq!(
            code_for(&cancelled(QueryCancellationReason::Cancelled)),
            code::CANCELLED,
        );
        assert_eq!(
            code_for(&cancelled(QueryCancellationReason::ClientDisconnected)),
            code::CANCELLED,
        );
    }
}
