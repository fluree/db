//! Internal helpers that compose the nameservice's branched store helpers
//! with the recurring "I have an `Option<NsRecord>` plus a fallback id" or
//! "load and parse a default-context blob" patterns.
//!
//! These exist so the API + provider call sites all funnel through one
//! decision instead of inlining the same `match record { Some => …, None
//! => … }` block at every place that wants a branch-aware store.

use std::sync::Arc;

use fluree_db_core::{ContentStore, StorageBackend};
use fluree_db_nameservice::{NameService, NsRecord};

use crate::error::{ApiError, Result};

/// Resolve a content store for code that has an `Option<NsRecord>` in hand
/// plus a fallback ledger id to use when the record is absent.
///
/// - `Some(record)`: returns a branch-aware store via
///   [`fluree_db_nameservice::branched_content_store_for_record`]. Flat
///   for non-branched records, recursive `BranchedContentStore` for
///   branched ones.
/// - `None`: falls back to the flat `backend.content_store(fallback_id)`.
///   This branch is hit by code paths that haven't loaded a record yet
///   (e.g. early bootstrap) and want best-effort flat behavior.
pub(crate) async fn content_store_for_record_or_id(
    backend: &StorageBackend,
    nameservice: &dyn NameService,
    record: Option<&NsRecord>,
    fallback_id: &str,
) -> Result<Arc<dyn ContentStore>> {
    match record {
        Some(r) => {
            Ok(
                fluree_db_nameservice::branched_content_store_for_record(backend, nameservice, r)
                    .await?,
            )
        }
        None => Ok(backend.content_store(fallback_id)),
    }
}

/// Read and parse a ledger's `default_context` blob (a JSON object) from
/// CAS, using a branch-aware store so branches that inherit their
/// parent's context CID can resolve it under the source branch's
/// namespace.
///
/// Returns:
/// - `Ok(None)` if `record.default_context` is `None`.
/// - `Ok(Some(value))` if the blob loads and parses as JSON.
/// - `Err(...)` on read or parse failure. Callers that want soft-fail
///   semantics should match on the result and log/discard the error
///   themselves — keeping the policy at the call site lets `lib.rs`
///   surface the error to the user while `loading.rs` and the view
///   path swallow it.
pub(crate) async fn load_default_context_blob(
    backend: &StorageBackend,
    nameservice: &dyn NameService,
    record: &NsRecord,
) -> Result<Option<serde_json::Value>> {
    let Some(ctx_id) = record.default_context.as_ref() else {
        return Ok(None);
    };
    let cs = fluree_db_nameservice::branched_content_store_for_record(backend, nameservice, record)
        .await?;
    let bytes = cs
        .get(ctx_id)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read default context {ctx_id}: {e}")))?;
    let value = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::internal(format!("failed to parse default context JSON: {e}")))?;
    Ok(Some(value))
}
