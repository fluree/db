//! Branch-aware content store construction.
//!
//! When a ledger branch is forked, commits that predate the fork remain in the
//! source branch's storage namespace. A flat content store scoped to the new
//! branch can find commits the branch produced, but 404s on any pre-fork
//! ancestor — which breaks every commit-chain walk that crosses the fork
//! point (incremental indexing, query-cache catch-up).
//!
//! The helpers in this module build a [`BranchedContentStore`] that reads
//! from the branch's own namespace first and falls back through parent
//! namespaces by following each [`NsRecord::source_branch`] pointer.
//! For non-branched ledgers, a flat namespace store is returned unchanged.

use std::sync::Arc;

use fluree_db_core::{format_ledger_id, BranchedContentStore, ContentStore, StorageBackend};

use crate::{NameService, NameServiceError, NsRecord, Result};

/// Build a content store for `record`, walking branch ancestry on read miss.
///
/// Returns the flat namespace store from
/// [`StorageBackend::content_store`] when `record.source_branch` is `None`.
/// Otherwise wraps it in a [`BranchedContentStore`] whose parent chain is
/// constructed by recursively looking up each ancestor branch in `ns`.
///
/// Prefer this over [`branched_content_store_for_id`] when an `NsRecord`
/// is already in hand — it avoids one extra nameservice round-trip on the
/// common (non-branched) path.
pub async fn branched_content_store_for_record(
    backend: &StorageBackend,
    ns: &dyn NameService,
    record: &NsRecord,
) -> Result<Arc<dyn ContentStore>> {
    if record.source_branch.is_none() {
        return Ok(backend.content_store(&record.ledger_id));
    }
    let branched = build_branched_store(backend, ns, record).await?;
    Ok(Arc::new(branched))
}

/// Build a content store for `ledger_id`, looking up its ancestry.
///
/// Variant of [`branched_content_store_for_record`] that performs the
/// nameservice lookup itself. Use this when no `NsRecord` is in hand;
/// otherwise prefer the `_for_record` variant to skip the extra lookup.
pub async fn branched_content_store_for_id(
    backend: &StorageBackend,
    ns: &dyn NameService,
    ledger_id: &str,
) -> Result<Arc<dyn ContentStore>> {
    let record = ns
        .lookup(ledger_id)
        .await?
        .ok_or_else(|| NameServiceError::not_found(ledger_id))?;
    branched_content_store_for_record(backend, ns, &record).await
}

/// Recursively assemble the [`BranchedContentStore`] for a branched record.
///
/// Public so [`fluree_db_ledger::LedgerState::build_branched_store`] can
/// delegate here without duplicating the ancestry walk. Callers that just
/// want a `ContentStore` should use [`branched_content_store_for_record`].
///
/// `record` MUST have `source_branch.is_some()`; otherwise this function
/// panics. Non-branched callers should use the
/// [`branched_content_store_for_record`] entry point, which handles the
/// non-branched case before recursing.
pub async fn build_branched_store(
    backend: &StorageBackend,
    ns: &dyn NameService,
    record: &NsRecord,
) -> Result<BranchedContentStore> {
    let source = record
        .source_branch
        .as_deref()
        .expect("build_branched_store called on non-branched record");
    let parent_id = format_ledger_id(&record.name, source);

    let parent_record = ns
        .lookup(&parent_id)
        .await?
        .ok_or_else(|| NameServiceError::not_found(&parent_id))?;

    let parent_store = if parent_record.source_branch.is_some() {
        Box::pin(build_branched_store(backend, ns, &parent_record)).await?
    } else {
        BranchedContentStore::leaf(backend.content_store(&parent_record.ledger_id))
    };

    Ok(BranchedContentStore::with_parents(
        backend.content_store(&record.ledger_id),
        vec![parent_store],
    ))
}
