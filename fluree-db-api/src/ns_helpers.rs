//! Namespace reconciliation helpers shared across load paths.
//!
//! These helpers deduplicate the pattern of syncing namespace codes between
//! a `BinaryIndexStore` (index root) and a `LedgerSnapshot` (commit chain),
//! enforcing that the commit chain is the namespace source of truth and that
//! the bimap uniqueness / immutability invariant holds.

use crate::error::{ApiError, Result};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::LedgerSnapshot;
use fluree_db_ledger::LedgerState;

/// Sync namespace codes between a `BinaryIndexStore` and a `LedgerSnapshot`.
///
/// Performs three operations in order:
/// 1. **Sets** the store's split mode to match the snapshot (must happen before
///    any `encode_iri` calls on the store).
/// 2. **Augments** the store with any namespace codes from the snapshot that the
///    store doesn't have (post-index allocations from novelty commits).
/// 3. **Reconciles** the store's namespace codes back into the snapshot, validating
///    that no conflicts exist. Any new codes from the index root are inserted into
///    the snapshot (keeps both forward and reverse maps in sync).
///
/// Returns `Err` if a namespace conflict is detected (indicates an indexer/publisher
/// bug or storage corruption).
pub fn sync_store_and_snapshot_ns(
    store: &mut BinaryIndexStore,
    snapshot: &mut LedgerSnapshot,
) -> Result<()> {
    // 1. Sync split mode first — ensures any subsequent encode_iri calls use
    //    the correct canonical split.
    store.set_ns_split_mode(snapshot.ns_split_mode());

    // 2. Augment the store with snapshot namespace codes (post-index allocations).
    store
        .augment_namespace_codes(snapshot.namespaces())
        .map_err(|e| ApiError::internal(format!("augment namespace codes: {e}")))?;

    // 3. Reconcile store codes back into snapshot (validation + insert).
    //    Check both directions of the bimap to detect conflicts:
    //    - code→prefix: store's code already in snapshot with a different prefix
    //    - prefix→code: store's prefix already in snapshot under a different code
    for (code, prefix) in store.namespace_codes() {
        // Forward check: same code, different prefix?
        if let Some(existing_prefix) = snapshot.namespaces().get(code) {
            if existing_prefix != prefix {
                return Err(ApiError::internal(format!(
                    "namespace reconciliation failure: index root ns code {code} maps to {prefix:?} \
                     but commit chain has {existing_prefix:?} — possible indexer/publisher bug"
                )));
            }
        }
        // Reverse check: same prefix, different code?
        if let Some(&existing_code) = snapshot.namespace_reverse().get(prefix.as_str()) {
            if existing_code != *code {
                return Err(ApiError::internal(format!(
                    "namespace reconciliation failure: prefix {prefix:?} has code {code} in index root \
                     but code {existing_code} in commit chain — possible indexer/publisher bug"
                )));
            }
        }
        snapshot
            .insert_namespace_code(*code, prefix.clone())
            .map_err(|e| ApiError::internal(format!("namespace reconciliation: {e}")))?;
    }

    Ok(())
}

pub fn binary_store_missing_snapshot_namespaces(state: &LedgerState) -> bool {
    let Some(store) = state
        .binary_store
        .as_ref()
        .and_then(|te| te.0.downcast_ref::<BinaryIndexStore>())
    else {
        return false;
    };

    state
        .snapshot
        .namespaces()
        .iter()
        .any(|(code, prefix)| store.namespace_codes().get(code) != Some(prefix))
}
