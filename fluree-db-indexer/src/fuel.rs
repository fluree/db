//! Indexer CAS-write fuel charging.
//!
//! Centralises the rule for billing indexer work to a fuel `Tracker`:
//!
//! - 1 [`INDEX_CAS_WRITE_MICRO_FUEL`] per successful indexer CAS write
//!   (`put`, `put_with_id`, or `content_write_bytes`).
//! - **Additionally**, for `ContentKind::IndexLeaf` writes that contain an
//!   FLI3 leaf with leaflets, 1 [`INDEX_CAS_WRITE_MICRO_FUEL`] per *re-encoded*
//!   leaflet in the leaf. Passthrough leaflets (byte-copied from a prior leaf
//!   in an incremental update) are not charged because no zstd encoding work
//!   was performed.
//!
//! The thin `charged_*` wrappers exist so call sites stay one line and the
//! "charge after a successful write" invariant is enforced in one place. New
//! CAS write sites in the indexer should go through these wrappers.
//!
//! Indexer trackers are expected to be **no-limit** (measurement, not
//! enforcement). A partial index is worse than a slow one, so we never want a
//! mid-build abort. The wrappers still propagate `FuelExceededError` for type
//! safety, surfaced via [`crate::IndexerError::FuelExceeded`].

use fluree_db_core::tracking::{schedule::INDEX_CAS_WRITE_MICRO_FUEL, Tracker};
use fluree_db_core::{ContentId, ContentKind, ContentStore};

use crate::{IndexerError, Result};

/// Charge the per-CAS-write fuel for a write that has already succeeded.
///
/// `leaflet_count` is the *re-encoded* leaflet count for FLI3 leaf writes (0
/// for any other kind, including non-FLI3 blobs tagged as `IndexLeaf` such as
/// fulltext segments). Prefer the `charged_*` wrappers below; this is exposed
/// for the rare site that must call `ContentStore` directly.
pub fn charge_index_write(tracker: &Tracker, leaflet_count: u32) -> Result<()> {
    tracker.consume_fuel(INDEX_CAS_WRITE_MICRO_FUEL)?;
    if leaflet_count > 0 {
        let extra = (leaflet_count as u64).saturating_mul(INDEX_CAS_WRITE_MICRO_FUEL);
        tracker.consume_fuel(extra)?;
    }
    Ok(())
}

/// `ContentStore::put` + per-write fuel charge.
///
/// For `IndexLeaf` writes prefer [`charged_put_leaf`]; this helper passes 0
/// for the leaflet count and so under-bills FLI3 leaves.
pub async fn charged_put(
    store: &dyn ContentStore,
    tracker: &Tracker,
    kind: ContentKind,
    bytes: &[u8],
) -> Result<ContentId> {
    let cid = store
        .put(kind, bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    charge_index_write(tracker, 0)?;
    Ok(cid)
}

/// `ContentStore::put` for an `IndexLeaf` blob, charging the base write fee
/// plus the per-leaflet fee for `re_encoded_leaflet_count` leaflets.
pub async fn charged_put_leaf(
    store: &dyn ContentStore,
    tracker: &Tracker,
    bytes: &[u8],
    re_encoded_leaflet_count: u32,
) -> Result<ContentId> {
    let cid = store
        .put(ContentKind::IndexLeaf, bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    charge_index_write(tracker, re_encoded_leaflet_count)?;
    Ok(cid)
}

/// `ContentStore::put_with_id` + per-write fuel charge.
///
/// `leaflet_count` should be 0 for any kind other than an FLI3 `IndexLeaf`.
pub async fn charged_put_with_id(
    store: &dyn ContentStore,
    tracker: &Tracker,
    id: &ContentId,
    bytes: &[u8],
    leaflet_count: u32,
) -> Result<()> {
    store
        .put_with_id(id, bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    charge_index_write(tracker, leaflet_count)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::tracking::{micro_to_fuel, TrackingOptions};

    fn enabled_tracker() -> Tracker {
        Tracker::new(TrackingOptions {
            track_fuel: true,
            ..Default::default()
        })
    }

    fn used(tracker: &Tracker) -> f64 {
        tracker
            .tally()
            .and_then(|t| t.fuel)
            .unwrap_or_else(|| micro_to_fuel(0))
    }

    #[test]
    fn base_write_no_leaflets_charges_one_fuel() {
        let t = enabled_tracker();
        charge_index_write(&t, 0).unwrap();
        assert_eq!(used(&t), 1.0);
    }

    #[test]
    fn leaf_with_three_re_encoded_leaflets_charges_four_fuel() {
        // 1 (write) + 3 (leaflets) = 4 fuel.
        let t = enabled_tracker();
        charge_index_write(&t, 3).unwrap();
        assert_eq!(used(&t), 4.0);
    }

    #[test]
    fn disabled_tracker_is_noop() {
        let t = Tracker::disabled();
        charge_index_write(&t, 100).unwrap();
        // tally() returns None on a disabled tracker; the call must not panic
        // and must succeed regardless of the leaflet count.
        assert!(t.tally().is_none());
    }
}
