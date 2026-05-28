//! Indexer CAS-write fuel charging.
//!
//! Centralises the rule for billing indexer work to a fuel `Tracker`:
//!
//! - 1 [`INDEX_CAS_WRITE_MICRO_FUEL`] per successful indexer CAS write
//!   (`ContentStore::put` or `put_with_id`). The lower-level
//!   `Storage::content_write_bytes` path is not wrapped — it has no active
//!   indexer call site today.
//! - **Additionally**, for FLI3 leaf writes, 1 [`INDEX_CAS_WRITE_MICRO_FUEL`]
//!   per *re-encoded* leaflet in the leaf. Passthrough leaflets (byte-copied
//!   from a prior leaf in an incremental update) are not charged because no
//!   zstd encoding work was performed.
//!
//! ## Wiring
//!
//! The build entry points wrap the caller-supplied [`ContentStore`] in
//! [`MeteredContentStore`] once at the boundary. From that point on the entire
//! build pipeline writes through the wrapper, and every `put` /
//! `put_with_id` is automatically billed at the base per-write rate — so new
//! upload sites need no changes to be billed correctly. Only the **two** FLI3
//! leaf upload sites (`upload_indexes_to_cas` for full rebuild and
//! `upload_leaf_blobs` for incremental) need an explicit extra call to
//! [`charge_extra_leaflets`] for the per-leaflet portion, because the wrapper
//! has no way to know how many leaflets in the blob were freshly encoded.
//!
//! ## Limits
//!
//! Indexer trackers are expected to be **no-limit** (measurement, not
//! enforcement). A partial index is worse than a slow one, so we never want a
//! mid-build abort. The wrappers still propagate [`FuelExceededError`] via
//! [`crate::IndexerError::FuelExceeded`] for type safety.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_core::error::Result as StorageResult;
use fluree_db_core::tracking::{schedule::INDEX_CAS_WRITE_MICRO_FUEL, Tracker};
use fluree_db_core::{ContentId, ContentKind, ContentStore};

use crate::Result;

/// Charge the per-CAS-write fuel for a write that has already succeeded.
///
/// `leaflet_count` is the *re-encoded* leaflet count to charge in **addition**
/// to the base write fee. Pass 0 for non-leaf writes.
pub fn charge_index_write(tracker: &Tracker, leaflet_count: u32) -> Result<()> {
    tracker.consume_fuel(INDEX_CAS_WRITE_MICRO_FUEL)?;
    charge_extra_leaflets(tracker, leaflet_count)
}

/// Charge the per-leaflet extra fuel for an FLI3 leaf write that has already
/// succeeded through a [`MeteredContentStore`] (which has already charged the
/// base write fee).
///
/// `re_encoded_leaflet_count` is the number of leaflets inside the leaf that
/// were freshly encoded — passthrough byte-copies are not charged because no
/// zstd encoding work was performed. Pass 0 to no-op.
pub fn charge_extra_leaflets(tracker: &Tracker, re_encoded_leaflet_count: u32) -> Result<()> {
    if re_encoded_leaflet_count > 0 {
        let extra = (re_encoded_leaflet_count as u64).saturating_mul(INDEX_CAS_WRITE_MICRO_FUEL);
        tracker.consume_fuel(extra)?;
    }
    Ok(())
}

/// A [`ContentStore`] wrapper that charges fuel for every successful CAS
/// write the indexer makes through it.
///
/// Delegates all read paths (`has`, `get`, `get_range`, `release`,
/// `resolve_local_path`) and the `put` / `put_with_id` writes to the inner
/// store. After each successful write the wrapper charges
/// [`INDEX_CAS_WRITE_MICRO_FUEL`] against the tracker — once per CAS call,
/// regardless of `ContentKind`.
///
/// FLI3 leaf writes need an **extra** per-leaflet charge that the wrapper
/// cannot compute from blob bytes alone (it cannot distinguish passthrough
/// from re-encoded leaflets). The two leaf upload sites
/// (`build::upload::upload_indexes_to_cas` and
/// `build::incremental::upload_leaf_blobs`) add that charge explicitly via
/// [`charge_extra_leaflets`] using the
/// [`re_encoded_leaflet_count`](fluree_db_binary_index::format::leaf::LeafInfo::re_encoded_leaflet_count)
/// they already carry.
///
/// On a failed write the underlying store's error is propagated and no fuel
/// is charged.
pub struct MeteredContentStore {
    inner: Arc<dyn ContentStore>,
    tracker: Tracker,
}

impl MeteredContentStore {
    pub fn new(inner: Arc<dyn ContentStore>, tracker: Tracker) -> Self {
        Self { inner, tracker }
    }

    /// The wrapped store, primarily for paths that need to clone the inner
    /// `Arc<dyn ContentStore>` directly (e.g. plumbing into helpers that
    /// don't go through the wrapper).
    pub fn inner(&self) -> &Arc<dyn ContentStore> {
        &self.inner
    }

    /// The tracker the wrapper charges against. Exposed so leaf upload sites
    /// can call [`charge_extra_leaflets`] for the per-leaflet portion.
    pub fn tracker(&self) -> &Tracker {
        &self.tracker
    }
}

impl fmt::Debug for MeteredContentStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MeteredContentStore")
            .field("inner", &self.inner)
            .field("tracker_enabled", &self.tracker.is_enabled())
            .finish()
    }
}

#[async_trait]
impl ContentStore for MeteredContentStore {
    async fn has(&self, id: &ContentId) -> StorageResult<bool> {
        self.inner.has(id).await
    }

    async fn get(&self, id: &ContentId) -> StorageResult<Vec<u8>> {
        self.inner.get(id).await
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> StorageResult<ContentId> {
        let cid = self.inner.put(kind, bytes).await?;
        // Tracker is no-limit by design; FuelExceededError → core::Error via
        // the trait's existing From impl preserves the trait contract.
        self.tracker.consume_fuel(INDEX_CAS_WRITE_MICRO_FUEL)?;
        Ok(cid)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> StorageResult<()> {
        self.inner.put_with_id(id, bytes).await?;
        self.tracker.consume_fuel(INDEX_CAS_WRITE_MICRO_FUEL)?;
        Ok(())
    }

    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        self.inner.resolve_local_path(id)
    }

    async fn release(&self, id: &ContentId) -> StorageResult<()> {
        self.inner.release(id).await
    }

    async fn get_range(
        &self,
        id: &ContentId,
        range: std::ops::Range<u64>,
    ) -> StorageResult<Vec<u8>> {
        self.inner.get_range(id, range).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::tracking::{micro_to_fuel, TrackingOptions};
    use fluree_db_core::storage::MemoryContentStore;

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
        assert!(t.tally().is_none());
    }

    #[tokio::test]
    async fn metered_store_charges_one_fuel_per_put() {
        let inner: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let tracker = enabled_tracker();
        let metered = MeteredContentStore::new(inner, tracker.clone());

        metered
            .put(ContentKind::IndexBranch, b"branch-bytes")
            .await
            .unwrap();
        assert_eq!(used(&tracker), 1.0);

        // Second write adds another fuel.
        metered
            .put(ContentKind::GarbageRecord, b"{}")
            .await
            .unwrap();
        assert_eq!(used(&tracker), 2.0);
    }

    #[tokio::test]
    async fn metered_store_plus_extra_leaflets_for_fli3_leaf() {
        let inner: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let tracker = enabled_tracker();
        let metered = MeteredContentStore::new(inner, tracker.clone());

        // Simulate an FLI3 leaf upload: 1 fuel for the put + 5 fuel for the
        // re-encoded leaflets inside it.
        metered
            .put(ContentKind::IndexLeaf, b"leaf-bytes")
            .await
            .unwrap();
        charge_extra_leaflets(metered.tracker(), 5).unwrap();
        assert_eq!(used(&tracker), 6.0);
    }
}
