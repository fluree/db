//! How a cluster orders an erasure's publication with its writes.
//!
//! One instance publishes an erasure by writing its map object. Cluster
//! members share that storage but list the map objects on their own clocks,
//! unordered with the writes Raft orders, so a write staged on a view from
//! before the erasure could land after it, and a member could stop reading
//! an original before every holder's writes are refused. A cluster instead
//! writes the prepared erasures as an immutable manifest outside the map
//! objects and activates it through Raft: the activation is refused if a
//! holder changed since preparation, otherwise it advances every holder's
//! erasure generation, so a write from an older view is refused. Each
//! member installs the manifest in activation order; the map object is
//! written once the activation is applied, for instances that only list.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fluree_db_nameservice::NsRecord;

use crate::{ApiError, Fluree, Result};

/// How long a hold keeps the holders' heads while their erasure is prepared
/// again. It lapses by itself if the preparation stops.
pub(crate) const HOLD: Duration = Duration::from_secs(30);

/// A cluster's activation of prepared erasures, proposed on its leader.
#[async_trait]
pub trait ErasureActivator: Send + Sync + std::fmt::Debug {
    /// The holder-set version, which every branch addition advances from
    /// now on. `None` until this member has installed every activation it
    /// has applied: preparation builds on the erasures installed here.
    async fn holder_set(&self) -> Result<Option<u64>>;

    /// Hold head advances on `holders` for `hold`, or until an activation
    /// or [`Self::release`] lifts it.
    async fn hold(&self, holders: &[NsRecord], hold: Duration) -> Result<()>;

    /// Lift the hold on `holders`.
    async fn release(&self, holders: &[NsRecord]) -> Result<()>;

    /// Activate the erasures `manifest` names, prepared against `holders`
    /// as read once the holder set was at `holder_set`. The activation's
    /// number; `None` when a holder was added or changed since.
    async fn activate(
        &self,
        manifest: &str,
        holder_set: u64,
        holders: &[NsRecord],
    ) -> Result<Option<u64>>;

    /// The number of `manifest`'s activation, once applied.
    async fn applied(&self, manifest: &str) -> Option<u64>;

    /// Whether every member has installed activation `number`.
    async fn installed_everywhere(&self, number: u64) -> bool;
    /// Record that the shared storage activation `number` reached is
    /// purged: the log index of the record (the first, for one recorded
    /// again), which members' cleanup reports must reach.
    async fn record_purged(&self, number: u64) -> Result<u64>;
    /// The members, voters and learners, that have not reported cleaning
    /// their own copies after log index `index`.
    async fn not_cleaned(&self, index: u64) -> Vec<u64>;
}

impl Fluree {
    /// Publish this cluster member's erasures through `activator`.
    pub fn set_erasure_activator(&self, activator: Arc<dyn ErasureActivator>) {
        if let Some(runtime) = self.retention.erasure.get() {
            let _ = runtime.activator.set(activator);
        }
    }

    /// What this member's collection has condemned, which its writes check
    /// once activated; `None` without managed storage.
    pub fn condemned(&self) -> Option<Arc<crate::collect::Condemned>> {
        self.retention
            .erasure
            .get()
            .map(|runtime| runtime.condemned.clone())
    }

    /// Clean this cluster member's own copies once the shared storage is
    /// purged: evict the cached state of `holders`, which a load before the
    /// purge's rewrite holds an older index in, retire its write-ahead log,
    /// whose segments may hold what the purge deleted and would write it
    /// back on a replay, and drop what this process decoded from the
    /// released indexes. A query still reading an evicted index keeps it
    /// until it finishes.
    pub async fn clean_after_purge(&self, holders: &[String]) -> Result<()> {
        self.evict_erased(holders).await;
        if let Some(root) = self.config.index_storage.path.clone() {
            tokio::task::spawn_blocking(move || {
                fluree_db_core::FileStorage::checkpoint_wal(root.as_ref())
            })
            .await
            .map_err(|e| ApiError::internal(format!("write-ahead log checkpoint: {e}")))??;
        }
        self.clear_decoded_caches();
        Ok(())
    }

    /// Install the erasures of the activated manifest `manifest` on this
    /// member: they read as their replacements from here on. Then the
    /// cached state of `holders` is evicted and reads of commits begun
    /// before are waited out.
    pub async fn install_erasures(&self, manifest: &str, holders: &[String]) -> Result<()> {
        let runtime = self.erasure_runtime()?;
        let erased = runtime.index.manifest(manifest).await?;
        runtime.index.install_manifest(erased);
        self.evict_erased(holders).await;
        Ok(())
    }
}
