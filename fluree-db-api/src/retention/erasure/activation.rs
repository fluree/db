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

use crate::{Fluree, Result};

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
}

impl Fluree {
    /// Publish this cluster member's erasures through `activator`.
    pub fn set_erasure_activator(&self, activator: Arc<dyn ErasureActivator>) {
        if let Some(runtime) = self.retention.erasure.get() {
            let _ = runtime.activator.set(activator);
        }
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
