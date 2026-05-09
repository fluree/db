//! API-side `AttachmentEventsProvider` implementation.
//!
//! Resolves a per-ledger attachment-event delta from the running
//! `LedgerManager` so the background indexer can seal authoritative
//! arenas with the live overlay state.
//!
//! ## Late-bound `LedgerManager`
//!
//! `BackgroundIndexerWorker` is constructed before `LedgerManager`
//! in [`FlureeBuilder::finalize_with_backend`], so the provider
//! can't capture a strong `Arc<LedgerManager>` at worker
//! construction time. Instead, we share a `OnceLock<Arc<LedgerManager>>`
//! between the provider (in the worker) and the builder
//! (post-LedgerManager construction). The builder fills the cell
//! once `LedgerManager` is built; the provider reads through it
//! lazily on each call.
//!
//! Until the cell is filled, the provider returns `None` —
//! "delta unknown" in the indexer's contract — which causes the
//! defensive arena-drop on the new root. Practically the cell is
//! filled before any background indexing job runs since
//! `LedgerManager` finishes construction synchronously after the
//! worker spawns.
//!
//! ## When the ledger isn't loaded
//!
//! `LedgerManager.try_running_attachment_events` returns `None`
//! when the ledger isn't currently loaded into the running
//! registry. That also routes through the indexer's "delta
//! unknown" path. The defensive drop is correct because we cannot
//! produce an authoritative event set without observing the
//! ledger's running novelty.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use fluree_db_indexer::{AttachmentEventCoverage, AttachmentEventsProvider};

use crate::ledger_manager::{LedgerManager, RunningCoverage};

/// Shared late-binding cell for the api's running `LedgerManager`.
pub(crate) type LedgerManagerCell = Arc<OnceLock<Arc<LedgerManager>>>;

/// Provider backed by the running `LedgerManager`. Reads the
/// snapshotted attachment overlay for the requested ledger and
/// returns its event-pair view, suitable for direct use as
/// `IndexerConfig.attachment_events`.
pub(crate) struct ApiAttachmentEventsProvider {
    pub(crate) manager: LedgerManagerCell,
}

impl std::fmt::Debug for ApiAttachmentEventsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiAttachmentEventsProvider").finish()
    }
}

#[async_trait]
impl AttachmentEventsProvider for ApiAttachmentEventsProvider {
    async fn attachment_events(&self, ledger_id: &str) -> Option<AttachmentEventCoverage> {
        let manager = self.manager.get()?;
        // Coverage from LedgerManager: when snapshot.t==0 (no index
        // has ever run on this ledger), the AttachmentNovelty was
        // built by walking every commit since genesis — provably
        // complete. Once snapshot.t > 0, we can't distinguish a
        // continuously-running ledger (full history preserved) from
        // a reloaded one (post-index tail only), so we fall back to
        // Augment so the indexer merges with the base arena's
        // events.
        let result = manager.try_running_attachment_events(ledger_id).await?;
        Some(match result.coverage {
            RunningCoverage::Authoritative => AttachmentEventCoverage::Authoritative(result.events),
            RunningCoverage::Augment => AttachmentEventCoverage::Augment(result.events),
        })
    }
}
