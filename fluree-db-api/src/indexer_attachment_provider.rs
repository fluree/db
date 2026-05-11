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

        // Bulk-import seal path. After `fluree create --import`, the
        // `f:reifies*` flakes live in the **base index**, not in the
        // running `AttachmentNovelty` overlay — so
        // `try_running_attachment_events` reports an empty event set
        // even though the ledger has annotations. Without a fallback
        // here the indexer's arena builder early-returns and the
        // arena never seals.
        //
        // When we see empty running events AND the snapshot's sticky
        // bit is set AND the snapshot has range-providing index
        // backing, walk the base index once for `f:reifies*` flakes,
        // decode each bundle into an `EdgeKey`, and surface the
        // result as `Authoritative` — a complete event set sourced
        // from the indexed flakes themselves.
        //
        // The scan is gated on `snapshot.has_annotations` so non-
        // annotation ledgers pay nothing. It runs at most once per
        // reindex pass (the resulting arena is sealed afterward and
        // the running overlay covers future events).
        if result.events.is_empty() {
            if let Some(events) = scan_base_index_for_attachment_events(manager, ledger_id).await {
                return Some(AttachmentEventCoverage::Authoritative(events));
            }
        }

        Some(match result.coverage {
            RunningCoverage::Authoritative => AttachmentEventCoverage::Authoritative(result.events),
            RunningCoverage::Augment => AttachmentEventCoverage::Augment(result.events),
        })
    }
}

/// Walk the running ledger's base index for `f:reifies*` flakes and
/// reconstruct the complete attachment-event set. Returns `None` when:
///
/// - the ledger isn't loaded into the manager,
/// - the snapshot's sticky bit is clear (non-annotation ledger),
/// - the snapshot has no range provider (no base index to scan), or
/// - any structural inconsistency would corrupt the event set.
///
/// The scan reads `POST f:reifiesSubject ?any` to enumerate candidate
/// annotation subjects across every graph the snapshot exposes, then
/// per-candidate fetches the full `f:reifies*` bundle via `SPOT s=?ann`
/// and decodes it through `EdgeKey::from_reifies_facts`. Malformed
/// bundles are skipped (consistent with `AttachmentNovelty::observe_flakes`).
async fn scan_base_index_for_attachment_events(
    manager: &LedgerManager,
    ledger_id: &str,
) -> Option<Vec<(fluree_db_core::edge::EdgeKey, fluree_db_core::Sid, i64, bool)>> {
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::edge::EdgeKey;
    use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
    use fluree_db_core::{is_reserved_reifies_predicate, FlakeValue, Sid};
    use std::collections::{BTreeMap, HashSet};

    let view = manager.get_loaded_view(ledger_id).await?;

    if !view.snapshot.has_annotations {
        return None;
    }
    if view.snapshot.range_provider.is_none() {
        return None;
    }
    tracing::debug!(
        ledger_id,
        t = view.t,
        "scan_base_index_for_attachment_events"
    );

    let f_reifies_subject = Sid::new(
        fluree_vocab::namespaces::FLUREE_DB,
        fluree_vocab::db::REIFIES_SUBJECT,
    );

    // Annotation flakes may live in the default graph (g_id=0) or
    // any named graph. Always include g_id=0 — `GraphRegistry::iter_entries`
    // explicitly skips the default graph slot — and add every named
    // graph the registry knows about.
    let mut graph_ids: HashSet<fluree_db_core::GraphId> = HashSet::new();
    graph_ids.insert(0);
    for (id, _) in view.snapshot.graph_registry.iter_entries() {
        graph_ids.insert(id);
    }
    let graph_ids: Vec<fluree_db_core::GraphId> = graph_ids.into_iter().collect();

    let overlay: &dyn fluree_db_core::OverlayProvider = view.novelty.as_ref();
    let to_t = view.t.max(view.snapshot.t);

    let mut events: Vec<(EdgeKey, Sid, i64, bool)> = Vec::new();
    let mut seen: HashSet<(fluree_db_core::GraphId, Sid)> = HashSet::new();

    for g_id in graph_ids {
        // Collect every `f:reifies*` flake in this graph by walking
        // each reserved predicate in turn (PSOT). We group in
        // memory by annotation SID, which sidesteps a SPOT-scan
        // quirk where a constant blank-node subject (namespace_code
        // = 0) doesn't return rows reliably across all backends.
        let mut by_ann: BTreeMap<Sid, Vec<fluree_db_core::Flake>> = BTreeMap::new();
        for p_iri in fluree_vocab::reifies_iris::ALL {
            let Some(p_sid) = view.snapshot.encode_iri(p_iri) else {
                // Predicate IRI never observed on this ledger — skip.
                continue;
            };
            let flakes = range_with_overlay(
                &view.snapshot,
                g_id,
                overlay,
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch::new().with_predicate(p_sid),
                RangeOptions::new().with_to_t(to_t),
            )
            .await
            .ok()?;
            for f in flakes {
                if !f.op {
                    // Skip retracted f:reifies* events: the seal pass
                    // only cares about currently-live bundles.
                    continue;
                }
                by_ann.entry(f.s.clone()).or_default().push(f);
            }
        }

        for (ann_sid, bundle) in by_ann {
            if !seen.insert((g_id, ann_sid.clone())) {
                continue;
            }
            // Decode → EdgeKey. Malformed bundles are skipped
            // (consistent with `AttachmentNovelty::observe_flakes`).
            let Ok(edge_key) = EdgeKey::from_reifies_facts(&bundle) else {
                continue;
            };
            // Assertion time from the f:reifiesSubject flake.
            let t = bundle
                .iter()
                .find(|f| f.p == f_reifies_subject)
                .map(|f| f.t)
                .unwrap_or(0);
            events.push((edge_key, ann_sid, t, /* op = */ true));
            // FlakeValue is unused — suppress the warning.
            let _ = std::marker::PhantomData::<FlakeValue>;
        }
    }

    Some(events)
}
