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
        // The fallback walks the base index for `f:reifies*` flakes
        // and surfaces the result as `Authoritative` — a complete
        // event set sourced from the indexed live state. The scan
        // **must only fire when no arena has *ever* been sealed**
        // for this ledger — i.e. the first reindex after a fresh
        // annotation-bearing import. Two states look identical at
        // the snapshot level (`has_annotations=true,
        // annotation_index=None`):
        //
        //   - fresh bulk import — annotation-bearing flakes
        //     landed via the bulk-import pipeline, no indexer
        //     pass has ever processed them
        //     (`had_annotation_arena=false`). Safe to bootstrap.
        //   - any indexer-owned state
        //     (`had_annotation_arena=true`). Must NOT bootstrap.
        //     Covers both:
        //       * defensive-drop after a prior arena seal — the
        //         dropped arena carried historical retract/reassert
        //         rows that aren't in the currently-live base.
        //       * indexer pass on an annotation-bearing ledger
        //         that didn't seal (e.g. no provider attached) —
        //         the pass observed events that the live base
        //         doesn't fully reflect.
        //     In either case a live-only `Authoritative` reseal
        //     would silently lose history. Stay in scan-fallback;
        //     reseal will happen on a future pass that supplies
        //     *explicit* `Authoritative` coverage. Plain `Augment`
        //     with no base arena still gets refused by the
        //     indexer (see
        //     `fluree-db-indexer/src/build/incremental.rs` phase
        //     3d's `Augment` branch) — Augment alone cannot
        //     recover missing history.
        //
        // The sticky `had_annotation_arena` bit — set whenever
        // the indexer produces a root with `has_annotations=true`
        // (regardless of whether an arena was sealed), never
        // cleared, plumbed through `IndexRoot.had_annotation_arena`
        // and surfaced on `LedgerSnapshot.had_annotation_arena` —
        // is the only signal that distinguishes "indexer-owned"
        // from "fresh bulk-import" states. Despite the name, the
        // load-bearing meaning is closer to "base-index bootstrap
        // is not allowed"; the bit is true for indexer-touched
        // annotation-bearing roots even when no arena was ever
        // sealed.
        //
        // Full gate (in order of cheapness):
        //   (i)   running overlay is empty,
        //   (ii)  loaded view exists,
        //   (iii) snapshot.has_annotations,
        //   (iv)  snapshot.annotation_index.is_none(),
        //   (v)   !snapshot.had_annotation_arena.
        // The scan itself further gates on `snapshot.has_annotations`
        // inside `scan_base_index_for_attachment_events`, so
        // non-annotation ledgers pay nothing even if the cheap
        // checks pass.
        if result.events.is_empty() {
            let load_view = manager.get_loaded_view(ledger_id).await;
            let bootstrap_eligible = load_view
                .as_ref()
                .map(|v| {
                    v.snapshot.has_annotations
                        && v.snapshot.annotation_index.is_none()
                        && !v.snapshot.had_annotation_arena
                })
                .unwrap_or(false);
            if bootstrap_eligible {
                if let Some(events) =
                    scan_base_index_for_attachment_events(manager, ledger_id).await
                {
                    return Some(AttachmentEventCoverage::Authoritative(events));
                }
            }
        }

        Some(match result.coverage {
            RunningCoverage::Authoritative => AttachmentEventCoverage::Authoritative(result.events),
            RunningCoverage::Augment => AttachmentEventCoverage::Augment(result.events),
        })
    }
}

/// Walk the running ledger's base index for `f:reifies*` flakes and
/// reconstruct the currently-live attachment-event set. Returns `None`
/// when:
///
/// - the ledger isn't loaded into the manager,
/// - the snapshot's sticky bit is clear (non-annotation ledger),
/// - the snapshot has no range provider (no base index to scan), or
/// - any structural inconsistency would corrupt the event set.
///
/// **Caller contract:** must only invoke when no arena exists yet
/// (`snapshot.annotation_index.is_none()`). The scan reconstructs the
/// currently-live event set only — it does not see retract/reassert
/// history — so using it to "refresh" an existing arena would drop
/// historical rows.
///
/// Strategy: for each reserved `f:reifiesSubject` / `f:reifiesPredicate`
/// / `f:reifiesObject` / `f:reifiesLang` predicate, walk PSOT
/// (`predicate = pX`) across every graph the snapshot exposes, group
/// flakes in memory by annotation SID, and decode each bundle through
/// `EdgeKey::from_reifies_facts`. We use PSOT-and-group rather than
/// per-SID SPOT scans because a SPOT scan with a constant blank-node
/// subject (namespace_code = 0) does not return rows reliably across
/// all backends. Malformed bundles are skipped (consistent with
/// `AttachmentNovelty::observe_flakes`).
async fn scan_base_index_for_attachment_events(
    manager: &LedgerManager,
    ledger_id: &str,
) -> Option<
    Vec<(
        fluree_db_core::edge::EdgeKey,
        fluree_db_core::Sid,
        i64,
        bool,
    )>,
> {
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::edge::EdgeKey;
    use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
    use fluree_db_core::Sid;
    use std::collections::{BTreeMap, HashSet};

    let view = manager.get_loaded_view(ledger_id).await?;

    if !view.snapshot.has_annotations {
        return None;
    }
    view.snapshot.range_provider.as_ref()?;
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
            // Assertion time from the f:reifiesSubject flake. The
            // decode above only succeeds when the bundle carries a
            // valid `f:reifiesSubject` row (the decoder returns
            // `Missing` otherwise), so the find below should always
            // hit. We treat a missing row as malformed — skip
            // rather than fall back to `t=0`, which would seal a
            // misdated row in the arena. If `EdgeKey::from_reifies_facts`
            // ever loosens its `f:reifiesSubject` requirement, this
            // gate keeps the arena's `t` axis trustworthy.
            let Some(t) = bundle
                .iter()
                .find(|f| f.p == f_reifies_subject)
                .map(|f| f.t)
            else {
                tracing::warn!(
                    ?ann_sid,
                    ?g_id,
                    "f:reifiesSubject flake absent from decoded bundle; \
                     skipping (would have produced a t=0 arena row)"
                );
                continue;
            };
            events.push((edge_key, ann_sid, t, /* op = */ true));
        }
    }

    Some(events)
}
