use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{
    GraphDbRef, GraphId, OverlayProvider, RuntimePredicateId, RuntimeSmallDicts, Sid, StatsView,
};
use fluree_db_novelty::{assemble_fast_stats, Novelty, StatsAssemblyError, StatsLookup};
use std::collections::HashMap;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_128;

struct BinaryStoreStatsLookup<'a> {
    store: Option<&'a BinaryIndexStore>,
    runtime_small_dicts: Option<&'a RuntimeSmallDicts>,
}

#[async_trait]
impl StatsLookup for BinaryStoreStatsLookup<'_> {
    fn runtime_small_dicts(&self) -> Option<&RuntimeSmallDicts> {
        self.runtime_small_dicts
    }

    fn persisted_predicate_id_for_sid(&self, sid: &Sid) -> Option<RuntimePredicateId> {
        self.store
            .and_then(|store| store.sid_to_p_id(sid).map(RuntimePredicateId::from_u32))
    }

    async fn lookup_subject_classes(
        &self,
        _snapshot: &fluree_db_core::LedgerSnapshot,
        _overlay: &dyn OverlayProvider,
        _to_t: i64,
        _g_id: GraphId,
        _subjects: &[Sid],
    ) -> std::result::Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError> {
        Err(StatsAssemblyError::Message(
            "full runtime class lookups are not available in query stats cache".to_string(),
        ))
    }
}

pub(crate) fn cached_stats_view_for_db(
    db: GraphDbRef<'_>,
    binary_store: Option<&Arc<BinaryIndexStore>>,
    allow_semantic_elision: bool,
) -> Option<Arc<StatsView>> {
    let build_view = || {
        let indexed = db.snapshot.stats.clone().unwrap_or_default();
        // Note: downcast_ref::<Novelty>() silently falls through for non-Novelty overlays
        // (e.g. PolicyOverlay). In those cases we skip novelty merging and return only
        // the persisted indexed stats, which is correct since policy overlays don't
        // produce new statistical flakes.
        let novelty = db.overlay.as_any().downcast_ref::<Novelty>();
        let stats = if let Some(novelty) = novelty {
            let lookup = BinaryStoreStatsLookup {
                store: binary_store.map(std::convert::AsRef::as_ref),
                runtime_small_dicts: db.runtime_small_dicts,
            };
            assemble_fast_stats(
                &indexed,
                db.snapshot,
                novelty,
                db.t,
                Some(&lookup as &dyn StatsLookup),
            )
        } else {
            indexed
        };

        let mut view = StatsView::from_db_stats_with_namespaces(&stats, db.snapshot.namespaces());
        // Per-(class, predicate) coverage counts may be consulted for semantic
        // elision of redundant `rdf:type` filters — but only when they are
        // exact for the current state. The query stats cache cannot resolve the
        // classes of novel subjects (`BinaryStoreStatsLookup::lookup_subject_classes`
        // errors), so with non-empty novelty the per-class counts can lag the
        // novelty-merged property totals (a retracted `rdf:type` would not be
        // reflected), which could make a stale equality falsely hold. A non-Novelty
        // overlay means a policy/visibility layer that can hide `rdf:type` and the
        // predicate differently. Trust the counts only when novelty is empty and
        // there is no such overlay.
        //
        // `allow_semantic_elision` is the prepare-time vouch that this execution
        // is current-state, single-stats-domain (one ledger, not a dataset) and
        // root-policy — facts the stats builder cannot see here. It is folded
        // into the cache key below, so a trusted view is never reused for a
        // non-vouched (policy/dataset) execution at the same overlay epoch.
        view.class_coverage_trustworthy =
            allow_semantic_elision && novelty.is_some_and(Novelty::is_empty);
        // Overlay arena-derived stats for `f:reifies*` predicates so the
        // join planner gets tight selectivity estimates on snapshots
        // with a built annotation index. See
        // `StatsView::merge_annotation_stats` for the synthesis rules.
        if let Some(ann) = db.snapshot.annotation_index.as_ref() {
            view.merge_annotation_stats(&ann.stats, db.snapshot.namespaces());
        }
        Arc::new(view)
    };

    // Cache key: epoch() is a monotonic counter incremented on each overlay mutation
    // (e.g. novelty commit). It is sufficient to discriminate cache entries because
    // the same (ledger_id, snapshot.t, query t) with different overlay contents will
    // always have different epoch values. Limitation: if an overlay is replaced by a
    // wholly new instance (e.g. after ledger reload), epoch resets to 0, but in that
    // case snapshot.t will also differ, so the key remains unique.
    //
    // We also fold in the annotation arena's identity (`forward_branch_cid` +
    // `reverse_branch_cid`) so a reindex/rebuild that swaps the arena at the
    // same `snapshot.t` produces a fresh cache slot — `merge_annotation_stats`
    // depends on these contents, and CIDs are content-addressed so they
    // rotate on any rebuild.
    let arena_key = db
        .snapshot
        .annotation_index
        .as_ref()
        .map(|a| format!("{}:{}", a.forward_branch_cid, a.reverse_branch_cid))
        .unwrap_or_else(|| "none".to_string());
    let cache_key = xxh3_128(
        format!(
            "stats-view:{}:{}:{}:{}:{}:{}:{}",
            db.snapshot.ledger_id,
            db.snapshot.t,
            db.t,
            db.overlay.epoch(),
            u8::from(db.runtime_small_dicts.is_some() || binary_store.is_some()),
            u8::from(allow_semantic_elision),
            arena_key,
        )
        .as_bytes(),
    );

    if let Some(cache) = binary_store.and_then(|store| store.leaflet_cache()) {
        return Some(cache.get_or_build_stats_view(cache_key, build_view));
    }

    Some(build_view())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{Flake, IndexStats, PropertyStatEntry, Sid};

    fn prop_flake(subject: Sid, property: Sid, value: i64, t: i64) -> Flake {
        Flake::new(
            subject,
            property,
            fluree_db_core::FlakeValue::Long(value),
            Sid::new(
                fluree_vocab::namespaces::XSD,
                fluree_vocab::xsd_names::INTEGER,
            ),
            t,
            true,
            None,
        )
    }

    #[test]
    fn uncached_builder_still_merges_novelty_without_store() {
        let mut snapshot = fluree_db_core::LedgerSnapshot::genesis("test:main");
        snapshot.stats = Some(IndexStats {
            flakes: 1,
            size: 10,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "score".to_string()),
                count: 1,
                ndv_values: 0,
                ndv_subjects: 0,
                last_modified_t: 1,
                datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
        });

        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![prop_flake(
                    Sid::new(10, "alice"),
                    Sid::new(10, "score"),
                    42,
                    2,
                )],
                2,
                &HashMap::new(),
            )
            .unwrap();

        let db = GraphDbRef::new(&snapshot, 0, &novelty, 2);
        let first = cached_stats_view_for_db(db, None, false).expect("first stats view");
        assert_eq!(
            first
                .get_property(&Sid::new(10, "score"))
                .expect("property stat")
                .count,
            2
        );
    }

    #[test]
    fn semantic_elision_vouch_gates_class_coverage_trust() {
        // Same empty-novelty db: coverage is trusted only when the caller vouches
        // (single-ledger, root policy, current-state). `false` must never trust,
        // even though novelty is empty — that is what keeps policy/dataset
        // executions, which pass `false`, from eliding type filters.
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(1); // empty

        let untrusted =
            cached_stats_view_for_db(GraphDbRef::new(&snapshot, 0, &novelty, 1), None, false)
                .expect("view");
        assert!(
            !untrusted.class_coverage_trustworthy,
            "vouch=false must not trust coverage even with empty novelty"
        );

        let trusted =
            cached_stats_view_for_db(GraphDbRef::new(&snapshot, 0, &novelty, 1), None, true)
                .expect("view");
        assert!(
            trusted.class_coverage_trustworthy,
            "vouch=true + empty novelty must trust coverage"
        );
    }
}
