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
) -> Option<Arc<StatsView>> {
    let build_view = || {
        let indexed = db.snapshot.stats.clone().unwrap_or_default();
        // Note: downcast_ref::<Novelty>() silently falls through for non-Novelty overlays
        // (e.g. PolicyOverlay). In those cases we skip novelty merging and return only
        // the persisted indexed stats, which is correct since policy overlays don't
        // produce new statistical flakes.
        let stats = if let Some(novelty) = db.overlay.as_any().downcast_ref::<Novelty>() {
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

        Arc::new(StatsView::from_db_stats_with_namespaces(
            &stats,
            db.snapshot.namespaces(),
        ))
    };

    // Cache key: epoch() is a monotonic counter incremented on each overlay mutation
    // (e.g. novelty commit). It is sufficient to discriminate cache entries because
    // the same (ledger_id, snapshot.t, query t) with different overlay contents will
    // always have different epoch values. Limitation: if an overlay is replaced by a
    // wholly new instance (e.g. after ledger reload), epoch resets to 0, but in that
    // case snapshot.t will also differ, so the key remains unique.
    let cache_key = xxh3_128(
        format!(
            "stats-view:{}:{}:{}:{}:{}",
            db.snapshot.ledger_id,
            db.snapshot.t,
            db.t,
            db.overlay.epoch(),
            u8::from(db.runtime_small_dicts.is_some() || binary_store.is_some()),
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
        let first = cached_stats_view_for_db(db, None).expect("first stats view");
        assert_eq!(
            first
                .get_property(&Sid::new(10, "score"))
                .expect("property stat")
                .count,
            2
        );
    }
}
