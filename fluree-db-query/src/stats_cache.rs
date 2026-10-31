use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::clock::Instant;
use fluree_db_core::{
    GraphDbRef, GraphId, IndexStats, OverlayProvider, RuntimePredicateId, RuntimeSmallDicts, Sid,
    StatsView,
};
use fluree_db_novelty::{assemble_planner_stats, Novelty, StatsAssemblyError, StatsLookup};
use std::collections::HashMap;
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3;

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
    // The one store-less cache every query in the process shares.
    static STORELESS: StorelessStatsCache = StorelessStatsCache::new();
    stats_view_for_db_in(&STORELESS, db, binary_store, allow_semantic_elision)
}

/// [`cached_stats_view_for_db`] with the store-less cache passed in. Tests
/// that assert what the cache returns pass one of their own: lib tests run as
/// threads of one process, so any eight insertions from other tests between
/// two of a test's calls would evict its slot from the shared one.
fn stats_view_for_db_in(
    storeless: &StorelessStatsCache,
    db: GraphDbRef<'_>,
    binary_store: Option<&Arc<BinaryIndexStore>>,
    allow_semantic_elision: bool,
) -> Option<Arc<StatsView>> {
    let build_view = || {
        let _span = tracing::debug_span!("stats_view_build").entered();
        let started = Instant::now();
        let indexed = db
            .snapshot
            .stats
            .clone()
            .unwrap_or_else(|| Arc::new(IndexStats::default()));
        // Note: downcast_ref::<Novelty>() silently falls through for non-Novelty overlays
        // (e.g. PolicyOverlay). In those cases we skip novelty merging and return only
        // the persisted indexed stats, which is correct since policy overlays don't
        // produce new statistical flakes.
        let novelty = db.overlay.as_any().downcast_ref::<Novelty>();
        let mut stats = if let Some(novelty) = novelty {
            let lookup = BinaryStoreStatsLookup {
                store: binary_store.map(std::convert::AsRef::as_ref),
                runtime_small_dicts: db.runtime_small_dicts,
            };
            // THE PLANNER LANE IS NOT RECONCILED, ON PURPOSE. This is the
            // ESTIMATE merge (`NoveltyMerge::Estimate`), not the base-reconciled
            // one the user-facing count surfaces use (#1391), because
            // reconciliation costs one base-index probe per
            // `(graph, subject, predicate)` in the window while this view is
            // rebuilt on every overlay epoch bump — i.e. every commit — so
            // accumulating a novelty window would be quadratic in its own size.
            //
            // NO COUNT ANSWER RIDES ON THIS VIEW. Several COUNT lanes do run
            // with novelty present — `count_plan_exec.rs` and `count_rows.rs`
            // both gate on `allow_cursor_fast_path` rather than
            // `fast_path_store`, the latter noting that the stricter gate
            // "forced the whole encoded-filters COUNT family onto the generic
            // fallback whenever any novelty was present (~50% of real
            // queries)". What makes them safe is not that they decline: they
            // read through a `BinaryCursor` that folds the overlay in and
            // applies set semantics. None of them reads this merged
            // `StatsView`.
            //
            // So a duplicate re-assert inflates planner cardinality estimates
            // by one until the next reindex — the same class of imprecision
            // `ndv_*` and `last_modified_t` already carry here.
            //
            // One consumer is NOT purely an estimate, and is tracked as #1721:
            // `StatsView::property_ref_only` is derived from the merged
            // per-datatype breakdown and feeds `filter_fold`'s node-only
            // soundness guard, which decides whether `FILTER(?x = ?y)` may be
            // folded into a term-equality join. `merge_property_datatypes`
            // drops any datatype whose merged count reaches zero, so a spurious
            // `-1` — a novelty retraction of a literal that was never there —
            // could in principle drop a predicate's last literal tag and
            // license that fold where SPARQL *value* equality was required.
            // Latent and pre-existing (the blind delta log long predates
            // #1391), not demonstrated end to end, and deliberately not
            // addressed in #1699: the repair belongs in the estimate lane
            // itself, and changing what `PropertyStatEntry.datatypes` emits
            // reaches every consumer that sums it. See #1721 for both candidate
            // fix directions — and note that reconciling THIS lane is not one
            // of them, for the quadratic reason above.
            assemble_planner_stats(
                &indexed,
                db.snapshot,
                novelty,
                db.t,
                Some(&lookup as &dyn StatsLookup),
            )
        } else {
            indexed
        };

        // Time travel below the published index `t`: the base index is
        // current state as of the publish, and novelty only ever carries
        // flakes *after* it, so nothing in `stats` describes the graph at
        // `db.t`. For counts that is the usual estimate-drift the planner
        // tolerates, but the observed-tag sets are read as soundness licences
        // (`StatsView::property_ref_only` for the equijoin-filter fold,
        // `GraphPropertyStatData::observed_datatypes` for exact-datatype scan
        // narrowing), and there they are wrong in the unsafe direction: a
        // predicate whose literals were legitimately deleted before the
        // publish has no literal tag left in the current-state set, so it
        // reads as all-ref and licenses a rewrite for a `t` at which it
        // demonstrably carried literals.
        //
        // The index persists a second set for exactly this read: the
        // historical tags, accumulated monotonically across publishes since
        // `historical_since_t`. For any `db.t` at or above that boundary the
        // historical set contains every tag visible at `db.t` (see the
        // invariant on `IndexStats::historical_since_t`), and since extra
        // tags only ever *decline* a rewrite, substituting it for the
        // current-state set keeps every licence sound — a never-literal
        // predicate keeps the fold at historical `t`s, instead of losing it
        // wholesale. Below the boundary (or on an index that predates the
        // historical wire tail) there is no sound set, so the observed sets
        // are cleared: empty means "unknown" and every consumer fails closed.
        // The counts are left alone in all cases.
        //
        // `make_mut` copies here, since the stats are shared with the
        // snapshot; current-state reads never take this branch.
        if db.t < db.snapshot.t {
            let stats = Arc::make_mut(&mut stats);
            let licensed = stats.historical_since_t.is_some_and(|since| db.t >= since);
            for property in stats.properties.iter_mut().flatten() {
                if licensed {
                    property.observed_datatypes =
                        std::mem::take(&mut property.historical_datatypes);
                } else {
                    property.observed_datatypes.clear();
                }
            }
            for graph in stats.graphs.iter_mut().flatten() {
                for property in &mut graph.properties {
                    if licensed {
                        property.observed_datatypes =
                            std::mem::take(&mut property.historical_datatypes);
                    } else {
                        property.observed_datatypes.clear();
                    }
                }
            }
        }

        let mut view = StatsView::from_db_stats_with_namespaces(&stats, db.snapshot);
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
        // The vouch admits `as_of` reads, whose class counts are still the
        // published index's, so a read below the index `t` gets no proof.
        view.class_coverage_trustworthy = allow_semantic_elision
            && novelty.is_some_and(Novelty::is_empty)
            && db.t >= db.snapshot.t;
        // `source` only serves the coverage proof. When that is off, `stats` may
        // be a merged or time-travel copy the cache weight does not count, so
        // do not keep it alive.
        if !view.class_coverage_trustworthy {
            view.source = None;
        }
        // Overlay arena-derived stats for `f:reifies*` predicates so the
        // join planner gets tight selectivity estimates on snapshots
        // with a built annotation index. See
        // `StatsView::merge_annotation_stats` for the synthesis rules.
        if let Some(ann) = db.snapshot.annotation_index.as_ref() {
            view.merge_annotation_stats(&ann.stats, db.snapshot.namespaces());
        }
        tracing::debug!(
            stats_view_build_ms = started.elapsed().as_secs_f64() * 1000.0,
            classes = view.classes.len(),
            properties = view.properties.len(),
            novelty_merged = novelty.is_some_and(|n| !n.is_empty()) && db.t > db.snapshot.t,
            "built planner stats view"
        );
        Arc::new(view)
    };

    // The view is a function of the index root the snapshot's stats, schema
    // and annotation arena came from, and `(ledger_id, snapshot.t)` doesn't
    // name that root: a ledger dropped and created again under its name can
    // reach the same `t` with different data, and the leaflet cache outlives
    // the drop. The root's content id does name it, and stays the same across
    // reloads of one root (a historical read loads its store again on every
    // call). Root-derived content with no root id to name it can't be told
    // apart from another snapshot's at the same `t`, so it's built rather
    // than shared.
    let index_root = db.snapshot.index_root_id();
    if index_root.is_none()
        && (db.snapshot.stats.is_some() || db.snapshot.annotation_index.is_some())
    {
        return Some(build_view());
    }

    // epoch() is a monotonic counter incremented on each overlay mutation
    // (e.g. novelty commit), so the same ledger, root and query `t` with
    // different overlay contents have different epochs. A replay of the same
    // commits reaches the same epoch, which keeps historical reads that
    // replay novelty on every call on one view.
    //
    // The fields are hashed as they are rather than formatted into a string:
    // this runs on every query, hit or miss.
    let mut key = Xxh3::new();
    key.update(b"stats-view\0");
    key.update(db.snapshot.ledger_id.as_str().as_bytes());
    key.update(&[0]);
    key.update(&db.snapshot.t.to_le_bytes());
    key.update(&db.t.to_le_bytes());
    match index_root {
        Some(root) => {
            let digest = root.as_cid().hash().digest();
            key.update(&[1, digest.len() as u8]);
            key.update(digest);
        }
        None => key.update(&[0]),
    }
    key.update(&db.overlay.epoch().to_le_bytes());
    key.update(&[
        u8::from(db.runtime_small_dicts.is_some() || binary_store.is_some()),
        u8::from(allow_semantic_elision),
    ]);

    if let Some(cache) = binary_store.and_then(|store| store.leaflet_cache()) {
        return Some(cache.get_or_build_stats_view(key.digest128(), build_view));
    }

    // No binary store (memory-mode / unindexed ledger): the leaflet-cache
    // home for stats views doesn't exist, and rebuilding per call walks the
    // whole novelty in `assemble_fast_stats` — per-query loops (SHACL
    // sh:sparql validation, transaction WHEREs) paid O(novelty) planning per
    // execution, which made file-mode sh:sparql validation quadratic in
    // focus nodes. Fall back to a small process-global cache, keyed by the
    // same key PLUS the overlay's `content_version()` — the documented
    // globally-unique content stamp — since `epoch()` alone is only unique
    // within one overlay instance's lineage. No version stamp → no caching
    // (identical to before).
    if let Some(version) = db.overlay.content_version() {
        key.update(&version.to_le_bytes());
        return Some(storeless.get_or_build(key.digest128(), build_view));
    }

    Some(build_view())
}

/// Tiny LRU for stats views of store-less (memory-mode) ledgers. A handful
/// of slots suffices: one validation or transaction loop reuses a single
/// entry thousands of times, and distinct concurrently-active memory ledgers
/// are rare. Capacity-bounded so long-lived processes can't accumulate views.
struct StorelessStatsCache {
    slots: parking_lot::Mutex<Vec<(u128, Arc<StatsView>)>>,
}

impl StorelessStatsCache {
    const CAPACITY: usize = 8;

    const fn new() -> Self {
        Self {
            slots: parking_lot::Mutex::new(Vec::new()),
        }
    }

    fn get_or_build(
        &self,
        cache_key: u128,
        build_view: impl FnOnce() -> Arc<StatsView>,
    ) -> Arc<StatsView> {
        {
            let mut cache = self.slots.lock();
            if let Some(pos) = cache.iter().position(|(k, _)| *k == cache_key) {
                let hit = cache.remove(pos);
                let view = Arc::clone(&hit.1);
                cache.push(hit); // most-recently-used at the back
                return view;
            }
        }

        // Build outside the lock — assembly walks the overlay and can be slow.
        let view = build_view();
        let mut cache = self.slots.lock();
        if cache.len() >= Self::CAPACITY {
            cache.remove(0);
        }
        cache.push((cache_key, Arc::clone(&view)));
        view
    }
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

    /// Builds through the test's own cache (see [`stats_view_for_db_in`]).
    fn view(
        cache: &StorelessStatsCache,
        db: GraphDbRef<'_>,
        allow_semantic_elision: bool,
    ) -> Arc<StatsView> {
        stats_view_for_db_in(cache, db, None, allow_semantic_elision).expect("stats view")
    }

    /// The redundant-`rdf:type` coverage proof reads class usage through the
    /// view's retained `source` and IRI encoder. The builder must keep both
    /// when the proof is licensed, or the elision silently stops firing; and
    /// drop `source` otherwise, since its weight is not counted.
    #[test]
    fn builder_keeps_class_usage_only_for_a_licensed_coverage_proof() {
        use fluree_db_core::{ClassPropertyUsage, ClassStatEntry, ValueTypeTag};
        let cache = StorelessStatsCache::new();
        let ref_tag = ValueTypeTag::JSON_LD_ID.as_u8();
        let mut snapshot = fluree_db_core::LedgerSnapshot::genesis("coverage-builder:main");
        snapshot
            .insert_namespace_code(100, "http://example.org/".to_string())
            .expect("register ex namespace");
        snapshot.stats = Some(Arc::new(IndexStats {
            properties: Some(vec![PropertyStatEntry {
                sid: (100, "p".to_string()),
                count: 4,
                ndv_values: 4,
                ndv_subjects: 4,
                last_modified_t: 0,
                datatypes: vec![(ref_tag, 4)],
                observed_datatypes: vec![ref_tag],
                historical_datatypes: vec![],
            }]),
            classes: Some(vec![ClassStatEntry {
                class_sid: Sid::new(100, "C"),
                count: 4,
                properties: vec![ClassPropertyUsage {
                    property_sid: Sid::new(100, "p"),
                    datatypes: vec![(ref_tag, 4)],
                    langs: vec![],
                    ref_classes: vec![],
                }],
            }]),
            ..Default::default()
        }));
        let (p, c) = ("http://example.org/p", "http://example.org/C");
        let empty = Novelty::new(1);

        let licensed = view(&cache, GraphDbRef::new(&snapshot, 0, &empty, 0), true);
        assert!(licensed.predicate_subjects_all_in_class_by_iri(p, c));
        assert!(
            Arc::ptr_eq(
                licensed.source.as_ref().expect("source kept"),
                snapshot.stats.as_ref().expect("stats")
            ),
            "the licensed view shares the snapshot's stats rather than a copy"
        );

        let unvouched = view(&cache, GraphDbRef::new(&snapshot, 0, &empty, 0), false);
        assert!(!unvouched.predicate_subjects_all_in_class_by_iri(p, c));
        assert!(unvouched.source.is_none());

        let mut window = Novelty::new(1);
        window
            .apply_commit(
                vec![prop_flake(Sid::new(100, "s"), Sid::new(100, "q"), 1, 2)],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let merged = view(&cache, GraphDbRef::new(&snapshot, 0, &window, 2), true);
        assert!(!merged.predicate_subjects_all_in_class_by_iri(p, c));
        assert!(merged.source.is_none(), "a merged copy is not retained");
        assert_eq!(merged.get_class_count_by_iri(c), Some(4));

        snapshot.t = 2;
        let time_travel = view(&cache, GraphDbRef::new(&snapshot, 0, &empty, 1), true);
        assert!(
            !time_travel.predicate_subjects_all_in_class_by_iri(p, c),
            "class coverage at the index t does not hold below it"
        );
        assert!(time_travel.source.is_none());
    }

    #[test]
    fn uncached_builder_still_merges_novelty_without_store() {
        let cache = StorelessStatsCache::new();
        let mut snapshot = fluree_db_core::LedgerSnapshot::genesis("test:main");
        snapshot.stats = Some(Arc::new(IndexStats {
            flakes: 1,
            size: 10,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "score".to_string()),
                count: 1,
                ndv_values: 0,
                ndv_subjects: 0,
                last_modified_t: 1,
                datatypes: vec![],
                observed_datatypes: vec![],
                historical_datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
            historical_since_t: None,
        }));

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
        let first = view(&cache, db, false);
        assert_eq!(
            first
                .get_property(&Sid::new(10, "score"))
                .expect("property stat")
                .count,
            2
        );
    }

    /// A second call at the same `content_version` returns the SAME view
    /// object rather than rebuilding. This is the property the store-less
    /// cache exists for: without it, per-query loops (sh:sparql validation,
    /// transaction WHEREs) pay a full `assemble_fast_stats` novelty walk on
    /// every execution.
    #[test]
    fn storeless_stats_view_is_reused_at_the_same_content_version() {
        let cache = StorelessStatsCache::new();
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("cache-hit:main");
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

        let first = view(&cache, GraphDbRef::new(&snapshot, 0, &novelty, 2), false);
        let second = view(&cache, GraphDbRef::new(&snapshot, 0, &novelty, 2), false);
        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged overlay must hit the cache, not rebuild"
        );
    }

    /// Two clones that diverge from the same base carry the SAME `epoch` —
    /// `epoch` is only unique within one instance's lifetime — so keying on
    /// it alone would serve one clone's stats for the other. The
    /// `content_version` stamp is what separates them.
    #[test]
    fn storeless_stats_view_misses_when_content_version_diverges() {
        let cache = StorelessStatsCache::new();
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("cache-miss:main");
        let base = Novelty::new(1);

        let mut one_flake = base.clone();
        one_flake
            .apply_commit(
                vec![prop_flake(Sid::new(10, "a"), Sid::new(10, "score"), 1, 2)],
                2,
                &HashMap::new(),
            )
            .unwrap();

        let mut two_flakes = base.clone();
        two_flakes
            .apply_commit(
                vec![
                    prop_flake(Sid::new(10, "b"), Sid::new(10, "score"), 2, 2),
                    prop_flake(Sid::new(10, "c"), Sid::new(10, "score"), 3, 2),
                ],
                2,
                &HashMap::new(),
            )
            .unwrap();

        // The precondition that makes this test meaningful.
        assert_eq!(
            one_flake.epoch, two_flakes.epoch,
            "divergent clones must collide on epoch, or this proves nothing"
        );
        assert_ne!(
            OverlayProvider::content_version(&one_flake),
            OverlayProvider::content_version(&two_flakes),
            "content versions must diverge"
        );

        let view_one = view(&cache, GraphDbRef::new(&snapshot, 0, &one_flake, 2), false);
        let view_two = view(&cache, GraphDbRef::new(&snapshot, 0, &two_flakes, 2), false);

        let count = |v: &Arc<StatsView>| {
            v.get_property(&Sid::new(10, "score"))
                .expect("score stat")
                .count
        };
        assert_eq!(count(&view_one), 1, "one-flake clone sees its own novelty");
        assert_eq!(count(&view_two), 2, "two-flake clone sees its own novelty");
    }

    /// Two snapshots of one ledger at one `t`, with different stats and empty
    /// novelty, agree on every other part of the key (a never-mutated
    /// novelty's version is always 0). With no index root to name their
    /// stats, neither view may be served for the other.
    #[test]
    fn stats_with_no_index_root_to_name_them_are_not_shared() {
        let cache = StorelessStatsCache::new();
        let at = |count| {
            let mut snapshot = fluree_db_core::LedgerSnapshot::genesis("unnamed-stats:main");
            snapshot.t = 3;
            snapshot.stats = Some(Arc::new(IndexStats {
                properties: Some(vec![PropertyStatEntry {
                    sid: (10, "score".to_string()),
                    count,
                    ndv_values: count,
                    ndv_subjects: count,
                    last_modified_t: 3,
                    datatypes: vec![],
                    observed_datatypes: vec![],
                    historical_datatypes: vec![],
                }]),
                ..Default::default()
            }));
            snapshot
        };
        let empty = Novelty::new(1);
        let count = |snapshot: &fluree_db_core::LedgerSnapshot| {
            view(&cache, GraphDbRef::new(snapshot, 0, &empty, 3), false)
                .get_property(&Sid::new(10, "score"))
                .expect("score stat")
                .count
        };
        let (one, two) = (at(1), at(2));
        assert_eq!(count(&one), 1);
        assert_eq!(
            count(&two),
            2,
            "the second snapshot was served the first one's view"
        );
    }

    /// The store-less cache keeps the eight most recently used views: a hit
    /// moves a view to the back, and a ninth distinct view displaces the
    /// least recently used one. Eight insertions are all it takes, which is
    /// why each test here builds through a cache of its own.
    #[test]
    fn storeless_cache_keeps_the_eight_most_recently_used_views() {
        let cache = StorelessStatsCache::new();
        let build = || Arc::new(StatsView::default());
        let views: Vec<_> = (0..8u64)
            .map(|i| cache.get_or_build(u128::from(i), build))
            .collect();

        assert!(
            Arc::ptr_eq(&cache.get_or_build(0, build), &views[0]),
            "eight views fit"
        );
        cache.get_or_build(8, build);
        assert!(
            Arc::ptr_eq(&cache.get_or_build(0, build), &views[0]),
            "a view that was just hit outlives the ninth insertion"
        );
        assert!(
            !Arc::ptr_eq(&cache.get_or_build(1, build), &views[1]),
            "the ninth insertion displaces the least recently used view"
        );
    }

    #[test]
    fn semantic_elision_vouch_gates_class_coverage_trust() {
        // Same empty-novelty db: coverage is trusted only when the caller vouches
        // (single-ledger, root policy, current-state). `false` must never trust,
        // even though novelty is empty — that is what keeps policy/dataset
        // executions, which pass `false`, from eliding type filters.
        let cache = StorelessStatsCache::new();
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(1); // empty

        let untrusted = view(&cache, GraphDbRef::new(&snapshot, 0, &novelty, 1), false);
        assert!(
            !untrusted.class_coverage_trustworthy,
            "vouch=false must not trust coverage even with empty novelty"
        );

        let trusted = view(&cache, GraphDbRef::new(&snapshot, 0, &novelty, 1), true);
        assert!(
            trusted.class_coverage_trustworthy,
            "vouch=true + empty novelty must trust coverage"
        );
    }

    /// A published index describes current state as of the publish. Read at the
    /// index's own `t` its tag set is a fact about the graph; read below it, it
    /// is a fact about a *later* graph, and the direction it is wrong in is the
    /// unsafe one — a predicate whose literals were deleted before the publish
    /// has no literal tag left, so it would license the equijoin-filter fold for
    /// a `t` at which those literals are still visible.
    ///
    /// The historical tag set exists so this does not cost the fold wholesale:
    /// below the index `t` the builder substitutes it (sound for every `t` at
    /// or above `historical_since_t`), so a predicate whose history is all-ref
    /// keeps the licence, one whose history carries a literal loses it, and
    /// reads below the boundary — or against an index that predates the
    /// historical wire tail — fall back to "unknown".
    #[test]
    fn below_the_index_t_the_historical_set_gates_the_ref_only_fold() {
        let ref_tag = fluree_db_core::ValueTypeTag::JSON_LD_ID.as_u8();
        let int_tag = fluree_db_core::ValueTypeTag::INTEGER.as_u8();
        // Two predicates, both all-ref in current state as of the publish:
        // `knows` has never carried anything else, `age` carried an integer
        // that was deleted before the publish.
        let stats_at = |ledger: &str, t: i64, since: Option<i64>| {
            let mut snapshot = fluree_db_core::LedgerSnapshot::genesis(ledger);
            snapshot.t = t;
            let entry = |name: &str, historical: Vec<u8>| PropertyStatEntry {
                sid: (10, name.to_string()),
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: t,
                datatypes: vec![(ref_tag, 1)],
                observed_datatypes: vec![ref_tag],
                historical_datatypes: historical,
            };
            snapshot.stats = Some(Arc::new(IndexStats {
                flakes: 2,
                size: 20,
                properties: Some(vec![
                    entry("age", vec![int_tag, ref_tag]),
                    entry("knows", vec![ref_tag]),
                ]),
                classes: None,
                graphs: None,
                historical_since_t: since,
            }));
            snapshot
        };
        let cache = StorelessStatsCache::new();
        let novelty = Novelty::new(1); // empty: the base index answers all reads
        let knows = Sid::new(10, "knows");
        let age = Sid::new(10, "age");

        // Current-state read: both licence the fold, historical sets unused.
        let snapshot = stats_at("hist-boundary:main", 5, Some(0));
        let current = view(&cache, GraphDbRef::new(&snapshot, 0, &novelty, 5), false);
        assert_eq!(
            current.is_property_ref_only(&knows),
            Some(true),
            "a read at the index's own t must still license the fold"
        );
        assert_eq!(current.is_property_ref_only(&age), Some(true));

        // Historical read at or above the boundary: the historical set is the
        // licence. Never-literal keeps the fold; deleted-literal loses it.
        let historical = view(&cache, GraphDbRef::new(&snapshot, 0, &novelty, 3), false);
        assert_eq!(
            historical.is_property_ref_only(&knows),
            Some(true),
            "a never-literal predicate lost the fold for a historical read the \
             historical set covers"
        );
        assert_eq!(
            historical.is_property_ref_only(&age),
            Some(false),
            "a read below the index t took the current-state tag set as if it \
             described that t"
        );

        // Below the adoption boundary: no coverage, everything falls back to
        // "unknown" — even the never-literal predicate.
        let adopted = stats_at("hist-adopted:main", 5, Some(3));
        let pre_adoption = view(&cache, GraphDbRef::new(&adopted, 0, &novelty, 2), false);
        assert_eq!(
            pre_adoption.is_property_ref_only(&knows),
            Some(false),
            "a read below the adoption boundary has no sound tag set and must \
             fail closed"
        );

        // An index without the boundary (an old blob) keeps today's
        // conservative behavior for every historical read.
        let old_blob = stats_at("hist-oldblob:main", 5, None);
        let old_historical = view(&cache, GraphDbRef::new(&old_blob, 0, &novelty, 3), false);
        assert_eq!(old_historical.is_property_ref_only(&knows), Some(false));
    }
}
