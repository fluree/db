use crate::Novelty;
use async_trait::async_trait;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::index_stats::union_per_graph_classes;
use fluree_db_core::is_rdf_type;
use fluree_db_core::range_provider::{RangeProvider, RangeQuery};
use fluree_db_core::{
    ClassPropertyUsage, ClassRefCount, ClassStatEntry, GraphId, GraphPropertyStatEntry,
    GraphStatsEntry, IndexStats, LedgerSnapshot, OverlayProvider, PropertyStatEntry, RangeMatch,
    RangeOptions, RangeTest, RuntimePredicateId, RuntimeSmallDicts, Sid, ValueTypeTag,
};
use fluree_db_core::{Flake, FlakeMeta, FlakeValue};
use fluree_vocab::namespaces::FLUREE_COMMIT;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

#[derive(Debug, thiserror::Error)]
pub enum StatsAssemblyError {
    #[error("{0}")]
    Message(String),
}

#[async_trait]
pub trait StatsLookup: Send + Sync {
    fn runtime_small_dicts(&self) -> Option<&RuntimeSmallDicts> {
        None
    }

    fn persisted_predicate_id_for_sid(&self, _sid: &Sid) -> Option<RuntimePredicateId> {
        None
    }

    fn runtime_predicate_id_for_sid(&self, sid: &Sid) -> Option<RuntimePredicateId> {
        resolve_runtime_predicate_id(sid, self.runtime_small_dicts(), || {
            self.persisted_predicate_id_for_sid(sid)
        })
    }

    async fn lookup_subject_classes(
        &self,
        snapshot: &LedgerSnapshot,
        overlay: &dyn OverlayProvider,
        to_t: i64,
        g_id: GraphId,
        subjects: &[Sid],
    ) -> Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError>;
}

pub fn resolve_runtime_predicate_id(
    sid: &Sid,
    runtime_small_dicts: Option<&RuntimeSmallDicts>,
    persisted_lookup: impl FnOnce() -> Option<RuntimePredicateId>,
) -> Option<RuntimePredicateId> {
    runtime_small_dicts
        .and_then(|dicts| dicts.predicate_id(sid))
        .or_else(persisted_lookup)
}

/// Tracing target the novelty/base reconciliation decision is stamped on.
/// Subscribe (e.g. `RUST_LOG=fluree::stats=debug`) to see whether an
/// assembly reconciled or fell back to the blind delta log — the routing
/// stamp regression tests assert against.
pub const STATS_MERGE_TARGET: &str = "fluree::stats";

/// Canonical `site` labels for [`NoveltyMerge::Reconciled`], one per calling
/// surface.
///
/// Constants rather than string literals at the call sites so a routing
/// assertion and the code it asserts about cannot drift apart, and so adding a
/// reconciling surface is a visible edit here.
pub mod stats_merge_site {
    /// `fluree info` / the ledger-info route, class-attributing arm — the
    /// default, since `LedgerInfoOptions::realtime_property_details` is `true`.
    pub const LEDGER_INFO_FULL: &str = "ledger-info-full";
    /// `fluree info` with `realtime_property_details` explicitly disabled.
    pub const LEDGER_INFO_FAST: &str = "ledger-info-fast";
    /// The Cypher `apoc.meta.data` per-`(class, property)` rollup.
    pub const APOC_META_DATA: &str = "apoc-meta-data";
    /// The Cypher catalog shims — `db.labels` / `db.relationshipTypes` /
    /// `db.propertyKeys` / `db.schema.visualization`.
    pub const MERGED_STATS: &str = "merged-stats";
}

/// Above this many novelty flakes an assembly that asked for
/// [`NoveltyMerge::Reconciled`] declines and falls back to
/// [`NoveltyMerge::Estimate`].
///
/// Reconciliation costs one base-index point lookup per distinct
/// `(graph, subject, predicate)` touched by novelty, so it is bounded work
/// proportional to the window. Measured worst case — every flake a duplicate
/// on its own `(s, p)`, so the probe cache misses on every one, release build,
/// local file-backed ledger (`issue_1391_reconciliation_cost` in
/// `fluree-db-api/tests/it_fast_stats_1391_regression.rs`, run with
/// `--ignored`):
///
/// ```text
/// novelty= 10,008 flakes   estimate 1.5ms   reconciled  41.7ms   (28.0x)
/// novelty= 48,008 flakes   estimate 7.6ms   reconciled 210.0ms   (27.7x)
/// ```
///
/// So ~4.3us per novelty flake, and ~210ms at the ceiling this cap permits.
/// That is a per-request metadata cost, not a query one, and it needs a
/// deliberately raised reindex threshold to reach — the default is low enough
/// that `server_defaults.rs` describes it as reindexing roughly every commit,
/// which keeps the window tiny and all of this in the noise. Real windows also
/// share `(s, p)` across values and carry new subjects, whose dictionary
/// lookup misses cheaply, so they land well under the worst case.
///
/// Above the cap, declining keeps the surface responsive and the counts carry
/// the same drift this fix removes — the stamp records which happened.
const MAX_RECONCILED_NOVELTY_FLAKES: usize = 50_000;

/// How novelty is folded onto the indexed base counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NoveltyMerge {
    /// Charge `+1` per novelty assertion and `-1` per retraction, blindly.
    ///
    /// Fast (no index reads) and wrong by exactly the number of novelty
    /// assertions that restate a fact already in the base index, plus the
    /// number of retractions of facts that were never there. Planner-grade:
    /// selectivity estimation tolerates it, and every COUNT-answering fast
    /// path declines outright when novelty is non-empty, so no query answer
    /// rides on it.
    #[default]
    Estimate,
    /// Resolve each novelty fact identity against the persisted base index
    /// before charging it, so an idempotent re-assert counts zero and a
    /// retraction of an absent fact counts zero.
    ///
    /// For surfaces that render counts to users (`fluree info`, the Cypher
    /// `apoc.meta.data` / `db.*` catalog shims). Falls back to
    /// [`NoveltyMerge::Estimate`] when the snapshot carries no base index to
    /// probe or novelty exceeds [`MAX_RECONCILED_NOVELTY_FLAKES`].
    ///
    /// `site` names the calling surface and is stamped on
    /// [`STATS_MERGE_TARGET`]. It is required, not optional, because a stamp
    /// shared across entry points is useless as a routing assertion: with
    /// several surfaces reconciling inside one capture window, a must-fire
    /// check on a shared label is satisfied by *any* of them rather than by the
    /// one whose numbers are under test.
    Reconciled { site: &'static str },
}

/// Merge novelty into `indexed` with planner-grade [`NoveltyMerge::Estimate`]
/// semantics. See [`assemble_fast_stats_with`] for the reconciled variant.
pub fn assemble_fast_stats(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
) -> IndexStats {
    assemble_fast_stats_with(
        indexed,
        snapshot,
        novelty,
        to_t,
        lookup,
        NoveltyMerge::default(),
    )
}

/// Merge novelty into `indexed` under an explicit [`NoveltyMerge`] policy.
pub fn assemble_fast_stats_with(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
    merge: NoveltyMerge,
) -> IndexStats {
    let mut deltas = NoveltyDeltaResolver::new(indexed, snapshot, novelty, merge);
    let stats = assemble_fast_stats_inner(
        indexed,
        snapshot,
        novelty,
        to_t,
        lookup,
        &mut deltas,
        RestatedAttribution::IntraPass,
    );
    deltas.finish();
    stats
}

/// Who attributes a base-present restatement to a class its subject only gains
/// inside the novelty window.
///
/// The base rollup filed that fact under the subject's *base* classes, so it
/// never counted it under the gained one — the reconciled delta of `0` is right
/// for the flat counts and wrong for that class. Exactly one pass may make up
/// the difference or the class-property breakdown doubles it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestatedAttribution {
    /// No class-attributing second pass follows, so
    /// [`assemble_fast_stats_inner`]'s intra-pass `rdf:type` side table is the
    /// only class information there is: it does the attribution itself.
    IntraPass,
    /// [`assemble_full_stats_with`]'s second pass follows and resolves classes
    /// through [`StatsLookup::lookup_subject_classes`], which is authoritative
    /// and independent of where `rdf:type` lands in POST order. Leave it there.
    DeferredToLookup,
}

/// Full (class-attributing) assembly with planner-grade
/// [`NoveltyMerge::Estimate`] semantics.
pub async fn assemble_full_stats(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    novelty: &Novelty,
    to_t: i64,
    lookup: &dyn StatsLookup,
) -> Result<IndexStats, StatsAssemblyError> {
    assemble_full_stats_with(
        indexed,
        snapshot,
        overlay,
        novelty,
        to_t,
        lookup,
        NoveltyMerge::default(),
    )
    .await
}

/// Full (class-attributing) assembly under an explicit [`NoveltyMerge`] policy.
#[allow(clippy::too_many_arguments)]
pub async fn assemble_full_stats_with(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    overlay: &dyn OverlayProvider,
    novelty: &Novelty,
    to_t: i64,
    lookup: &dyn StatsLookup,
    merge: NoveltyMerge,
) -> Result<IndexStats, StatsAssemblyError> {
    let mut deltas = NoveltyDeltaResolver::new(indexed, snapshot, novelty, merge);
    let mut stats = assemble_fast_stats_inner(
        indexed,
        snapshot,
        novelty,
        to_t,
        Some(lookup),
        &mut deltas,
        RestatedAttribution::DeferredToLookup,
    );
    // Second pass over the same POST stream: restarting run tracking replays
    // the same first-flake-per-identity decisions, and the probe's per-(g,s,p)
    // cache is already warm, so reconciliation costs no extra index reads here.
    deltas.restart_walk();
    let mut touched_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    let mut object_refs_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    let mut subject_props: HashMap<(GraphId, Sid), HashMap<Sid, PropertyDelta>> = HashMap::new();
    // Facts the base index already holds, restated inside this window. They move
    // no count, so they belong under a class only if the base rollup could not
    // already have counted them there — see `gained_classes`.
    let mut restated_props: HashMap<(GraphId, Sid), HashMap<Sid, PropertyDelta>> = HashMap::new();
    // `(graph, subject)` → the classes whose membership is new in this window.
    // A base-present fact on such a subject is still attributable under these,
    // because the base rollup attributed it under the subject's *base* classes
    // and the subject did not have this one at index time. Only built when
    // reconciling: the blind delta log resolves nothing to zero, so it would
    // never be read.
    let track_gained = deltas.is_reconciling();
    let mut gained_classes: HashMap<(GraphId, Sid), HashSet<Sid>> = HashMap::new();

    for flake in novelty.iter_flakes(IndexType::Post) {
        // Mirror the delta pass's filter order exactly, so this resolver walks
        // the same subsequence and makes the same per-identity decisions.
        if flake.t > to_t {
            continue;
        }
        let g_id = graph_id_for_flake(snapshot, flake);
        let delta = deltas.delta_in_graph(g_id, flake);
        if !include_in_runtime_stats(flake, to_t) {
            continue;
        }
        if is_rdf_type(&flake.p) {
            if delta == 0 {
                // Membership unchanged from base: no attribution moves, and the
                // subject gains nothing this window's facts must be re-filed
                // under.
                continue;
            }
            touched_by_graph
                .entry(g_id)
                .or_default()
                .insert(flake.s.clone());
            if let FlakeValue::Ref(target_class) = &flake.o {
                touched_by_graph
                    .entry(g_id)
                    .or_default()
                    .insert(target_class.clone());
                if track_gained && delta > 0 {
                    gained_classes
                        .entry((g_id, flake.s.clone()))
                        .or_default()
                        .insert(target_class.clone());
                }
            }
            continue;
        }
        if delta == 0 && !flake.op {
            // Retraction of a fact the base never held: nothing changes, under
            // any class.
            continue;
        }

        if delta == 0 {
            // Base-present and restated. It contributes no datatype/lang/ref
            // churn under the classes the base rollup already filed it under,
            // but the rollup could not have filed it under a class the subject
            // only gains here.
            restated_props
                .entry((g_id, flake.s.clone()))
                .or_default()
                .entry(flake.p.clone())
                .or_default()
                .apply_flake(flake);
            continue;
        }

        touched_by_graph
            .entry(g_id)
            .or_default()
            .insert(flake.s.clone());

        let entry = subject_props
            .entry((g_id, flake.s.clone()))
            .or_default()
            .entry(flake.p.clone())
            .or_default();
        entry.apply_flake(flake);

        if let FlakeValue::Ref(target) = &flake.o {
            object_refs_by_graph
                .entry(g_id)
                .or_default()
                .insert(target.clone());
        }
    }

    deltas.finish();

    // A restated fact on a subject that gained no class is exactly the no-op the
    // reconciliation says it is. Dropping those here keeps the class lookups
    // proportional to the subjects that actually moved: every surviving subject
    // is already in `touched_by_graph`, put there by the type flake that made it
    // gain.
    restated_props.retain(|key, _| gained_classes.contains_key(key));
    for ((g_id, _), props) in &restated_props {
        for delta in props.values() {
            for target in &delta.ref_targets {
                object_refs_by_graph
                    .entry(*g_id)
                    .or_default()
                    .insert(target.clone());
            }
        }
    }

    if subject_props.is_empty() && restated_props.is_empty() {
        return Ok(stats);
    }

    let mut graph_subject_classes: HashMap<(GraphId, Sid), Vec<Sid>> = HashMap::new();
    for (g_id, subjects) in &touched_by_graph {
        let subject_vec: Vec<Sid> = subjects.iter().cloned().collect();
        let resolved = lookup
            .lookup_subject_classes(snapshot, overlay, to_t, *g_id, &subject_vec)
            .await?;
        for (subject, classes) in resolved {
            graph_subject_classes.insert((*g_id, subject), classes);
        }
    }

    for (g_id, objects) in &object_refs_by_graph {
        let object_vec: Vec<Sid> = objects.iter().cloned().collect();
        let resolved = lookup
            .lookup_subject_classes(snapshot, overlay, to_t, *g_id, &object_vec)
            .await?;
        for (subject, classes) in resolved {
            graph_subject_classes.insert((*g_id, subject), classes);
        }
    }

    let graphs = stats.graphs.get_or_insert_with(Vec::new);
    let mut graph_index: HashMap<GraphId, usize> = graphs
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.g_id, idx))
        .collect();

    // KNOWN DRIFT, pre-existing and not addressed here: a subject whose
    // `rdf:type` lands in this same window is attributed TWICE for its property
    // flakes — once by the first pass, off its intra-pass `graph_subject_classes`
    // side table, and again here off `lookup_subject_classes`. One fact then
    // reports a class-property count of 2, under either merge mode. Closing it
    // means deciding which pass owns class attribution for the two-pass
    // assembly, which is a bigger change than reconciliation; see the residuals
    // list on #1391.
    for (props_by_subject, gained_only) in [(subject_props, false), (restated_props, true)] {
        for ((g_id, subject), props) in props_by_subject {
            let Some(class_sids) = graph_subject_classes.get(&(g_id, subject.clone())) else {
                continue;
            };
            let gained = gained_classes.get(&(g_id, subject.clone()));
            let graph_entry = get_or_insert_graph_entry(graphs, &mut graph_index, g_id);
            let classes = graph_entry.classes.get_or_insert_with(Vec::new);

            for class_sid in class_sids {
                if gained_only && !gained.is_some_and(|gained| gained.contains(class_sid)) {
                    continue;
                }
                let class_entry = get_or_insert_class_entry(classes, class_sid);
                for (property_sid, delta) in &props {
                    let prop_usage = get_or_insert_class_property(class_entry, property_sid);
                    merge_datatypes(&mut prop_usage.datatypes, &delta.datatypes);
                    merge_langs(&mut prop_usage.langs, &delta.langs);

                    for target in &delta.ref_targets {
                        if let Some(target_classes) =
                            graph_subject_classes.get(&(g_id, target.clone()))
                        {
                            for target_class in target_classes {
                                increment_ref_class(&mut prop_usage.ref_classes, target_class, 1);
                            }
                        }
                    }
                }
            }
        }
    }

    graphs.sort_by_key(|entry| entry.g_id);
    for graph in graphs.iter_mut() {
        if let Some(classes) = &mut graph.classes {
            classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
            for class in classes.iter_mut() {
                class
                    .properties
                    .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
                for prop in &mut class.properties {
                    prop.datatypes.sort_by_key(|entry| entry.0);
                    prop.langs.sort_by(|a, b| a.0.cmp(&b.0));
                    prop.ref_classes
                        .sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                }
            }
        }
    }

    stats.classes = union_per_graph_classes(graphs);
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn assemble_fast_stats_inner(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
    deltas: &mut NoveltyDeltaResolver<'_>,
    restated: RestatedAttribution,
) -> IndexStats {
    // Below the published index `t` the base index already answers the query:
    // novelty only ever holds flakes after it. Note the returned stats are
    // current state *as of the publish*, not as of `to_t` — a caller that reads
    // any of this as a statement about `to_t` (`observed_datatypes` is the one
    // that matters, see `StatsView::property_ref_only`) has to handle the
    // historical case itself.
    if novelty.is_empty() || to_t <= indexed_t(indexed, snapshot) {
        return indexed.clone();
    }

    let mut property_deltas = build_property_deltas(indexed);

    // Distinct-value / distinct-subject tracking for predicates whose indexed
    // entry carries no ndv — i.e. every predicate on an unindexed (memory-
    // mode) ledger, and brand-new predicates on indexed ones. The planner's
    // bound-object estimate is `count / ndv_values`; with ndv stuck at 0 a
    // one-value predicate and a unique-key predicate rank identically and
    // lowering order picks the join order (a per-focus sh:sparql uniqueness
    // query went quadratic in the group size exactly this way). Retraction
    // pairs net each entry to zero, so positive entries = live facts.
    // Predicates that already have indexed ndv skip this entirely.
    let indexed_ndv: HashSet<(u16, &str)> = indexed
        .properties
        .as_ref()
        .map(|props| {
            props
                .iter()
                .filter(|p| p.ndv_values > 0 || p.ndv_subjects > 0)
                .map(|p| (p.sid.0, p.sid.1.as_str()))
                .collect()
        })
        .unwrap_or_default();
    type NdvAcc = (HashMap<(FlakeValue, Sid), i64>, HashMap<Sid, i64>);
    let mut ndv_acc: HashMap<(u16, &str), NdvAcc> = HashMap::new();
    let mut class_data = build_class_data(indexed);
    let mut graphs = indexed.graphs.clone().unwrap_or_default();
    let mut graph_index: HashMap<GraphId, usize> = graphs
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.g_id, idx))
        .collect();
    let mut flakes_delta: i64 = 0;
    let mut graph_subject_classes: HashMap<(GraphId, Sid), HashSet<Sid>> = HashMap::new();
    // The subset of `graph_subject_classes` whose membership is new relative to
    // the base index — see [`RestatedAttribution`]. Only built when this pass
    // is the one that will use it, so the planner's blind delta log (where no
    // flake resolves to zero and the question never arises) pays nothing.
    let track_gained = restated == RestatedAttribution::IntraPass && deltas.is_reconciling();
    let mut gained_classes: HashMap<(GraphId, Sid), HashSet<Sid>> = HashMap::new();

    for flake in novelty.iter_flakes(IndexType::Post) {
        if flake.t > to_t {
            continue;
        }

        let g_id = graph_id_for_flake(snapshot, flake);
        let delta = deltas.delta_in_graph(g_id, flake);
        flakes_delta += delta;

        let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
        graph_entry.flakes = ((graph_entry.flakes as i64) + delta).max(0) as u64;

        if !include_in_runtime_stats(flake, to_t) {
            continue;
        }

        if is_rdf_type(&flake.p) {
            if let FlakeValue::Ref(class_sid) = &flake.o {
                let data = class_data.entry(class_sid.clone()).or_default();
                data.count_delta += delta;
                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let classes = graph_entry.classes.get_or_insert_with(Vec::new);
                let class_entry = get_or_insert_class_entry(classes, class_sid);
                class_entry.count = ((class_entry.count as i64) + delta).max(0) as u64;

                let subject_classes = graph_subject_classes
                    .entry((g_id, flake.s.clone()))
                    .or_default();
                if flake.op {
                    subject_classes.insert(class_sid.clone());
                } else {
                    subject_classes.remove(class_sid);
                }
                if track_gained && delta > 0 {
                    gained_classes
                        .entry((g_id, flake.s.clone()))
                        .or_default()
                        .insert(class_sid.clone());
                } else if track_gained && delta < 0 {
                    if let Some(gained) = gained_classes.get_mut(&(g_id, flake.s.clone())) {
                        gained.remove(class_sid);
                    }
                }
            }
            continue;
        }

        let datatype_tag = runtime_datatype_tag(flake);
        let sid_key = (flake.p.namespace_code, &*flake.p.name);
        if !indexed_ndv.contains(&sid_key) {
            let acc = ndv_acc.entry(sid_key).or_default();
            *acc.0
                .entry((flake.o.clone(), flake.dt.clone()))
                .or_insert(0) += delta;
            *acc.1.entry(flake.s.clone()).or_insert(0) += delta;
        }
        let property = property_deltas.entry(sid_key).or_default();
        property.count += delta;
        if flake.op {
            property.asserted_datatypes.insert(datatype_tag);
        }
        *property.datatype_deltas.entry(datatype_tag).or_insert(0) += delta;

        if let Some(stats_lookup) = lookup {
            if let Some(p_id) = stats_lookup.runtime_predicate_id_for_sid(&flake.p) {
                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let prop_entry = get_or_insert_graph_property(graph_entry, p_id);
                prop_entry.count = ((prop_entry.count as i64) + delta).max(0) as u64;
                prop_entry.last_modified_t = prop_entry.last_modified_t.max(flake.t);
                update_graph_property_datatypes(prop_entry, flake, delta);
            }
        }

        if let Some(class_sids) = graph_subject_classes.get(&(g_id, flake.s.clone())) {
            let gained = gained_classes.get(&(g_id, flake.s.clone()));
            for class_sid in class_sids {
                // A restatement of a fact the base index already holds moves no
                // count — except under a class the subject only gains in this
                // window, which the base rollup could not have filed it under.
                let class_delta = if delta == 0
                    && flake.op
                    && gained.is_some_and(|gained| gained.contains(class_sid))
                {
                    1
                } else {
                    delta
                };
                let class = class_data.entry(class_sid.clone()).or_default();
                let prop = class.properties.entry(flake.p.clone()).or_default();
                prop.count_delta += class_delta;

                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let classes = graph_entry.classes.get_or_insert_with(Vec::new);
                let class_entry = get_or_insert_class_entry(classes, class_sid);
                let prop_usage = get_or_insert_class_property(class_entry, &flake.p);
                update_class_property_usage(
                    prop_usage,
                    flake,
                    class_delta,
                    &graph_subject_classes,
                    g_id,
                );
            }
        }
    }

    let novelty_ndv: HashMap<(u16, &str), (u64, u64)> = ndv_acc
        .into_iter()
        .map(|(sid, (values, subjects))| {
            (
                sid,
                (
                    values.values().filter(|&&c| c > 0).count() as u64,
                    subjects.values().filter(|&&c| c > 0).count() as u64,
                ),
            )
        })
        .collect();

    let mut stats = finalize_stats(indexed, property_deltas, class_data, &novelty_ndv);
    stats.flakes = (indexed.flakes as i64 + flakes_delta).max(0) as u64;
    stats.size = indexed.size + novelty.size as u64;
    if !graphs.is_empty() {
        graphs.sort_by_key(|entry| entry.g_id);
        for graph in &mut graphs {
            graph.properties.sort_by_key(|entry| entry.p_id);
            if let Some(classes) = &mut graph.classes {
                classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                for class in classes.iter_mut() {
                    class
                        .properties
                        .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
                    for prop in &mut class.properties {
                        prop.datatypes.sort_by_key(|entry| entry.0);
                        prop.langs.sort_by(|a, b| a.0.cmp(&b.0));
                        prop.ref_classes
                            .sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
                    }
                }
            }
        }
        stats.classes = union_per_graph_classes(&graphs);
        stats.graphs = Some(graphs);
    }
    stats
}

// ---------------------------------------------------------------------------
// Novelty → base reconciliation (#1391)
// ---------------------------------------------------------------------------

/// Cache key for one base-index probe: the `(graph, subject, predicate)` the
/// scan was bounded to.
type BaseFactKey = (GraphId, Sid, Sid);

/// One currently-asserted base fact under a [`BaseFactKey`]: `(o, dt, m)` —
/// the rest of the identity `NoveltyFactState` keys on.
type BaseFact = (FlakeValue, Sid, Option<FlakeMeta>);

/// Point-lookup oracle for "does the persisted base index already hold this
/// fact?", with a per-`(graph, subject, predicate)` result cache.
///
/// The cache is what makes reconciliation affordable: novelty is walked in
/// POST order, so the same `(s, p)` recurs across objects and across the two
/// passes [`assemble_full_stats_with`] makes, and each distinct pair costs one
/// bounded SPOT scan of the base index (`s` and `p` both bound). Reads go
/// against an *empty* overlay at the indexed `t` — the question is strictly
/// about the base, not about the merged view.
struct BaseIndexProbe<'a> {
    /// Calling surface, for the routing stamp.
    site: &'static str,
    provider: &'a dyn RangeProvider,
    /// The `t` the `indexed` stats describe — the base state to probe.
    ///
    /// This must be the same `t` novelty was flushed to, or a partial flush
    /// could leave a retraction in the window whose matching assert has already
    /// moved into a base this probe reads as of an earlier `t`, charging `0`
    /// where the truth is `-1`. It is, by construction: both production callers
    /// of `Novelty::clear_up_to` (`fluree-db-ledger/src/lib.rs`) pass
    /// `new_snapshot.t` — the cutoff and the snapshot installed alongside it are
    /// the same value — and both refuse a snapshot whose `t` is below the
    /// current index `t`, so it cannot lag.
    indexed_t: i64,
    /// Read base-only: an overlay with nothing in it.
    empty_overlay: Novelty,
    /// `(g_id, s, p)` → the base facts currently asserted for that pair, as
    /// `(o, dt, m)`. Fan-out per `(s, p)` is small in practice.
    cache: HashMap<BaseFactKey, Vec<BaseFact>>,
    /// Base scans issued (one per cache miss) — stamped for observability.
    scans: usize,
    /// Novelty facts found already present in base — the over-count this
    /// reconciliation removes. Stamped so a regression test can prove the
    /// pass actually did something rather than passing vacuously.
    duplicates: usize,
}

impl<'a> BaseIndexProbe<'a> {
    /// Build a probe when `merge` asks for reconciliation AND the snapshot can
    /// answer it cheaply enough. Returns `None` (blind delta log, today's
    /// behavior) otherwise, stamping why.
    fn open(
        indexed: &IndexStats,
        snapshot: &'a LedgerSnapshot,
        novelty: &Novelty,
        merge: NoveltyMerge,
    ) -> Option<BaseIndexProbe<'a>> {
        let NoveltyMerge::Reconciled { site } = merge else {
            return None;
        };
        // Nothing indexed yet ⇒ no base fact can be duplicated; every novelty
        // assertion is genuinely new and the blind delta is already exact.
        if indexed.graphs.is_none() && indexed.properties.is_none() && indexed.classes.is_none() {
            stamp_merge(site, "declined:no_base_index", 0, 0);
            return None;
        }
        let Some(provider) = snapshot.range_provider.as_ref() else {
            stamp_merge(site, "declined:no_range_provider", 0, 0);
            return None;
        };
        if novelty.len() > MAX_RECONCILED_NOVELTY_FLAKES {
            stamp_merge(site, "declined:novelty_too_large", 0, 0);
            return None;
        }
        Some(BaseIndexProbe {
            site,
            provider: provider.as_ref(),
            indexed_t: snapshot.t,
            empty_overlay: Novelty::new(0),
            cache: HashMap::new(),
            scans: 0,
            duplicates: 0,
        })
    }

    /// Is `flake`'s fact identity `(s, p, o, dt, m)` currently asserted in the
    /// base index of graph `g_id`?
    ///
    /// `o` and `dt` are compared with `Ord` because the cached facts are kept
    /// sorted on `(o, dt)` for the binary search below, and that ordering is
    /// the one the index comparators and `NoveltyFactState`'s keys already use.
    ///
    /// `Ord` is not the stricter relation here — on numerics it is the same
    /// one. `FlakeValue`'s `cmp` and its `eq` both route cross-representation
    /// numerics through `numeric_cmp`, so `Long(3)` and `Double(3.0)` compare
    /// `Equal` under either. What keeps them from being one identity is the
    /// `dt` tiebreak applied alongside `o`; drop that and this would suppress a
    /// legitimate assertion whichever relation `o` used.
    ///
    /// `m` is matched with `==` rather than `cmp` because it is not part of the
    /// sort key: the block the search lands on is ordered on `(o, dt)` only, so
    /// `m` filters that block rather than continuing the ordering. (#1727
    /// brings `FlakeMeta`'s `Ord` into agreement with its `Eq`, so the pairing
    /// holds either way.)
    ///
    /// Binary-searching to the matching block and walking only that is what
    /// keeps the work bounded: a `(s, p)` with large base fan-out — an RDF
    /// list, a heavily multi-valued predicate — would otherwise cost
    /// `O(identities x fan-out)` on the surface whose whole point is bounded work.
    fn base_contains(&mut self, g_id: GraphId, flake: &Flake) -> bool {
        let key = (g_id, flake.s.clone(), flake.p.clone());
        if !self.cache.contains_key(&key) {
            self.scans += 1;
            let facts = self.scan_base(g_id, &flake.s, &flake.p);
            self.cache.insert(key.clone(), facts);
        }
        let facts = &self.cache[&key];
        let start = facts.partition_point(|(o, dt, _)| {
            o.cmp(&flake.o).then_with(|| dt.cmp(&flake.dt)) == Ordering::Less
        });
        let hit = facts[start..]
            .iter()
            .take_while(|(o, dt, _)| {
                o.cmp(&flake.o) == Ordering::Equal && dt.cmp(&flake.dt) == Ordering::Equal
            })
            .any(|(_, _, m)| *m == flake.m);
        if hit && flake.op {
            self.duplicates += 1;
        }
        hit
    }

    /// One bounded base scan for `(s, p)`, sorted on `(o, dt)` for
    /// [`Self::base_contains`]'s binary search.
    ///
    /// The sort is defensive rather than necessary — SPOT is
    /// `(s, p, o, dt, t, op, m)`, so with `s` and `p` bound a provider returns
    /// this already ordered — but it costs `O(n log n)` once per `(s, p)` and
    /// removes a silent dependency on provider ordering.
    ///
    /// A provider error (or an absent index order) yields an empty set, which
    /// degrades to today's blind delta for that pair — never to a suppressed
    /// assertion.
    fn scan_base(&self, g_id: GraphId, s: &Sid, p: &Sid) -> Vec<BaseFact> {
        let match_val = RangeMatch::subject_predicate(s.clone(), p.clone());
        let opts = RangeOptions::new().with_to_t(self.indexed_t);
        let query = RangeQuery {
            g_id,
            index: IndexType::Spot,
            test: RangeTest::Eq,
            match_val: &match_val,
            opts: &opts,
            overlay: &self.empty_overlay,
            tracker: None,
        };
        match self.provider.range(&query) {
            Ok(flakes) => {
                let mut facts: Vec<BaseFact> = flakes
                    .into_iter()
                    .filter(|f| f.op)
                    .map(|f| (f.o, f.dt, f.m))
                    .collect();
                facts.sort_by(|(o1, dt1, _), (o2, dt2, _)| o1.cmp(o2).then_with(|| dt1.cmp(dt2)));
                facts
            }
            Err(e) => {
                tracing::debug!(
                    target: STATS_MERGE_TARGET,
                    error = %e,
                    "base-presence probe failed; falling back to the blind novelty delta"
                );
                Vec::new()
            }
        }
    }
}

/// Stamp one reconciliation decision on [`STATS_MERGE_TARGET`], labelled with
/// the calling surface so a routing assertion can name the lane it means.
fn stamp_merge(site: &'static str, outcome: &'static str, scans: usize, duplicates: usize) {
    tracing::debug!(
        target: STATS_MERGE_TARGET,
        site,
        outcome,
        scans,
        duplicates,
        "fast-stats novelty merge outcome",
    );
}

/// Turns the novelty POST stream into per-flake current-state deltas.
///
/// Drive this over a novelty walk in `IndexType::Post` order to get each
/// flake's *current-state* delta instead of a blind `±1`.
///
/// It folds **presence**, not ops. Per fact identity it carries the op that has
/// won lifecycle resolution so far and charges each flake the change in
/// presence it causes; the first flake of an identity costs one base-index
/// probe and charges `op − base_present`. Those deltas telescope, so the sum
/// over an identity is `final_present − base_present` regardless of how many
/// flakes the window holds or what order they arrive in.
///
/// It deliberately does **not** assume kept flakes alternate assert/retract.
/// Novelty's set semantics are one-sided — `Novelty::apply_commit` suppresses
/// redundant *asserts* only — and a same-`t` assert/retract pair reaches this
/// fold retract-first because POST sorts `op` ascending, so runs like
/// `(retract, assert, assert)` and `(retract, retract)` are both reachable.
/// See `delta_in_graph`.
///
/// Any consumer that folds novelty into indexed counts by hand — the stats
/// assemblers here, `apoc.meta.data`'s per-`(class, property)` rollup — should
/// take its deltas from one of these rather than reading `flake.op` directly,
/// or it inherits #1391.
///
/// One caveat for consumers that attribute per class rather than counting flat:
/// a delta of `0` says the fact's presence is unchanged from the base index, not
/// that the base rollup counted it under the class you are about to file it
/// under. The rollup filed each base fact under the classes its subject held AT
/// INDEX TIME, so a class the subject gains inside this window has no base
/// contribution to double, and a restatement is the only thing that can put the
/// fact there. `delta > 0` on the `rdf:type` flake is the signal, and both
/// assemblers here and `meta_data_rows` use it.
///
/// # Ordering contract
///
/// Flakes must arrive grouped by graph, and in `IndexType::Post` order —
/// `(p, o, dt, s, t, op, m)` — within each graph. That is exactly what
/// `Novelty::iter_flakes(IndexType::Post)` yields: `present_graphs()` ascending,
/// each graph's segments k-way merged on the POST comparator. A debug build
/// asserts it. Skipping flakes is fine as long as the filter is by identity
/// component (predicate, subject, graph): a partially consumed run still
/// resolves correctly because same-run flakes stay contiguous.
pub struct NoveltyDeltaResolver<'a> {
    snapshot: &'a LedgerSnapshot,
    probe: Option<BaseIndexProbe<'a>>,
    /// Identity components of the run in progress, `(g_id, p, o, dt, s)`.
    run: Option<(GraphId, Sid, FlakeValue, Sid, Sid)>,
    /// Per-identity lifecycle state for the run in progress: `(m, t, op)`,
    /// where `(t, op)` is the op that has WON resolution so far. Only `m`
    /// varies within a run, so this vec is as long as the value's distinct
    /// language tags / list positions.
    seen: Vec<(Option<FlakeMeta>, i64, bool)>,
}

impl<'a> NoveltyDeltaResolver<'a> {
    /// Open a resolver. Under [`NoveltyMerge::Estimate`], or when the snapshot
    /// carries no base index to probe, or when novelty is larger than
    /// reconciliation's budget, this degrades to the blind `±1` delta log and
    /// stamps why on [`STATS_MERGE_TARGET`].
    pub fn new(
        indexed: &IndexStats,
        snapshot: &'a LedgerSnapshot,
        novelty: &Novelty,
        merge: NoveltyMerge,
    ) -> Self {
        Self {
            snapshot,
            probe: BaseIndexProbe::open(indexed, snapshot, novelty, merge),
            run: None,
            seen: Vec::new(),
        }
    }

    /// Whether this resolver is actually reconciling against the base index.
    pub fn is_reconciling(&self) -> bool {
        self.probe.is_some()
    }

    /// Delta `flake` contributes to current-state counts, resolving its graph
    /// from the snapshot's registry.
    pub fn delta_for(&mut self, flake: &Flake) -> i64 {
        let g_id = graph_id_for_flake(self.snapshot, flake);
        self.delta_in_graph(g_id, flake)
    }

    /// Delta `flake` contributes, for callers that already resolved its graph.
    ///
    /// This is a running fold over *presence*, not over ops. The first flake of
    /// an identity costs one base probe and charges `op − base_present`; each
    /// later flake charges `new_present − old_present`, where the new presence
    /// is whatever lifecycle resolution says it is. The deltas telescope, so
    /// their sum over an identity is exactly `final_present − base_present`
    /// however many flakes the window holds.
    ///
    /// Resolution is the same rule [`crate::Novelty`]'s `fact_state::record`
    /// applies — the highest `t` wins, and at equal `t` a retract beats an
    /// assert — so the fold cannot disagree with what a read would see. It
    /// deliberately does **not** assume kept flakes alternate assert/retract.
    /// They do not: `Novelty::apply_commit`'s dedup gate short-circuits on
    /// `flake.op &&`, so it suppresses redundant *asserts* only and never
    /// examines a retraction. And because POST sorts `op` ascending at equal
    /// `t`, a same-`t` pair reaches this fold retract-first — the assert
    /// arrives last despite having lost. `bulk_apply_commits` replays raw
    /// persisted flakes on cold load, which is why the crate keeps
    /// `same_t_assert_retract_keeps_later_reassert` around to pin that state as
    /// legal.
    ///
    /// POST order puts `m` last, so flakes sharing `(p, o, dt, s)` are
    /// contiguous while distinct `m`s (language tags, list positions) interleave
    /// by `t` within that run — hence per-identity state keyed on `m`. The run
    /// key carries `g_id` too: the same triple can live in two named graphs, and
    /// each has its own base-index state to resolve against.
    pub fn delta_in_graph(&mut self, g_id: GraphId, flake: &Flake) -> i64 {
        let Some(probe) = self.probe.as_mut() else {
            return if flake.op { 1 } else { -1 };
        };
        // Commit-metadata flakes: subject is the commit's own CID digest hex
        // (`commit_flakes.rs`), so it is content-addressed and unique per
        // commit — no base fact can share the identity, and the blind delta is
        // already exact. Probing them anyway would be 7-10 guaranteed cache
        // misses per commit in the window, and `include_in_runtime_stats`
        // discards every one of them from the per-class and per-property counts
        // this reconciliation exists to fix. Skipping is legal under the
        // ordering contract above: the subject namespace is an identity
        // component, so the skipped set is disjoint by identity from the
        // resolved set and no run is left half-consumed.
        if flake.s.namespace_code == FLUREE_COMMIT {
            return if flake.op { 1 } else { -1 };
        }
        // Contiguity is what lets per-identity state live in a per-run vec
        // instead of a map over the whole window: if a run could reopen after
        // closing, an identity's first flake would be charged against base
        // presence twice. Fail loudly in debug rather than silently mis-count
        // if the stream ever stops being non-decreasing within a graph (a
        // segmented-novelty merge regression). Note this checks *contiguity*
        // only — the fold makes no assumption about op order within a run.
        #[cfg(debug_assertions)]
        if let Some((prev_g, p, o, dt, s)) = self.run.as_ref() {
            let ord = p
                .cmp(&flake.p)
                .then_with(|| o.cmp(&flake.o))
                .then_with(|| dt.cmp(&flake.dt))
                .then_with(|| s.cmp(&flake.s));
            debug_assert!(
                *prev_g != g_id || ord != Ordering::Greater,
                "NoveltyDeltaResolver requires a non-decreasing POST stream \
                 within a graph; got {:?}/{:?} after {p:?}/{o:?} in graph {g_id}",
                flake.p,
                flake.o,
            );
        }
        if !self.run.as_ref().is_some_and(|(g, p, o, dt, s)| {
            *g == g_id
                && *p == flake.p
                && o.cmp(&flake.o) == Ordering::Equal
                && *dt == flake.dt
                && *s == flake.s
        }) {
            self.run = Some((
                g_id,
                flake.p.clone(),
                flake.o.clone(),
                flake.dt.clone(),
                flake.s.clone(),
            ));
            self.seen.clear();
        }
        if let Some((_, cur_t, cur_op)) = self.seen.iter_mut().find(|(m, _, _)| *m == flake.m) {
            // Lifecycle resolution, verbatim from `NoveltyFactState::record`.
            // A flake that does not win changes no presence and charges zero —
            // that covers the same-`t` assert arriving behind its winning
            // retract, and a redundant retract repeated at a later `t`.
            let wins = flake.t > *cur_t || (flake.t == *cur_t && *cur_op && !flake.op);
            if !wins {
                return 0;
            }
            let delta = i64::from(flake.op) - i64::from(*cur_op);
            *cur_t = flake.t;
            *cur_op = flake.op;
            return delta;
        }
        self.seen.push((flake.m.clone(), flake.t, flake.op));
        let base = i64::from(probe.base_contains(g_id, flake));
        i64::from(flake.op) - base
    }

    /// Restart run tracking for a second walk of the same POST stream, keeping
    /// the base-probe cache warm so the second pass reads no extra leaflets.
    ///
    /// `duplicates` resets with it: `base_contains` counts a duplicate on every
    /// call, so without this the two-pass `assemble_full_stats_with` would stamp
    /// 2x. That matters because the regression test's anti-vacuity guard is a
    /// *floor* on `duplicates` — inflating it makes the guard easier to satisfy,
    /// which is the wrong direction for a check whose job is to prove the pass
    /// did real work. `scans` deliberately keeps accumulating: it measures index
    /// reads, and the second pass genuinely issues none.
    pub fn restart_walk(&mut self) {
        self.run = None;
        self.seen.clear();
        if let Some(probe) = self.probe.as_mut() {
            probe.duplicates = 0;
        }
    }

    /// Stamp what this resolver did. Call once per assembly; a resolver that
    /// declined already stamped its reason at construction.
    pub fn finish(&self) {
        if let Some(probe) = self.probe.as_ref() {
            stamp_merge(probe.site, "reconciled", probe.scans, probe.duplicates);
        }
    }
}

#[derive(Debug, Default)]
struct PropertyDelta {
    datatypes: Vec<(u8, u64)>,
    langs: Vec<(String, u64)>,
    ref_targets: Vec<Sid>,
}

impl PropertyDelta {
    fn apply_flake(&mut self, flake: &Flake) {
        let delta = if flake.op { 1 } else { -1 };
        increment_count(&mut self.datatypes, runtime_datatype_tag(flake), delta);
        if let Some(lang) = flake.m.as_ref().and_then(|meta| meta.lang.as_ref()) {
            increment_string_count(&mut self.langs, lang.clone(), delta);
        }
        if let FlakeValue::Ref(target) = &flake.o {
            if flake.op {
                self.ref_targets.push(target.clone());
            }
        }
    }
}

fn include_in_runtime_stats(flake: &Flake, to_t: i64) -> bool {
    if flake.t > to_t {
        return false;
    }
    if flake.s.namespace_code == FLUREE_COMMIT {
        return false;
    }
    if let Some(g) = &flake.g {
        let name = g.name.as_ref();
        if name.contains("txn-meta") {
            return false;
        }
    }
    true
}

fn graph_id_for_flake(snapshot: &LedgerSnapshot, flake: &Flake) -> GraphId {
    let Some(g_sid) = &flake.g else {
        return 0;
    };
    snapshot
        .decode_sid(g_sid)
        .and_then(|iri| snapshot.graph_registry.graph_id_for_iri(&iri))
        .unwrap_or(0)
}

fn indexed_t(indexed: &IndexStats, snapshot: &LedgerSnapshot) -> i64 {
    if indexed.graphs.is_some() || indexed.properties.is_some() || indexed.classes.is_some() {
        snapshot.t
    } else {
        0
    }
}

fn get_or_insert_graph_entry<'a>(
    graphs: &'a mut Vec<GraphStatsEntry>,
    graph_index: &mut HashMap<GraphId, usize>,
    g_id: GraphId,
) -> &'a mut GraphStatsEntry {
    if let Some(idx) = graph_index.get(&g_id).copied() {
        return &mut graphs[idx];
    }
    let idx = graphs.len();
    graphs.push(GraphStatsEntry {
        g_id,
        flakes: 0,
        size: 0,
        properties: Vec::new(),
        classes: Some(Vec::new()),
    });
    graph_index.insert(g_id, idx);
    &mut graphs[idx]
}

fn get_or_insert_graph_property(
    graph_entry: &mut GraphStatsEntry,
    p_id: RuntimePredicateId,
) -> &mut GraphPropertyStatEntry {
    if let Some(idx) = graph_entry
        .properties
        .iter()
        .position(|entry| entry.p_id == p_id.as_u32())
    {
        return &mut graph_entry.properties[idx];
    }
    graph_entry.properties.push(GraphPropertyStatEntry {
        p_id: p_id.as_u32(),
        count: 0,
        ndv_values: 0,
        ndv_subjects: 0,
        last_modified_t: 0,
        datatypes: Vec::new(),
        observed_datatypes: Vec::new(),
        historical_datatypes: Vec::new(),
    });
    graph_entry.properties.last_mut().expect("just inserted")
}

fn get_or_insert_class_entry<'a>(
    classes: &'a mut Vec<ClassStatEntry>,
    class_sid: &Sid,
) -> &'a mut ClassStatEntry {
    if let Some(idx) = classes
        .iter()
        .position(|entry| entry.class_sid == *class_sid)
    {
        return &mut classes[idx];
    }
    classes.push(ClassStatEntry {
        class_sid: class_sid.clone(),
        count: 0,
        properties: Vec::new(),
    });
    classes.last_mut().expect("just inserted")
}

fn get_or_insert_class_property<'a>(
    class_entry: &'a mut ClassStatEntry,
    property_sid: &Sid,
) -> &'a mut ClassPropertyUsage {
    if let Some(idx) = class_entry
        .properties
        .iter()
        .position(|entry| entry.property_sid == *property_sid)
    {
        return &mut class_entry.properties[idx];
    }
    class_entry.properties.push(ClassPropertyUsage {
        property_sid: property_sid.clone(),
        datatypes: Vec::new(),
        langs: Vec::new(),
        ref_classes: Vec::new(),
    });
    class_entry.properties.last_mut().expect("just inserted")
}

fn update_graph_property_datatypes(
    prop_entry: &mut GraphPropertyStatEntry,
    flake: &Flake,
    delta: i64,
) {
    let tag = runtime_datatype_tag(flake);
    increment_count(&mut prop_entry.datatypes, tag, delta);
    // The graph-scoped twin of the aggregate lane's `asserted_datatypes`
    // (#1738): the counts above are a blind ±1 delta log, so the *set* a
    // consumer may license a rewrite on is maintained separately — assertions
    // only ever add to it, so no retraction, spurious or not, can take a tag
    // away. `historical_datatypes` is deliberately not touched here: the
    // runtime merge's output is never consulted below the index `t`, and the
    // historical sets are owned by the build pipelines and decoders.
    if flake.op {
        note_observed_tag(&mut prop_entry.observed_datatypes, tag);
    }
}

fn update_class_property_usage(
    prop_usage: &mut ClassPropertyUsage,
    flake: &Flake,
    delta: i64,
    graph_subject_classes: &HashMap<(GraphId, Sid), HashSet<Sid>>,
    g_id: GraphId,
) {
    increment_count(
        &mut prop_usage.datatypes,
        runtime_datatype_tag(flake),
        delta,
    );
    if let Some(lang) = flake.m.as_ref().and_then(|meta| meta.lang.as_ref()) {
        increment_string_count(&mut prop_usage.langs, lang.clone(), delta);
    }
    if let FlakeValue::Ref(target_sid) = &flake.o {
        if let Some(target_classes) = graph_subject_classes.get(&(g_id, target_sid.clone())) {
            for target_class in target_classes {
                increment_ref_class(&mut prop_usage.ref_classes, target_class, delta);
            }
        }
    }
}

fn runtime_datatype_tag(flake: &Flake) -> u8 {
    if matches!(flake.o, FlakeValue::Ref(_)) {
        ValueTypeTag::JSON_LD_ID.as_u8()
    } else {
        ValueTypeTag::from_ns_name(flake.dt.namespace_code, &flake.dt.name).as_u8()
    }
}

/// Insert `tag` into a sorted, deduplicated tag set.
fn note_observed_tag(tags: &mut Vec<u8>, tag: u8) {
    if let Err(idx) = tags.binary_search(&tag) {
        tags.insert(idx, tag);
    }
}

fn increment_count(entries: &mut Vec<(u8, u64)>, tag: u8, delta: i64) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.0 == tag) {
        entry.1 = ((entry.1 as i64) + delta).max(0) as u64;
    } else if delta > 0 {
        entries.push((tag, delta as u64));
    }
    entries.retain(|entry| entry.1 > 0);
}

fn increment_string_count(entries: &mut Vec<(String, u64)>, key: String, delta: i64) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.0 == key) {
        entry.1 = ((entry.1 as i64) + delta).max(0) as u64;
    } else if delta > 0 {
        entries.push((key, delta as u64));
    }
    entries.retain(|entry| entry.1 > 0);
}

fn merge_datatypes(target: &mut Vec<(u8, u64)>, source: &[(u8, u64)]) {
    for (tag, count) in source {
        increment_count(target, *tag, *count as i64);
    }
}

fn merge_langs(target: &mut Vec<(String, u64)>, source: &[(String, u64)]) {
    for (lang, count) in source {
        increment_string_count(target, lang.clone(), *count as i64);
    }
}

fn increment_ref_class(entries: &mut Vec<ClassRefCount>, class_sid: &Sid, delta: i64) {
    if let Some(entry) = entries
        .iter_mut()
        .find(|entry| entry.class_sid == *class_sid)
    {
        entry.count = ((entry.count as i64) + delta).max(0) as u64;
    } else if delta > 0 {
        entries.push(ClassRefCount {
            class_sid: class_sid.clone(),
            count: delta as u64,
        });
    }
    entries.retain(|entry| entry.count > 0);
}

/// Everything the novelty walk accumulates per predicate, in one entry.
///
/// The three quantities below are keyed by the same `(namespace_code, name)`
/// pair, so keeping them in three maps cost three key constructions — three
/// heap allocations, when the key owned its name — for every flake in the
/// window. The walk covers the whole novelty window on every stats-cache
/// rebuild (once per overlay epoch), so it is worth keying once. Borrowing the
/// name from the flake (or from the index entry that seeded it) drops that to
/// zero allocations per flake; the owned `String` the wire format wants is
/// built once per distinct predicate in [`finalize_stats`].
#[derive(Debug, Default)]
struct PropertyStatDelta {
    /// Index count, then novelty's ±1 per flake.
    count: i64,
    /// Per-datatype ±1 from novelty, so the aggregate `datatypes` breakdown
    /// tracks novelty rather than the index alone. Estimates only — see
    /// [`merge_property_datatypes`].
    datatype_deltas: HashMap<u8, i64>,
    /// The tags novelty **asserted**. Unioned with the base index's tags this
    /// gives `PropertyStatEntry::observed_datatypes`, which is what the
    /// equijoin-filter fold's node-only soundness guard reads. Assertions only
    /// ever add to it, so no retraction — spurious or not — can take a literal
    /// tag away and make a mixed predicate read as all-ref.
    asserted_datatypes: HashSet<u8>,
}

type PropertyDeltaMap<'a> = HashMap<(u16, &'a str), PropertyStatDelta>;

fn build_property_deltas(indexed: &IndexStats) -> PropertyDeltaMap<'_> {
    let mut deltas = PropertyDeltaMap::new();
    if let Some(ref props) = indexed.properties {
        for entry in props {
            deltas.insert(
                (entry.sid.0, entry.sid.1.as_str()),
                PropertyStatDelta {
                    count: entry.count as i64,
                    ..PropertyStatDelta::default()
                },
            );
        }
    }
    deltas
}

#[derive(Debug, Default)]
struct ClassDataMut {
    count_delta: i64,
    properties: HashMap<Sid, PropertyDataMut>,
}

#[derive(Debug, Default)]
struct PropertyDataMut {
    count_delta: i64,
    ref_classes: Vec<ClassRefCount>,
}

fn build_class_data(indexed: &IndexStats) -> HashMap<Sid, ClassDataMut> {
    let mut class_data = HashMap::new();
    if let Some(ref classes) = indexed.classes {
        for entry in classes {
            let mut props = HashMap::new();
            for prop_usage in &entry.properties {
                props.insert(
                    prop_usage.property_sid.clone(),
                    PropertyDataMut {
                        count_delta: 1,
                        ref_classes: prop_usage.ref_classes.clone(),
                    },
                );
            }
            class_data.insert(
                entry.class_sid.clone(),
                ClassDataMut {
                    count_delta: entry.count as i64,
                    properties: props,
                },
            );
        }
    }
    class_data
}

/// Merge index per-datatype counts with novelty deltas, dropping any datatype
/// whose current-state count is zero. Keeps the aggregate `datatypes` breakdown
/// tracking novelty rather than the index alone, for the estimators that sum it.
///
/// These are estimates, and the drop is not reliable: a retraction of a fact the
/// base index never held still charges its `-1`, so a tag can vanish while the
/// data it described is still there. Read [`union_observed_datatypes`] instead
/// of this if you need the *set* of datatypes a property carries.
fn merge_property_datatypes(index: &[(u8, u64)], deltas: &HashMap<u8, i64>) -> Vec<(u8, u64)> {
    let mut merged: HashMap<u8, i64> = index.iter().map(|&(t, c)| (t, c as i64)).collect();
    for (&tag, &d) in deltas {
        *merged.entry(tag).or_insert(0) += d;
    }
    let mut out: Vec<(u8, u64)> = merged
        .into_iter()
        .filter(|&(_, c)| c > 0)
        .map(|(t, c)| (t, c as u64))
        .collect();
    out.sort_by_key(|&(t, _)| t);
    out
}

/// Union the base index's observed datatype tags with the tags novelty
/// ASSERTED, which is the input to `StatsView::property_ref_only`.
///
/// Deliberately not derived from [`merge_property_datatypes`]: that merge is
/// arithmetic over a blind ±1 delta log, so a retraction of a fact the base
/// index never held drives a tag's count to zero and drops it — and a predicate
/// carrying both refs and literals then reads as all-ref, licensing the
/// equijoin-filter fold to rewrite a `FILTER(?x = ?y)` it must not touch
/// (#1721). A union over assertions cannot lose a tag, so the flag it feeds can
/// only ever be conservative: after legitimately deleting every literal under a
/// predicate the fold stays declined until the next index publish reissues the
/// base tag set without it.
fn union_observed_datatypes(base: &[u8], asserted: &HashSet<u8>) -> Vec<u8> {
    let mut tags: Vec<u8> = base.to_vec();
    tags.extend(asserted.iter().copied());
    tags.sort_unstable();
    tags.dedup();
    tags
}

fn finalize_stats(
    indexed: &IndexStats,
    property_deltas: PropertyDeltaMap<'_>,
    class_data: HashMap<Sid, ClassDataMut>,
    novelty_ndv: &HashMap<(u16, &str), (u64, u64)>,
) -> IndexStats {
    let properties = if property_deltas.is_empty() {
        indexed.properties.clone()
    } else {
        let indexed_props: HashMap<(u16, &str), &PropertyStatEntry> = indexed
            .properties
            .as_ref()
            .map(|props| {
                props
                    .iter()
                    .map(|p| ((p.sid.0, p.sid.1.as_str()), p))
                    .collect()
            })
            .unwrap_or_default();

        let mut entries: Vec<_> = property_deltas.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let props: Vec<PropertyStatEntry> = entries
            .into_iter()
            .filter(|(_, delta)| delta.count > 0)
            .map(|(sid, delta)| {
                let indexed_entry = indexed_props.get(&sid);
                let datatypes = merge_property_datatypes(
                    indexed_entry.map(|e| e.datatypes.as_slice()).unwrap_or(&[]),
                    &delta.datatype_deltas,
                );
                let observed_datatypes = union_observed_datatypes(
                    indexed_entry
                        .map(|e| e.observed_datatypes.as_slice())
                        .unwrap_or(&[]),
                    &delta.asserted_datatypes,
                );
                // ndv: prefer the indexed figure; fall back to the live-set
                // counts assembled from novelty for predicates the index has
                // no (or zero) ndv for — without this, memory-mode planning
                // saw every bound-object probe as equally unselective.
                let (novelty_values, novelty_subjects) =
                    novelty_ndv.get(&sid).copied().unwrap_or((0, 0));
                PropertyStatEntry {
                    sid: (sid.0, sid.1.to_string()),
                    count: delta.count.max(0) as u64,
                    ndv_values: indexed_entry
                        .map(|e| e.ndv_values)
                        .filter(|&v| v > 0)
                        .unwrap_or(novelty_values),
                    ndv_subjects: indexed_entry
                        .map(|e| e.ndv_subjects)
                        .filter(|&v| v > 0)
                        .unwrap_or(novelty_subjects),
                    last_modified_t: indexed_entry.map(|e| e.last_modified_t).unwrap_or(0),
                    datatypes,
                    observed_datatypes,
                    // Owned by the build pipelines: the runtime merge's output
                    // is never consulted below the index `t`, so the base's
                    // set passes through untouched.
                    historical_datatypes: indexed_entry
                        .map(|e| e.historical_datatypes.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();
        if props.is_empty() {
            None
        } else {
            Some(props)
        }
    };

    let classes = if class_data.is_empty() {
        indexed.classes.clone()
    } else {
        let mut entries: Vec<_> = class_data.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let class_entries: Vec<ClassStatEntry> = entries
            .into_iter()
            .filter(|(_, data)| data.count_delta > 0)
            .map(|(class_sid, data)| {
                let mut prop_entries: Vec<_> = data.properties.into_iter().collect();
                prop_entries.sort_by(|a, b| a.0.cmp(&b.0));
                let properties: Vec<ClassPropertyUsage> = prop_entries
                    .into_iter()
                    .filter(|(_, prop)| prop.count_delta > 0)
                    .map(|(property_sid, prop)| ClassPropertyUsage {
                        property_sid,
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: prop.ref_classes,
                    })
                    .collect();
                ClassStatEntry {
                    class_sid,
                    count: data.count_delta.max(0) as u64,
                    properties,
                }
            })
            .collect();
        if class_entries.is_empty() {
            None
        } else {
            Some(class_entries)
        }
    };

    IndexStats {
        flakes: indexed.flakes,
        size: indexed.size,
        properties,
        classes,
        graphs: indexed.graphs.clone(),
        historical_since_t: indexed.historical_since_t,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Novelty;
    use fluree_db_core::{
        Flake, GraphStatsEntry, PropertyStatEntry, RuntimePredicateId, ValueTypeTag,
    };

    /// Site label for resolver unit tests.
    const TEST_RECONCILE: NoveltyMerge = NoveltyMerge::Reconciled { site: "unit-test" };

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn prop_flake(subject: Sid, property: Sid, value: i64, t: i64) -> Flake {
        Flake::new(
            subject,
            property,
            FlakeValue::Long(value),
            Sid::new(
                fluree_vocab::namespaces::XSD,
                fluree_vocab::xsd_names::INTEGER,
            ),
            t,
            true,
            None,
        )
    }

    fn ref_prop_flake(subject: Sid, property: Sid, target: Sid, t: i64) -> Flake {
        Flake::new(
            subject,
            property,
            FlakeValue::Ref(target),
            Sid::new(fluree_vocab::namespaces::JSON_LD, "@id"),
            t,
            true,
            None,
        )
    }

    /// A base index for `ex:p`: five ref objects and exactly one integer
    /// literal, i.e. the mixed ref/literal shape the ref-only guard exists for.
    fn mixed_property_index(p: &Sid) -> IndexStats {
        let datatypes = vec![
            (ValueTypeTag::INTEGER.as_u8(), 1),
            (ValueTypeTag::JSON_LD_ID.as_u8(), 5),
        ];
        IndexStats {
            flakes: 6,
            size: 60,
            properties: Some(vec![PropertyStatEntry {
                sid: (p.namespace_code, p.name.to_string()),
                count: 6,
                ndv_values: 6,
                ndv_subjects: 6,
                last_modified_t: 1,
                observed_datatypes: PropertyStatEntry::tags_of(&datatypes),
                historical_datatypes: vec![],
                datatypes,
            }]),
            classes: None,
            graphs: None,
            historical_since_t: None,
        }
    }

    /// #1721: novelty is merged into the aggregate datatype breakdown as a blind
    /// ±1 delta log, so a retraction of a fact the base index never held charges
    /// a `-1` against a tag it does not own and can zero it out. The tag set the
    /// ref-only flag reads must survive that: a predicate that carries literals
    /// may not start reading as all-ref because of a delete that removed
    /// nothing.
    #[test]
    fn spurious_retraction_keeps_a_literal_datatype_observed() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let p = sid(10, "p");
        let indexed = mixed_property_index(&p);

        let base_view = fluree_db_core::StatsView::from_db_stats(&indexed);
        assert_eq!(base_view.is_property_ref_only(&p), Some(false), "baseline");

        // ONE retraction of an integer literal the base index never contained:
        // different subject, different value.
        let mut retract = prop_flake(sid(10, "ghost"), p.clone(), 999, 2);
        retract.op = false;
        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(vec![retract], 2, &HashMap::new())
            .expect("apply retraction");

        let merged = assemble_fast_stats(&indexed, &snapshot, &novelty, 2, None);
        let entry = merged
            .properties
            .as_ref()
            .expect("properties")
            .iter()
            .find(|e| e.sid == (10, "p".to_string()))
            .expect("ex:p entry");

        // The counts still drift — they are estimates and the fix does not try
        // to reconcile them — but the observed-tag set does not.
        assert_eq!(
            entry.datatypes,
            vec![(ValueTypeTag::JSON_LD_ID.as_u8(), 5)],
            "the count breakdown is expected to drop the zeroed tag"
        );
        assert_eq!(
            entry.observed_datatypes,
            vec![
                ValueTypeTag::INTEGER.as_u8(),
                ValueTypeTag::JSON_LD_ID.as_u8()
            ]
        );

        let view = fluree_db_core::StatsView::from_db_stats(&merged);
        assert_eq!(
            view.is_property_ref_only(&p),
            Some(false),
            "a spurious retraction made a mixed predicate read as ref-only"
        );
    }

    /// The other direction has to keep working: the flag is monotone under
    /// retraction, not frozen. A novelty assertion that introduces a literal
    /// under a previously all-ref predicate must take the ref-only licence away.
    #[test]
    fn novelty_assertion_can_add_a_literal_datatype() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let p = sid(10, "p");
        let datatypes = vec![(ValueTypeTag::JSON_LD_ID.as_u8(), 5)];
        let indexed = IndexStats {
            flakes: 5,
            size: 50,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "p".to_string()),
                count: 5,
                ndv_values: 5,
                ndv_subjects: 5,
                last_modified_t: 1,
                observed_datatypes: PropertyStatEntry::tags_of(&datatypes),
                historical_datatypes: vec![],
                datatypes,
            }]),
            classes: None,
            graphs: None,
            historical_since_t: None,
        };
        assert_eq!(
            fluree_db_core::StatsView::from_db_stats(&indexed).is_property_ref_only(&p),
            Some(true),
            "an all-ref base index must still license the fold"
        );

        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![prop_flake(sid(10, "s6"), p.clone(), 42, 2)],
                2,
                &HashMap::new(),
            )
            .expect("apply assertion");

        let merged = assemble_fast_stats(&indexed, &snapshot, &novelty, 2, None);
        assert_eq!(
            fluree_db_core::StatsView::from_db_stats(&merged).is_property_ref_only(&p),
            Some(false),
            "a novelty literal must revoke the ref-only licence"
        );
    }

    /// A predicate that novelty introduces outright, with only ref objects,
    /// still qualifies — the fix must not cost the optimization on ledgers with
    /// no published index at all.
    #[test]
    fn novelty_only_ref_predicate_is_ref_only() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let p = sid(10, "knows");
        let indexed = IndexStats::default();

        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![ref_prop_flake(sid(10, "s1"), p.clone(), sid(10, "s2"), 2)],
                2,
                &HashMap::new(),
            )
            .expect("apply assertion");

        let merged = assemble_fast_stats(&indexed, &snapshot, &novelty, 2, None);
        assert_eq!(
            fluree_db_core::StatsView::from_db_stats(&merged).is_property_ref_only(&p),
            Some(true)
        );
    }

    fn type_flake(subject: Sid, class_sid: Sid, t: i64) -> Flake {
        Flake::new(
            subject,
            Sid::new(
                fluree_vocab::namespaces::RDF,
                fluree_vocab::predicates::RDF_TYPE,
            ),
            FlakeValue::Ref(class_sid),
            Sid::new(fluree_vocab::namespaces::JSON_LD, "@id"),
            t,
            true,
            None,
        )
    }

    struct StubLookup {
        p_ids: HashMap<Sid, RuntimePredicateId>,
        classes: HashMap<Sid, Vec<Sid>>,
    }

    #[async_trait]
    impl StatsLookup for StubLookup {
        fn runtime_predicate_id_for_sid(&self, sid: &Sid) -> Option<RuntimePredicateId> {
            self.p_ids.get(sid).copied()
        }

        async fn lookup_subject_classes(
            &self,
            _snapshot: &LedgerSnapshot,
            _overlay: &dyn OverlayProvider,
            _to_t: i64,
            _g_id: GraphId,
            subjects: &[Sid],
        ) -> Result<HashMap<Sid, Vec<Sid>>, StatsAssemblyError> {
            Ok(subjects
                .iter()
                .filter_map(|subject| {
                    self.classes
                        .get(subject)
                        .cloned()
                        .map(|classes| (subject.clone(), classes))
                })
                .collect())
        }
    }

    /// Regression for the segmented-novelty top hazard: fast-stats reads the
    /// POST index as a +1/-1 delta log AND builds an order-dependent
    /// `graph_subject_classes` side table (rdf:type assert/retract per subject)
    /// that it reads mid-pass to attribute properties. The k-way merge over
    /// multiple segments must feed that consumer in the SAME comparator order as
    /// a single segment — so the same flakes split per-commit (many segments) vs
    /// bulk-loaded (one segment) must yield identical stats, including a class
    /// asserted in one segment and retracted in a later one.
    #[test]
    fn fast_stats_match_across_segment_boundaries() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let indexed = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: None,
            graphs: None,
            historical_since_t: None,
        };
        let alice = sid(10, "alice");
        let bob = sid(10, "bob");
        let person = sid(10, "Person");
        let name = sid(10, "name");
        let mut retract_alice_type = type_flake(alice.clone(), person.clone(), 3);
        retract_alice_type.op = false;
        let commits = vec![
            (
                vec![
                    type_flake(alice.clone(), person.clone(), 1),
                    prop_flake(alice.clone(), name.clone(), 1, 1),
                ],
                1i64,
            ),
            (
                vec![
                    type_flake(bob.clone(), person.clone(), 2),
                    prop_flake(bob.clone(), name.clone(), 2, 2),
                ],
                2,
            ),
            (vec![retract_alice_type], 3),
        ];

        // Per-commit application => three segments.
        let mut seg_nov = Novelty::new(0);
        for (flakes, t) in &commits {
            seg_nov
                .apply_commit(flakes.clone(), *t, &HashMap::new())
                .unwrap();
        }
        // Bulk application => one consolidated segment.
        let mut bulk_nov = Novelty::new(0);
        bulk_nov
            .bulk_apply_commits(commits.clone(), &HashMap::new())
            .unwrap();

        let a = assemble_fast_stats(&indexed, &snapshot, &seg_nov, 3, None);
        let b = assemble_fast_stats(&indexed, &snapshot, &bulk_nov, 3, None);

        let class_count = |s: &IndexStats, class: &Sid| {
            s.classes
                .as_ref()
                .and_then(|cs| cs.iter().find(|c| &c.class_sid == class))
                .map_or(0, |c| c.count)
        };
        let prop_count = |s: &IndexStats| {
            s.properties
                .as_ref()
                .and_then(|ps| ps.iter().find(|p| p.sid == (10, "name".to_string())))
                .map_or(0, |p| p.count)
        };

        assert_eq!(a.flakes, b.flakes, "flake delta differs across layouts");
        assert_eq!(a.flakes, 3, "4 asserts - 1 retract");
        assert_eq!(
            class_count(&a, &person),
            class_count(&b, &person),
            "Person count differs across segment layout"
        );
        assert_eq!(
            class_count(&a, &person),
            1,
            "alice un-typed (later segment), bob remains => 1 Person"
        );
        assert_eq!(prop_count(&a), prop_count(&b), "name count differs");
        assert_eq!(prop_count(&a), 2, "two name assertions survive");
    }

    #[test]
    fn fast_stats_updates_graph_datatypes() {
        let indexed = IndexStats {
            flakes: 1,
            size: 10,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "name".to_string()),
                count: 1,
                ndv_values: 0,
                ndv_subjects: 0,
                last_modified_t: 1,
                datatypes: vec![],
                observed_datatypes: vec![],
                historical_datatypes: vec![],
            }]),
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 1,
                size: 10,
                properties: vec![],
                classes: Some(vec![]),
            }]),
            historical_since_t: None,
        };
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut novelty = Novelty::new(1);
        let subject = sid(10, "alice");
        let property = sid(10, "name");
        novelty
            .apply_commit(
                vec![
                    type_flake(subject.clone(), sid(10, "Person"), 2),
                    prop_flake(subject, property.clone(), 42, 2),
                ],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(property, RuntimePredicateId::from_u32(7))]),
            classes: HashMap::new(),
        };

        let stats = assemble_fast_stats(&indexed, &snapshot, &novelty, 2, Some(&lookup));
        let graph_prop = stats
            .graphs
            .as_ref()
            .and_then(|graphs| graphs[0].properties.iter().find(|entry| entry.p_id == 7))
            .expect("graph property stats");
        assert_eq!(graph_prop.count, 1);
        assert_eq!(
            graph_prop.datatypes,
            vec![(ValueTypeTag::INTEGER.as_u8(), 1)]
        );
    }

    #[tokio::test]
    async fn full_stats_recovers_class_property_from_lookup() {
        let person = sid(10, "Person");
        let property = sid(10, "name");
        let subject = sid(10, "alice");
        let indexed = IndexStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid: person.clone(),
                count: 1,
                properties: Vec::new(),
            }]),
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 0,
                size: 0,
                properties: vec![],
                classes: Some(vec![ClassStatEntry {
                    class_sid: person.clone(),
                    count: 1,
                    properties: Vec::new(),
                }]),
            }]),
            historical_since_t: None,
        };
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![prop_flake(subject.clone(), property.clone(), 42, 2)],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(property.clone(), RuntimePredicateId::from_u32(7))]),
            classes: HashMap::from([(subject, vec![person.clone()])]),
        };

        let stats = assemble_full_stats(&indexed, &snapshot, &novelty, &novelty, 2, &lookup)
            .await
            .expect("full stats");
        let class_entry = stats
            .graphs
            .as_ref()
            .and_then(|graphs| graphs[0].classes.as_ref())
            .and_then(|classes| classes.iter().find(|entry| entry.class_sid == person))
            .expect("class entry");
        assert_eq!(class_entry.count, 1);
        assert!(
            class_entry
                .properties
                .iter()
                .any(|usage| usage.property_sid == property),
            "full lookup should recover property attribution from base class membership"
        );
    }

    // -----------------------------------------------------------------------
    // #1391: novelty asserts reconciled against the base index
    // -----------------------------------------------------------------------

    /// A stand-in base index holding a fixed set of currently-asserted facts
    /// **in graph 0**, so the reconciliation fold can be driven without a real
    /// store (and so a probe against any other graph correctly finds nothing).
    struct StubBase {
        facts: Vec<Flake>,
        /// Scans issued — pins that the `(g, s, p)` cache actually caches.
        scans: std::sync::atomic::AtomicUsize,
    }

    impl fluree_db_core::RangeProvider for StubBase {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn range(
            &self,
            query: &fluree_db_core::range_provider::RangeQuery<'_>,
        ) -> std::io::Result<Vec<Flake>> {
            self.scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if query.g_id != 0 {
                return Ok(Vec::new());
            }
            let m = query.match_val;
            Ok(self
                .facts
                .iter()
                .filter(|f| {
                    m.s.as_ref().is_none_or(|s| *s == f.s) && m.p.as_ref().is_none_or(|p| *p == f.p)
                })
                .cloned()
                .collect())
        }
    }

    fn string_flake(s: &Sid, p: &Sid, value: &str, t: i64, op: bool, lang: Option<&str>) -> Flake {
        Flake::new(
            s.clone(),
            p.clone(),
            FlakeValue::String(value.to_string()),
            Sid::new(fluree_vocab::namespaces::XSD, "string"),
            t,
            op,
            lang.map(fluree_db_core::FlakeMeta::with_lang),
        )
    }

    /// Non-empty indexed stats + a stub base index, so `BaseIndexProbe::open`
    /// takes the reconciling path.
    fn reconciling_snapshot(
        base: Vec<Flake>,
    ) -> (LedgerSnapshot, IndexStats, std::sync::Arc<StubBase>) {
        let provider = std::sync::Arc::new(StubBase {
            facts: base,
            scans: std::sync::atomic::AtomicUsize::new(0),
        });
        let snapshot = LedgerSnapshot::genesis("test:main").with_range_provider(
            provider.clone() as std::sync::Arc<dyn fluree_db_core::RangeProvider>
        );
        let indexed = IndexStats {
            historical_since_t: None,
            flakes: 1,
            size: 0,
            properties: Some(Vec::new()),
            classes: None,
            graphs: None,
        };
        (snapshot, indexed, provider)
    }

    /// Deltas the resolver assigns to a POST-ordered novelty walk.
    fn deltas_over(base: Vec<Flake>, novelty_flakes: Vec<Flake>) -> (Vec<i64>, usize) {
        let (snapshot, indexed, provider) = reconciling_snapshot(base);
        let mut novelty = Novelty::new(0);
        // One commit per t so `apply_commit`'s set-semantics dedup sees the
        // same sequence a real ledger would.
        let mut by_t: Vec<(i64, Vec<Flake>)> = Vec::new();
        for f in novelty_flakes {
            match by_t.iter_mut().find(|(t, _)| *t == f.t) {
                Some((_, batch)) => batch.push(f),
                None => by_t.push((f.t, vec![f])),
            }
        }
        by_t.sort_by_key(|(t, _)| *t);
        for (t, batch) in by_t {
            novelty.apply_commit(batch, t, &HashMap::new()).unwrap();
        }
        let mut resolver = NoveltyDeltaResolver::new(&indexed, &snapshot, &novelty, TEST_RECONCILE);
        assert!(
            resolver.is_reconciling(),
            "fixture must take the reconciling path, or the assertions are vacuous"
        );
        let deltas: Vec<i64> = novelty
            .iter_flakes(IndexType::Post)
            .map(|f| resolver.delta_for(f))
            .collect();
        (
            deltas,
            provider.scans.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    #[test]
    fn reconcile_charges_zero_for_a_reassert_already_in_base() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let base = vec![string_flake(&s, &p, "a", 1, true, None)];
        // Identical fact re-asserted after it was indexed: novelty's own
        // set-semantics dedup can't see the base, so the flake is kept.
        let (deltas, _) = deltas_over(base, vec![string_flake(&s, &p, "a", 2, true, None)]);
        assert_eq!(deltas, vec![0], "an idempotent re-assert adds nothing");
    }

    #[test]
    fn reconcile_keeps_a_genuinely_new_assert() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let base = vec![string_flake(&s, &p, "a", 1, true, None)];
        let (deltas, _) = deltas_over(base, vec![string_flake(&s, &p, "b", 2, true, None)]);
        assert_eq!(deltas, vec![1], "a new value on an indexed subject counts");
    }

    #[test]
    fn reconcile_keeps_a_retraction_of_an_indexed_fact() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let base = vec![string_flake(&s, &p, "a", 1, true, None)];
        let (deltas, _) = deltas_over(base, vec![string_flake(&s, &p, "a", 2, false, None)]);
        assert_eq!(deltas, vec![-1], "retracting an indexed fact removes it");
    }

    #[test]
    fn reconcile_charges_zero_for_a_retraction_of_an_absent_fact() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        // The mirror image of the over-count: novelty accepts every
        // retraction, so a DELETE that matched nothing used to drive counts
        // BELOW the truth.
        let (deltas, _) = deltas_over(
            Vec::new(),
            vec![string_flake(&s, &p, "gone", 2, false, None)],
        );
        assert_eq!(
            deltas,
            vec![0],
            "retracting what was never there is a no-op"
        );
    }

    #[test]
    fn reconcile_folds_a_reassert_retract_pair_over_a_base_fact() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let base = vec![string_flake(&s, &p, "a", 1, true, None)];
        // Re-assert (duplicate) at t=2 then retract at t=3. Blind arithmetic
        // nets 0; the truth is -1, because base held the fact all along.
        let (deltas, _) = deltas_over(
            base,
            vec![
                string_flake(&s, &p, "a", 2, true, None),
                string_flake(&s, &p, "a", 3, false, None),
            ],
        );
        assert_eq!(deltas, vec![0, -1], "only the retraction moves the count");
        assert_eq!(deltas.iter().sum::<i64>(), -1);
    }

    #[test]
    fn reconcile_folds_a_retract_reassert_pair_over_a_base_fact() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let base = vec![string_flake(&s, &p, "a", 1, true, None)];
        // Retract then re-assert: the fact is present before and after, so
        // the window nets zero. Only the FIRST flake of the identity is
        // charged against base presence; the re-assert is a true transition.
        let (deltas, _) = deltas_over(
            base,
            vec![
                string_flake(&s, &p, "a", 2, false, None),
                string_flake(&s, &p, "a", 3, true, None),
            ],
        );
        assert_eq!(deltas, vec![-1, 1]);
        assert_eq!(deltas.iter().sum::<i64>(), 0, "net presence unchanged");
    }

    #[test]
    fn reconcile_treats_language_tags_as_distinct_identities() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        // Same (s, p, o, dt); only the tag differs. POST sorts `m` LAST, so
        // these interleave inside one run and the resolver must keep them
        // apart — otherwise the `@fr` assert would be charged as a duplicate.
        let base = vec![string_flake(&s, &p, "a", 1, true, Some("en"))];
        let (deltas, _) = deltas_over(
            base,
            vec![
                string_flake(&s, &p, "a", 2, true, Some("en")),
                string_flake(&s, &p, "a", 2, true, Some("fr")),
            ],
        );
        assert_eq!(
            deltas.iter().sum::<i64>(),
            1,
            "the @en re-assert is a duplicate; the @fr assert is new: {deltas:?}"
        );
    }

    #[test]
    fn reconcile_probes_each_subject_predicate_pair_once() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let base = vec![
            string_flake(&s, &p, "a", 1, true, None),
            string_flake(&s, &p, "b", 1, true, None),
        ];
        // Three objects under one (s, p): POST order interleaves them by
        // object, so without the cache this would be three base scans.
        let (deltas, scans) = deltas_over(
            base,
            vec![
                string_flake(&s, &p, "a", 2, true, None),
                string_flake(&s, &p, "b", 2, true, None),
                string_flake(&s, &p, "c", 2, true, None),
            ],
        );
        assert_eq!(deltas.iter().sum::<i64>(), 1, "only `c` is new: {deltas:?}");
        assert_eq!(scans, 1, "one base scan per (graph, subject, predicate)");
    }

    // -- Non-alternating runs (adversarial review of #1699) ----------------
    //
    // Novelty's set semantics are ONE-SIDED: `Novelty::apply_commit`'s gate is
    // `if flake.op && self.fact_state.is_asserted(..)`, which short-circuits so
    // a retraction is never examined and never dropped. Kept flakes therefore
    // do NOT strictly alternate, and the resolver may not assume they do.

    #[test]
    fn reconcile_ignores_a_repeated_retraction_of_an_absent_fact() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        // The same no-op DELETE run twice. Both retractions are kept (novelty
        // dedups asserts only), so the run is (retract, retract) — the second
        // is not a state transition and must charge zero.
        let (deltas, _) = deltas_over(
            Vec::new(),
            vec![
                string_flake(&s, &p, "gone", 2, false, None),
                string_flake(&s, &p, "gone", 3, false, None),
            ],
        );
        assert_eq!(deltas, vec![0, 0], "neither retraction removes anything");
    }

    #[test]
    fn reconcile_folds_a_same_t_pair_then_reassert() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        // `bulk_apply_commits` (cold load) replays raw persisted flakes, so an
        // assert+retract of one identity at the SAME `t` can both land — see
        // `same_t_assert_retract_keeps_later_reassert` in lib.rs, which builds
        // exactly this on purpose. `fact_state` resolves it retract-wins, so a
        // later re-assert is kept; but `cmp_post` sorts `op` ASCENDING at equal
        // `t`, so the losing assert arrives AFTER the winning retract and the
        // walk sees (retract@1, assert@1, assert@2) — two consecutive asserts.
        let (deltas, _) = deltas_over(
            Vec::new(),
            vec![
                string_flake(&s, &p, "a", 1, true, None),
                string_flake(&s, &p, "a", 1, false, None),
                string_flake(&s, &p, "a", 2, true, None),
            ],
        );
        assert_eq!(
            deltas.iter().sum::<i64>(),
            1,
            "the fact ends up asserted exactly once: {deltas:?}"
        );
    }

    #[test]
    fn reconcile_folds_two_consecutive_same_t_pairs() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        // Two same-`t` assert/retract pairs back to back. The first pair leaves
        // the fact ABSENT (retract wins), so the second pair's assert passes
        // `apply_commit`'s dedup gate and both of its ops land. The walk sees
        // (retract@1, assert@1, retract@2, assert@2) — the second retract
        // repeats the first's op, so a fold that only compared against the
        // previous op would swallow it and then credit the trailing assert.
        // Resolving presence the way `fact_state::record` does gets it right.
        let (deltas, _) = deltas_over(
            Vec::new(),
            vec![
                string_flake(&s, &p, "a", 1, true, None),
                string_flake(&s, &p, "a", 1, false, None),
                string_flake(&s, &p, "a", 2, true, None),
                string_flake(&s, &p, "a", 2, false, None),
            ],
        );
        assert_eq!(
            deltas.iter().sum::<i64>(),
            0,
            "retract wins at t=2, so the fact is absent and was never in base: {deltas:?}"
        );
    }

    #[test]
    fn reconcile_treats_list_positions_as_distinct_identities() {
        let s = sid(10, "alice");
        let p = sid(10, "items");
        // Same (s, p, o, dt) at two `@list` positions. POST sorts `m` last, so
        // these interleave inside one run exactly like language tags do, and
        // `NoveltyFactState`'s FactKey includes `m` — so position 1 must not be
        // charged as a duplicate of the already-indexed position 0.
        let at = |i: i32, t: i64| {
            let mut f = string_flake(&s, &p, "a", t, true, None);
            f.m = Some(fluree_db_core::FlakeMeta {
                i: Some(i),
                ..Default::default()
            });
            f
        };
        let (deltas, _) = deltas_over(vec![at(0, 1)], vec![at(0, 2), at(1, 2)]);
        assert_eq!(
            deltas.iter().sum::<i64>(),
            1,
            "position 0 duplicates the base fact; position 1 is new: {deltas:?}"
        );
    }

    #[test]
    fn reconcile_folds_a_same_t_pair_then_reassert_over_a_base_fact() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        // Same sequence, but base already holds the fact: it is present before
        // and after, so the window nets zero.
        let base = vec![string_flake(&s, &p, "a", 0, true, None)];
        let (deltas, _) = deltas_over(
            base,
            vec![
                string_flake(&s, &p, "a", 1, true, None),
                string_flake(&s, &p, "a", 1, false, None),
                string_flake(&s, &p, "a", 2, true, None),
            ],
        );
        assert_eq!(
            deltas.iter().sum::<i64>(),
            0,
            "present in base, present at the end: {deltas:?}"
        );
    }

    #[test]
    fn reconcile_resolves_the_same_triple_per_graph() {
        // `iter_flakes` walks graph 0's POST stream, then graph 1's — so the
        // SAME (p, o, dt, s) can reappear after its run closed. The run key
        // carries `g_id` for exactly this: graph 1 has its own base state (here,
        // nothing), so its assertion is new even though graph 0's duplicates.
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let (snapshot, indexed, _) =
            reconciling_snapshot(vec![string_flake(&s, &p, "a", 1, true, None)]);
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(
                vec![string_flake(&s, &p, "a", 2, true, None)],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let mut resolver = NoveltyDeltaResolver::new(&indexed, &snapshot, &novelty, TEST_RECONCILE);
        let flake = novelty
            .iter_flakes(IndexType::Post)
            .next()
            .expect("one flake")
            .clone();
        assert_eq!(
            resolver.delta_in_graph(0, &flake),
            0,
            "graph 0 already holds it"
        );
        assert_eq!(
            resolver.delta_in_graph(1, &flake),
            1,
            "graph 1 does not — the run must reopen across the graph boundary"
        );
    }

    #[test]
    fn estimate_lane_keeps_the_blind_delta_log() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let (snapshot, indexed, provider) =
            reconciling_snapshot(vec![string_flake(&s, &p, "a", 1, true, None)]);
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(
                vec![string_flake(&s, &p, "a", 2, true, None)],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let mut resolver =
            NoveltyDeltaResolver::new(&indexed, &snapshot, &novelty, NoveltyMerge::Estimate);
        assert!(!resolver.is_reconciling());
        let deltas: Vec<i64> = novelty
            .iter_flakes(IndexType::Post)
            .map(|f| resolver.delta_for(f))
            .collect();
        assert_eq!(deltas, vec![1], "the planner lane stays blind by design");
        assert_eq!(
            provider.scans.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "and reads no leaflets"
        );
    }

    #[test]
    fn reconciled_assembly_matches_current_state_counts() {
        let s = sid(10, "alice");
        let p = sid(10, "name");
        let (snapshot, _, provider) =
            reconciling_snapshot(vec![string_flake(&s, &p, "a", 1, true, None)]);
        let indexed = IndexStats {
            historical_since_t: None,
            flakes: 1,
            size: 0,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "name".to_string()),
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: 1,
                datatypes: vec![(ValueTypeTag::STRING.as_u8(), 1)],
                observed_datatypes: vec![ValueTypeTag::STRING.as_u8()],
                historical_datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
        };
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(
                vec![string_flake(&s, &p, "a", 2, true, None)],
                2,
                &HashMap::new(),
            )
            .unwrap();

        let name_count = |stats: &IndexStats| {
            stats
                .properties
                .as_ref()
                .and_then(|props| props.iter().find(|e| e.sid == (10, "name".to_string())))
                .map_or(0, |e| e.count)
        };

        let estimate = assemble_fast_stats(&indexed, &snapshot, &novelty, 2, None);
        assert_eq!(name_count(&estimate), 2, "the estimate lane double-counts");
        assert_eq!(estimate.flakes, 2);
        assert_eq!(provider.scans.load(std::sync::atomic::Ordering::Relaxed), 0);

        let reconciled =
            assemble_fast_stats_with(&indexed, &snapshot, &novelty, 2, None, TEST_RECONCILE);
        assert_eq!(name_count(&reconciled), 1, "one fact, asserted twice");
        assert_eq!(reconciled.flakes, 1);
        assert_eq!(
            reconciled.properties.as_ref().unwrap()[0].datatypes,
            vec![(ValueTypeTag::STRING.as_u8(), 1)],
            "the per-datatype breakdown must not double-count either"
        );
    }

    // -- Class attribution on a subject typed inside the window -------------
    //
    // The base-presence probe answers "is this fact in the base index?", but
    // class attribution needs "did the base rollup count this fact under THIS
    // class?". Those diverge exactly when the subject's class membership is new
    // in the window: the fact is base-present, so it charges zero, yet the
    // rollup filed it under the subject's classes AS OF THE INDEX and this one
    // was not among them. The ordinary trigger is a whole-document re-upsert
    // that adds `@type` to documents imported untyped.

    /// Indexed stats for a base holding `alice ex:name "n"` under `classes`.
    fn indexed_with_classes(classes: Option<Vec<ClassStatEntry>>) -> IndexStats {
        IndexStats {
            historical_since_t: None,
            flakes: 1,
            size: 0,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "name".to_string()),
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: 1,
                datatypes: vec![(ValueTypeTag::STRING.as_u8(), 1)],
                observed_datatypes: vec![ValueTypeTag::STRING.as_u8()],
                historical_datatypes: vec![],
            }]),
            classes: classes.clone(),
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 1,
                size: 0,
                properties: vec![],
                classes,
            }]),
        }
    }

    /// One class rolled up with `ex:name` used once by its instances.
    fn class_with_name(class_sid: &Sid, name: &Sid) -> ClassStatEntry {
        ClassStatEntry {
            class_sid: class_sid.clone(),
            count: 1,
            properties: vec![ClassPropertyUsage {
                property_sid: name.clone(),
                datatypes: vec![(ValueTypeTag::STRING.as_u8(), 1)],
                langs: Vec::new(),
                ref_classes: Vec::new(),
            }],
        }
    }

    /// `ex:name`'s per-datatype counts under `class`, on both assembly arms.
    /// Returns `(fast_arm, full_arm)`.
    async fn name_usage_under(
        indexed: &IndexStats,
        snapshot: &LedgerSnapshot,
        novelty: &Novelty,
        lookup: &StubLookup,
        class: &Sid,
        name: &Sid,
        merge: NoveltyMerge,
    ) -> (Vec<(u8, u64)>, Vec<(u8, u64)>) {
        let read = |stats: IndexStats| -> Vec<(u8, u64)> {
            stats
                .graphs
                .as_ref()
                .and_then(|graphs| graphs.iter().find(|g| g.g_id == 0))
                .and_then(|graph| graph.classes.as_ref())
                .and_then(|classes| classes.iter().find(|c| c.class_sid == *class))
                .and_then(|c| c.properties.iter().find(|u| u.property_sid == *name))
                .map(|u| u.datatypes.clone())
                .unwrap_or_default()
        };
        let fast = read(assemble_fast_stats_with(
            indexed,
            snapshot,
            novelty,
            2,
            Some(lookup as &dyn StatsLookup),
            merge,
        ));
        let full = read(
            assemble_full_stats_with(indexed, snapshot, novelty, novelty, 2, lookup, merge)
                .await
                .expect("full stats"),
        );
        (fast, full)
    }

    #[tokio::test]
    async fn reconcile_attributes_a_restatement_under_a_class_gained_in_the_window() {
        let alice = sid(10, "alice");
        let name = sid(10, "name");
        let person = sid(10, "Person");
        // Base holds the name and NO type for alice, so the base rollup has no
        // `Person` at all. Novelty types her and restates the name verbatim.
        let (snapshot, _, _) =
            reconciling_snapshot(vec![string_flake(&alice, &name, "n", 1, true, None)]);
        let indexed = indexed_with_classes(None);
        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![
                    type_flake(alice.clone(), person.clone(), 2),
                    string_flake(&alice, &name, "n", 2, true, None),
                ],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(name.clone(), RuntimePredicateId::from_u32(7))]),
            classes: HashMap::from([(alice.clone(), vec![person.clone()])]),
        };

        let (fast, full) = name_usage_under(
            &indexed,
            &snapshot,
            &novelty,
            &lookup,
            &person,
            &name,
            TEST_RECONCILE,
        )
        .await;
        let one = vec![(ValueTypeTag::STRING.as_u8(), 1)];
        // Charging the restatement zero here drops the only contribution
        // `Person ex:name` has — `apoc.meta.data` skips zero-count datatypes,
        // so the row disappears entirely: #1391's own shape, inverted.
        assert_eq!(fast, one, "fast arm lost the newly typed subject's name");
        assert_eq!(full, one, "full arm lost the newly typed subject's name");
    }

    #[tokio::test]
    async fn reconcile_leaves_a_restatement_alone_when_the_class_set_is_unchanged() {
        let alice = sid(10, "alice");
        let name = sid(10, "name");
        let person = sid(10, "Person");
        // Both facts already indexed and both restated: the rollup already
        // counted the name under `Person`, so the window must add nothing.
        let (snapshot, _, _) = reconciling_snapshot(vec![
            string_flake(&alice, &name, "n", 1, true, None),
            type_flake(alice.clone(), person.clone(), 1),
        ]);
        let indexed = indexed_with_classes(Some(vec![class_with_name(&person, &name)]));
        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![
                    type_flake(alice.clone(), person.clone(), 2),
                    string_flake(&alice, &name, "n", 2, true, None),
                ],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(name.clone(), RuntimePredicateId::from_u32(7))]),
            classes: HashMap::from([(alice.clone(), vec![person.clone()])]),
        };

        let (fast, full) = name_usage_under(
            &indexed,
            &snapshot,
            &novelty,
            &lookup,
            &person,
            &name,
            TEST_RECONCILE,
        )
        .await;
        let one = vec![(ValueTypeTag::STRING.as_u8(), 1)];
        assert_eq!(fast, one, "a restated type is not a gained one");
        assert_eq!(full, one, "a restated type is not a gained one");
    }

    #[tokio::test]
    async fn reconcile_credits_only_the_gained_class_not_the_ones_base_already_filed() {
        let alice = sid(10, "alice");
        let name = sid(10, "name");
        let employee = sid(10, "Employee");
        let person = sid(10, "Person");
        // alice is an indexed `Employee` with an indexed name; the window adds
        // `Person`. The restated name belongs under `Person` (the rollup could
        // not have filed it there) and NOT again under `Employee` (it did).
        let (snapshot, _, _) = reconciling_snapshot(vec![
            string_flake(&alice, &name, "n", 1, true, None),
            type_flake(alice.clone(), employee.clone(), 1),
        ]);
        let indexed = indexed_with_classes(Some(vec![class_with_name(&employee, &name)]));
        let mut novelty = Novelty::new(1);
        novelty
            .apply_commit(
                vec![
                    type_flake(alice.clone(), person.clone(), 2),
                    string_flake(&alice, &name, "n", 2, true, None),
                ],
                2,
                &HashMap::new(),
            )
            .unwrap();
        let lookup = StubLookup {
            p_ids: HashMap::from([(name.clone(), RuntimePredicateId::from_u32(7))]),
            classes: HashMap::from([(alice.clone(), vec![employee.clone(), person.clone()])]),
        };

        let one = vec![(ValueTypeTag::STRING.as_u8(), 1)];
        let (fast_person, full_person) = name_usage_under(
            &indexed,
            &snapshot,
            &novelty,
            &lookup,
            &person,
            &name,
            TEST_RECONCILE,
        )
        .await;
        assert_eq!(fast_person, one, "the gained class needs the restatement");
        assert_eq!(full_person, one, "the gained class needs the restatement");

        let (fast_employee, full_employee) = name_usage_under(
            &indexed,
            &snapshot,
            &novelty,
            &lookup,
            &employee,
            &name,
            TEST_RECONCILE,
        )
        .await;
        assert_eq!(
            fast_employee, one,
            "the base class already counted it — charging it again is the \
             over-count this whole change exists to remove"
        );
        assert_eq!(full_employee, one, "same, on the class-lookup arm");
    }
}
