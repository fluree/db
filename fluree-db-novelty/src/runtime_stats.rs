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

/// Above this many novelty flakes an assembly that asked for
/// [`NoveltyMerge::Reconciled`] declines and falls back to
/// [`NoveltyMerge::Estimate`].
///
/// Reconciliation costs one base-index point lookup per distinct
/// `(graph, subject, predicate)` touched by novelty, so it is bounded work
/// proportional to the novelty window — fine for a per-request introspection
/// answer, not for an unbounded pre-index backlog. Declining keeps the
/// surface responsive; the counts then carry the same over-count this cap
/// exists to bound, which the stamp records.
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
    Reconciled,
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
    let stats = assemble_fast_stats_inner(indexed, snapshot, novelty, to_t, lookup, &mut deltas);
    deltas.finish();
    stats
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
    let mut stats =
        assemble_fast_stats_inner(indexed, snapshot, novelty, to_t, Some(lookup), &mut deltas);
    // Second pass over the same POST stream: restarting run tracking replays
    // the same first-flake-per-identity decisions, and the probe's per-(g,s,p)
    // cache is already warm, so reconciliation costs no extra index reads here.
    deltas.restart_walk();
    let mut touched_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    let mut object_refs_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    let mut subject_props: HashMap<(GraphId, Sid), HashMap<Sid, PropertyDelta>> = HashMap::new();

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
        if delta == 0 {
            // The fact's presence is unchanged from the base index (an
            // idempotent re-assert, or a retraction of something that was
            // never there): it contributes no datatype/lang/ref-target churn.
            continue;
        }
        if is_rdf_type(&flake.p) {
            touched_by_graph
                .entry(g_id)
                .or_default()
                .insert(flake.s.clone());
            if let FlakeValue::Ref(target_class) = &flake.o {
                touched_by_graph
                    .entry(g_id)
                    .or_default()
                    .insert(target_class.clone());
            }
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

    if subject_props.is_empty() {
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

    for ((g_id, subject), props) in subject_props {
        let Some(class_sids) = graph_subject_classes.get(&(g_id, subject.clone())) else {
            continue;
        };
        let graph_entry = get_or_insert_graph_entry(graphs, &mut graph_index, g_id);
        let classes = graph_entry.classes.get_or_insert_with(Vec::new);

        for class_sid in class_sids {
            let class_entry = get_or_insert_class_entry(classes, class_sid);
            for (property_sid, delta) in &props {
                let prop_usage = get_or_insert_class_property(class_entry, property_sid);
                merge_datatypes(&mut prop_usage.datatypes, &delta.datatypes);
                merge_langs(&mut prop_usage.langs, &delta.langs);

                for target in &delta.ref_targets {
                    if let Some(target_classes) = graph_subject_classes.get(&(g_id, target.clone()))
                    {
                        for target_class in target_classes {
                            increment_ref_class(&mut prop_usage.ref_classes, target_class, 1);
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

fn assemble_fast_stats_inner(
    indexed: &IndexStats,
    snapshot: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    lookup: Option<&dyn StatsLookup>,
    deltas: &mut NoveltyDeltaResolver<'_>,
) -> IndexStats {
    if novelty.is_empty() || to_t <= indexed_t(indexed, snapshot) {
        return indexed.clone();
    }

    let mut property_counts = build_property_counts(indexed);
    // Per-predicate (Sid) datatype deltas from novelty, so the aggregate
    // `PropertyStatEntry.datatypes` stays current-state-exact (not index-only).
    // Consumed by the equijoin-filter fold's node-only soundness guard.
    let mut property_datatype_deltas: HashMap<(u16, String), HashMap<u8, i64>> = HashMap::new();
    let mut class_data = build_class_data(indexed);
    let mut graphs = indexed.graphs.clone().unwrap_or_default();
    let mut graph_index: HashMap<GraphId, usize> = graphs
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.g_id, idx))
        .collect();
    let mut flakes_delta: i64 = 0;
    let mut graph_subject_classes: HashMap<(GraphId, Sid), HashSet<Sid>> = HashMap::new();

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
            }
            continue;
        }

        let sid_key = (flake.p.namespace_code, flake.p.name.to_string());
        *property_counts.entry(sid_key.clone()).or_insert(0) += delta;
        *property_datatype_deltas
            .entry(sid_key)
            .or_default()
            .entry(runtime_datatype_tag(flake))
            .or_insert(0) += delta;

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
            for class_sid in class_sids {
                let class = class_data.entry(class_sid.clone()).or_default();
                let prop = class.properties.entry(flake.p.clone()).or_default();
                prop.count_delta += delta;

                let graph_entry = get_or_insert_graph_entry(&mut graphs, &mut graph_index, g_id);
                let classes = graph_entry.classes.get_or_insert_with(Vec::new);
                let class_entry = get_or_insert_class_entry(classes, class_sid);
                let prop_usage = get_or_insert_class_property(class_entry, &flake.p);
                update_class_property_usage(prop_usage, flake, delta, &graph_subject_classes, g_id);
            }
        }
    }

    let mut stats = finalize_stats(
        indexed,
        property_counts,
        property_datatype_deltas,
        class_data,
    );
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
    provider: &'a dyn RangeProvider,
    /// The `t` the `indexed` stats describe — the base state to probe.
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
        if merge != NoveltyMerge::Reconciled {
            return None;
        }
        // Nothing indexed yet ⇒ no base fact can be duplicated; every novelty
        // assertion is genuinely new and the blind delta is already exact.
        if indexed.graphs.is_none() && indexed.properties.is_none() && indexed.classes.is_none() {
            stamp_merge("declined:no_base_index", 0, 0);
            return None;
        }
        let Some(provider) = snapshot.range_provider.as_ref() else {
            stamp_merge("declined:no_range_provider", 0, 0);
            return None;
        };
        if novelty.len() > MAX_RECONCILED_NOVELTY_FLAKES {
            stamp_merge("declined:novelty_too_large", 0, 0);
            return None;
        }
        Some(BaseIndexProbe {
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
    /// Identity comparison uses `Ord`, matching both the index comparators and
    /// `NoveltyFactState`'s key ordering — `FlakeValue`'s `Eq` is looser across
    /// numeric representations, and equating `Long(3)` with `Double(3.0)` here
    /// would suppress a legitimate assertion.
    fn base_contains(&mut self, g_id: GraphId, flake: &Flake) -> bool {
        let key = (g_id, flake.s.clone(), flake.p.clone());
        if !self.cache.contains_key(&key) {
            self.scans += 1;
            let facts = self.scan_base(g_id, &flake.s, &flake.p);
            self.cache.insert(key.clone(), facts);
        }
        let hit = self.cache[&key].iter().any(|(o, dt, m)| {
            o.cmp(&flake.o) == Ordering::Equal && *dt == flake.dt && *m == flake.m
        });
        if hit && flake.op {
            self.duplicates += 1;
        }
        hit
    }

    /// One bounded base scan for `(s, p)`. A provider error (or an absent
    /// index order) yields an empty set, which degrades to today's blind
    /// delta for that pair — never to a suppressed assertion.
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
            Ok(flakes) => flakes
                .into_iter()
                .filter(|f| f.op)
                .map(|f| (f.o, f.dt, f.m))
                .collect(),
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

/// Stamp one reconciliation decision on [`STATS_MERGE_TARGET`].
fn stamp_merge(outcome: &'static str, scans: usize, duplicates: usize) {
    tracing::debug!(
        target: STATS_MERGE_TARGET,
        site = "fast-stats-novelty-merge",
        outcome,
        scans,
        duplicates,
        "fast-stats novelty merge outcome",
    );
}

/// Turns the novelty POST stream into per-flake current-state deltas.
///
/// The blind version charges `+1` per assertion and `-1` per retraction. That
/// is right for every flake *except the first one of each fact identity*: only
/// there does the base index's own state matter. Because novelty applies RDF
/// set semantics within its window, an identity's kept flakes strictly
/// alternate assert/retract, so once the first flake is charged against base
/// presence every later flake is a faithful state transition and needs no
/// probe.
///
/// POST order is `(p, o, dt, s, t, op, m)`, so all flakes sharing
/// `(p, o, dt, s)` are contiguous and `t`-ascending; only `m` varies within a
/// run, and it is tracked with a tiny per-run vec (fan-out is the number of
/// distinct language tags / list positions on one value).
/// Drive this over a novelty walk in `IndexType::Post` order to get each
/// flake's *current-state* delta instead of a blind `±1`.
///
/// Any consumer that folds novelty into indexed counts by hand — the stats
/// assemblers here, `apoc.meta.data`'s per-`(class, property)` rollup — should
/// take its deltas from one of these rather than reading `flake.op` directly,
/// or it inherits #1391.
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
    /// Metas already seen in this run — i.e. identities already opened.
    seen_metas: Vec<Option<FlakeMeta>>,
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
            seen_metas: Vec::new(),
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
    /// The blind version charges `+1` per assertion and `-1` per retraction.
    /// That is right for every flake *except the first one of each fact
    /// identity*: only there does the base index's own state matter. Because
    /// novelty applies RDF set semantics within its window, an identity's kept
    /// flakes strictly alternate assert/retract, so once the first flake is
    /// charged against base presence every later flake is a faithful state
    /// transition and needs no probe.
    ///
    /// POST order puts `m` last, so flakes sharing `(p, o, dt, s)` are
    /// contiguous while distinct `m`s (language tags, list positions) interleave
    /// by `t` within that run — hence the small per-run set of opened metas.
    /// The run key carries `g_id` too: the same triple can live in two named
    /// graphs, and each has its own base-index state to resolve against.
    pub fn delta_in_graph(&mut self, g_id: GraphId, flake: &Flake) -> i64 {
        let op_delta = if flake.op { 1 } else { -1 };
        let Some(probe) = self.probe.as_mut() else {
            return op_delta;
        };
        // Contiguity is the whole basis of the "first flake of an identity"
        // rule: if a run could reopen after closing, an identity's first flake
        // would be charged against base presence more than once and a
        // novelty-local assert/retract pair could net non-zero. Fail loudly in
        // debug rather than silently mis-count if the stream ever stops being
        // non-decreasing within a graph (a segmented-novelty merge regression).
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
            self.seen_metas.clear();
        }
        if self.seen_metas.contains(&flake.m) {
            // Not the first flake of this identity: novelty already told us
            // what the pre-state was, so the transition is exact as written.
            return op_delta;
        }
        self.seen_metas.push(flake.m.clone());
        let base = i64::from(probe.base_contains(g_id, flake));
        i64::from(flake.op) - base
    }

    /// Restart run tracking for a second walk of the same POST stream, keeping
    /// the base-probe cache warm so the second pass reads no extra leaflets.
    pub fn restart_walk(&mut self) {
        self.run = None;
        self.seen_metas.clear();
    }

    /// Stamp what this resolver did. Call once per assembly; a resolver that
    /// declined already stamped its reason at construction.
    pub fn finish(&self) {
        if let Some(probe) = self.probe.as_ref() {
            stamp_merge("reconciled", probe.scans, probe.duplicates);
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
    increment_count(
        &mut prop_entry.datatypes,
        runtime_datatype_tag(flake),
        delta,
    );
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

type PropertyCountMap = HashMap<(u16, String), i64>;

fn build_property_counts(indexed: &IndexStats) -> PropertyCountMap {
    let mut counts = HashMap::new();
    if let Some(ref props) = indexed.properties {
        for entry in props {
            counts.insert(entry.sid.clone(), entry.count as i64);
        }
    }
    counts
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
/// current-state-exact so a predicate that gains/loses literal values via
/// novelty is reflected (used by the equijoin-filter fold's node-only guard).
fn merge_property_datatypes(
    index: &[(u8, u64)],
    deltas: Option<&HashMap<u8, i64>>,
) -> Vec<(u8, u64)> {
    let mut merged: HashMap<u8, i64> = index.iter().map(|&(t, c)| (t, c as i64)).collect();
    if let Some(deltas) = deltas {
        for (&tag, &d) in deltas {
            *merged.entry(tag).or_insert(0) += d;
        }
    }
    let mut out: Vec<(u8, u64)> = merged
        .into_iter()
        .filter(|&(_, c)| c > 0)
        .map(|(t, c)| (t, c as u64))
        .collect();
    out.sort_by_key(|&(t, _)| t);
    out
}

fn finalize_stats(
    indexed: &IndexStats,
    property_counts: PropertyCountMap,
    property_datatype_deltas: HashMap<(u16, String), HashMap<u8, i64>>,
    class_data: HashMap<Sid, ClassDataMut>,
) -> IndexStats {
    let properties = if property_counts.is_empty() {
        indexed.properties.clone()
    } else {
        let indexed_props: HashMap<(u16, String), &PropertyStatEntry> = indexed
            .properties
            .as_ref()
            .map(|props| props.iter().map(|p| (p.sid.clone(), p)).collect())
            .unwrap_or_default();

        let mut entries: Vec<_> = property_counts.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let props: Vec<PropertyStatEntry> = entries
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(sid, count)| {
                let indexed_entry = indexed_props.get(&sid);
                let datatypes = merge_property_datatypes(
                    indexed_entry.map(|e| e.datatypes.as_slice()).unwrap_or(&[]),
                    property_datatype_deltas.get(&sid),
                );
                PropertyStatEntry {
                    sid,
                    count: count.max(0) as u64,
                    ndv_values: indexed_entry.map(|e| e.ndv_values).unwrap_or(0),
                    ndv_subjects: indexed_entry.map(|e| e.ndv_subjects).unwrap_or(0),
                    last_modified_t: indexed_entry.map(|e| e.last_modified_t).unwrap_or(0),
                    datatypes,
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Novelty;
    use fluree_db_core::{
        Flake, GraphStatsEntry, PropertyStatEntry, RuntimePredicateId, ValueTypeTag,
    };

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
            }]),
            classes: None,
            graphs: Some(vec![GraphStatsEntry {
                g_id: 0,
                flakes: 1,
                size: 10,
                properties: vec![],
                classes: Some(vec![]),
            }]),
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
        let mut resolver =
            NoveltyDeltaResolver::new(&indexed, &snapshot, &novelty, NoveltyMerge::Reconciled);
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
        let mut resolver =
            NoveltyDeltaResolver::new(&indexed, &snapshot, &novelty, NoveltyMerge::Reconciled);
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
            flakes: 1,
            size: 0,
            properties: Some(vec![PropertyStatEntry {
                sid: (10, "name".to_string()),
                count: 1,
                ndv_values: 1,
                ndv_subjects: 1,
                last_modified_t: 1,
                datatypes: vec![(ValueTypeTag::STRING.as_u8(), 1)],
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

        let reconciled = assemble_fast_stats_with(
            &indexed,
            &snapshot,
            &novelty,
            2,
            None,
            NoveltyMerge::Reconciled,
        );
        assert_eq!(name_count(&reconciled), 1, "one fact, asserted twice");
        assert_eq!(reconciled.flakes, 1);
        assert_eq!(
            reconciled.properties.as_ref().unwrap()[0].datatypes,
            vec![(ValueTypeTag::STRING.as_u8(), 1)],
            "the per-datatype breakdown must not double-count either"
        );
    }
}
