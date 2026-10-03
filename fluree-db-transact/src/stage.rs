//! Transaction staging
//!
//! This module provides the `stage` function that executes a parsed transaction
//! against a ledger and produces a staged view with the resulting flakes.
//!
//! ## SHACL Validation
//!
//! When the `shacl` feature is enabled, [`validate_view_with_shacl`] validates a
//! staged view against SHACL shapes.

use crate::error::{Result, TransactError};
use crate::generate::{infer_datatype, FlakeAccumulator, FlakeGenerator};
use crate::ir::InlineValues;
use crate::ir::{
    names_ledger, GraphMgmtOp, GraphSel, GraphTarget, TemplateGraph, TemplateTerm, TripleTemplate,
    Txn, TxnType,
};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::graph_registry::{FIRST_USER_GRAPH_ID, TXN_META_GRAPH_ID};
use fluree_db_core::query_bounds::RangeTest;
use fluree_db_core::range::RangeMatch;
use fluree_db_core::tracking::schedule::TXN_BASELINE_MICRO_FUEL;
use fluree_db_core::OverlayProvider;
use fluree_db_core::Tracker;
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, GraphId, Sid};
use fluree_db_ledger::{IndexConfig, LedgerState, StagedLedger};
use fluree_db_policy::{
    is_schema_flake, lookup_subject_classes, PolicyContext, PolicyDecision, PolicyError,
    WriteFlakeInfo, WriteVerb,
};
use fluree_db_query::parse::{lower_unresolved_patterns, UnresolvedPattern};
use fluree_db_query::{
    Batch, Binding, Pattern, QueryPolicyEnforcer, QueryPolicyExecutor, Ref, Term, TriplePattern,
    VarId, VarRegistry,
};
use fluree_db_sparql::ast::{
    QueryBody as SparqlQueryBody, SelectClause, SelectQuery, SolutionModifiers, SparqlAst,
    WhereClause as SparqlWhereClauseAst,
};
use fluree_db_sparql::lower_sparql;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

#[cfg(feature = "shacl")]
use fluree_db_shacl::{ShaclCache, ShaclEngine, ValidationReport};

/// Build a reverse lookup from graph Sid → GraphId.
///
/// Given `graph_sids` (ledger GraphId → Sid), returns the
/// inverse mapping. Used by SHACL/policy to determine which graph a flake
/// belongs to based on its `Flake.g` field.
/// Retract the annotations a transaction's retracts leave dangling, so a link
/// never names a triple that is no longer asserted (annotation-syntax reads
/// rely on it and skip the base edge).
///
/// 1. A retracted triple retracts every link naming it.
/// 2. Retracting all of a reifier's body retracts its links.
/// 3. A reifier left with no link loses its body when it is a blank node, or
///    in LPG mode (`opts.lpgEdgeLifecycle`), where deleting a relationship
///    deletes its properties. An IRI reifier's body otherwise stays, as
///    ordinary RDF about a named resource.
///
/// Links derived from legacy `f:reifies*` bundles read like any other link;
/// retracting one leaves the bundle, whose derived assert the retract cancels
/// in novelty and at the next index build alike.
async fn cascade_attachment_retracts(
    flakes: &[Flake],
    ledger: &LedgerState,
    reverse_graph: &HashMap<Sid, GraphId>,
    new_t: i64,
    lpg_edge_lifecycle: bool,
) -> Result<Vec<Flake>> {
    use fluree_db_core::comparator::IndexType;
    use fluree_db_core::range::{RangeMatch, RangeOptions, RangeTest};
    use fluree_db_core::{is_annotation_predicate, is_rdf_reifies, FlakeValue, TripleTermValue};
    use std::collections::BTreeMap;

    let mut cascade = Vec::new();
    if !ledger.snapshot.has_annotations && !ledger.novelty.has_annotations() {
        return Ok(cascade);
    }

    let span = tracing::debug_span!(
        "cascade_reifies_bundle",
        retract_input_count = flakes.iter().filter(|f| !f.op).count(),
        lpg_edge_lifecycle,
        cascade_count = tracing::field::Empty,
    );
    async {
        let to_t = ledger.t();
        let scan = |g_id: GraphId, index: IndexType, rm: RangeMatch| {
            fluree_db_core::range_with_overlay(
                &ledger.snapshot,
                g_id,
                ledger.novelty.as_ref(),
                index,
                RangeTest::Eq,
                rm,
                RangeOptions::new().with_to_t(to_t),
            )
        };
        let reifies = fluree_db_core::rdf_reifies_sid().clone();
        let retract = |f: &Flake, g: Option<&Sid>| {
            let mut r = f.clone();
            r.g = g.cloned();
            r.t = new_t;
            r.op = false;
            r
        };

        // This transaction's own link ops, by (graph, reifier).
        let mut txn_link_retracts: HashSet<(GraphId, Sid, FlakeValue)> = HashSet::new();
        let mut txn_linked: HashSet<(GraphId, Sid)> = HashSet::new();
        let mut explicit: BTreeMap<(GraphId, Sid), Option<Sid>> = BTreeMap::new();
        for f in flakes.iter().filter(|f| is_rdf_reifies(&f.p)) {
            let g_id = resolve_flake_graph_id(f, reverse_graph)?;
            if f.op {
                txn_linked.insert((g_id, f.s.clone()));
            } else {
                txn_link_retracts.insert((g_id, f.s.clone(), f.o.clone()));
                explicit.insert((g_id, f.s.clone()), f.g.clone());
            }
        }

        // Links this cascade retracts, by (graph, reifier), with the graph
        // they are retracted in.
        let mut unlinked: BTreeMap<(GraphId, Sid), (Option<Sid>, Vec<FlakeValue>)> =
            BTreeMap::new();

        // 1. Retracted triples.
        for flake in flakes {
            if flake.op || is_annotation_predicate(&flake.p) {
                continue;
            }
            // A list element is never reified.
            if flake.m.as_ref().is_some_and(|m| m.i.is_some()) {
                continue;
            }
            let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
            let term = FlakeValue::TripleTerm(Box::new(TripleTermValue {
                s: flake.s.clone(),
                p: flake.p.clone(),
                o: flake.o.clone(),
                dt: flake.dt.clone(),
                lang: flake.m.as_ref().and_then(|m| m.lang.clone()),
            }));
            let links = scan(
                g_id,
                IndexType::Post,
                RangeMatch::new()
                    .with_predicate(reifies.clone())
                    .with_object(term.clone()),
            )
            .await?;
            for link in links {
                if txn_link_retracts.contains(&(g_id, link.s.clone(), term.clone())) {
                    continue;
                }
                cascade.push(retract(&link, flake.g.as_ref()));
                let entry = unlinked
                    .entry((g_id, link.s))
                    .or_insert_with(|| (flake.g.clone(), Vec::new()));
                entry.1.push(term.clone());
            }
        }

        // 2. Reifiers whose whole body this transaction retracts. Same-txn
        //    asserts count toward what survives, so replacing a body value
        //    keeps the reifier.
        type FlakeIdentity = (Sid, FlakeValue, Sid, Option<fluree_db_core::FlakeMeta>);
        let identity =
            |f: &Flake| -> FlakeIdentity { (f.p.clone(), f.o.clone(), f.dt.clone(), f.m.clone()) };
        let mut body_retracts: BTreeMap<(GraphId, Sid), (Option<Sid>, HashSet<FlakeIdentity>)> =
            BTreeMap::new();
        let mut body_asserts: HashSet<(GraphId, Sid)> = HashSet::new();
        for f in flakes.iter().filter(|f| !is_annotation_predicate(&f.p)) {
            let key = (resolve_flake_graph_id(f, reverse_graph)?, f.s.clone());
            if f.op {
                body_asserts.insert(key);
            } else {
                body_retracts
                    .entry(key)
                    .or_insert_with(|| (f.g.clone(), HashSet::new()))
                    .1
                    .insert(identity(f));
            }
        }
        for ((g_id, ann), (g_sid, retracted)) in body_retracts {
            if body_asserts.contains(&(g_id, ann.clone())) {
                continue;
            }
            let current = scan(
                g_id,
                IndexType::Spot,
                RangeMatch::new().with_subject(ann.clone()),
            )
            .await?;
            let (links, body): (Vec<Flake>, Vec<Flake>) = current
                .into_iter()
                .filter(|f| !fluree_db_core::is_reserved_reifies_predicate(&f.p))
                .partition(|f| is_rdf_reifies(&f.p));
            if links.is_empty() || body.iter().any(|f| !retracted.contains(&identity(f))) {
                continue;
            }
            for link in links {
                let already = unlinked
                    .get(&(g_id, ann.clone()))
                    .is_some_and(|(_, terms)| terms.contains(&link.o));
                if already || txn_link_retracts.contains(&(g_id, ann.clone(), link.o.clone())) {
                    continue;
                }
                cascade.push(retract(&link, g_sid.as_ref()));
                unlinked
                    .entry((g_id, ann.clone()))
                    .or_insert_with(|| (g_sid.clone(), Vec::new()))
                    .1
                    .push(link.o);
            }
        }

        // 3. Bodies of reifiers left with no link.
        for (key, g_sid) in explicit {
            unlinked.entry(key).or_insert((g_sid, Vec::new()));
        }
        for ((g_id, ann), (g_sid, terms)) in unlinked {
            let anonymous = ann.namespace_code == fluree_vocab::namespaces::BLANK_NODE;
            if !(anonymous || lpg_edge_lifecycle) || txn_linked.contains(&(g_id, ann.clone())) {
                continue;
            }
            let current = scan(
                g_id,
                IndexType::Spot,
                RangeMatch::new().with_subject(ann.clone()),
            )
            .await?;
            let keeps_link = current.iter().any(|f| {
                is_rdf_reifies(&f.p)
                    && !terms.contains(&f.o)
                    && !txn_link_retracts.contains(&(g_id, ann.clone(), f.o.clone()))
            });
            if keeps_link {
                continue;
            }
            let body: Vec<Flake> = current
                .iter()
                .filter(|f| !is_annotation_predicate(&f.p))
                .map(|f| retract(f, g_sid.as_ref()))
                .collect();
            cascade.extend(body);
        }

        tracing::Span::current().record("cascade_count", cascade.len());
        Ok(cascade)
    }
    .instrument(span)
    .await
}

fn build_reverse_graph_lookup(graph_sids: &HashMap<GraphId, Sid>) -> HashMap<Sid, GraphId> {
    graph_sids
        .iter()
        .map(|(&g_id, sid)| (sid.clone(), g_id))
        .collect()
}

/// Resolve a flake's graph ID from its `Flake.g` field.
///
/// - `None` → default graph (g_id = 0)
/// - `Some(sid)` → looked up in `reverse_graph`; returns error if unknown
fn resolve_flake_graph_id(flake: &Flake, reverse_graph: &HashMap<Sid, GraphId>) -> Result<GraphId> {
    match &flake.g {
        None => Ok(0),
        Some(g_sid) => reverse_graph.get(g_sid).copied().ok_or_else(|| {
            TransactError::FlakeGeneration(format!(
                "staged flake references unknown graph Sid: {g_sid}"
            ))
        }),
    }
}

/// Options for transaction staging
///
/// This struct groups optional configuration parameters for the [`stage`] function,
/// reducing the number of function parameters and making call sites cleaner.
#[derive(Default, Clone)]
pub struct StageOptions<'a> {
    /// Index configuration for backpressure checks.
    /// If provided, staging will fail with `NoveltyAtMax` when novelty is at capacity.
    pub index_config: Option<&'a IndexConfig>,

    /// Policy context for authorization.
    /// If provided (and not root), modify policies will be enforced on staged flakes.
    pub policy_ctx: Option<&'a PolicyContext>,

    /// Tracker for fuel accounting.
    /// If provided, fuel will be consumed for each staged flake.
    pub tracker: Option<&'a Tracker>,

    /// Graph routing map for named-graph flakes.
    ///
    /// Maps `GraphId → Sid` so that `stage_flakes` can resolve each flake's
    /// `Flake.g` to a `GraphId` for per-graph policy enforcement and SHACL validation.
    ///
    /// **Required** when any flake has `g != None`. If `None` is provided and
    /// named-graph flakes are present, `stage_flakes` will return an error.
    ///
    /// The normal `stage()` path builds this internally from `txn.write_graphs`.
    pub graph_sids: Option<&'a HashMap<GraphId, Sid>>,
}

impl<'a> StageOptions<'a> {
    /// Create new stage options with all fields set to None
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index configuration for backpressure checks
    pub fn with_index_config(mut self, config: &'a IndexConfig) -> Self {
        self.index_config = Some(config);
        self
    }

    /// Set the policy context for authorization
    pub fn with_policy(mut self, policy: &'a PolicyContext) -> Self {
        self.policy_ctx = Some(policy);
        self
    }

    /// Set the tracker for fuel accounting
    pub fn with_tracker(mut self, tracker: &'a Tracker) -> Self {
        self.tracker = Some(tracker);
        self
    }

    /// Set the graph routing map for named-graph flakes
    pub fn with_graph_sids(mut self, graph_sids: &'a HashMap<GraphId, Sid>) -> Self {
        self.graph_sids = Some(graph_sids);
        self
    }
}

/// Stage a transaction against a ledger
///
/// This function:
/// 1. Checks backpressure (rejects if novelty at max)
/// 2. Executes WHERE patterns against the ledger to get bindings
/// 3. Generates retractions from DELETE templates with those bindings
/// 4. Generates assertions from INSERT templates with those bindings
/// 5. Applies cancellation (matching assertion/retraction pairs cancel out)
/// 6. Returns a StagedLedger with the staged flakes
///
/// # Arguments
///
/// * `ledger` - The ledger state (consumed by value)
/// * `txn` - The parsed transaction IR
/// * `ns_registry` - Namespace registry for IRI resolution
/// * `options` - Optional configuration for backpressure, policy, and tracking
///
/// # Unbound Variable Behavior
///
/// When a variable in a template is unbound (no matching WHERE result) or poisoned
/// (from an OPTIONAL that didn't match), the flake is **silently skipped**. This
/// follows SPARQL UPDATE semantics where:
///
/// - `DELETE { ?s :name ?name }` with unbound `?name` produces no retractions
/// - `INSERT { ?s :name ?name }` with unbound `?name` produces no assertions
///
/// This is intentional: it allows patterns like "delete all existing values before
/// inserting new ones" to work correctly when there are no existing values.
///
/// If you need to require that all variables are bound, validate the WHERE results
/// before calling stage.
///
/// # Errors
///
/// Returns `TransactError::NoveltyAtMax` if novelty is at the maximum size and
/// reindexing is required before new transactions can be processed.
///
///
/// # Example
///
/// ```ignore
/// let options = StageOptions::new().with_index_config(&config);
/// let view = stage(ledger, txn, ns_registry, options).await?;
/// // Query the view to see staged changes
/// // Or commit the view to persist changes
/// ```
pub async fn stage(
    ledger: LedgerState,
    txn: Txn,
    ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
) -> Result<(StagedLedger, NamespaceRegistry)> {
    let (view, ns_registry, _) = stage_with_graph_delta(ledger, txn, ns_registry, options).await?;
    Ok((view, ns_registry))
}

/// [`stage`], also returning the named graphs the transaction writes, keyed
/// by ledger graph id. Graphs not yet registered carry the id the commit will
/// give them. The map covers the `Txn`'s `write_graphs`, every graph a
/// `GRAPH ?g` template resolved to, and every graph a graph-management
/// operation touches, so per-graph governance and commit registration can
/// use it directly.
pub async fn stage_with_graph_delta(
    ledger: LedgerState,
    mut txn: Txn,
    mut ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
) -> Result<(
    StagedLedger,
    NamespaceRegistry,
    rustc_hash::FxHashMap<u16, String>,
)> {
    // SPARQL graph-management verbs (CLEAR/DROP/COPY/MOVE/ADD) execute by a
    // whole-graph scan + retract/re-home at staging time rather than by the
    // template/WHERE pipeline below. Dispatch before any hot-path setup so the
    // ordinary insert/upsert/update path is byte-identical.
    if txn.graph_mgmt.is_some() {
        return stage_graph_mgmt(ledger, txn, ns_registry, options).await;
    }

    let span = tracing::debug_span!("txn_stage",
        current_t = ledger.t(),
        txn_type = ?txn.txn_type,
        insert_count = txn.insert_templates.len(),
        delete_count = txn.delete_templates.len()
    );
    async move {
        tracing::info!("starting transaction staging");

        // 1. Check backpressure - reject early if novelty is at max
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                tracing::warn!("novelty at max, rejecting transaction");
                return Err(TransactError::NoveltyAtMax);
            }
        }

        // Per-transaction baseline (10 fuel) covering parse, validation,
        // commit log write, and indexing overhead. Per-flake cost (1 micro-fuel)
        // is charged later against the staged flake set.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(TXN_BASELINE_MICRO_FUEL)?;
        }

        let new_t = ledger.t() + 1;
        tracing::debug!(new_t = new_t, "computed new transaction t");

        // Pure-DELETE fast path: no INSERT templates and not an Upsert. Skips
        // assertion generation and the assertion/retraction cancellation hashmap;
        // a sort-and-dedup pass over retractions is sufficient since all
        // retractions share `t` and `op=false`.
        let pure_delete = txn.insert_templates.is_empty() && txn.txn_type != TxnType::Upsert;

        // Project WHERE results down to only template-used vars before materialization.
        // For pure delete, only delete-template vars matter; otherwise both groups.
        let template_vars: Vec<VarId> = if pure_delete {
            collect_template_vars(&[txn.delete_templates.as_slice()])
        } else {
            collect_template_vars(&[
                txn.delete_templates.as_slice(),
                txn.insert_templates.as_slice(),
            ])
        };

        // Transaction ID for blank node skolemization — caller-supplied when
        // created-entity Sids must be reconstructible (Cypher write RETURN),
        // otherwise generated.
        let txn_id = txn
            .opts
            .skolem_txn_id
            .clone()
            .unwrap_or_else(generate_txn_id);

        // A `WITH`/`graph` template default that is this ledger's own address
        // writes the ledger's default graph, the graph the WHERE reads for it.
        template_default_address_to_default_graph(&mut txn, &ledger.snapshot.ledger_id);

        // B2 (data writes): `#txn-meta` is never a write target (see
        // `refuse_txn_meta_write`). `txn.write_graphs` holds the fixed write
        // targets — a `GRAPH <iri>` block, a `WITH <iri>` default, a sync
        // target, a `CREATE GRAPH <iri>`; WHERE-side graph references live on
        // the where clause, so this refuses writes without touching reads.
        // `GRAPH ?g` targets are checked as the WHERE resolves them
        // (`route_var_graphs`).
        for iri in &txn.write_graphs {
            refuse_txn_meta_write(&ledger, iri)?;
        }

        // Graph Sid of each named graph written, by IRI.
        let mut graph_sids: HashMap<String, Sid> = txn
            .write_graphs
            .iter()
            .map(|iri| (iri.clone(), ns_registry.sid_for_iri(iri)))
            .collect();
        let mut reverse_graph = txn_reverse_graph(&ledger, &graph_sids);
        // Fixed write targets, which `GRAPH ?g` targets must not collide with
        // when assigned provisional graph ids mid-stream.
        let fixed_graph_iris: Vec<String> = txn.write_graphs.iter().cloned().collect();

        // Graph-sync target: resolve the g_id + graph Sid now, before the
        // generator takes `ns_registry` mutably. An unregistered target is a
        // first population — nothing to retract (`None` scan). Reserved
        // system graphs are refused the same way CLEAR refuses them. The
        // default graph is g_id 0, whose flakes carry no graph Sid.
        let sync_scan: Option<(GraphId, Option<Sid>)> = match &txn.sync_graph {
            Some(GraphSel::Default) => Some((0, None)),
            Some(GraphSel::Graph(iri)) => {
                // Guard the target by shape, independent of registration:
                // every entry point (builder, consensus applier, HTTP) meets
                // this check, so a malformed IRI can't be registered as a
                // graph and the ledger's own system-graph IRIs are refused
                // even on a ledger whose registry never seeded them.
                fluree_db_core::graph_registry::validate_absolute_graph_iri(iri)
                    .map_err(|msg| TransactError::Parse(format!("sync target: {msg}")))?;
                let ledger_id = ledger.snapshot.ledger_id.as_ref();
                if *iri == fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id)
                    || *iri == fluree_db_core::graph_registry::config_graph_iri(ledger_id)
                {
                    return Err(TransactError::ReservedGraphTarget {
                        graph_iri: iri.clone(),
                    });
                }
                match ledger.snapshot.graph_registry.graph_id_for_iri(iri) {
                    Some(g_id) if g_id < FIRST_USER_GRAPH_ID => {
                        return Err(TransactError::ReservedGraphTarget {
                            graph_iri: iri.clone(),
                        });
                    }
                    Some(g_id) => Some((g_id, Some(ns_registry.sid_for_iri(iri)))),
                    None => None,
                }
            }
            None => None,
        };

        let mut generator = FlakeGenerator::new(new_t, &mut ns_registry, txn_id);

        // Stream the WHERE result into a single accumulator per-batch,
        // projecting / materializing / hydrating in the same step. This keeps
        // peak memory bounded by one batch (plus the accumulator's survivor
        // set) rather than by the total WHERE cardinality.
        let mut acc = if pure_delete {
            FlakeAccumulator::pure_delete(64)
        } else if txn.sync_graph.is_some() || txn.txn_type == TxnType::Upsert {
            // The payload is a set: a fact it states twice must not out-vote
            // the single retraction of the current copy that the sync or
            // upsert wave contributes.
            //
            // Upsert needs this for the same reason sync does. The
            // Turtle-to-JSON-LD adapter states an edge once per reifier
            // attached to it, so `s p o ~ c1 {| … |} ~ c2 {| … |}` asserts the
            // base edge twice while the upsert wave retracts the stored copy
            // once. The surplus assertion survived, and re-upserting a payload
            // byte-for-byte identical to what was already stored committed a
            // delta of one flake every time.
            FlakeAccumulator::mixed_set_assertions(64)
        } else {
            FlakeAccumulator::mixed(64)
        };

        let where_span = tracing::debug_span!(
            "where_exec",
            pattern_count = txn.where_patterns.len(),
            binding_rows = tracing::field::Empty,
            retraction_count = tracing::field::Empty,
            assertion_count = tracing::field::Empty,
        );
        let stream_stats = async {
            let stats = stream_where_into_accumulator(
                &ledger,
                &mut txn,
                &template_vars,
                &mut generator,
                pure_delete,
                &fixed_graph_iris,
                &mut reverse_graph,
                &mut acc,
                options.policy_ctx,
            )
            .await?;
            let span = tracing::Span::current();
            span.record("binding_rows", stats.total_binding_rows);
            span.record("retraction_count", stats.retraction_count as u64);
            span.record("assertion_count", stats.assertion_count as u64);
            Ok::<_, TransactError>(stats)
        }
        .instrument(where_span)
        .await?;

        // Graphs a `GRAPH ?g` template resolved to join the fixed targets.
        let mut resolved_new_graph = false;
        for (iri, sid) in generator.written_graphs() {
            if !graph_sids.contains_key(iri) {
                txn.write_graphs.insert(iri.clone());
                graph_sids.insert(iri.clone(), sid.clone());
                resolved_new_graph = true;
            }
        }
        if resolved_new_graph {
            reverse_graph = txn_reverse_graph(&ledger, &graph_sids);
        }

        // Per SPARQL 1.1 Update §3.1.3: INSERT/DELETE templates are instantiated
        // once per WHERE solution, so a WHERE that matches zero solutions is a
        // no-op. The no-WHERE case (no patterns, no VALUES) is handled inside
        // `stream_where_into_accumulator`: the cursor's SingleEmpty variant emits
        // one empty-schema-empty batch, which generators interpret as a single
        // empty solution, firing all-literal templates exactly once. A present
        // WHERE that yields no rows therefore correctly inserts nothing — we do
        // NOT fall back to a synthetic empty solution here.

        // Upsert second wave: retractions derived from direct ledger lookups
        // (not WHERE). These flakes already carry correct `m` from the
        // underlying asserted flakes, so no hydration is needed.
        if txn.txn_type == TxnType::Upsert {
            tracing::debug!("generating upsert deletions");
            let upsert_retractions =
                generate_upsert_deletions(&ledger, &txn, new_t, &graph_sids).await?;
            tracing::debug!(
                upsert_retraction_count = upsert_retractions.len(),
                "upsert deletions generated"
            );
            acc.push_retractions(upsert_retractions);
        }

        // Graph-sync wave: push every currently-asserted flake of the target
        // graph as a retraction (see [`Txn::sync_graph`]). The accumulator
        // nets retract+assert of the same fact to nothing, so what survives
        // `finalize()` is exactly `current − payload` retractions plus
        // `payload − current` assertions — the delta. Scanned flakes carry
        // correct `m` from storage, so (like the upsert wave) no hydration
        // is needed.
        //
        // Policy model follows CLEAR (roadmap O4): the scan is not
        // view-policy filtered — sync is an authoritative whole-graph
        // replacement, and a view-filtered scan would leave rows the caller
        // cannot see in place, breaking "the graph now equals the payload".
        // Modify-policy is still enforced on the resulting flakes below.
        //
        // Scale note: like CLEAR/COPY/MOVE, this materializes the whole
        // graph's flakes at staging time; backpressure is the pre-check
        // above plus `NoveltyWouldExceed` sizing at commit (which sees only
        // the surviving delta). Chunked staging for whole-graph ops is the
        // same known follow-up flagged on `scan_graph_flakes`.
        if let Some((sync_g_id, sync_graph_sid)) = &sync_scan {
            // The scan attributes every flake to the graph Sid, matching the
            // payload's assertions — both sides must agree on `flake.g` for
            // the accumulator's unchanged-fact cancellation to fire.
            let mut sync_retractions = scan_graph_flakes(
                &ledger,
                *sync_g_id,
                sync_graph_sid.as_ref(),
                options.tracker,
            )
            .await?;
            for f in &mut sync_retractions {
                f.op = false;
                f.t = new_t;
            }
            tracing::debug!(
                graph_id = sync_g_id,
                scanned = sync_retractions.len(),
                "graph-sync retractions generated"
            );
            acc.push_retractions(sync_retractions);
        }

        let retraction_count = stream_stats.retraction_count;
        let assertion_count = stream_stats.assertion_count;
        let total_inputs = acc.input_count();
        let mut flakes = if pure_delete {
            let _span =
                tracing::debug_span!("dedup_retractions", retraction_count = retraction_count)
                    .entered();
            let f = acc.finalize();
            if f.len() as u64 != total_inputs {
                tracing::debug!(
                    before = total_inputs,
                    after = f.len(),
                    cancelled = total_inputs - f.len() as u64,
                    "duplicate retractions collapsed"
                );
            }
            f
        } else {
            let _span = tracing::debug_span!(
                "cancellation",
                retraction_count = retraction_count,
                assertion_count = assertion_count,
            )
            .entered();
            let f = acc.finalize();
            if f.len() as u64 != total_inputs {
                tracing::debug!(
                    before = total_inputs,
                    after = f.len(),
                    cancelled = total_inputs - f.len() as u64,
                    "cancellation applied"
                );
            }
            f
        };

        // Cascade-retract `f:reifies*` bundles for any base edge that
        // is being retracted in this transaction. Without this, a
        // DELETE of the base edge would leave the attachment pointers
        // orphaned in the durable encoding, and `@reifies` queries
        // would still surface annotations for retracted edges.
        //
        // **M2 scan-based path:** the cascade looks up annotations
        // via `range_with_overlay` over the merged snapshot+novelty
        // view. This catches annotations whether they're still in
        // the novelty overlay or have rolled into indexed base
        // storage post-reindex.
        //
        // M1b minimum: retracts the `f:reifies*` bundle only. The
        // anonymous-annotation metadata cascade (RDF default) and the
        // explicit-IRI metadata cascade (LPG mode opt-in) are tracked
        // as follow-ups in the plan.
        let lpg_edge_lifecycle = txn.opts.lpg_edge_lifecycle.unwrap_or(false);
        let cascade = cascade_attachment_retracts(
            &flakes,
            &ledger,
            &reverse_graph,
            new_t,
            lpg_edge_lifecycle,
        )
        .await?;
        if !cascade.is_empty() {
            // Dedup cascade retracts against retracts already in
            // `flakes` (e.g. a same-txn by-id annotation retract that
            // targets the same bundle the cascade is producing) and
            // against itself.
            //
            // `Flake`'s `Eq`/`Hash` ignore `g` (and `t`/`op`), so a
            // plain `HashSet<Flake>` would collapse two retracts
            // targeting the same `(s, p, o, dt, m)` in different
            // named graphs — losing one of them and leaving the
            // other graph's annotation bundle live. Key explicitly
            // on the graph-bearing flake identity tuple instead.
            //
            // Seed from existing retracts ONLY (not asserts):
            // otherwise an in-set assertion of the same fact would
            // suppress a legitimate cascade retract via the
            // `Eq`-ignores-`op` collapse. The reserved-predicate
            // firewall makes this currently impossible for
            // `f:reifies*`, but the gate stays correct either way.
            type CascadeKey = (
                Option<Sid>,
                Sid,
                Sid,
                FlakeValue,
                Sid,
                Option<fluree_db_core::FlakeMeta>,
            );
            let key_of = |f: &Flake| -> CascadeKey {
                (
                    f.g.clone(),
                    f.s.clone(),
                    f.p.clone(),
                    f.o.clone(),
                    f.dt.clone(),
                    f.m.clone(),
                )
            };
            let cascade_in = cascade.len();
            let mut seen: HashSet<CascadeKey> =
                HashSet::with_capacity(flakes.len() / 2 + cascade_in);
            for f in flakes.iter().filter(|f| !f.op) {
                seen.insert(key_of(f));
            }
            let mut deduped: Vec<Flake> = Vec::with_capacity(cascade_in);
            for f in cascade {
                debug_assert!(!f.op, "cascade output must be pure-retract");
                if seen.insert(key_of(&f)) {
                    deduped.push(f);
                }
            }
            let dropped = cascade_in - deduped.len();
            if dropped > 0 {
                tracing::debug!(
                    cascade_in,
                    cascade_out = deduped.len(),
                    duplicates_dropped = dropped,
                    "deduped overlapping cascade retracts"
                );
            }
            if !deduped.is_empty() {
                tracing::debug!(
                    cascade_count = deduped.len(),
                    "cascading f:reifies* retracts for retracted base edges"
                );
                flakes.extend(deduped);
            }
        }

        // Charge 1 micro-fuel per staged flake. Matches query-side scan fuel,
        // which also charges per flake without filtering schema flakes.
        // Fuel exhaustion returns an error so transactions exceeding fuel
        // limits fail before policy enforcement runs.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(flakes.len() as u64)?;
        }

        // Enforce modify policies (if policy context provided and not root)
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                let policy_span = tracing::debug_span!("policy_enforce");
                async {
                    enforce_modify_policies(
                        &flakes,
                        policy,
                        &ledger,
                        options.tracker,
                        &reverse_graph,
                    )
                    .await
                }
                .instrument(policy_span)
                .await?;
            }
        }

        let total_flakes = flakes.len();
        let assertions = flakes.iter().filter(|f| f.op).count();
        let retractions = total_flakes - assertions;

        tracing::info!(
            flake_count = total_flakes,
            assertions = assertions,
            retractions = retractions,
            "transaction staging completed"
        );

        let graph_delta = ledger_graph_delta(&ledger, &txn.write_graphs);
        Ok((
            StagedLedger::new(ledger, flakes, &reverse_graph)?,
            ns_registry,
            graph_delta,
        ))
    }
    .instrument(span)
    .await
}

/// Refuse a write that targets `#txn-meta`.
///
/// `#txn-meta` (g_id 1) holds commit provenance, and `resolve_commit_prefix`
/// / `commit_to_t` resolve a user-typed commit prefix by scanning exactly
/// those indexed `fluree:commit:sha256:<hex>` subjects. They trust what they
/// find, so a forged record sharing a real commit's prefix permanently
/// shadows that commit for `fluree show`, `--at`, `@commit:`, `history` and
/// `branch create --at` — reachable with ordinary write access and persistent
/// through indexing.
///
/// Checked by IRI shape *and* by what the IRI actually routes to: the shape
/// check refuses the ledger's own system-graph IRI even on a ledger whose
/// registry never seeded it, and the registry check refuses any other
/// spelling that resolves to g_id 1.
///
/// `#config` (g_id 2) is DELIBERATELY not covered here.
/// `docs/ledger-config/README.md` and `docs/ledger-config/writing-config.md`
/// document maintaining ledger configuration through an ordinary transaction,
/// so refusing config writes at this site would contradict shipped
/// documentation. The asymmetry is intentional — it is not an oversight to
/// tidy up. Graph management (CLEAR/DROP/COPY/MOVE/ADD) and graph sync refuse
/// BOTH reserved graphs; those paths have their own guards
/// (`stage_graph_mgmt`; `sync_scan` in [`stage_with_graph_delta`]) because
/// they destroy or re-home a whole graph rather than adding facts to one.
fn refuse_txn_meta_write(ledger: &LedgerState, iri: &str) -> Result<()> {
    let ledger_id = ledger.snapshot.ledger_id.as_ref();
    let routes_to_txn_meta = ledger
        .snapshot
        .graph_registry
        .graph_id_for_iri(iri)
        .is_some_and(|g_id| g_id == TXN_META_GRAPH_ID);
    if iri == fluree_db_core::graph_registry::txn_meta_graph_iri(ledger_id) || routes_to_txn_meta {
        return Err(TransactError::ReservedGraphTarget {
            graph_iri: iri.to_string(),
        });
    }
    Ok(())
}

/// SPARQL `WITH <iri>` and a JSON-LD update's top-level `graph` name the
/// update's template default graph ([`Txn::template_default_graph`]). With no
/// `USING`/`from`, the WHERE reads the same IRI as its default graph
/// (`resolve_where_default_graph`), and when the IRI is this ledger's own
/// address ([`names_ledger`]) that is the ledger's default graph. Make the
/// write half agree: the templates that took the default write the default
/// graph, and the IRI is not registered as a named graph unless a template
/// names it itself. Templates that name their graph are left alone.
fn template_default_address_to_default_graph(txn: &mut Txn, ledger_id: &fluree_db_core::LedgerId) {
    let Some(iri) = txn.template_default_graph.clone() else {
        return;
    };
    if !names_ledger(ledger_id, &iri) {
        return;
    }
    let mut named_by_a_template = false;
    for template in txn
        .insert_templates
        .iter_mut()
        .chain(txn.delete_templates.iter_mut())
    {
        if template.graph_from_template_default {
            template.graph = TemplateGraph::Default;
            template.graph_from_template_default = false;
        } else if matches!(&template.graph, TemplateGraph::Iri(g) if **g == *iri) {
            named_by_a_template = true;
        }
    }
    if !named_by_a_template {
        txn.write_graphs.remove(&iri);
    }
}

/// Ledger graph id → IRI for the named graphs `iris`. Unregistered graphs get
/// `GraphRegistry::provisional_ids()`, the id the commit's `apply_delta` will
/// assign.
fn ledger_graph_delta<'a>(
    ledger: &LedgerState,
    iris: impl IntoIterator<Item = &'a String>,
) -> rustc_hash::FxHashMap<u16, String> {
    let iris: Vec<String> = iris.into_iter().cloned().collect();
    let ids = ledger.snapshot.graph_registry.provisional_ids(&iris);
    iris.into_iter()
        .filter_map(|iri| Some((*ids.get(iri.as_str())?, iri)))
        .collect()
}

/// Graph Sid → ledger graph id for the named graphs in `graph_sids`, numbered
/// as in [`ledger_graph_delta`].
fn txn_reverse_graph(
    ledger: &LedgerState,
    graph_sids: &HashMap<String, Sid>,
) -> HashMap<Sid, GraphId> {
    ledger_graph_delta(ledger, graph_sids.keys())
        .into_iter()
        .filter_map(|(g_id, iri)| Some((graph_sids.get(&iri)?.clone(), g_id)))
        .collect()
}

/// Route graphs first reached through a `GRAPH ?g` template during the WHERE
/// stream, so retraction hydration can resolve them, and refuse `#txn-meta`.
///
/// Ids given to new graphs here are provisional for the stream only: the
/// final routing is rebuilt from the full delta once the stream ends, because
/// `provisional_ids` numbers new graphs in sorted-IRI order and a later batch
/// can introduce one that sorts first. New graphs hold no data yet, so the
/// interim ids never select existing rows.
fn route_var_graphs(
    ledger: &LedgerState,
    written_graphs: &HashMap<String, Sid>,
    fixed_graph_iris: &[String],
    reverse_graph: &mut HashMap<Sid, GraphId>,
) -> Result<()> {
    if written_graphs
        .values()
        .all(|sid| reverse_graph.contains_key(sid))
    {
        return Ok(());
    }
    let all_iris: Vec<String> = fixed_graph_iris
        .iter()
        .cloned()
        .chain(written_graphs.keys().cloned())
        .collect();
    let provisional = ledger.snapshot.graph_registry.provisional_ids(&all_iris);
    for (iri, sid) in written_graphs {
        if reverse_graph.contains_key(sid) {
            continue;
        }
        refuse_txn_meta_write(ledger, iri)?;
        let g_id = provisional.get(iri.as_str()).copied().ok_or_else(|| {
            TransactError::FlakeGeneration(format!("no provisional graph id for <{iri}>"))
        })?;
        reverse_graph.insert(sid.clone(), g_id);
    }
    Ok(())
}

/// Content identity of a flake, ignoring its graph, transaction time, and
/// assertion flag. Two flakes with the same identity denote "the same triple"
/// and may be moved between graphs by carrying only a different `g`.
type FlakeContent = (Sid, Sid, FlakeValue, Sid, Option<FlakeMeta>);

fn flake_content(f: &Flake) -> FlakeContent {
    (
        f.s.clone(),
        f.p.clone(),
        f.o.clone(),
        f.dt.clone(),
        f.m.clone(),
    )
}

/// Default for [`whole_graph_scan_limit`]: ~2 GB peak at the accumulator's
/// two-copies-per-fact profile. Any graph that worked before the limit
/// existed still works — whole-graph verbs errored outright on
/// index-resident graphs, and novelty-resident graphs are already bounded
/// well below this by `reindex_max_bytes`.
const DEFAULT_MAX_GRAPH_SCAN_FLAKES: usize = 10_000_000;

/// Memory backstop for whole-graph scans (graph sync, CLEAR, DROP, COPY,
/// MOVE): staging materializes the target graph's currently-asserted
/// flakes, so peak memory scales with the graph, not the delta — an
/// identical resync of a huge graph is the worst case, and no other guard
/// sees it (`NoveltyWouldExceed` measures only the surviving delta, after
/// materialization). `FLUREE_MAX_GRAPH_SCAN_FLAKES` overrides; `0`
/// disables. Read per call — once per graph-management op, never per
/// flake — so tests and embedders can change it at runtime.
fn whole_graph_scan_limit() -> Option<usize> {
    match std::env::var("FLUREE_MAX_GRAPH_SCAN_FLAKES") {
        Ok(v) => match v.trim().parse::<usize>() {
            Ok(0) => None,
            Ok(n) => Some(n),
            Err(_) => Some(DEFAULT_MAX_GRAPH_SCAN_FLAKES),
        },
        Err(_) => Some(DEFAULT_MAX_GRAPH_SCAN_FLAKES),
    }
}

/// Scan every currently-asserted flake in graph `g_id` (merged snapshot +
/// novelty view as of the ledger's current `t`), attributed to `g_sid`.
///
/// Every flake comes back with `g = g_sid` (`None` for the default graph).
/// The range provider materializes index-resident rows with `g: None`
/// regardless of graph — only novelty-resident flakes carry it — and every
/// caller here routes by `flake.g` (`resolve_flake_graph_id`, where `None`
/// is the default graph). Without the stamp, retracting an indexed named
/// graph silently retracted phantoms from the default graph instead.
///
/// Scale note: a whole-graph operation (`CLEAR ALL`, a large COPY/MOVE)
/// materializes every scanned flake into a `Vec` and re-stages it, and
/// backpressure (`at_max_novelty`) is only checked at commit entry — so one
/// graph-management op can roughly double novelty in a single commit. That is
/// exactly the op class most likely to touch the whole store; chunked staging
/// for whole-graph ops is a known follow-up if this cliff is hit in practice.
///
/// O4 (by design): this scan is NOT view-policy filtered — unlike the
/// DELETE-WHERE path, which reads through a `QueryPolicyEnforcer`. So the set a
/// graph-management op (CLEAR/DROP/COPY/MOVE/ADD) acts on is the *modifiable*
/// set, not *viewable ∩ modifiable*. That is deliberate: `CLEAR`/`DROP` are
/// unconditional whole-graph operations per SPARQL 1.1 Update §3.2 (view-
/// filtering them would leave a "cleared" graph non-empty). Modify-policy is
/// still enforced on the resulting flakes (see `stage_graph_mgmt`), so this is
/// not a privilege escalation; it only means that under `default_allow` + a
/// view restriction, a `CLEAR` can retract flakes an equivalent DELETE-WHERE
/// (which only sees viewable rows) would not.
async fn scan_graph_flakes(
    ledger: &LedgerState,
    g_id: GraphId,
    g_sid: Option<&Sid>,
    tracker: Option<&Tracker>,
) -> Result<Vec<Flake>> {
    let db_ref = match tracker {
        Some(t) => ledger.as_graph_db_ref(g_id).with_tracker(t),
        None => ledger.as_graph_db_ref(g_id),
    };
    // `Eq` with an empty match is the whole-graph scan on both range paths:
    // the V3 provider treats "nothing bound" as a full-index cursor and
    // rejects every other `RangeTest`, and the genesis (overlay-only) path
    // matches an empty `Eq` against every flake. `Ge` only ever worked on
    // the genesis path, where non-`Eq` tests pass through unfiltered.
    // `flake_limit` stops the provider's drain loop mid-scan, so the
    // backstop bounds what is materialized, not just what is returned.
    let limit = whole_graph_scan_limit();
    let opts = fluree_db_core::RangeOptions {
        flake_limit: limit.map(|l| l.saturating_add(1)),
        ..Default::default()
    };
    let mut flakes = db_ref
        .range_with_opts(IndexType::Spot, RangeTest::Eq, RangeMatch::new(), opts)
        .await
        .map_err(|e| TransactError::FlakeGeneration(format!("graph scan failed: {e}")))?;
    if let Some(l) = limit {
        if flakes.len() > l {
            return Err(TransactError::WholeGraphScanTooLarge { limit: l });
        }
    }
    // A legacy attachment bundle is inert: its link reads and moves as a link.
    flakes.retain(|f| !fluree_db_core::is_reserved_reifies_predicate(&f.p));
    for f in &mut flakes {
        f.g = g_sid.cloned();
    }
    Ok(flakes)
}

/// Resolve the ledger `GraphId` and graph `Sid` for a named graph IRI, if it
/// is registered (populated) in the ledger. Returns `None` for a graph that
/// does not exist — which, in Fluree's model, is indistinguishable from an
/// empty one (roadmap D-6), so callers treat "no g_id" as "no flakes".
fn resolve_named_graph(
    ledger: &LedgerState,
    ns_registry: &mut NamespaceRegistry,
    iri: &str,
) -> Option<(GraphId, Sid)> {
    ledger
        .snapshot
        .graph_registry
        .graph_id_for_iri(iri)
        .map(|g_id| (g_id, ns_registry.sid_for_iri(iri)))
}

/// Execute a SPARQL graph-management operation (CLEAR/DROP/COPY/MOVE/ADD).
///
/// Produces retraction and/or re-homed assertion flakes by scanning whole
/// graphs at staging time, then runs them through the same policy enforcement
/// and [`StagedLedger`] construction as any other transaction. CLEAR/DROP
/// retract every flake in the target graph(s); COPY/MOVE/ADD scan the source
/// and re-assert its facts into the destination (re-homing by rewriting only
/// the flake's `g`), clearing the destination first for COPY/MOVE and the
/// source afterward for MOVE. Because whole flakes are copied verbatim,
/// datatypes, language tags, and list-index metadata are preserved exactly.
async fn stage_graph_mgmt(
    ledger: LedgerState,
    txn: Txn,
    mut ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
) -> Result<(
    StagedLedger,
    NamespaceRegistry,
    rustc_hash::FxHashMap<u16, String>,
)> {
    let op = txn
        .graph_mgmt
        .as_ref()
        .expect("stage_graph_mgmt called without a graph_mgmt directive");
    let span = tracing::debug_span!("txn_stage_graph_mgmt", ?op);
    async move {
        // Backpressure + per-transaction baseline fuel, mirroring `stage`.
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                return Err(TransactError::NoveltyAtMax);
            }
        }
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(TXN_BASELINE_MICRO_FUEL)?;
        }

        let new_t = ledger.t() + 1;
        let mut flakes: Vec<Flake> = Vec::new();
        // Ledger g_id -> graph Sid, for every named graph our flakes touch;
        // becomes the reverse routing map for novelty application / policy.
        let mut graph_sids: HashMap<GraphId, Sid> = HashMap::new();

        match op {
            GraphMgmtOp::Clear(target) => {
                // Resolve the target to a set of (g_id, Option<graph Sid>) —
                // `None` Sid = the default graph (g_id 0).
                //
                // N3 (documented footgun): CLEAR/DROP DEFAULT and CLEAR/DROP ALL
                // retract the WHOLE default graph, including schema flakes
                // (rdfs:Class, rdfs:subClassOf, …). That is spec-correct — CLEAR
                // is an unconditional whole-graph retraction — but a one-line
                // `CLEAR ALL` strips the ontology, and because `is_schema_flake`
                // exempts schema flakes from modify policy, that retraction is
                // not policy-blockable. `COPY/MOVE <g> TO DEFAULT` reach the
                // same wholesale default-graph retraction through their
                // `clear_dest` pass, so the footgun applies to them equally.
                let mut targets: Vec<(GraphId, Option<Sid>)> = Vec::new();
                match target {
                    GraphTarget::Default => targets.push((0, None)),
                    GraphTarget::Graph(iri) => {
                        if let Some((g_id, sid)) =
                            resolve_named_graph(&ledger, &mut ns_registry, iri)
                        {
                            // B2: reserved system graphs (config = g_id 2,
                            // txn-meta = g_id 1) are Fluree-internal and never a
                            // valid CLEAR/DROP target. Reject by IRI here, the way
                            // the `Named | All` arm below filters them out by g_id.
                            if g_id < FIRST_USER_GRAPH_ID {
                                return Err(TransactError::ReservedGraphTarget {
                                    graph_iri: iri.clone(),
                                });
                            }
                            targets.push((g_id, Some(sid)));
                        }
                        // Nonexistent named graph: nothing to clear (a no-op).
                    }
                    GraphTarget::Named | GraphTarget::All => {
                        if matches!(target, GraphTarget::All) {
                            targets.push((0, None));
                        }
                        // Every *user* named graph (g_id >= 3); the reserved
                        // txn-meta (1) and config (2) graphs are Fluree-internal
                        // and never part of the W3C dataset.
                        let user_graphs: Vec<(GraphId, String)> = ledger
                            .snapshot
                            .graph_registry
                            .iter_entries()
                            .filter(|(g_id, _)| *g_id >= FIRST_USER_GRAPH_ID)
                            .map(|(g_id, iri)| (g_id, iri.to_string()))
                            .collect();
                        for (g_id, iri) in user_graphs {
                            let sid = ns_registry.sid_for_iri(&iri);
                            targets.push((g_id, Some(sid)));
                        }
                    }
                }

                for (g_id, sid) in targets {
                    if let Some(sid) = &sid {
                        graph_sids.insert(g_id, sid.clone());
                    }
                    for mut f in
                        scan_graph_flakes(&ledger, g_id, sid.as_ref(), options.tracker).await?
                    {
                        f.op = false;
                        f.t = new_t;
                        flakes.push(f);
                    }
                }
            }

            GraphMgmtOp::Transfer {
                from,
                to,
                clear_dest,
                clear_src,
                silent,
            } => {
                // B2: a reserved system graph is refused even when `from ==
                // to` — the same-graph no-op below must not read as accepting
                // `#config`/`#txn-meta` as a transfer target. (SILENT
                // deliberately does not suppress the reserved-graph guards,
                // here or below: safety over silence — the reserved graphs are
                // Fluree-internal, not part of the W3C dataset a SILENT verb
                // is scoped to.)
                if from == to {
                    if let GraphSel::Graph(iri) = from {
                        if matches!(
                            resolve_named_graph(&ledger, &mut ns_registry, iri),
                            Some((g_id, _)) if g_id < FIRST_USER_GRAPH_ID
                        ) {
                            return Err(TransactError::ReservedGraphTarget {
                                graph_iri: iri.clone(),
                            });
                        }
                    }
                }
                // `from == to` is a spec no-op for ADD/COPY/MOVE.
                if from != to {
                    // Resolve the source (existing only) and destination.
                    let (src_g_id, src_sid): (Option<GraphId>, Option<Sid>) = match from {
                        GraphSel::Default => (Some(0), None),
                        GraphSel::Graph(iri) => {
                            match resolve_named_graph(&ledger, &mut ns_registry, iri) {
                                // B2: reserved system graphs are never a valid
                                // COPY/MOVE/ADD source.
                                Some((g_id, _)) if g_id < FIRST_USER_GRAPH_ID => {
                                    return Err(TransactError::ReservedGraphTarget {
                                        graph_iri: iri.clone(),
                                    });
                                }
                                Some((g_id, sid)) => (Some(g_id), Some(sid)),
                                // O3: a never-registered (typo'd) source IRI
                                // resolves to `None` here. Per SPARQL 1.1 Update
                                // §3.2, ADD/COPY/MOVE from a nonexistent source
                                // MUST error unless SILENT — otherwise COPY/MOVE
                                // clear the destination (below) and copy nothing
                                // back in, silently emptying it. The additive-only
                                // registry (D-6) keeps this distinguishable from an
                                // emptied-but-registered source, which resolves to
                                // `Some(g_id)` with zero flakes (a legitimate empty
                                // source that proceeds). SILENT opts into the
                                // clear-and-copy-nothing behavior — note that a
                                // SILENT transfer from a missing source therefore
                                // still CLEARS the destination (the spec's own
                                // shortcut equivalence: `DROP SILENT dest;
                                // INSERT ... WHERE source`), it is not a no-op.
                                //
                                // Source-EXISTENCE here deliberately uses REGISTRY
                                // semantics (a graph exists once registered, even
                                // when emptied) — distinct from the query
                                // surface's D-6 flake-carried model, where
                                // `GRAPH ?g` lists only graphs holding ≥1 flake.
                                // Only the registry can tell a typo'd IRI from a
                                // CLEARed graph, which is exactly the distinction
                                // O3 needs.
                                None if !*silent => {
                                    return Err(TransactError::SourceGraphNotFound {
                                        graph_iri: iri.clone(),
                                    });
                                }
                                None => (None, None),
                            }
                        }
                    };
                    // Destination may be brand new — provision its ledger g_id.
                    let (dest_g_id, dest_sid): (GraphId, Option<Sid>) = match to {
                        GraphSel::Default => (0, None),
                        GraphSel::Graph(iri) => {
                            let g_id = ledger
                                .snapshot
                                .graph_registry
                                .provisional_ids(std::slice::from_ref(iri))
                                .get(iri.as_str())
                                .copied()
                                .expect("provisional_ids returns every requested IRI");
                            // B2: reserved system graphs are never a valid
                            // COPY/MOVE/ADD destination.
                            if g_id < FIRST_USER_GRAPH_ID {
                                return Err(TransactError::ReservedGraphTarget {
                                    graph_iri: iri.clone(),
                                });
                            }
                            let sid = ns_registry.sid_for_iri(iri);
                            (g_id, Some(sid))
                        }
                    };
                    if let Some(sid) = &dest_sid {
                        graph_sids.insert(dest_g_id, sid.clone());
                    }

                    let src_flakes = match src_g_id {
                        Some(g) => {
                            scan_graph_flakes(&ledger, g, src_sid.as_ref(), options.tracker).await?
                        }
                        None => Vec::new(),
                    };

                    let dest_flakes =
                        scan_graph_flakes(&ledger, dest_g_id, dest_sid.as_ref(), options.tracker)
                            .await?;

                    let dest_contents: HashSet<FlakeContent> =
                        dest_flakes.iter().map(flake_content).collect();

                    // Build the source assertions, re-homed into the destination
                    // graph.
                    let rehomed: Vec<Flake> = src_flakes
                        .iter()
                        .map(|f| {
                            let mut a = f.clone();
                            a.op = true;
                            a.t = new_t;
                            a.g = dest_sid.clone();
                            a
                        })
                        .collect();

                    // The content set the transfer will land in the destination —
                    // keyed on the RE-HOMED flakes so COPY/MOVE stay symmetric: a
                    // fact common to source and destination (including an anchor
                    // that already names the dest graph) is left in place rather
                    // than retracted-and-re-asserted at the same `new_t`.
                    let rehomed_contents: HashSet<FlakeContent> =
                        rehomed.iter().map(flake_content).collect();

                    // COPY/MOVE: retract destination facts the re-homed source lacks.
                    // (ADD keeps the destination intact.)
                    //
                    // O3: a never-registered (typo'd) source without SILENT already
                    // errored at source resolution above, so reaching here means the
                    // source is either the default graph, a registered graph
                    // (possibly emptied — a legitimate empty source), or a missing
                    // source the user marked SILENT. In every case clearing the
                    // destination against an empty source (retracting it wholesale)
                    // is the intended behavior, so this no longer silently loses data
                    // on a typo.
                    if *clear_dest {
                        for f in &dest_flakes {
                            if !rehomed_contents.contains(&flake_content(f)) {
                                let mut r = f.clone();
                                r.op = false;
                                r.t = new_t;
                                flakes.push(r);
                            }
                        }
                    }

                    // Assert re-homed source facts not already present in the
                    // destination.
                    for a in rehomed {
                        if !dest_contents.contains(&flake_content(&a)) {
                            flakes.push(a);
                        }
                    }

                    // MOVE: clear the source afterward (retract all of it). The
                    // source flakes live in a different graph than the
                    // destination assertions, so there is no cancellation.
                    if *clear_src {
                        if let Some(src_g) = src_g_id {
                            for mut f in src_flakes {
                                if let Some(g_sid) = &f.g {
                                    graph_sids.entry(src_g).or_insert_with(|| g_sid.clone());
                                }
                                f.op = false;
                                f.t = new_t;
                                flakes.push(f);
                            }
                        }
                    }
                }
            }
        }

        // Charge per-flake fuel, mirroring `stage`.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(flakes.len() as u64)?;
        }

        let reverse_graph = build_reverse_graph_lookup(&graph_sids);

        // Policy enforcement (skipped for root), identical to `stage`.
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                enforce_modify_policies(&flakes, policy, &ledger, options.tracker, &reverse_graph)
                    .await?;
            }
        }

        tracing::info!(
            flake_count = flakes.len(),
            retractions = flakes.iter().filter(|f| !f.op).count(),
            "graph-management staging completed"
        );

        // Every named graph the operation writes: the (possibly new)
        // destination, plus each registered graph it clears or moves from.
        let mut graph_delta = ledger_graph_delta(&ledger, &txn.write_graphs);
        for g_id in graph_sids.keys() {
            if let Some(iri) = ledger.snapshot.graph_registry.iri_for_graph_id(*g_id) {
                graph_delta.entry(*g_id).or_insert_with(|| iri.to_string());
            }
        }

        Ok((
            StagedLedger::new(ledger, flakes, &reverse_graph)?,
            ns_registry,
            graph_delta,
        ))
    }
    .instrument(span)
    .await
}

/// Stage pre-built flakes against a ledger (bypass WHERE/template pipeline).
///
/// This is the fast path for bulk INSERT from Turtle where flakes are already
/// constructed by [`FlakeSink`](crate::flake_sink::FlakeSink). No WHERE
/// execution, template materialization, or cancellation is performed.
///
/// # Named Graph Support
///
/// When flakes include named-graph data (`Flake.g = Some(_)`), the caller
/// **must** provide `StageOptions.graph_sids` so that policy enforcement
/// and SHACL validation can resolve each flake's graph. If named-graph flakes
/// are present without a routing map, this function returns an error.
///
/// # Arguments
/// * `ledger` - The ledger state (consumed)
/// * `flakes` - Pre-built assertion flakes
/// * `options` - Optional backpressure / policy / tracking / graph routing configuration
pub async fn stage_flakes(
    ledger: LedgerState,
    flakes: Vec<Flake>,
    options: StageOptions<'_>,
) -> Result<StagedLedger> {
    let span = tracing::debug_span!("stage_flakes", flake_count = flakes.len());
    async move {
        // 1. Backpressure check
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                tracing::warn!("novelty at max, rejecting transaction");
                return Err(TransactError::NoveltyAtMax);
            }
        }

        // Per-transaction baseline (10 fuel) covering parse, validation,
        // commit log write, and indexing overhead. Per-flake cost (1 micro-fuel)
        // is charged below.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(TXN_BASELINE_MICRO_FUEL)?;
        }

        // 2. Build graph routing map.
        //
        // If the caller provided graph_sids (push/import path), use it.
        // Otherwise, verify no named-graph flakes are present — stage_flakes
        // cannot correctly enforce policy/SHACL without a routing map.
        let reverse_graph: HashMap<Sid, GraphId> = match options.graph_sids {
            Some(gs) => build_reverse_graph_lookup(gs),
            None => {
                if flakes.iter().any(|f| f.g.is_some()) {
                    return Err(TransactError::FlakeGeneration(
                        "stage_flakes received named-graph flakes but no graph_sids \
                         routing map was provided in StageOptions"
                            .to_string(),
                    ));
                }
                HashMap::new()
            }
        };

        // 4. Charge 1 micro-fuel per staged flake.
        if let Some(tracker) = options.tracker {
            tracker.consume_fuel(flakes.len() as u64)?;
        }

        // 5. Policy enforcement
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                tracing::debug!("enforcing modify policies on pre-built flakes");
                enforce_modify_policies(&flakes, policy, &ledger, options.tracker, &reverse_graph)
                    .await?;
            }
        }

        tracing::info!(flake_count = flakes.len(), "stage_flakes completed");
        Ok(StagedLedger::new(ledger, flakes, &reverse_graph)?)
    }
    .instrument(span)
    .await
}

async fn hydrate_list_index_meta_for_retractions(
    ledger: &LedgerState,
    retractions: &mut [Flake],
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    use std::collections::BTreeMap;

    // Nothing to copy when neither the indexed base nor novelty holds a
    // single `@list` position. `Some(false)` is an exact observation by the
    // indexer; `None` (legacy root, bulk import) must fall through.
    if ledger.snapshot.has_list_meta == Some(false) && !ledger.novelty.has_list_meta {
        return Ok(());
    }

    // Group candidates by (graph, subject, predicate).
    let mut groups: HashMap<(GraphId, Sid, Sid), Vec<usize>> = HashMap::new();
    for (idx, flake) in retractions.iter().enumerate() {
        // Only retractions lacking a list position are candidates. A
        // language-tagged binding already carries `m = { lang, i: None }`
        // and still needs its position filled in.
        if flake.op || flake.m.as_ref().is_some_and(|m| m.i.is_some()) {
            continue;
        }
        let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
        groups
            .entry((g_id, flake.s.clone(), flake.p.clone()))
            .or_default()
            .push(idx);
    }
    if groups.is_empty() {
        return Ok(());
    }

    let to_t = ledger.t();

    // Novelty side: ONE SPOT walk per touched graph, keeping every op on a
    // requested (subject, predicate) pair. `range_with_overlay` per group
    // would instead translate (or walk) the graph's entire overlay once per
    // group — O(groups × novelty), observed as a multi-minute-to-never
    // filtered DELETE once both are in the tens of thousands.
    let mut wanted: HashMap<GraphId, HashSet<(&Sid, &Sid)>> = HashMap::new();
    for (g_id, s, p) in groups.keys() {
        wanted.entry(*g_id).or_default().insert((s, p));
    }
    let mut overlay_by_key: HashMap<(GraphId, Sid, Sid), Vec<Flake>> = HashMap::new();
    for (g_id, pairs) in &wanted {
        ledger.novelty.for_each_overlay_flake(
            *g_id,
            IndexType::Spot,
            None,
            None,
            true,
            to_t,
            &mut |f| {
                if f.t <= to_t && pairs.contains(&(&f.s, &f.p)) {
                    overlay_by_key
                        .entry((*g_id, f.s.clone(), f.p.clone()))
                        .or_default()
                        .push(f.clone());
                }
            },
        );
    }

    for ((g_id, s, p), members) in groups {
        // Base side: subject + predicate bound against the persisted index
        // only (`NoOverlay`) — a leaf seek, no overlay translation.
        let rm = fluree_db_core::RangeMatch::new()
            .with_subject(s.clone())
            .with_predicate(p.clone());
        let mut found = fluree_db_core::range_with_overlay(
            &ledger.snapshot,
            g_id,
            &fluree_db_core::NoOverlay,
            IndexType::Spot,
            fluree_db_core::RangeTest::Eq,
            rm,
            fluree_db_core::RangeOptions::new().with_to_t(to_t),
        )
        .await?;
        if let Some(ops) = overlay_by_key.remove(&(g_id, s, p)) {
            found.extend(ops);
        }
        // Same lifecycle rule `range_with_overlay` applies to its merged
        // result: newest op per fact key wins, retractions drop out.
        let found = fluree_db_core::range::resolve_current_flakes(found, IndexType::Spot);

        // Index asserted list-carrying metas per object value, in index
        // order. Every matching retraction copies the FIRST dt-compatible
        // meta: identical duplicates then collapse in the accumulator, so a
        // value asserted at N list positions loses exactly one entry per
        // distinct WHERE binding (pinned by the `object-probe-list-retract`
        // case in `it_join_batched_overlay.rs`).
        let mut metas: BTreeMap<FlakeValue, Vec<(Sid, fluree_db_core::FlakeMeta)>> =
            BTreeMap::new();
        for f in found {
            if f.op {
                if let Some(m) = f.m.filter(|m| m.i.is_some()) {
                    metas.entry(f.o).or_default().push((f.dt, m));
                }
            }
        }
        if metas.is_empty() {
            continue;
        }

        for idx in members {
            let flake = &mut retractions[idx];
            let Some(candidates) = metas.get(&flake.o) else {
                continue;
            };
            // Same lexical value under different language tags are distinct
            // facts: the candidate must match the retraction's tag (absent
            // on both for plain literals) as well as its datatype.
            let lang = flake.m.as_ref().and_then(|m| m.lang.as_deref());
            if let Some((_, m)) = candidates
                .iter()
                .find(|(dt, m)| &flake.dt == dt && m.lang.as_deref() == lang)
            {
                flake.m = Some(m.clone());
            }
        }
    }

    Ok(())
}

/// Per-(graph, subject) write-time state computed once per transaction for
/// policy enforcement: the class sets each policy flavor targets against and
/// the subject's lifecycle verb.
struct SubjectWriteState {
    /// Subject's classes in pre-transaction state (legacy `f:modify` class
    /// targeting).
    pre_classes: Vec<Sid>,
    /// pre_classes ∪ classes asserted via rdf:type in this transaction
    /// (write-verb class targeting).
    union_classes: Vec<Sid>,
    /// Lifecycle verb: create / update / delete (see [`WriteVerb`]).
    lifecycle: WriteVerb,
}

/// Per-(graph, subject) deltas gathered from the staged flake batch.
#[derive(Default)]
struct SubjectDelta {
    has_assert: bool,
    has_retract: bool,
    /// Classes asserted via rdf:type in this transaction.
    asserted_classes: Vec<Sid>,
    /// (p, o, dt) of this subject's retractions, for full-removal detection.
    retracts: Vec<(Sid, FlakeValue, Sid)>,
}

/// Enforce modify policies on staged flakes
///
/// This function handles the complete policy enforcement flow:
/// 1. Computes per-subject write state (classes and, when write-verb
///    policies are present, lifecycle classification)
/// 2. Enforces modify policies on each flake with full f:query support
///
/// Returns `Ok(())` if all flakes pass policy, or an error if any flake is
/// denied or a pre-state lookup fails.
async fn enforce_modify_policies(
    flakes: &[Flake],
    policy: &PolicyContext,
    ledger: &LedgerState,
    tracker: Option<&Tracker>,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    let needs_classes = policy.wrapper().has_class_policies();
    let needs_lifecycle = policy.wrapper().has_write_verb_policies();

    let mut states: HashMap<(GraphId, Sid), SubjectWriteState> = HashMap::new();
    if needs_classes || needs_lifecycle {
        // Gather per-subject deltas from the staged batch.
        let mut deltas: HashMap<(GraphId, Sid), SubjectDelta> = HashMap::new();
        for flake in flakes {
            let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
            let d = deltas.entry((g_id, flake.s.clone())).or_default();
            if flake.op {
                d.has_assert = true;
                if fluree_db_core::is_rdf_type(&flake.p) {
                    if let FlakeValue::Ref(c) = &flake.o {
                        if !d.asserted_classes.contains(c) {
                            d.asserted_classes.push(c.clone());
                        }
                    }
                }
            } else {
                d.has_retract = true;
                if needs_lifecycle {
                    d.retracts
                        .push((flake.p.clone(), flake.o.clone(), flake.dt.clone()));
                }
            }
        }

        // Batched pre-state class lookup per graph.
        let mut subjects_by_graph: HashMap<GraphId, Vec<Sid>> = HashMap::new();
        for (g_id, s) in deltas.keys() {
            subjects_by_graph.entry(*g_id).or_default().push(s.clone());
        }
        let mut pre_classes_map: HashMap<(GraphId, Sid), Vec<Sid>> = HashMap::new();
        for (g_id, subjects) in &subjects_by_graph {
            let map = lookup_subject_classes(subjects, ledger.as_graph_db_ref(*g_id))
                .await
                .map_err(|e| {
                    TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                        "Failed to look up subject classes for policy: {e}"
                    )))
                })?;
            for (s, classes) in map {
                pre_classes_map.insert((*g_id, s), classes);
            }
        }

        for ((g_id, s), d) in deltas {
            let pre_classes = pre_classes_map
                .remove(&(g_id, s.clone()))
                .unwrap_or_default();
            let mut union_classes = pre_classes.clone();
            for c in &d.asserted_classes {
                if !union_classes.contains(c) {
                    union_classes.push(c.clone());
                }
            }
            let lifecycle = if needs_lifecycle {
                classify_subject_lifecycle(ledger, g_id, &s, &d, &pre_classes).await?
            } else {
                // Without write-verb policies the lifecycle is never read.
                WriteVerb::Update
            };
            states.insert(
                (g_id, s),
                SubjectWriteState {
                    pre_classes,
                    union_classes,
                    lifecycle,
                },
            );
        }
    }

    // f:queryState f:postState conditions read committed + staged state; the
    // StagedLedger overlay (the same view SHACL validates against) provides
    // it. Built only when such a condition is loaded.
    let staged_view = if policy.wrapper().has_post_state_conditions() {
        let mut view = StagedLedger::new(ledger.clone(), flakes.to_vec(), reverse_graph)?;
        crate::staged_dicts::attach_staged_dicts(&mut view)?;
        Some(view)
    } else {
        None
    };

    // Enforce modify policies with full f:query support
    enforce_modify_policy_per_flake(
        flakes,
        policy,
        ledger,
        tracker,
        reverse_graph,
        &states,
        staged_view.as_ref(),
    )
    .await
}

/// Classify a subject's lifecycle within this transaction:
/// `(exists pre, exists post)` → Create `(no, yes)`, Update `(yes, yes)`,
/// Delete `(yes, no)`.
///
/// Existence probes hit pre-state (snapshot + committed novelty) only when
/// the cheap signals are inconclusive: a subject with pre-state classes
/// exists; a subject with asserts exists post-state. The full pre-state
/// flake scan runs only for retract-only subjects, where full removal must
/// be distinguished from partial retraction.
async fn classify_subject_lifecycle(
    ledger: &LedgerState,
    g_id: GraphId,
    subject: &Sid,
    delta: &SubjectDelta,
    pre_classes: &[Sid],
) -> Result<WriteVerb> {
    // A legacy attachment bundle is never retracted by a transaction, so it
    // would make every full delete of a reifier look partial.
    let scan_pre_state = || async move {
        let rm = fluree_db_core::RangeMatch::new().with_subject(subject.clone());
        let opts = fluree_db_core::RangeOptions::new().with_to_t(ledger.t());
        fluree_db_core::range_with_overlay(
            &ledger.snapshot,
            g_id,
            ledger.novelty.as_ref(),
            fluree_db_core::IndexType::Spot,
            fluree_db_core::RangeTest::Eq,
            rm,
            opts,
        )
        .await
        .map(|mut flakes| {
            flakes.retain(|f| !fluree_db_core::is_reserved_reifies_predicate(&f.p));
            flakes
        })
    };

    if delta.has_assert {
        // Post-state existence is guaranteed by the assert; lifecycle is
        // decided by pre-state existence alone. Results can include
        // retraction flakes, so existence means at least one LIVE assert.
        let pre_exists = !pre_classes.is_empty() || scan_pre_state().await?.iter().any(|f| f.op);
        return Ok(if pre_exists {
            WriteVerb::Update
        } else {
            WriteVerb::Create
        });
    }

    // Retract-only subject: Delete iff every live pre-state flake is
    // retracted by this transaction; otherwise the subject persists (Update).
    let pre_flakes = scan_pre_state().await?;
    if !pre_flakes.iter().any(|f| f.op) {
        // Retracting from a nonexistent subject nets to nothing; classify as
        // Update so no create/delete grant is consumed by a no-op.
        return Ok(WriteVerb::Update);
    }
    let fully_retracted = pre_flakes.iter().filter(|f| f.op).all(|pre| {
        delta
            .retracts
            .iter()
            .any(|(p, o, dt)| p == &pre.p && o == &pre.o && dt == &pre.dt)
    });
    Ok(if fully_retracted {
        WriteVerb::Delete
    } else {
        WriteVerb::Update
    })
}

/// Enforce modify policies on each flake individually
///
/// Returns `Ok(())` if all flakes pass policy, or `Err(PolicyError)` with
/// the policy's f:exMessage if any flake is denied.
///
/// This function supports f:query policies by executing them against
/// the pre-transaction ledger view (db + novelty at current t); conditions
/// declaring `f:queryState f:postState` execute against `staged_view`
/// (committed + this transaction's staged flakes) instead.
#[allow(clippy::too_many_arguments)]
async fn enforce_modify_policy_per_flake(
    flakes: &[Flake],
    policy: &PolicyContext,
    ledger: &LedgerState,
    tracker: Option<&Tracker>,
    reverse_graph: &HashMap<Sid, GraphId>,
    states: &HashMap<(GraphId, Sid), SubjectWriteState>,
    staged_view: Option<&StagedLedger>,
) -> Result<()> {
    // Build per-graph QueryPolicyExecutors so f:query policies execute against
    // the correct graph. Cache executors to avoid rebuilding for every flake.
    let mut executors: HashMap<GraphId, QueryPolicyExecutor<'_>> = HashMap::new();

    // Fuel is now counted upstream in stage() after flake generation,
    // so we only use the tracker here for async policy query calls.
    let async_tracker = tracker.cloned().unwrap_or_else(Tracker::disabled);

    let empty_classes: Vec<Sid> = Vec::new();

    for flake in flakes {
        // Schema flakes always allowed (needed for internal operations)
        if is_schema_flake(&flake.p, &flake.o) {
            continue;
        }

        // Resolve the graph for this flake and get/create a cached executor.
        let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
        let executor = executors.entry(g_id).or_insert_with(|| {
            // Modify policy queries see the state *before* this transaction
            // by default; f:postState conditions read through the staged
            // overlay when one was built.
            let mut ex = QueryPolicyExecutor::with_overlay(
                &ledger.snapshot,
                ledger.novelty.as_ref(),
                ledger.t(),
            )
            .with_graph_id(g_id);
            if let Some(staged) = staged_view {
                ex = ex
                    .with_post_state(staged, staged.staged_t())
                    .with_post_state_snapshot(staged.db());
            }
            ex
        });

        // Per-subject write state (absent when no class/verb policies are
        // loaded — the evaluator then never reads classes or lifecycle).
        let state = states.get(&(g_id, flake.s.clone()));
        let write = WriteFlakeInfo {
            lifecycle: state.map_or(WriteVerb::Update, |st| st.lifecycle),
            op: flake.op,
            pre_classes: state.map_or(&empty_classes, |st| &st.pre_classes),
            union_classes: state.map_or(&empty_classes, |st| &st.union_classes),
            type_object_class: if fluree_db_core::is_rdf_type(&flake.p) {
                match &flake.o {
                    FlakeValue::Ref(c) => Some(c),
                    _ => None,
                }
            } else {
                None
            },
        };

        // Evaluate modify policies with full f:query support using detailed API
        let decision = policy
            .allow_modify_flake_write_async_detailed(
                &flake.s,
                &flake.p,
                &flake.o,
                write,
                executor,
                &async_tracker,
            )
            .await?;

        if let PolicyDecision::Denied { .. } = &decision {
            // Extract error message from the candidate restrictions, or use default
            let message = decision
                .deny_message()
                .unwrap_or("Policy enforcement prevents modification.");
            return Err(PolicyError::modify_denied(message.to_string()).into());
        }
    }
    Ok(())
}

/// Collect the set of variables referenced by INSERT/DELETE templates.
///
/// Used to project the WHERE-result Batch down to only the columns flake
/// generation actually reads, before materialization or any further copies.
fn collect_template_vars(template_groups: &[&[TripleTemplate]]) -> Vec<VarId> {
    let mut seen: HashSet<VarId> = HashSet::new();
    let mut out: Vec<VarId> = Vec::new();
    for group in template_groups {
        for tmpl in *group {
            for term in [&tmpl.subject, &tmpl.predicate, &tmpl.object] {
                term.for_each_leaf(&mut |leaf| {
                    if let TemplateTerm::Var(v) = leaf {
                        if seen.insert(*v) {
                            out.push(*v);
                        }
                    }
                });
            }
            if let TemplateGraph::Var(v) = tmpl.graph {
                if seen.insert(v) {
                    out.push(v);
                }
            }
        }
    }
    out
}

/// Stats collected while streaming the WHERE result into the accumulator.
///
/// - `total_binding_rows` is the sum of row counts across all batches, recorded
///   on the `where_exec` span for tracing.
/// - `retraction_count` / `assertion_count` are the pre-dedup totals pushed
///   into the accumulator (for tracing).
struct WhereStreamStats {
    total_binding_rows: u64,
    retraction_count: usize,
    assertion_count: usize,
}

/// Stream the WHERE result into `acc`, projecting → materializing encoded
/// bindings in place → generating retractions → hydrating list-index meta →
/// pushing into the accumulator, one batch at a time. Assertions are
/// generated and pushed on the same batch when not in pure-delete mode.
///
/// `template_vars` is the union of variables referenced by INSERT/DELETE
/// templates; WHERE-only helper columns are dropped before materialization
/// to keep per-batch memory tied to template width, not WHERE width.
///
/// **Hydration must run before push.** `Flake` identity includes `m`, so a
/// raw retraction with `m = None` would collapse with its peers in the
/// accumulator before hydrate had a chance to fill in the list-index — we'd
/// end up retracting only one of N list entries. Hydrating per batch keeps
/// `m` correct on every retraction before it reaches the dedup layer.
#[allow(clippy::too_many_arguments)]
async fn stream_where_into_accumulator(
    ledger: &LedgerState,
    txn: &mut Txn,
    template_vars: &[VarId],
    generator: &mut FlakeGenerator<'_>,
    pure_delete: bool,
    fixed_graph_iris: &[String],
    reverse_graph: &mut HashMap<Sid, GraphId>,
    acc: &mut FlakeAccumulator,
    view_policy: Option<&PolicyContext>,
) -> Result<WhereStreamStats> {
    // Lower transaction WHERE clause to query patterns.
    //
    // - JSON-LD updates: `txn.where_patterns` is an UnresolvedPattern list, lowered here using the
    //   current ledger snapshot as the IRI encoder.
    // - SPARQL UPDATE (Modify): `txn.sparql_where` is lowered here using the SPARQL lowering
    //   pipeline + the shared query engine, also using the current ledger snapshot as the IRI encoder.
    let mut query_patterns = if let Some(sparql_where) = txn.sparql_where.as_ref() {
        lower_sparql_where_patterns(sparql_where, &ledger.snapshot, &mut txn.vars)?
    } else {
        // Lower UnresolvedPattern to Pattern using the ledger's LedgerSnapshot as the IRI encoder.
        // This also assigns VarIds to any variables referenced in WHERE patterns.
        lower_where_patterns(&txn.where_patterns, &ledger.snapshot, &mut txn.vars)?
    };

    // If VALUES clause present, prepend it as first pattern (seeds the join)
    if let Some(inline_values) = &txn.values {
        let values_pattern = inline_values_to_pattern(inline_values)?;
        query_patterns.insert(0, values_pattern);
    }

    // If no patterns at all (no WHERE, no VALUES), the streaming cursor's
    // SingleEmpty variant emits one empty batch (schema=[], len=0) so the
    // per-batch loop below still fires — `generate_retractions` /
    // `generate_assertions` interpret an empty-schema-empty batch as "single
    // empty solution", letting all-literal templates fire once.

    // Select the default graph(s) for WHERE execution.
    //
    // SPARQL Update semantics:
    // - `USING <g>` clauses scope WHERE evaluation (default graphs). Multiple USING clauses
    //   are evaluated as a merged default graph.
    // - `WITH <g>` scopes WHERE evaluation only when no USING is present
    //
    // JSON-LD Update semantics:
    // - top-level `graph` scopes WHERE evaluation (default graph)
    let desired_where_default_graph_iris: Vec<&str> =
        if let Some(sparql_where) = txn.sparql_where.as_ref() {
            if !sparql_where.using_default_graph_iris.is_empty() {
                sparql_where
                    .using_default_graph_iris
                    .iter()
                    .map(std::string::String::as_str)
                    .collect()
            } else if let Some(with) = sparql_where.with_graph_iri.as_deref() {
                vec![with]
            } else {
                Vec::new()
            }
        } else if let Some(iris) = txn.update_where_default_graph_iris.as_deref() {
            iris.iter().map(std::string::String::as_str).collect()
        } else {
            Vec::new()
        };

    // Resolve IRI -> graph id, preferring the snapshot registry with a binary-store fallback.
    let binary_store: Option<Arc<fluree_db_binary_index::BinaryIndexStore>> =
        ledger.binary_store.as_ref().and_then(|te| {
            Arc::clone(&te.0)
                .downcast::<fluree_db_binary_index::BinaryIndexStore>()
                .ok()
        });
    let resolve_graph_id = |iri: &str| -> Option<GraphId> {
        ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .or_else(|| binary_store.as_ref().and_then(|s| s.graph_id_for_iri(iri)))
    };

    // A WHERE default graph named by `USING`, `WITH` or JSON-LD `from`/`graph`:
    // this ledger's own address names its default graph (see `names_ledger`),
    // a registered IRI names that graph, and anything else names a graph that
    // does not exist here, so `None`.
    let resolve_where_default_graph = |iri: &str| -> Option<GraphId> {
        if names_ledger(&ledger.snapshot.ledger_id, iri) {
            return Some(0);
        }
        resolve_graph_id(iri)
    };
    let where_default_g_ids: Vec<Option<GraphId>> = desired_where_default_graph_iris
        .iter()
        .map(|iri| resolve_where_default_graph(iri))
        .collect();

    // Base GraphDbRef is used to provide snapshot/overlay/time; dataset controls active graphs.
    // A single resolved default graph is the base; otherwise g_id=0 is the base reference.
    let base_db = match where_default_g_ids.as_slice() {
        [Some(g_id)] => ledger.as_graph_db_ref(*g_id),
        _ => ledger.as_graph_db_ref(0),
    };

    // View-policy enforcement for the WHERE read. The transaction WHERE is a
    // read: it must see only the flakes the requesting identity may VIEW, so a
    // conditional match cannot probe data the identity can't read (e.g.
    // `INSERT { ?s :flag 1 } WHERE { ?s :secret ?v }` must bind nothing when
    // `:secret` is hidden). Modify-policy enforcement is separate and runs later
    // on the staged flakes; a modify-only/no-view-rules identity therefore reads
    // exactly what a plain query would (same enforcer, same default_allow). Pure
    // inserts have no WHERE and so are unaffected. Root policies skip filtering.
    let view_enforcer: Option<Arc<QueryPolicyEnforcer>> = view_policy
        .filter(|p| !p.wrapper().is_root())
        .map(|p| Arc::new(QueryPolicyEnforcer::new(Arc::new(p.clone()))));

    let make_graph_ref = |g_id: GraphId| -> fluree_db_query::GraphRef {
        match &view_enforcer {
            Some(enforcer) => fluree_db_query::GraphRef::with_policy(
                base_db.snapshot,
                g_id,
                base_db.overlay,
                base_db.t,
                base_db.snapshot.ledger_id.as_str(),
                Arc::clone(enforcer),
            ),
            None => fluree_db_query::GraphRef::new(
                base_db.snapshot,
                g_id,
                base_db.overlay,
                base_db.t,
                base_db.snapshot.ledger_id.as_str(),
            ),
        }
    };

    let composite_graph_key =
        |iri: &str| -> Arc<str> { format!("{}#{}", base_db.snapshot.ledger_id, iri).into() };

    // SPARQL 1.1 §13.2.1 (via Update §3.1.3): when the operation carries one
    // or more `USING NAMED` clauses but no plain `USING`, the WHERE dataset's
    // default graph is EMPTY — "if there is no FROM clause, but there is one
    // or more FROM NAMED, then the dataset includes an empty graph for the
    // default graph". A `WITH` clause, if given, is likewise ignored for the
    // WHERE clause whenever any USING/USING NAMED is present (§3.1.3). Without
    // this, default-graph selection fell through to the ledger's REAL default
    // graph (g_id 0), so `DELETE { ?s ?p ?o } USING NAMED <h> WHERE
    // { ?s ?p ?o }` matched — and deleted — the entire default graph. An
    // empty default-graph list makes default-scope scans iterate zero members
    // (`ActiveGraphs::Many([])`) and bind nothing. Named-graph visibility is
    // unaffected (built below from the USING NAMED set).
    let where_default_is_empty = txn.sparql_where.as_ref().is_some_and(|w| {
        w.using_default_graph_iris.is_empty() && !w.using_named_graph_iris.is_empty()
    });

    // With no `USING`/`WITH`/`from`, the WHERE reads the ledger's default graph.
    // Otherwise each named graph that exists joins the default-graph union and
    // one that does not contributes nothing (SPARQL 1.1 Update §3.1.3, Query
    // §13.2), so a lone unknown IRI leaves the default graph EMPTY. Falling back
    // to g_id 0 instead made `DELETE { ?s ?p ?o } USING <typo> WHERE { ?s ?p ?o }`
    // delete the ledger's whole default graph.
    let mut runtime_dataset = if where_default_is_empty {
        fluree_db_query::DataSet::new()
    } else if desired_where_default_graph_iris.is_empty() {
        fluree_db_query::DataSet::new().with_default_graph(make_graph_ref(base_db.g_id))
    } else {
        where_default_g_ids
            .iter()
            .flatten()
            .fold(fluree_db_query::DataSet::new(), |ds, &g_id| {
                ds.with_default_graph(make_graph_ref(g_id))
            })
    };

    // Prefer snapshot GraphRegistry, but also include binary-store graph entries as a fallback.
    // This mirrors the query path's safety fallback when registry is temporarily missing entries.
    //
    // Named-graph visibility restrictions:
    // - SPARQL UPDATE `USING NAMED <iri>` restricts WHERE-visible named graphs to that one graph
    // - JSON-LD update `fromNamed` restricts WHERE-visible named graphs to the provided set,
    //   optionally providing dataset-local aliases for `["graph", "<alias>", ...]` patterns.
    let allowed_named_graphs: Option<Vec<(String, Option<String>)>> =
        if let Some(w) = txn.sparql_where.as_ref() {
            // SPARQL UPDATE dataset scoping. A `USING` / `USING NAMED` clause
            // defines the WHERE dataset EXACTLY (SPARQL 1.1 §3.1.3): the
            // WHERE-visible named graphs are precisely the `USING NAMED` set,
            // which is EMPTY when only a plain `USING <g>` is given. So an
            // explicit `GRAPH <g>` block inside the WHERE addresses a named
            // graph that is not in the dataset and matches nothing — "the GRAPH
            // clause does not override the USING clause" (W3C
            // dawg-delete-using-02a/06a; #1441). Selecting all registered named
            // graphs here (the `None` fallback below) is what over-deleted: the
            // `GRAPH <g2>` probe reached g2 despite `USING <g3>`.
            //
            // `None` is returned ONLY when there is no `USING`/`USING NAMED`
            // clause at all, so the ambient graph-store dataset (every
            // registered named graph) applies — the case a plain
            // `DELETE WHERE { GRAPH <g> { .. } }` (no USING) relies on. That
            // no-USING path stays byte-identical to before.
            if w.using_default_graph_iris.is_empty() && w.using_named_graph_iris.is_empty() {
                None
            } else {
                Some(
                    w.using_named_graph_iris
                        .iter()
                        .map(|iri| (iri.clone(), None))
                        .collect(),
                )
            }
        } else {
            txn.update_where_named_graphs
                .as_ref()
                .map(|v| v.iter().map(|g| (g.iri.clone(), g.alias.clone())).collect())
        };

    // Each graph is enumerable by `GRAPH ?g` under exactly one name. Its
    // composite `<ledger_id>#<graph_iri>` key (the syntax used to reference a
    // named graph as a queryable graph source) and any `fromNamed` alias are
    // addressable by `GRAPH <name>` only: were they enumerable, every match
    // would bind `?g` once per name, and a `GRAPH ?g` template would write to
    // a graph named after the alias.
    // (name, g_id, enumerable, canonical IRI), first entry per name wins.
    let mut named: Vec<(Arc<str>, GraphId, bool, Arc<str>)> = Vec::new();
    if let Some(allowlist) = allowed_named_graphs {
        for (iri, alias) in allowlist {
            let Some(g_id) = resolve_graph_id(&iri) else {
                continue;
            };
            // Explicitly listed, so enumerable even when reserved.
            let composite = composite_graph_key(&iri);
            let iri: Arc<str> = iri.into();
            named.push((iri.clone(), g_id, true, iri.clone()));
            named.push((composite, g_id, false, iri.clone()));
            if let Some(alias) = alias {
                named.push((alias.into(), g_id, false, iri));
            }
        }
    } else {
        // The ambient graph store. Reserved system graphs (txn-meta, config)
        // stay addressable by their full IRI — config maintenance reads
        // `GRAPH <…#config>` in an update's WHERE — but, as on the query
        // side, are never enumerated.
        let binary_entries = binary_store
            .as_ref()
            .map(|store| store.graph_entries())
            .unwrap_or_default();
        for (g_id, iri) in ledger
            .snapshot
            .graph_registry
            .iter_entries()
            .chain(binary_entries)
        {
            let iri: Arc<str> = iri.into();
            named.push((iri.clone(), g_id, g_id >= FIRST_USER_GRAPH_ID, iri.clone()));
            named.push((composite_graph_key(&iri), g_id, false, iri));
        }
    }
    let mut seen_named_keys: HashSet<Arc<str>> = HashSet::new();
    let mut graph_aliases: HashMap<Arc<str>, Arc<str>> = HashMap::new();
    for (name, g_id, enumerable, canonical) in named {
        if !seen_named_keys.insert(name.clone()) {
            continue;
        }
        runtime_dataset = if enumerable {
            runtime_dataset.with_named_graph(name, make_graph_ref(g_id))
        } else {
            if name != canonical {
                graph_aliases.insert(name.clone(), canonical);
            }
            runtime_dataset.with_named_graph_alias(name, make_graph_ref(g_id))
        };
    }
    generator.set_graph_aliases(graph_aliases);

    // Open the streaming WHERE cursor. For empty patterns it emits one
    // empty-schema/empty-len batch then EOF, mirroring the eager API's
    // `vec![Batch::empty(...)]` behavior.
    let mut cursor = fluree_db_query::execute_where_streaming(
        base_db,
        &txn.vars,
        &query_patterns,
        Some(&runtime_dataset),
        txn.unmatched_optional,
    )
    .await
    .map_err(TransactError::Query)?;

    let mut total_binding_rows: u64 = 0;
    let mut retraction_count: usize = 0;
    let mut assertion_count: usize = 0;

    while let Some(batch) = cursor.next_batch().await.map_err(TransactError::Query)? {
        // Blank nodes in INSERT templates are fresh per WHERE solution
        // (SPARQL 1.1 Update §3.1.3); give this batch its global solution
        // offset so retractions and assertions of the same row agree.
        generator.set_solution_base(total_binding_rows);
        total_binding_rows += batch.len() as u64;

        // Per-batch shape: project → materialize in place → generate →
        // hydrate (retractions only) → push. Batch drops at end of iter.
        let batch = batch.project_owned(template_vars);
        let batch = materialize_encoded_bindings_for_txn(ledger, batch)?;

        // Per-batch `delete_gen` span. Nested under `where_exec`. Fields:
        // `template_count` (stable per txn), `retraction_count` (per-batch
        // generated count, recorded deferred).
        let delete_span = tracing::debug_span!(
            "delete_gen",
            template_count = txn.delete_templates.len(),
            retraction_count = tracing::field::Empty,
        );
        let retractions = {
            let _g = delete_span.enter();
            let mut r = generator.generate_retractions(&txn.delete_templates, &batch)?;
            route_var_graphs(
                ledger,
                generator.written_graphs(),
                fixed_graph_iris,
                reverse_graph,
            )?;

            // Hydrate BEFORE push. `Flake::eq` includes `m`, so raw retractions
            // with `m = None` must have their list-index filled in from the
            // asserted flake before they reach the accumulator — otherwise
            // N list entries with the same `(s,p,o,dt)` would collapse to one
            // retraction survivor and only one list entry would actually be
            // retracted from the index.
            hydrate_list_index_meta_for_retractions(ledger, &mut r, reverse_graph).await?;

            delete_span.record("retraction_count", r.len() as u64);
            r
        };
        retraction_count += retractions.len();
        acc.push_retractions(retractions);

        if !pure_delete {
            // Per-batch `insert_gen` span. Nested under `where_exec`.
            let insert_span = tracing::debug_span!(
                "insert_gen",
                template_count = txn.insert_templates.len(),
                assertion_count = tracing::field::Empty,
            );
            let assertions = {
                let _g = insert_span.enter();
                let a = generator.generate_assertions(&txn.insert_templates, &batch)?;
                route_var_graphs(
                    ledger,
                    generator.written_graphs(),
                    fixed_graph_iris,
                    reverse_graph,
                )?;
                insert_span.record("assertion_count", a.len() as u64);
                a
            };
            assertion_count += assertions.len();
            acc.push_assertions(assertions);
        }
    }
    cursor.close();

    Ok(WhereStreamStats {
        total_binding_rows,
        retraction_count,
        assertion_count,
    })
}

/// Lower a stored SPARQL WHERE clause (from SPARQL UPDATE) into query patterns.
///
/// This constructs a synthetic `SELECT * WHERE { ... }` query so we can reuse the
/// existing SPARQL lowering pipeline (which already supports subqueries + aggregates)
/// and keep one execution path in `fluree-db-query`.
fn lower_sparql_where_patterns(
    sparql_where: &crate::ir::SparqlWhereClause,
    encoder: &fluree_db_core::LedgerSnapshot,
    vars: &mut VarRegistry,
) -> Result<Vec<Pattern>> {
    // Propagate the original parsed span so lowering errors report helpful locations.
    let span = sparql_where.pattern.span();
    let where_clause = SparqlWhereClauseAst::new(sparql_where.pattern.clone(), true, span);
    let select = SelectClause::star(span);
    let modifiers = SolutionModifiers::new();
    let select_query = SelectQuery::new(select, where_clause, modifiers, span);
    let ast = SparqlAst::new(
        sparql_where.prologue.clone(),
        SparqlQueryBody::Select(select_query),
        span,
    );

    lower_sparql(&ast, encoder, vars)
        .map(|pq| pq.patterns)
        .map_err(Into::into)
}

/// Materialize any late-materialized (`Binding::Encoded*`) values in a WHERE-result batch.
///
/// Transaction flake generation (`FlakeGenerator`) expects concrete `Binding::Sid` and
/// `Binding::Lit` values, and will error on encoded bindings.
///
/// This consumes `batch` by value and rewrites encoded bindings in place. Two
/// short-circuits keep the steady state cheap:
///
/// - If no binary store is configured, encoded bindings cannot appear and the
///   batch is returned as-is.
/// - Per column: a one-pass scan checks whether the column contains any
///   `Encoded*` variant. Already-concrete columns are left untouched (no
///   per-binding clone, no Vec reallocation). Only columns that need it pay
///   for in-place rewriting.
fn materialize_encoded_bindings_for_txn(ledger: &LedgerState, batch: Batch) -> Result<Batch> {
    if batch.is_empty() {
        return Ok(batch);
    }

    // If no binary store is present, encoded bindings should not appear.
    let Some(te) = &ledger.binary_store else {
        return Ok(batch);
    };
    let Ok(store) = Arc::clone(&te.0).downcast::<fluree_db_binary_index::BinaryIndexStore>() else {
        return Ok(batch);
    };

    let gv = fluree_db_binary_index::BinaryGraphView::new(Arc::clone(&store), 0);

    let (schema, mut columns, len) = batch.into_parts();

    for col in &mut columns {
        if !column_needs_materialization(col) {
            continue;
        }
        for b in col.iter_mut() {
            materialize_one_binding(b, ledger, &gv)?;
        }
    }

    // Use `from_parts` (not `Batch::new`) so the row count survives when
    // `columns` is empty — e.g. an `empty_schema_with_len(N)` batch produced
    // by `project_owned` against an all-literal template set must keep `N`
    // so flake generation fires once per WHERE solution row, not once total.
    Batch::from_parts(schema, columns, len).map_err(|e| TransactError::Query(e.into()))
}

/// True if any binding in `col` is an `Encoded*` variant requiring rewrite.
/// Tight loop with no allocation; `Encoded*` columns return early on first hit.
fn column_needs_materialization(col: &[Binding]) -> bool {
    col.iter().any(|b| {
        matches!(
            b,
            Binding::EncodedSid { .. } | Binding::EncodedPid { .. } | Binding::EncodedLit { .. }
        )
    })
}

/// Rewrite a single `Binding` in place if it is an `Encoded*` variant.
/// Already-concrete bindings are left untouched (no clone).
fn materialize_one_binding(
    b: &mut Binding,
    ledger: &LedgerState,
    gv: &fluree_db_binary_index::BinaryGraphView,
) -> Result<()> {
    let store_ref = gv.store();
    match b {
        Binding::EncodedSid { s_id, .. } => {
            let iri = store_ref.resolve_subject_iri(*s_id).map_err(|e| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "resolve_subject_iri: {e}"
                )))
            })?;
            let sid = ledger.snapshot.encode_iri(&iri).ok_or_else(|| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "encode_iri returned None for subject IRI: {iri}"
                )))
            })?;
            *b = Binding::sid(sid);
        }
        Binding::EncodedPid { p_id } => {
            let iri = store_ref.resolve_predicate_iri(*p_id).ok_or_else(|| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "unknown predicate id: {p_id}"
                )))
            })?;
            let sid = ledger.snapshot.encode_iri(iri).ok_or_else(|| {
                TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                    "encode_iri returned None for predicate IRI: {iri}"
                )))
            })?;
            *b = Binding::sid(sid);
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            i_val,
            t,
        } => {
            let (o_kind, o_key, p_id, dt_id, lang_id, i_val, t) =
                (*o_kind, *o_key, *p_id, *dt_id, *lang_id, *i_val, *t);
            let val = gv
                .decode_value_from_kind(o_kind, o_key, p_id, dt_id, lang_id)
                .map_err(|e| {
                    TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                        "decode_value_from_kind: {e}"
                    )))
                })?;
            match val {
                FlakeValue::Ref(sid) => {
                    *b = Binding::sid(sid);
                }
                other => {
                    let dt_sid = store_ref
                        .dt_sids()
                        .get(dt_id as usize)
                        .cloned()
                        .unwrap_or_else(|| Sid::new(0, ""));
                    let dt_iri = store_ref.sid_to_iri(&dt_sid).ok_or_else(|| {
                        TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                            "sid_to_iri failed: unknown namespace code {} for datatype {:?}",
                            dt_sid.namespace_code, dt_sid.name
                        )))
                    })?;
                    let dt = ledger.snapshot.encode_iri(&dt_iri).ok_or_else(|| {
                        TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                            "encode_iri returned None for datatype IRI: {dt_iri}"
                        )))
                    })?;
                    let meta = store_ref.decode_meta(lang_id, i_val);
                    let dtc = meta
                        .as_ref()
                        .and_then(|m| m.lang.as_ref())
                        .map(|s| {
                            fluree_db_core::DatatypeConstraint::LangTag(std::sync::Arc::from(
                                s.as_str(),
                            ))
                        })
                        .unwrap_or_else(|| fluree_db_core::DatatypeConstraint::Explicit(dt));
                    *b = Binding::Lit {
                        val: other,
                        dtc,
                        t: Some(t),
                        op: None,
                        p_id: Some(p_id),
                    };
                }
            }
        }
        // Already-concrete bindings need no rewrite.
        _ => {}
    }
    Ok(())
}

/// Lower UnresolvedPattern list to Pattern list
///
/// This converts string IRIs to encoded Sids using the database, and assigns
/// VarIds to variables using the provided VarRegistry (shared with INSERT/DELETE).
fn lower_where_patterns(
    patterns: &[UnresolvedPattern],
    db: &fluree_db_core::LedgerSnapshot,
    vars: &mut VarRegistry,
) -> Result<Vec<Pattern>> {
    let mut pp_counter: u32 = 0;
    lower_unresolved_patterns(patterns, db, vars, &mut pp_counter)
        .map_err(|e| TransactError::Parse(format!("WHERE pattern lowering: {e}")))
}

/// Generate a unique transaction ID for blank node skolemization
pub fn generate_txn_id() -> String {
    use fluree_db_core::clock::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{now:x}")
}

/// Convert a Binding to a (FlakeValue, datatype Sid) pair for flake generation
///
/// Returns `None` for non-materializable bindings (Unbound, Poisoned, Grouped, Iri).
/// This is used when generating retraction flakes from query results.
///
/// When a `Materializer` is provided, encoded bindings (`EncodedLit`, `EncodedSid`)
/// are decoded via the binary index store before conversion; a value that cannot be
/// decoded is an error, since skipping it would leave the old value unretracted.
/// Without a materializer, encoded bindings return `None` (this can cause upsert to
/// silently skip retractions for values that live in the binary index — see issue #88).
fn binding_to_flake_object(
    binding: &Binding,
    materializer: Option<&mut fluree_db_query::Materializer>,
) -> Result<Option<(FlakeValue, Sid)>> {
    Ok(match binding {
        Binding::Sid { sid, .. } => Some((FlakeValue::Ref(sid.clone()), Sid::new(1, "id"))),
        Binding::IriMatch { primary_sid, .. } => {
            Some((FlakeValue::Ref(primary_sid.clone()), Sid::new(1, "id")))
        }
        Binding::Lit { val, dtc, .. } => Some((val.clone(), dtc.datatype().clone())),
        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            match materializer {
                Some(mat) => binding_to_flake_object(&mat.to_term(binding)?, None)?,
                None => None,
            }
        }
        // Non-materializable bindings
        Binding::Unbound | Binding::Poisoned => None,
        Binding::Grouped(_) => {
            debug_assert!(
                false,
                "Grouped binding encountered in flake generation (unexpected)"
            );
            None
        }
        Binding::Path { .. } | Binding::Rel(_) | Binding::List(_) | Binding::Map(_) => {
            debug_assert!(
                false,
                "Path/List binding encountered in flake generation (unexpected)"
            );
            None
        }
        Binding::Iri(_) => {
            debug_assert!(
                false,
                "Raw IRI binding cannot be materialized to flake (no SID)"
            );
            None
        }
    })
}

/// Convert a TemplateTerm to a Binding for VALUES clause
fn template_term_to_binding(term: &TemplateTerm) -> Result<Binding> {
    match term {
        TemplateTerm::Sid(sid) => Ok(Binding::sid(sid.clone())),
        TemplateTerm::Value(val) => {
            let dt = infer_datatype(val);
            Ok(Binding::lit(val.clone(), dt))
        }
        TemplateTerm::Var(_) => Err(TransactError::InvalidTerm(
            "Variables not allowed in VALUES data rows".to_string(),
        )),
        TemplateTerm::BlankNode(_) => Err(TransactError::InvalidTerm(
            "Blank nodes not allowed in VALUES data rows".to_string(),
        )),
        TemplateTerm::TripleTerm(_) => Err(TransactError::InvalidTerm(
            "Triple terms not allowed in VALUES data rows".to_string(),
        )),
    }
}

/// Convert InlineValues to Pattern::Values
fn inline_values_to_pattern(values: &InlineValues) -> Result<Pattern> {
    let vars = values.vars.clone();
    let rows: Result<Vec<Vec<Binding>>> = values
        .rows
        .iter()
        .map(|row| row.iter().map(template_term_to_binding).collect())
        .collect();
    Ok(Pattern::Values { vars, rows: rows? })
}

/// Generate deletions for Upsert transactions
///
/// For each (subject, predicate, graph) tuple with concrete SIDs in the insert templates,
/// query existing values and generate retractions for them. This implements the
/// "replace mode" semantics of Upsert.
///
/// Subjects absent from both the persisted subject dictionary and novelty are
/// skipped without any index query: they cannot have existing values. This
/// matters because a bound-subject scan for a subject the dictionaries can't
/// resolve degrades to a full PSOT predicate-partition walk with per-row IRI
/// decoding (`unresolved_bound_subject_iri` in `BinaryScanOperator`) — for bulk
/// upserts of new entities that turned staging into minutes of work producing
/// zero retractions.
///
/// Named graph support: retractions are created in the same graph as the insert
/// templates to ensure proper cancellation with assertions.
async fn generate_upsert_deletions(
    ledger: &LedgerState,
    txn: &Txn,
    new_t: i64,
    graph_sids: &HashMap<String, Sid>,
) -> Result<Vec<fluree_db_core::Flake>> {
    use fluree_db_binary_index::BinaryGraphView;
    use fluree_db_core::{Flake, IndexType};
    use fluree_db_query::materializer::JoinKeyMode;
    use fluree_db_query::{BinaryRangeProvider, Materializer};

    // Group deduplicated predicates by (subject, graph IRI) so subject
    // existence is resolved once per subject rather than once per (subject,
    // predicate).
    let mut subject_groups: HashMap<(Sid, Option<Arc<str>>), Vec<Sid>> = HashMap::new();
    for template in &txn.insert_templates {
        let graph = match &template.graph {
            TemplateGraph::Default => None,
            TemplateGraph::Iri(iri) => Some(Arc::clone(iri)),
            // Upsert payloads name their graphs; a graph variable has no
            // stored values to replace.
            TemplateGraph::Var(_) => continue,
        };
        if let (TemplateTerm::Sid(s), TemplateTerm::Sid(p)) =
            (&template.subject, &template.predicate)
        {
            subject_groups
                .entry((s.clone(), graph))
                .or_default()
                .push(p.clone());
        }
        // Variables and blank nodes are skipped - we can't query for them
    }
    for predicates in subject_groups.values_mut() {
        predicates.sort_unstable();
        predicates.dedup();
    }

    if subject_groups.is_empty() {
        return Ok(Vec::new());
    }

    // Extract the binary index store and DictNovelty (if present) so we can
    // materialize EncodedLit/EncodedSid bindings returned by the binary scan path.
    let brp_ref = ledger
        .snapshot
        .range_provider
        .as_ref()
        .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>());
    let binary_store = brp_ref.map(|brp| Arc::clone(brp.store()));
    let dict_novelty = brp_ref.map(|brp| Arc::clone(brp.dict_novelty()));

    // Ledger graph id per graph IRI. None in the value position means the
    // graph is not yet in the ledger registry (new graph in this txn), so
    // there cannot be existing values.
    let ledger_g_for_txn_g: HashMap<Option<Arc<str>>, Option<u16>> = subject_groups
        .keys()
        .map(|(_, graph)| graph.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|graph| {
            let ledger_g = match &graph {
                None => Some(0),
                Some(iri) => ledger.snapshot.graph_registry.graph_id_for_iri(iri),
            };
            (graph, ledger_g)
        })
        .collect();

    // Novelty presence, resolved lazily: the per-graph set is built by a
    // filtered overlay walk only when a subject misses the base dictionary
    // (see the skip below), so upserts whose subjects all resolve in the
    // persisted dictionary never pay it. The walk is O(novelty flakes in the
    // graph), and novelty is bounded by `reindex_max_bytes` — up to 20% of
    // system RAM when indexing lags — which is why it runs at most once per
    // graph, and only on demand. It is authoritative in Sid space, needing
    // no dictionary translation.
    // Genesis, or nothing indexed yet: novelty is the only place a subject can
    // exist, so the presence check below is authoritative on its own.
    //
    // The `t == 0` conjunct mirrors `fluree_db_core::range`, which treats a
    // missing range provider as an empty index only at genesis and errors
    // otherwise ("binary-only db has no range_provider attached"). A binary
    // store that fails to load is non-fatal in the ledger manager, which leaves
    // an indexed ledger (`t > 0`) with no provider attached; subjects there DO
    // have base rows we cannot see, so absence must stay undecidable and the
    // per-predicate query must run — that path surfaces the load failure
    // instead of silently skipping every retraction.
    let base_index_absent = ledger.snapshot.range_provider.is_none() && ledger.snapshot.t == 0;
    let can_decide_absence = binary_store.is_some() || base_index_absent;

    let mut per_g_subjects: HashMap<u16, HashSet<&Sid>> = HashMap::new();
    if can_decide_absence {
        for (subject, txn_g) in subject_groups.keys() {
            if let Some(Some(ledger_g)) = ledger_g_for_txn_g.get(txn_g) {
                per_g_subjects.entry(*ledger_g).or_default().insert(subject);
            }
        }
    }
    let mut novelty_present: HashMap<u16, HashSet<Sid>> = HashMap::new();

    // Persisted presence: the subject reverse dictionary is authoritative under
    // canonical namespace encoding (see `fluree_db_core::ns_encoding`) — a miss
    // on both the (ns_code, suffix) key and the full-IRI key means the subject
    // has no rows in the base index. Lookup errors fall back to "present" so the
    // per-predicate query surfaces the real failure.
    let subject_in_base = |subject: &Sid| -> bool {
        let Some(store) = binary_store.as_deref() else {
            // Only reachable under `base_index_absent` (the caller gates on
            // `can_decide_absence`): there is no base index, so no subject has
            // rows in one. Novelty presence decides.
            return false;
        };
        if matches!(
            store.find_subject_id_by_parts(subject.namespace_code, &subject.name),
            Ok(Some(_))
        ) {
            return true;
        }
        // Resolve the subject IRI so the store's full-IRI lookup can run.
        //
        // A namespace code the pre-transaction snapshot cannot decode was
        // minted by this transaction, so it provably names no base-index row —
        // report absent rather than failing open. The store's own namespace
        // table is no help as a fallback: it is a subset of the snapshot's (the
        // index root is a materialized cache at `index_t`, and
        // `ns_helpers::sync_store_and_snapshot_ns` reconciles its codes back
        // into the snapshot on load), so it can never decode a code the
        // snapshot could not.
        //
        // Reporting "present" here instead defeats the skip for every IRI shape
        // that mints a namespace per subject — `MostGranular` splits
        // `urn:…:<id>:r:<sig>` at the last `:` — sending each one down a
        // per-(subject, predicate) degraded scan. Novelty presence is still
        // checked by the caller, so a subject that exists only in unindexed
        // commits is never wrongly skipped.
        match ledger.snapshot.decode_sid(subject) {
            Some(iri) => !matches!(store.find_subject_id(&iri), Ok(None)),
            None => false,
        }
    };

    let mut retractions = Vec::new();
    let mut skipped_subjects = 0usize;
    let mut pattern_queries = 0usize;

    // Query existing values for each (subject, predicate, graph) tuple
    let mut query_vars = VarRegistry::new();
    let o_var = query_vars.get_or_insert("?o");

    for ((subject, graph_id), predicates) in &subject_groups {
        let ledger_g_id: Option<u16> = ledger_g_for_txn_g.get(graph_id).copied().flatten();

        // Retraction flakes carry the graph Sid (flake.g). Resolved before any
        // skip so broken graph wiring still surfaces as an error.
        let graph_sid: Option<Sid> = match graph_id {
            None => None,
            Some(iri) => Some(graph_sids.get(&**iri).cloned().ok_or_else(|| {
                TransactError::FlakeGeneration(format!(
                    "upsert deletion generation references graph <{iri}> with no graph Sid; \
                     this indicates a bug in graph wiring"
                ))
            })?),
        };

        // Named graph not yet in the ledger registry: nothing to retract.
        if graph_id.is_some() && ledger_g_id.is_none() {
            continue;
        }
        let effective_g_id = ledger_g_id.unwrap_or(0);

        // Skip subjects with no persisted or novelty presence entirely. The
        // base dictionary is a point probe, so it is consulted first; the
        // per-graph novelty set is built on the first dictionary miss only.
        if can_decide_absence && !subject_in_base(subject) {
            let present = novelty_present.entry(effective_g_id).or_insert_with(|| {
                let mut set = HashSet::new();
                if let Some(subjects) = per_g_subjects.get(&effective_g_id) {
                    ledger.novelty.for_each_overlay_flake(
                        effective_g_id,
                        IndexType::Spot,
                        None,
                        None,
                        true,
                        ledger.t(),
                        &mut |flake| {
                            if subjects.contains(&flake.s) && !set.contains(&flake.s) {
                                set.insert(flake.s.clone());
                            }
                        },
                    );
                }
                set
            });
            if !present.contains(subject) {
                skipped_subjects += 1;
                continue;
            }
        }

        // Create a materializer for this graph context if a binary store exists.
        // BinaryGraphView::with_novelty handles watermark routing internally,
        // so novelty-only string/subject IDs resolve correctly.
        let mut materializer = binary_store.as_ref().map(|store| {
            let view = BinaryGraphView::with_novelty(
                Arc::clone(store),
                effective_g_id,
                dict_novelty.clone(),
            );
            Materializer::new(view, JoinKeyMode::SingleLedger)
        });

        for predicate in predicates {
            pattern_queries += 1;
            // Query: <subject> <predicate> ?o
            let pattern = TriplePattern::new(
                Ref::Sid(subject.clone()),
                Ref::Sid(predicate.clone()),
                Term::Var(o_var),
            );

            let batches = if graph_id.is_some() {
                if ledger.snapshot.range_provider.is_some() {
                    fluree_db_query::execute_pattern(
                        ledger.as_graph_db_ref(effective_g_id),
                        &query_vars,
                        pattern,
                    )
                    .await?
                } else {
                    // No binary store available (genesis / not indexed): scan novelty directly.
                    query_novelty_for_graph(ledger, subject, predicate, effective_g_id, o_var)
                }
            } else {
                // Default graph: use standard query path through range_provider
                fluree_db_query::execute_pattern(ledger.as_graph_db_ref(0), &query_vars, pattern)
                    .await?
            };

            for batch in &batches {
                for row in 0..batch.len() {
                    let flake_obj = match batch.get(row, o_var) {
                        Some(b) => binding_to_flake_object(b, materializer.as_mut())?,
                        None => None,
                    };
                    if let Some((o, dt)) = flake_obj {
                        let flake = match graph_sid.clone() {
                            Some(g) => Flake::new_in_graph(
                                g,
                                subject.clone(),
                                predicate.clone(),
                                o,
                                dt,
                                new_t,
                                false, // retraction
                                None,
                            ),
                            None => Flake::new(
                                subject.clone(),
                                predicate.clone(),
                                o,
                                dt,
                                new_t,
                                false, // retraction
                                None,
                            ),
                        };
                        retractions.push(flake);
                    }
                }
            }
        }
    }

    tracing::debug!(
        subject_count = subject_groups.len(),
        skipped_subjects,
        pattern_queries,
        "upsert deletion subject pre-check"
    );

    Ok(retractions)
}

/// Query novelty directly for a specific named graph
///
/// This function scans the novelty overlay for flakes matching the given
/// subject, predicate, and graph context. It's used for named graph upserts
/// because the db.range_provider is scoped to the default graph (g_id=0).
fn query_novelty_for_graph(
    ledger: &LedgerState,
    subject: &Sid,
    predicate: &Sid,
    target_g_id: u16,
    o_var: VarId,
) -> Vec<Batch> {
    use fluree_db_core::IndexType;

    // Collect matching flakes from novelty for the target graph
    let mut matching_values = Vec::new();
    ledger.novelty.for_each_overlay_flake(
        target_g_id,
        IndexType::Spot,
        None,
        None,
        true,
        ledger.t(),
        &mut |flake| {
            // Check if flake matches (subject, predicate) and is an assertion
            if &flake.s == subject && &flake.p == predicate && flake.op {
                matching_values.push((flake.o.clone(), flake.dt.clone()));
            }
        },
    );

    // Convert to batch format
    if matching_values.is_empty() {
        return Vec::new();
    }

    // Create a simple batch with just the object values
    let schema: Arc<[VarId]> = Arc::new([o_var]);
    let mut o_col = Vec::with_capacity(matching_values.len());
    for (o, dt) in &matching_values {
        o_col.push(Binding::from_object(o.clone(), dt.clone()));
    }

    match Batch::new(schema, vec![o_col]) {
        Ok(batch) => vec![batch],
        Err(_) => Vec::new(),
    }
}

/// Per-graph SHACL policy — how a specific graph's violations should be
/// treated at transaction time.
///
/// Absence from the policy map passed to [`validate_view_with_shacl`] means
/// the graph is **disabled** (shapes do not fire for subjects in that graph).
/// Presence with `mode = Reject` means violations cause the transaction to
/// fail; `mode = Warn` means violations are returned for the caller to log.
#[cfg(feature = "shacl")]
#[derive(Debug, Clone, Copy)]
pub struct ShaclGraphPolicy {
    pub mode: fluree_db_core::ledger_config::ValidationMode,
}

/// Outcome of a staged SHACL validation, split by mode so the caller can
/// apply warn (log-and-continue) vs reject (propagate as error) per graph.
#[cfg(feature = "shacl")]
#[derive(Debug, Default)]
pub struct ShaclValidationOutcome {
    /// Violations from graphs in `Reject` mode. Non-empty → transaction fails.
    pub reject_violations: Vec<fluree_db_shacl::ValidationResult>,
    /// Violations from graphs in `Warn` mode. The caller should log these.
    pub warn_violations: Vec<fluree_db_shacl::ValidationResult>,
}

#[cfg(feature = "shacl")]
impl ShaclValidationOutcome {
    pub fn conforms(&self) -> bool {
        self.reject_violations.is_empty() && self.warn_violations.is_empty()
    }
}

/// Validate a staged [`StagedLedger`] against SHACL shapes, each focus node
/// in the graph staging routed its flakes to.
///
/// `per_graph_policy`:
/// - `None` = treat every graph containing staged flakes as `Reject` mode
///   (legacy / unconditional reject — matches commit-transfer's previous
///   behavior and shapes-exist heuristic).
/// - `Some(map)` = only graphs in the map are validated; their mode comes
///   from the map. Graphs absent from the map are skipped (disabled).
///
/// Returns a [`ShaclValidationOutcome`] split into reject / warn buckets.
/// The caller decides whether to propagate an error, log warnings, or both.
#[cfg(feature = "shacl")]
#[allow(clippy::too_many_arguments)]
pub async fn validate_view_with_shacl(
    view: &StagedLedger,
    shacl_cache: std::sync::Arc<ShaclCache>,
    hierarchy: Option<fluree_db_core::SchemaHierarchy>,
    tracker: Option<&fluree_db_core::Tracker>,
    per_graph_policy: Option<&HashMap<GraphId, ShaclGraphPolicy>>,
    membership_g_ids: &[GraphId],
    cross_ledger: Option<fluree_db_shacl::CrossLedgerMembership<'_>>,
    sparql_iri_encoder: Option<&(dyn fluree_db_query::parse::IriEncoder + Sync)>,
) -> Result<ShaclValidationOutcome> {
    // Fast path: if there are no SHACL shapes, elide validation entirely.
    if shacl_cache.is_empty() {
        return Ok(ShaclValidationOutcome::default());
    }

    // `membership_g_ids` (the `f:shapesSource` graph[s]) are unioned into
    // `sh:class` value-membership resolution so a shared value-set vocabulary
    // can live alongside the shapes rather than in each data graph.
    // `cross_ledger_db` is a live handle into a model ledger holding the
    // controlled vocabulary (cross-ledger `f:shapesSource`), consulted on
    // demand for `sh:class` membership.
    let engine = ShaclEngine::from_shared_cache(shacl_cache, hierarchy)
        .with_membership_graphs(membership_g_ids.to_vec());
    let enabled_graphs: Option<HashSet<GraphId>> =
        per_graph_policy.map(|m| m.keys().copied().collect());
    let report = validate_staged_nodes(
        view,
        &engine,
        tracker,
        enabled_graphs.as_ref(),
        cross_ledger,
        sparql_iri_encoder,
    )
    .await?;

    // Split violations by the graph's configured mode. `graph_id` on each
    // result was tagged during the per-graph loop in validate_staged_nodes.
    // When per_graph_policy is None, every violation defaults to Reject.
    let mut outcome = ShaclValidationOutcome::default();
    for r in report.results {
        if r.severity != fluree_db_shacl::Severity::Violation {
            continue;
        }
        let mode = match (per_graph_policy, r.graph_id) {
            (Some(m), Some(g_id)) => m
                .get(&g_id)
                .map(|p| p.mode)
                .unwrap_or(fluree_db_core::ledger_config::ValidationMode::Reject),
            _ => fluree_db_core::ledger_config::ValidationMode::Reject,
        };
        match mode {
            fluree_db_core::ledger_config::ValidationMode::Reject => {
                outcome.reject_violations.push(r);
            }
            fluree_db_core::ledger_config::ValidationMode::Warn => {
                outcome.warn_violations.push(r);
            }
        }
    }
    Ok(outcome)
}

/// Validate staged nodes against SHACL shapes, per graph.
///
/// Groups staged subjects by their graph and validates each group with a
/// `GraphDbRef` targeting the correct `g_id`. Shape *compilation* graph is
/// chosen upstream by `f:shapesSource` (see `apply_shacl_policy_to_staged_view`)
/// — this loop only drives per-graph *validation*. `sh:class` value membership
/// additionally consults the engine's `membership_g_ids` (the `f:shapesSource`
/// vocabulary graph[s]) unioned with each focus node's own data graph.
#[cfg(feature = "shacl")]
async fn validate_staged_nodes(
    view: &StagedLedger,
    engine: &ShaclEngine,
    tracker: Option<&fluree_db_core::Tracker>,
    enabled_graphs: Option<&HashSet<GraphId>>,
    cross_ledger: Option<fluree_db_shacl::CrossLedgerMembership<'_>>,
    sparql_iri_encoder: Option<&(dyn fluree_db_query::parse::IriEncoder + Sync)>,
) -> Result<ValidationReport> {
    use fluree_vocab::namespaces::RDF;
    use fluree_vocab::rdf_names;

    // Fast path: no shapes means no validation work.
    if engine.cache().all_shapes().is_empty() {
        return Ok(ValidationReport::conforming());
    }

    if !view.has_staged() {
        return Ok(ValidationReport::conforming());
    }

    // Group staged focus nodes by graph. A subject may appear in multiple
    // graphs.
    //
    // Ref-objects of assert flakes are pulled in as focus nodes too, so
    // `sh:targetObjectsOf` shapes targeting a newly-referenced node get
    // evaluated on the write path. Retractions do NOT expand the focus set
    // via their object — removing an inbound edge doesn't introduce
    // validation work at the target.
    //
    // Predicate-target applicability (`sh:targetSubjectsOf` / `ObjectsOf`)
    // is resolved inside `ShaclEngine::validate_node` by querying the
    // post-transaction view directly. We don't pre-compute hints here
    // because hints derived from staged flakes miss the "base edge persists,
    // node touched for an unrelated reason" case — e.g., alice already has
    // `ex:ssn` in the base DB, and this txn retracts `ex:name`.
    let mut subjects_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        // Subject is always a focus (including for retractions — validators
        // must still see retracted-on subjects so class/node-targeted shapes
        // can re-check cardinality, and so the engine's post-state check can
        // notice that a predicate-target no longer applies).
        subjects_by_graph
            .entry(g_id)
            .or_default()
            .insert(flake.s.clone());

        // Ref-objects of assert flakes become focus nodes in the flake's
        // graph. This is the only way a node that wasn't otherwise touched
        // by the transaction gets pulled in to be validated against
        // `sh:targetObjectsOf` shapes targeting the newly-introduced edge.
        if flake.op {
            if let fluree_db_core::FlakeValue::Ref(obj) = &flake.o {
                subjects_by_graph
                    .entry(g_id)
                    .or_default()
                    .insert(obj.clone());
            }
        }
    }

    let snapshot = view.db();
    let mut all_results = Vec::new();

    for (g_id, subjects) in &subjects_by_graph {
        // Per-graph enable/disable: when the caller supplies an explicit
        // enabled set, graphs not in the set are skipped. Subjects staged in
        // a disabled graph therefore receive no shape validation from this
        // transaction, which matches the documented `shacl.enabled: false`
        // semantics for that graph (`override-control.md`).
        if let Some(enabled) = enabled_graphs {
            if !enabled.contains(g_id) {
                continue;
            }
        }

        // Build GraphDbRef for this graph.
        // Use staged_t so GraphDbRef sees staged flakes (which have t > snapshot.t).
        let mut db = fluree_db_core::GraphDbRef::new(snapshot, *g_id, view, view.staged_t());
        if let Some(t) = tracker {
            db = db.with_tracker(t);
        }

        for subject in subjects {
            // Get the node's types for shape targeting
            let rdf_type = Sid::new(RDF, rdf_names::TYPE);
            let type_flakes = db
                .range(
                    fluree_db_core::IndexType::Spot,
                    fluree_db_core::RangeTest::Eq,
                    fluree_db_core::RangeMatch::subject_predicate(subject.clone(), rdf_type),
                )
                .await?;

            let node_types: Vec<Sid> = type_flakes
                .iter()
                .filter_map(|f| {
                    if let fluree_db_core::FlakeValue::Ref(type_sid) = &f.o {
                        Some(type_sid.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Validate this node. Predicate-target applicability is resolved
            // inside `validate_node` via post-state range queries — see the
            // SubjectsOf/ObjectsOf handling there for why hints can't be
            // reliably built from staged flakes alone.
            let report = engine
                .validate_node(db, subject, &node_types, cross_ledger, sparql_iri_encoder)
                .await?;
            // Tag each result with the graph it was validated under so the
            // caller can route warn vs reject per-graph (see
            // `ShaclValidationOutcome`).
            all_results.extend(report.results.into_iter().map(|mut r| {
                r.graph_id = Some(*g_id);
                r
            }));
        }
    }

    // Spec semantics: `sh:conforms` is true iff there are NO results, matching
    // `ShaclEngine::validate_staged` so `ValidationReport.conforms` means the
    // same thing across crates. Enforcement gates here key off
    // `violation_count()` (warnings/info don't reject), not `conforms`.
    let conforms = all_results.is_empty();

    Ok(ValidationReport {
        conforms,
        results: all_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{TemplateTerm, TripleTemplate, Txn};
    use fluree_db_core::{FlakeValue, LedgerSnapshot, MemoryStorage, Sid};
    use fluree_db_novelty::Novelty;
    use fluree_db_query::parse::{UnresolvedTerm, UnresolvedTriplePattern};

    /// Helper to create an UnresolvedPattern::Triple for WHERE clauses in tests
    fn where_triple(s: UnresolvedTerm, p: &str, o: UnresolvedTerm) -> UnresolvedPattern {
        UnresolvedPattern::Triple(UnresolvedTriplePattern::new(s, UnresolvedTerm::iri(p), o))
    }

    #[test]
    fn column_needs_materialization_detects_each_encoded_variant() {
        // Already-concrete bindings — must NOT trigger rewrite.
        let concrete = vec![
            Binding::sid(Sid::new(1, "a")),
            Binding::Unbound,
            Binding::Poisoned,
        ];
        assert!(!column_needs_materialization(&concrete));

        // Each Encoded* variant must trigger rewrite individually.
        assert!(column_needs_materialization(&[Binding::encoded_sid(7)]));
        assert!(column_needs_materialization(&[Binding::EncodedPid {
            p_id: 3
        }]));
        assert!(column_needs_materialization(&[Binding::EncodedLit {
            o_kind: 0,
            o_key: 0,
            p_id: 0,
            dt_id: 0,
            lang_id: 0,
            i_val: 0,
            t: 0,
        }]));

        // A column with a single encoded entry among many concrete entries
        // must still trigger — early-exit on first hit.
        let mut mixed = vec![Binding::sid(Sid::new(1, "a")); 8];
        mixed.push(Binding::encoded_sid(1));
        mixed.extend(std::iter::repeat_n(Binding::Unbound, 4));
        assert!(column_needs_materialization(&mixed));
    }

    #[tokio::test]
    async fn test_stage_simple_insert() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Create a simple insert transaction
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 1);
    }

    #[tokio::test]
    async fn test_stage_insert_multiple_triples() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Insert multiple triples
        let txn = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:age")),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 2);
    }

    #[tokio::test]
    async fn test_stage_with_blank_nodes() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Insert with blank node
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Anonymous".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 1);
        // Blank nodes use the predefined _: prefix (BLANK_NODE code), no new namespace allocation needed
        assert!(ns_registry.has_prefix("_:"));
    }

    #[tokio::test]
    async fn test_stage_backpressure_at_max() {
        use fluree_db_core::Flake;

        let db = LedgerSnapshot::genesis("test:main");

        // Create novelty that's at max size
        let mut novelty = Novelty::new(0);
        // Add a lot of flakes to exceed the limit
        for i in 0..1000 {
            let flake = Flake::new(
                Sid::new(1, format!("s{i}")),
                Sid::new(1, "p"),
                FlakeValue::Long(i),
                Sid::new(2, "long"),
                1,
                true,
                None,
            );
            novelty
                .apply_commit(vec![flake], 1, &HashMap::new())
                .unwrap();
        }

        let ledger = LedgerState::new(db, novelty);

        // Use a very small config to trigger backpressure
        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 500, // Small limit
        };

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        // Stage should fail with NoveltyAtMax
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let options = StageOptions::new().with_index_config(&config);
        let result = stage(ledger, txn, ns_registry, options).await;
        assert!(matches!(result, Err(TransactError::NoveltyAtMax)));
    }

    #[tokio::test]
    async fn test_insert_with_blank_node_always_succeeds() {
        // Blank nodes are always new, so insert should succeed even if
        // the blank node was used before (it gets a new skolemized ID)
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Test".to_string())),
        ));

        // Should succeed - blank nodes don't trigger existence check
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let result = stage(ledger, txn, ns_registry, StageOptions::default()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_upsert_replaces_existing_values() {
        use crate::commit::{commit, CommitOpts};
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");

        // First: insert ex:alice with name="Alice"
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view1, ns_registry1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_receipt, state1) = commit(
            view1,
            ns_registry1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Now: upsert ex:alice with name="Alicia" (should replace)
        let txn2 = Txn::upsert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alicia".to_string())),
        ));

        let ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let (view2, _ns_registry2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();

        // Check that we have both a retraction and an assertion
        let (_base, staged) = view2.into_parts();

        // Should have 2 flakes: one retraction for "Alice", one assertion for "Alicia"
        assert_eq!(staged.len(), 2);

        // Find retraction
        let retraction = staged
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "ex:alice");
        assert_eq!(retraction.p.name.as_ref(), "ex:name");
        assert_eq!(retraction.o, FlakeValue::String("Alice".to_string()));

        // Find assertion
        let assertion = staged.iter().find(|f| f.op).expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "ex:alice");
        assert_eq!(assertion.p.name.as_ref(), "ex:name");
        assert_eq!(assertion.o, FlakeValue::String("Alicia".to_string()));
    }

    #[tokio::test]
    async fn test_upsert_on_nonexistent_subject() {
        // Upsert on a subject that doesn't exist should just insert
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let txn = Txn::upsert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Should have just one assertion (no retraction since nothing existed)
        assert_eq!(view.staged_len(), 1);
        let (_base, staged) = view.into_parts();
        assert!(staged[0].op); // assertion
    }

    #[tokio::test]
    async fn test_where_uses_ledger_t_not_db_t() {
        // Test that WHERE patterns see data in novelty (committed but not indexed),
        // not just data in the indexed db. This is the "time boundary" correctness test.
        use crate::commit::{commit, CommitOpts};
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");

        // Commit 1: Insert schema:alice with schema:name="Alice"
        // Do NOT rely on pre-registered SCHEMA_ORG codes — this build intentionally keeps
        // the default namespace table minimal. Allocate via NamespaceRegistry.
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_name = ns_registry.sid_for_iri("http://schema.org/name");
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(schema_alice.clone()),
            TemplateTerm::Sid(schema_name.clone()),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // state1 now has t=1 with data in NOVELTY (not indexed)
        assert_eq!(state1.t(), 1);
        // Novelty includes 1 txn flake + commit metadata flakes
        assert!(
            !state1.novelty.is_empty(),
            "novelty should have at least 1 transaction flake (Alice's name)"
        );

        // Commit 2: Update with WHERE pattern that should match data in novelty
        // This UPDATE should find schema:alice's name (in novelty) and change it
        let mut vars = VarRegistry::new();
        let name_var = vars.get_or_insert("?name");

        // WHERE pattern uses UnresolvedPattern with string IRIs.
        // The variable "?name" will be assigned the same VarId during lowering
        // as was registered for DELETE/INSERT templates.
        let txn2 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::iri("http://schema.org/alice"),
                "http://schema.org/name",
                UnresolvedTerm::var("?name"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_name.clone()),
                TemplateTerm::Var(name_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice),
                TemplateTerm::Sid(schema_name),
                TemplateTerm::Value(FlakeValue::String("Alicia".to_string())),
            ))
            .with_vars(vars);

        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        // Ensure schema.org prefix is present in the registry used for lowering.
        // (Should already be in LedgerSnapshot.namespace_codes via commit delta, but this makes the test robust.)
        let _ = ns_registry2.sid_for_iri("http://schema.org/alice");
        let _ = ns_registry2.sid_for_iri("http://schema.org/name");
        let (view2, _ns2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();

        // The WHERE should have found "Alice" (in novelty), so we should have:
        // - A retraction for "Alice"
        // - An assertion for "Alicia"
        let (_base2, staged2) = view2.into_parts();
        assert_eq!(staged2.len(), 2);

        // Verify we got the retraction (proving WHERE saw the novelty data)
        let retraction = staged2.iter().find(|f| !f.op);
        assert!(
            retraction.is_some(),
            "WHERE should have found data in novelty"
        );
        assert_eq!(
            retraction.unwrap().o,
            FlakeValue::String("Alice".to_string())
        );
    }

    #[tokio::test]
    async fn test_multi_pattern_where_join() {
        // Test that multiple WHERE patterns are joined correctly.
        // This verifies that execute_where_with_overlay_at handles joins.
        use crate::commit::{commit, CommitOpts};
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");

        // Commit 1: Insert schema:alice with name="Alice" and age=30
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_name = ns_registry.sid_for_iri("http://schema.org/name");
        let schema_age = ns_registry.sid_for_iri("http://schema.org/age");
        let txn1 = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_name.clone()),
                TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Commit 2: Also insert schema:bob with only a name (no age)
        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let schema_bob = ns_registry2.sid_for_iri("http://schema.org/bob");
        let schema_name2 = ns_registry2.sid_for_iri("http://schema.org/name");
        let txn2 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(schema_bob.clone()),
            TemplateTerm::Sid(schema_name2.clone()),
            TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
        ));

        let (view2, ns2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();
        let (_r2, state2) = commit(
            view2,
            ns2,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Now: Multi-pattern UPDATE
        // WHERE { ?s schema:name ?name . ?s schema:age ?age }  <- requires BOTH patterns to match
        // DELETE { ?s schema:age ?age }
        // INSERT { ?s schema:age 31 }
        //
        // This should ONLY match schema:alice (who has both name and age).
        // schema:bob should NOT match (has name but no age).

        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let _name_var = vars.get_or_insert("?name");
        let age_var = vars.get_or_insert("?age");

        // WHERE patterns use UnresolvedPattern with string IRIs and variable names
        let txn3 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/name",
                UnresolvedTerm::var("?name"),
            ))
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/age",
                UnresolvedTerm::var("?age"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Var(age_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age),
                TemplateTerm::Value(FlakeValue::Long(31)),
            ))
            .with_vars(vars);

        let mut ns_registry3 = NamespaceRegistry::from_db(&state2.snapshot);
        // Ensure schema.org prefix exists for lowering WHERE IRIs.
        let _ = ns_registry3.sid_for_iri("http://schema.org/age");
        let _ = ns_registry3.sid_for_iri("http://schema.org/name");
        let (view3, _ns3) = stage(state2, txn3, ns_registry3, StageOptions::default())
            .await
            .unwrap();

        // Should have exactly 2 flakes:
        // - Retraction of schema:alice schema:age 30
        // - Assertion of schema:alice schema:age 31
        let (_base3, staged3) = view3.into_parts();
        assert_eq!(
            staged3.len(),
            2,
            "Should have exactly 2 flakes (1 retraction + 1 assertion)"
        );

        // Verify the retraction is for alice's old age
        let retraction = staged3
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "alice");
        assert_eq!(retraction.p.name.as_ref(), "age");
        assert_eq!(retraction.o, FlakeValue::Long(30));

        // Verify the assertion is for alice's new age
        let assertion = staged3
            .iter()
            .find(|f| f.op)
            .expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "alice");
        assert_eq!(assertion.p.name.as_ref(), "age");
        assert_eq!(assertion.o, FlakeValue::Long(31));
    }

    #[tokio::test]
    async fn test_values_seeding_insert() {
        // Test that VALUES can seed bindings for INSERT templates.
        // This supports transactions like:
        //   VALUES ?s ?name { (ex:alice "Alice") (ex:bob "Bob") }
        //   INSERT { ?s ex:name ?name }
        // Which should create two triples with different subjects and names.
        use crate::ir::InlineValues;

        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Create a transaction with VALUES seeding - using named subjects
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let name_var = vars.get_or_insert("?name");

        let values = InlineValues::new(
            vec![s_var, name_var],
            vec![
                vec![
                    TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                    TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
                ],
                vec![
                    TemplateTerm::Sid(Sid::new(1, "ex:bob")),
                    TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
                ],
            ],
        );

        let txn = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Var(name_var),
            ))
            .with_values(values)
            .with_vars(vars);

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Should have 2 assertions (one for "Alice", one for "Bob")
        let (_base, staged) = view.into_parts();
        assert_eq!(staged.len(), 2, "Should have 2 flakes from VALUES seeding");

        // Both should be assertions
        assert!(
            staged.iter().all(|f| f.op),
            "All flakes should be assertions"
        );

        // Verify we got both names with correct subjects
        let alice_flake = staged.iter().find(|f| f.s.name.as_ref() == "ex:alice");
        let bob_flake = staged.iter().find(|f| f.s.name.as_ref() == "ex:bob");

        assert!(alice_flake.is_some(), "Should have alice flake");
        assert!(bob_flake.is_some(), "Should have bob flake");

        assert_eq!(
            alice_flake.unwrap().o,
            FlakeValue::String("Alice".to_string())
        );
        assert_eq!(bob_flake.unwrap().o, FlakeValue::String("Bob".to_string()));
    }

    #[tokio::test]
    async fn test_values_seeding_with_where_join() {
        // Test VALUES seeding combined with WHERE patterns.
        // This verifies that VALUES can constrain which subjects are matched.
        use crate::commit::{commit, CommitOpts};
        use crate::ir::InlineValues;
        use fluree_db_core::content_store_for;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig {
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000_000,
        };
        let cs = content_store_for(storage.clone(), "test:main");

        // Insert data: alice has age 30, bob has age 25
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_bob = ns_registry.sid_for_iri("http://schema.org/bob");
        let schema_age = ns_registry.sid_for_iri("http://schema.org/age");
        let txn1 = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_bob.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(25)),
            ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &cs,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Verify state after first commit
        assert_eq!(state1.t(), 1);
        // Novelty includes 2 txn flakes + commit metadata flakes
        assert!(
            state1.novelty.len() >= 2,
            "novelty should have at least 2 transaction flakes (alice and bob's ages)"
        );

        // Now: Update with VALUES constraining to only alice
        // VALUES ?s { schema:alice }
        // WHERE { ?s schema:age ?age }
        // DELETE { ?s schema:age ?age }
        // INSERT { ?s schema:age 35 }
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let age_var = vars.get_or_insert("?age");

        let values = InlineValues::new(
            vec![s_var],
            vec![vec![TemplateTerm::Sid(schema_alice.clone())]],
        );

        // WHERE pattern uses UnresolvedPattern with string variable names
        let txn2 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/age",
                UnresolvedTerm::var("?age"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Var(age_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age),
                TemplateTerm::Value(FlakeValue::Long(35)),
            ))
            .with_values(values)
            .with_vars(vars);

        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let _ = ns_registry2.sid_for_iri("http://schema.org/age");
        let result = stage(state1, txn2, ns_registry2, StageOptions::default()).await;

        // Check if stage succeeded
        let (view2, _ns2) = result.expect("stage should succeed");

        // Should have exactly 2 flakes (retraction + assertion for alice only)
        let (_base2, staged2) = view2.into_parts();
        assert_eq!(
            staged2.len(),
            2,
            "Should have 2 flakes (alice only, not bob)"
        );

        // Verify only alice's age was affected
        let retraction = staged2
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "alice");
        assert_eq!(retraction.o, FlakeValue::Long(30));

        let assertion = staged2
            .iter()
            .find(|f| f.op)
            .expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "alice");
        assert_eq!(assertion.o, FlakeValue::Long(35));
    }
}
