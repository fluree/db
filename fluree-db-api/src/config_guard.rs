//! Authoring checks on what a transaction writes into the ledger config.
//!
//! Ledger configuration is read only from the ledger's own config graph, from
//! one `f:LedgerConfig` subject, taking one value of each single-valued
//! setting. A write that breaks one of these commits and then quietly does
//! something other than what its author meant: group fields stranded in
//! another graph read as an empty group, a config typed in another graph is
//! never read, and of two values for one setting the reader picks one. Each
//! is refused here instead, with a message saying how to write it.
//!
//! The guard reads only the staged flakes and the pre-transaction config
//! graph. It resolves no shapes, schema, constraints or policy artifact, so a
//! config repair is judged by it alone. It runs where transactions are
//! authored (JSON-LD and SPARQL transactions, Turtle insert); commit replay and
//! bulk import carry already-authored history and skip it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use fluree_db_core::graph_registry::{config_graph_iri, CONFIG_GRAPH_ID};
use fluree_db_core::{
    range_with_overlay, Flake, FlakeValue, GraphId, IndexType, RangeMatch, RangeOptions, RangeTest,
    Sid,
};
use fluree_db_ledger::StagedLedger;
use fluree_db_transact::{NamespaceRegistry, TransactError};
use fluree_vocab::config_iris;
use fluree_vocab::namespaces::{FLUREE_DB, RDF};
use rustc_hash::FxHashMap;

/// Edges from a config node to another config node. The node an edge points
/// at is part of the configuration, so its fields belong in the config graph
/// with the edge.
const CONFIG_EDGES: &[&str] = &[
    config_iris::POLICY_DEFAULTS,
    config_iris::SHACL_DEFAULTS,
    config_iris::REASONING_DEFAULTS,
    config_iris::DATALOG_DEFAULTS,
    config_iris::TRANSACT_DEFAULTS,
    config_iris::FULL_TEXT_DEFAULTS,
    config_iris::SERVING_DEFAULTS,
    config_iris::GRAPH_OVERRIDES,
    config_iris::OVERRIDE_CONTROL,
    config_iris::SHAPES_SOURCE,
    config_iris::POLICY_SOURCE,
    config_iris::SCHEMA_SOURCE,
    config_iris::RULES_SOURCE,
    config_iris::CONSTRAINTS_SOURCE,
    config_iris::GRAPH_SOURCE,
    config_iris::TRUST_POLICY_PRED,
    config_iris::ROLLBACK_GUARD,
    config_iris::ONTOLOGY_IMPORT_MAP,
    config_iris::GRAPH_REF_PROP,
    config_iris::FULL_TEXT_PROPERTY,
];

/// Config predicates the reader takes a single value of.
const SINGLE_VALUED: &[&str] = &[
    config_iris::POLICY_DEFAULTS,
    config_iris::SHACL_DEFAULTS,
    config_iris::REASONING_DEFAULTS,
    config_iris::DATALOG_DEFAULTS,
    config_iris::TRANSACT_DEFAULTS,
    config_iris::FULL_TEXT_DEFAULTS,
    config_iris::SERVING_DEFAULTS,
    config_iris::DEFAULT_ALLOW,
    config_iris::POLICY_SOURCE,
    config_iris::SHACL_ENABLED,
    config_iris::SHAPES_SOURCE,
    config_iris::VALIDATION_MODE,
    config_iris::OVERRIDE_CONTROL,
    config_iris::CONTROL_MODE,
    config_iris::SCHEMA_SOURCE,
    config_iris::FOLLOW_OWL_IMPORTS,
    config_iris::REASONING_MAX_FACTS,
    config_iris::REASONING_MAX_SECONDS,
    config_iris::REASONING_MAX_MEMORY_MB,
    config_iris::DATALOG_ENABLED,
    config_iris::RULES_SOURCE,
    config_iris::ALLOW_QUERY_TIME_RULES,
    config_iris::SERVE_QUERY,
    config_iris::SERVE_BLOCKS,
    config_iris::PUBLIC_VISIBILITY,
    config_iris::UNIQUE_ENABLED,
    config_iris::DEFAULT_LANGUAGE,
    config_iris::TARGET_GRAPH,
    config_iris::GRAPH_SOURCE,
    config_iris::LEDGER_PRED,
    config_iris::GRAPH_SELECTOR,
    config_iris::AT_T,
    config_iris::TRUST_POLICY_PRED,
    config_iris::TRUST_MODE,
    config_iris::ROLLBACK_GUARD,
    config_iris::MIN_T,
    config_iris::ONTOLOGY_IRI,
    config_iris::GRAPH_REF_PROP,
    config_iris::FULL_TEXT_TARGET,
];

/// Maximum RDF-list length walked when validating a staged `f:reasoningModes`
/// collection — a malformed cyclic list must not spin.
const MAX_STAGED_REASONING_LIST_LEN: usize = 64;

/// Check what a staged transaction writes into the ledger config.
///
/// Refuses, with `TransactError::Parse`:
/// - a config group split across graphs: a config edge written into the
///   config graph whose target node gets its `f:` fields in another graph;
/// - an `f:LedgerConfig` or `f:GraphConfig` typed outside the config graph,
///   which has no effect;
/// - a second value for a single-valued config setting, or a second
///   `f:LedgerConfig` subject, counting what the config graph already holds;
/// - an unrecognized `f:reasoningModes` value.
///
/// A plain data transaction writes nothing to the config graph, types nothing
/// with an `f:` class and asserts no reasoning modes: one pass over the staged
/// flakes establishes that, and nothing else is read.
///
/// `ns` and `graph_delta` name terms and graphs in messages.
pub(crate) async fn validate_staged_config(
    view: &StagedLedger,
    ns: &NamespaceRegistry,
    graph_delta: &FxHashMap<u16, String>,
) -> Result<(), TransactError> {
    let rdf_type = Sid::new(RDF, fluree_vocab::rdf_names::TYPE);
    let modes_p = fluree_sid(config_iris::REASONING_MODES);
    let (mut writes_config, mut types_config, mut asserts_modes) = (false, false, false);
    for (g_id, flake) in view.staged_flakes_by_graph() {
        writes_config |= g_id == CONFIG_GRAPH_ID;
        types_config |= flake.op
            && flake.p == rdf_type
            && matches!(&flake.o, FlakeValue::Ref(o) if o.namespace_code == FLUREE_DB);
        asserts_modes |= flake.op && flake.p == modes_p;
    }
    if !(writes_config || types_config || asserts_modes) {
        return Ok(());
    }

    let names = Names::new(view, ns, graph_delta);
    if types_config {
        refuse_config_typed_outside_config_graph(view, &names, &rdf_type)?;
    }
    if writes_config {
        refuse_split_groups(view, &names)?;
        refuse_second_values(view, &names, &rdf_type).await?;
    }
    if asserts_modes {
        validate_reasoning_modes(view, &modes_p)?;
    }
    Ok(())
}

/// The `Sid` of a Fluree vocabulary IRI (`https://ns.flur.ee/db#...`).
fn fluree_sid(iri: &str) -> Sid {
    let local = iri
        .strip_prefix(fluree_vocab::fluree::DB)
        .expect("a Fluree vocabulary IRI");
    Sid::new(FLUREE_DB, local)
}

/// An `f:LedgerConfig` / `f:GraphConfig` typed outside the config graph is
/// never read.
fn refuse_config_typed_outside_config_graph(
    view: &StagedLedger,
    names: &Names<'_>,
    rdf_type: &Sid,
) -> Result<(), TransactError> {
    let config_types = [
        fluree_sid(config_iris::LEDGER_CONFIG),
        fluree_sid(config_iris::GRAPH_CONFIG),
    ];
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if !flake.op || g_id == CONFIG_GRAPH_ID || flake.p != *rdf_type {
            continue;
        }
        if let FlakeValue::Ref(class) = &flake.o {
            if config_types.contains(class) {
                return Err(TransactError::Parse(format!(
                    "{} is typed {} in {}, where it has no effect: ledger configuration is \
                     read only from the config graph <{}>; write it into that graph",
                    names.term(&flake.s),
                    names.term(class),
                    names.graph(g_id),
                    names.config_graph,
                )));
            }
        }
    }
    Ok(())
}

/// A config edge written into the config graph whose target node gets `f:`
/// fields in another graph: the reader sees the group without them.
fn refuse_split_groups(view: &StagedLedger, names: &Names<'_>) -> Result<(), TransactError> {
    let edges: Vec<Sid> = CONFIG_EDGES.iter().map(|iri| fluree_sid(iri)).collect();
    let mut groups: HashMap<&Sid, &Sid> = HashMap::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if flake.op && g_id == CONFIG_GRAPH_ID && edges.contains(&flake.p) {
            if let FlakeValue::Ref(node) = &flake.o {
                groups.insert(node, &flake.p);
            }
        }
    }
    if groups.is_empty() {
        return Ok(());
    }
    let mut stray: BTreeMap<(&Sid, GraphId), Vec<&Sid>> = BTreeMap::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if flake.op
            && g_id != CONFIG_GRAPH_ID
            && flake.p.namespace_code == FLUREE_DB
            && groups.contains_key(&flake.s)
        {
            stray.entry((&flake.s, g_id)).or_default().push(&flake.p);
        }
    }
    match stray.into_iter().next() {
        None => Ok(()),
        Some(((node, g_id), fields)) => {
            let fields: BTreeSet<String> = fields.into_iter().map(|p| names.term(p)).collect();
            Err(TransactError::Parse(format!(
                "config group {} (the value of {} in the config graph) has its fields {} in {}; \
                 a group is read only from the config graph <{}>, so write its fields into \
                 that graph too",
                names.term(node),
                names.term(groups[node]),
                fields.into_iter().collect::<Vec<_>>().join(", "),
                names.graph(g_id),
                names.config_graph,
            )))
        }
    }
}

/// A single-valued config setting with more than one value after this
/// transaction, or a second `f:LedgerConfig` subject.
async fn refuse_second_values(
    view: &StagedLedger,
    names: &Names<'_>,
    rdf_type: &Sid,
) -> Result<(), TransactError> {
    let single: Vec<Sid> = SINGLE_VALUED.iter().map(|iri| fluree_sid(iri)).collect();
    let ledger_config = fluree_sid(config_iris::LEDGER_CONFIG);
    type Value = (FlakeValue, Sid);
    let mut asserted: BTreeMap<(&Sid, &Sid), Vec<Value>> = BTreeMap::new();
    let mut retracted: HashMap<(&Sid, &Sid), Vec<Value>> = HashMap::new();
    let mut typed: BTreeSet<&Sid> = BTreeSet::new();
    let mut untyped: BTreeSet<&Sid> = BTreeSet::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if g_id != CONFIG_GRAPH_ID {
            continue;
        }
        if single.contains(&flake.p) {
            let value = (flake.o.clone(), flake.dt.clone());
            if flake.op {
                asserted
                    .entry((&flake.s, &flake.p))
                    .or_default()
                    .push(value);
            } else {
                retracted
                    .entry((&flake.s, &flake.p))
                    .or_default()
                    .push(value);
            }
        } else if flake.p == *rdf_type && flake.o == FlakeValue::Ref(ledger_config.clone()) {
            if flake.op {
                typed.insert(&flake.s);
            } else {
                untyped.insert(&flake.s);
            }
        }
    }

    let base = view.base();
    for ((subject, predicate), added) in asserted {
        let mut values: Vec<Value> = config_graph_flakes(
            view,
            RangeMatch {
                s: Some(subject.clone()),
                p: Some(predicate.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .map(|flake| (flake.o, flake.dt))
        .collect();
        if let Some(removed) = retracted.get(&(subject, predicate)) {
            values.retain(|value| !removed.contains(value));
        }
        for value in added {
            if !values.contains(&value) {
                values.push(value);
            }
        }
        if values.len() > 1 {
            return Err(TransactError::Parse(format!(
                "{} would have {} values for {}, which takes one; use upsert, or delete the old \
                 value in the same transaction",
                names.term(subject),
                values.len(),
                names.term(predicate),
            )));
        }
    }

    if !typed.is_empty() {
        let mut subjects: BTreeSet<Sid> = crate::config_resolver::ledger_config_subjects(
            &base.snapshot,
            &*base.novelty,
            base.t(),
        )
        .await
        .map_err(|e| TransactError::Parse(format!("failed to load ledger config: {e}")))?
        .into_iter()
        .filter(|s| !untyped.contains(s))
        .collect();
        subjects.extend(typed.into_iter().cloned());
        if subjects.len() > 1 {
            let listed: Vec<String> = subjects.iter().map(|s| names.term(s)).collect();
            return Err(TransactError::Parse(format!(
                "the config graph would hold {} f:LedgerConfig subjects ({}); a ledger has one, \
                 so add settings to the existing subject",
                subjects.len(),
                listed.join(", "),
            )));
        }
    }
    Ok(())
}

/// The config graph's flakes matching `pattern`, as of the pre-transaction
/// state.
async fn config_graph_flakes(
    view: &StagedLedger,
    pattern: RangeMatch,
) -> Result<Vec<Flake>, TransactError> {
    let base = view.base();
    range_with_overlay(
        &base.snapshot,
        CONFIG_GRAPH_ID,
        base.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        pattern,
        RangeOptions {
            to_t: Some(base.t()),
            ..Default::default()
        },
    )
    .await
    .map_err(TransactError::from)
}

/// Reject a transaction that writes an unrecognized `f:reasoningModes` value.
///
/// Config reasoning modes are otherwise only parsed at query time, where an
/// unknown mode is warned-and-skipped — so a typo silently disables reasoning
/// with no signal. Handles the same value shapes as the config reader — a
/// direct string literal, a direct mode IRI, and an RDF collection of either —
/// collected from this transaction's own staged flakes.
fn validate_reasoning_modes(view: &StagedLedger, modes_p: &Sid) -> Result<(), TransactError> {
    let snapshot = &view.base().snapshot;
    let flakes = view.staged_flakes();

    let mut candidates: Vec<String> = Vec::new();
    let mut list_heads: Vec<Sid> = Vec::new();
    for f in flakes {
        if !f.op || f.p != *modes_p {
            continue;
        }
        match &f.o {
            FlakeValue::String(s) => candidates.push(s.to_string()),
            FlakeValue::Ref(sid) => list_heads.push(sid.clone()),
            _ => {}
        }
    }
    if candidates.is_empty() && list_heads.is_empty() {
        return Ok(());
    }

    // Resolve any RDF-collection heads against this transaction's own flakes.
    if !list_heads.is_empty() {
        if let (Some(first_p), Some(rest_p)) = (
            snapshot.encode_iri(fluree_vocab::rdf::FIRST),
            snapshot.encode_iri(fluree_vocab::rdf::REST),
        ) {
            let mut first_of: HashMap<Sid, FlakeValue> = HashMap::new();
            let mut rest_of: HashMap<Sid, Sid> = HashMap::new();
            for f in flakes {
                if !f.op {
                    continue;
                }
                if f.p == first_p {
                    first_of.entry(f.s.clone()).or_insert_with(|| f.o.clone());
                } else if f.p == rest_p {
                    if let FlakeValue::Ref(next) = &f.o {
                        rest_of.entry(f.s.clone()).or_insert_with(|| next.clone());
                    }
                }
            }
            for head in list_heads {
                // A ref that is not a list node is a direct mode IRI object.
                if !first_of.contains_key(&head) {
                    if let Some(iri) = snapshot.decode_sid(&head) {
                        candidates.push(iri);
                    }
                    continue;
                }
                let mut node = head;
                for _ in 0..MAX_STAGED_REASONING_LIST_LEN {
                    match first_of.get(&node) {
                        Some(FlakeValue::String(s)) => candidates.push(s.to_string()),
                        Some(FlakeValue::Ref(sid)) => {
                            if let Some(iri) = snapshot.decode_sid(sid) {
                                candidates.push(iri);
                            }
                        }
                        _ => {}
                    }
                    match rest_of.get(&node) {
                        Some(next) => node = next.clone(),
                        None => break,
                    }
                }
            }
        }
    }

    fluree_db_query::ir::ReasoningModes::validate_mode_names(&candidates).map_err(|e| {
        TransactError::Parse(format!("invalid f:reasoningModes in ledger #config: {e}"))
    })
}

/// Names terms and graphs for messages.
struct Names<'a> {
    ns: &'a NamespaceRegistry,
    graph_delta: &'a FxHashMap<u16, String>,
    view: &'a StagedLedger,
    config_graph: String,
}

impl<'a> Names<'a> {
    fn new(
        view: &'a StagedLedger,
        ns: &'a NamespaceRegistry,
        graph_delta: &'a FxHashMap<u16, String>,
    ) -> Self {
        Self {
            ns,
            graph_delta,
            view,
            config_graph: config_graph_iri(&view.base().snapshot.ledger_id),
        }
    }

    /// `f:name` for Fluree vocabulary, else the full IRI in angle brackets.
    fn term(&self, sid: &Sid) -> String {
        if sid.namespace_code == FLUREE_DB {
            return format!("f:{}", sid.name);
        }
        match self.ns.get_prefix(sid.namespace_code) {
            Some(prefix) => format!("<{prefix}{}>", sid.name),
            None => format!("<{}>", sid.name),
        }
    }

    fn graph(&self, g_id: GraphId) -> String {
        if g_id == fluree_db_core::DEFAULT_GRAPH_ID {
            return "the default graph".to_string();
        }
        match self.graph_delta.get(&g_id).map(String::as_str).or_else(|| {
            self.view
                .base()
                .snapshot
                .graph_registry
                .iri_for_graph_id(g_id)
        }) {
            Some(iri) => format!("graph <{iri}>"),
            None => format!("graph {g_id}"),
        }
    }
}
