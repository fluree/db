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
//! The checks: R1, a config group split across graphs (in one transaction or
//! across two); R2, an `f:LedgerConfig`/`f:GraphConfig` typed outside the
//! config graph, or config fields on the conventional config subject
//! (`urn:fluree:<ledger>:config:ledger`) written there; R3, a second value for a single-valued setting or a second
//! `f:LedgerConfig` subject; and unrecognized `f:reasoningModes` values.
//!
//! The guard reads only the staged flakes and the pre-transaction config
//! graph. It resolves no shapes, schema, constraints or policy artifact, so a
//! config repair is judged by it alone. It runs every check where transactions
//! are authored (JSON-LD and SPARQL transactions, Turtle insert). R2 is
//! enforced when the data is written; branch operations (merge, rebase,
//! revert) re-check the integrity of the target's config groups (R1/R3).
//! Commit replay and bulk import carry already-authored history and skip it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::config_vocab::{
    conventional_config_subject, is_config_field, CONFIG_CLASSES, CONFIG_EDGES, SINGLE_VALUED,
};
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

/// Maximum RDF-list length walked when validating a staged `f:reasoningModes`
/// collection — a malformed cyclic list must not spin.
const MAX_STAGED_REASONING_LIST_LEN: usize = 64;

/// Which checks the guard runs, by what is writing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GuardScope {
    /// A transaction authoring the write: every check.
    Authoring,
    /// A branch operation (`merge`, `rebase`, `revert`, named here for
    /// messages) staging commits onto its target. R2 is enforced when the
    /// data is written; branch operations re-check the integrity of the
    /// target's config groups (R1/R3).
    BranchOperation(&'static str),
}

/// Why the guard did not accept a staged write.
#[derive(Debug)]
pub(crate) enum GuardError {
    /// The staged state breaks a config rule; the message says which.
    Refused(String),
    /// The guard could not read what it checks against.
    Failed(TransactError),
}

impl From<TransactError> for GuardError {
    fn from(error: TransactError) -> Self {
        GuardError::Failed(error)
    }
}

/// Why a node is part of the configuration, so that its fields belong in
/// the config graph: it is the value of a config edge there, or a subject
/// typed `f:LedgerConfig` / `f:GraphConfig` there.
#[derive(Clone, Debug)]
enum Link {
    Edge(Sid),
    Typed(Sid),
}

impl Link {
    /// "config group X (the value of f:p in the config graph)", or
    /// "config subject X (typed f:LedgerConfig in the config graph)".
    fn node(&self, names: &Names<'_>, node: &Sid) -> String {
        match self {
            Link::Edge(edge) => format!(
                "config group {} (the value of {} in the config graph)",
                names.term(node),
                names.term(edge)
            ),
            Link::Typed(class) => format!(
                "config subject {} (typed {} in the config graph)",
                names.term(node),
                names.term(class)
            ),
        }
    }
}

/// The links a staged flake in the config graph makes: `Some((node, link))`
/// for an asserted or retracted config edge or config typing.
fn link_of<'f>(
    flake: &'f Flake,
    edges: &[Sid],
    classes: &[Sid],
    rdf_type: &Sid,
) -> Option<(&'f Sid, Link)> {
    let FlakeValue::Ref(object) = &flake.o else {
        return None;
    };
    if edges.contains(&flake.p) {
        Some((object, Link::Edge(flake.p.clone())))
    } else if flake.p == *rdf_type && classes.contains(object) {
        Some((&flake.s, Link::Typed(object.clone())))
    } else {
        None
    }
}

fn config_link_terms() -> (Vec<Sid>, Vec<Sid>, Sid) {
    (
        CONFIG_EDGES.iter().map(|iri| fluree_sid(iri)).collect(),
        CONFIG_CLASSES.iter().map(|iri| fluree_sid(iri)).collect(),
        Sid::new(RDF, fluree_vocab::rdf_names::TYPE),
    )
}

/// What a branch operation is told when it would split one of the target's
/// config nodes: the node, and the repair. Never the graph the fields would
/// land in.
fn split_group_for_branch_op(
    names: &Names<'_>,
    op: &str,
    node: &Sid,
    link: &Link,
    fields: &str,
) -> String {
    format!(
        "{} would have fields {} outside the config graph, where they are not read; a {op} \
         may not split a config group: repair the configuration first (see \"Repairing a \
         config split across graphs\" in the ledger config docs)",
        link.node(names, node),
        fields,
    )
}

/// Check what a staged transaction writes into the ledger config.
///
/// Refuses (a transaction gets the message as a `Parse error`):
/// - a config group split across graphs: a config node (the value of a
///   config edge in the config graph, or a subject typed `f:LedgerConfig` /
///   `f:GraphConfig` there) that gets config fields ([`is_config_field`]) in
///   another graph, in this transaction or across two;
/// - an `f:LedgerConfig` or `f:GraphConfig` typed outside the config graph,
///   or a config field written there on the conventional config subject
///   (`urn:fluree:<ledger>:config:ledger`), which has no effect;
/// - a second value for a single-valued config setting, or a second
///   `f:LedgerConfig` subject, counting what the config graph already holds;
/// - an unrecognized `f:reasoningModes` value.
///
/// A plain data transaction writes nothing to the config graph, types nothing
/// with an `f:` class, asserts no reasoning modes and writes no config field
/// ([`is_config_field`]) in a user graph: one pass over the staged flakes
/// establishes that, and nothing else is read. Edge annotations, uniqueness
/// annotations and policy documents are such data. A config field can also
/// be ordinary data: `f:policyClass` on an identity. A write of one outside
/// the config graph reads the config graph (small) to learn whether its
/// subject is a config group, unless the config graph holds no data at all.
///
/// `scope` picks the checks: a branch operation runs R1 and R3 only, and its
/// messages name the target's config group and the repair. `ns` and
/// `graph_delta` name terms and graphs in messages.
pub(crate) async fn validate_staged_config(
    view: &StagedLedger,
    ns: &NamespaceRegistry,
    graph_delta: &FxHashMap<GraphId, String>,
    scope: GuardScope,
) -> Result<(), GuardError> {
    let authoring = scope == GuardScope::Authoring;
    let rdf_type = Sid::new(RDF, fluree_vocab::rdf_names::TYPE);
    let modes_p = fluree_sid(config_iris::REASONING_MODES);
    let (mut writes_config, mut types_config, mut asserts_modes) = (false, false, false);
    let mut writes_fields_elsewhere = false;
    for (g_id, flake) in view.staged_flakes_by_graph() {
        writes_config |= g_id == CONFIG_GRAPH_ID;
        types_config |= flake.op
            && flake.p == rdf_type
            && matches!(&flake.o, FlakeValue::Ref(o) if o.namespace_code == FLUREE_DB);
        asserts_modes |= flake.op && flake.p == modes_p;
        writes_fields_elsewhere |=
            flake.op && is_config_field(&flake.p) && !crate::export::is_system_graph(g_id);
    }
    let (types_config, asserts_modes) = (authoring && types_config, authoring && asserts_modes);
    if !(writes_config || types_config || asserts_modes || writes_fields_elsewhere) {
        return Ok(());
    }

    let names = Names::new(view, ns, graph_delta);
    if types_config {
        refuse_config_typed_outside_config_graph(view, &names, &rdf_type)?;
    }
    if writes_config {
        refuse_split_groups(view, &names, scope)?;
        refuse_second_values(view, &names, &rdf_type, scope).await?;
    }
    if writes_config || writes_fields_elsewhere {
        refuse_groups_split_across_transactions(view, &names, scope).await?;
    }
    if authoring && writes_fields_elsewhere {
        refuse_config_subject_fields_outside_config_graph(view, &names)?;
    }
    if asserts_modes {
        validate_reasoning_modes(view, &modes_p)?;
    }
    Ok(())
}

/// The branch (and the graph's role, `config` or `txn-meta`) when `iri` is
/// one of this ledger's system graphs on a branch other than `ledger_id`'s:
/// `urn:fluree:<name>:<branch>#config` or `…#txn-meta`. A branch's system
/// graphs are named for the branch, so on another branch they are ordinary
/// graphs, never its configuration or commit metadata. The IRI is read by the
/// address grammar's own lexer and parser, and the role by the one keyword
/// table.
pub(crate) fn other_branch_system_graph<'i>(
    iri: &'i str,
    ledger_id: &str,
) -> Option<(&'i str, &'i str)> {
    use fluree_db_core::{AddressText, GraphSel, LedgerId, LedgerRef};
    let text = AddressText::split(iri);
    let role = text.graph()?;
    if !text.is_urn()
        || text.pin().is_some()
        || !matches!(
            GraphSel::bare_keyword(role),
            Some(GraphSel::Config | GraphSel::TxnMeta)
        )
    {
        return None;
    }
    let (_, branch) = text.ledger().split_once(':')?;
    let address = LedgerRef::parse(iri).ok()?;
    let own = LedgerId::parse(ledger_id).ok()?;
    (address.id().name() == own.name() && address.id().branch() != own.branch())
        .then_some((branch, role))
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
) -> Result<(), GuardError> {
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
                return Err(GuardError::Refused(format!(
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

/// Config fields written outside the config graph on the conventional config
/// subject, `urn:fluree:<ledger>:config:ledger`. Its IRI names it the config
/// subject whether or not the config graph holds it yet, so the fields are a
/// config written to the wrong graph, which the reader never sees. Needs no
/// read: when the config graph already holds the subject,
/// [`refuse_groups_split_across_transactions`] has refused the write first.
fn refuse_config_subject_fields_outside_config_graph(
    view: &StagedLedger,
    names: &Names<'_>,
) -> Result<(), GuardError> {
    let subject = conventional_config_subject(&view.base().snapshot.ledger_id);
    let Some(subject) = names.ns.lookup_sid_for_iri(&subject) else {
        return Ok(());
    };
    let mut stray: BTreeMap<GraphId, BTreeSet<&Sid>> = BTreeMap::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if flake.op
            && flake.s == subject
            && is_config_field(&flake.p)
            && !crate::export::is_system_graph(g_id)
        {
            stray.entry(g_id).or_default().insert(&flake.p);
        }
    }
    match stray.into_iter().next() {
        None => Ok(()),
        Some((g_id, fields)) => Err(GuardError::Refused(format!(
            "the config subject {} would get its fields {} in {}; config is read only from the \
             config graph <{}>, from a subject typed f:LedgerConfig there, so write it into \
             that graph",
            names.term(&subject),
            fields
                .into_iter()
                .map(|p| names.term(p))
                .collect::<Vec<_>>()
                .join(", "),
            names.graph(g_id),
            names.config_graph,
        ))),
    }
}

/// A config node this transaction links in the config graph (a config edge
/// to it, or its typing as `f:LedgerConfig` / `f:GraphConfig`) whose config
/// fields it writes to another graph: the reader sees the node without them.
fn refuse_split_groups(
    view: &StagedLedger,
    names: &Names<'_>,
    scope: GuardScope,
) -> Result<(), GuardError> {
    let (edges, classes, rdf_type) = config_link_terms();
    let mut nodes: HashMap<&Sid, Link> = HashMap::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if flake.op && g_id == CONFIG_GRAPH_ID {
            if let Some((node, link)) = link_of(flake, &edges, &classes, &rdf_type) {
                nodes.entry(node).or_insert(link);
            }
        }
    }
    if nodes.is_empty() {
        return Ok(());
    }
    let mut stray: BTreeMap<(&Sid, GraphId), Vec<&Sid>> = BTreeMap::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if flake.op
            && g_id != CONFIG_GRAPH_ID
            && is_config_field(&flake.p)
            && nodes.contains_key(&flake.s)
        {
            stray.entry((&flake.s, g_id)).or_default().push(&flake.p);
        }
    }
    match stray.into_iter().next() {
        None => Ok(()),
        Some(((node, g_id), fields)) => {
            let fields: BTreeSet<String> = fields.into_iter().map(|p| names.term(p)).collect();
            let fields = fields.into_iter().collect::<Vec<_>>().join(", ");
            Err(GuardError::Refused(match scope {
                GuardScope::Authoring => format!(
                    "{} has its fields {} in {}; config is read only from the config graph \
                     <{}>, so write its fields into that graph too",
                    nodes[node].node(names, node),
                    fields,
                    names.graph(g_id),
                    names.config_graph,
                ),
                GuardScope::BranchOperation(op) => {
                    split_group_for_branch_op(names, op, node, &nodes[node], &fields)
                }
            }))
        }
    }
}

/// A config node split across transactions, which [`refuse_split_groups`]
/// (seeing only this transaction) cannot catch: config fields
/// ([`is_config_field`]) this transaction writes outside the config graph for
/// a node the config graph already links (the value of a config edge, or a
/// subject typed `f:LedgerConfig` / `f:GraphConfig`), or a link it writes
/// into the config graph to a node that already has config fields in another
/// graph. Either way the reader would see the node without those fields: a
/// policy group's `f:defaultAllow false` lost, a SHACL group read as off, the
/// ledger's config subject without the group it was given.
///
/// Runs only for a transaction that writes the config graph or config
/// fields in a user graph, and reads only what it needs: the config graph
/// (small) once, and each newly linked node's statements in the user graphs.
/// Fields this transaction retracts (a repair moving them into the config
/// graph) do not count.
async fn refuse_groups_split_across_transactions(
    view: &StagedLedger,
    names: &Names<'_>,
    scope: GuardScope,
) -> Result<(), GuardError> {
    let (edges, classes, rdf_type) = config_link_terms();
    let mut new_links: BTreeMap<&Sid, Link> = BTreeMap::new();
    let mut unlinked: BTreeSet<(&Sid, &Sid, &FlakeValue)> = BTreeSet::new();
    let mut fields_elsewhere: BTreeMap<(&Sid, GraphId), BTreeSet<&Sid>> = BTreeMap::new();
    let mut retracted_elsewhere: Vec<(GraphId, &Sid, &Sid, &FlakeValue)> = Vec::new();
    for (g_id, flake) in view.staged_flakes_by_graph() {
        if g_id == CONFIG_GRAPH_ID {
            if let Some((node, link)) = link_of(flake, &edges, &classes, &rdf_type) {
                if flake.op {
                    new_links.entry(node).or_insert(link);
                } else {
                    unlinked.insert((&flake.s, &flake.p, &flake.o));
                }
            }
        } else if is_config_field(&flake.p) && !crate::export::is_system_graph(g_id) {
            if flake.op {
                fields_elsewhere
                    .entry((&flake.s, g_id))
                    .or_default()
                    .insert(&flake.p);
            } else {
                retracted_elsewhere.push((g_id, &flake.s, &flake.p, &flake.o));
            }
        }
    }
    let fields_list = |fields: &BTreeSet<&Sid>| {
        fields
            .iter()
            .map(|p| names.term(p))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Fields written elsewhere for a node the config graph already links.
    // The config graph is small: it is read once, and not at all when it
    // holds no data (a ledger without config writing `f:policyClass` on an
    // identity).
    let base = view.base();
    if !fields_elsewhere.is_empty()
        && crate::config_resolver::config_graph_may_hold_data(&base.snapshot, &*base.novelty)
    {
        let mut linked: HashMap<Sid, Link> = HashMap::new();
        for flake in config_graph_flakes(view, RangeMatch::new()).await? {
            if unlinked.contains(&(&flake.s, &flake.p, &flake.o)) {
                continue;
            }
            if let Some((node, link)) = link_of(&flake, &edges, &classes, &rdf_type) {
                linked.entry(node.clone()).or_insert(link);
            }
        }
        for ((node, g_id), fields) in &fields_elsewhere {
            if let Some(link) = linked.get(*node) {
                return Err(GuardError::Refused(match scope {
                    GuardScope::Authoring => format!(
                        "{} would get its fields {} in {}; config is read only from the config \
                         graph <{}>, so write its fields into that graph",
                        link.node(names, node),
                        fields_list(fields),
                        names.graph(*g_id),
                        names.config_graph,
                    ),
                    GuardScope::BranchOperation(op) => {
                        split_group_for_branch_op(names, op, node, link, &fields_list(fields))
                    }
                }));
            }
        }
    }

    // A new link to a node whose fields already sit in another graph.
    if new_links.is_empty() {
        return Ok(());
    }
    let registry = &view.base().snapshot.graph_registry;
    let user_graphs: BTreeSet<GraphId> = std::iter::once(fluree_db_core::DEFAULT_GRAPH_ID)
        .chain(registry.iter_entries().map(|(g_id, _)| g_id))
        .filter(|g_id| !crate::export::is_system_graph(*g_id))
        .collect();
    for (node, link) in new_links {
        for &g_id in &user_graphs {
            let statements = graph_flakes(
                view,
                g_id,
                RangeMatch {
                    s: Some(node.clone()),
                    ..Default::default()
                },
            )
            .await?;
            let fields: BTreeSet<&Sid> = statements
                .iter()
                .filter(|f| {
                    is_config_field(&f.p)
                        && !retracted_elsewhere.contains(&(g_id, &f.s, &f.p, &f.o))
                })
                .map(|f| &f.p)
                .collect();
            if !fields.is_empty() {
                return Err(GuardError::Refused(match scope {
                    GuardScope::Authoring => format!(
                        "{} already has fields {} in {}; config is read only from the config \
                         graph <{}>, so move them into that graph in the same transaction",
                        link.node(names, node),
                        fields_list(&fields),
                        names.graph(g_id),
                        names.config_graph,
                    ),
                    GuardScope::BranchOperation(op) => {
                        split_group_for_branch_op(names, op, node, &link, &fields_list(&fields))
                    }
                }));
            }
        }
    }
    Ok(())
}

/// A single-valued config setting with more than one value after this
/// transaction, or a second `f:LedgerConfig` subject.
async fn refuse_second_values(
    view: &StagedLedger,
    names: &Names<'_>,
    rdf_type: &Sid,
    scope: GuardScope,
) -> Result<(), GuardError> {
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
            let remedy = match scope {
                GuardScope::Authoring => {
                    "use upsert, or delete the old value in the same transaction".to_string()
                }
                GuardScope::BranchOperation(op) => format!(
                    "a {op} may not leave a setting with two values: keep one first (see \
                     \"What a config write is checked for\" in the ledger config docs)"
                ),
            };
            return Err(GuardError::Refused(format!(
                "{} would have {} values for {}, which takes one; {remedy}",
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
        .map_err(|e| GuardError::Failed(TransactError::ConfigUnavailable(e.to_string())))?
        .into_iter()
        .filter(|s| !untyped.contains(s))
        .collect();
        subjects.extend(typed.into_iter().cloned());
        if subjects.len() > 1 {
            let listed: Vec<String> = subjects.iter().map(|s| names.term(s)).collect();
            let remedy = match scope {
                GuardScope::Authoring => format!(
                    "so add settings to the existing subject, in the config graph <{}>",
                    names.config_graph
                ),
                GuardScope::BranchOperation(op) => format!(
                    "and a {op} may not add a second one: keep one first (see \"What a config \
                     write is checked for\" in the ledger config docs)"
                ),
            };
            return Err(GuardError::Refused(format!(
                "the config graph would hold {} f:LedgerConfig subjects ({}); a ledger has one, \
                 {remedy}",
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
    graph_flakes(view, CONFIG_GRAPH_ID, pattern).await
}

/// Graph `g_id`'s flakes matching `pattern` (by subject), as of the
/// pre-transaction state.
async fn graph_flakes(
    view: &StagedLedger,
    g_id: GraphId,
    pattern: RangeMatch,
) -> Result<Vec<Flake>, TransactError> {
    let base = view.base();
    range_with_overlay(
        &base.snapshot,
        g_id,
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
    .map_err(|e| TransactError::ConfigUnavailable(e.to_string()))
}

/// Reject a transaction that writes an unrecognized `f:reasoningModes` value.
///
/// Config reasoning modes are otherwise only parsed at query time, where an
/// unknown mode is warned-and-skipped — so a typo silently disables reasoning
/// with no signal. Handles the same value shapes as the config reader — a
/// direct string literal, a direct mode IRI, and an RDF collection of either —
/// collected from this transaction's own staged flakes.
fn validate_reasoning_modes(view: &StagedLedger, modes_p: &Sid) -> Result<(), GuardError> {
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
        GuardError::Refused(format!("invalid f:reasoningModes in ledger #config: {e}"))
    })
}

/// Names terms and graphs for messages.
struct Names<'a> {
    ns: &'a NamespaceRegistry,
    graph_delta: &'a FxHashMap<GraphId, String>,
    view: &'a StagedLedger,
    config_graph: String,
}

impl<'a> Names<'a> {
    fn new(
        view: &'a StagedLedger,
        ns: &'a NamespaceRegistry,
        graph_delta: &'a FxHashMap<GraphId, String>,
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
        match self.graph_iri(g_id) {
            Some(iri) => format!("graph <{iri}>"),
            None => format!("graph {g_id}"),
        }
    }

    /// A named graph's IRI: the write's own routing, else the registry.
    fn graph_iri(&self, g_id: GraphId) -> Option<&'a str> {
        self.graph_delta.get(&g_id).map(String::as_str).or_else(|| {
            self.view
                .base()
                .snapshot
                .graph_registry
                .iri_for_graph_id(g_id)
        })
    }
}
