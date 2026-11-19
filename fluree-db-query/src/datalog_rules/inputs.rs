//! What a rule program can read, checked before it runs.
//!
//! Rule bodies run against one graph of one ledger, the graph the query's
//! default graph resolves to, with no dataset and no graph-source reader
//! (`execute.rs`, `apply_with_pair_limit`). In that context a `GRAPH <iri>`
//! naming anything other than a graph of that ledger yields no rows, which is
//! SPARQL's answer for a graph outside the dataset but a wrong one for a rule:
//! under a negation an empty input reads as "absent for everyone", so a typo'd
//! graph IRI, another ledger or a graph source derives a full, plausible set of
//! conclusions. The same holds for a default graph that merges several graphs:
//! the rules read one of them, while a plain query over the same default graph
//! reads them all.
//!
//! Rule heads already refuse a graph the ledger does not have
//! (`HeadGraphs::resolve`); [`refuse_unreadable_inputs`] does the same for what
//! the bodies read, and for the default graph they read it over.

use super::DatalogRuleSet;
use crate::context::{single_ledger_graph, SingleLedgerGraph, UnreadGraph};
use crate::error::{QueryError, Result};
use crate::r2rml::R2rmlProvider;
use fluree_db_core::{DatasetRef, GraphDbRef, CONFIG_GRAPH_ID, TXN_META_GRAPH_ID};

/// What the query a rule program runs for reads, beyond the one graph the
/// program runs against.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuleInputScope<'a> {
    /// The query's default graph merges two or more graphs (several ledgers,
    /// or several graphs of one ledger), of which the rules would read one.
    pub multi_default_graph: bool,
    /// The reader the query resolves graph sources through, if it has one.
    /// Asked only to say, in a refusal, that a name is a graph source; the
    /// refusal does not depend on it.
    pub graph_sources: Option<&'a dyn R2rmlProvider>,
}

/// Refuse a rule program that would read something it cannot see: a default
/// graph of several graphs, or a body `GRAPH <iri>` (at any depth) naming
/// anything but a graph of the ledger the rules run against, as of the `t`
/// they run at. That is its default graph (the ledger's own address, in any
/// spelling) or a user named graph it has as of `t`, exactly the names the
/// graph operator reads ([`single_ledger_graph`]). `GRAPH ?g` is not
/// affected: it ranges over the ledger's named graphs.
///
/// A graph a commit after `t` dropped fails as the graph operator fails it
/// ([`QueryError::GraphDropped`]). An empty rule set reads nothing, so
/// nothing is refused.
pub async fn refuse_unreadable_inputs(
    rule_set: &DatalogRuleSet,
    db: GraphDbRef<'_>,
    scope: RuleInputScope<'_>,
) -> Result<()> {
    if rule_set.is_empty() {
        return Ok(());
    }
    if scope.multi_default_graph {
        return Err(several_default_graphs(rule_set, db));
    }
    for rule in rule_set.iter_in_order() {
        for iri in &rule.footprint.read_graphs {
            match single_ledger_graph(db.snapshot, db.t, iri) {
                SingleLedgerGraph::Default | SingleLedgerGraph::User(_) => {}
                SingleLedgerGraph::DroppedAfter { floor, .. } => {
                    return Err(QueryError::GraphDropped {
                        graph: iri.to_string(),
                        floor,
                        t: db.t,
                    });
                }
                SingleLedgerGraph::Unread(unread) => {
                    let why = why_unreadable(iri, unread, db, scope.graph_sources).await;
                    return Err(QueryError::InvalidQuery(format!(
                        "datalog rule {} reads graph <{iri}>, {why}",
                        rule.name
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Why a body cannot read `iri` ([`single_ledger_graph`] read it as
/// `unread`): the clause after "reads graph <iri>, ".
async fn why_unreadable(
    iri: &str,
    unread: UnreadGraph,
    db: GraphDbRef<'_>,
    graph_sources: Option<&dyn R2rmlProvider>,
) -> String {
    let ledger = &db.snapshot.ledger_id;
    let only_own = format!(
        "A rule body reads only the graphs of the ledger its rules run against ('{ledger}')"
    );
    match unread {
        UnreadGraph::System(g_id) => {
            let role = match g_id {
                CONFIG_GRAPH_ID => "the config graph",
                TXN_META_GRAPH_ID => "the txn-meta graph",
                _ => "a system graph",
            };
            format!(
                "which is {role} of ledger '{ledger}'. A rule body reads the ledger's default \
                 graph and its user named graphs, not its system graphs"
            )
        }
        UnreadGraph::NotAGraph => format!(
            "which names ledger '{ledger}' itself (at a time, or as a graph of itself) \
             rather than one of its graphs. {only_own}, as of the query's t"
        ),
        UnreadGraph::OtherLedgersSystemGraph => {
            format!("which is a system graph of another ledger. {only_own}")
        }
        UnreadGraph::Dropped(_) | UnreadGraph::New => {
            if names_graph_source(iri, graph_sources).await {
                return format!("which is a graph source. {only_own}, not graph sources");
            }
            match DatasetRef::parse(iri) {
                Ok(DatasetRef::Address(address)) if address.id() != ledger => format!(
                    "which names another ledger ('{}'). {only_own}",
                    address.id()
                ),
                Ok(DatasetRef::Ambiguous { address, .. }) if address.id() != ledger => format!(
                    "which ledger '{ledger}' does not have as of t={}, and as a ledger address \
                     names another ledger ('{}'). {only_own}",
                    db.t,
                    address.id()
                ),
                _ => format!(
                    "which ledger '{ledger}' does not have as of t={}. {only_own}, and a named \
                     graph exists once data is transacted into it",
                    db.t
                ),
            }
        }
    }
}

/// Whether `iri` names a registered graph source, readable as a graph or not.
/// A lookup that cannot say (no reader, or a nameservice error) is `false`:
/// the body is refused either way, and only the message differs.
async fn names_graph_source(iri: &str, graph_sources: Option<&dyn R2rmlProvider>) -> bool {
    let Some(provider) = graph_sources else {
        return false;
    };
    match provider.has_r2rml_mapping(iri).await {
        Ok(mapped) => mapped,
        Err(e) => e.is_unreadable_graph_source(),
    }
}

/// The refusal for a rule program over a default graph of several graphs.
fn several_default_graphs(rule_set: &DatalogRuleSet, db: GraphDbRef<'_>) -> QueryError {
    const NAMED: usize = 3;
    let mut names: Vec<&str> = rule_set
        .iter_in_order()
        .take(NAMED)
        .map(|r| r.name.as_str())
        .collect();
    if rule_set.len() > NAMED {
        names.push("…");
    }
    QueryError::InvalidQuery(format!(
        "this query's default graph merges several graphs, but datalog rules ({}) run over \
         one graph: they would read only ledger '{}' and derive as if the other graphs' \
         facts were absent. Run the rules over a default graph of one graph; a rule body can \
         read the same ledger's named graphs with GRAPH <iri>",
        names.join(", "),
        db.snapshot.ledger_id
    ))
}
