//! The union default graph: a query can read the ledger's default graph
//! together with its named graphs as one default graph, switched per request
//! (`# PRAGMA union-default-graph`, `opts.unionDefaultGraph`) or per ledger
//! (`f:queryDefaults` / `f:unionDefaultGraph`).
//!
//! The union executes as a dataset of the ledger's own graphs, the one a
//! `FROM` naming each of them would build, so it has that path's set
//! semantics: a triple held by two graphs is one triple of the union.

use std::sync::Arc;

use fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID;
use fluree_db_core::{GraphId, DEFAULT_GRAPH_ID, DEFAULT_GRAPH_IRI};
use fluree_db_query::{DataSet, ExecutableQuery, GraphRef};

use crate::view::{DataSetDb, GraphDb};
use crate::{Fluree, Result};

/// `view`'s user named graphs: every registered graph except the reserved
/// `#txn-meta` / `#config` graphs and a graph registered under a name of the
/// default graph (the ledger alias, `urn:default`). The set `GRAPH ?g` ranges
/// over.
fn user_graphs(view: &GraphDb) -> impl Iterator<Item = (GraphId, &str)> {
    let alias = view.snapshot.ledger_id.as_str();
    view.snapshot
        .graph_registry
        .iter_entries()
        .filter(move |(g_id, iri)| {
            *g_id >= FIRST_USER_GRAPH_ID && *iri != alias && *iri != DEFAULT_GRAPH_IRI
        })
}

/// `view`'s graph `g_id` as a dataset member, under `view`'s policy.
fn member(view: &GraphDb, g_id: GraphId) -> GraphRef<'_> {
    let mut graph = GraphRef::new(
        view.snapshot.as_ref(),
        g_id,
        view.overlay.as_ref(),
        view.t,
        Arc::clone(&view.ledger_id),
    );
    graph.policy_enforcer = view.policy_enforcer().cloned();
    graph
}

impl Fluree {
    /// Whether a query on `view` reads its default graph as the union of the
    /// ledger's default graph and its user named graphs.
    ///
    /// The request's own switch (`requested`) decides first, then the view's
    /// (see [`GraphDb::with_union_default_graph`]), then the ledger's
    /// `f:unionDefaultGraph`; unset everywhere, it is off. Only a native
    /// ledger's default graph unions, and only with a named graph to union
    /// with, so a ledger without named graphs never leaves the single-graph
    /// path.
    pub(crate) async fn reads_union_default_graph(
        &self,
        view: &GraphDb,
        requested: Option<bool>,
    ) -> Result<bool> {
        if view.graph_id != DEFAULT_GRAPH_ID
            || view.graph_source_id.is_some()
            || user_graphs(view).next().is_none()
        {
            return Ok(false);
        }
        if let Some(on) = requested.or(view.union_default_graph) {
            return Ok(on);
        }
        let config = if view.config_is_resolved() {
            view.ledger_config.clone()
        } else {
            crate::policy_view::resolve_ledger_config_cached(
                self,
                &view.snapshot,
                &*view.overlay,
                view.novelty_for_stats(),
                view.t,
            )
            .await?
        };
        Ok(config
            .as_ref()
            .and_then(|c| c.query.as_ref())
            .and_then(|q| q.union_default_graph)
            .unwrap_or(false))
    }

    /// The runtime dataset `dataset` executes against, with each default-graph
    /// member that is a ledger's default graph widened to that ledger's union
    /// when a query on it reads one (see [`Self::reads_union_default_graph`]).
    ///
    /// A member that names a graph of its own (`ledger#graph`, a named-graph
    /// IRI) is taken as named, and so is every member of that ledger: a default
    /// graph that names one of a ledger's graphs chose that ledger's graphs
    /// itself. The named graphs stay exactly those the query named. A history
    /// range reads the default graph alone.
    pub(crate) async fn runtime_dataset<'a>(
        &self,
        dataset: &'a DataSetDb,
        requested: Option<bool>,
    ) -> Result<DataSet<'a>> {
        let names_a_graph: std::collections::HashSet<&str> = dataset
            .default
            .iter()
            .filter(|v| v.graph_id != DEFAULT_GRAPH_ID)
            .map(|v| v.ledger_id.as_ref())
            .collect();
        let mut widened = Vec::with_capacity(dataset.default.len());
        for view in &dataset.default {
            widened.push(
                !dataset.is_history_mode()
                    && !names_a_graph.contains(view.ledger_id.as_ref())
                    && self.reads_union_default_graph(view, requested).await?,
            );
        }
        let mut ds = dataset.as_runtime_dataset();
        if !widened.contains(&true) {
            return Ok(ds);
        }
        let mut members: std::collections::HashSet<(&str, GraphId, i64)> = dataset
            .default
            .iter()
            .map(|v| (v.ledger_id.as_ref(), v.graph_id, v.t))
            .collect();
        for (view, _) in dataset.default.iter().zip(widened).filter(|(_, w)| *w) {
            for (g_id, _) in user_graphs(view) {
                if members.insert((view.ledger_id.as_ref(), g_id, view.t)) {
                    ds = ds.with_default_graph(member(view, g_id));
                }
            }
        }
        Ok(ds)
    }
}

/// The runtime dataset a query on `view` executes against when `executable`
/// reads the union default graph, or `None` when it reads `view`'s graph
/// alone. [`Fluree::build_executable_for_view`] settles which.
///
/// The ledger's default graph and each user named graph are default-graph
/// members. Each named graph stays addressable by `GRAPH`, and the ledger alias
/// and `urn:default` still name the default graph alone, as without the union:
/// the dataset is [implicit](DataSet::implicit), so only default-graph patterns
/// read it differently.
pub(crate) fn union_default_dataset<'a>(
    view: &'a GraphDb,
    executable: &ExecutableQuery,
) -> Option<DataSet<'a>> {
    if executable.query.union_default_graph != Some(true) {
        return None;
    }
    let mut ds = DataSet::new()
        .implicit()
        .with_default_graph(member(view, DEFAULT_GRAPH_ID))
        .with_named_graph_alias(
            view.snapshot.ledger_id.as_str(),
            member(view, DEFAULT_GRAPH_ID),
        )
        .with_named_graph_alias(DEFAULT_GRAPH_IRI, member(view, DEFAULT_GRAPH_ID));
    for (g_id, iri) in user_graphs(view) {
        ds = ds
            .with_default_graph(member(view, g_id))
            .with_named_graph(iri, member(view, g_id));
    }
    Some(ds)
}
