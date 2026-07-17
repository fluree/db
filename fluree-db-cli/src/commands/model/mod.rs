//! `fluree model` — governance model tooling.
//!
//! A governance model has three facets: **entity** (SHACL shapes — what
//! things are), **access** (policies — who may do what), and **reasoning**
//! (RDFS hierarchy — what follows). This module hosts the facet
//! subcommands; v1 ships the access facet.
//!
//! Architecture: commands here are **compilers to data** — they transform
//! declared intent into ordinary JSON-LD transactions and queries against
//! the target ledger. There is no bespoke server API behind them, so they
//! work identically against local ledgers, `fluree-db-server`, and hosted
//! stacks.

pub mod access;
pub mod class;
pub mod entity;

use crate::cli::ModelAction;
use crate::context::{self, LedgerMode};
use crate::error::CliResult;
use fluree_db_api::server_defaults::FlureeDir;
use serde_json::Value;

pub async fn run(action: &ModelAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        ModelAction::Access { action } => access::run(action, dirs, direct).await,
        ModelAction::Entity { action } => entity::run(action, dirs, direct).await,
        ModelAction::Class { action } => class::run(action, dirs, direct).await,
    }
}

// ── mode-agnostic ledger IO ─────────────────────────────────────────────

pub(crate) async fn resolve_mode(
    dataset: &str,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<LedgerMode> {
    if let Some(remote_name) = remote {
        let alias = context::resolve_ledger(Some(dataset), dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await
    } else {
        let mode = context::resolve_ledger_mode(Some(dataset), dirs).await?;
        Ok(if direct {
            mode
        } else {
            context::try_server_route(mode, dirs)
        })
    }
}

/// Run a JSON-LD query in either mode, returning the JSON-LD result value.
pub(crate) async fn query(mode: &LedgerMode, body: &Value) -> CliResult<Value> {
    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            ..
        } => Ok(client.query_jsonld(remote_alias, body).await?),
        LedgerMode::Local { fluree, alias } => {
            let view = fluree.db_with_default_context(alias).await?;
            let result = fluree.query(&view, body).await?;
            Ok(result.to_jsonld_async(view.as_graph_db_ref()).await?)
        }
    }
}

/// Upsert JSON-LD in either mode (replace-listed-properties semantics; used
/// for nodes shared with other authorship, where unlisted properties must
/// survive).
pub(crate) async fn upsert(mode: &LedgerMode, body: &Value) -> CliResult<()> {
    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            ..
        } => {
            client.upsert_jsonld(remote_alias, body).await?;
        }
        LedgerMode::Local { fluree, alias } => {
            fluree.graph(alias).transact().upsert(body).commit().await?;
        }
    }
    Ok(())
}

/// Run a WHERE/DELETE/INSERT update in either mode.
pub(crate) async fn update(mode: &LedgerMode, body: &Value) -> CliResult<()> {
    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            ..
        } => {
            client.update_jsonld(remote_alias, body).await?;
        }
        LedgerMode::Local { fluree, alias } => {
            fluree.graph(alias).transact().update(body).commit().await?;
        }
    }
    Ok(())
}

/// Install compiled nodes with full-replace semantics for the ids the
/// compiler OWNS: in one atomic transaction, wildcard-delete every owned
/// node and insert the compiled graph. Owned nodes end up exactly as
/// compiled — a property from a previous compilation cannot linger (the
/// policy loader gives `f:allow` precedence over `f:query`, so a stale
/// `f:allow: true` would silently disable a newly compiled gate). Nodes in
/// the graph that are NOT listed as owned are inserted additively.
pub(crate) async fn replace_nodes(
    mode: &LedgerMode,
    graph: &Value,
    owned_ids: &[&str],
) -> CliResult<()> {
    update(mode, &replace_txn(graph, owned_ids)).await
}

/// Pure builder for [`replace_nodes`]'s transaction: optional wildcard
/// match + delete per owned id (delete-if-exists — unbound rows skip their
/// delete template), then insert the compiled nodes.
pub(crate) fn replace_txn(graph: &Value, owned_ids: &[&str]) -> Value {
    let mut where_clause: Vec<Value> = Vec::new();
    let mut delete: Vec<Value> = Vec::new();
    for (i, id) in owned_ids.iter().enumerate() {
        let pattern = serde_json::json!({"@id": id, format!("?p{i}"): format!("?o{i}")});
        where_clause.push(serde_json::json!(["optional", pattern]));
        delete.push(pattern);
    }
    let insert = graph
        .get("@graph")
        .cloned()
        .unwrap_or_else(|| graph.clone());
    serde_json::json!({
        "where": where_clause,
        "delete": delete,
        "insert": insert,
    })
}

#[cfg(test)]
mod tests {
    use super::replace_txn;
    use serde_json::json;

    #[test]
    fn replace_txn_wildcard_deletes_each_owned_node() {
        let graph = json!({"@graph": [
            {"@id": "https://x/A", "https://x/p": 1},
            {"@id": "https://x/B", "https://x/p": 2},
        ]});
        let txn = replace_txn(&graph, &["https://x/A", "https://x/B"]);

        let where_clause = txn["where"].as_array().unwrap();
        assert_eq!(where_clause.len(), 2);
        // Every owned id gets a delete-if-exists: optional wildcard match
        // with per-node variables, mirrored in the delete templates.
        assert_eq!(where_clause[0][0], "optional");
        assert_eq!(where_clause[0][1]["@id"], "https://x/A");
        assert_eq!(where_clause[0][1]["?p0"], "?o0");
        assert_eq!(where_clause[1][1]["?p1"], "?o1");

        let delete = txn["delete"].as_array().unwrap();
        assert_eq!(delete[0]["@id"], "https://x/A");
        assert_eq!(delete[0]["?p0"], "?o0");
        assert_eq!(delete[1]["@id"], "https://x/B");
        assert_eq!(delete[1]["?p1"], "?o1");

        assert_eq!(txn["insert"], graph["@graph"]);
    }

    #[test]
    fn replace_txn_can_own_ids_absent_from_the_graph() {
        // Switching profiles must revoke the node the new profile no longer
        // emits (e.g. write → read drops {class}/write): owned ids are
        // deleted even when nothing in the graph reinserts them.
        let graph = json!({"@graph": [{"@id": "https://x/view", "https://x/p": 1}]});
        let txn = replace_txn(&graph, &["https://x/view", "https://x/write"]);
        assert_eq!(txn["delete"].as_array().unwrap().len(), 2);
        assert_eq!(txn["delete"][1]["@id"], "https://x/write");
        assert_eq!(txn["insert"].as_array().unwrap().len(), 1);
    }
}
