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

/// Upsert JSON-LD in either mode (replace-listed-properties semantics keeps
/// `enable` idempotent when the property set changes).
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
