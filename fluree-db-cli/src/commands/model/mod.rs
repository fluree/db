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

use crate::cli::ModelAction;
use crate::error::CliResult;
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run(action: &ModelAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        ModelAction::Access { action } => access::run(action, dirs, direct).await,
    }
}
