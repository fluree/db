use crate::context::{self, build_fluree};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::ReindexOptions;

/// Run incremental indexing for a ledger.
///
/// Uses `build_index_for_ledger` which tries incremental indexing first
/// (merges only new commits into the existing index), falling back to
/// a full rebuild when incremental isn't possible.
pub async fn run_index(ledger: Option<&str>, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(ledger, dirs)?;
    let fluree = build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    // Verify ledger exists
    if !fluree.ledger_exists(&ledger_id).await.unwrap_or(false) {
        return Err(CliError::NotFound(format!("ledger '{alias}' not found")));
    }

    eprintln!("  {} indexing {}...", "index:".cyan().bold(), alias);

    // Attach the api-side full-text config provider so each incremental
    // build picks up `f:fullTextDefaults` changes — otherwise configured
    // plain-string values written since the last reindex wouldn't flow
    // into BM25 arenas.
    let config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());

    let result = fluree_db_indexer::build_index_for_ledger(
        fluree.content_store(&ledger_id),
        fluree.nameservice(),
        &ledger_id,
        config,
    )
    .await
    .map_err(|e| CliError::Import(format!("indexing failed: {e}")))?;

    // Publish the new index
    fluree
        .nameservice_mode()
        .publisher()
        .ok_or_else(|| {
            CliError::Config("write operations require a read-write nameservice".into())
        })?
        .publish_index_allow_equal(&ledger_id, result.index_t, &result.root_id)
        .await
        .map_err(|e| CliError::Import(format!("failed to publish index: {e}")))?;

    println!(
        "Indexed {} to t={} (root: {})",
        alias, result.index_t, result.root_id
    );

    Ok(())
}

/// Run a full reindex (rebuild from commit history) for a ledger.
pub async fn run_reindex(ledger: Option<&str>, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(ledger, dirs)?;
    let fluree = build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    // Verify ledger exists
    if !fluree.ledger_exists(&ledger_id).await.unwrap_or(false) {
        return Err(CliError::NotFound(format!("ledger '{alias}' not found")));
    }

    eprintln!(
        "  {} rebuilding index for {} from commit history...",
        "reindex:".cyan().bold(),
        alias
    );

    let result = fluree
        .reindex(&ledger_id, ReindexOptions::default())
        .await?;

    println!(
        "Reindexed {} to t={} (root: {})",
        alias, result.index_t, result.root_id
    );

    Ok(())
}
