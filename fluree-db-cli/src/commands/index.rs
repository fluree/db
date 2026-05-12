use crate::context::{self, build_fluree, LedgerMode};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::wire::ReindexResponse;
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
    let mut config = fluree_db_indexer::IndexerConfig::default()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());

    // Resolve the attachment-events provider into a concrete
    // coverage envelope so an annotated ledger seals an
    // authoritative `f:reifies*` arena on this indexing run.
    // `build_index_for_ledger` reads `IndexerConfig.attachment_events`
    // directly — only the orchestrator's job dispatcher consults
    // the provider trait, so we have to resolve here. Without this,
    // the indexer takes the defensive-drop path (`annotation_index
    // = None`) and queries fall back to the M2a scan path —
    // correct but slower. The provider reads from the running
    // `LedgerManager`, so we cache the ledger first to make sure
    // its attachment overlay is loaded.
    //
    // **Sticky-bit gate.** Mirror `admin.rs::reindex`: only resolve
    // for ledgers that have actually observed a `f:reifies*` flake.
    // On non-annotation ledgers, going through the provider has been
    // observed to disturb novelty bookkeeping for unrelated facts
    // (regression caught by
    // `it_select_star_novelty_retract::expansion_applies_novelty_retractions`).
    // The CLI shared the same code shape pre-gate and was vulnerable
    // to the same regression; keep the two paths symmetric.
    if let Some(provider) = fluree.attachment_events_provider() {
        let handle = fluree.ledger_cached(&ledger_id).await.map_err(|e| {
            CliError::Import(format!("indexing failed: failed to load ledger: {e}"))
        })?;
        let view = handle.snapshot().await;
        let ledger_has_annotations =
            view.snapshot.has_annotations || view.novelty.attachments.has_annotations();
        if ledger_has_annotations {
            config.attachment_events = provider.attachment_events(&ledger_id).await;
        }
    }

    let cs = fluree
        .branched_content_store(&ledger_id)
        .await
        .map_err(|e| CliError::Import(format!("indexing failed: {e}")))?;

    let result =
        fluree_db_indexer::build_index_for_ledger(cs, fluree.nameservice(), &ledger_id, config)
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
///
/// With `--remote`, routes to the named remote's `POST /reindex` endpoint.
/// Without `--remote` but with a local server running, auto-routes to it
/// via `server.meta.json` (pass `--direct` to bypass).
pub async fn run_reindex(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.reindex(&alias).await?;

        context::persist_refreshed_tokens(&client, remote_name, dirs).await;

        print_reindex_result(&result);
        return Ok(());
    }

    let mode = {
        let mode = context::resolve_ledger_mode(ledger, dirs).await?;
        if direct {
            mode
        } else {
            context::try_server_route(mode, dirs)
        }
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let result = client.reindex(&remote_alias).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_reindex_result(&result);
        }
        LedgerMode::Local { fluree, alias } => {
            let ledger_id = context::to_ledger_id(&alias);

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
        }
    }

    Ok(())
}

fn print_reindex_result(result: &ReindexResponse) {
    println!(
        "Reindexed {} to t={} (root: {})",
        result.ledger_id, result.index_t, result.root_id
    );
}
