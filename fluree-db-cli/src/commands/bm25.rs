//! BM25 full-text search index commands.
//!
//! BM25 is a Fluree *graph source* (like Iceberg/R2RML). Index **creation** and
//! **sync** have no HTTP route or other shipped entrypoint today — they are
//! Rust-API-only operations (`create_full_text_index` / `sync_bm25_index`).
//! These commands expose that API so an index can be built and kept fresh
//! reproducibly, running **in-process** against local storage via
//! [`build_fluree`]. Native file storage coordinates writers with a per-file
//! advisory flock (not an exclusive whole-store lock), so `create`/`drop`/`sync`
//! work under `docker exec` against a directory a server is already serving:
//! each writes new content-addressed snapshots plus/against a graph-source
//! nameservice record (no key the server writes). `sync` is incremental
//! (watermark-based) and lets a maintenance job keep an index current as its
//! source ledger is materialized — there is no HTTP `sync`, and the standalone
//! `fluree-search-httpd` is read-only, so this CLI is the way to advance an
//! index. Querying the resulting index is done separately — through
//! `fluree-search-httpd` (`POST /v1/search`, reading the same storage), or
//! embedded via an FQL `f:searchText` query.

use crate::cli::Bm25Action;
use crate::context::build_fluree;
use crate::error::{CliError, CliResult};
use crate::input;
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::Bm25CreateConfig;
use std::path::Path;

pub async fn run(action: Bm25Action, dirs: &FlureeDir) -> CliResult<()> {
    match action {
        Bm25Action::Create {
            name,
            ledger,
            branch,
            query,
            query_file,
            k1,
            b,
        } => {
            run_create(
                &name,
                &ledger,
                &branch,
                query.as_deref(),
                query_file.as_deref(),
                k1,
                b,
                dirs,
            )
            .await
        }
        Bm25Action::Drop { index, force } => run_drop(&index, force, dirs).await,
        Bm25Action::Sync { index } => run_sync(&index, dirs).await,
        Bm25Action::List { stale } => run_list(stale, dirs).await,
    }
}

/// List BM25 indexes with their source ledger and staleness — what a maintenance
/// job enumerates to decide which to `sync`. An index is STALE when its source
/// ledger's commit `t` has advanced past the index's watermark (`index_t`).
async fn run_list(stale_only: bool, dirs: &FlureeDir) -> CliResult<()> {
    use comfy_table::{ContentArrangement, Table};
    use std::collections::HashMap;

    let fluree = build_fluree(dirs)?;
    let ledgers = fluree.nameservice().all_records().await?;
    let sources = fluree.nameservice().all_graph_source_records().await?;

    // Source-ledger alias -> current commit t (skip retracted).
    let commit_t: HashMap<String, i64> = ledgers
        .iter()
        .filter(|r| !r.retracted)
        .map(|r| (format!("{}:{}", r.name, r.branch), r.commit_t))
        .collect();

    // (name, branch, source ledger, index_t, source commit_t?, stale)
    let mut rows: Vec<(String, String, String, i64, Option<i64>, bool)> = sources
        .iter()
        .filter(|r| r.is_bm25() && !r.retracted)
        .map(|gs| {
            let source = gs.dependencies.first().cloned().unwrap_or_default();
            // A stored dependency alias may omit the branch (a bare `name` means
            // `name:main` to Fluree), while ledger records are keyed `name:branch`
            // — so try the alias as-is, then with an implicit `:main`.
            let ledger_t = commit_t
                .get(&source)
                .or_else(|| commit_t.get(&format!("{source}:main")))
                .copied();
            let stale = ledger_t.is_some_and(|lt| gs.index_t < lt);
            (
                gs.name.clone(),
                gs.branch.clone(),
                source,
                gs.index_t,
                ledger_t,
                stale,
            )
        })
        .collect();
    rows.sort();

    // Script-friendly mode: just the stale indexes, one alias per line, so a
    // maintenance loop can do: `for i in $(fluree bm25 list --stale); do
    // fluree bm25 sync --index "$i"; done`.
    if stale_only {
        for (name, branch, ..) in rows.iter().filter(|r| r.5) {
            println!("{name}:{branch}");
        }
        return Ok(());
    }

    if rows.is_empty() {
        println!("No BM25 full-text indexes found. Run 'fluree bm25 create ...' to add one.");
        return Ok(());
    }

    // Same look and feel as `fluree list` (comfy_table, dynamic arrangement).
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        "NAME",
        "BRANCH",
        "SOURCE LEDGER",
        "INDEX_T",
        "LEDGER_T",
        "STALE",
    ]);
    for (name, branch, source, index_t, ledger_t, stale) in &rows {
        let index_t_str = if *index_t > 0 {
            index_t.to_string()
        } else {
            "-".to_string()
        };
        let ledger_t_str = ledger_t.map_or_else(|| "-".to_string(), |v| v.to_string());
        table.add_row(vec![
            name.clone(),
            branch.clone(),
            source.clone(),
            index_t_str,
            ledger_t_str,
            if *stale { "YES" } else { "no" }.to_string(),
        ]);
    }
    println!("{table}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_create(
    name: &str,
    ledger: &str,
    branch: &str,
    query_inline: Option<&str>,
    query_file: Option<&Path>,
    k1: Option<f64>,
    b: Option<f64>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Resolve the indexing query: -e inline > -f file > stdin.
    let source = input::resolve_input(query_inline, None, query_file, None)?;
    let content = input::read_input(&source)?;
    let query: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CliError::Input(format!("indexing query must be valid JSON: {e}")))?;

    let mut config = Bm25CreateConfig::new(name, ledger, query).with_branch(branch);
    if let Some(k1) = k1 {
        config = config.with_k1(k1);
    }
    if let Some(b) = b {
        config = config.with_b(b);
    }
    config.validate().map_err(CliError::Api)?;

    eprintln!(
        "  {} indexing {} -> {}:{}...",
        "bm25:".cyan().bold(),
        ledger,
        name,
        branch
    );

    let fluree = build_fluree(dirs)?;
    let result = fluree
        .create_full_text_index(config)
        .await
        .map_err(CliError::Api)?;

    println!(
        "Created full-text index {} (docs={}, terms={}, index_t={}).",
        result.graph_source_id, result.doc_count, result.term_count, result.index_t
    );
    Ok(())
}

async fn run_sync(index: &str, dirs: &FlureeDir) -> CliResult<()> {
    eprintln!("  {} syncing {}...", "bm25:".cyan().bold(), index);

    let fluree = build_fluree(dirs)?;
    let result = fluree.sync_bm25_index(index).await.map_err(CliError::Api)?;

    if result.old_watermark == result.new_watermark && result.upserted == 0 && result.removed == 0 {
        println!(
            "Full-text index {} already up to date (watermark {}).",
            result.graph_source_id, result.new_watermark
        );
    } else {
        println!(
            "Synced full-text index {} ({} upserted, {} removed, {} subject{}; \
             watermark {} -> {}{}).",
            result.graph_source_id,
            result.upserted,
            result.removed,
            result.affected_subjects,
            if result.affected_subjects == 1 {
                ""
            } else {
                "s"
            },
            result.old_watermark,
            result.new_watermark,
            if result.was_full_resync {
                ", full resync"
            } else {
                ""
            }
        );
    }
    Ok(())
}

/// Drop is destructive — it retracts the nameservice record *and* deletes the
/// snapshot blobs — so it gates behind `--force` like every other destructive
/// command here (`fluree drop` for a ledger, `fluree iceberg drop` for the
/// adjacent graph-source family). Rebuilding an index over a large corpus is
/// expensive enough that the confirmation earns its keep.
async fn run_drop(index: &str, force: bool, dirs: &FlureeDir) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of '{index}'"
        )));
    }

    let fluree = build_fluree(dirs)?;
    let result = fluree
        .drop_full_text_index(index)
        .await
        .map_err(CliError::Api)?;

    if result.was_already_retracted {
        println!("Full-text index {index} was already retracted.");
    } else {
        println!(
            "Dropped full-text index {} (deleted {} snapshot{}).",
            result.graph_source_id,
            result.deleted_snapshots,
            if result.deleted_snapshots == 1 {
                ""
            } else {
                "s"
            }
        );
    }
    Ok(())
}
