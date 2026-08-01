//! BM25 full-text search index commands.
//!
//! BM25 is a Fluree *graph source* (like Iceberg/R2RML).
//!
//! `create`, `sync`, and `drop` take one of two routes, chosen by
//! [`resolve_client`]: against a server (`--remote <name>`, or a
//! locally-running server picked up automatically) via
//! `POST /v1/fluree/bm25/create`, `…/bm25/sync`, and the shared `…/drop`; or
//! in-process against local storage via [`build_fluree`] when `--direct` is
//! passed or no server is reachable. `list` is always in-process — computing
//! staleness needs each index's source dependencies, which the server's
//! `/ledgers` response does not carry.
//!
//! The in-process route works under `docker exec` against a directory a server
//! is already serving: native file storage coordinates writers with a per-file
//! advisory flock (not an exclusive whole-store lock), and each command writes
//! new content-addressed snapshots plus/against a graph-source nameservice
//! record (no key the server writes).
//!
//! `sync` is incremental (watermark-based) and lets a maintenance job keep an
//! index current as its source ledger is materialized; a server started with
//! `--bm25-auto-sync` does the same on every source commit. The standalone
//! `fluree-search-httpd` is read-only either way. Querying the resulting index
//! is done separately — through `fluree-search-httpd` (`POST /v1/search`,
//! reading the same storage), or embedded via an FQL `f:searchText` query.

use crate::cli::Bm25Action;
use crate::context::build_fluree;
use crate::error::{CliError, CliResult};
use crate::input;
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::Bm25CreateConfig;

pub async fn run(action: Bm25Action, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        Bm25Action::Create {
            name,
            ledger,
            branch,
            query,
            query_file,
            k1,
            b,
            remote,
        } => {
            let args = CreateArgs {
                name,
                ledger,
                branch,
                query,
                query_file,
                k1,
                b,
            };
            run_create(args, dirs, remote.as_deref(), direct).await
        }
        Bm25Action::Drop {
            index,
            force,
            remote,
        } => run_drop(&index, force, dirs, remote.as_deref(), direct).await,
        Bm25Action::Sync { index, t, remote } => {
            run_sync(&index, t, dirs, remote.as_deref(), direct).await
        }
        Bm25Action::List { stale } => run_list(stale, dirs).await,
    }
}

/// Pick the server to drive, if any.
///
/// An explicit `--remote` names one; otherwise a locally-running server is
/// auto-routed to unless `--direct` was passed. `None` means run in-process
/// against local storage, which is what these commands did before the HTTP
/// endpoints existed.
async fn resolve_client(
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<Option<crate::remote_client::RemoteLedgerClient>> {
    if let Some(remote_name) = remote_flag {
        return crate::context::build_remote_client(remote_name, dirs)
            .await
            .map(Some);
    }
    if direct {
        return Ok(None);
    }
    Ok(crate::context::try_server_route_client(dirs))
}

/// Persist tokens the server refreshed during the call — only meaningful for a
/// named remote, since the auto-routed local server has no stored credentials.
async fn persist_tokens(
    client: &crate::remote_client::RemoteLedgerClient,
    remote_flag: Option<&str>,
    dirs: &FlureeDir,
) {
    if let Some(remote_name) = remote_flag {
        crate::context::persist_refreshed_tokens(client, remote_name, dirs).await;
    }
}

/// Resolved `bm25 create` inputs, grouped so the local and remote paths take
/// one argument rather than seven.
struct CreateArgs {
    name: String,
    ledger: String,
    branch: String,
    query: Option<String>,
    query_file: Option<std::path::PathBuf>,
    k1: Option<f64>,
    b: Option<f64>,
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
    args: CreateArgs,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    // Resolve the indexing query: -e inline > -f file > stdin.
    let source = input::resolve_input(
        args.query.as_deref(),
        None,
        args.query_file.as_deref(),
        None,
    )?;
    let content = input::read_input(&source)?;
    let query: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CliError::Input(format!("indexing query must be valid JSON: {e}")))?;

    eprintln!(
        "  {} indexing {} -> {}:{}...",
        "bm25:".cyan().bold(),
        args.ledger,
        args.name,
        args.branch
    );

    if let Some(client) = resolve_client(dirs, remote_flag, direct).await? {
        let body = create_request_body(&args, &query);
        let result = client
            .bm25_create(&body)
            .await
            .map_err(|e| CliError::Remote(format!("failed to create full-text index: {e}")))?;
        persist_tokens(&client, remote_flag, dirs).await;
        return print_remote_create(&result);
    }

    let mut config =
        Bm25CreateConfig::new(&args.name, &args.ledger, query).with_branch(&args.branch);
    if let Some(k1) = args.k1 {
        config = config.with_k1(k1);
    }
    if let Some(b) = args.b {
        config = config.with_b(b);
    }

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

/// Body for `POST /bm25/create`, mirroring `Bm25CreateConfig`'s optional fields
/// so the server applies the same defaults the local path would.
fn create_request_body(args: &CreateArgs, query: &serde_json::Value) -> serde_json::Value {
    let mut body = serde_json::json!({
        "name": args.name,
        "ledger": args.ledger,
        "branch": args.branch,
        "query": query,
    });
    if let Some(k1) = args.k1 {
        body["k1"] = serde_json::json!(k1);
    }
    if let Some(b) = args.b {
        body["b"] = serde_json::json!(b);
    }
    body
}

/// Render a response field for display.
///
/// Strings are unquoted: `Value`'s own `Display` would print
/// `"docsearch:main"` with the quotes included.
fn field(result: &serde_json::Value, key: &str) -> String {
    match result.get(key) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => "?".to_string(),
    }
}

fn print_remote_create(result: &serde_json::Value) -> CliResult<()> {
    println!(
        "Created full-text index {} (docs={}, terms={}, index_t={}).",
        field(result, "graph_source_id"),
        field(result, "doc_count"),
        field(result, "term_count"),
        field(result, "index_t")
    );
    Ok(())
}

async fn run_sync(
    index: &str,
    target_t: Option<i64>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    eprintln!("  {} syncing {}...", "bm25:".cyan().bold(), index);

    if let Some(client) = resolve_client(dirs, remote_flag, direct).await? {
        let result = client
            .bm25_sync(index, target_t)
            .await
            .map_err(|e| CliError::Remote(format!("failed to sync full-text index: {e}")))?;
        persist_tokens(&client, remote_flag, dirs).await;
        println!(
            "Synced full-text index {} ({} upserted, {} removed; watermark {} -> {}).",
            field(&result, "graph_source_id"),
            field(&result, "upserted"),
            field(&result, "removed"),
            field(&result, "old_watermark"),
            field(&result, "new_watermark")
        );
        return Ok(());
    }

    let fluree = build_fluree(dirs)?;
    let result = match target_t {
        Some(t) => fluree.sync_bm25_index_to(index, t, None).await,
        None => fluree.sync_bm25_index(index).await,
    }
    .map_err(CliError::Api)?;

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
async fn run_drop(
    index: &str,
    force: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of '{index}'"
        )));
    }

    // No BM25-specific drop route: an index is a graph source, so the shared
    // `POST /drop` handles it (and sweeps its snapshots).
    if let Some(client) = resolve_client(dirs, remote_flag, direct).await? {
        let response = client
            .drop_resource(index, true)
            .await
            .map_err(|e| CliError::Remote(format!("failed to drop full-text index: {e}")))?;
        persist_tokens(&client, remote_flag, dirs).await;
        let status = response
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("dropped");
        match response
            .get("files_deleted")
            .and_then(serde_json::Value::as_u64)
        {
            Some(n) => println!("Dropped full-text index {index} ({status}, deleted {n} file(s))."),
            None => println!("Dropped full-text index {index} ({status})."),
        }
        return Ok(());
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args() -> CreateArgs {
        CreateArgs {
            name: "docsearch".to_string(),
            ledger: "docs:main".to_string(),
            branch: "main".to_string(),
            query: None,
            query_file: None,
            k1: None,
            b: None,
        }
    }

    /// The body has to match `Bm25CreateRequest` field-for-field, or the server
    /// rejects it as malformed rather than doing anything useful.
    #[test]
    fn create_body_carries_the_required_fields() {
        let query = json!({"select": {"?x": ["@id"]}});
        let body = create_request_body(&args(), &query);

        assert_eq!(body["name"], json!("docsearch"));
        assert_eq!(body["ledger"], json!("docs:main"));
        assert_eq!(body["branch"], json!("main"));
        assert_eq!(body["query"], query);
    }

    /// `k1`/`b` are `Option` server-side, so omitting them is what makes the
    /// server apply the same defaults the in-process path would.
    #[test]
    fn create_body_omits_unset_tuning_knobs() {
        let body = create_request_body(&args(), &json!({}));

        assert!(body.get("k1").is_none(), "k1 should be absent: {body}");
        assert!(body.get("b").is_none(), "b should be absent: {body}");
    }

    #[test]
    fn create_body_includes_tuning_knobs_when_set() {
        let mut a = args();
        a.k1 = Some(1.5);
        a.b = Some(0.4);
        let body = create_request_body(&a, &json!({}));

        assert_eq!(body["k1"], json!(1.5));
        assert_eq!(body["b"], json!(0.4));
    }

    /// Strings must print unquoted — `Value`'s `Display` renders
    /// `"docsearch:main"` with the quotes, which would leak into CLI output.
    #[test]
    fn field_unquotes_strings() {
        let result = json!({"graph_source_id": "docsearch:main", "doc_count": 3});

        assert_eq!(field(&result, "graph_source_id"), "docsearch:main");
        assert_eq!(field(&result, "doc_count"), "3");
        assert_eq!(field(&result, "missing"), "?");
    }
}
