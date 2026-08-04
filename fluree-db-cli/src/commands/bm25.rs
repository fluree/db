//! BM25 full-text search index commands.
//!
//! BM25 is a Fluree *graph source* (like Iceberg/R2RML).
//!
//! Every subcommand takes one of two routes, chosen by [`resolve_client`]:
//! against a server (`--remote <name>`, or a locally-running server picked up
//! automatically) via `POST /v1/fluree/bm25/create`, `…/bm25/sync`, the shared
//! `…/drop`, and `GET /v1/fluree/ledgers`; or in-process against local storage
//! via [`build_fluree`] when `--direct` is passed or no server is reachable.
//!
//! `list` computes staleness by pairing each index against its source ledger's
//! `t`. Both routes read that from a single listing — the `/ledgers` response
//! carries ledgers and graph sources together, each with its `t` and (for graph
//! sources) its dependencies. A server predating that field yields rows with an
//! unknown source rather than an error.
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
use std::collections::HashMap;

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
        Bm25Action::List { stale, remote } => {
            run_list(stale, dirs, remote.as_deref(), direct).await
        }
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

/// One row of `bm25 list`: an index paired with the source ledger it covers.
struct IndexRow {
    name: String,
    branch: String,
    source: String,
    index_t: i64,
    /// `None` when the source ledger could not be resolved — it may have been
    /// dropped out from under the index.
    ledger_t: Option<i64>,
}

impl IndexRow {
    /// An index is stale once its source has committed past the watermark.
    fn is_stale(&self) -> bool {
        self.ledger_t
            .is_some_and(|ledger_t| self.index_t < ledger_t)
    }

    fn alias(&self) -> String {
        format!("{}:{}", self.name, self.branch)
    }
}

/// Resolve a source ledger's current `t`.
///
/// A stored dependency alias may omit the branch (a bare `name` means
/// `name:main` to Fluree) while ledger records are keyed `name:branch`, so try
/// the alias as-is before assuming `:main`.
fn resolve_source_t(commit_t: &HashMap<String, i64>, source: &str) -> Option<i64> {
    commit_t
        .get(source)
        .or_else(|| commit_t.get(&format!("{source}:main")))
        .copied()
}

/// List BM25 indexes with their source ledger and staleness — what a maintenance
/// job enumerates to decide which to `sync`. An index is STALE when its source
/// ledger's commit `t` has advanced past the index's watermark (`index_t`).
async fn run_list(
    stale_only: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    let mut rows = if let Some(client) = resolve_client(dirs, remote_flag, direct).await? {
        let entries = client
            .list_ledgers()
            .await
            .map_err(|e| CliError::Remote(format!("failed to list indexes: {e}")))?;
        persist_tokens(&client, remote_flag, dirs).await;
        let rows = remote_index_rows(&entries);
        warn_unknown_sources(&rows);
        rows
    } else {
        local_index_rows(dirs).await?
    };
    rows.sort_by(|a, b| (&a.name, &a.branch).cmp(&(&b.name, &b.branch)));

    render_index_rows(&rows, stale_only);
    Ok(())
}

async fn local_index_rows(dirs: &FlureeDir) -> CliResult<Vec<IndexRow>> {
    let fluree = build_fluree(dirs)?;
    let ledgers = fluree.nameservice().all_records().await?;
    let sources = fluree.nameservice().all_graph_source_records().await?;

    // Source-ledger alias -> current commit t (skip retracted).
    let commit_t: HashMap<String, i64> = ledgers
        .iter()
        .filter(|r| !r.retracted)
        .map(|r| (format!("{}:{}", r.name, r.branch), r.commit_t))
        .collect();

    let rows = sources
        .iter()
        .filter(|r| r.is_bm25() && !r.retracted)
        .map(|gs| {
            let source = gs.dependencies.first().cloned().unwrap_or_default();
            IndexRow {
                name: gs.name.clone(),
                branch: gs.branch.clone(),
                ledger_t: resolve_source_t(&commit_t, &source),
                source,
                index_t: gs.index_t,
            }
        })
        .collect();
    Ok(rows)
}

/// Names of the indexes whose source ledger the server did not report.
fn unknown_source_aliases(rows: &[IndexRow]) -> Vec<String> {
    rows.iter()
        .filter(|row| row.source.is_empty())
        .map(IndexRow::alias)
        .collect()
}

/// Warn when the server did not say which ledger an index derives from.
///
/// An index with no known source reports as *not* stale, so a listing full of
/// them looks like everything is current and `--stale` returns nothing. The
/// usual cause is a server predating `dependencies` on the `/ledgers` response;
/// `--direct` reads the records locally and is unaffected.
fn warn_unknown_sources(rows: &[IndexRow]) {
    let unknown = unknown_source_aliases(rows);
    if unknown.is_empty() {
        return;
    }

    eprintln!(
        "  {} no source ledger reported for {}: staleness unknown. The server may \
         predate this field — upgrade it, or use --direct to read local records.",
        "warn:".yellow().bold(),
        unknown.join(", ")
    );
}

/// Build rows from a `GET /ledgers` response.
///
/// That response carries ledgers and graph sources together, each with its `t`
/// and (for graph sources) its dependencies, so one request has everything
/// staleness needs. A server too old to send `dependencies` yields rows with an
/// empty source and no staleness rather than an error.
fn remote_index_rows(entries: &serde_json::Value) -> Vec<IndexRow> {
    let Some(entries) = entries.as_array() else {
        return Vec::new();
    };

    let entry_type = |e: &serde_json::Value| {
        e.get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let alias = |e: &serde_json::Value| {
        format!(
            "{}:{}",
            field(e, "name"),
            e.get("branch")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("main")
        )
    };
    let t_of = |e: &serde_json::Value| e.get("t").and_then(serde_json::Value::as_i64);

    let commit_t: HashMap<String, i64> = entries
        .iter()
        .filter(|e| entry_type(e) == "Ledger")
        .filter_map(|e| t_of(e).map(|t| (alias(e), t)))
        .collect();

    entries
        .iter()
        .filter(|e| entry_type(e) == "BM25")
        .map(|e| {
            let source = e
                .get("dependencies")
                .and_then(serde_json::Value::as_array)
                .and_then(|deps| deps.first())
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            IndexRow {
                name: field(e, "name"),
                branch: e
                    .get("branch")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("main")
                    .to_string(),
                ledger_t: resolve_source_t(&commit_t, &source),
                source,
                index_t: t_of(e).unwrap_or_default(),
            }
        })
        .collect()
}

fn render_index_rows(rows: &[IndexRow], stale_only: bool) {
    use comfy_table::{ContentArrangement, Table};

    // Script-friendly mode: just the stale indexes, one alias per line, so a
    // maintenance loop can do: `for i in $(fluree bm25 list --stale); do
    // fluree bm25 sync --index "$i"; done`.
    if stale_only {
        for row in rows.iter().filter(|r| r.is_stale()) {
            println!("{}", row.alias());
        }
        return;
    }

    if rows.is_empty() {
        println!("No BM25 full-text indexes found. Run 'fluree bm25 create ...' to add one.");
        return;
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
    for row in rows {
        let index_t = if row.index_t > 0 {
            row.index_t.to_string()
        } else {
            "-".to_string()
        };
        let ledger_t = row
            .ledger_t
            .map_or_else(|| "-".to_string(), |v| v.to_string());
        table.add_row(vec![
            row.name.clone(),
            row.branch.clone(),
            row.source.clone(),
            index_t,
            ledger_t,
            if row.is_stale() { "YES" } else { "no" }.to_string(),
        ]);
    }
    println!("{table}");
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

/// Report a `POST /drop` outcome the way the in-process path reports it.
///
/// The endpoint answers `200` with a `status` for every outcome, including one
/// that dropped nothing. Left as-is that would make `bm25 drop` on an unknown
/// index print a success line and exit `0` over a server while erroring under
/// `--direct` — so a missing index is turned back into an error here, and an
/// already-retracted one into the same note the local path prints.
fn report_remote_drop(index: &str, response: &serde_json::Value) -> CliResult<()> {
    match response.get("status").and_then(serde_json::Value::as_str) {
        Some("not_found") => Err(CliError::Api(fluree_db_api::ApiError::NotFound(format!(
            "Graph source not found: {index}"
        )))),
        Some("already_retracted") => {
            println!("Full-text index {index} was already retracted.");
            Ok(())
        }
        _ => {
            match response
                .get("files_deleted")
                .and_then(serde_json::Value::as_u64)
            {
                Some(n) => println!("Dropped full-text index {index} (deleted {n} file(s))."),
                None => println!("Dropped full-text index {index}."),
            }
            Ok(())
        }
    }
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
        return report_remote_drop(index, &response);
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

    /// A `GET /ledgers` payload: two ledgers, a BM25 index over one of them,
    /// and a non-BM25 graph source that must not appear in the output.
    fn ledgers_payload() -> serde_json::Value {
        json!([
            {"name": "docs", "branch": "main", "type": "Ledger", "t": 7},
            {"name": "other", "branch": "main", "type": "Ledger", "t": 2},
            {"name": "docsearch", "branch": "main", "type": "BM25", "t": 5,
             "dependencies": ["docs:main"]},
            {"name": "warehouse", "branch": "main", "type": "Iceberg", "t": 0,
             "dependencies": ["docs:main"]},
        ])
    }

    #[test]
    fn remote_rows_pair_an_index_with_its_source_ledger() {
        let rows = remote_index_rows(&ledgers_payload());

        assert_eq!(rows.len(), 1, "only the BM25 entry is a row");
        let row = &rows[0];
        assert_eq!(row.alias(), "docsearch:main");
        assert_eq!(row.source, "docs:main");
        assert_eq!(row.index_t, 5);
        assert_eq!(row.ledger_t, Some(7));
        assert!(row.is_stale(), "index at 5 vs source at 7");
    }

    #[test]
    fn remote_rows_report_a_current_index_as_fresh() {
        let mut payload = ledgers_payload();
        payload[2]["t"] = json!(7);

        assert!(!remote_index_rows(&payload)[0].is_stale());
    }

    /// A dependency alias may omit the branch; a bare `docs` means `docs:main`.
    #[test]
    fn remote_rows_resolve_a_branchless_dependency_alias() {
        let mut payload = ledgers_payload();
        payload[2]["dependencies"] = json!(["docs"]);

        assert_eq!(remote_index_rows(&payload)[0].ledger_t, Some(7));
    }

    /// Against a server predating the `dependencies` field, the source cannot
    /// be resolved. That degrades to "unknown staleness" rather than claiming
    /// the index is current, which would send a maintenance loop back to sleep.
    #[test]
    fn remote_rows_survive_a_server_without_dependencies() {
        let mut payload = ledgers_payload();
        payload[2]
            .as_object_mut()
            .expect("object")
            .remove("dependencies");

        let rows = remote_index_rows(&payload);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ledger_t, None);
        assert!(!rows[0].is_stale(), "unknown staleness is not staleness");
    }

    #[test]
    fn remote_rows_handle_an_empty_or_malformed_payload() {
        assert!(remote_index_rows(&json!([])).is_empty());
        assert!(remote_index_rows(&json!({"unexpected": "shape"})).is_empty());
    }

    /// The listing degrades quietly — an unknown source reads as "not stale" —
    /// so the unresolved indexes have to be called out by name.
    #[test]
    fn unknown_sources_are_reported_by_alias() {
        let mut payload = ledgers_payload();
        payload[2]
            .as_object_mut()
            .expect("object")
            .remove("dependencies");

        let rows = remote_index_rows(&payload);
        assert_eq!(unknown_source_aliases(&rows), vec!["docsearch:main"]);
    }

    #[test]
    fn a_resolved_listing_warns_about_nothing() {
        let rows = remote_index_rows(&ledgers_payload());
        assert!(unknown_source_aliases(&rows).is_empty());
    }

    /// `POST /drop` answers 200 even when it dropped nothing, so dropping an
    /// unknown index must not read as success just because it went over HTTP.
    #[test]
    fn remote_drop_of_an_unknown_index_is_an_error() {
        let response = json!({"ledger_id": "nosuch:main", "status": "not_found"});

        assert!(report_remote_drop("nosuch:main", &response).is_err());
    }

    #[test]
    fn remote_drop_reports_an_already_retracted_index() {
        let response = json!({"ledger_id": "docsearch:main", "status": "already_retracted"});

        assert!(report_remote_drop("docsearch:main", &response).is_ok());
    }

    #[test]
    fn remote_drop_reports_a_successful_drop() {
        let response =
            json!({"ledger_id": "docsearch:main", "status": "dropped", "files_deleted": 2});

        assert!(report_remote_drop("docsearch:main", &response).is_ok());
    }
}
