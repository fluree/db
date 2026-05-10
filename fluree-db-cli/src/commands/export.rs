//! `fluree export` — streaming RDF export via the API builder, plus
//! `--format ledger` for full `.flpack` archive export.

use crate::context;
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use colored::Colorize;
use fluree_db_api::export::ExportFormat;
use fluree_db_api::server_defaults::FlureeDir;
use std::io::{self, BufWriter, IsTerminal, Write};
use std::path::{Path, PathBuf};

/// Whether the user requested the full ledger archive format.
fn is_ledger_format(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "ledger" | "flpack")
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    explicit_ledger: Option<&str>,
    format_str: &str,
    output: Option<&Path>,
    no_indexes: bool,
    all_graphs: bool,
    graph: Option<&str>,
    context_expr: Option<&str>,
    context_file: Option<&Path>,
    at: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    let alias = context::resolve_ledger(explicit_ledger, dirs)?;

    if alias.contains('#') {
        return Err(CliError::Usage(
            "export does not support 'ledger#fragment' syntax; use --graph <IRI> to export a specific named graph"
                .to_string(),
        ));
    }
    if all_graphs && graph.is_some() {
        return Err(CliError::Usage(
            "cannot use both --all-graphs and --graph; choose one".to_string(),
        ));
    }

    if is_ledger_format(format_str) {
        return run_ledger_archive(
            &alias,
            output,
            no_indexes,
            at,
            all_graphs,
            graph,
            context_expr,
            context_file,
            dirs,
            remote_flag,
        )
        .await;
    }

    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = run_remote_rdf(
            &alias,
            format_str,
            output,
            all_graphs,
            graph,
            context_expr,
            context_file,
            at,
            &client,
        )
        .await;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let result = run_remote_rdf(
                &alias,
                format_str,
                output,
                all_graphs,
                graph,
                context_expr,
                context_file,
                at,
                &client,
            )
            .await;
            context::persist_refreshed_tokens(&client, context::LOCAL_SERVER_REMOTE, dirs).await;
            return result;
        }
    }

    run_local_rdf(
        &alias,
        format_str,
        output,
        all_graphs,
        graph,
        context_expr,
        context_file,
        at,
        dirs,
    )
    .await
}

// =============================================================================
// Ledger archive (--format ledger / flpack)
// =============================================================================

#[allow(clippy::too_many_arguments)]
async fn run_ledger_archive(
    alias: &str,
    output: Option<&Path>,
    no_indexes: bool,
    at: Option<&str>,
    all_graphs: bool,
    graph: Option<&str>,
    context_expr: Option<&str>,
    context_file: Option<&Path>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    if at.is_some() {
        return Err(CliError::Usage(
            "fluree export --format ledger does not support --at — archives capture the current head; \
             use a TimeTravel restore on import instead."
                .to_string(),
        ));
    }
    if all_graphs || graph.is_some() {
        return Err(CliError::Usage(
            "fluree export --format ledger archives the entire ledger; --all-graphs / --graph apply only to RDF formats"
                .to_string(),
        ));
    }
    if context_expr.is_some() || context_file.is_some() {
        return Err(CliError::Usage(
            "fluree export --format ledger does not use --context / --context-file (the archive is binary)"
                .to_string(),
        ));
    }

    let ledger_id = context::to_ledger_id(alias);

    if let Some(remote_name) = remote_flag {
        return run_ledger_archive_remote(alias, &ledger_id, output, no_indexes, dirs, remote_name)
            .await;
    }

    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(alias).is_some() || store.get_tracked(&ledger_id).is_some() {
        return Err(CliError::Usage(
            "this alias points at a tracked ledger (no local data); \
             pass `--remote <name>` to archive the upstream copy."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;

    match output {
        Some(path) => {
            let path: PathBuf = path.to_path_buf();
            let file = tokio::fs::File::create(&path).await.map_err(|e| {
                CliError::Config(format!("failed to create '{}': {e}", path.display()))
            })?;
            let mut writer = tokio::io::BufWriter::new(file);
            let archive_result = fluree
                .archive_ledger(&ledger_id, !no_indexes, &mut writer)
                .await;
            // Drop writer before we touch the file again on the error path,
            // so the underlying file handle is closed.
            drop(writer);

            let stats = match archive_result {
                Ok(stats) => stats,
                Err(e) => {
                    // Don't leave a corrupt or empty .flpack on disk for the
                    // user to discover later — clean up and surface the error.
                    let _ = std::fs::remove_file(&path);
                    return Err(e.into());
                }
            };
            eprintln!(
                "{} Archived '{}' → {} ({} commits, {} txn blobs, {} index artifacts)",
                "✓".green(),
                alias,
                path.display(),
                stats.commits_sent,
                stats.txn_blobs_sent,
                stats.index_artifacts_sent,
            );
        }
        None => {
            if io::stdout().is_terminal() {
                return Err(CliError::Usage(
                    "refusing to write a binary .flpack archive to a TTY; pass -o <FILE> or redirect stdout"
                        .to_string(),
                ));
            }
            let stdout = tokio::io::stdout();
            let mut writer = tokio::io::BufWriter::new(stdout);
            let stats = fluree
                .archive_ledger(&ledger_id, !no_indexes, &mut writer)
                .await?;
            // stdout already owns its bytes; nothing to clean up on failure.
            eprintln!(
                "{} Archived '{}' to stdout ({} commits, {} txn blobs, {} index artifacts)",
                "✓".green(),
                alias,
                stats.commits_sent,
                stats.txn_blobs_sent,
                stats.index_artifacts_sent,
            );
        }
    }
    Ok(())
}

/// Remote variant of `run_ledger_archive`.
///
/// Fetches the remote `NsRecord` (so we can synthesize the trailing
/// nameservice manifest), then issues `POST /pack/{ledger}` and copies the
/// pack stream through the user's writer. The remote client swaps the
/// terminal End frame for `manifest + End` on the fly, producing a byte
/// stream that's identical in shape to a local archive.
async fn run_ledger_archive_remote(
    alias: &str,
    ledger_id: &str,
    output: Option<&Path>,
    no_indexes: bool,
    dirs: &FlureeDir,
    remote_name: &str,
) -> CliResult<()> {
    use fluree_db_core::pack::PackRequest;

    let client = context::build_remote_client(remote_name, dirs).await?;

    // Pull the NsRecord first; we need its head CIDs and t values both to
    // build the pack request and to construct the trailing manifest.
    let record = client
        .fetch_ns_record(ledger_id)
        .await
        .map_err(|e| {
            CliError::Remote(format!(
                "failed to fetch NsRecord for '{ledger_id}' on '{remote_name}': {e}"
            ))
        })?
        .ok_or_else(|| {
            CliError::NotFound(format!(
                "ledger '{ledger_id}' not found on remote '{remote_name}'"
            ))
        })?;

    let head_commit_id = record.commit_head_id.clone().ok_or_else(|| {
        CliError::Remote(format!(
            "remote ledger '{ledger_id}' has no head commit to archive"
        ))
    })?;

    // Mirror `Fluree::archive_ledger`: only request indexes when the user
    // wants them AND the remote actually has an index root. Otherwise the
    // archive degrades to commits-only and the manifest will omit
    // `index_head_id` accordingly.
    let include_indexes = !no_indexes;
    let request = match (include_indexes, record.index_head_id.clone()) {
        (true, Some(index_root)) => {
            PackRequest::with_indexes(vec![head_commit_id], vec![], index_root, None)
        }
        _ => PackRequest::commits(vec![head_commit_id], vec![]),
    };

    match output {
        Some(path) => {
            let path: PathBuf = path.to_path_buf();
            let file = tokio::fs::File::create(&path).await.map_err(|e| {
                CliError::Config(format!("failed to create '{}': {e}", path.display()))
            })?;
            let mut writer = tokio::io::BufWriter::new(file);
            let result = client
                .archive_ledger_to_writer(ledger_id, &request, &record, &mut writer)
                .await;
            drop(writer);
            context::persist_refreshed_tokens(&client, remote_name, dirs).await;

            let frames = match result {
                Ok(frames) => frames,
                Err(e) => {
                    let _ = std::fs::remove_file(&path);
                    return Err(CliError::Remote(format!(
                        "failed to archive '{alias}' from '{remote_name}': {e}"
                    )));
                }
            };
            eprintln!(
                "{} Archived '{}' from '{}' → {} ({} pack frames forwarded)",
                "✓".green(),
                alias,
                remote_name,
                path.display(),
                frames,
            );
        }
        None => {
            if io::stdout().is_terminal() {
                return Err(CliError::Usage(
                    "refusing to write a binary .flpack archive to a TTY; pass -o <FILE> or redirect stdout"
                        .to_string(),
                ));
            }
            let stdout = tokio::io::stdout();
            let mut writer = tokio::io::BufWriter::new(stdout);
            let frames = client
                .archive_ledger_to_writer(ledger_id, &request, &record, &mut writer)
                .await
                .map_err(|e| {
                    CliError::Remote(format!(
                        "failed to archive '{alias}' from '{remote_name}': {e}"
                    ))
                })?;
            context::persist_refreshed_tokens(&client, remote_name, dirs).await;
            eprintln!(
                "{} Archived '{}' from '{}' to stdout ({} pack frames forwarded)",
                "✓".green(),
                alias,
                remote_name,
                frames,
            );
        }
    }
    Ok(())
}

// =============================================================================
// RDF formats (turtle, ntriples, nquads, trig, jsonld)
// =============================================================================

#[allow(clippy::too_many_arguments)]
async fn run_remote_rdf(
    alias: &str,
    format_str: &str,
    output: Option<&Path>,
    all_graphs: bool,
    graph: Option<&str>,
    context_expr: Option<&str>,
    context_file: Option<&Path>,
    at: Option<&str>,
    client: &RemoteLedgerClient,
) -> CliResult<()> {
    let context_override = resolve_context_override(context_expr, context_file)?;

    let mut body = serde_json::json!({ "format": format_str });
    if all_graphs {
        body["all_graphs"] = serde_json::Value::Bool(true);
    }
    if let Some(iri) = graph {
        body["graph"] = serde_json::Value::String(iri.to_string());
    }
    if let Some(at_str) = at {
        body["at"] = serde_json::Value::String(at_str.to_string());
    }
    if let Some(ctx) = context_override {
        body["context"] = ctx;
    }

    let bytes = client
        .export_rdf(alias, &body)
        .await
        .map_err(|e| CliError::Remote(format!("failed to export '{alias}': {e}")))?;

    write_bytes_to_sink(&bytes, output)
}

#[allow(clippy::too_many_arguments)]
async fn run_local_rdf(
    alias: &str,
    format_str: &str,
    output: Option<&Path>,
    all_graphs: bool,
    graph: Option<&str>,
    context_expr: Option<&str>,
    context_file: Option<&Path>,
    at: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(alias).is_some()
        || store.get_tracked(&context::to_ledger_id(alias)).is_some()
    {
        return Err(CliError::Usage(
            "export is not available for tracked ledgers (no local data); pass --remote <name> to export from the upstream."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;
    let format = parse_rdf_format(format_str)?;

    let mut builder = fluree.export(alias).format(format);

    if all_graphs {
        builder = builder.all_graphs();
    }
    if let Some(iri) = graph {
        builder = builder.graph(iri);
    }
    if let Some(at_str) = at {
        builder = builder.as_of(crate::commands::query::parse_time_spec(at_str));
    }
    if let Some(ctx) = resolve_context_override(context_expr, context_file)? {
        builder = builder.context(&ctx);
    }

    match output {
        Some(path) => {
            let file = std::fs::File::create(path).map_err(|e| {
                CliError::Config(format!("failed to create '{}': {e}", path.display()))
            })?;
            let mut writer = BufWriter::new(file);
            builder.write_to(&mut writer).await?;
            writer
                .flush()
                .map_err(|e| CliError::Config(format!("failed to flush output: {e}")))?;
        }
        None => {
            let stdout = io::stdout().lock();
            let mut writer = BufWriter::new(stdout);
            builder.write_to(&mut writer).await?;
        }
    }

    Ok(())
}

fn write_bytes_to_sink(bytes: &[u8], output: Option<&Path>) -> CliResult<()> {
    match output {
        Some(path) => {
            let file = std::fs::File::create(path).map_err(|e| {
                CliError::Config(format!("failed to create '{}': {e}", path.display()))
            })?;
            let mut writer = BufWriter::new(file);
            writer
                .write_all(bytes)
                .map_err(|e| CliError::Config(format!("failed to write export: {e}")))?;
            writer
                .flush()
                .map_err(|e| CliError::Config(format!("failed to flush output: {e}")))?;
        }
        None => {
            let stdout = io::stdout().lock();
            let mut writer = BufWriter::new(stdout);
            writer
                .write_all(bytes)
                .map_err(|e| CliError::Config(format!("failed to write export to stdout: {e}")))?;
            writer
                .flush()
                .map_err(|e| CliError::Config(format!("failed to flush stdout: {e}")))?;
        }
    }
    Ok(())
}

/// Parse a CLI format string into an `ExportFormat` (RDF formats only).
fn parse_rdf_format(s: &str) -> CliResult<ExportFormat> {
    match s.to_lowercase().as_str() {
        "turtle" | "ttl" => Ok(ExportFormat::Turtle),
        "ntriples" | "nt" => Ok(ExportFormat::NTriples),
        "nquads" | "n-quads" => Ok(ExportFormat::NQuads),
        "trig" => Ok(ExportFormat::TriG),
        "jsonld" | "json-ld" | "json" => Ok(ExportFormat::JsonLd),
        other => Err(CliError::Usage(format!(
            "unknown export format '{other}'; valid formats: turtle, ntriples, nquads, trig, jsonld, ledger"
        ))),
    }
}

/// Parse an optional context override from `--context` or `--context-file`.
fn resolve_context_override(
    expr: Option<&str>,
    file: Option<&Path>,
) -> CliResult<Option<serde_json::Value>> {
    let json_str = if let Some(e) = expr {
        Some(e.to_string())
    } else if let Some(path) = file {
        let s = std::fs::read_to_string(path).map_err(|e| {
            CliError::Usage(format!(
                "failed to read context file '{}': {e}",
                path.display()
            ))
        })?;
        Some(s)
    } else {
        None
    };

    match json_str {
        Some(s) => {
            let val: serde_json::Value = serde_json::from_str(&s)
                .map_err(|e| CliError::Usage(format!("invalid context JSON: {e}")))?;
            // Accept either { "@context": {...} } wrapper or bare object
            let ctx = if let Some(inner) = val.get("@context") {
                inner.clone()
            } else {
                val
            };
            Ok(Some(ctx))
        }
        None => Ok(None),
    }
}
