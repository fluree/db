//! `fluree export` — streaming RDF export via the API builder.

use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::export::ExportFormat;
use fluree_db_api::server_defaults::FlureeDir;
use std::io::{self, BufWriter};
use std::path::Path;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    explicit_ledger: Option<&str>,
    format_str: &str,
    all_graphs: bool,
    graph: Option<&str>,
    context_expr: Option<&str>,
    context_file: Option<&Path>,
    at: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for tracked ledger — export requires local data
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let alias = context::resolve_ledger(explicit_ledger, dirs)?;

    // Reject ledger#fragment syntax — use --graph instead
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

    if store.get_tracked(&alias).is_some()
        || store.get_tracked(&context::to_ledger_id(&alias)).is_some()
    {
        return Err(CliError::Usage(
            "export is not available for tracked ledgers (no local data).".to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;

    // Parse format string → ExportFormat
    let format = parse_format(format_str)?;

    // Build the export
    let mut builder = fluree.export(&alias).format(format);

    if all_graphs {
        builder = builder.all_graphs();
    }

    if let Some(iri) = graph {
        builder = builder.graph(iri);
    }

    if let Some(at_str) = at {
        builder = builder.as_of(crate::commands::query::parse_time_spec(at_str));
    }

    // Resolve context override (--context or --context-file)
    if let Some(ctx) = resolve_context_override(context_expr, context_file)? {
        builder = builder.context(&ctx);
    }

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);
    builder.write_to(&mut writer).await?;

    Ok(())
}

/// Parse a CLI format string into an `ExportFormat`.
fn parse_format(s: &str) -> CliResult<ExportFormat> {
    match s.to_lowercase().as_str() {
        "turtle" | "ttl" => Ok(ExportFormat::Turtle),
        "ntriples" | "nt" => Ok(ExportFormat::NTriples),
        "nquads" | "n-quads" => Ok(ExportFormat::NQuads),
        "trig" => Ok(ExportFormat::TriG),
        "jsonld" | "json-ld" | "json" => Ok(ExportFormat::JsonLd),
        other => Err(CliError::Usage(format!(
            "unknown export format '{other}'; valid formats: turtle, ntriples, nquads, trig, jsonld"
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
