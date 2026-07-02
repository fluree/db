//! `fluree validate` — SHACL validation reports.
//!
//! Ledger mode validates the current state of a local ledger; file mode
//! loads an RDF file into an ephemeral in-memory ledger (staging-time SHACL
//! enforcement disabled so embedded shapes can't reject the load) and runs
//! the same validation core. Both call `fluree_db_api::validate`.

use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::{CliError, CliResult, EXIT_ERROR};
use fluree_db_api::validate::{ShapesSource, ValidateOptions, ValidateReport};
use fluree_db_api::Fluree;
use std::io::Write;
use std::path::Path;

/// Severity threshold for the process exit code.
#[derive(Clone, Copy, PartialEq)]
enum FailOn {
    Violation,
    Warning,
    Info,
}

enum ReportFormat {
    Table,
    JsonLd,
    Turtle,
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    target: Option<&str>,
    graph: Option<&str>,
    shacl: Option<&Path>,
    shacl_graph: Option<&str>,
    include_attached: bool,
    format: &str,
    fail_on: &str,
    config_path: Option<&Path>,
) -> CliResult<()> {
    let format = parse_format(format)?;
    let fail_on = parse_fail_on(fail_on)?;
    let shapes = resolve_shapes_source(shacl, shacl_graph)?;

    // File mode: an existing RDF file validates in an ephemeral memory ledger.
    if let Some(t) = target {
        let path = Path::new(t);
        if path.is_file() {
            let report = validate_file(path, graph, shapes, include_attached).await?;
            return finish(&report, format, fail_on);
        }
        if looks_like_data_file(t) {
            return Err(CliError::Usage(format!(
                "no such file: '{t}' — pass an existing RDF file or a ledger name"
            )));
        }
    }

    // Ledger mode: local ledgers only (the HTTP validate endpoint comes later).
    let dirs = crate::config::require_fluree_dir(config_path)?;
    let mode = context::resolve_ledger_mode(target, &dirs).await?;
    match mode {
        LedgerMode::Local { fluree, alias } => {
            let options = ValidateOptions {
                graph: graph.map(String::from),
                shapes,
                include_attached,
            };
            let report = fluree.validate_ledger(&alias, &options).await?;
            finish(&report, format, fail_on)
        }
        LedgerMode::Tracked { .. } => Err(CliError::Usage(
            "fluree validate runs against local ledgers; validating a remote \
             ledger over HTTP is not yet supported"
                .into(),
        )),
    }
}

/// Load an RDF file into an ephemeral in-memory ledger and validate it.
async fn validate_file(
    path: &Path,
    graph: Option<&str>,
    shapes: ShapesSource,
    include_attached: bool,
) -> CliResult<ValidateReport> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| CliError::Input(format!("cannot read data file '{}': {e}", path.display())))?;
    let data_format = detect::detect_data_format(Some(path), &content, None)?;

    let fluree = context::build_memory_fluree();
    let alias = "validate/scratch:main";
    fluree.create_ledger(alias).await?;

    // Disable staging-time SHACL so a file that embeds its own shapes loads
    // even when the data violates them — surfacing violations is this
    // command's job, not the loader's.
    disable_staging_shacl(&fluree, alias).await?;

    let ledger_graph = fluree.graph(alias);
    match data_format {
        detect::DataFormat::Turtle => {
            ledger_graph
                .transact()
                .insert_turtle(&content)
                .commit()
                .await?;
        }
        detect::DataFormat::JsonLd => {
            let json: serde_json::Value = serde_json::from_str(&content)?;
            ledger_graph.transact().insert(&json).commit().await?;
        }
    }

    let options = ValidateOptions {
        graph: graph.map(String::from),
        shapes,
        include_attached,
    };
    Ok(fluree.validate_ledger(alias, &options).await?)
}

async fn disable_staging_shacl(fluree: &Fluree, alias: &str) -> CliResult<()> {
    let config_iri = fluree_db_core::graph_registry::config_graph_iri(alias);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:validate> rdf:type f:LedgerConfig .
            <urn:config:validate> f:shaclDefaults <urn:config:validate-shacl> .
            <urn:config:validate-shacl> f:shaclEnabled false .
        }}
    "
    );
    fluree
        .graph(alias)
        .transact()
        .upsert_turtle(&trig)
        .commit()
        .await?;
    Ok(())
}

fn resolve_shapes_source(
    shacl: Option<&Path>,
    shacl_graph: Option<&str>,
) -> CliResult<ShapesSource> {
    if let Some(path) = shacl {
        let content = std::fs::read_to_string(path).map_err(|e| {
            CliError::Input(format!("cannot read shapes file '{}': {e}", path.display()))
        })?;
        return Ok(
            match detect::detect_data_format(Some(path), &content, None)? {
                detect::DataFormat::Turtle => ShapesSource::InlineTurtle(content),
                detect::DataFormat::JsonLd => {
                    ShapesSource::InlineJsonLd(serde_json::from_str(&content)?)
                }
            },
        );
    }
    if let Some(iri) = shacl_graph {
        return Ok(ShapesSource::Graph(iri.to_string()));
    }
    Ok(ShapesSource::Attached)
}

fn parse_format(format: &str) -> CliResult<ReportFormat> {
    match format {
        "table" => Ok(ReportFormat::Table),
        "jsonld" | "json-ld" | "json" => Ok(ReportFormat::JsonLd),
        "turtle" | "ttl" => Ok(ReportFormat::Turtle),
        other => Err(CliError::Usage(format!(
            "unknown --format '{other}' (expected table, jsonld, or turtle)"
        ))),
    }
}

fn parse_fail_on(fail_on: &str) -> CliResult<FailOn> {
    match fail_on {
        "violation" => Ok(FailOn::Violation),
        "warning" => Ok(FailOn::Warning),
        "info" => Ok(FailOn::Info),
        other => Err(CliError::Usage(format!(
            "unknown --fail-on '{other}' (expected violation, warning, or info)"
        ))),
    }
}

/// Print the report and exit non-zero when the fail-on threshold is met.
fn finish(report: &ValidateReport, format: ReportFormat, fail_on: FailOn) -> CliResult<()> {
    match format {
        ReportFormat::Table => print_table(report),
        ReportFormat::JsonLd => println!("{}", serde_json::to_string_pretty(&report.to_jsonld())?),
        ReportFormat::Turtle => print!("{}", report.to_turtle()),
    }

    if report.shape_count == 0 {
        eprintln!("warning: no SHACL shapes found — nothing was validated");
    }

    let failing = match fail_on {
        FailOn::Violation => report.violation_count(),
        FailOn::Warning => report.violation_count() + report.warning_count(),
        FailOn::Info => report.results.len(),
    };
    if failing > 0 {
        std::io::stdout().flush().ok();
        return Err(CliError::ExitCode(EXIT_ERROR));
    }
    Ok(())
}

fn print_table(report: &ValidateReport) {
    for result in &report.results {
        let severity = short_iri(&result.severity);
        let component = short_iri(&result.constraint_component);
        match result.focus_node.as_str() {
            Some(iri) => println!("{severity}: {iri}"),
            None => println!("{severity}: {}", result.focus_node),
        }
        if let Some(path) = &result.result_path {
            println!("    path:      {path}");
        }
        println!("    component: {component}");
        println!("    message:   {}", result.message);
        if let Some(value) = &result.value {
            println!("    value:     {value}");
        }
    }
    if !report.results.is_empty() {
        println!();
    }
    println!(
        "Conforms: {} — {} violation(s), {} warning(s), {} info ({} shape(s) checked)",
        report.conforms,
        report.violation_count(),
        report.warning_count(),
        report.info_count(),
        report.shape_count
    );
}

/// Shorten a SHACL-namespace IRI to its local name for table display.
fn short_iri(iri: &str) -> &str {
    iri.rsplit_once('#').map_or(iri, |(_, local)| local)
}

/// A path-shaped or extension-bearing argument that names no existing file is
/// almost always a typo'd file path — reject clearly instead of resolving it
/// as a ledger name.
fn looks_like_data_file(s: &str) -> bool {
    // Ledger aliases may contain '/', so key off data-file extensions only.
    let lower = s.to_ascii_lowercase();
    [".json", ".jsonld", ".ttl", ".nt", ".nq", ".trig"]
        .iter()
        .any(|ext| lower.ends_with(ext))
}
