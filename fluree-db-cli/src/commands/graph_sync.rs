//! `fluree sync` — make a named graph's contents exactly the supplied data,
//! committing only the delta.
//!
//! The target graph is the constant of this command; the source of the
//! desired contents is pluggable ([`SyncSource`]). Every source resolves to
//! one JSON-LD payload and flows through the same verb — locally
//! `Fluree::sync_named_graph_with`, remotely `POST /sync` — so adding a
//! mapped source (R2RML over Iceberg / CSV / Excel) is one new variant here,
//! not a new command or endpoint.

use crate::cli::PolicyArgs;
use crate::commands::insert::{build_policy_ctx, resolve_inputs};
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::{CliError, CliResult};
use crate::input;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{SyncGraphOpts, SyncGraphReport, TxnOpts};
use std::path::Path;

/// Arguments for [`run`].
pub struct SyncArgs<'a> {
    pub args: &'a [String],
    pub ledger: Option<&'a str>,
    pub graph: &'a str,
    pub expr: Option<&'a str>,
    pub file: Option<&'a Path>,
    pub format: Option<&'a str>,
    pub dry_run: bool,
    pub allow_empty: bool,
    pub json: bool,
    pub remote: Option<&'a str>,
    pub direct: bool,
    pub policy: &'a PolicyArgs,
    pub dirs: &'a FlureeDir,
}

/// Where the graph's desired contents come from.
///
/// Today: RDF text. Designed as the seam for mapped sources — an R2RML
/// mapping applied to an Iceberg table, CSV, or spreadsheet would be a new
/// variant whose [`SyncSource::into_payload`] materializes the mapping's
/// output (locally, or via a server-side materialization when running
/// `--remote`) into the same JSON-LD payload.
pub enum SyncSource {
    /// Turtle or JSON-LD text, already read from a file / expression / stdin.
    RdfText {
        content: String,
        format: detect::DataFormat,
    },
}

impl SyncSource {
    /// Materialize the desired contents as one JSON-LD payload.
    ///
    /// Turtle is converted client-side: the sync endpoint is JSON-LD only,
    /// so a Turtle export (the common ontology-editor case) works against
    /// any server that implements it.
    pub fn into_payload(self) -> CliResult<serde_json::Value> {
        match self {
            SyncSource::RdfText { content, format } => match format {
                detect::DataFormat::JsonLd => Ok(serde_json::from_str(&content)?),
                detect::DataFormat::Turtle => fluree_graph_turtle::parse_to_json(&content)
                    .map_err(|e| CliError::Usage(format!("failed to parse Turtle: {e}"))),
            },
        }
    }
}

pub async fn run(a: SyncArgs<'_>) -> CliResult<()> {
    if a.graph.is_empty() {
        return Err(CliError::Usage(
            "--graph is required: sync targets exactly one named graph".to_string(),
        ));
    }

    let (explicit_ledger, positional_inline, positional_file) = resolve_inputs(a.ledger, a.args)?;
    let source = input::resolve_input(
        a.expr,
        positional_inline,
        a.file,
        positional_file.as_deref(),
    )?;
    let content = input::read_input(&source)?;
    let detect_path = a.file.or(positional_file.as_deref());
    let format = detect::detect_data_format(detect_path, &content, a.format)?;
    let payload = SyncSource::RdfText { content, format }.into_payload()?;

    // The empty-payload gate is enforced server-side too; checking here
    // gives a precise message before any network or staging work.
    let explicitly_empty = payload
        .get("@graph")
        .and_then(serde_json::Value::as_array)
        .is_some_and(Vec::is_empty);
    if explicitly_empty && !a.allow_empty {
        return Err(CliError::Usage(
            "payload is empty; syncing it would clear the graph — pass --allow-empty to confirm"
                .to_string(),
        ));
    }

    let mode = if let Some(remote_name) = a.remote {
        let alias = context::resolve_ledger(explicit_ledger, a.dirs)?;
        context::build_remote_mode(remote_name, &alias, a.dirs).await?
    } else {
        let mode = context::resolve_ledger_mode(explicit_ledger, a.dirs).await?;
        if a.direct {
            mode
        } else {
            context::try_server_route(mode, a.dirs)
        }
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let client = client.with_policy(a.policy.clone());
            let response = client
                .sync_jsonld(&remote_alias, a.graph, &payload, a.dry_run, a.allow_empty)
                .await?;
            context::persist_refreshed_tokens(&client, &remote_name, a.dirs).await;
            if a.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&response)
                        .unwrap_or_else(|_| response.to_string())
                );
            } else {
                print_remote_response(a.graph, &response, a.dry_run);
            }
        }
        LedgerMode::Local { fluree, alias } => {
            let policy_ctx = build_policy_ctx(&fluree, &alias, a.policy).await?;
            let report = fluree
                .sync_named_graph_with(
                    &alias,
                    a.graph,
                    &payload,
                    SyncGraphOpts {
                        dry_run: a.dry_run,
                        allow_empty: a.allow_empty,
                    },
                    TxnOpts::default(),
                    policy_ctx,
                )
                .await?;
            if a.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report_json(&report)).expect("report serializes")
                );
            } else {
                print_local_report(&report);
            }
        }
    }
    Ok(())
}

/// The machine-readable report — the same shape the server's dry-run
/// response uses, so scripts consume either path identically.
fn report_json(r: &SyncGraphReport) -> serde_json::Value {
    serde_json::json!({
        "ledger": r.ledger_id,
        "graph": r.graph_iri,
        "asserted": r.asserted,
        "retracted": r.retracted,
        "committed": r.committed,
        "dryRun": r.dry_run,
        "t": r.t,
    })
}

fn print_local_report(r: &SyncGraphReport) {
    if r.dry_run {
        println!(
            "Would sync graph <{}> in '{}': +{} asserted, -{} retracted (dry run; head t={}).",
            r.graph_iri, r.ledger_id, r.asserted, r.retracted, r.t
        );
    } else if r.committed {
        println!(
            "Synced graph <{}> in '{}': +{} asserted, -{} retracted (t={}).",
            r.graph_iri, r.ledger_id, r.asserted, r.retracted, r.t
        );
    } else {
        println!(
            "Graph <{}> in '{}' already matches the payload — no commit produced (t={}).",
            r.graph_iri, r.ledger_id, r.t
        );
    }
}

fn print_remote_response(graph: &str, value: &serde_json::Value, dry_run: bool) {
    // Dry runs answer with the report shape; real runs with the standard
    // transact response (ledger, t, tx-id, ...).
    if dry_run {
        let n = |k: &str| {
            value
                .get(k)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        println!(
            "Would sync graph <{graph}>: +{} asserted, -{} retracted (dry run; head t={}).",
            n("asserted"),
            n("retracted"),
            value
                .get("t")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0)
        );
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
    }
}
