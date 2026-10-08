//! `fluree sync` — make a graph's contents exactly the supplied data,
//! committing only the delta. Without `--graph`, the default graph.
//!
//! The target graph is the constant of this command; the source of the
//! desired contents is pluggable ([`SyncSource`]). Every source resolves to
//! one [`SyncPayload`] and flows through the same verb — locally
//! `Fluree::sync_graph_with`, remotely `POST /sync` — so adding a mapped
//! source (R2RML over Iceberg / CSV / Excel) is one new variant here, not a
//! new command or endpoint.

use crate::cli::PolicyArgs;
use crate::commands::insert::{build_policy_ctx, resolve_inputs};
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::{CliError, CliResult};
use crate::input;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{GraphPayload, GraphSel, SyncGraphOpts, SyncGraphReport, TxnOpts};
use std::path::Path;

/// Arguments for [`run`].
pub struct SyncArgs<'a> {
    pub args: &'a [String],
    pub ledger: Option<&'a str>,
    /// Target named graph; `None` is the default graph.
    pub graph: Option<&'a str>,
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

/// The desired contents, as submitted.
pub enum SyncPayload {
    JsonLd(serde_json::Value),
    /// Sent as TriG: the Turtle-to-JSON-LD conversion has no graph blocks.
    /// The API or server requires every block to name the target graph.
    Trig(String),
}

impl SyncSource {
    /// Materialize the desired contents as one payload.
    ///
    /// Turtle is converted to JSON-LD client-side, so a Turtle export (the
    /// common ontology-editor case) works against a server from before
    /// `/sync` read RDF bodies. TriG is sent as-is.
    pub fn into_payload(self) -> CliResult<SyncPayload> {
        match self {
            SyncSource::RdfText { content, format } => match format {
                detect::DataFormat::JsonLd => {
                    Ok(SyncPayload::JsonLd(serde_json::from_str(&content)?))
                }
                // TriG always fails the Turtle parse, so a TriG body sniffed
                // or named as Turtle is only looked for once that happens.
                detect::DataFormat::Turtle => match fluree_graph_turtle::parse_to_json(&content) {
                    Ok(json) => Ok(SyncPayload::JsonLd(json)),
                    Err(_) if detect::is_trig_body(&content) => Ok(SyncPayload::Trig(content)),
                    Err(e) => Err(CliError::Usage(format!("failed to parse Turtle: {e}"))),
                },
                detect::DataFormat::Trig => Ok(SyncPayload::Trig(content)),
            },
        }
    }
}

pub async fn run(a: SyncArgs<'_>) -> CliResult<()> {
    if a.graph == Some("") {
        return Err(CliError::Usage(
            "--graph needs an IRI; omit it to sync the default graph".to_string(),
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
    let explicitly_empty = match &payload {
        SyncPayload::JsonLd(json) => json
            .get("@graph")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty),
        SyncPayload::Trig(_) => false,
    };
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
            let response = match &payload {
                SyncPayload::JsonLd(json) => {
                    client
                        .sync_jsonld(&remote_alias, a.graph, json, a.dry_run, a.allow_empty)
                        .await?
                }
                SyncPayload::Trig(trig) => {
                    client
                        .sync_trig(&remote_alias, a.graph, trig, a.dry_run, a.allow_empty)
                        .await?
                }
            };
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
            let graph = match a.graph {
                Some(iri) => GraphSel::Graph(iri.to_string()),
                None => GraphSel::Default,
            };
            let graph_payload = match &payload {
                SyncPayload::JsonLd(json) => GraphPayload::JsonLd(json),
                SyncPayload::Trig(trig) => GraphPayload::Rdf(trig),
            };
            let report = fluree
                .sync_graph_with(
                    &alias,
                    &graph,
                    graph_payload,
                    SyncGraphOpts {
                        dry_run: a.dry_run,
                        allow_empty: a.allow_empty,
                        ..Default::default()
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
    let graph = match &r.graph_iri {
        Some(iri) => format!("graph <{iri}>"),
        None => "the default graph".to_string(),
    };
    if r.dry_run {
        println!(
            "Would sync {graph} in '{}': +{} asserted, -{} retracted (dry run; head t={}).",
            r.ledger_id, r.asserted, r.retracted, r.t
        );
    } else if r.committed {
        println!(
            "Synced {graph} in '{}': +{} asserted, -{} retracted (t={}).",
            r.ledger_id, r.asserted, r.retracted, r.t
        );
    } else {
        let mut graph = graph;
        graph[..1].make_ascii_uppercase();
        println!(
            "{graph} in '{}' already matches the payload — no commit produced (t={}).",
            r.ledger_id, r.t
        );
    }
}

fn print_remote_response(graph: Option<&str>, value: &serde_json::Value, dry_run: bool) {
    // Dry runs answer with the report shape; real runs with the standard
    // transact response (ledger, t, tx-id, ...).
    if dry_run {
        let n = |k: &str| {
            value
                .get(k)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        let graph = match graph {
            Some(iri) => format!("graph <{iri}>"),
            None => "the default graph".to_string(),
        };
        println!(
            "Would sync {graph}: +{} asserted, -{} retracted (dry run; head t={}).",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turtle_star_sync_source_converts_to_annotation_blocks() {
        // `fluree graph sync` converts Turtle client-side; an ontology
        // export carrying RDF 1.2 annotations must reach the sync endpoint
        // as `@annotation` blocks instead of failing the conversion.
        let source = SyncSource::RdfText {
            content: "@prefix ex: <http://example.org/> .\n\
                      ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} .\n"
                .to_string(),
            format: detect::DataFormat::Turtle,
        };
        let SyncPayload::JsonLd(payload) = source.into_payload().expect("Turtle-star converts")
        else {
            panic!("Turtle converts to JSON-LD");
        };
        let alice = payload
            .as_array()
            .expect("node array")
            .iter()
            .find(|n| n["@id"] == "http://example.org/alice")
            .expect("alice node");
        assert_eq!(
            alice["http://example.org/knows"][0]["@annotation"]["@id"],
            "http://example.org/claim1"
        );
    }

    fn payload_of(content: &str, format: detect::DataFormat) -> CliResult<SyncPayload> {
        SyncSource::RdfText {
            content: content.to_string(),
            format,
        }
        .into_payload()
    }

    /// TriG goes out as TriG whether it was named as TriG or detected in a
    /// body that sniffed as Turtle; Turtle with a `{` in a literal stays
    /// Turtle, converted to JSON-LD.
    #[test]
    fn trig_is_sent_as_trig_and_turtle_as_json_ld() {
        let trig =
            "GRAPH <http://example.org/g> { <http://example.org/s> <http://example.org/p> 1 . }";
        for format in [detect::DataFormat::Trig, detect::DataFormat::Turtle] {
            assert!(matches!(
                payload_of(trig, format),
                Ok(SyncPayload::Trig(t)) if t == trig
            ));
        }
        assert!(matches!(
            payload_of(
                "<http://example.org/s> <http://example.org/p> \"{v}\" .",
                detect::DataFormat::Turtle
            ),
            Ok(SyncPayload::JsonLd(_))
        ));
        let err = payload_of(
            "<http://example.org/s> <http://example.org/p>",
            detect::DataFormat::Turtle,
        )
        .err()
        .expect("malformed Turtle")
        .to_string();
        assert!(err.contains("failed to parse Turtle"), "{err}");
    }
}
