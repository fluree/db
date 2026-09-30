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
use crate::remote_client::{RemoteLedgerClient, RemoteLedgerError};
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
/// Today: RDF text or JSON-LD. Designed as the seam for mapped sources — an
/// R2RML mapping applied to an Iceberg table, CSV, or spreadsheet would be a
/// new variant whose [`SyncSource::into_payload`] materializes the mapping's
/// output (locally, or via a server-side materialization when running
/// `--remote`) into the same payload.
pub enum SyncSource {
    /// Turtle, TriG or JSON-LD text, already read from a file / expression /
    /// stdin.
    RdfText {
        content: String,
        format: detect::DataFormat,
    },
}

/// The desired contents, as submitted.
pub enum SyncPayload {
    JsonLd(serde_json::Value),
    /// RDF text as written: Turtle, N-Triples, or TriG (`trig`) whose graph
    /// blocks must name the target graph. It is parsed once where it is
    /// staged, as every write lane parses RDF text.
    Rdf {
        text: String,
        trig: bool,
    },
}

impl SyncSource {
    /// Materialize the desired contents as one payload. RDF text is sent as
    /// written; nothing converts it to JSON-LD on the way.
    pub fn into_payload(self) -> CliResult<SyncPayload> {
        match self {
            SyncSource::RdfText { content, format } => match format {
                detect::DataFormat::JsonLd => {
                    Ok(SyncPayload::JsonLd(serde_json::from_str(&content)?))
                }
                detect::DataFormat::Turtle => {
                    let trig = detect::is_trig_body(&content);
                    Ok(SyncPayload::Rdf {
                        text: content,
                        trig,
                    })
                }
                detect::DataFormat::Trig => Ok(SyncPayload::Rdf {
                    text: content,
                    trig: true,
                }),
            },
        }
    }
}

/// Send `payload` to a remote `/sync`. RDF text goes as written; only a
/// server from before `/sync` read RDF bodies answers 415, and for that one
/// Turtle is converted to JSON-LD here and sent again (TriG has no JSON-LD
/// form for its blocks, so it gets the 415).
async fn sync_remote(
    client: &RemoteLedgerClient,
    ledger: &str,
    graph: Option<&str>,
    payload: &SyncPayload,
    dry_run: bool,
    allow_empty: bool,
) -> CliResult<serde_json::Value> {
    match payload {
        SyncPayload::JsonLd(json) => Ok(client
            .sync_jsonld(ledger, graph, json, dry_run, allow_empty)
            .await?),
        SyncPayload::Rdf { text, trig } => {
            let content_type = if *trig {
                "application/trig"
            } else {
                "text/turtle"
            };
            match client
                .sync_rdf(ledger, graph, text, content_type, dry_run, allow_empty)
                .await
            {
                Err(RemoteLedgerError::UnsupportedMediaType(_)) if !*trig => {
                    let json = turtle_as_json_ld(text)?;
                    Ok(client
                        .sync_jsonld(ledger, graph, &json, dry_run, allow_empty)
                        .await?)
                }
                other => Ok(other?),
            }
        }
    }
}

/// The client-side conversion a server without RDF bodies on `/sync` needs.
/// It is lossy (collection order, `rdf:type` objects that are not IRIs, IRIs
/// whose scheme reads as a prefix); upgrading the server avoids it.
#[allow(clippy::disallowed_methods)] // the one fallback that must convert
fn turtle_as_json_ld(text: &str) -> CliResult<serde_json::Value> {
    fluree_graph_turtle::parse_to_json(text)
        .map_err(|e| CliError::Usage(format!("failed to parse Turtle: {e}")))
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
        SyncPayload::Rdf { .. } => false,
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
            let response = sync_remote(
                &client,
                &remote_alias,
                a.graph,
                &payload,
                a.dry_run,
                a.allow_empty,
            )
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
            let graph = match a.graph {
                Some(iri) => GraphSel::Graph(iri.to_string()),
                None => GraphSel::Default,
            };
            let graph_payload = match &payload {
                SyncPayload::JsonLd(json) => GraphPayload::JsonLd(json),
                SyncPayload::Rdf { text, .. } => GraphPayload::Rdf(text),
            };
            let report = fluree
                .sync_graph_with(
                    &alias,
                    &graph,
                    graph_payload,
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
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    /// The fallback conversion for a server without RDF bodies on `/sync`
    /// keeps RDF 1.2 annotations, as `@annotation` blocks.
    #[test]
    fn the_fallback_conversion_keeps_annotations() {
        let payload = turtle_as_json_ld(
            "@prefix ex: <http://example.org/> .\n\
             ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} .\n",
        )
        .expect("Turtle-star converts");
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

    /// RDF text goes out as written. TriG is marked whether it was named as
    /// TriG or detected in a body that sniffed as Turtle; Turtle with a `{`
    /// in a literal stays Turtle; malformed Turtle is left for the parser
    /// that stages it to report.
    #[test]
    fn rdf_text_is_sent_as_written() {
        let trig =
            "GRAPH <http://example.org/g> { <http://example.org/s> <http://example.org/p> 1 . }";
        for format in [detect::DataFormat::Trig, detect::DataFormat::Turtle] {
            assert!(matches!(
                payload_of(trig, format),
                Ok(SyncPayload::Rdf { text, trig: true }) if text == trig
            ));
        }
        for turtle in [
            "<http://example.org/s> <http://example.org/p> \"{v}\" .",
            "<http://example.org/s> <http://example.org/p>",
        ] {
            assert!(matches!(
                payload_of(turtle, detect::DataFormat::Turtle),
                Ok(SyncPayload::Rdf { text, trig: false }) if text == turtle
            ));
        }
    }

    /// A stub `/sync` answering its requests with `statuses` in turn,
    /// recording each request's content type.
    async fn stub(statuses: Vec<u16>) -> (RemoteLedgerClient, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Arc::default();
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            for status in statuses {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let header_end = loop {
                    let n = sock.read(&mut tmp).await.unwrap();
                    if n == 0 {
                        break buf.len();
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
                let header = |name: &str| {
                    headers
                        .lines()
                        .find_map(|l| l.strip_prefix(name))
                        .map(|v| v.trim().to_string())
                };
                let length: usize = header("content-length:")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while buf.len() < header_end + length {
                    let n = sock.read(&mut tmp).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                recorded
                    .lock()
                    .unwrap()
                    .push(header("content-type:").unwrap_or_default());
                let body = if status == 200 {
                    r#"{"ledger":"db:main","t":2}"#
                } else {
                    r#"{"error":"unsupported media type"}"#
                };
                let reply = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(reply.as_bytes()).await.unwrap();
                sock.flush().await.unwrap();
            }
        });
        (
            RemoteLedgerClient::new(&format!("http://{addr}/v1/fluree"), None),
            seen,
        )
    }

    fn turtle() -> SyncPayload {
        SyncPayload::Rdf {
            text: "<http://example.org/s> <http://example.org/p> ( 1 2 ) .".to_string(),
            trig: false,
        }
    }

    #[tokio::test]
    async fn a_server_that_reads_rdf_gets_the_text() {
        let (client, seen) = stub(vec![200]).await;
        sync_remote(&client, "db:main", None, &turtle(), false, false)
            .await
            .expect("sync");
        assert_eq!(*seen.lock().unwrap(), ["text/turtle"]);
    }

    #[tokio::test]
    async fn turtle_falls_back_to_json_ld_only_on_a_415() {
        let (client, seen) = stub(vec![415, 200]).await;
        sync_remote(&client, "db:main", None, &turtle(), false, false)
            .await
            .expect("sync after the fallback");
        assert_eq!(*seen.lock().unwrap(), ["text/turtle", "application/json"]);
    }

    #[tokio::test]
    async fn trig_is_not_converted() {
        let (client, seen) = stub(vec![415]).await;
        let trig = SyncPayload::Rdf {
            text:
                "GRAPH <http://example.org/g> { <http://example.org/s> <http://example.org/p> 1 . }"
                    .to_string(),
            trig: true,
        };
        let err = sync_remote(
            &client,
            "db:main",
            Some("http://example.org/g"),
            &trig,
            false,
            false,
        )
        .await
        .expect_err("no JSON-LD form for graph blocks");
        assert!(err.to_string().contains("415"), "{err}");
        assert_eq!(*seen.lock().unwrap(), ["application/trig"]);
    }
}
