//! `fluree load` — stream a local CSV into a ledger as batched per-row upserts
//! (the `LOAD CSV` analog).
//!
//! The CLI is the component that actually holds the file, so it reads the CSV
//! locally, batches the rows, and sends each batch to the ledger — local or
//! remote — as one transaction (one commit per batch). This sidesteps
//! server-side file access entirely: the server only ever receives ordinary
//! parameterized Cypher writes or JSON-LD updates.
//!
//! Two template languages, same batch:
//! - `--cypher`: the per-row body rides in `UNWIND $batch AS row <template>`;
//!   columns are read as `row.<column>`. Empty cell → `null`.
//! - `--jsonld`: the batch is injected as the update's `values` clause, one
//!   `?<column>` variable per CSV column. Empty cell → `""` (the JSON-LD
//!   `values` parser rejects nulls).

use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use serde_json::{json, Map, Value};
use std::path::Path;

/// The per-row write, in one of the two supported template languages.
pub enum Template {
    /// Cypher body wrapped as `UNWIND $batch AS row <body>`.
    Cypher(String),
    /// A JSON-LD update object; the batch is injected as its `values` clause.
    JsonLd(Value),
}

impl Template {
    /// Resolve the mutually-exclusive `--cypher` / `--jsonld` flags. clap's arg
    /// group already rejects passing both; this enforces that exactly one is
    /// present and parses the JSON-LD body up front.
    pub fn from_flags(cypher: Option<String>, jsonld: Option<String>) -> CliResult<Self> {
        match (cypher, jsonld) {
            (Some(c), None) => Ok(Template::Cypher(c)),
            (None, Some(j)) => {
                let obj = serde_json::from_str::<Value>(&j)
                    .map_err(|e| CliError::Usage(format!("--jsonld is not valid JSON: {e}")))?;
                if !obj.is_object() {
                    return Err(CliError::Usage(
                        "--jsonld must be a JSON-LD update object (with where/insert/delete)"
                            .into(),
                    ));
                }
                Ok(Template::JsonLd(obj))
            }
            (None, None) => Err(CliError::Usage(
                "provide the per-row template with --cypher or --jsonld".into(),
            )),
            (Some(_), Some(_)) => Err(CliError::Usage(
                "pass only one of --cypher or --jsonld".into(),
            )),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ledger_flag: Option<&str>,
    from: &Path,
    template: Template,
    batch_size: usize,
    field_terminator: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    // Each batch becomes literal AST rows inlined into one query/transaction
    // (see `expand_unwind_match` in fluree-db-cypher), so an unbounded
    // batch-size is a user-inflicted OOM rather than a throughput knob.
    const MAX_BATCH_SIZE: usize = 100_000;
    if batch_size == 0 {
        return Err(CliError::Usage("--batch-size must be at least 1".into()));
    }
    if batch_size > MAX_BATCH_SIZE {
        return Err(CliError::Usage(format!(
            "--batch-size must be at most {MAX_BATCH_SIZE} (each batch is inlined into one transaction)"
        )));
    }
    let delimiter = single_byte_delimiter(field_terminator)?;

    // Resolve where the write lands: explicit --remote, or the local ledger
    // (auto-routed to a running local server unless --direct), mirroring
    // `fluree update`.
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger_flag, dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await?
    } else {
        let mode = context::resolve_ledger_mode(ledger_flag, dirs).await?;
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
            let mut reader = csv_reader(from, delimiter)?;
            let headers = header_row(&mut reader)?;
            let (mut total, mut commits) = (0usize, 0usize);
            let mut rows: Vec<csv::StringRecord> = Vec::with_capacity(batch_size);
            for record in reader.records() {
                rows.push(record.map_err(csv_err)?);
                if rows.len() >= batch_size {
                    send_remote(&client, &remote_alias, &template, &headers, &rows).await?;
                    commits += 1;
                    total += rows.len();
                    report_progress(total);
                    rows.clear();
                }
            }
            if !rows.is_empty() {
                send_remote(&client, &remote_alias, &template, &headers, &rows).await?;
                commits += 1;
                total += rows.len();
            }
            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
            println!("Loaded {total} rows into `{remote_alias}` in {commits} commit(s)");
        }
        LedgerMode::Local { fluree, alias } => {
            let mut ledger = fluree.ledger(&alias).await?;
            let mut reader = csv_reader(from, delimiter)?;
            let headers = header_row(&mut reader)?;
            let (mut total, mut commits) = (0usize, 0usize);
            let mut rows: Vec<csv::StringRecord> = Vec::with_capacity(batch_size);
            for record in reader.records() {
                rows.push(record.map_err(csv_err)?);
                if rows.len() >= batch_size {
                    ledger = send_local(&fluree, ledger, &template, &headers, &rows).await?;
                    commits += 1;
                    total += rows.len();
                    report_progress(total);
                    rows.clear();
                }
            }
            if !rows.is_empty() {
                ledger = send_local(&fluree, ledger, &template, &headers, &rows).await?;
                commits += 1;
                total += rows.len();
            }
            let _ = ledger;
            println!("Loaded {total} rows into `{alias}` in {commits} commit(s)");
        }
    }

    Ok(())
}

/// Send one batch to a remote ledger via the update endpoint.
async fn send_remote(
    client: &crate::remote_client::RemoteLedgerClient,
    ledger: &str,
    template: &Template,
    headers: &[String],
    rows: &[csv::StringRecord],
) -> CliResult<()> {
    match template {
        Template::Cypher(body) => {
            let wrapped = wrap_cypher(body);
            let params = json!({ "batch": cypher_batch(headers, rows) });
            client
                .update_cypher(ledger, &wrapped, params.as_object())
                .await?;
        }
        Template::JsonLd(obj) => {
            let update = jsonld_with_values(obj, headers, rows);
            client.update_jsonld(ledger, &update).await?;
        }
    }
    Ok(())
}

/// Send one batch to the local ledger, returning the advanced ledger state.
async fn send_local(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    template: &Template,
    headers: &[String],
    rows: &[csv::StringRecord],
) -> CliResult<fluree_db_api::LedgerState> {
    match template {
        Template::Cypher(body) => {
            let wrapped = wrap_cypher(body);
            let params = json!({ "batch": cypher_batch(headers, rows) });
            let result = fluree
                .transact_cypher_with_params(ledger, &wrapped, params.as_object())
                .await?;
            Ok(result.ledger)
        }
        Template::JsonLd(obj) => {
            let update = jsonld_with_values(obj, headers, rows);
            let result = fluree.stage_owned(ledger).update(&update).execute().await?;
            Ok(result.ledger)
        }
    }
}

/// Wrap a per-row Cypher body so the batch rides in as `$batch` and each row
/// binds as `row`, exactly like `LOAD CSV … AS row`.
fn wrap_cypher(body: &str) -> String {
    format!("UNWIND $batch AS row\n{body}")
}

/// The `$batch` parameter for a Cypher load: one map per row, keyed by column.
/// Empty cell → `null` (Neo4j-faithful).
fn cypher_batch(headers: &[String], rows: &[csv::StringRecord]) -> Value {
    Value::Array(
        rows.iter()
            .map(|record| {
                let mut obj = Map::with_capacity(headers.len());
                for (header, cell) in headers.iter().zip(record.iter()) {
                    let value = if cell.is_empty() {
                        Value::Null
                    } else {
                        Value::String(cell.to_string())
                    };
                    obj.insert(header.clone(), value);
                }
                Value::Object(obj)
            })
            .collect(),
    )
}

/// Clone the JSON-LD update template and inject the batch as its `values`
/// clause: `[["?col1", …], [[cells…], …]]`, one `?<column>` variable per CSV
/// column. Empty cell → `""` (the `values` parser rejects nulls).
fn jsonld_with_values(template: &Value, headers: &[String], rows: &[csv::StringRecord]) -> Value {
    let vars: Vec<Value> = headers.iter().map(|h| json!(format!("?{h}"))).collect();
    let value_rows: Vec<Value> = rows
        .iter()
        .map(|record| {
            let cells: Vec<Value> = headers
                .iter()
                .enumerate()
                .map(|(i, _)| Value::String(record.get(i).unwrap_or("").to_string()))
                .collect();
            Value::Array(cells)
        })
        .collect();

    let mut update = template.clone();
    if let Some(obj) = update.as_object_mut() {
        obj.insert(
            "values".to_string(),
            Value::Array(vec![Value::Array(vars), Value::Array(value_rows)]),
        );
    }
    update
}

/// One CSV field delimiter byte. Cypher/CSV field terminators are single
/// characters; multi-byte input is a user error.
fn single_byte_delimiter(s: &str) -> CliResult<u8> {
    let bytes = s.as_bytes();
    if bytes.len() != 1 {
        return Err(CliError::Usage(format!(
            "--field-terminator must be a single character, got {s:?}"
        )));
    }
    Ok(bytes[0])
}

fn csv_reader(path: &Path, delimiter: u8) -> CliResult<csv::Reader<std::fs::File>> {
    csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(csv_err)
}

fn header_row(reader: &mut csv::Reader<std::fs::File>) -> CliResult<Vec<String>> {
    let headers = reader.headers().map_err(csv_err)?;
    if headers.is_empty() {
        return Err(CliError::Usage(
            "CSV has no header row — the first line must name the columns".into(),
        ));
    }
    Ok(headers.iter().map(str::to_string).collect())
}

fn report_progress(total: usize) {
    eprintln!("  … {total} rows");
}

fn csv_err(e: csv::Error) -> CliError {
    CliError::Usage(format!("CSV read error: {e}"))
}
