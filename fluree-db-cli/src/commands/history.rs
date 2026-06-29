use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use crate::output::OutputFormatKind;
use crate::remote_client::RemoteLedgerClient;
use fluree_db_api::server_defaults::FlureeDir;
use std::path::Path;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    entity: &str,
    ledger: Option<&str>,
    from: &str,
    to: &str,
    predicate: Option<&str>,
    format_str: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    let alias = context::resolve_ledger(ledger, dirs)?;

    // Parse output format up-front so all paths share the validation.
    let output_format = match format_str.to_lowercase().as_str() {
        "json" => OutputFormatKind::Json,
        "table" => OutputFormatKind::Table,
        "csv" => OutputFormatKind::Csv,
        other => {
            return Err(CliError::Usage(format!(
                "unknown output format '{other}'; valid formats: json, table, csv"
            )));
        }
    };

    // Expand compact IRIs using stored prefixes — done locally since prefixes
    // are stored in the project's config and aren't available on the remote.
    let entity_iri = config::expand_iri(dirs.data_dir(), entity);
    let predicate_iri = predicate.map(|p| config::expand_iri(dirs.data_dir(), p));

    let query = build_history_query(
        &alias,
        &entity_iri,
        from,
        to,
        predicate_iri.as_deref(),
        dirs.data_dir(),
    );

    // Bare ledger ID (e.g. "mydb:main") for the auth-driving path segment.
    // The body's `from` carries the time-travel suffix ("mydb:main@t:N");
    // the server's auth check uses the path, the query engine uses the body.
    let ledger_id = context::to_ledger_id(&alias);

    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = run_remote(&alias, &ledger_id, &query, output_format, &client).await;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let result = run_remote(&alias, &ledger_id, &query, output_format, &client).await;
            context::persist_refreshed_tokens(&client, context::LOCAL_SERVER_REMOTE, dirs).await;
            return result;
        }
    }

    // Local path: tracked ledgers have no commit chain, so history can't run.
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(&alias).is_some()
        || store.get_tracked(&context::to_ledger_id(&alias)).is_some()
    {
        return Err(CliError::Usage(
            "history is not available locally for tracked ledgers (no commit chain).\n  \
             Use `fluree track status`, or pass `--remote <name>` to query the upstream."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;
    let ledger_view = fluree.ledger(&alias).await?;
    let result = fluree.query_connection(&query).await?;
    let json = result.to_jsonld(&ledger_view.snapshot)?;

    let output = format_history_result(&json, output_format)?;
    println!("{output}");

    Ok(())
}

async fn run_remote(
    alias: &str,
    ledger_id: &str,
    query: &serde_json::Value,
    output_format: OutputFormatKind,
    client: &RemoteLedgerClient,
) -> CliResult<()> {
    // Use the ledger-scoped query path (`POST /query/{ledger}`) rather than
    // connection-level. The server's auth check derives the ledger ID from
    // the path when present, so a token scoped to `mydb:main` matches; if we
    // posted to `/query` instead, auth would see body.from = `mydb:main@t:N`
    // and reject scoped tokens.
    let json = client
        .query_jsonld(ledger_id, query)
        .await
        .map_err(|e| CliError::Remote(format!("failed to query history for '{alias}': {e}")))?;

    let output = format_history_result(&json, output_format)?;
    println!("{output}");
    Ok(())
}

/// Build a JSON-LD history query for an entity.
fn build_history_query(
    alias: &str,
    entity_iri: &str,
    from: &str,
    to: &str,
    predicate: Option<&str>,
    data_dir: &Path,
) -> serde_json::Value {
    // Build time specs
    let from_spec = format_time_spec(alias, from);
    let to_spec = format_time_spec(alias, to);

    // Build context from stored prefixes
    let context = config::prefixes_to_context(data_dir);

    // Build where clause as an array (required format for history queries)
    let where_clause = if let Some(pred) = predicate {
        serde_json::json!([
            {
                "@id": entity_iri,
                pred: { "@value": "?v", "@t": "?t", "@op": "?op" }
            }
        ])
    } else {
        serde_json::json!([
            {
                "@id": entity_iri,
                "?p": { "@value": "?v", "@t": "?t", "@op": "?op" }
            }
        ])
    };

    // This projection order is a contract: rows come back as positional arrays
    // in exactly this order, and `HistoryRow::from_value` reads them by index.
    // If you change the select list (order, length, or which vars are
    // projected), update `HistoryRow::from_value` to match or the table/CSV
    // formatters will silently render the wrong columns.
    let select = if predicate.is_some() {
        serde_json::json!(["?v", "?t", "?op"])
    } else {
        serde_json::json!(["?p", "?v", "?t", "?op"])
    };

    serde_json::json!({
        "@context": context,
        "from": from_spec,
        "to": to_spec,
        "select": select,
        "where": where_clause,
        "orderBy": "?t"
    })
}

/// Format a time specification for the query.
fn format_time_spec(alias: &str, spec: &str) -> String {
    if spec == "latest" {
        format!("{alias}:main@t:latest")
    } else if let Ok(_t) = spec.parse::<i64>() {
        format!("{alias}:main@t:{spec}")
    } else if spec.contains('-') && spec.contains(':') {
        // ISO-8601 timestamp
        format!("{alias}:main@iso:{spec}")
    } else {
        // Assume commit CID prefix
        format!("{alias}:main@commit:{spec}")
    }
}

/// Format history results for display.
fn format_history_result(json: &serde_json::Value, format: OutputFormatKind) -> CliResult<String> {
    match format {
        // cypher-json isn't meaningful for history; render as plain JSON.
        OutputFormatKind::Json | OutputFormatKind::TypedJson | OutputFormatKind::CypherJson => {
            Ok(serde_json::to_string_pretty(json).unwrap_or_else(|_| json.to_string()))
        }
        OutputFormatKind::Table => format_history_table(json),
        OutputFormatKind::Csv => format_history_csv(json),
        OutputFormatKind::Tsv => Err(CliError::Usage(
            "--format tsv is not supported for history queries; use json, table, or csv instead"
                .to_string(),
        )),
        OutputFormatKind::Ndjson => Err(CliError::Usage(
            "--format ndjson is not supported for history queries; use json, table, or csv instead"
                .to_string(),
        )),
    }
}

/// Field accessors for one history result row.
///
/// History queries project a select list, so the engine returns each row as a
/// positional JSON array matching the projected variable order:
///   - no predicate filter: `[?p, ?v, ?t, ?op]`
///   - predicate filter:     `[?v, ?t, ?op]`
///
/// Object-keyed rows (`{"?t": ...}`) are also accepted defensively.
struct HistoryRow<'a> {
    predicate: Option<&'a serde_json::Value>,
    value: Option<&'a serde_json::Value>,
    t: Option<&'a serde_json::Value>,
    op: Option<&'a serde_json::Value>,
}

impl<'a> HistoryRow<'a> {
    fn from_value(row: &'a serde_json::Value) -> Self {
        match row {
            serde_json::Value::Array(cols) if cols.len() >= 4 => HistoryRow {
                predicate: cols.first(),
                value: cols.get(1),
                t: cols.get(2),
                op: cols.get(3),
            },
            serde_json::Value::Array(cols) if cols.len() == 3 => HistoryRow {
                predicate: None,
                value: cols.first(),
                t: cols.get(1),
                op: cols.get(2),
            },
            serde_json::Value::Object(_) => HistoryRow {
                predicate: row.get("?p"),
                value: row.get("?v"),
                t: row.get("?t"),
                op: row.get("?op"),
            },
            _ => HistoryRow {
                predicate: None,
                value: None,
                t: None,
                op: None,
            },
        }
    }

    fn t_str(&self) -> String {
        self.t
            .and_then(serde_json::Value::as_i64)
            .map(|n| n.to_string())
            .unwrap_or_default()
    }

    fn op_str(&self) -> &'static str {
        self.op
            .and_then(serde_json::Value::as_bool)
            .map(|b| if b { "+" } else { "-" })
            .unwrap_or("?")
    }
}

fn format_history_table(json: &serde_json::Value) -> CliResult<String> {
    use comfy_table::{ContentArrangement, Table};

    let arr = match json.as_array() {
        Some(a) => a,
        None => return Ok(serde_json::to_string_pretty(json).unwrap_or_default()),
    };

    if arr.is_empty() {
        return Ok("(no history found)".to_string());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    // Determine columns from first row: the predicate is projected only when
    // the query was not filtered to a single predicate.
    let has_predicate = arr
        .first()
        .map(|r| HistoryRow::from_value(r).predicate.is_some())
        .unwrap_or(false);

    if has_predicate {
        table.set_header(["t", "op", "predicate", "value"]);
    } else {
        table.set_header(["t", "op", "value"]);
    }

    for row in arr {
        let fields = HistoryRow::from_value(row);
        let t = fields.t_str();
        let op = fields.op_str();
        let val = format_value(fields.value);

        if has_predicate {
            let pred = format_value(fields.predicate);
            table.add_row([t, op.to_string(), pred, val]);
        } else {
            table.add_row([t, op.to_string(), val]);
        }
    }

    Ok(table.to_string())
}

fn format_history_csv(json: &serde_json::Value) -> CliResult<String> {
    let arr = match json.as_array() {
        Some(a) => a,
        None => return Ok(serde_json::to_string_pretty(json).unwrap_or_default()),
    };

    if arr.is_empty() {
        return Ok(String::new());
    }

    let has_predicate = arr
        .first()
        .map(|r| HistoryRow::from_value(r).predicate.is_some())
        .unwrap_or(false);

    let mut lines = Vec::new();

    // Header
    if has_predicate {
        lines.push("t,op,predicate,value".to_string());
    } else {
        lines.push("t,op,value".to_string());
    }

    for row in arr {
        let fields = HistoryRow::from_value(row);
        let t = fields.t_str();
        let op = fields.op_str();
        let val = csv_escape(&format_value(fields.value));

        if has_predicate {
            let pred = csv_escape(&format_value(fields.predicate));
            lines.push(format!("{t},{op},{pred},{val}"));
        } else {
            lines.push(format!("{t},{op},{val}"));
        }
    }

    Ok(lines.join("\n"))
}

fn format_value(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Object(obj)) => {
            // Handle {"@value": ..., "@type": ...} or {"@id": ...}
            if let Some(val) = obj.get("@value") {
                return format_value(Some(val));
            }
            if let Some(id) = obj.get("@id") {
                return format_value(Some(id));
            }
            serde_json::to_string(&serde_json::Value::Object(obj.clone())).unwrap_or_default()
        }
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unfiltered history rows arrive as `[?p, ?v, ?t, ?op]` positional arrays.
    #[test]
    fn table_renders_positional_rows_with_predicate() {
        let json = serde_json::json!([
            ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "ex:Article", 1, true],
            ["ex:content", {"@value": "v2", "@type": "https://ns.flur.ee/db#fullText"}, 4, true],
            ["ex:content", {"@value": "v1", "@type": "https://ns.flur.ee/db#fullText"}, 4, false],
        ]);

        let out = format_history_table(&json).unwrap();

        assert!(out.contains("predicate"), "predicate column header missing");
        // t/op/value/predicate cells are populated, not blank or `?`.
        assert!(out.contains("ex:Article"));
        assert!(out.contains("rdf-syntax-ns#type"));
        assert!(out.contains('1') && out.contains('4'));
        assert!(out.contains('+') && out.contains('-'));
        assert!(out.contains("v1") && out.contains("v2"));
        assert!(
            !out.contains('?'),
            "rows should not contain `?` placeholders"
        );
    }

    /// Predicate-filtered history rows arrive as `[?v, ?t, ?op]` (no `?p`).
    #[test]
    fn table_renders_positional_rows_without_predicate() {
        let json = serde_json::json!([["Alice", 1, true], ["Alice Smith", 2, true],]);

        let out = format_history_table(&json).unwrap();

        assert!(
            !out.contains("predicate"),
            "predicate column should be absent"
        );
        assert!(out.contains("Alice Smith"));
        assert!(out.contains('1') && out.contains('2'));
        assert!(
            !out.contains('?'),
            "rows should not contain `?` placeholders"
        );
    }

    #[test]
    fn csv_renders_positional_rows() {
        let json = serde_json::json!([["http://schema.org/name", "Deployment Runbook", 1, true],]);

        let out = format_history_csv(&json).unwrap();
        let lines: Vec<&str> = out.lines().collect();

        assert_eq!(lines[0], "t,op,predicate,value");
        assert_eq!(lines[1], "1,+,http://schema.org/name,Deployment Runbook");
    }

    /// Object-keyed rows remain supported for robustness.
    #[test]
    fn table_still_handles_object_rows() {
        let json = serde_json::json!([
            {"?p": "ex:name", "?v": "Alice", "?t": 1, "?op": true},
        ]);

        let out = format_history_table(&json).unwrap();
        assert!(out.contains("ex:name") && out.contains("Alice") && out.contains('1'));
    }

    #[test]
    fn empty_history_reports_none_found() {
        let json = serde_json::json!([]);
        assert_eq!(format_history_table(&json).unwrap(), "(no history found)");
    }
}
