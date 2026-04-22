use crate::config;
use crate::context;
use crate::error::{CliError, CliResult};
use crate::output::OutputFormatKind;
use fluree_db_api::server_defaults::FlureeDir;
use std::path::Path;

pub async fn run(
    entity: &str,
    ledger: Option<&str>,
    from: &str,
    to: &str,
    predicate: Option<&str>,
    format_str: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for tracked ledger — history requires local query execution
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let alias = context::resolve_ledger(ledger, dirs)?;
    if store.get_tracked(&alias).is_some()
        || store.get_tracked(&context::to_ledger_id(&alias)).is_some()
    {
        return Err(CliError::Usage(
            "history is not available for tracked ledgers (no server endpoint).\n  \
             Use `fluree track status` to check remote state instead."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;

    // Expand compact IRIs using stored prefixes
    let entity_iri = config::expand_iri(dirs.data_dir(), entity);
    let predicate_iri = predicate.map(|p| config::expand_iri(dirs.data_dir(), p));

    // Build the history query
    let query = build_history_query(
        &alias,
        &entity_iri,
        from,
        to,
        predicate_iri.as_deref(),
        dirs.data_dir(),
    );

    // Parse output format
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

    // Execute the query via connection (required for from/to history support)
    let ledger_view = fluree.ledger(&alias).await?;
    let result = fluree.query_connection(&query).await?;
    let json = result.to_jsonld(&ledger_view.snapshot)?;

    // Format output
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
        OutputFormatKind::Json | OutputFormatKind::TypedJson => {
            Ok(serde_json::to_string_pretty(json).unwrap_or_else(|_| json.to_string()))
        }
        OutputFormatKind::Table => format_history_table(json),
        OutputFormatKind::Csv => format_history_csv(json),
        OutputFormatKind::Tsv => Err(CliError::Usage(
            "--format tsv is not supported for history queries; use json, table, or csv instead"
                .to_string(),
        )),
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

    // Determine columns from first row
    let has_predicate = arr.first().map(|r| r.get("?p").is_some()).unwrap_or(false);

    if has_predicate {
        table.set_header(["t", "op", "predicate", "value"]);
    } else {
        table.set_header(["t", "op", "value"]);
    }

    for row in arr {
        let t = row
            .get("?t")
            .and_then(serde_json::Value::as_i64)
            .map(|n| n.to_string())
            .unwrap_or_default();
        let op = row
            .get("?op")
            .and_then(serde_json::Value::as_bool)
            .map(|b| if b { "+" } else { "-" })
            .unwrap_or("?");
        let val = format_value(row.get("?v"));

        if has_predicate {
            let pred = format_value(row.get("?p"));
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

    let has_predicate = arr.first().map(|r| r.get("?p").is_some()).unwrap_or(false);

    let mut lines = Vec::new();

    // Header
    if has_predicate {
        lines.push("t,op,predicate,value".to_string());
    } else {
        lines.push("t,op,value".to_string());
    }

    for row in arr {
        let t = row
            .get("?t")
            .and_then(serde_json::Value::as_i64)
            .map(|n| n.to_string())
            .unwrap_or_default();
        let op = row
            .get("?op")
            .and_then(serde_json::Value::as_bool)
            .map(|b| if b { "+" } else { "-" })
            .unwrap_or("?");
        let val = csv_escape(&format_value(row.get("?v")));

        if has_predicate {
            let pred = csv_escape(&format_value(row.get("?p")));
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
