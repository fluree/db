use crate::detect::QueryFormat;
use crate::error::CliResult;
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::format::IriCompactor;
use fluree_db_api::QueryResult;
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::{FlakeValue, LedgerSnapshot};
use fluree_db_query::binding::Binding;

/// Output format for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormatKind {
    Json,
    TypedJson,
    Table,
    Csv,
    Tsv,
}

impl std::fmt::Display for OutputFormatKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => f.write_str("json"),
            Self::TypedJson => f.write_str("typed-json"),
            Self::Table => f.write_str("table"),
            Self::Csv => f.write_str("csv"),
            Self::Tsv => f.write_str("tsv"),
        }
    }
}

/// Result of formatting: the rendered string plus the total row count.
pub struct FormatOutput {
    pub text: String,
    pub total_rows: usize,
}

/// Fast-path SPARQL table formatting directly from `QueryResult` (no intermediate JSON).
///
/// Returns:
/// - `Ok(Some(output))` when formatting succeeded
/// - `Ok(None)` when the result contains grouped bindings that require SPARQL disaggregation;
///   callers should fall back to the JSON-based formatter for correctness.
pub fn format_sparql_table_from_result(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    limit: Option<usize>,
) -> CliResult<Option<FormatOutput>> {
    // ASK queries: display boolean result directly instead of an empty table.
    if result.output.is_boolean() {
        let has_solution = result.batches.iter().any(|b| !b.is_empty());
        return Ok(Some(FormatOutput {
            text: has_solution.to_string(),
            total_rows: 1,
        }));
    }

    // Grouped bindings require cartesian disaggregation (SPARQL formatter logic).
    // Rather than re-implement that here, fall back to the existing SPARQL JSON formatter.
    let compactor = IriCompactor::new(snapshot.namespaces(), &result.context);
    let gv = result.binary_graph.as_ref();

    let head_var_ids: Vec<fluree_db_query::VarId> = if result.output.is_wildcard() {
        result
            .batches
            .first()
            .map(|b| {
                b.schema()
                    .iter()
                    .copied()
                    // Skip internal variables (?__pp0, ?__s0, etc.) from wildcard output.
                    .filter(|&vid| !result.vars.name(vid).starts_with("?__"))
                    .collect()
            })
            .unwrap_or_else(|| {
                // Empty result set: derive vars from the registry (all user-visible variables).
                result
                    .vars
                    .iter()
                    .filter(|(name, _)| !name.starts_with("?__"))
                    .map(|(_, id)| id)
                    .collect()
            })
    } else {
        result.output.projected_vars_or_empty().to_vec()
    };

    // Match SPARQL JSON head var behavior: strip '?' and sort lexicographically.
    let mut head_pairs: Vec<(String, fluree_db_query::VarId)> = head_var_ids
        .iter()
        .map(|&var_id| (strip_question_mark(result.vars.name(var_id)), var_id))
        .collect();
    head_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let headers: Vec<String> = head_pairs.iter().map(|(name, _)| name.clone()).collect();
    if headers.is_empty() {
        return Ok(Some(FormatOutput {
            text: "(empty result set)".to_string(),
            total_rows: 0,
        }));
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(&headers);

    let mut printed = 0usize;
    let max_rows = limit.unwrap_or(usize::MAX);

    // SelectOne should render only a single row (parity with SPARQL formatter).
    let select_one = result.output.is_select_one();

    for batch in &result.batches {
        for row in 0..batch.len() {
            let mut cells: Vec<String> = Vec::with_capacity(head_pairs.len());
            for (_, var_id) in &head_pairs {
                let b = batch.get(row, *var_id).unwrap_or(&Binding::Unbound);
                match sparql_table_cell(b, &compactor, gv) {
                    Ok(cell) => cells.push(cell),
                    Err(SparqlTableFastPath::NeedsDisaggregation) => return Ok(None),
                }
            }
            table.add_row(cells);
            printed += 1;

            if select_one || printed >= max_rows {
                break;
            }
        }
        if select_one || printed >= max_rows {
            break;
        }
    }

    let total_rows = if select_one {
        usize::from(printed > 0)
    } else {
        result.row_count()
    };

    Ok(Some(FormatOutput {
        text: table.to_string(),
        total_rows,
    }))
}

#[derive(Debug)]
enum SparqlTableFastPath {
    NeedsDisaggregation,
}

fn strip_question_mark(var_name: &str) -> String {
    var_name.strip_prefix('?').unwrap_or(var_name).to_string()
}

fn sparql_table_cell(
    b: &Binding,
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
) -> Result<String, SparqlTableFastPath> {
    let s = match b {
        Binding::Unbound | Binding::Poisoned => String::new(),

        // Use display compaction (includes auto-derived fallback prefixes)
        Binding::Sid { sid, .. } => {
            compact_bnode_strip(compactor.compact_sid_for_display(sid).ok())
        }
        Binding::IriMatch { iri, .. } => {
            compact_bnode_strip(compactor.compact_iri_for_display(iri).ok())
        }
        Binding::Iri(iri) => compact_bnode_strip(compactor.compact_iri_for_display(iri).ok()),

        Binding::Lit { val, .. } => flake_value_to_table_cell(val, compactor),

        Binding::EncodedSid { s_id, .. } => {
            let Some(gv) = gv else {
                return Ok(format!("{b:?}"));
            };
            match gv.store().resolve_subject_iri(*s_id) {
                Ok(iri) => compact_bnode_strip(compactor.compact_iri_for_display(&iri).ok()),
                Err(_) => format!("{b:?}"),
            }
        }
        Binding::EncodedPid { p_id } => {
            let Some(gv) = gv else {
                return Ok(format!("{b:?}"));
            };
            match gv.store().resolve_predicate_iri(*p_id) {
                Some(iri) => compact_bnode_strip(compactor.compact_iri_for_display(iri).ok()),
                None => format!("{b:?}"),
            }
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => {
            let Some(gv) = gv else {
                return Ok(format!("{b:?}"));
            };
            match gv.decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id) {
                Ok(v) => flake_value_to_table_cell(&v, compactor),
                Err(_) => format!("{b:?}"),
            }
        }

        // Grouped values must be disaggregated into multiple rows for SPARQL semantics.
        Binding::Grouped(_) => return Err(SparqlTableFastPath::NeedsDisaggregation),
    };
    Ok(s)
}

fn compact_bnode_strip(compacted: Option<String>) -> String {
    let Some(s) = compacted else {
        return String::new();
    };
    s.strip_prefix("_:").unwrap_or(&s).to_string()
}

fn flake_value_to_table_cell(v: &FlakeValue, compactor: &IriCompactor) -> String {
    match v {
        FlakeValue::String(s) => s.clone(),
        FlakeValue::Long(n) => n.to_string(),
        FlakeValue::Double(d) => d.to_string(),
        FlakeValue::Boolean(b) => b.to_string(),
        FlakeValue::Vector(v) => serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string()),
        FlakeValue::Json(s) => s.clone(),
        FlakeValue::Ref(sid) => compact_bnode_strip(compactor.compact_sid_for_display(sid).ok()),
        FlakeValue::Null => String::new(),
        other => other.to_string(),
    }
}

/// Format a query result JSON value for display.
///
/// When `limit` is `Some(n)`, only the first `n` rows are rendered.
/// `total_rows` always reflects the *untruncated* result set size.
pub fn format_result(
    json: &serde_json::Value,
    format: OutputFormatKind,
    query_format: QueryFormat,
    limit: Option<usize>,
) -> CliResult<FormatOutput> {
    match format {
        OutputFormatKind::Json | OutputFormatKind::TypedJson => {
            format_json(json, query_format, limit)
        }
        OutputFormatKind::Table => format_as_table(json, query_format, limit),
        OutputFormatKind::Csv | OutputFormatKind::Tsv => {
            // TSV/CSV should be handled before reaching this function (via QueryResult methods).
            // If we get here, the caller didn't have access to the raw QueryResult.
            Err(crate::error::CliError::Usage(format!(
                "{format} format requires direct access to query results (not available for remote queries)",
            )))
        }
    }
}

fn format_json(
    json: &serde_json::Value,
    query_format: QueryFormat,
    limit: Option<usize>,
) -> CliResult<FormatOutput> {
    let (total, output_json) = match query_format {
        QueryFormat::Sparql => {
            let total = sparql_row_count(json);
            match limit {
                Some(n) if n < total => {
                    let mut truncated = json.clone();
                    if let Some(bindings) = truncated
                        .pointer_mut("/results/bindings")
                        .and_then(|v| v.as_array_mut())
                    {
                        bindings.truncate(n);
                    }
                    (total, truncated)
                }
                _ => (total, json.clone()),
            }
        }
        QueryFormat::JsonLd => {
            let total = fql_row_count(json);
            match limit {
                Some(n) if n < total => {
                    let mut truncated = json.clone();
                    if let Some(arr) = truncated.as_array_mut() {
                        arr.truncate(n);
                    }
                    (total, truncated)
                }
                _ => (total, json.clone()),
            }
        }
    };
    let text =
        serde_json::to_string_pretty(&output_json).unwrap_or_else(|_| output_json.to_string());
    Ok(FormatOutput {
        text,
        total_rows: total,
    })
}

fn format_as_table(
    json: &serde_json::Value,
    query_format: QueryFormat,
    limit: Option<usize>,
) -> CliResult<FormatOutput> {
    match query_format {
        QueryFormat::Sparql => format_sparql_table(json, limit),
        QueryFormat::JsonLd => format_jsonld_table(json, limit),
    }
}

fn sparql_row_count(json: &serde_json::Value) -> usize {
    json.pointer("/results/bindings")
        .and_then(|v| v.as_array())
        .map(std::vec::Vec::len)
        .unwrap_or(0)
}

fn fql_row_count(json: &serde_json::Value) -> usize {
    json.as_array().map(std::vec::Vec::len).unwrap_or(0)
}

fn format_sparql_table(json: &serde_json::Value, limit: Option<usize>) -> CliResult<FormatOutput> {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let vars = json
        .pointer("/head/vars")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if vars.is_empty() {
        return Ok(FormatOutput {
            text: serde_json::to_string_pretty(json).unwrap_or_default(),
            total_rows: 0,
        });
    }

    table.set_header(&vars);

    let bindings = json.pointer("/results/bindings").and_then(|v| v.as_array());
    let total_rows = bindings.map(std::vec::Vec::len).unwrap_or(0);

    if let Some(rows) = bindings {
        let display_rows: &[serde_json::Value] = match limit {
            Some(n) if n < rows.len() => &rows[..n],
            _ => rows,
        };
        for row in display_rows {
            let cells: Vec<String> = vars
                .iter()
                .map(|var| {
                    row.get(var)
                        .and_then(|b| b.get("value"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            table.add_row(cells);
        }
    }

    Ok(FormatOutput {
        text: table.to_string(),
        total_rows,
    })
}

fn format_jsonld_table(json: &serde_json::Value, limit: Option<usize>) -> CliResult<FormatOutput> {
    let arr = match json.as_array() {
        Some(a) => a,
        None => {
            return Ok(FormatOutput {
                text: serde_json::to_string_pretty(json).unwrap_or_default(),
                total_rows: 0,
            })
        }
    };

    let total_rows = arr.len();
    if arr.is_empty() {
        return Ok(FormatOutput {
            text: "(empty result set)".to_string(),
            total_rows: 0,
        });
    }

    // Collect all keys from all objects for column headers
    let mut columns: Vec<String> = Vec::new();
    for obj in arr {
        if let Some(map) = obj.as_object() {
            for key in map.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(&columns);

    let display_rows: &[serde_json::Value] = match limit {
        Some(n) if n < arr.len() => &arr[..n],
        _ => arr,
    };
    for obj in display_rows {
        let cells: Vec<String> = columns
            .iter()
            .map(|col| {
                obj.get(col)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default()
            })
            .collect();
        table.add_row(cells);
    }

    Ok(FormatOutput {
        text: table.to_string(),
        total_rows,
    })
}
