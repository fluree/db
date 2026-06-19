//! AgentJson format — optimized for LLM/agent consumption
//!
//! Produces a self-describing envelope with:
//! - **schema**: per-variable datatype (extracted in a single pass)
//! - **rows**: compact JSON objects with native types
//! - **rowCount**: number of rows included
//! - **t** / **iso**: time-pinning metadata
//! - **hasMore** / **message** / **resume**: pagination when truncated

use std::collections::{BTreeSet, HashMap};

use serde_json::{json, Map, Value as JsonValue};

use super::iri::IriCompactor;
use super::json_write::{push_bool, push_i64, push_json_string, push_value};
use super::Result;
use crate::QueryResult;
use fluree_db_query::binding::Binding;
use fluree_db_query::VarId;

/// Format query results as an AgentJson envelope
pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    config: &super::config::FormatterConfig,
) -> Result<JsonValue> {
    let select_vars = if result.output.is_wildcard() {
        None
    } else {
        Some(result.output.projected_vars_or_empty())
    };
    let select_vars = select_vars.as_deref();

    let max_bytes = config.max_bytes;
    let total_row_hint = result
        .batches
        .iter()
        .map(fluree_db_query::Batch::len)
        .sum::<usize>();
    let mut rows = Vec::with_capacity(if max_bytes.is_some() {
        total_row_hint.min(256) // don't over-allocate when truncating
    } else {
        total_row_hint
    });
    let mut type_map: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let mut cumulative_bytes: usize = 0;
    let mut has_more = false;
    // Scratch buffer for byte measurement — reused across rows to avoid allocation
    let mut size_buf: Vec<u8> = if max_bytes.is_some() {
        Vec::with_capacity(512)
    } else {
        Vec::new()
    };

    'outer: for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let vars_to_scan = select_vars.unwrap_or_else(|| batch.schema());

            // Single pass: format row values AND extract types simultaneously
            let row = format_row_with_types(
                result,
                batch,
                row_idx,
                vars_to_scan,
                &result.vars,
                compactor,
                &mut type_map,
            )?;

            // Check byte budget using scratch buffer
            if let Some(budget) = max_bytes {
                size_buf.clear();
                if serde_json::to_writer(&mut size_buf, &row).is_ok() {
                    if cumulative_bytes + size_buf.len() > budget && !rows.is_empty() {
                        has_more = true;
                        break 'outer;
                    }
                    cumulative_bytes += size_buf.len();
                }
            }

            rows.push(row);
        }
    }

    let row_count = rows.len();

    // Build schema from collected types
    let schema = build_schema(&type_map, &result.vars);

    // Build envelope
    let mut envelope = Map::new();
    envelope.insert("schema".to_string(), schema);
    envelope.insert("rows".to_string(), JsonValue::Array(rows));
    envelope.insert("rowCount".to_string(), json!(row_count));

    // Time-pinning metadata
    // `t` is only present for single-ledger queries (Option<i64>)
    if let Some(t) = result.t {
        let include_t = match config.agent_json_context {
            Some(ref ctx) => ctx.from_count <= 1,
            None => true, // no context: single-ledger assumed
        };
        if include_t {
            envelope.insert("t".to_string(), json!(t));
        }
    }
    if let Some(ref ctx) = config.agent_json_context {
        if let Some(ref iso) = ctx.iso_timestamp {
            envelope.insert("iso".to_string(), JsonValue::String(iso.clone()));
        }
    }

    envelope.insert("hasMore".to_string(), JsonValue::Bool(has_more));

    if row_count == 0 {
        envelope.insert(
            "message".to_string(),
            JsonValue::String(NO_RESULTS_MESSAGE.to_string()),
        );
    }

    if has_more {
        let (resume, msg) =
            truncation_resume_and_message(config, result.t, row_count, total_row_hint, max_bytes);
        if let Some(resume) = resume {
            envelope.insert("resume".to_string(), JsonValue::String(resume));
        }
        envelope.insert("message".to_string(), JsonValue::String(msg));
    }

    Ok(JsonValue::Object(envelope))
}

/// Message used when a query returns zero rows.
const NO_RESULTS_MESSAGE: &str =
    "Query returned no results. The schema is empty because no rows were \
     available to infer types from. This does not necessarily indicate an \
     incorrect query — the data may not exist for the given constraints.";

/// Build the `(resume, message)` pair for a size-truncated response.
///
/// Shared by the DOM and streaming envelopes so the wording and resume-query
/// rules stay in lockstep. `resume` is `Some` only for the single-FROM,
/// `@t:`-pinnable case.
fn truncation_resume_and_message(
    config: &super::config::FormatterConfig,
    t: Option<i64>,
    row_count: usize,
    total_row_hint: usize,
    max_bytes: Option<usize>,
) -> (Option<String>, String) {
    let budget_str = max_bytes.map(|b| b.to_string()).unwrap_or_default();
    let mut msg = format!(
        "Response truncated due to size limit of {budget_str} bytes. {row_count} of {total_row_hint} total rows included."
    );
    let mut resume = None;

    if let Some(ref ctx) = config.agent_json_context {
        if ctx.from_count <= 1 {
            if let (Some(ref sparql), Some(t)) = (&ctx.sparql_text, t) {
                if let Some(q) = generate_resume_query(sparql, t, row_count, ctx.resume_limit) {
                    msg = format!(
                        "Response truncated due to size limit of {budget_str} bytes. \
                         Use the query below to retrieve the next batch."
                    );
                    resume = Some(q);
                }
            }
        } else if let Some(ref iso) = ctx.iso_timestamp {
            // Multi-ledger: advise using @iso: for time-pinning
            msg.push_str(&format!(
                " To retrieve the next batch, re-issue your query with \
                 @iso:{} on each FROM clause and add OFFSET {} LIMIT {}.",
                iso, row_count, ctx.resume_limit
            ));
        }
    }

    (resume, msg)
}

/// Stream the AgentJson envelope directly into a `String`, byte-identical to
/// `serde_json::to_string(&format(...))`.
///
/// Rows stream into an inner buffer (no per-row `serde_json::Value`), the byte
/// budget is measured from buffer-length deltas (no separate size-measuring
/// serialization), and the schema / metadata envelope is assembled around the
/// rows in serde's insertion order.
pub fn format_string(
    result: &QueryResult,
    compactor: &IriCompactor,
    config: &super::config::FormatterConfig,
) -> Result<String> {
    let select_vars = if result.output.is_wildcard() {
        None
    } else {
        Some(result.output.projected_vars_or_empty())
    };
    let select_vars = select_vars.as_deref();

    let max_bytes = config.max_bytes;
    let total_row_hint = result
        .batches
        .iter()
        .map(fluree_db_query::Batch::len)
        .sum::<usize>();

    let mut type_map: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let mut cumulative_bytes: usize = 0;
    let mut has_more = false;
    let mut row_count: usize = 0;

    // Stream the row objects (comma-separated) into their own buffer so the
    // schema — computed from the same pass — can be written ahead of them.
    //
    // When a byte budget is set the buffer never exceeds `budget` (the running
    // total is capped, and the one over-budget row is truncated away), so cap the
    // preallocation at the budget rather than sizing for the full untruncated row
    // count — otherwise a large result set with a small budget would still
    // allocate megabytes up front, defeating the truncation path. Mirrors the DOM
    // path's `total_row_hint.min(256)` guard.
    let full_est = total_row_hint.saturating_mul(64).saturating_add(16);
    let est_rows = match max_bytes {
        Some(budget) => budget.saturating_add(64).min(full_est),
        None => full_est,
    };
    let mut rows_buf = String::with_capacity(est_rows);

    'outer: for batch in &result.batches {
        let vars_to_scan = select_vars.unwrap_or_else(|| batch.schema());
        for row_idx in 0..batch.len() {
            let pre = rows_buf.len();
            if row_count > 0 {
                rows_buf.push(',');
            }
            let row_start = rows_buf.len();

            // Formats the row AND records its types — done before the budget
            // check, exactly as the DOM path does (so a budget-rejected row's
            // types still contribute to the schema).
            write_row_with_types(
                &mut rows_buf,
                result,
                batch,
                row_idx,
                vars_to_scan,
                &result.vars,
                compactor,
                &mut type_map,
            )?;
            let row_size = rows_buf.len() - row_start;

            if let Some(budget) = max_bytes {
                if cumulative_bytes + row_size > budget && row_count > 0 {
                    has_more = true;
                    rows_buf.truncate(pre); // drop the comma + rejected row
                    break 'outer;
                }
                cumulative_bytes += row_size;
            }

            row_count += 1;
        }
    }

    // Schema (built from the types collected above).
    let schema = build_schema(&type_map, &result.vars);

    let mut out = String::with_capacity(rows_buf.len() + 256);
    out.push_str("{\"schema\":");
    push_value(&mut out, &schema)?;
    out.push_str(",\"rows\":[");
    out.push_str(&rows_buf);
    out.push_str("],\"rowCount\":");
    push_i64(&mut out, row_count as i64);

    if let Some(t) = result.t {
        let include_t = match config.agent_json_context {
            Some(ref ctx) => ctx.from_count <= 1,
            None => true,
        };
        if include_t {
            out.push_str(",\"t\":");
            push_i64(&mut out, t);
        }
    }
    if let Some(ref ctx) = config.agent_json_context {
        if let Some(ref iso) = ctx.iso_timestamp {
            out.push_str(",\"iso\":");
            push_json_string(&mut out, iso);
        }
    }

    out.push_str(",\"hasMore\":");
    push_bool(&mut out, has_more);

    if row_count == 0 {
        out.push_str(",\"message\":");
        push_json_string(&mut out, NO_RESULTS_MESSAGE);
    }

    if has_more {
        let (resume, msg) =
            truncation_resume_and_message(config, result.t, row_count, total_row_hint, max_bytes);
        if let Some(resume) = resume {
            out.push_str(",\"resume\":");
            push_json_string(&mut out, &resume);
        }
        out.push_str(",\"message\":");
        push_json_string(&mut out, &msg);
    }

    out.push('}');
    Ok(out)
}

/// Stream one row object (`{"?v":value,...}`) into `out`, recording each cell's
/// datatype label in `type_map`. Mirrors [`format_row_with_types`].
#[allow(clippy::too_many_arguments)]
fn write_row_with_types(
    out: &mut String,
    result: &QueryResult,
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    vars: &[VarId],
    registry: &fluree_db_query::VarRegistry,
    compactor: &IriCompactor,
    type_map: &mut HashMap<VarId, BTreeSet<String>>,
) -> Result<()> {
    out.push('{');
    let mut first = true;
    for &var_id in vars {
        let var_name = registry.name(var_id);
        if var_name.starts_with("?__") {
            continue;
        }

        // Resolve the cell to (value-source binding, type label). Encoded
        // bindings are materialized once and reused for both, matching the DOM.
        let binding = batch.get(row_idx, var_id);

        if !first {
            out.push(',');
        }
        first = false;
        push_json_string(out, var_name);
        out.push(':');

        match binding {
            None | Some(Binding::Unbound | Binding::Poisoned) => out.push_str("null"),
            Some(b) if b.is_encoded() => {
                let m = super::materialize::materialize_binding(result, b)?;
                super::jsonld::write_value_with_result(out, result, &m, compactor)?;
                if let Some(label) = binding_type_label(&m, compactor)? {
                    type_map.entry(var_id).or_default().insert(label);
                }
            }
            Some(b) => {
                super::jsonld::write_value_with_result(out, result, b, compactor)?;
                if let Some(label) = binding_type_label(b, compactor)? {
                    type_map.entry(var_id).or_default().insert(label);
                }
            }
        }
    }
    out.push('}');
    Ok(())
}

/// Format a single row as a JSON object AND extract type info in one pass.
///
/// Each binding is visited exactly once: its value is formatted and its datatype
/// is recorded in `type_map`.
fn format_row_with_types(
    result: &QueryResult,
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    vars: &[VarId],
    registry: &fluree_db_query::VarRegistry,
    compactor: &IriCompactor,
    type_map: &mut HashMap<VarId, BTreeSet<String>>,
) -> Result<JsonValue> {
    let mut obj = Map::new();

    for &var_id in vars {
        let var_name = registry.name(var_id);

        // Skip internal variables
        if var_name.starts_with("?__") {
            continue;
        }

        let binding = match batch.get(row_idx, var_id) {
            Some(b) => b,
            None => {
                obj.insert(var_name.to_string(), JsonValue::Null);
                continue;
            }
        };

        if matches!(binding, Binding::Unbound | Binding::Poisoned) {
            obj.insert(var_name.to_string(), JsonValue::Null);
            continue;
        }

        // Handle encoded bindings: materialize once, then format + extract type
        let (value, type_label) = if binding.is_encoded() {
            let materialized = super::materialize::materialize_binding(result, binding)?;
            let val = super::jsonld::format_binding_with_result(result, &materialized, compactor)?;
            let tl = binding_type_label(&materialized, compactor)?;
            (val, tl)
        } else {
            let val = super::jsonld::format_binding_with_result(result, binding, compactor)?;
            let tl = binding_type_label(binding, compactor)?;
            (val, tl)
        };

        obj.insert(var_name.to_string(), value);

        if let Some(label) = type_label {
            type_map.entry(var_id).or_default().insert(label);
        }
    }

    Ok(JsonValue::Object(obj))
}

/// Extract the compact datatype label from a (non-encoded) binding.
///
/// Returns `None` for Unbound/Poisoned (caller already handles those).
fn binding_type_label(binding: &Binding, compactor: &IriCompactor) -> Result<Option<String>> {
    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(None),
        Binding::Sid { .. } | Binding::IriMatch { .. } | Binding::Iri(_) => {
            Ok(Some("uri".to_string()))
        }
        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => Ok(Some("uri".to_string())),
        Binding::Lit { dtc, .. } => {
            if dtc.lang_tag().is_some() {
                Ok(Some("rdf:langString".to_string()))
            } else {
                Ok(Some(compactor.compact_sid(dtc.datatype())?))
            }
        }
        Binding::EncodedLit { .. } => Ok(None), // shouldn't reach here after materialization
        Binding::Grouped(_) => Ok(Some("grouped".to_string())),
    }
}

/// Build the schema JSON from collected type information
fn build_schema(
    type_map: &HashMap<VarId, BTreeSet<String>>,
    vars: &fluree_db_query::VarRegistry,
) -> JsonValue {
    let mut schema = Map::new();

    // Sort by variable name for deterministic output
    let mut entries: Vec<_> = type_map.iter().collect();
    entries.sort_by_key(|(vid, _)| vars.name(**vid).to_string());

    for (var_id, types) in entries {
        let var_name = vars.name(*var_id);
        if var_name.starts_with("?__") {
            continue;
        }
        let type_val = if types.len() == 1 {
            JsonValue::String(types.iter().next().unwrap().clone())
        } else {
            JsonValue::Array(types.iter().map(|t| JsonValue::String(t.clone())).collect())
        };
        schema.insert(var_name.to_string(), type_val);
    }

    JsonValue::Object(schema)
}

/// Generate a resume SPARQL query for single-FROM pagination
///
/// Rewrites the original SPARQL to pin time with `@t:` and add OFFSET/LIMIT.
/// Returns `None` if the query has zero or multiple FROM clauses.
fn generate_resume_query(sparql: &str, t: i64, row_count: usize, limit: usize) -> Option<String> {
    // Find all FROM <...> occurrences (case insensitive)
    let lower = sparql.to_lowercase();
    let from_positions: Vec<usize> = lower
        .match_indices("from")
        .filter(|(pos, _)| {
            // Must be followed by whitespace then '<' (not "from named")
            let rest = &lower[pos + 4..];
            let trimmed = rest.trim_start();
            trimmed.starts_with('<') && !rest.trim_start().starts_with("named")
        })
        .map(|(pos, _)| pos)
        .collect();

    if from_positions.len() != 1 {
        return None;
    }

    let from_pos = from_positions[0];

    // Find the angle-bracket IRI
    let open = sparql[from_pos..].find('<')? + from_pos;
    let close = sparql[open..].find('>')? + open;
    let iri = &sparql[open + 1..close];

    // Pin with @t: (replace existing time-travel suffix if present)
    let base_iri = if let Some(at_pos) = iri.rfind('@') {
        &iri[..at_pos]
    } else {
        iri
    };
    let pinned_iri = format!("{base_iri}@t:{t}");

    // Rebuild query with pinned IRI
    let mut result = String::with_capacity(sparql.len() + 32);
    result.push_str(&sparql[..=open]);
    result.push_str(&pinned_iri);
    result.push_str(&sparql[close..]);

    // Strip existing OFFSET and LIMIT (case insensitive)
    result = strip_clause(&result, "offset");
    result = strip_clause(&result, "limit");

    // Append new OFFSET and LIMIT
    let trimmed = result.trim_end();
    format!("{trimmed} OFFSET {row_count} LIMIT {limit}").into()
}

/// Remove a SPARQL clause like "OFFSET 10" or "LIMIT 50" (case insensitive).
///
/// Uses word-boundary checks so that IRIs containing the keyword as a substring
/// (e.g. `<http://example.com/offsetValue>`) are not matched.
fn strip_clause(sparql: &str, keyword: &str) -> String {
    let lower = sparql.to_lowercase();
    let kw_len = keyword.len();

    // Search for the keyword on a word boundary
    let mut search_from = 0;
    while let Some(rel) = lower[search_from..].find(keyword) {
        let pos = search_from + rel;

        // Check word boundary: char before must be non-alphanumeric (or start of string)
        let boundary_before = pos == 0
            || !sparql.as_bytes()[pos - 1].is_ascii_alphanumeric()
                && sparql.as_bytes()[pos - 1] != b'_';

        // Char after must be non-alphanumeric (or end of string)
        let boundary_after = pos + kw_len >= sparql.len()
            || !sparql.as_bytes()[pos + kw_len].is_ascii_alphanumeric()
                && sparql.as_bytes()[pos + kw_len] != b'_';

        if boundary_before && boundary_after {
            let before = &sparql[..pos];
            let after = &sparql[pos + kw_len..];
            // Skip whitespace and digits after the keyword
            let rest = after.trim_start();
            let digit_end = rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len());
            let consumed = after.len() - rest.len() + digit_end;
            return format!(
                "{}{}",
                before.trim_end(),
                &sparql[pos + kw_len + consumed..]
            );
        }

        search_from = pos + kw_len;
    }

    sparql.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // Streaming `format_string` parity with the DOM `format` + serde_json.
    // ------------------------------------------------------------------

    use crate::QueryResult;
    use fluree_db_core::{FlakeValue, Sid};
    use fluree_db_query::binding::Binding;
    use fluree_db_query::var_registry::VarRegistry;
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc;

    fn make_test_compactor() -> IriCompactor {
        let mut namespaces = StdHashMap::new();
        namespaces.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        namespaces.insert(3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
        namespaces.insert(100, "http://example.org/".to_string());
        IriCompactor::from_namespaces(Arc::new(namespaces))
    }

    fn make_result(var_names: &[&str], rows: Vec<Vec<Binding>>) -> QueryResult {
        let mut vars = VarRegistry::new();
        let var_ids: Vec<VarId> = var_names.iter().map(|&n| vars.get_or_insert(n)).collect();
        let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); var_ids.len()];
        for row in rows {
            for (col, b) in row.into_iter().enumerate() {
                columns[col].push(b);
            }
        }
        let batch = fluree_db_query::binding::Batch::new(
            Arc::from(var_ids.clone().into_boxed_slice()),
            columns,
        )
        .unwrap();
        QueryResult {
            vars,
            t: Some(7),
            novelty: None,
            context: crate::ParsedContext::default(),
            orig_context: None,
            output: crate::QueryOutput::select_all(var_ids),
            batches: vec![batch],
            binary_graph: None,
        }
    }

    fn assert_parity(result: &QueryResult, config: &super::super::config::FormatterConfig) {
        let c = make_test_compactor();
        let dom = format(result, &c, config).unwrap();
        let want = serde_json::to_string(&dom).unwrap();
        let got = format_string(result, &c, config).unwrap();
        assert_eq!(got, want, "streaming AgentJson diverged from DOM");
    }

    #[test]
    fn parity_basic_envelope() {
        let r = make_result(
            &["?s", "?n", "?d"],
            vec![
                vec![
                    Binding::sid(Sid::new(100, "alice")),
                    Binding::lit(
                        FlakeValue::String("Alice & co".to_string()),
                        Sid::new(2, "string"),
                    ),
                    Binding::lit(FlakeValue::Double(3.13), Sid::new(2, "double")),
                ],
                vec![
                    Binding::sid(Sid::new(100, "bob")),
                    Binding::lit(FlakeValue::Long(42), Sid::new(2, "long")),
                    Binding::Unbound,
                ],
            ],
        );
        assert_parity(&r, &super::super::config::FormatterConfig::agent_json());
    }

    #[test]
    fn parity_mixed_types_and_schema_array() {
        // Same var carries two datatypes across rows -> schema becomes an array.
        let r = make_result(
            &["?v"],
            vec![
                vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                vec![Binding::lit(
                    FlakeValue::String("x".to_string()),
                    Sid::new(2, "string"),
                )],
            ],
        );
        assert_parity(&r, &super::super::config::FormatterConfig::agent_json());
    }

    #[test]
    fn parity_empty_results() {
        let r = make_result(&["?s"], vec![]);
        assert_parity(&r, &super::super::config::FormatterConfig::agent_json());
    }

    #[test]
    fn parity_wildcard_and_grouped() {
        let mut r = make_result(
            &["?s", "?g"],
            vec![vec![
                Binding::sid(Sid::new(100, "a")),
                Binding::Grouped(vec![
                    Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
                    Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
                ]),
            ]],
        );
        r.output = crate::QueryOutput::wildcard();
        assert_parity(&r, &super::super::config::FormatterConfig::agent_json());
    }

    #[test]
    fn parity_budget_truncation() {
        // Small byte budget so the second/third rows are dropped (has_more).
        let r = make_result(
            &["?s", "?label"],
            vec![
                vec![
                    Binding::sid(Sid::new(100, "a")),
                    Binding::lit(
                        FlakeValue::String("first row label".to_string()),
                        Sid::new(2, "string"),
                    ),
                ],
                vec![
                    Binding::sid(Sid::new(100, "b")),
                    Binding::lit(
                        FlakeValue::String("second row label".to_string()),
                        Sid::new(2, "string"),
                    ),
                ],
                vec![
                    Binding::sid(Sid::new(100, "c")),
                    Binding::lit(
                        FlakeValue::String("third row label".to_string()),
                        Sid::new(2, "string"),
                    ),
                ],
            ],
        );
        let mut config = super::super::config::FormatterConfig::agent_json();
        config.max_bytes = Some(60);
        assert_parity(&r, &config);
    }

    #[test]
    fn parity_budget_truncation_with_resume() {
        let r = make_result(
            &["?s"],
            vec![
                vec![Binding::sid(Sid::new(100, "a"))],
                vec![Binding::sid(Sid::new(100, "b"))],
                vec![Binding::sid(Sid::new(100, "c"))],
            ],
        );
        let mut config = super::super::config::FormatterConfig::agent_json();
        config.max_bytes = Some(40);
        config.agent_json_context = Some(super::super::config::AgentJsonContext {
            sparql_text: Some("SELECT ?s FROM <mydb:main> WHERE { ?s ?p ?o }".to_string()),
            from_count: 1,
            iso_timestamp: Some("2026-03-26T14:30:00Z".to_string()),
            resume_limit: 100,
        });
        assert_parity(&r, &config);
    }

    #[test]
    fn parity_json_and_vector_cells() {
        let r = make_result(
            &["?j", "?v"],
            vec![vec![
                Binding::lit(
                    FlakeValue::Json(r#"{"k":[1,2]}"#.to_string()),
                    Sid::new(3, "JSON"),
                ),
                Binding::lit(FlakeValue::Vector(vec![1.0, -2.5]), Sid::new(2, "double")),
            ]],
        );
        assert_parity(&r, &super::super::config::FormatterConfig::agent_json());
    }

    #[test]
    fn test_generate_resume_basic() {
        let sparql = "SELECT ?s ?p ?o FROM <mydb:main> WHERE { ?s ?p ?o }";
        let result = generate_resume_query(sparql, 5, 47, 100).unwrap();
        assert!(result.contains("mydb:main@t:5"));
        assert!(result.contains("OFFSET 47"));
        assert!(result.contains("LIMIT 100"));
    }

    #[test]
    fn test_generate_resume_existing_time_suffix() {
        let sparql = "SELECT ?s FROM <mydb:main@t:3> WHERE { ?s ?p ?o }";
        let result = generate_resume_query(sparql, 5, 10, 100).unwrap();
        assert!(result.contains("mydb:main@t:5"));
        assert!(!result.contains("@t:3"));
    }

    #[test]
    fn test_generate_resume_existing_offset_limit() {
        let sparql = "SELECT ?s FROM <mydb:main> WHERE { ?s ?p ?o } OFFSET 10 LIMIT 50";
        let result = generate_resume_query(sparql, 5, 47, 100).unwrap();
        assert!(result.contains("OFFSET 47"));
        assert!(result.contains("LIMIT 100"));
        assert!(!result.contains("OFFSET 10"));
        assert!(!result.contains("LIMIT 50"));
    }

    #[test]
    fn test_generate_resume_multi_from_returns_none() {
        let sparql = "SELECT ?s FROM <db1:main> FROM <db2:main> WHERE { ?s ?p ?o }";
        assert!(generate_resume_query(sparql, 5, 10, 100).is_none());
    }

    #[test]
    fn test_generate_resume_no_from_returns_none() {
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";
        assert!(generate_resume_query(sparql, 5, 10, 100).is_none());
    }

    #[test]
    fn test_build_schema_single_type() {
        let mut vars = fluree_db_query::VarRegistry::new();
        let v1 = vars.get_or_insert("?name");
        let v2 = vars.get_or_insert("?age");

        let mut type_map = HashMap::new();
        type_map
            .entry(v1)
            .or_insert_with(BTreeSet::new)
            .insert("xsd:string".to_string());
        type_map
            .entry(v2)
            .or_insert_with(BTreeSet::new)
            .insert("xsd:integer".to_string());

        let schema = build_schema(&type_map, &vars);
        assert_eq!(schema["?name"], json!("xsd:string"));
        assert_eq!(schema["?age"], json!("xsd:integer"));
    }

    #[test]
    fn test_build_schema_mixed_types() {
        let mut vars = fluree_db_query::VarRegistry::new();
        let v1 = vars.get_or_insert("?value");

        let mut type_map = HashMap::new();
        let types = type_map.entry(v1).or_insert_with(BTreeSet::new);
        types.insert("xsd:string".to_string());
        types.insert("xsd:integer".to_string());

        let schema = build_schema(&type_map, &vars);
        assert_eq!(schema["?value"], json!(["xsd:integer", "xsd:string"]));
    }

    #[test]
    fn test_build_schema_skips_internal_vars() {
        let mut vars = fluree_db_query::VarRegistry::new();
        let v1 = vars.get_or_insert("?name");
        let v_internal = vars.get_or_insert("?__pp0");

        let mut type_map = HashMap::new();
        type_map
            .entry(v1)
            .or_insert_with(BTreeSet::new)
            .insert("xsd:string".to_string());
        type_map
            .entry(v_internal)
            .or_insert_with(BTreeSet::new)
            .insert("uri".to_string());

        let schema = build_schema(&type_map, &vars);
        assert!(schema.get("?name").is_some());
        assert!(schema.get("?__pp0").is_none());
    }

    #[test]
    fn test_strip_clause_word_boundary() {
        // Should NOT match "offset" inside an IRI
        let sparql = "SELECT ?s FROM <http://example.com/offsetValue> WHERE { ?s ?p ?o }";
        let result = strip_clause(sparql, "offset");
        assert_eq!(result, sparql, "should not strip 'offset' inside an IRI");

        // Should strip standalone OFFSET
        let sparql2 = "SELECT ?s FROM <db:main> WHERE { ?s ?p ?o } OFFSET 10";
        let result2 = strip_clause(sparql2, "offset");
        assert!(
            !result2.contains("OFFSET"),
            "should strip standalone OFFSET"
        );
        assert!(
            !result2.contains("10"),
            "should strip the digit after OFFSET"
        );
    }

    #[test]
    fn test_strip_clause_limit_word_boundary() {
        // Should NOT match "limit" inside "limitless"
        let sparql = "SELECT ?s FROM <limitless/db1> WHERE { ?s ?p ?o }";
        let result = strip_clause(sparql, "limit");
        assert_eq!(
            result, sparql,
            "should not strip 'limit' inside 'limitless'"
        );

        // Should strip standalone LIMIT
        let sparql2 = "SELECT ?s WHERE { ?s ?p ?o } LIMIT 50";
        let result2 = strip_clause(sparql2, "limit");
        assert!(!result2.contains("LIMIT"), "should strip standalone LIMIT");
    }

    #[test]
    fn test_generate_resume_with_custom_limit() {
        let sparql = "SELECT ?s ?p ?o FROM <mydb:main> WHERE { ?s ?p ?o }";
        let result = generate_resume_query(sparql, 5, 47, 500).unwrap();
        assert!(result.contains("LIMIT 500"), "should use custom limit");
        assert!(result.contains("OFFSET 47"));
    }
}
