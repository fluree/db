//! Delimited-text formatter (TSV and CSV) — high-performance path
//!
//! Bypasses JSON DOM construction and JSON serialization entirely.
//! Resolves bindings directly to bytes in a pre-allocated buffer.
//! IRIs are compacted via `IriCompactor` using the query's `@context`.
//!
//! # Performance
//!
//! - Writes to `Vec<u8>` via `extend_from_slice` (no `fmt::Write` overhead)
//! - Uses `itoa`/`ryu` for zero-alloc numeric formatting
//! - Column indices computed once per batch (not per cell)
//! - No `serde_json::Value` allocation
//!
//! # Formats
//!
//! - **TSV**: Tab-separated. Control chars (`\t`, `\n`, `\r`) replaced with space.
//! - **CSV**: Comma-separated. RFC 4180 quoting (values containing `,`, `"`, or
//!   newlines are wrapped in double-quotes; internal `"` doubled).

use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{FlakeValue, LedgerSnapshot, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::VarId;

// ---------------------------------------------------------------------------
// Delimiter
// ---------------------------------------------------------------------------

/// Delimiter type for tabular output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delimiter {
    Tab,
    Comma,
}

impl Delimiter {
    /// The byte used between cells.
    #[inline]
    fn byte(self) -> u8 {
        match self {
            Delimiter::Tab => b'\t',
            Delimiter::Comma => b',',
        }
    }

    /// Human-readable name for error messages.
    fn name(self) -> &'static str {
        match self {
            Delimiter::Tab => "TSV",
            Delimiter::Comma => "CSV",
        }
    }
}

// ---------------------------------------------------------------------------
// Public API — TSV
// ---------------------------------------------------------------------------

/// Format query results as TSV bytes.
pub fn format_tsv_bytes(result: &QueryResult, snapshot: &LedgerSnapshot) -> Result<Vec<u8>> {
    format_delimited_bytes(result, snapshot, Delimiter::Tab)
}

/// Format query results as a TSV string.
pub fn format_tsv(result: &QueryResult, snapshot: &LedgerSnapshot) -> Result<String> {
    format_delimited(result, snapshot, Delimiter::Tab)
}

/// Format TSV with a row limit. Returns `(tsv_bytes, total_row_count)`.
pub fn format_tsv_bytes_limited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    limit: usize,
) -> Result<(Vec<u8>, usize)> {
    format_delimited_bytes_limited(result, snapshot, Delimiter::Tab, limit)
}

/// Format TSV string with a row limit. Returns `(tsv_string, total_row_count)`.
pub fn format_tsv_limited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    limit: usize,
) -> Result<(String, usize)> {
    format_delimited_limited(result, snapshot, Delimiter::Tab, limit)
}

// ---------------------------------------------------------------------------
// Public API — CSV
// ---------------------------------------------------------------------------

/// Format query results as CSV bytes.
pub fn format_csv_bytes(result: &QueryResult, snapshot: &LedgerSnapshot) -> Result<Vec<u8>> {
    format_delimited_bytes(result, snapshot, Delimiter::Comma)
}

/// Format query results as a CSV string.
pub fn format_csv(result: &QueryResult, snapshot: &LedgerSnapshot) -> Result<String> {
    format_delimited(result, snapshot, Delimiter::Comma)
}

/// Format CSV with a row limit. Returns `(csv_bytes, total_row_count)`.
pub fn format_csv_bytes_limited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    limit: usize,
) -> Result<(Vec<u8>, usize)> {
    format_delimited_bytes_limited(result, snapshot, Delimiter::Comma, limit)
}

/// Format CSV string with a row limit. Returns `(csv_string, total_row_count)`.
pub fn format_csv_limited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    limit: usize,
) -> Result<(String, usize)> {
    format_delimited_limited(result, snapshot, Delimiter::Comma, limit)
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

fn format_delimited_bytes(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    delimiter: Delimiter,
) -> Result<Vec<u8>> {
    reject_non_tabular(result, delimiter)?;

    let compactor = IriCompactor::new(snapshot.namespaces(), &result.context);
    let gv = result.binary_graph.as_ref();
    let select_vars = resolve_select_vars(result);

    let est_size = (result.row_count() + 1)
        .saturating_mul(select_vars.len())
        .saturating_mul(80);
    let mut out = Vec::with_capacity(est_size);

    write_header(&mut out, result, &select_vars, delimiter);
    out.push(b'\n');

    write_data_rows(
        &mut out,
        result,
        &select_vars,
        &compactor,
        gv,
        delimiter,
        None,
    )?;

    Ok(out)
}

fn format_delimited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    delimiter: Delimiter,
) -> Result<String> {
    let bytes = format_delimited_bytes(result, snapshot, delimiter)?;
    #[cfg(debug_assertions)]
    {
        Ok(String::from_utf8(bytes).expect("delimited output should always be valid UTF-8"))
    }
    #[cfg(not(debug_assertions))]
    {
        // SAFETY: We only write ASCII bytes, valid UTF-8 strings from IRIs/literals,
        // and numeric formatting (itoa/ryu produce ASCII). Sanitization replaces
        // control chars with ASCII space; CSV quoting uses ASCII `"`.
        Ok(unsafe { String::from_utf8_unchecked(bytes) })
    }
}

fn format_delimited_bytes_limited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    delimiter: Delimiter,
    limit: usize,
) -> Result<(Vec<u8>, usize)> {
    reject_non_tabular(result, delimiter)?;

    let compactor = IriCompactor::new(snapshot.namespaces(), &result.context);
    let gv = result.binary_graph.as_ref();
    let select_vars = resolve_select_vars(result);
    let total = result.row_count();

    let est_size = (limit.min(total) + 1)
        .saturating_mul(select_vars.len())
        .saturating_mul(80);
    let mut out = Vec::with_capacity(est_size);

    write_header(&mut out, result, &select_vars, delimiter);
    out.push(b'\n');

    write_data_rows(
        &mut out,
        result,
        &select_vars,
        &compactor,
        gv,
        delimiter,
        Some(limit),
    )?;

    Ok((out, total))
}

fn format_delimited_limited(
    result: &QueryResult,
    snapshot: &LedgerSnapshot,
    delimiter: Delimiter,
    limit: usize,
) -> Result<(String, usize)> {
    let (bytes, total) = format_delimited_bytes_limited(result, snapshot, delimiter, limit)?;
    #[cfg(debug_assertions)]
    let s = String::from_utf8(bytes).expect("delimited output should always be valid UTF-8");
    #[cfg(not(debug_assertions))]
    let s = unsafe { String::from_utf8_unchecked(bytes) };
    Ok((s, total))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Reject non-tabular results (CONSTRUCT, graph crawl).
fn reject_non_tabular(result: &QueryResult, delimiter: Delimiter) -> Result<()> {
    let name = delimiter.name();
    if result.output.is_construct() {
        return Err(FormatError::InvalidBinding(format!(
            "{name} format not supported for CONSTRUCT queries (use JSON-LD instead)"
        )));
    }
    if result.output.is_boolean() {
        return Err(FormatError::InvalidBinding(format!(
            "{name} format not supported for ASK queries (boolean result)"
        )));
    }
    if result.graph_select.is_some() {
        return Err(FormatError::InvalidBinding(format!(
            "{name} format not supported for graph crawl queries (use JSON-LD instead)"
        )));
    }
    Ok(())
}

/// Resolve the select variable list, handling Wildcard mode.
fn resolve_select_vars(result: &QueryResult) -> Vec<VarId> {
    if result.output.is_wildcard() {
        let mut pairs: Vec<(String, VarId)> = result
            .batches
            .first()
            .map(|b| {
                b.schema()
                    .iter()
                    .copied()
                    .filter(|&vid| !result.vars.name(vid).starts_with("?__"))
                    .map(|vid| {
                        let name = result
                            .vars
                            .name(vid)
                            .strip_prefix('?')
                            .unwrap_or(result.vars.name(vid))
                            .to_string();
                        (name, vid)
                    })
                    .collect()
            })
            .unwrap_or_else(|| {
                // Empty result set: derive vars from the registry.
                result
                    .vars
                    .iter()
                    .filter(|(name, _)| !name.starts_with("?__"))
                    .map(|(name, vid)| {
                        let stripped = name.strip_prefix('?').unwrap_or(name).to_string();
                        (stripped, vid)
                    })
                    .collect()
            });
        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
        pairs.into_iter().map(|(_, vid)| vid).collect()
    } else {
        result.output.select_vars_or_empty().to_vec()
    }
}

/// Write the header row (variable names without `?` prefix).
fn write_header(out: &mut Vec<u8>, result: &QueryResult, vars: &[VarId], delimiter: Delimiter) {
    for (i, &var) in vars.iter().enumerate() {
        if i > 0 {
            out.push(delimiter.byte());
        }
        let name = result.vars.name(var);
        let stripped = name.strip_prefix('?').unwrap_or(name);
        // Header values are simple identifiers — no escaping needed for either format
        out.extend_from_slice(stripped.as_bytes());
    }
}

/// Write data rows with the specified delimiter.
#[allow(clippy::too_many_arguments)]
fn write_data_rows(
    out: &mut Vec<u8>,
    result: &QueryResult,
    select_vars: &[VarId],
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
    delimiter: Delimiter,
    limit: Option<usize>,
) -> Result<()> {
    let max_rows = limit.unwrap_or(usize::MAX);
    let mut emitted = 0usize;
    let select_one = result.output.is_select_one();

    // Reusable cell buffer to avoid per-cell allocation
    let mut cell_buf = Vec::with_capacity(256);

    for batch in &result.batches {
        let schema = batch.schema();
        let col_indices: Vec<usize> = select_vars
            .iter()
            .map(|&v| {
                schema.iter().position(|&sv| sv == v).ok_or_else(|| {
                    FormatError::InvalidBinding(format!(
                        "Variable {:?} not found in batch schema",
                        result.vars.name(v)
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for row in 0..batch.len() {
            for (i, &col) in col_indices.iter().enumerate() {
                if i > 0 {
                    out.push(delimiter.byte());
                }
                cell_buf.clear();
                let binding = batch.get_by_col(row, col);
                write_binding_cell(&mut cell_buf, binding, compactor, gv)?;
                flush_cell(out, &cell_buf, delimiter);
            }
            out.push(b'\n');

            emitted += 1;
            if emitted >= max_rows || select_one {
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Write a single binding value to the cell buffer.
///
/// The cell buffer is flushed to the output after this call with
/// delimiter-appropriate escaping.
fn write_binding_cell(
    cell: &mut Vec<u8>,
    binding: &Binding,
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
) -> Result<()> {
    match binding {
        Binding::Unbound | Binding::Poisoned => {
            // Empty cell
        }
        Binding::Sid { sid, .. } => {
            write_compacted_sid(cell, compactor, sid)?;
        }
        Binding::IriMatch { iri, .. } => {
            let compacted = compactor.compact_vocab_iri(iri);
            cell.extend_from_slice(compacted.as_bytes());
        }
        Binding::Iri(iri) => {
            let compacted = compactor.compact_vocab_iri(iri);
            cell.extend_from_slice(compacted.as_bytes());
        }
        Binding::Lit { val, .. } => {
            write_flake_value(cell, val, compactor);
        }
        Binding::EncodedSid { s_id, .. } => {
            let gv = require_graph_view(gv)?;
            let store = gv.store();
            let iri = store.resolve_subject_iri(*s_id).map_err(|e| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve subject IRI for s_id {s_id}: {e}"
                ))
            })?;
            let compacted = compactor.compact_vocab_iri(&iri);
            cell.extend_from_slice(compacted.as_bytes());
        }
        Binding::EncodedPid { p_id } => {
            let gv = require_graph_view(gv)?;
            let store = gv.store();
            let iri = store.resolve_predicate_iri(*p_id).ok_or_else(|| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve predicate IRI for p_id {p_id}"
                ))
            })?;
            let compacted = compactor.compact_vocab_iri(iri);
            cell.extend_from_slice(compacted.as_bytes());
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => {
            let gv = require_graph_view(gv)?;
            let store = gv.store();
            if *o_kind == ObjKind::LEX_ID.as_u8() || *o_kind == ObjKind::JSON_ID.as_u8() {
                store
                    .write_string_value_bytes(*o_key as u32, cell)
                    .map_err(|e| {
                        FormatError::InvalidBinding(format!(
                            "Failed to resolve string (kind={o_kind}, key={o_key}): {e}"
                        ))
                    })?;
            } else if *o_kind == ObjKind::REF_ID.as_u8() {
                let iri = store.resolve_subject_iri(*o_key).map_err(|e| {
                    FormatError::InvalidBinding(format!(
                        "Failed to resolve ref IRI for s_id {o_key}: {e}"
                    ))
                })?;
                let compacted = compactor.compact_vocab_iri(&iri);
                cell.extend_from_slice(compacted.as_bytes());
            } else {
                let val = gv
                    .decode_value_from_kind(*o_kind, *o_key, *p_id, *dt_id, *lang_id)
                    .map_err(|e| {
                        FormatError::InvalidBinding(format!(
                            "Failed to decode value (kind={o_kind}, key={o_key}, p_id={p_id}): {e}"
                        ))
                    })?;
                write_flake_value(cell, &val, compactor);
            }
        }
        Binding::Grouped(values) => {
            // Semicolon-separate for pragmatic consumption.
            for (j, val) in values.iter().enumerate() {
                if j > 0 {
                    cell.push(b';');
                }
                write_binding_cell(cell, val, compactor, gv)?;
            }
        }
    }
    Ok(())
}

/// Require a BinaryGraphView for encoded binding resolution.
fn require_graph_view(gv: Option<&BinaryGraphView>) -> Result<&BinaryGraphView> {
    gv.ok_or_else(|| {
        FormatError::InvalidBinding(
            "Encountered encoded binding but QueryResult has no binary_store".to_string(),
        )
    })
}

/// Write a compacted Sid IRI to the cell buffer.
fn write_compacted_sid(cell: &mut Vec<u8>, compactor: &IriCompactor, sid: &Sid) -> Result<()> {
    let compacted = compactor.compact_sid(sid)?;
    cell.extend_from_slice(compacted.as_bytes());
    Ok(())
}

/// Write a FlakeValue to the cell buffer.
fn write_flake_value(cell: &mut Vec<u8>, val: &FlakeValue, compactor: &IriCompactor) {
    match val {
        FlakeValue::String(s) => cell.extend_from_slice(s.as_bytes()),
        FlakeValue::Ref(sid) => {
            // Best-effort: if compaction fails (unknown namespace), write raw
            match compactor.compact_sid(sid) {
                Ok(compacted) => cell.extend_from_slice(compacted.as_bytes()),
                Err(_) => {
                    // Fallback: code:name
                    let mut buf = itoa::Buffer::new();
                    cell.extend_from_slice(buf.format(sid.namespace_code).as_bytes());
                    cell.push(b':');
                    cell.extend_from_slice(sid.name.as_bytes());
                }
            }
        }
        FlakeValue::Long(n) => {
            let mut buf = itoa::Buffer::new();
            cell.extend_from_slice(buf.format(*n).as_bytes());
        }
        FlakeValue::Double(d) => {
            let mut buf = ryu::Buffer::new();
            cell.extend_from_slice(buf.format(*d).as_bytes());
        }
        FlakeValue::Boolean(b) => {
            cell.extend_from_slice(if *b { b"true" } else { b"false" });
        }
        FlakeValue::Null => {}
        FlakeValue::BigInt(n) => cell.extend_from_slice(n.to_string().as_bytes()),
        FlakeValue::Decimal(d) => cell.extend_from_slice(d.to_string().as_bytes()),
        FlakeValue::DateTime(dt) => cell.extend_from_slice(dt.to_string().as_bytes()),
        FlakeValue::Date(d) => cell.extend_from_slice(d.to_string().as_bytes()),
        FlakeValue::Time(t) => cell.extend_from_slice(t.to_string().as_bytes()),
        FlakeValue::GYear(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::GYearMonth(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::GMonth(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::GDay(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::GMonthDay(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::YearMonthDuration(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::DayTimeDuration(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::Duration(v) => cell.extend_from_slice(v.to_string().as_bytes()),
        FlakeValue::Vector(v) => {
            let s = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            cell.extend_from_slice(s.as_bytes());
        }
        FlakeValue::Json(json_str) => cell.extend_from_slice(json_str.as_bytes()),
        FlakeValue::GeoPoint(v) => cell.extend_from_slice(v.to_string().as_bytes()),
    }
}

// ---------------------------------------------------------------------------
// Cell flushing — delimiter-specific escaping
// ---------------------------------------------------------------------------

/// Flush a cell buffer to the output with delimiter-appropriate escaping.
#[inline]
fn flush_cell(out: &mut Vec<u8>, cell: &[u8], delimiter: Delimiter) {
    match delimiter {
        Delimiter::Tab => flush_cell_tsv(out, cell),
        Delimiter::Comma => flush_cell_csv(out, cell),
    }
}

/// TSV: replace `\t`, `\n`, `\r` with space.
#[inline]
fn flush_cell_tsv(out: &mut Vec<u8>, cell: &[u8]) {
    out.reserve(cell.len());
    for &b in cell {
        match b {
            b'\t' | b'\n' | b'\r' => out.push(b' '),
            _ => out.push(b),
        }
    }
}

/// CSV: RFC 4180 quoting.
///
/// If the cell contains `,`, `"`, `\n`, or `\r`, wrap the entire value in
/// double-quotes and double any internal `"`.
#[inline]
fn flush_cell_csv(out: &mut Vec<u8>, cell: &[u8]) {
    let needs_quoting = cell
        .iter()
        .any(|&b| b == b',' || b == b'"' || b == b'\n' || b == b'\r');

    if !needs_quoting {
        out.extend_from_slice(cell);
        return;
    }

    out.push(b'"');
    for &b in cell {
        if b == b'"' {
            out.push(b'"');
        }
        out.push(b);
    }
    out.push(b'"');
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;
    use fluree_db_query::VarRegistry;
    use fluree_graph_json_ld::ParsedContext;
    use serde_json::json;
    use std::sync::Arc;

    fn make_test_snapshot() -> LedgerSnapshot {
        let mut db = LedgerSnapshot::genesis("test:main");
        db.insert_namespace_code(100, "http://example.org/".to_string())
            .unwrap();
        db
    }

    fn make_test_context() -> ParsedContext {
        ParsedContext::parse(
            None,
            &json!({
                "ex": "http://example.org/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            }),
        )
        .unwrap()
    }

    fn make_result_with_context(
        var_names: &[&str],
        rows: Vec<Vec<Binding>>,
        context: ParsedContext,
    ) -> QueryResult {
        let mut vars = VarRegistry::new();
        let var_ids: Vec<VarId> = var_names
            .iter()
            .map(|&name| vars.get_or_insert(name))
            .collect();

        let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); var_ids.len()];
        for row in rows {
            for (col, binding) in row.into_iter().enumerate() {
                columns[col].push(binding);
            }
        }

        let batch = fluree_db_query::binding::Batch::new(
            Arc::from(var_ids.clone().into_boxed_slice()),
            columns,
        )
        .expect("test batch construction should not fail");

        QueryResult {
            vars,
            t: Some(0),
            novelty: None,
            context,
            orig_context: None,
            output: crate::QueryOutput::select(var_ids),
            batches: vec![batch],
            binary_graph: None,
            graph_select: None,
        }
    }

    fn make_result(var_names: &[&str], rows: Vec<Vec<Binding>>) -> QueryResult {
        make_result_with_context(var_names, rows, crate::ParsedContext::default())
    }

    // ---- TSV tests ----

    #[test]
    fn test_tsv_empty_result() {
        let snapshot = make_test_snapshot();
        let result = make_result(&["?s", "?name"], vec![]);
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "s\tname\n");
    }

    #[test]
    fn test_tsv_sid_binding_no_context() {
        // Without @context, Sid outputs full IRI (no compaction possible)
        let snapshot = make_test_snapshot();
        let result = make_result(&["?s"], vec![vec![Binding::sid(Sid::new(100, "alice"))]]);
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "s\nhttp://example.org/alice\n");
    }

    #[test]
    fn test_tsv_sid_binding_with_context() {
        // With @context that maps "ex" -> "http://example.org/", IRIs should be compacted
        let snapshot = make_test_snapshot();
        let result = make_result_with_context(
            &["?s"],
            vec![vec![Binding::sid(Sid::new(100, "alice"))]],
            make_test_context(),
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "s\nex:alice\n");
    }

    #[test]
    fn test_tsv_literal_bindings() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?name", "?age"],
            vec![vec![
                Binding::lit(
                    FlakeValue::String("Alice".to_string()),
                    Sid::new(2, "string"),
                ),
                Binding::lit(FlakeValue::Long(30), Sid::new(2, "long")),
            ]],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "name\tage\nAlice\t30\n");
    }

    #[test]
    fn test_tsv_sanitization() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?val"],
            vec![vec![Binding::lit(
                FlakeValue::String("hello\tworld\nfoo\rbar".to_string()),
                Sid::new(2, "string"),
            )]],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "val\nhello world foo bar\n");
    }

    #[test]
    fn test_tsv_unbound_binding() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?a", "?b"],
            vec![vec![Binding::sid(Sid::new(100, "x")), Binding::Unbound]],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "a\tb\nhttp://example.org/x\t\n");
    }

    #[test]
    fn test_tsv_multiple_rows() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?s"],
            vec![
                vec![Binding::sid(Sid::new(100, "a"))],
                vec![Binding::sid(Sid::new(100, "b"))],
                vec![Binding::sid(Sid::new(100, "c"))],
            ],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(
            tsv,
            "s\nhttp://example.org/a\nhttp://example.org/b\nhttp://example.org/c\n"
        );
    }

    #[test]
    fn test_tsv_limited_output() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?s"],
            vec![
                vec![Binding::sid(Sid::new(100, "a"))],
                vec![Binding::sid(Sid::new(100, "b"))],
                vec![Binding::sid(Sid::new(100, "c"))],
            ],
        );
        let (tsv, total) = format_tsv_limited(&result, &snapshot, 2).unwrap();
        assert_eq!(total, 3);
        assert_eq!(tsv, "s\nhttp://example.org/a\nhttp://example.org/b\n");
    }

    #[test]
    fn test_tsv_boolean_and_double() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?flag", "?score"],
            vec![vec![
                Binding::lit(FlakeValue::Boolean(true), Sid::new(2, "boolean")),
                Binding::lit(FlakeValue::Double(3.125), Sid::new(2, "double")),
            ]],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        let lines: Vec<&str> = tsv.lines().collect();
        assert_eq!(lines[0], "flag\tscore");
        assert!(lines[1].starts_with("true\t"));
    }

    #[test]
    fn test_tsv_iri_binding() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?g"],
            vec![vec![Binding::Iri(Arc::from("http://example.org/graph1"))]],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        // No context → full IRI
        assert_eq!(tsv, "g\nhttp://example.org/graph1\n");
    }

    #[test]
    fn test_tsv_iri_binding_with_context() {
        let snapshot = make_test_snapshot();
        let result = make_result_with_context(
            &["?g"],
            vec![vec![Binding::Iri(Arc::from("http://example.org/graph1"))]],
            make_test_context(),
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "g\nex:graph1\n");
    }

    #[test]
    fn test_tsv_grouped_binding() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?vals"],
            vec![vec![Binding::Grouped(vec![
                Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
                Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
                Binding::lit(FlakeValue::Long(3), Sid::new(2, "long")),
            ])]],
        );
        let tsv = format_tsv(&result, &snapshot).unwrap();
        assert_eq!(tsv, "vals\n1;2;3\n");
    }

    // ---- CSV tests ----

    #[test]
    fn test_csv_empty_result() {
        let snapshot = make_test_snapshot();
        let result = make_result(&["?s", "?name"], vec![]);
        let csv = format_csv(&result, &snapshot).unwrap();
        assert_eq!(csv, "s,name\n");
    }

    #[test]
    fn test_csv_basic() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?name", "?age"],
            vec![vec![
                Binding::lit(
                    FlakeValue::String("Alice".to_string()),
                    Sid::new(2, "string"),
                ),
                Binding::lit(FlakeValue::Long(30), Sid::new(2, "long")),
            ]],
        );
        let csv = format_csv(&result, &snapshot).unwrap();
        assert_eq!(csv, "name,age\nAlice,30\n");
    }

    #[test]
    fn test_csv_quoting_comma() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?val"],
            vec![vec![Binding::lit(
                FlakeValue::String("hello, world".to_string()),
                Sid::new(2, "string"),
            )]],
        );
        let csv = format_csv(&result, &snapshot).unwrap();
        assert_eq!(csv, "val\n\"hello, world\"\n");
    }

    #[test]
    fn test_csv_quoting_double_quotes() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?val"],
            vec![vec![Binding::lit(
                FlakeValue::String("say \"hello\"".to_string()),
                Sid::new(2, "string"),
            )]],
        );
        let csv = format_csv(&result, &snapshot).unwrap();
        assert_eq!(csv, "val\n\"say \"\"hello\"\"\"\n");
    }

    #[test]
    fn test_csv_quoting_newline() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?val"],
            vec![vec![Binding::lit(
                FlakeValue::String("line1\nline2".to_string()),
                Sid::new(2, "string"),
            )]],
        );
        let csv = format_csv(&result, &snapshot).unwrap();
        assert_eq!(csv, "val\n\"line1\nline2\"\n");
    }

    #[test]
    fn test_csv_sid_with_context() {
        let snapshot = make_test_snapshot();
        let result = make_result_with_context(
            &["?s"],
            vec![vec![Binding::sid(Sid::new(100, "alice"))]],
            make_test_context(),
        );
        let csv = format_csv(&result, &snapshot).unwrap();
        assert_eq!(csv, "s\nex:alice\n");
    }

    #[test]
    fn test_csv_limited() {
        let snapshot = make_test_snapshot();
        let result = make_result(
            &["?n"],
            vec![
                vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(2), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(3), Sid::new(2, "long"))],
            ],
        );
        let (csv, total) = format_csv_limited(&result, &snapshot, 2).unwrap();
        assert_eq!(total, 3);
        assert_eq!(csv, "n\n1\n2\n");
    }
}
