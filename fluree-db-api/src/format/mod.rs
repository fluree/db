//! Query result formatting
//!
//! This module provides formatters for converting `QueryResult` to various output formats:
//!
//! **JSON formats** (produce `serde_json::Value`):
//! - **JSON-LD Query** (`OutputFormat::JsonLd`): Simple JSON with compact IRIs
//! - **SPARQL JSON** (`OutputFormat::SparqlJson`): W3C SPARQL 1.1 Query Results JSON
//! - **TypedJson** (`OutputFormat::TypedJson`): Always includes explicit datatypes
//!
//! **Delimited-text formats** (produce `String` / `Vec<u8>` directly):
//! - **TSV** (`OutputFormat::Tsv`): Tab-separated values
//! - **CSV** (`OutputFormat::Csv`): Comma-separated values (RFC 4180)
//!
//! # Sync vs Async Formatting
//!
//! Most queries can use the synchronous `format_results()` function. However, **hydration
//! queries** require async database access for property expansion and must use
//! `format_results_async()` instead.
//!
//! # Delimited-Text Fast Path
//!
//! TSV and CSV bypass JSON DOM construction and JSON serialization.
//! Use `format_results_string()` or `QueryResult::to_tsv()` / `to_csv()` —
//! **not** `format_results()` (which returns `JsonValue`).
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_api::format::{format_results, format_results_async, FormatterConfig};
//!
//! // For regular SELECT queries (sync)
//! let json = format_results(&result, &parsed.context, &ledger.snapshot, &FormatterConfig::jsonld())?;
//!
//! // For hydration queries (async)
//! let json = format_results_async(&result, &parsed.context, &ledger.snapshot, &FormatterConfig::jsonld()).await?;
//!
//! // For TSV/CSV (high-performance)
//! let tsv = result.to_tsv(&ledger.snapshot)?;
//! let csv = result.to_csv(&ledger.snapshot)?;
//! ```

mod agent_json;
pub mod config;
mod construct;
mod cypher;
pub mod datatype;
pub mod delimited;
mod hydration;
pub mod iri;
mod json_write;
mod jsonld;
mod materialize;
mod rdf_xml;
mod sparql;
mod sparql_xml;
mod typed;

/// Registry-name predicate: does this variable name belong to a
/// non-projected internal / non-distinguished variable that should be
/// hidden from `SELECT *` wildcard output?
///
/// Three categories are reserved:
/// - `?__*` — planner / aggregate / property-path synthetics.
/// - `?#*`  — annotation-reifier synthetics (the `#` is comment-start
///   in the SPARQL lexer so users cannot lex this prefix).
/// - `_:*`  — SPARQL blank nodes used in WHERE patterns. Per SPARQL
///   §4.1.4 these are non-distinguished variables; they bind values
///   but are not part of the SELECT scope, so they don't appear in
///   `SELECT *` results. Hiding them here also covers
///   blank-node-labelled reifiers (`~ _:ann`, `_:ann rdf:reifies …`)
///   that the edge-annotation lowering registers under their literal
///   blank-node label.
///
/// Every wildcard formatter (SPARQL JSON, SPARQL XML, JSON-LD, typed,
/// agent-JSON, delimited) routes through this predicate so the rule
/// stays consistent across output shapes.
pub(crate) fn is_internal_var_name(name: &str) -> bool {
    name.starts_with("?__") || name.starts_with("?#") || name.starts_with("_:")
}
mod xml_escape;

pub use config::{AgentJsonContext, FormatterConfig, OutputFormat, QueryOutput};
pub use iri::IriCompactor;

use crate::QueryResult;
use fluree_db_core::LedgerSnapshot;
use fluree_db_core::{FuelExceededError, GraphDbRef, Tracker};
use fluree_graph_json_ld::ParsedContext;
use serde_json::{json, Value as JsonValue};

/// Error type for formatting operations
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    /// JSON serialization error
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Unknown namespace code (Sid could not be decoded to IRI)
    #[error("Unknown namespace code: {0}")]
    UnknownNamespace(u16),

    /// Invalid binding state encountered
    #[error("Invalid binding state: {0}")]
    InvalidBinding(String),

    /// Fuel limit exceeded during formatting (expansion)
    #[error(transparent)]
    FuelExceeded(#[from] FuelExceededError),
}

/// Result type for formatting operations
pub type Result<T> = std::result::Result<T, FormatError>;

/// Format query results to JSON using the specified configuration
///
/// This is the main entry point for formatting. It dispatches to the appropriate
/// formatter based on `config.format`.
///
/// # Arguments
///
/// * `result` - Query result to format
/// * `context` - Parsed @context from the query (for IRI compaction)
/// * `snapshot` - Database snapshot (for namespace code lookup)
/// * `config` - Formatting configuration
///
/// # Returns
///
/// A `serde_json::Value` containing the formatted results
pub fn format_results(
    result: &QueryResult,
    context: &ParsedContext,
    snapshot: &LedgerSnapshot,
    config: &FormatterConfig,
) -> Result<JsonValue> {
    // Delimited-text formats produce bytes/String, not JsonValue. Reject early.
    if matches!(
        config.format,
        OutputFormat::Tsv | OutputFormat::Csv | OutputFormat::SparqlXml | OutputFormat::RdfXml
    ) {
        return Err(FormatError::InvalidBinding(format!(
            "{:?} format produces bytes/String, not JsonValue. \
             Use format_results_string() or QueryResult::to_tsv()/to_csv() instead.",
            config.format
        )));
    }

    let compactor = IriCompactor::new(snapshot.shared_namespaces(), context);

    // CONSTRUCT / DESCRIBE produce a graph, not a binding table, so the only
    // sensible JSON rendering is JSON-LD. Any JSON-producing format request
    // (including the SPARQL default `SparqlJson`) is coerced to JSON-LD rather
    // than rejected — a graph has no SPARQL-results-JSON / TypedJson / AgentJson
    // form. RDF/XML and the delimited formats never reach here: they are bytes
    // formats handled (or rejected) earlier on the string path. See issue #1274.
    if result.output.construct_template().is_some() {
        return construct::format(result, &compactor);
    }

    // ASK queries: return boolean based on solution existence
    if let Some(result) = format_ask(result, config) {
        return result;
    }

    // Hydration queries require async formatting for database access
    if result.output.has_hydration() {
        return Err(FormatError::InvalidBinding(
            "Hydration queries require async database access for property expansion. \
             Use format_results_async() instead of format_results()."
                .to_string(),
        ));
    }

    // SELECT query dispatch
    match config.format {
        OutputFormat::JsonLd => jsonld::format(result, &compactor, config),
        OutputFormat::SparqlJson => sparql::format(result, &compactor, config),
        OutputFormat::TypedJson => typed::format(result, &compactor, config),
        OutputFormat::AgentJson => agent_json::format(result, &compactor, config),
        OutputFormat::CypherJson => cypher::format(result, &compactor, config),
        OutputFormat::SparqlXml => Err(FormatError::InvalidBinding(
            "SPARQL XML produces String, not JsonValue. Use format_results_string() instead."
                .to_string(),
        )),
        OutputFormat::RdfXml => Err(FormatError::InvalidBinding(
            "RDF/XML produces String, not JsonValue. Use format_results_string() instead."
                .to_string(),
        )),
        OutputFormat::Tsv | OutputFormat::Csv => {
            unreachable!("Delimited formats rejected before dispatch")
        }
    }
}

/// True when the JSON **string** output for this result can be produced by the
/// allocation-light streaming serializers instead of building (and then
/// re-serializing) a `serde_json::Value` DOM.
///
/// The DOM path handles every excluded case identically and remains the
/// reference implementation:
/// - `pretty` — uses serde's pretty-printer
/// - `select_one` — single-row output; per-format shaping quirks, and the win
///   from streaming is negligible for one row
/// - ASK — tiny boolean envelope
/// - CONSTRUCT/DESCRIBE — coerced to a JSON-LD graph (`construct::format`)
/// - hydration — async DB expansion during formatting
fn json_stream_eligible(result: &QueryResult, config: &FormatterConfig) -> bool {
    !config.pretty
        && matches!(
            config.format,
            OutputFormat::JsonLd
                | OutputFormat::SparqlJson
                | OutputFormat::TypedJson
                | OutputFormat::AgentJson
        )
        && !result.output.is_select_one()
        && !result.output.is_ask()
        && result.output.construct_template().is_none()
        && !result.output.has_hydration()
}

/// Dispatch a stream-eligible result to the matching streaming JSON serializer.
/// Each `format_string` is parity-tested to be byte-identical to
/// `serde_json::to_string(&<dom format>(...))`.
fn stream_json(
    result: &QueryResult,
    compactor: &IriCompactor,
    config: &FormatterConfig,
) -> Result<String> {
    match config.format {
        OutputFormat::JsonLd => jsonld::format_string(result, compactor, config),
        OutputFormat::SparqlJson => sparql::format_string(result, compactor, config),
        OutputFormat::TypedJson => typed::format_string(result, compactor, config),
        OutputFormat::AgentJson => agent_json::format_string(result, compactor, config),
        _ => unreachable!("stream_json only called for JSON formats (see json_stream_eligible)"),
    }
}

/// Format query results to a JSON string
///
/// Convenience function that formats and serializes in one step.
/// Respects `config.pretty` for formatting.
///
/// Note: For hydration queries, use `format_results_string_async()` instead.
pub fn format_results_string(
    result: &QueryResult,
    context: &ParsedContext,
    snapshot: &LedgerSnapshot,
    config: &FormatterConfig,
) -> Result<String> {
    // Delimited-text fast-path: skip JSON DOM and JSON serialization entirely
    match config.format {
        OutputFormat::Tsv => return delimited::format_tsv(result, snapshot),
        OutputFormat::Csv => return delimited::format_csv(result, snapshot),
        OutputFormat::SparqlXml => {
            let compactor = IriCompactor::new(snapshot.shared_namespaces(), context);
            return sparql_xml::format(result, &compactor, config);
        }
        OutputFormat::RdfXml => {
            let compactor = IriCompactor::new(snapshot.shared_namespaces(), context);
            return rdf_xml::format(result, &compactor, config);
        }
        _ => {}
    }

    // Stream JSON straight to a String for the common SELECT case, skipping the
    // serde_json::Value DOM and its second serialization pass.
    if json_stream_eligible(result, config) {
        let compactor = IriCompactor::new(snapshot.shared_namespaces(), context);
        return stream_json(result, &compactor, config);
    }

    let value = format_results(result, context, snapshot, config)?;

    if config.pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

// ============================================================================
// ASK formatting
// ============================================================================

/// If this is an ASK query, produce the result directly.
/// Returns `None` for non-ASK queries (caller continues to normal dispatch).
fn format_ask(result: &QueryResult, config: &FormatterConfig) -> Option<Result<JsonValue>> {
    if !result.output.is_ask() {
        return None;
    }
    let has_solution = result.batches.iter().any(|b| !b.is_empty());
    Some(match config.format {
        OutputFormat::SparqlJson => Ok(json!({"head": {}, "boolean": has_solution})),
        _ => Ok(JsonValue::Bool(has_solution)),
    })
}

// ============================================================================
// Async formatting (required for hydration)
// ============================================================================

/// Format query results to JSON using async database access
///
/// This is the async entry point for formatting. It supports all query types including
/// **hydration queries** which require database access during formatting for property
/// expansion.
///
/// For non-hydration queries, this delegates to the sync formatters internally.
///
/// # Arguments
///
/// * `result` - Query result to format
/// * `context` - Parsed @context from the query (for IRI compaction)
/// * `snapshot` - Database snapshot (for namespace code lookup and property fetching)
/// * `config` - Formatting configuration
///
/// # Returns
///
/// A `serde_json::Value` containing the formatted results
///
/// # Policy Support
///
/// When `policy` is `Some`, hydration queries filter flakes according to view policies.
/// When `policy` is `None`, no filtering is applied (zero overhead for the common case).
pub async fn format_results_async(
    result: &QueryResult,
    context: &ParsedContext,
    db: GraphDbRef<'_>,
    config: &FormatterConfig,
    policy: Option<&fluree_db_policy::PolicyContext>,
    tracker: Option<&Tracker>,
) -> Result<JsonValue> {
    // Delimited-text formats produce bytes/String, not JsonValue. Reject early.
    if matches!(config.format, OutputFormat::Tsv | OutputFormat::Csv) {
        return Err(FormatError::InvalidBinding(format!(
            "{:?} format produces bytes/String, not JsonValue. \
             Use format_results_string() or QueryResult::to_tsv()/to_csv() instead.",
            config.format
        )));
    }

    let compactor = IriCompactor::new(db.snapshot.shared_namespaces(), context);

    // CONSTRUCT / DESCRIBE produce a graph (sync, no DB access needed); coerce
    // any JSON-producing format to JSON-LD rather than rejecting it. RDF/XML and
    // delimited formats are handled on the string/bytes path and never reach
    // here. See the sync `format_results` above and issue #1274.
    if result.output.construct_template().is_some() {
        return construct::format(result, &compactor);
    }

    // ASK queries: return boolean based on solution existence
    if let Some(result) = format_ask(result, config) {
        return result;
    }

    // Hydration queries use async formatter with DB access
    if result.output.has_hydration() {
        if !matches!(
            config.format,
            OutputFormat::JsonLd | OutputFormat::TypedJson
        ) {
            return Err(FormatError::InvalidBinding(
                "Hydration only supports JSON-LD and TypedJson output formats".to_string(),
            ));
        }
        // For cross-ledger queries (connection/dataset), the result carries a
        // composite overlay merging data from all queried ledgers. The
        // expansion must use this overlay so it can resolve references that
        // span ledger boundaries (e.g. a movie's `isBasedOn` pointing to a
        // book in a different ledger).
        let hydration_db = match (&result.novelty, result.t) {
            (Some(novelty), Some(t)) => GraphDbRef::new(db.snapshot, db.g_id, novelty.as_ref(), t),
            // Multi-ledger dataset results may carry a composite overlay but no
            // meaningful shared `t`; keep the primary view's real bound instead
            // of imposing a synthetic dataset-wide max.
            (Some(novelty), None) => GraphDbRef::new(db.snapshot, db.g_id, novelty.as_ref(), db.t),
            (None, _) => db,
        };
        let v = hydration::format_async(result, hydration_db, &compactor, config, policy, tracker)
            .await?;
        // Hydration formatter returns an array of rows; honor selectOne by
        // returning the first row (or null if empty).
        return if result.output.is_select_one() {
            match v {
                JsonValue::Array(mut rows) => Ok(rows.drain(..).next().unwrap_or(JsonValue::Null)),
                other => Ok(other),
            }
        } else {
            Ok(v)
        };
    }

    // SELECT query dispatch (sync formatters)
    match config.format {
        OutputFormat::JsonLd => jsonld::format(result, &compactor, config),
        OutputFormat::SparqlJson => sparql::format(result, &compactor, config),
        OutputFormat::TypedJson => typed::format(result, &compactor, config),
        OutputFormat::AgentJson => agent_json::format(result, &compactor, config),
        OutputFormat::CypherJson => cypher::format(result, &compactor, config),
        OutputFormat::SparqlXml => Err(FormatError::InvalidBinding(
            "SPARQL XML produces String, not JsonValue. Use format_results_string_async() instead."
                .to_string(),
        )),
        OutputFormat::RdfXml => Err(FormatError::InvalidBinding(
            "RDF/XML produces String, not JsonValue. Use format_results_string_async() instead."
                .to_string(),
        )),
        OutputFormat::Tsv | OutputFormat::Csv => {
            unreachable!("Delimited formats rejected before dispatch")
        }
    }
}

/// Dataset-aware async formatting (multi-ledger).
///
/// Like [`format_results_async`], but for queries that span multiple ledgers:
/// **hydration** expansion routes each subject to its home-ledger view so
/// properties and `@id`s decode against the ledger that stores them, under that
/// ledger's own policy (issue #1259). Non-hydration output (flat SELECT,
/// CONSTRUCT, ASK) is dict-independent for cross-ledger IRIs — those carry
/// `IriMatch.iri` — so it formats against the dataset's primary view exactly as
/// before.
pub async fn format_results_async_dataset(
    result: &QueryResult,
    context: &ParsedContext,
    dataset: &crate::view::DataSetDb,
    config: &FormatterConfig,
    tracker: Option<&Tracker>,
) -> Result<JsonValue> {
    if matches!(config.format, OutputFormat::Tsv | OutputFormat::Csv) {
        return Err(FormatError::InvalidBinding(format!(
            "{:?} format produces bytes/String, not JsonValue. \
             Use format_results_string() or QueryResult::to_tsv()/to_csv() instead.",
            config.format
        )));
    }

    if result.output.has_hydration() {
        if !matches!(
            config.format,
            OutputFormat::JsonLd | OutputFormat::TypedJson
        ) {
            return Err(FormatError::InvalidBinding(
                "Hydration only supports JSON-LD and TypedJson output formats".to_string(),
            ));
        }
        let v = hydration::format_async_dataset(result, dataset, context, config, tracker).await?;
        return if result.output.is_select_one() {
            match v {
                JsonValue::Array(mut rows) => Ok(rows.drain(..).next().unwrap_or(JsonValue::Null)),
                other => Ok(other),
            }
        } else {
            Ok(v)
        };
    }

    // Non-hydration: format against the primary view. Cross-ledger subject/ref
    // IRIs in flat SELECT / CONSTRUCT come from `IriMatch.iri` (dict-independent),
    // so the single primary dict is sufficient.
    let primary = dataset.primary().ok_or_else(|| {
        FormatError::InvalidBinding("dataset has no graphs for formatting".to_string())
    })?;
    format_results_async(
        result,
        context,
        primary.as_graph_db_ref(),
        config,
        None,
        tracker,
    )
    .await
}

/// Format query results to a JSON string using async database access
///
/// Async convenience function that formats and serializes in one step.
/// Respects `config.pretty` for formatting.
///
/// Required for hydration queries. For other queries, can use sync version.
///
/// # Policy Support
///
/// When `policy` is `Some`, hydration queries filter flakes according to view policies.
/// When `policy` is `None`, no filtering is applied (zero overhead).
pub async fn format_results_string_async(
    result: &QueryResult,
    context: &ParsedContext,
    db: GraphDbRef<'_>,
    config: &FormatterConfig,
    policy: Option<&fluree_db_policy::PolicyContext>,
) -> Result<String> {
    // Delimited-text fast-path: skip JSON DOM and JSON serialization entirely
    match config.format {
        OutputFormat::Tsv => return delimited::format_tsv(result, db.snapshot),
        OutputFormat::Csv => return delimited::format_csv(result, db.snapshot),
        OutputFormat::SparqlXml => {
            let compactor = IriCompactor::new(db.snapshot.shared_namespaces(), context);
            return sparql_xml::format(result, &compactor, config);
        }
        OutputFormat::RdfXml => {
            let compactor = IriCompactor::new(db.snapshot.shared_namespaces(), context);
            return rdf_xml::format(result, &compactor, config);
        }
        _ => {}
    }

    // Stream JSON straight to a String for the common SELECT case (mirrors the
    // compactor that `format_results_async` would build internally).
    if json_stream_eligible(result, config) {
        let compactor = IriCompactor::new(db.snapshot.shared_namespaces(), context);
        return stream_json(result, &compactor, config);
    }

    let value = format_results_async(result, context, db, config, policy, None).await?;

    if config.pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

/// Dataset-aware string formatting (multi-ledger), mirroring
/// [`format_results_string_async`]. Delimited / XML output is dict-independent
/// for cross-ledger IRIs (carried as `IriMatch.iri`), so it formats against the
/// primary view; JSON output (incl. hydration) goes through the dataset-aware
/// path so cross-ledger subjects decode against their home ledger (issue #1259).
pub async fn format_results_string_async_dataset(
    result: &QueryResult,
    context: &ParsedContext,
    dataset: &crate::view::DataSetDb,
    config: &FormatterConfig,
    tracker: Option<&Tracker>,
) -> Result<String> {
    let primary = dataset.primary().ok_or_else(|| {
        FormatError::InvalidBinding("dataset has no graphs for formatting".to_string())
    })?;
    let primary_db = primary.as_graph_db_ref();
    match config.format {
        OutputFormat::Tsv => return delimited::format_tsv(result, primary_db.snapshot),
        OutputFormat::Csv => return delimited::format_csv(result, primary_db.snapshot),
        OutputFormat::SparqlXml => {
            let compactor = IriCompactor::new(primary_db.snapshot.shared_namespaces(), context);
            return sparql_xml::format(result, &compactor, config);
        }
        OutputFormat::RdfXml => {
            let compactor = IriCompactor::new(primary_db.snapshot.shared_namespaces(), context);
            return rdf_xml::format(result, &compactor, config);
        }
        _ => {}
    }

    // Non-hydration JSON formats against the primary view, exactly as the DOM
    // dataset path does — so the streaming compactor matches. Hydration is not
    // stream-eligible and continues through the dataset-aware DOM path below.
    if json_stream_eligible(result, config) {
        let compactor = IriCompactor::new(primary_db.snapshot.shared_namespaces(), context);
        return stream_json(result, &compactor, config);
    }

    let value = format_results_async_dataset(result, context, dataset, config, tracker).await?;

    if config.pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::var_registry::VarRegistry;

    fn make_test_result() -> QueryResult {
        QueryResult {
            vars: VarRegistry::new(),
            t: Some(0),
            novelty: None,
            context: fluree_graph_json_ld::ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![]),
            batches: vec![],
            binary_graph: None,
        }
    }

    #[test]
    fn format_ask_returns_none_for_non_boolean() {
        let result = make_test_result();
        let config = FormatterConfig::jsonld();
        assert!(format_ask(&result, &config).is_none());
    }

    #[test]
    fn format_ask_true_sparql_json() {
        let mut result = make_test_result();
        result.output = QueryOutput::Ask;
        result.batches = vec![fluree_db_query::binding::Batch::single_empty()];

        let config = FormatterConfig::sparql_json();
        let output = format_ask(&result, &config).unwrap().unwrap();
        assert_eq!(output, json!({"head": {}, "boolean": true}));
    }

    #[test]
    fn format_ask_false_sparql_json() {
        let mut result = make_test_result();
        result.output = QueryOutput::Ask;

        let config = FormatterConfig::sparql_json();
        let output = format_ask(&result, &config).unwrap().unwrap();
        assert_eq!(output, json!({"head": {}, "boolean": false}));
    }

    #[test]
    fn format_ask_true_jsonld() {
        let mut result = make_test_result();
        result.output = QueryOutput::Ask;
        result.batches = vec![fluree_db_query::binding::Batch::single_empty()];

        let config = FormatterConfig::jsonld();
        let output = format_ask(&result, &config).unwrap().unwrap();
        assert_eq!(output, JsonValue::Bool(true));
    }

    #[test]
    fn format_ask_false_jsonld() {
        let mut result = make_test_result();
        result.output = QueryOutput::Ask;

        let config = FormatterConfig::jsonld();
        let output = format_ask(&result, &config).unwrap().unwrap();
        assert_eq!(output, JsonValue::Bool(false));
    }

    // ====================================================================
    // Manual perf harness: streaming `format_string` vs DOM `format` +
    // `serde_json::to_string`, for each JSON format across number-heavy,
    // string-heavy, and mixed result sets.
    //
    // Ignored by default (it's a benchmark, not an assertion). Run in RELEASE
    // for meaningful numbers:
    //
    //   cargo test -p fluree-db-api --release --lib \
    //     format::tests::bench_stream_vs_dom -- --ignored --nocapture
    //
    // It also asserts byte-identity at 10k rows, so it doubles as a
    // large-scale parity check.
    // ====================================================================

    use fluree_db_core::{FlakeValue, Sid};
    use fluree_db_query::binding::{Batch, Binding};
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Instant;

    fn bench_compactor() -> IriCompactor {
        let mut ns = std::collections::HashMap::new();
        ns.insert(2u16, "http://www.w3.org/2001/XMLSchema#".to_string());
        ns.insert(100u16, "http://example.org/".to_string());
        IriCompactor::from_namespaces(Arc::new(ns))
    }

    /// Build an `n`-row `QueryResult` for one of the data shapes.
    fn bench_result(shape: &str, n: usize) -> QueryResult {
        let mut vars = VarRegistry::new();
        let xsd_long = Sid::new(2, "long");
        let xsd_double = Sid::new(2, "double");
        let xsd_string = Sid::new(2, "string");
        let xsd_bool = Sid::new(2, "boolean");

        let (names, cols): (Vec<&str>, Vec<Vec<Binding>>) = match shape {
            // All numeric literals (Long + Double).
            "numbers" => {
                let names = vec!["?a", "?b", "?c"];
                let a = (0..n)
                    .map(|i| Binding::lit(FlakeValue::Long(i as i64), xsd_long.clone()))
                    .collect();
                let b = (0..n)
                    .map(|i| Binding::lit(FlakeValue::Double(i as f64 * 1.5), xsd_double.clone()))
                    .collect();
                let c = (0..n)
                    .map(|i| Binding::lit(FlakeValue::Long((i as i64) * 7 - 3), xsd_long.clone()))
                    .collect();
                (names, vec![a, b, c])
            }
            // An IRI ref + two string literals (short + longer).
            "strings" => {
                let names = vec!["?id", "?name", "?desc"];
                let id = (0..n)
                    .map(|i| Binding::sid(Sid::new(100, format!("item{i}"))))
                    .collect();
                let name = (0..n)
                    .map(|i| {
                        Binding::lit(FlakeValue::String(format!("Name {i}")), xsd_string.clone())
                    })
                    .collect();
                let desc = (0..n)
                    .map(|i| {
                        Binding::lit(
                            FlakeValue::String(format!(
                                "A reasonably descriptive sentence for record number {i}."
                            )),
                            xsd_string.clone(),
                        )
                    })
                    .collect();
                (names, vec![id, name, desc])
            }
            // Mixed: ref + string + long + double + boolean.
            _mixed => {
                let names = vec!["?id", "?name", "?age", "?score", "?active"];
                let id = (0..n)
                    .map(|i| Binding::sid(Sid::new(100, format!("person{i}"))))
                    .collect();
                let name = (0..n)
                    .map(|i| {
                        Binding::lit(
                            FlakeValue::String(format!("Person {i}")),
                            xsd_string.clone(),
                        )
                    })
                    .collect();
                let age = (0..n)
                    .map(|i| Binding::lit(FlakeValue::Long((i % 90) as i64), xsd_long.clone()))
                    .collect();
                let score = (0..n)
                    .map(|i| Binding::lit(FlakeValue::Double((i as f64) / 3.0), xsd_double.clone()))
                    .collect();
                let active = (0..n)
                    .map(|i| Binding::lit(FlakeValue::Boolean(i % 2 == 0), xsd_bool.clone()))
                    .collect();
                (names, vec![id, name, age, score, active])
            }
        };

        let var_ids: Vec<fluree_db_query::VarId> =
            names.iter().map(|&nm| vars.get_or_insert(nm)).collect();
        let batch = Batch::new(Arc::from(var_ids.clone().into_boxed_slice()), cols).unwrap();
        QueryResult {
            vars,
            t: Some(1),
            novelty: None,
            context: fluree_graph_json_ld::ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(var_ids),
            batches: vec![batch],
            binary_graph: None,
        }
    }

    #[test]
    #[ignore = "perf benchmark; run with --release --ignored --nocapture"]
    fn bench_stream_vs_dom() {
        type DomFn = fn(&QueryResult, &IriCompactor, &FormatterConfig) -> Result<JsonValue>;
        type StreamFn = fn(&QueryResult, &IriCompactor, &FormatterConfig) -> Result<String>;

        const ROWS: usize = 10_000;
        const ITERS: usize = 100;

        let compactor = bench_compactor();
        let formats: [(&str, DomFn, StreamFn, FormatterConfig); 4] = [
            (
                "sparql_json",
                sparql::format,
                sparql::format_string,
                FormatterConfig::sparql_json(),
            ),
            (
                "typed_json",
                typed::format,
                typed::format_string,
                FormatterConfig::typed_json(),
            ),
            (
                "jsonld",
                jsonld::format,
                jsonld::format_string,
                FormatterConfig::jsonld(),
            ),
            (
                "agent_json",
                agent_json::format,
                agent_json::format_string,
                FormatterConfig::agent_json(),
            ),
        ];

        println!(
            "\n{ROWS} rows, {ITERS} iters/measurement  (DOM = format + serde_json::to_string, STREAM = format_string)\n"
        );
        println!(
            "{:<8} {:<12} {:>10} {:>11} {:>11} {:>9} {:>11}",
            "shape", "format", "bytes", "dom µs/it", "strm µs/it", "speedup", "MB/s strm"
        );
        println!("{}", "-".repeat(80));

        for shape in ["numbers", "strings", "mixed"] {
            let result = bench_result(shape, ROWS);
            for (name, dom, stream, cfg) in &formats {
                // Correctness at scale: streaming must equal DOM-then-serialize.
                let dom_once =
                    serde_json::to_string(&dom(&result, &compactor, cfg).unwrap()).unwrap();
                let stream_once = stream(&result, &compactor, cfg).unwrap();
                assert_eq!(
                    dom_once, stream_once,
                    "parity mismatch at {ROWS} rows: shape={shape} format={name}"
                );
                let bytes = stream_once.len();

                // Warm up.
                for _ in 0..3 {
                    black_box(
                        serde_json::to_string(&dom(&result, &compactor, cfg).unwrap()).unwrap(),
                    );
                    black_box(stream(&result, &compactor, cfg).unwrap());
                }

                let t0 = Instant::now();
                for _ in 0..ITERS {
                    let v = dom(&result, &compactor, cfg).unwrap();
                    black_box(serde_json::to_string(&v).unwrap().len());
                }
                let dom_elapsed = t0.elapsed();

                let t1 = Instant::now();
                for _ in 0..ITERS {
                    black_box(stream(&result, &compactor, cfg).unwrap().len());
                }
                let stream_elapsed = t1.elapsed();

                let dom_us = dom_elapsed.as_secs_f64() * 1e6 / ITERS as f64;
                let strm_us = stream_elapsed.as_secs_f64() * 1e6 / ITERS as f64;
                let speedup = dom_us / strm_us;
                let mbps = (bytes as f64) / (strm_us / 1e6) / (1024.0 * 1024.0);

                println!(
                    "{shape:<8} {name:<12} {bytes:>10} {dom_us:>11.1} {strm_us:>11.1} {speedup:>8.2}x {mbps:>10.0}"
                );
            }
        }
        println!();
    }
}
