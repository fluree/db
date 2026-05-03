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
pub mod datatype;
pub mod delimited;
mod hydration;
pub mod iri;
mod jsonld;
mod materialize;
mod rdf_xml;
mod sparql;
mod sparql_xml;
mod typed;
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

    let compactor = IriCompactor::new(snapshot.namespaces(), context);

    // CONSTRUCT queries have dedicated output format
    if result.output.construct_template().is_some() {
        // Only JSON-LD makes sense for CONSTRUCT
        if config.format != OutputFormat::JsonLd {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT queries only support JSON-LD output format".to_string(),
            ));
        }
        return construct::format(result, &compactor);
    }

    // ASK queries: return boolean based on solution existence
    if let Some(result) = format_ask(result, config) {
        return result;
    }

    // Hydration queries require async formatting for database access
    if result.output.hydration().is_some() {
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
            let compactor = IriCompactor::new(snapshot.namespaces(), context);
            return sparql_xml::format(result, &compactor, config);
        }
        OutputFormat::RdfXml => {
            let compactor = IriCompactor::new(snapshot.namespaces(), context);
            return rdf_xml::format(result, &compactor, config);
        }
        _ => {}
    }

    let value = format_results(result, context, snapshot, config)?;

    if config.pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

// ============================================================================
// Boolean (ASK) formatting
// ============================================================================

/// If this is an ASK/Boolean query, produce the result directly.
/// Returns `None` for non-Boolean queries (caller continues to normal dispatch).
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

    let compactor = IriCompactor::new(db.snapshot.namespaces(), context);

    // CONSTRUCT queries have dedicated output format (sync, no DB access needed)
    if result.output.construct_template().is_some() {
        if config.format != OutputFormat::JsonLd {
            return Err(FormatError::InvalidBinding(
                "CONSTRUCT queries only support JSON-LD output format".to_string(),
            ));
        }
        return construct::format(result, &compactor);
    }

    // ASK queries: return boolean based on solution existence
    if let Some(result) = format_ask(result, config) {
        return result;
    }

    // Hydration queries use async formatter with DB access
    if result.output.hydration().is_some() {
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
        let v =
            hydration::format_async(result, hydration_db, &compactor, config, policy, tracker)
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
            let compactor = IriCompactor::new(db.snapshot.namespaces(), context);
            return sparql_xml::format(result, &compactor, config);
        }
        OutputFormat::RdfXml => {
            let compactor = IriCompactor::new(db.snapshot.namespaces(), context);
            return rdf_xml::format(result, &compactor, config);
        }
        _ => {}
    }

    let value = format_results_async(result, context, db, config, policy, None).await?;

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
}
