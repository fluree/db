//! Result formatting configuration types
//!
//! This module provides configuration for controlling how query results
//! are formatted. Supports JSON-based formats (JSON-LD, SPARQL JSON, TypedJson)
//! and high-performance delimited-text formats (TSV, CSV).

// Re-export QueryOutput from fluree-db-query (canonical source)
pub use fluree_db_query::ir::QueryOutput;

/// Output format selection
///
/// Determines which format to use for query results. JSON formats produce
/// `serde_json::Value`; TSV produces bytes/strings directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// JSON-LD Query format (default)
    ///
    /// Simple JSON with compact IRIs. Rows are arrays aligned to SELECT order.
    /// - Array mode: `[["ex:alice", "Alice", 30], ...]`
    /// - Object mode: `[{"?s": "ex:alice", "?name": "Alice"}, ...]`
    #[default]
    JsonLd,

    /// W3C SPARQL 1.1 Query Results JSON format
    ///
    /// Standard format with type metadata:
    /// ```json
    /// {
    ///   "head": {"vars": ["s", "name"]},
    ///   "results": {"bindings": [{"s": {"type": "uri", "value": "..."}}]}
    /// }
    /// ```
    SparqlJson,

    /// W3C SPARQL 1.1 Query Results XML format
    ///
    /// Produces an XML document with root `<sparql>` in the
    /// `http://www.w3.org/2005/sparql-results#` namespace.
    ///
    /// **Note**: SPARQL XML produces `String`, not `JsonValue`. Use
    /// `format_results_string()` / `format_results_string_async()` or
    /// query builder `.execute_formatted_string()`.
    SparqlXml,

    /// RDF/XML graph serialization (`application/rdf+xml`)
    ///
    /// **Graph results only** (SPARQL CONSTRUCT / DESCRIBE). Produces `String`, not `JsonValue`.
    ///
    /// **Note**: Use `format_results_string()` / `format_results_string_async()` or
    /// query builder `.execute_formatted_string()`.
    RdfXml,

    /// Typed JSON format
    ///
    /// Always includes explicit datatype (even for inferable types):
    /// ```json
    /// [{"?s": {"@id": "ex:alice"}, "?name": {"@value": "Alice", "@type": "xsd:string"}}]
    /// ```
    TypedJson,

    /// Tab-separated values (high-performance path)
    ///
    /// Produces a header row of variable names followed by tab-separated values.
    /// IRIs are compacted via `@context`. Bypasses JSON DOM construction and JSON
    /// serialization entirely — writes directly to a byte buffer.
    ///
    /// **Note**: TSV produces `Vec<u8>` / `String`, not `JsonValue`. Use
    /// `format_results_string()`, `QueryResult::to_tsv()`, or `to_tsv_bytes()`
    /// instead of `format_results()`.
    Tsv,

    /// Comma-separated values (high-performance path)
    ///
    /// Same approach as TSV but with comma delimiter and RFC 4180 quoting.
    /// IRIs are compacted via `@context`. Bypasses JSON DOM construction and JSON
    /// serialization entirely — writes directly to a byte buffer.
    ///
    /// **Note**: CSV produces `Vec<u8>` / `String`, not `JsonValue`. Use
    /// `format_results_string()`, `QueryResult::to_csv()`, or `to_csv_bytes()`
    /// instead of `format_results()`.
    Csv,

    /// Agent JSON format (optimized for LLM/agent consumption)
    ///
    /// Produces a self-describing envelope with a schema header, compact object rows
    /// using native JSON types, and optional byte-budget truncation with pagination
    /// metadata:
    /// ```json
    /// {
    ///   "schema": {"?name": "xsd:string", "?age": "xsd:integer"},
    ///   "rows": [{"?name": "Alice", "?age": 30}],
    ///   "rowCount": 1,
    ///   "t": 5,
    ///   "hasMore": false
    /// }
    /// ```
    AgentJson,
}

/// Additional context for AgentJson formatting (resume query, timestamps)
///
/// Passed via `FormatterConfig` to avoid modifying `QueryResult`.
#[derive(Debug, Clone)]
pub struct AgentJsonContext {
    /// Original SPARQL query text (for generating resume queries)
    pub sparql_text: Option<String>,
    /// Number of FROM clauses in the query (1 = single-ledger)
    pub from_count: usize,
    /// Pre-resolved ISO-8601 timestamp for the query's effective time
    pub iso_timestamp: Option<String>,
    /// Row limit to use in resume queries (defaults to 100)
    pub resume_limit: usize,
}

impl Default for AgentJsonContext {
    fn default() -> Self {
        Self {
            sparql_text: None,
            from_count: 0,
            iso_timestamp: None,
            resume_limit: 100,
        }
    }
}

/// Configuration for result formatting
///
/// Controls all aspects of how query results are converted to JSON.
#[derive(Debug, Clone, Default)]
pub struct FormatterConfig {
    /// Output format to use
    pub format: OutputFormat,

    /// Pretty-print JSON output
    ///
    /// When true, uses indentation and newlines for human readability.
    pub pretty: bool,

    /// Normalize multi-value properties to always use arrays
    ///
    /// When true, graph crawl results always wrap property values in arrays,
    /// even when there is only a single value. This ensures predictable shapes
    /// for programmatic consumers (e.g., deserializing into `Vec<T>` fields).
    ///
    /// When false (default), single-valued properties return bare scalars and
    /// multi-valued properties return arrays (existing behavior). The
    /// `@container: @set` context annotation still forces arrays per-property.
    ///
    /// Only affects graph crawl formatting; tabular SELECT results are unaffected.
    pub normalize_arrays: bool,

    /// Maximum byte budget for AgentJson output
    ///
    /// When set, the AgentJson formatter truncates rows once cumulative serialized
    /// size exceeds this limit and sets `hasMore: true` in the envelope.
    pub max_bytes: Option<usize>,

    /// Additional context for AgentJson formatting
    pub agent_json_context: Option<AgentJsonContext>,
}

impl FormatterConfig {
    /// Create a default JSON-LD Query config (array rows)
    pub fn jsonld() -> Self {
        Self::default()
    }

    /// Create a SPARQL JSON config
    pub fn sparql_json() -> Self {
        Self {
            format: OutputFormat::SparqlJson,
            ..Default::default()
        }
    }

    /// Create a SPARQL XML config
    pub fn sparql_xml() -> Self {
        Self {
            format: OutputFormat::SparqlXml,
            ..Default::default()
        }
    }

    /// Create an RDF/XML config (graph results only: CONSTRUCT/DESCRIBE)
    pub fn rdf_xml() -> Self {
        Self {
            format: OutputFormat::RdfXml,
            ..Default::default()
        }
    }

    /// Create a TypedJson config
    pub fn typed_json() -> Self {
        Self {
            format: OutputFormat::TypedJson,
            ..Default::default()
        }
    }

    /// Create a TSV config (high-performance path)
    pub fn tsv() -> Self {
        Self {
            format: OutputFormat::Tsv,
            ..Default::default()
        }
    }

    /// Create a CSV config (high-performance path)
    pub fn csv() -> Self {
        Self {
            format: OutputFormat::Csv,
            ..Default::default()
        }
    }

    /// Create an AgentJson config (LLM/agent-optimized envelope format)
    pub fn agent_json() -> Self {
        Self {
            format: OutputFormat::AgentJson,
            ..Default::default()
        }
    }

    /// Enable pretty printing
    pub fn with_pretty(mut self) -> Self {
        self.pretty = true;
        self
    }

    /// Enable array normalization for graph crawl results
    ///
    /// When enabled, all multi-value properties are wrapped in arrays even
    /// when only a single value exists. This produces predictable shapes for
    /// deserialization into typed structs (e.g., `Vec<String>` fields).
    pub fn with_normalize_arrays(mut self) -> Self {
        self.normalize_arrays = true;
        self
    }

    /// Set byte budget for AgentJson truncation
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    /// Set AgentJson context (SPARQL text, FROM count, ISO timestamp)
    pub fn with_agent_json_context(mut self, context: AgentJsonContext) -> Self {
        self.agent_json_context = Some(context);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FormatterConfig::default();
        assert_eq!(config.format, OutputFormat::JsonLd);
        assert!(!config.pretty);
    }

    #[test]
    fn test_sparql_json_config() {
        let config = FormatterConfig::sparql_json();
        assert_eq!(config.format, OutputFormat::SparqlJson);
    }

    #[test]
    fn test_sparql_xml_config() {
        let config = FormatterConfig::sparql_xml();
        assert_eq!(config.format, OutputFormat::SparqlXml);
    }

    #[test]
    fn test_rdf_xml_config() {
        let config = FormatterConfig::rdf_xml();
        assert_eq!(config.format, OutputFormat::RdfXml);
    }

    #[test]
    fn test_typed_json_config() {
        let config = FormatterConfig::typed_json();
        assert_eq!(config.format, OutputFormat::TypedJson);
    }

    #[test]
    fn test_tsv_config() {
        let config = FormatterConfig::tsv();
        assert_eq!(config.format, OutputFormat::Tsv);
    }

    #[test]
    fn test_csv_config() {
        let config = FormatterConfig::csv();
        assert_eq!(config.format, OutputFormat::Csv);
    }

    #[test]
    fn test_agent_json_config() {
        let config = FormatterConfig::agent_json();
        assert_eq!(config.format, OutputFormat::AgentJson);
        assert!(config.max_bytes.is_none());
        assert!(config.agent_json_context.is_none());
    }

    #[test]
    fn test_builder_methods() {
        let config = FormatterConfig::jsonld().with_pretty();

        assert!(config.pretty);
    }

    #[test]
    fn test_agent_json_builder_methods() {
        let config = FormatterConfig::agent_json()
            .with_max_bytes(32768)
            .with_agent_json_context(AgentJsonContext {
                sparql_text: Some("SELECT * WHERE { ?s ?p ?o }".to_string()),
                from_count: 1,
                iso_timestamp: Some("2026-03-26T14:30:00Z".to_string()),
                ..Default::default()
            });

        assert_eq!(config.max_bytes, Some(32768));
        let ctx = config.agent_json_context.unwrap();
        assert_eq!(ctx.from_count, 1);
        assert!(ctx.iso_timestamp.is_some());
    }
}
