//! Dataset types for multi-graph query execution
//!
//! This module provides the API-layer types for declaring and resolving datasets:
//!
//! - [`DatasetSpec`]: Declarative specification from query parsing (unresolved)
//! - [`GraphSource`]: Individual graph source with optional time specification
//! - [`TimeSpec`]: Time-travel specification (at t, commit, or time)
//! - [`DataSetDb`](crate::view::DataSetDb): Resolved dataset composed of views
//!
//! # Architecture
//!
//! Dataset resolution follows a clear separation:
//!
//! | Layer | Responsibility |
//! |-------|---------------|
//! | `fluree-db-api` | Parse `DatasetSpec` from query, resolve aliases via nameservice, apply time-travel, build `DataSetDb` |
//! | `fluree-db-query` | Receive runtime `DataSet<'a>` (borrowed views), execute with graph-aware scanning |
//!
//! `fluree-db-query` should NOT know about ledger aliases, nameservice, or time-travel resolution.
//!
//! # Example
//!
//! ```ignore
//! // Parse dataset from JSON-LD query
//! let spec = DatasetSpec::from_json(&query)?;
//!
//! // Resolve via nameservice
//! let dataset = fluree.build_dataset_view(&spec).await?;
//!
//! // Execute query with dataset
//! let result = fluree.query_dataset(&dataset, &query).await?;
//! ```

use fluree_db_core::{
    ledger_id::{split_time_travel_suffix, LedgerIdTimeSpec},
    TrackingOptions,
};
use fluree_db_sparql::ast::{DatasetClause as SparqlDatasetClause, IriValue};

/// Convert a SPARQL IriValue to a string for use as a ledger identifier.
///
/// - Full IRIs (from `<...>` syntax) return the IRI string directly
/// - Prefixed IRIs return `prefix:local` (unexpanded)
///
/// # Note on Prefixed IRIs
///
/// SPARQL `FROM` clauses typically use full IRI syntax: `FROM <ledger:main>`.
/// The angle brackets make this a full IRI, even if it looks like a CURIE.
/// Actual prefixed names (`ex:graph` without brackets) would need the prologue
/// prefix map to expand properly.
///
/// For dataset identifiers (ledger aliases), we expect full IRIs in `<...>` form.
/// If prefixed names appear, they're passed through as-is and will likely fail
/// nameservice resolution unless the identifier happens to match.
fn iri_value_to_string(iri: &IriValue) -> String {
    match iri {
        IriValue::Full(s) => s.to_string(),
        IriValue::Prefixed { prefix, local } => {
            if prefix.is_empty() {
                format!(":{local}")
            } else {
                format!("{prefix}:{local}")
            }
        }
    }
}

/// Declarative dataset specification from query parsing
///
/// This is the API-layer type containing unresolved ledger aliases
/// and time-travel specs. It represents what the user requested,
/// before resolution via nameservice.
///
/// # Examples
///
/// JSON-LD query:
/// ```json
/// {
///   "from": "ledger:main",
///   "fromNamed": {
///     "graph1": { "@id": "graph1:main" },
///     "graph2": { "@id": "graph2:main" }
///   }
/// }
/// ```
///
/// SPARQL:
/// ```sparql
/// FROM <ledger:main>
/// FROM NAMED <graph1>
/// FROM NAMED <graph2>
/// ```
#[derive(Debug, Clone, Default)]
pub struct DatasetSpec {
    /// Default graphs - unioned for non-GRAPH patterns
    pub default_graphs: Vec<GraphSource>,
    /// Named graphs - accessible via GRAPH patterns
    pub named_graphs: Vec<GraphSource>,
    /// History mode time range (if detected)
    ///
    /// Set when explicit `from` and `to` keys are provided with time-specced endpoints
    /// for the same ledger (e.g., `"from": "ledger@t:1", "to": "ledger@t:latest"`).
    /// This indicates a history/changes query rather than a point-in-time query.
    pub history_range: Option<HistoryTimeRange>,
}

impl DatasetSpec {
    /// Create an empty dataset spec
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a default graph
    pub fn with_default(mut self, source: GraphSource) -> Self {
        self.default_graphs.push(source);
        self
    }

    /// Add a named graph
    pub fn with_named(mut self, source: GraphSource) -> Self {
        self.named_graphs.push(source);
        self
    }

    /// Check if this spec is empty (no graphs specified)
    pub fn is_empty(&self) -> bool {
        self.default_graphs.is_empty()
            && self.named_graphs.is_empty()
            && self.history_range.is_none()
    }

    /// Get total number of graphs specified
    pub fn num_graphs(&self) -> usize {
        self.default_graphs.len() + self.named_graphs.len()
    }

    /// Check if this is a history/changes query
    ///
    /// History mode is detected when explicit `from` and `to` keys are provided
    /// with time-specced endpoints for the same ledger, e.g.:
    /// ```json
    /// { "from": "ledger:main@t:1", "to": "ledger:main@t:latest" }
    /// ```
    pub fn is_history_mode(&self) -> bool {
        self.history_range.is_some()
    }

    /// Get the history time range if in history mode
    pub fn history_range(&self) -> Option<&HistoryTimeRange> {
        self.history_range.as_ref()
    }

    /// Create a DatasetSpec from a SPARQL DatasetClause
    ///
    /// Converts SPARQL FROM and FROM NAMED clauses to the API-layer
    /// DatasetSpec format.
    ///
    /// # Example
    ///
    /// ```sparql
    /// SELECT ?s
    /// FROM <http://example.org/graph1>
    /// FROM <http://example.org/graph2>
    /// FROM NAMED <http://example.org/named1>
    /// WHERE { ?s ?p ?o }
    /// ```
    ///
    /// Would produce a DatasetSpec with:
    /// - 2 default graphs (graph1, graph2)
    /// - 1 named graph (named1)
    ///
    /// ## Fluree Extension: History Range
    ///
    /// ```sparql
    /// SELECT ?s ?t ?op
    /// FROM <ledger:main@t:1> TO <ledger:main@t:latest>
    /// WHERE { ... }
    /// ```
    ///
    /// When `TO` clause is present, creates a history range query.
    pub fn from_sparql_clause(clause: &SparqlDatasetClause) -> Result<Self, DatasetParseError> {
        let default_graphs = clause
            .default_graphs
            .iter()
            .map(|iri| {
                let iri_str = iri_value_to_string(&iri.value);
                let (identifier, time_spec) = parse_ledger_id_time_travel(&iri_str)?;
                let mut source = GraphSource::new(identifier);
                source.time_spec = time_spec;
                Ok(source)
            })
            .collect::<Result<Vec<_>, DatasetParseError>>()?;

        let named_graphs = clause
            .named_graphs
            .iter()
            .map(|iri| {
                let iri_str = iri_value_to_string(&iri.value);
                let (identifier, time_spec) = parse_ledger_id_time_travel(&iri_str)?;
                let mut source = GraphSource::new(identifier);
                source.time_spec = time_spec;
                Ok(source)
            })
            .collect::<Result<Vec<_>, DatasetParseError>>()?;

        // Check for explicit TO clause (Fluree extension for history range)
        let history_range = if let Some(to_iri) = &clause.to_graph {
            // Explicit FROM...TO syntax
            if default_graphs.is_empty() {
                return Err(DatasetParseError::InvalidFrom(
                    "FROM...TO requires a FROM graph".to_string(),
                ));
            }
            let from_source = &default_graphs[0];
            let from_time = from_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "FROM graph in history range must have time specification".to_string(),
                )
            })?;

            let to_iri_str = iri_value_to_string(&to_iri.value);
            let (to_identifier, to_time_spec) = parse_ledger_id_time_travel(&to_iri_str)?;
            let to_time = to_time_spec.ok_or_else(|| {
                DatasetParseError::InvalidFrom("TO graph must have time specification".to_string())
            })?;

            // Verify same ledger
            if from_source.identifier != to_identifier {
                return Err(DatasetParseError::InvalidFrom(format!(
                    "FROM and TO must reference the same ledger: {} vs {}",
                    from_source.identifier, to_identifier
                )));
            }

            Some(HistoryTimeRange::new(
                &from_source.identifier,
                from_time.clone(),
                to_time,
            ))
        } else {
            // No TO clause = not a history query
            // Multiple FROM clauses are treated as a union query, not history
            None
        };

        Ok(Self {
            default_graphs,
            named_graphs,
            history_range,
        })
    }
}

/// Individual graph source with optional time specification
///
/// Represents a single graph in a dataset, identified by a ledger alias
/// (IRI) and optionally pinned to a specific time.
///
/// ## New fields (query-connection named graph support)
///
/// - `source_alias`: Dataset-local alias for referencing this source in the query.
///   Must be unique across all sources in a request.
/// - `graph_selector`: Which graph within the ledger to query (default, txn-meta, or IRI).
/// - `policy_override`: Per-source policy options (overrides global query options).
#[derive(Debug, Clone)]
pub struct GraphSource {
    /// Ledger alias or IRI (e.g., "mydb:main", "http://example.org/ledger1")
    pub identifier: String,
    /// Optional time-travel specification
    pub time_spec: Option<TimeSpec>,
    /// Dataset-local alias for this source (unique within the request)
    ///
    /// Used to reference this specific graph source in query patterns,
    /// especially when the same graph IRI exists in multiple ledgers.
    pub source_alias: Option<String>,
    /// Graph selector within the ledger
    ///
    /// If None, the default graph is selected (same as `GraphSelector::Default`).
    /// This is separate from the `#txn-meta` fragment in the identifier for cleaner semantics.
    pub graph_selector: Option<GraphSelector>,
    /// Per-source policy override
    ///
    /// If present, applies policy options only to this source, overriding
    /// any global policy settings for this specific graph.
    pub policy_override: Option<SourcePolicyOverride>,
}

/// Per-source policy override options
///
/// A subset of `QueryConnectionOptions` that can be applied per-source.
/// When present on a `GraphSource`, this policy takes precedence over any
/// global policy specified in `QueryConnectionOptions`.
#[derive(Debug, Clone, Default)]
pub struct SourcePolicyOverride {
    pub identity: Option<String>,
    pub policy_class: Option<Vec<String>>,
    pub policy: Option<JsonValue>,
    pub policy_values: Option<HashMap<String, JsonValue>>,
    pub default_allow: Option<bool>,
}

impl SourcePolicyOverride {
    /// Check if this override specifies any policy fields.
    ///
    /// Returns true if at least one policy field is set.
    pub fn has_policy(&self) -> bool {
        self.identity.is_some()
            || self.policy_class.is_some()
            || self.policy.is_some()
            || self.policy_values.is_some()
            || self.default_allow.is_some()
    }

    /// Convert to `QueryConnectionOptions` for policy wrapping.
    ///
    /// This creates a minimal `QueryConnectionOptions` with only the policy
    /// fields from this override, suitable for passing to `wrap_policy()`.
    pub fn to_query_connection_options(&self) -> QueryConnectionOptions {
        QueryConnectionOptions {
            identity: self.identity.clone(),
            policy_class: self.policy_class.clone(),
            policy: self.policy.clone(),
            policy_values: self.policy_values.clone(),
            default_allow: self.default_allow.unwrap_or(false),
            tracking: TrackingOptions::default(),
        }
    }
}

impl GraphSource {
    /// Create a graph source from an identifier
    pub fn new(identifier: impl Into<String>) -> Self {
        Self {
            identifier: identifier.into(),
            time_spec: None,
            source_alias: None,
            graph_selector: None,
            policy_override: None,
        }
    }

    /// Set time specification
    pub fn with_time(mut self, time_spec: TimeSpec) -> Self {
        self.time_spec = Some(time_spec);
        self
    }

    /// Set dataset-local alias
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.source_alias = Some(alias.into());
        self
    }

    /// Set graph selector
    pub fn with_graph(mut self, selector: GraphSelector) -> Self {
        self.graph_selector = Some(selector);
        self
    }

    /// Set per-source policy override
    pub fn with_policy(mut self, policy: SourcePolicyOverride) -> Self {
        self.policy_override = Some(policy);
        self
    }

    /// Create from identifier string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<&str> for GraphSource {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for GraphSource {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Graph selector for specifying which graph within a ledger to query.
///
/// A ledger can contain multiple named graphs:
/// - Default graph (g_id=0): the main data graph
/// - txn-meta graph (g_id=1): transaction metadata
/// - User-defined named graphs: arbitrary IRIs mapped to g_id via registry
#[derive(Debug, Clone, PartialEq)]
pub enum GraphSelector {
    /// The ledger's default graph (g_id=0)
    Default,
    /// The built-in transaction metadata graph (g_id=1)
    TxnMeta,
    /// A user-defined named graph by IRI
    /// The IRI is resolved to a g_id via the ledger's graph registry
    Iri(String),
}

impl GraphSelector {
    /// Create a selector for the default graph
    pub fn default_graph() -> Self {
        Self::Default
    }

    /// Create a selector for the txn-meta graph
    pub fn txn_meta() -> Self {
        Self::TxnMeta
    }

    /// Create a selector for a named graph by IRI
    pub fn iri(iri: impl Into<String>) -> Self {
        Self::Iri(iri.into())
    }

    /// Parse from string value (as used in JSON "graph" field)
    ///
    /// - `"default"` → Default
    /// - `"txn-meta"` → TxnMeta
    /// - anything else → Iri(value)
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "default" => Self::Default,
            "txn-meta" => Self::TxnMeta,
            _ => Self::Iri(s.to_string()),
        }
    }
}

/// Time specification for graph sources
///
/// Allows pinning a graph to a specific point in time.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeSpec {
    /// At a specific transaction number
    AtT(i64),
    /// At a specific commit hash
    AtCommit(String),
    /// At a specific ISO 8601 timestamp
    AtTime(String),
    /// "latest" keyword - resolves to current ledger t
    Latest,
}

impl TimeSpec {
    /// Create at-t specification
    pub fn at_t(t: i64) -> Self {
        Self::AtT(t)
    }

    /// Create at-commit specification
    pub fn at_commit(commit: impl Into<String>) -> Self {
        Self::AtCommit(commit.into())
    }

    /// Create at-time specification
    pub fn at_time(time: impl Into<String>) -> Self {
        Self::AtTime(time.into())
    }

    /// Create latest specification
    pub fn latest() -> Self {
        Self::Latest
    }
}

/// Time range for history queries
///
/// Represents a range of time for querying changes/history.
/// Detected when `from` is an array with two time-specced endpoints
/// for the same ledger (e.g., `["ledger@t:1", "ledger@t:latest"]`).
#[derive(Debug, Clone)]
pub struct HistoryTimeRange {
    /// The ledger identifier (without time suffix)
    pub identifier: String,
    /// Start of the time range
    pub from: TimeSpec,
    /// End of the time range
    pub to: TimeSpec,
}

impl HistoryTimeRange {
    /// Create a new history time range
    pub fn new(identifier: impl Into<String>, from: TimeSpec, to: TimeSpec) -> Self {
        Self {
            identifier: identifier.into(),
            from,
            to,
        }
    }
}

// =============================================================================
// JSON-LD Query Parsing
// =============================================================================

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Error type for dataset spec parsing
#[derive(Debug, Clone)]
pub enum DatasetParseError {
    /// Invalid "from" value type
    InvalidFrom(String),
    /// Invalid "fromNamed" / "from-named" value type
    InvalidFromNamed(String),
    /// Invalid graph source object
    InvalidGraphSource(String),
    /// Invalid query-connection options object
    InvalidOptions(String),
    /// Duplicate dataset-local alias
    DuplicateAlias(String),
    /// Ambiguous graph selector (both #txn-meta fragment and graph field)
    AmbiguousGraphSelector(String),
}

impl std::fmt::Display for DatasetParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFrom(msg) => write!(f, "Invalid 'from' value: {msg}"),
            Self::InvalidFromNamed(msg) => write!(f, "Invalid 'fromNamed' value: {msg}"),
            Self::InvalidGraphSource(msg) => write!(f, "Invalid graph source: {msg}"),
            Self::InvalidOptions(msg) => write!(f, "Invalid query options: {msg}"),
            Self::DuplicateAlias(alias) => {
                write!(f, "Duplicate dataset-local alias: '{alias}'")
            }
            Self::AmbiguousGraphSelector(id) => {
                write!(
                    f,
                    "Ambiguous graph selector for '{id}': cannot use both #txn-meta fragment and 'graph' field"
                )
            }
        }
    }
}

impl std::error::Error for DatasetParseError {}

impl DatasetSpec {
    /// Parse a DatasetSpec from JSON-LD query options
    ///
    /// Extracts "from" and "fromNamed" keys from the query object.
    ///
    /// # Supported formats
    ///
    /// **"from" (default graphs)**:
    /// - Single string: `"from": "ledger:main"`
    /// - Array of strings: `"from": ["ledger1:main", "ledger2:main"]`
    /// - Object with time: `"from": {"@id": "ledger:main", "t": 42}`
    /// - Array of objects: `"from": [{"@id": "ledger1", "t": 10}, "ledger2"]`
    /// - Object with alias/graph: `"from": {"@id": "ledger:main", "alias": "a", "graph": "txn-meta"}`
    ///
    /// **"fromNamed" (named graphs)** — object format (preferred):
    /// - Keys are dataset-local aliases, values have `@id` and optional `@graph`:
    ///   `"fromNamed": { "products": { "@id": "mydb:main", "@graph": "http://example.org/products" } }`
    ///
    /// **"from-named" (legacy)** — array format (backward compatible):
    /// - Single string: `"from-named": "graph1"`
    /// - Array: `"from-named": ["graph1", "graph2"]`
    /// - Objects with alias/graph/policy
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = json!({
    ///     "from": "ledger1:main",
    ///     "fromNamed": {
    ///         "graph1": { "@id": "graph1:main" }
    ///     },
    ///     "select": ["?s"],
    ///     "where": {"@id": "?s"}
    /// });
    ///
    /// let spec = DatasetSpec::from_json(&query)?;
    /// assert_eq!(spec.num_graphs(), 2);
    /// ```
    pub fn from_json(json: &JsonValue) -> Result<Self, DatasetParseError> {
        let obj = match json.as_object() {
            Some(o) => o,
            None => return Ok(Self::new()), // Not an object, return empty spec
        };

        let mut spec = Self::new();

        // Parse "from" (default graphs)
        if let Some(from_val) = obj.get("from") {
            spec.default_graphs = parse_graph_sources(from_val, "from")?;
        }

        // Check for explicit "to" key (history query)
        // Syntax: { "from": "ledger@t:1", "to": "ledger@t:latest" }
        // This mirrors SPARQL's FROM ... TO ... syntax
        if let Some(to_val) = obj.get("to") {
            // Must have exactly one "from" graph
            if spec.default_graphs.len() != 1 {
                return Err(DatasetParseError::InvalidFrom(
                    "'to' requires exactly one 'from' graph".to_string(),
                ));
            }
            let from_source = &spec.default_graphs[0];
            let to_source = parse_single_graph_source(to_val, "to")?;

            // Validate same ledger
            if from_source.identifier != to_source.identifier {
                return Err(DatasetParseError::InvalidFrom(format!(
                    "'from' and 'to' must reference the same ledger: '{}' vs '{}'",
                    from_source.identifier, to_source.identifier
                )));
            }

            // Require time specs on both
            let from_time = from_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'from' graph in history query must have time specification (e.g., ledger@t:1)"
                        .to_string(),
                )
            })?;
            let to_time = to_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'to' graph must have time specification (e.g., ledger@t:latest)".to_string(),
                )
            })?;

            spec.history_range = Some(HistoryTimeRange::new(
                &from_source.identifier,
                from_time.clone(),
                to_time.clone(),
            ));
        }

        // Parse "fromNamed" (preferred) or "from-named" (legacy key).
        // "fromNamed" takes precedence if both are present.
        // Both keys accept: object (keys = aliases), string, array, or null.
        if let Some(from_named_val) = obj.get("fromNamed") {
            if let Some(named_obj) = from_named_val.as_object() {
                spec.named_graphs = parse_named_graph_object(named_obj)?;
            } else {
                spec.named_graphs = parse_graph_sources(from_named_val, "fromNamed")?;
            }
        } else if let Some(from_named_val) = obj.get("from-named") {
            spec.named_graphs = parse_graph_sources(from_named_val, "from-named")?;
        }

        // Validate alias uniqueness across all sources
        validate_alias_uniqueness(&spec)?;

        Ok(spec)
    }

    /// Parse dataset + connection options from a query JSON object.
    ///
    /// Mirrors `query-connection` semantics:
    /// - Dataset spec may live at top-level (`from`, `fromNamed`, `ledger`) OR inside `opts`.
    /// - Connection/policy-related options are read from `opts`.
    /// - History queries use explicit `to` key: `{ "from": "ledger@t:1", "to": "ledger@t:latest" }`
    /// - Both `fromNamed` (object) and `from-named` (array, legacy) are accepted.
    pub fn from_query_json(
        json: &JsonValue,
    ) -> Result<(Self, QueryConnectionOptions), DatasetParseError> {
        let obj = match json.as_object() {
            Some(o) => o,
            None => return Ok((Self::new(), QueryConnectionOptions::default())),
        };

        let opts_obj = obj.get("opts").and_then(|v| v.as_object());

        // Dataset location precedence:
        // default aliases: opts.from || opts.ledger || query.from || query.ledger
        // named aliases:   opts.fromNamed || opts.from-named || query.fromNamed || query.from-named
        // to (history):    opts.to || query.to
        let from_val = opts_obj
            .and_then(|o| o.get("from"))
            .or_else(|| opts_obj.and_then(|o| o.get("ledger")))
            .or_else(|| obj.get("from"))
            .or_else(|| obj.get("ledger"));

        // "fromNamed" (new) takes precedence over "from-named" (legacy).
        let from_named_val = opts_obj
            .and_then(|o| o.get("fromNamed").or_else(|| o.get("from-named")))
            .or_else(|| obj.get("fromNamed"))
            .or_else(|| obj.get("from-named"));

        let to_val = opts_obj.and_then(|o| o.get("to")).or_else(|| obj.get("to"));

        let mut spec = Self::new();
        if let Some(v) = from_val {
            spec.default_graphs = parse_graph_sources(v, "from")?;
        }

        // Check for explicit "to" key (history query)
        if let Some(to_v) = to_val {
            // Must have exactly one "from" graph
            if spec.default_graphs.len() != 1 {
                return Err(DatasetParseError::InvalidFrom(
                    "'to' requires exactly one 'from' graph".to_string(),
                ));
            }
            let from_source = &spec.default_graphs[0];
            let to_source = parse_single_graph_source(to_v, "to")?;

            // Validate same ledger
            if from_source.identifier != to_source.identifier {
                return Err(DatasetParseError::InvalidFrom(format!(
                    "'from' and 'to' must reference the same ledger: '{}' vs '{}'",
                    from_source.identifier, to_source.identifier
                )));
            }

            // Require time specs on both
            let from_time = from_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'from' graph in history query must have time specification (e.g., ledger@t:1)"
                        .to_string(),
                )
            })?;
            let to_time = to_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'to' graph must have time specification (e.g., ledger@t:latest)".to_string(),
                )
            })?;

            spec.history_range = Some(HistoryTimeRange::new(
                &from_source.identifier,
                from_time.clone(),
                to_time.clone(),
            ));
        }

        if let Some(v) = from_named_val {
            if let Some(named_obj) = v.as_object() {
                spec.named_graphs = parse_named_graph_object(named_obj)?;
            } else {
                spec.named_graphs = parse_graph_sources(v, "from-named")?;
            }
        }

        // Validate alias uniqueness across all sources
        validate_alias_uniqueness(&spec)?;

        let qc_opts = QueryConnectionOptions::from_json(json)?;
        Ok((spec, qc_opts))
    }
}

/// Parsed query-connection options (policy/identity-related).
///
/// Supported keys in the query `opts` object:
/// - `identity`
/// - `policy-class`
/// - `policy`
/// - `policy-values`
/// - `default-allow`
/// - `meta` (tracking enablement: bool or object)
/// - `max-fuel` (fuel limit, also enables fuel tracking)
#[derive(Debug, Clone, Default)]
pub struct QueryConnectionOptions {
    pub identity: Option<String>,
    pub policy_class: Option<Vec<String>>,
    pub policy: Option<JsonValue>,
    pub policy_values: Option<HashMap<String, JsonValue>>,
    pub default_allow: bool,
    /// Tracking options parsed from `meta` and `max-fuel` in opts
    pub tracking: TrackingOptions,
}

impl QueryConnectionOptions {
    pub fn from_json(query: &JsonValue) -> Result<Self, DatasetParseError> {
        let obj = match query.as_object() {
            Some(o) => o,
            None => return Ok(Self::default()),
        };

        let opts_val = obj.get("opts");
        let opts = match opts_val {
            None | Some(JsonValue::Null) => return Ok(Self::default()),
            Some(JsonValue::Object(o)) => o,
            Some(other) => {
                return Err(DatasetParseError::InvalidOptions(format!(
                    "'opts' must be an object, got {other}"
                )))
            }
        };

        // Parse tracking options from opts
        let tracking = TrackingOptions::from_opts_value(opts_val);

        let identity = opts
            .get("identity")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string);

        let policy_class_val = opts
            .get("policy-class")
            .or_else(|| opts.get("policy_class"))
            .or_else(|| opts.get("policyClass"));
        let policy_class = match policy_class_val {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::String(s)) => Some(vec![s.to_string()]),
            Some(JsonValue::Array(arr)) => {
                let mut out = Vec::with_capacity(arr.len());
                for v in arr {
                    let Some(s) = v.as_str() else {
                        return Err(DatasetParseError::InvalidOptions(
                            "'policy-class' must be a string or array of strings".to_string(),
                        ));
                    };
                    out.push(s.to_string());
                }
                Some(out)
            }
            Some(_) => {
                return Err(DatasetParseError::InvalidOptions(
                    "'policy-class' must be a string or array of strings".to_string(),
                ))
            }
        };

        let policy = opts.get("policy").cloned().and_then(|v| match v {
            JsonValue::Null => None,
            other => Some(other),
        });

        let policy_values_val = opts
            .get("policy-values")
            .or_else(|| opts.get("policy_values"))
            .or_else(|| opts.get("policyValues"));
        let policy_values = match policy_values_val {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::Object(map)) => {
                Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            Some(_) => {
                return Err(DatasetParseError::InvalidOptions(
                    "'policy-values' must be an object".to_string(),
                ))
            }
        };

        let default_allow = opts
            .get("default-allow")
            .or_else(|| opts.get("default_allow"))
            .or_else(|| opts.get("defaultAllow"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        Ok(Self {
            identity,
            policy_class,
            policy,
            policy_values,
            default_allow,
            tracking,
        })
    }

    pub fn has_any_policy_inputs(&self) -> bool {
        self.identity.is_some()
            || self.policy_class.as_ref().is_some_and(|v| !v.is_empty())
            || self.policy.is_some()
            || self.policy_values.as_ref().is_some_and(|m| !m.is_empty())
            || self.default_allow
    }
}

/// Parse time-travel specification from ledger ID string.
///
/// Supports compatible formats:
/// - `ledger:main@t:42` → identifier="ledger:main", TimeSpec::AtT(42)
/// - `ledger:main@t:latest` → identifier="ledger:main", TimeSpec::Latest
/// - `ledger:main@iso:2025-01-01T00:00:00Z` → identifier="ledger:main", TimeSpec::AtTime(...)
/// - `ledger:main@commit:abc123` → identifier="ledger:main", TimeSpec::AtCommit(...)
///
/// Returns (identifier, Option<TimeSpec>).
fn parse_ledger_id_time_travel(
    ledger_id: &str,
) -> Result<(String, Option<TimeSpec>), DatasetParseError> {
    // Support optional named-graph fragment selector after time spec:
    //   ledger:main@t:42#txn-meta
    // We parse time-travel on the portion before '#', then re-attach the fragment
    // to the identifier (so the identifier remains stable and time is separate).
    let (before_fragment, fragment) = match ledger_id.split_once('#') {
        Some((left, right)) => {
            if right.is_empty() {
                return Err(DatasetParseError::InvalidGraphSource(
                    "Missing named graph after '#'".to_string(),
                ));
            }
            (left, Some(right))
        }
        None => (ledger_id, None),
    };
    let fragment_suffix = fragment.map(|f| format!("#{f}")).unwrap_or_default();

    // Check for @t:latest special case before standard parsing
    if let Some(base) = before_fragment.strip_suffix("@t:latest") {
        if base.is_empty() {
            return Err(DatasetParseError::InvalidGraphSource(
                "Ledger ID cannot be empty before '@'".to_string(),
            ));
        }
        let identifier = format!("{base}{fragment_suffix}");
        return Ok((identifier, Some(TimeSpec::Latest)));
    }

    let (identifier, time) = split_time_travel_suffix(before_fragment)
        .map_err(|e| DatasetParseError::InvalidGraphSource(e.to_string()))?;

    let time_spec = time.map(|spec| match spec {
        LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
        LedgerIdTimeSpec::AtIso(value) => TimeSpec::AtTime(value),
        LedgerIdTimeSpec::AtCommit(value) => TimeSpec::AtCommit(value),
    });

    Ok((format!("{identifier}{fragment_suffix}"), time_spec))
}

/// Parse graph sources from a JSON value
///
/// Accepts:
/// - String: single graph source (may include @t:/@iso:/@commit: time-travel syntax)
/// - Array: multiple graph sources
/// - Object: single graph source with time spec
fn parse_graph_sources(
    val: &JsonValue,
    field_name: &str,
) -> Result<Vec<GraphSource>, DatasetParseError> {
    match val {
        JsonValue::String(s) => {
            let (identifier, time_spec) = parse_ledger_id_time_travel(s)?;
            let mut source = GraphSource::new(identifier);
            source.time_spec = time_spec;
            Ok(vec![source])
        }
        JsonValue::Array(arr) => arr
            .iter()
            .map(|item| parse_single_graph_source(item, field_name))
            .collect(),
        JsonValue::Object(_) => Ok(vec![parse_single_graph_source(val, field_name)?]),
        JsonValue::Null => Ok(vec![]),
        _ => Err(DatasetParseError::InvalidFrom(format!(
            "'{field_name}' must be a string, array, or object"
        ))),
    }
}

/// Parse named graph sources from the new object format.
///
/// Accepts a JSON object where keys are dataset-local aliases and values are
/// objects with `@id` (optional ledger ref) and `@graph` (graph selector):
///
/// ```json
/// {
///   "products": {
///     "@id": "mydb:main",
///     "@graph": "http://example.org/graphs/products"
///   },
///   "services": {
///     "@id": "mydb:main",
///     "@graph": "http://example.org/graphs/services"
///   }
/// }
/// ```
///
/// Keys become the `source_alias`. The `@id` field is required (ledger reference).
/// The `@graph` field is optional (graph selector — "default", "txn-meta", or IRI).
fn parse_named_graph_object(
    obj: &serde_json::Map<String, JsonValue>,
) -> Result<Vec<GraphSource>, DatasetParseError> {
    let mut sources = Vec::with_capacity(obj.len());
    for (alias, entry_val) in obj {
        let entry = entry_val.as_object().ok_or_else(|| {
            DatasetParseError::InvalidFromNamed(format!(
                "fromNamed entry '{alias}' must be an object"
            ))
        })?;

        let raw_identifier = entry
            .get("@id")
            .or_else(|| entry.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                DatasetParseError::InvalidGraphSource(format!(
                    "fromNamed entry '{alias}' must have an '@id' string field"
                ))
            })?;

        let (identifier, time_spec) = parse_ledger_id_time_travel(raw_identifier)?;
        let mut source = GraphSource::new(&identifier);
        source.time_spec = time_spec;
        source.source_alias = Some(alias.clone());

        // Parse time specification from explicit keys (overrides string suffix)
        if let Some(t_val) = entry.get("t") {
            if let Some(t) = t_val.as_i64() {
                source.time_spec = Some(TimeSpec::AtT(t));
            }
        } else if let Some(at_val) = entry.get("at") {
            if let Some(at_str) = at_val.as_str() {
                if let Some(commit_hash) = at_str.strip_prefix("commit:") {
                    source.time_spec = Some(TimeSpec::AtCommit(commit_hash.to_string()));
                } else {
                    source.time_spec = Some(TimeSpec::AtTime(at_str.to_string()));
                }
            }
        }

        // Parse graph selector from @graph
        if let Some(graph_val) = entry.get("@graph") {
            if identifier.contains("#txn-meta") {
                return Err(DatasetParseError::AmbiguousGraphSelector(
                    raw_identifier.to_string(),
                ));
            }
            if let Some(graph_str) = graph_val.as_str() {
                source.graph_selector = Some(GraphSelector::from_str(graph_str));
            } else {
                return Err(DatasetParseError::InvalidGraphSource(
                    "'@graph' must be a string ('default', 'txn-meta', or a graph IRI)".to_string(),
                ));
            }
        }

        // Parse policy override
        if let Some(policy_val) = entry.get("policy") {
            source.policy_override = Some(parse_source_policy_override(policy_val)?);
        }

        sources.push(source);
    }
    Ok(sources)
}

/// Parse a single graph source from a JSON value
///
/// Accepts:
/// - String: identifier (may include @t:/@iso:/@commit: time-travel syntax and #txn-meta fragment)
/// - Object: Extended graph source object with optional fields:
///   - `@id` / `id`: ledger reference (required)
///   - `t` / `at`: time specification
///   - `alias`: dataset-local alias (optional)
///   - `graph`: graph selector - "default", "txn-meta", or IRI string (optional)
///   - `policy`: per-source policy override (optional)
fn parse_single_graph_source(
    val: &JsonValue,
    field_name: &str,
) -> Result<GraphSource, DatasetParseError> {
    match val {
        JsonValue::String(s) => {
            let (identifier, time_spec) = parse_ledger_id_time_travel(s)?;
            let mut source = GraphSource::new(identifier);
            source.time_spec = time_spec;
            Ok(source)
        }
        JsonValue::Object(obj) => {
            // Get identifier from @id or id
            let raw_identifier = obj
                .get("@id")
                .or_else(|| obj.get("id"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    DatasetParseError::InvalidGraphSource(format!(
                        "'{field_name}' object must have '@id' or 'id' string field"
                    ))
                })?;

            // Parse time-travel and fragment from the identifier
            let (identifier, time_spec) = parse_ledger_id_time_travel(raw_identifier)?;

            let mut source = GraphSource::new(&identifier);
            source.time_spec = time_spec;

            // Parse time specification from explicit keys (overrides string suffix)
            if let Some(t_val) = obj.get("t") {
                if let Some(t) = t_val.as_i64() {
                    source.time_spec = Some(TimeSpec::AtT(t));
                }
            } else if let Some(at_val) = obj.get("at") {
                if let Some(at_str) = at_val.as_str() {
                    // Determine if it's a commit hash or timestamp
                    if let Some(commit_hash) = at_str.strip_prefix("commit:") {
                        source.time_spec = Some(TimeSpec::AtCommit(commit_hash.to_string()));
                    } else {
                        // Assume ISO timestamp
                        source.time_spec = Some(TimeSpec::AtTime(at_str.to_string()));
                    }
                }
            }

            // Parse alias (dataset-local identifier for this source)
            if let Some(alias_val) = obj.get("alias") {
                if let Some(alias) = alias_val.as_str() {
                    source.source_alias = Some(alias.to_string());
                } else {
                    return Err(DatasetParseError::InvalidGraphSource(
                        "'alias' must be a string".to_string(),
                    ));
                }
            }

            // Parse graph selector
            if let Some(graph_val) = obj.get("graph") {
                // Check for ambiguity: identifier has #txn-meta AND graph field provided
                if identifier.contains("#txn-meta") {
                    return Err(DatasetParseError::AmbiguousGraphSelector(
                        raw_identifier.to_string(),
                    ));
                }

                if let Some(graph_str) = graph_val.as_str() {
                    source.graph_selector = Some(GraphSelector::from_str(graph_str));
                } else {
                    return Err(DatasetParseError::InvalidGraphSource(
                        "'graph' must be a string ('default', 'txn-meta', or a graph IRI)"
                            .to_string(),
                    ));
                }
            }

            // Parse policy override
            if let Some(policy_val) = obj.get("policy") {
                source.policy_override = Some(parse_source_policy_override(policy_val)?);
            }

            Ok(source)
        }
        _ => Err(DatasetParseError::InvalidGraphSource(format!(
            "'{field_name}' item must be a string or object"
        ))),
    }
}

/// Parse per-source policy override from JSON
fn parse_source_policy_override(
    val: &JsonValue,
) -> Result<SourcePolicyOverride, DatasetParseError> {
    let obj = val.as_object().ok_or_else(|| {
        DatasetParseError::InvalidGraphSource("'policy' must be an object".to_string())
    })?;

    let identity = obj
        .get("identity")
        .and_then(|v| v.as_str())
        .map(std::string::ToString::to_string);

    let policy_class_val = obj
        .get("policy-class")
        .or_else(|| obj.get("policy_class"))
        .or_else(|| obj.get("policyClass"));
    let policy_class = match policy_class_val {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::String(s)) => Some(vec![s.to_string()]),
        Some(JsonValue::Array(arr)) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                let Some(s) = v.as_str() else {
                    return Err(DatasetParseError::InvalidGraphSource(
                        "'policy-class' must be a string or array of strings".to_string(),
                    ));
                };
                out.push(s.to_string());
            }
            Some(out)
        }
        Some(_) => {
            return Err(DatasetParseError::InvalidGraphSource(
                "'policy-class' must be a string or array of strings".to_string(),
            ))
        }
    };

    let policy = obj.get("policy").cloned().and_then(|v| match v {
        JsonValue::Null => None,
        other => Some(other),
    });

    let policy_values_val = obj
        .get("policy-values")
        .or_else(|| obj.get("policy_values"))
        .or_else(|| obj.get("policyValues"));
    let policy_values = match policy_values_val {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::Object(map)) => {
            Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        }
        Some(_) => {
            return Err(DatasetParseError::InvalidGraphSource(
                "'policy-values' must be an object".to_string(),
            ))
        }
    };

    let default_allow = obj
        .get("default-allow")
        .or_else(|| obj.get("default_allow"))
        .or_else(|| obj.get("defaultAllow"))
        .and_then(serde_json::Value::as_bool);

    Ok(SourcePolicyOverride {
        identity,
        policy_class,
        policy,
        policy_values,
        default_allow,
    })
}

/// Validate that all dataset-local aliases are unique across the dataset spec.
///
/// Per the handoff spec: "if an alias appears more than once in the request
/// (across both 'from' and 'fromNamed'), return an error."
///
/// Also validates that aliases don't collide with identifiers, since the dataset
/// builder adds both identifier and alias as lookup keys in the runtime dataset.
fn validate_alias_uniqueness(spec: &DatasetSpec) -> Result<(), DatasetParseError> {
    use std::collections::HashSet;

    // Collect all identifiers first (these are always present)
    let mut all_keys: HashSet<String> = spec
        .default_graphs
        .iter()
        .chain(spec.named_graphs.iter())
        .map(|s| s.identifier.clone())
        .collect();

    // Check each alias for collisions
    for source in spec.default_graphs.iter().chain(spec.named_graphs.iter()) {
        if let Some(alias) = &source.source_alias {
            // Check against identifiers and other aliases
            if !all_keys.insert(alias.clone()) {
                return Err(DatasetParseError::DuplicateAlias(alias.clone()));
            }
        }
    }

    Ok(())
}

/// Extract unique ledger identifiers from a SPARQL query's FROM / FROM NAMED clauses.
///
/// Parses the SPARQL, extracts the dataset clause, strips time-travel suffixes,
/// and returns the de-duplicated base ledger IDs.
///
/// Returns `Ok(vec![])` if the query has no FROM/FROM NAMED clauses.
/// Returns `Err` only for SPARQL parse failures that prevent dataset extraction.
pub fn sparql_dataset_ledger_ids(sparql: &str) -> Result<Vec<String>, DatasetParseError> {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    let ast = parsed.ast.ok_or_else(|| {
        let msg = parsed
            .diagnostics
            .first()
            .map(|d| d.message.clone())
            .unwrap_or_else(|| "unknown parse error".to_string());
        DatasetParseError::InvalidFrom(format!("SPARQL parse error: {msg}"))
    })?;

    let dataset_clause = match &ast.body {
        fluree_db_sparql::ast::QueryBody::Select(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Construct(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Ask(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Describe(q) => q.dataset.as_ref(),
        fluree_db_sparql::ast::QueryBody::Update(_) => None,
    };

    let Some(clause) = dataset_clause else {
        return Ok(vec![]);
    };

    let spec = DatasetSpec::from_sparql_clause(clause)?;

    // Collect unique identifiers (base ledger IDs, time-travel already stripped)
    let mut seen = std::collections::HashSet::new();
    let mut ledger_ids = Vec::new();
    for source in spec.default_graphs.iter().chain(spec.named_graphs.iter()) {
        // Strip #txn-meta or other fragments — the scope check is on the base ledger
        let base = source
            .identifier
            .split('#')
            .next()
            .unwrap_or(&source.identifier);
        if seen.insert(base.to_string()) {
            ledger_ids.push(base.to_string());
        }
    }
    // Also include the history range ledger if present
    if let Some(range) = &spec.history_range {
        if seen.insert(range.identifier.clone()) {
            ledger_ids.push(range.identifier.clone());
        }
    }

    Ok(ledger_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_spec_empty() {
        let spec = DatasetSpec::new();
        assert!(spec.is_empty());
        assert_eq!(spec.num_graphs(), 0);
    }

    #[test]
    fn test_dataset_spec_with_graphs() {
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("ledger1:main"))
            .with_default(GraphSource::new("ledger2:main"))
            .with_named(GraphSource::new("graph1"));

        assert!(!spec.is_empty());
        assert_eq!(spec.num_graphs(), 3);
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.named_graphs.len(), 1);
    }

    #[test]
    fn test_graph_source_with_time() {
        let source = GraphSource::new("mydb:main").with_time(TimeSpec::at_t(42));

        assert_eq!(source.identifier, "mydb:main");
        assert!(matches!(source.time_spec, Some(TimeSpec::AtT(42))));
    }

    #[test]
    fn test_graph_source_from_str() {
        let source: GraphSource = "test:ledger".into();
        assert_eq!(source.identifier, "test:ledger");
        assert!(source.time_spec.is_none());
    }

    #[test]
    fn test_time_spec_variants() {
        let t = TimeSpec::at_t(100);
        assert!(matches!(t, TimeSpec::AtT(100)));

        let commit = TimeSpec::at_commit("abc123");
        assert!(matches!(commit, TimeSpec::AtCommit(ref s) if s == "abc123"));

        let time = TimeSpec::at_time("2024-01-01T00:00:00Z");
        assert!(matches!(time, TimeSpec::AtTime(ref s) if s == "2024-01-01T00:00:00Z"));
    }

    // JSON-LD Query Parsing Tests

    use serde_json::json;

    #[test]
    fn test_parse_from_single_string() {
        let query = json!({
            "from": "ledger:main",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(spec.named_graphs.is_empty());
    }

    #[test]
    fn test_parse_from_array() {
        let query = json!({
            "from": ["ledger1:main", "ledger2:main"],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
        assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
    }

    #[test]
    fn test_parse_from_with_time_t() {
        let query = json!({
            "from": {"@id": "ledger:main", "t": 42},
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
    }

    #[test]
    fn test_parse_from_with_commit() {
        let query = json!({
            "from": {"@id": "ledger:main", "at": "commit:abc123"},
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert!(matches!(
            &spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtCommit(s)) if s == "abc123"
        ));
    }

    // Ledger ID time-travel syntax tests (@t:, @iso:, @commit:)

    #[test]
    fn test_parse_ledger_id_at_t() {
        let query = json!({
            "from": "ledger:main@t:42",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
    }

    #[test]
    fn test_parse_ledger_id_at_t_with_named_graph_fragment() {
        let query = json!({
            "from": "ledger:main@t:42#txn-meta",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main#txn-meta");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
    }

    #[test]
    fn test_parse_ledger_id_at_iso() {
        let query = json!({
            "from": "ledger:main@iso:2025-01-20T00:00:00Z",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            &spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtTime(s)) if s == "2025-01-20T00:00:00Z"
        ));
    }

    #[test]
    fn test_parse_ledger_id_at_commit() {
        let query = json!({
            "from": "ledger:main@commit:abc123def456",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            &spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtCommit(s)) if s == "abc123def456"
        ));
    }

    #[test]
    fn test_parse_ledger_id_at_commit_too_short() {
        let query = json!({
            "from": "ledger:main@commit:abc",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_ledger_id_array_mixed_time_specs() {
        let query = json!({
            "from": ["ledger1:main@t:10", "ledger2:main", "ledger3:main@iso:2025-01-01T00:00:00Z"],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 3);

        assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(10))
        ));

        assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
        assert!(spec.default_graphs[1].time_spec.is_none());

        assert_eq!(spec.default_graphs[2].identifier, "ledger3:main");
        assert!(matches!(
            &spec.default_graphs[2].time_spec,
            Some(TimeSpec::AtTime(s)) if s == "2025-01-01T00:00:00Z"
        ));
    }

    #[test]
    fn test_parse_ledger_id_invalid_time_format() {
        let query = json!({
            "from": "ledger:main@invalid:123",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_from_named_legacy_array() {
        // Legacy array format: "from-named": ["graph1", "graph2"]
        let query = json!({
            "from": "default:main",
            "from-named": ["graph1", "graph2"],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(spec.named_graphs[0].identifier, "graph1");
        assert_eq!(spec.named_graphs[1].identifier, "graph2");
    }

    #[test]
    fn test_parse_from_named_object_format() {
        // New object format: "fromNamed": { alias: { "@id": ..., "@graph": ... } }
        let query = json!({
            "from": "default:main",
            "fromNamed": {
                "products": {
                    "@id": "mydb:main",
                    "@graph": "http://example.org/graphs/products"
                },
                "services": {
                    "@id": "mydb:main",
                    "@graph": "http://example.org/graphs/services"
                }
            },
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.named_graphs.len(), 2);

        // Find by alias (order not guaranteed in JSON objects)
        let products = spec
            .named_graphs
            .iter()
            .find(|g| g.source_alias.as_deref() == Some("products"))
            .expect("should have products alias");
        assert_eq!(products.identifier, "mydb:main");
        assert!(matches!(
            &products.graph_selector,
            Some(GraphSelector::Iri(ref iri)) if iri == "http://example.org/graphs/products"
        ));

        let services = spec
            .named_graphs
            .iter()
            .find(|g| g.source_alias.as_deref() == Some("services"))
            .expect("should have services alias");
        assert_eq!(services.identifier, "mydb:main");
        assert!(matches!(
            &services.graph_selector,
            Some(GraphSelector::Iri(ref iri)) if iri == "http://example.org/graphs/services"
        ));
    }

    #[test]
    fn test_parse_from_named_object_takes_precedence_over_legacy() {
        // When both "fromNamed" and "from-named" are present, "fromNamed" wins.
        let query = json!({
            "from": "default:main",
            "fromNamed": {
                "products": { "@id": "mydb:main", "@graph": "http://example.org/products" }
            },
            "from-named": ["should-be-ignored"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.named_graphs.len(), 1);
        assert_eq!(
            spec.named_graphs[0].source_alias,
            Some("products".to_string())
        );
    }

    #[test]
    fn test_parse_from_named_object_rejects_non_object_entries() {
        let query = json!({
            "fromNamed": {
                "bad": "not-an-object"
            },
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_from_named_accepts_string_array() {
        // "fromNamed" accepts both object (keys = aliases) and array (simple identifiers)
        let query = json!({
            "fromNamed": ["graph1", "graph2"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(spec.named_graphs[0].identifier, "graph1");
        assert_eq!(spec.named_graphs[1].identifier, "graph2");
    }

    #[test]
    fn test_parse_mixed_from_array() {
        let query = json!({
            "from": [
                "ledger1:main",
                {"@id": "ledger2:main", "t": 10}
            ],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
        assert!(spec.default_graphs[0].time_spec.is_none());
        assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
        assert!(matches!(
            spec.default_graphs[1].time_spec,
            Some(TimeSpec::AtT(10))
        ));
    }

    #[test]
    fn test_parse_no_dataset() {
        let query = json!({
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(spec.is_empty());
    }

    #[test]
    fn test_parse_null_from() {
        let query = json!({
            "from": null,
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(spec.default_graphs.is_empty());
    }

    // SPARQL DatasetClause Conversion Tests

    use fluree_db_sparql::ast::{DatasetClause as SparqlDatasetClause, Iri};
    use fluree_db_sparql::SourceSpan;

    fn make_span() -> SourceSpan {
        SourceSpan::new(0, 0)
    }

    #[test]
    fn test_from_sparql_clause_empty() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![],
            named_graphs: vec![],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert!(spec.is_empty());
    }

    #[test]
    fn test_from_sparql_clause_single_default() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::full("http://example.org/graph1", make_span())],
            named_graphs: vec![],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(
            spec.default_graphs[0].identifier,
            "http://example.org/graph1"
        );
        assert!(spec.named_graphs.is_empty());
    }

    #[test]
    fn test_from_sparql_clause_multiple_default() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![
                Iri::full("http://example.org/graph1", make_span()),
                Iri::full("http://example.org/graph2", make_span()),
            ],
            named_graphs: vec![],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(
            spec.default_graphs[0].identifier,
            "http://example.org/graph1"
        );
        assert_eq!(
            spec.default_graphs[1].identifier,
            "http://example.org/graph2"
        );
    }

    #[test]
    fn test_from_sparql_clause_named_graphs() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![],
            named_graphs: vec![
                Iri::full("http://example.org/named1", make_span()),
                Iri::full("http://example.org/named2", make_span()),
            ],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert!(spec.default_graphs.is_empty());
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(spec.named_graphs[0].identifier, "http://example.org/named1");
        assert_eq!(spec.named_graphs[1].identifier, "http://example.org/named2");
    }

    #[test]
    fn test_from_sparql_clause_mixed() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::full("http://example.org/default1", make_span())],
            named_graphs: vec![
                Iri::full("http://example.org/named1", make_span()),
                Iri::full("http://example.org/named2", make_span()),
            ],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(
            spec.default_graphs[0].identifier,
            "http://example.org/default1"
        );
    }

    #[test]
    fn test_from_sparql_clause_prefixed_iri() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::prefixed("ex", "graph1", make_span())],
            named_graphs: vec![Iri::prefixed("", "localname", make_span())],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ex:graph1");
        assert_eq!(spec.named_graphs.len(), 1);
        assert_eq!(spec.named_graphs[0].identifier, ":localname");
    }

    #[test]
    fn test_from_sparql_clause_time_travel_suffix() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![
                Iri::full("ledger:main@t:42", make_span()),
                Iri::full("ledger:main@iso:2025-01-01T00:00:00Z", make_span()),
            ],
            named_graphs: vec![Iri::full("ledger:main@commit:abc123def456", make_span())],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
        assert_eq!(spec.default_graphs[1].identifier, "ledger:main");
        assert!(matches!(
            &spec.default_graphs[1].time_spec,
            Some(TimeSpec::AtTime(s)) if s == "2025-01-01T00:00:00Z"
        ));

        assert_eq!(spec.named_graphs.len(), 1);
        assert_eq!(spec.named_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            &spec.named_graphs[0].time_spec,
            Some(TimeSpec::AtCommit(s)) if s == "abc123def456"
        ));
    }

    #[test]
    fn test_from_sparql_clause_to_graph_history_range() {
        // FROM <ledger:main@t:1> TO <ledger:main@t:latest>
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::full("ledger:main@t:1", make_span())],
            named_graphs: vec![],
            to_graph: Some(Iri::full("ledger:main@t:latest", make_span())),
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode from TO clause"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.identifier, "ledger:main");
        assert!(matches!(range.from, TimeSpec::AtT(1)));
        assert!(matches!(range.to, TimeSpec::Latest));
    }

    // History Mode Detection Tests - Explicit "to" Syntax
    //
    // History mode is now detected via explicit "to" key syntax, mirroring SPARQL FROM ... TO ...
    // The old heuristic (detecting from two-element arrays) was removed as it was ambiguous:
    // - `from: ["ledger@t:1", "ledger@t:latest"]` could mean either:
    //   1. History query (show changes between t:1 and t:latest)
    //   2. Union query (join two immutable views of the same ledger)
    //
    // New explicit syntax:
    // - History query: `{ "from": "ledger@t:1", "to": "ledger@t:latest" }`
    // - Union query:   `{ "from": ["ledger@t:1", "ledger@t:latest"] }`

    #[test]
    fn test_history_mode_explicit_to_key() {
        // Explicit "to" key = history mode
        let query = json!({
            "from": "ledger:main@t:1",
            "to": "ledger:main@t:latest",
            "select": ["?t", "?op", "?age"],
            "where": {"@id": "ex:alice", "ex:age": {"@value": "?age", "@t": "?t", "@op": "?op"}}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode from explicit 'to' key"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.identifier, "ledger:main");
        assert!(matches!(range.from, TimeSpec::AtT(1)));
        assert!(matches!(range.to, TimeSpec::Latest));
    }

    #[test]
    fn test_history_mode_with_iso_range_explicit() {
        // Explicit "to" key with ISO dates
        let query = json!({
            "from": "ledger:main@iso:2024-01-01T00:00:00Z",
            "to": "ledger:main@iso:2024-12-31T23:59:59Z",
            "select": ["?t", "?age"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode with ISO dates"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.identifier, "ledger:main");
        assert!(matches!(&range.from, TimeSpec::AtTime(s) if s == "2024-01-01T00:00:00Z"));
        assert!(matches!(&range.to, TimeSpec::AtTime(s) if s == "2024-12-31T23:59:59Z"));
    }

    #[test]
    fn test_history_mode_mixed_time_types_explicit() {
        // Different time types (commit and t) for same ledger with explicit "to"
        let query = json!({
            "from": "ledger:main@commit:abc123def456",
            "to": "ledger:main@t:latest",
            "select": ["?t", "?age"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            spec.is_history_mode(),
            "Mixed time types should be history mode"
        );

        let range = spec.history_range().expect("Should have history range");
        assert!(matches!(&range.from, TimeSpec::AtCommit(s) if s == "abc123def456"));
        assert!(matches!(range.to, TimeSpec::Latest));
    }

    #[test]
    fn test_not_history_mode_array_same_ledger_different_times() {
        // Array syntax with same ledger at different times = union query, NOT history mode
        // This is the key semantic change: arrays are always union queries, even with time specs
        let query = json!({
            "from": ["ledger:main@t:1", "ledger:main@t:latest"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Array syntax should NOT be history mode (use explicit 'to')"
        );
        assert!(spec.history_range().is_none());
        // Should have two separate graphs
        assert_eq!(spec.default_graphs.len(), 2);
    }

    #[test]
    fn test_not_history_mode_different_ledgers() {
        // Two endpoints for DIFFERENT ledgers = NOT history mode
        let query = json!({
            "from": ["ledger1:main@t:1", "ledger2:main@t:latest"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Different ledgers should not be history mode"
        );
        assert!(spec.history_range().is_none());
    }

    #[test]
    fn test_not_history_mode_single_endpoint() {
        // Single endpoint = NOT history mode (point-in-time query)
        let query = json!({
            "from": "ledger:main@t:100",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Single endpoint should not be history mode"
        );
    }

    #[test]
    fn test_not_history_mode_no_time_specs() {
        // Array without time specs = NOT history mode (multi-ledger union)
        let query = json!({
            "from": ["ledger1:main", "ledger2:main"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "No time specs should not be history mode"
        );
    }

    #[test]
    fn test_not_history_mode_partial_time_specs() {
        // Only one endpoint has time spec = NOT history mode
        let query = json!({
            "from": ["ledger:main@t:1", "ledger:main"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Partial time specs should not be history mode"
        );
    }

    // Error cases for explicit "to" syntax

    #[test]
    fn test_to_requires_single_from_graph() {
        // "to" key requires exactly one "from" graph
        let query = json!({
            "from": ["ledger:main@t:1", "ledger2:main@t:1"],
            "to": "ledger:main@t:latest",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "'to' with multiple 'from' graphs should error"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exactly one"),
            "Error should mention 'exactly one': {err}"
        );
    }

    #[test]
    fn test_to_requires_same_ledger_as_from() {
        // "from" and "to" must reference the same ledger
        let query = json!({
            "from": "ledger1:main@t:1",
            "to": "ledger2:main@t:latest",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "'from' and 'to' with different ledgers should error"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("same ledger"),
            "Error should mention 'same ledger': {err}"
        );
    }

    #[test]
    fn test_to_requires_time_spec_on_from() {
        // "from" in history query must have time specification
        let query = json!({
            "from": "ledger:main",
            "to": "ledger:main@t:latest",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "'from' without time spec should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("time specification"),
            "Error should mention 'time specification': {err}"
        );
    }

    #[test]
    fn test_to_requires_time_spec_on_to() {
        // "to" must have time specification
        let query = json!({
            "from": "ledger:main@t:1",
            "to": "ledger:main",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "'to' without time spec should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("time specification"),
            "Error should mention 'time specification': {err}"
        );
    }

    #[test]
    fn test_parse_latest_keyword() {
        let query = json!({
            "from": "ledger:main@t:latest",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::Latest)
        ));
    }

    #[test]
    fn test_parse_latest_keyword_with_named_graph_fragment() {
        let query = json!({
            "from": "ledger:main@t:latest#txn-meta",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main#txn-meta");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::Latest)
        ));
    }

    // =============================================================================
    // Named Graph / Graph Selector Tests (query-connection handoff spec)
    // =============================================================================

    #[test]
    fn test_graph_selector_from_str() {
        assert!(matches!(
            GraphSelector::from_str("default"),
            GraphSelector::Default
        ));
        assert!(matches!(
            GraphSelector::from_str("txn-meta"),
            GraphSelector::TxnMeta
        ));
        assert!(matches!(
            GraphSelector::from_str("http://example.org/graph"),
            GraphSelector::Iri(ref s) if s == "http://example.org/graph"
        ));
        // IRI with hash (should not be confused with "default" or "txn-meta")
        assert!(matches!(
            GraphSelector::from_str("http://example.org/vocab#products"),
            GraphSelector::Iri(ref s) if s == "http://example.org/vocab#products"
        ));
    }

    #[test]
    fn test_graph_source_with_alias() {
        let source = GraphSource::new("ledger:main")
            .with_alias("myAlias")
            .with_time(TimeSpec::at_t(42));

        assert_eq!(source.identifier, "ledger:main");
        assert_eq!(source.source_alias, Some("myAlias".to_string()));
        assert!(matches!(source.time_spec, Some(TimeSpec::AtT(42))));
    }

    #[test]
    fn test_graph_source_with_graph_selector() {
        let source = GraphSource::new("ledger:main").with_graph(GraphSelector::TxnMeta);

        assert_eq!(source.identifier, "ledger:main");
        assert!(matches!(
            source.graph_selector,
            Some(GraphSelector::TxnMeta)
        ));
    }

    #[test]
    fn test_parse_from_object_with_alias() {
        let query = json!({
            "from": {"@id": "ledger:main", "alias": "mydb"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert_eq!(
            spec.default_graphs[0].source_alias,
            Some("mydb".to_string())
        );
    }

    #[test]
    fn test_parse_from_object_with_graph_default() {
        let query = json!({
            "from": {"@id": "ledger:main", "graph": "default"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert!(matches!(
            spec.default_graphs[0].graph_selector,
            Some(GraphSelector::Default)
        ));
    }

    #[test]
    fn test_parse_from_object_with_graph_txn_meta() {
        let query = json!({
            "from": {"@id": "ledger:main", "alias": "meta", "graph": "txn-meta"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert_eq!(
            spec.default_graphs[0].source_alias,
            Some("meta".to_string())
        );
        assert!(matches!(
            spec.default_graphs[0].graph_selector,
            Some(GraphSelector::TxnMeta)
        ));
    }

    #[test]
    fn test_parse_from_object_with_graph_iri() {
        let query = json!({
            "from": {
                "@id": "ledger:main",
                "alias": "products",
                "graph": "http://example.org/vocab#products"
            },
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert_eq!(
            spec.default_graphs[0].source_alias,
            Some("products".to_string())
        );
        assert!(matches!(
            &spec.default_graphs[0].graph_selector,
            Some(GraphSelector::Iri(ref iri)) if iri == "http://example.org/vocab#products"
        ));
    }

    #[test]
    fn test_parse_from_named_with_graph_iri() {
        // Cross-ledger named graphs with collision disambiguation (handoff spec example)
        // New object format: keys are aliases, @graph for graph selector
        let query = json!({
            "fromNamed": {
                "salesProducts": {
                    "@id": "sales:main",
                    "@graph": "http://example.org/vocab#products"
                },
                "inventoryProducts": {
                    "@id": "inventory:main",
                    "@graph": "http://example.org/vocab#products"
                }
            },
            "select": ["?g", "?sku"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.named_graphs.len(), 2);

        let sales = spec
            .named_graphs
            .iter()
            .find(|g| g.source_alias.as_deref() == Some("salesProducts"))
            .expect("should have salesProducts alias");
        assert_eq!(sales.identifier, "sales:main");
        assert!(matches!(
            &sales.graph_selector,
            Some(GraphSelector::Iri(ref iri)) if iri == "http://example.org/vocab#products"
        ));

        let inventory = spec
            .named_graphs
            .iter()
            .find(|g| g.source_alias.as_deref() == Some("inventoryProducts"))
            .expect("should have inventoryProducts alias");
        assert_eq!(inventory.identifier, "inventory:main");
        assert!(matches!(
            &inventory.graph_selector,
            Some(GraphSelector::Iri(ref iri)) if iri == "http://example.org/vocab#products"
        ));
    }

    #[test]
    fn test_parse_from_object_with_time_in_id_and_alias() {
        // Time travel in @id string plus alias field
        let query = json!({
            "from": {"@id": "ledger:main@t:5", "alias": "oldData"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(5))
        ));
        assert_eq!(
            spec.default_graphs[0].source_alias,
            Some("oldData".to_string())
        );
    }

    #[test]
    fn test_parse_from_with_policy_override() {
        let query = json!({
            "from": {
                "@id": "ledger:main",
                "alias": "restricted",
                "policy": {
                    "identity": "did:example:user1",
                    "policy-class": ["ReadOnly"],
                    "default-allow": false
                }
            },
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);

        let policy = spec.default_graphs[0].policy_override.as_ref().unwrap();
        assert_eq!(policy.identity, Some("did:example:user1".to_string()));
        assert_eq!(policy.policy_class, Some(vec!["ReadOnly".to_string()]));
        assert_eq!(policy.default_allow, Some(false));
    }

    // Error cases for named graph features

    #[test]
    fn test_duplicate_alias_error() {
        let query = json!({
            "from": [
                {"@id": "ledger1:main", "alias": "mydb"},
                {"@id": "ledger2:main", "alias": "mydb"}
            ],
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "Duplicate aliases should error");
        let err = result.unwrap_err();
        assert!(matches!(err, DatasetParseError::DuplicateAlias(ref a) if a == "mydb"));
    }

    #[test]
    fn test_duplicate_alias_across_from_and_from_named_error() {
        let query = json!({
            "from": {"@id": "ledger1:main", "alias": "shared"},
            "fromNamed": {
                "shared": { "@id": "ledger2:main" }
            },
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "Duplicate aliases across from/fromNamed should error"
        );
        let err = result.unwrap_err();
        assert!(matches!(err, DatasetParseError::DuplicateAlias(ref a) if a == "shared"));
    }

    #[test]
    fn test_alias_collides_with_identifier_error() {
        // Alias "ledger1:main" collides with the identifier of another source
        let query = json!({
            "from": "ledger1:main",
            "fromNamed": {
                "ledger1:main": { "@id": "ledger2:main" }
            },
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "Alias matching another source's identifier should error"
        );
        let err = result.unwrap_err();
        assert!(matches!(err, DatasetParseError::DuplicateAlias(ref a) if a == "ledger1:main"));
    }

    #[test]
    fn test_ambiguous_graph_selector_error() {
        // Both #txn-meta fragment AND graph field = error
        let query = json!({
            "from": {"@id": "ledger:main#txn-meta", "graph": "txn-meta"},
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "Both fragment and graph field should error"
        );
        let err = result.unwrap_err();
        assert!(matches!(err, DatasetParseError::AmbiguousGraphSelector(_)));
    }

    #[test]
    fn test_ambiguous_graph_selector_error_with_different_graph() {
        // #txn-meta in id but graph field points to different graph
        let query = json!({
            "from": {"@id": "ledger:main#txn-meta", "graph": "default"},
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "Fragment and different graph field should error"
        );
        assert!(matches!(
            result.unwrap_err(),
            DatasetParseError::AmbiguousGraphSelector(_)
        ));
    }

    #[test]
    fn test_invalid_alias_type_error() {
        let query = json!({
            "from": {"@id": "ledger:main", "alias": 123},
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "Non-string alias should error");
    }

    #[test]
    fn test_invalid_graph_type_error() {
        let query = json!({
            "from": {"@id": "ledger:main", "graph": ["array"]},
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "Non-string graph should error");
    }

    // Backward compatibility tests

    #[test]
    fn test_backward_compat_txn_meta_fragment() {
        // Old style #txn-meta fragment should still work
        let query = json!({
            "from": "ledger:main#txn-meta",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main#txn-meta");
        // No graph_selector since it's in the identifier
        assert!(spec.default_graphs[0].graph_selector.is_none());
    }

    #[test]
    fn test_backward_compat_object_with_time() {
        // Old style object with just @id and t
        let query = json!({
            "from": {"@id": "ledger:main", "t": 42},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
        // New fields are None
        assert!(spec.default_graphs[0].source_alias.is_none());
        assert!(spec.default_graphs[0].graph_selector.is_none());
        assert!(spec.default_graphs[0].policy_override.is_none());
    }

    // =============================================================================
    // sparql_dataset_ledger_ids tests
    // =============================================================================

    #[test]
    fn test_sparql_dataset_ledger_ids_single_from() {
        let sparql = "SELECT ?s FROM <ledger:main> WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert_eq!(ledger_ids, vec!["ledger:main"]);
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_multiple_from() {
        let sparql = "SELECT ?s FROM <ledger:one> FROM <ledger:two> WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert_eq!(ledger_ids, vec!["ledger:one", "ledger:two"]);
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_from_named() {
        let sparql = "SELECT ?s FROM <ledger:main> FROM NAMED <ledger:named1> WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert_eq!(ledger_ids, vec!["ledger:main", "ledger:named1"]);
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_deduplicates() {
        let sparql = "SELECT ?s FROM <ledger:main> FROM NAMED <ledger:main> WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert_eq!(ledger_ids, vec!["ledger:main"]);
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_strips_time_travel() {
        let sparql = "SELECT ?s FROM <ledger:main@t:42> WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert_eq!(ledger_ids, vec!["ledger:main"]);
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_strips_fragment() {
        let sparql = "SELECT ?s FROM <ledger:main#txn-meta> WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert_eq!(ledger_ids, vec!["ledger:main"]);
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_no_from() {
        let sparql = "SELECT ?s WHERE { ?s ?p ?o }";
        let ledger_ids = sparql_dataset_ledger_ids(sparql).unwrap();
        assert!(ledger_ids.is_empty());
    }

    #[test]
    fn test_sparql_dataset_ledger_ids_parse_error() {
        let result = sparql_dataset_ledger_ids("NOT VALID SPARQL }{}{");
        assert!(result.is_err());
    }
}
