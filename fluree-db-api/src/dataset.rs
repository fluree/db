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

use fluree_db_core::ledger_id::{
    parse_time_travel_spec, LedgerIdParseError, LedgerIdTimeSpec, COMMIT_PREFIX_MIN_LEN,
    TIME_TRAVEL_TAGS,
};
use fluree_db_core::VerifiedIdentity;
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
/// A subset of `GovernanceOptions` that can be applied per-source.
/// When present on a `GraphSource`, this policy takes precedence over any
/// global policy specified in `GovernanceOptions`.
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
    ///
    /// An explicit default or empty class selection must be applied even
    /// without other policy inputs.
    pub fn has_policy(&self) -> bool {
        self.identity.is_some()
            || self.policy_class.is_some()
            || self.policy.is_some()
            || self.policy_values.is_some()
            || self.default_allow.is_some()
    }

    /// Convert to `GovernanceOptions` for policy wrapping.
    ///
    /// This creates a minimal `GovernanceOptions` with only the policy
    /// fields from this override, suitable for passing to `wrap_policy()`.
    pub fn to_query_connection_options(&self) -> GovernanceOptions {
        GovernanceOptions {
            identity: self.identity.clone(),
            policy_class: self.policy_class.clone(),
            policy: self.policy.clone(),
            policy_values: self.policy_values.clone(),
            // A per-source override comes from the request body and can never
            // carry a verified identity of its own. The caller stamps this from
            // the request-level `GovernanceOptions` before wrapping policy.
            server_identity: None,
            // Already tri-state here; it used to collapse to `false` at this
            // boundary, which is the same lost-unset bug one scope down.
            // Carrying it through is only half the fix — the explicit value
            // then has to survive `merge_policy_opts`, which it does because
            // config fills unset rather than overwriting.
            default_allow: self.default_allow,
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
/// - config graph (g_id=2): ledger governance/config
/// - User-defined named graphs: arbitrary IRIs mapped to g_id via registry
///
/// `TxnMeta` and `Config` name RESERVED graphs. Selecting one is an explicit,
/// ledger-qualified act — the selector only exists because a caller wrote it —
/// so it is permitted here; what stays closed is implicit reachability
/// (`GRAPH ?g` enumeration, an unnamed `GRAPH <iri>`). See the reserved-graph
/// contract table on `Fluree::resolve_within_ledger_graph` in `view/query.rs`.
#[derive(Debug, Clone, PartialEq)]
pub enum GraphSelector {
    /// The ledger's default graph (g_id=0)
    Default,
    /// The built-in transaction metadata graph (g_id=1)
    TxnMeta,
    /// The ledger's config graph (g_id=2)
    Config,
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

    /// Create a selector for the config graph
    pub fn config() -> Self {
        Self::Config
    }

    /// Create a selector for a named graph by IRI
    pub fn iri(iri: impl Into<String>) -> Self {
        Self::Iri(iri.into())
    }

    /// Parse from string value (as used in JSON "graph" field)
    ///
    /// - `"default"` → Default
    /// - `"txn-meta"` → TxnMeta
    /// - `"config"` → Config
    /// - anything else → Iri(value)
    ///
    /// `"config"` is a well-known name like `"txn-meta"`, not a graph IRI:
    /// without this arm it fell through to `Iri("config")`, an exact-IRI
    /// lookup for the bare word that can never match the registered
    /// `urn:fluree:<ledger>#config`.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "default" => Self::Default,
            "txn-meta" => Self::TxnMeta,
            "config" => Self::Config,
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
    /// At a specific ISO 8601 timestamp, resolved against commit *event
    /// time* (`db:time` — user-suppliable for backdated historical loads)
    AtTime(String),
    /// At a specific ISO 8601 timestamp, resolved against the wall-clock
    /// time commits were *recorded* (`db:receivedAt`, audit axis).
    /// Identical to `AtTime` on ledgers that never used caller-supplied
    /// event times.
    AtRecorded(String),
    /// At a table format's own snapshot id (`@snapshot:<id>`). Resolvable only
    /// by a graph source backed by a snapshotted table; native ledgers reject it.
    AtSnapshot(i64),
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

    /// Create at-recorded specification (audit axis)
    pub fn at_recorded(time: impl Into<String>) -> Self {
        Self::AtRecorded(time.into())
    }

    /// Create latest specification
    pub fn latest() -> Self {
        Self::Latest
    }

    /// Parse the canonical time-travel grammar: a ledger address's `@` suffix
    /// (`mydb:main@t:5`) with the `@` removed.
    ///
    /// | spelling | meaning |
    /// |---|---|
    /// | `t:<N>` | transaction number `N` |
    /// | `t:latest` | the ledger's current head |
    /// | `time:<timestamp>` (alias `iso:`) | commit *event* time (`db:time`) |
    /// | `recorded:<timestamp>` | wall-clock time the commit was recorded (`db:receivedAt`) |
    /// | `commit:<prefix>` | commit hex-digest prefix, at least 6 characters |
    ///
    /// This is an address grammar, so it is deliberately strict — widening it
    /// here widens what `ledger@<spec>` accepts on every query surface. User
    /// typed `--at` / `at=` arguments go through [`TimeSpec::parse_at`], which
    /// layers the CLI's older bare spellings on top without touching this one.
    pub fn parse(spec: &str) -> Result<Self, LedgerIdParseError> {
        Self::parse_with_sigil(spec, "")
    }

    /// [`TimeSpec::parse`] for the suffix of a ledger address — identical
    /// grammar, but errors quote the tag with the `@` the user actually typed
    /// (`Missing value after '@t:'`, not `'t:'`).
    pub fn parse_address_suffix(spec: &str) -> Result<Self, LedgerIdParseError> {
        Self::parse_with_sigil(spec, "@")
    }

    fn parse_with_sigil(spec: &str, sigil: &str) -> Result<Self, LedgerIdParseError> {
        // `LedgerIdTimeSpec` has no `Latest`: resolving it needs the ledger's
        // current `t`, which the core parser has no access to. Taking it here
        // is what makes `@t:latest` work, and is the case `dataset.rs` used to
        // special-case in its own copy of this logic.
        if spec == "t:latest" {
            return Ok(TimeSpec::Latest);
        }
        parse_time_travel_spec(spec, sigil).map(TimeSpec::from)
    }

    /// Parse an `--at` / `at=` argument: [`TimeSpec::parse`]'s grammar plus the
    /// three bare spellings the CLI and `POST /export` accepted before the
    /// canonical tags were wired up.
    ///
    /// Beyond the tagged forms it accepts, in this order:
    ///
    /// - `latest` — the untagged spelling of `t:latest`, which `fluree history
    ///   --to` has always taken.
    /// - a bare integer → `AtT`
    /// - a string containing `-` → `AtTime` (an ISO-8601 timestamp or date; a
    ///   hex digest never contains one, so a date-only value fails as a bad
    ///   timestamp rather than as an unknown commit)
    /// - anything else → `AtCommit` (a bare hex-digest prefix)
    ///
    /// **A bare integer is a `t`, not a commit prefix.** `123456` is both a
    /// valid `t` and a valid 6-character hex prefix; this grammar resolves the
    /// ambiguity in favour of `t` because that is what `--at` has always done.
    /// `commit:123456` forces the prefix reading and `t:123456` forces the
    /// other — which is the point of having the tags.
    ///
    /// A spec that *starts with* a canonical tag is never reinterpreted as one
    /// of the bare forms: `--at t:abc` is a malformed `t:`, not a commit prefix
    /// named `t:abc`. Silently falling through was the whole of #1805.
    pub fn parse_at(spec: &str) -> Result<Self, LedgerIdParseError> {
        if spec == "latest" {
            return Ok(TimeSpec::Latest);
        }
        if TIME_TRAVEL_TAGS.iter().any(|tag| spec.starts_with(tag)) {
            return Self::parse(spec).map_err(|e| {
                LedgerIdParseError::new(format!("{e}. {ACCEPTED_TIME_SPEC_SPELLINGS}"))
            });
        }
        if spec.is_empty() {
            return Err(LedgerIdParseError::new(format!(
                "Empty time spec. {ACCEPTED_TIME_SPEC_SPELLINGS}"
            )));
        }
        if let Ok(t) = spec.parse::<i64>() {
            return Ok(TimeSpec::AtT(t));
        }
        if spec.contains('-') {
            // Looks like ISO-8601 (e.g. "2024-01-15T10:30:00Z", or a bare date).
            return Ok(TimeSpec::AtTime(spec.to_string()));
        }
        // The same floor the `commit:` arm applies, so the two spellings of a
        // too-short prefix agree: `commit:abc` was rejected by the tag arm
        // while a bare `abc` was accepted and died at the resolver with the
        // same complaint several layers down.
        //
        // This is a fast-path rejection, not the authority. `normalize_commit_ref`
        // strips `fluree:commit:` / `sha256:` and decodes canonical CIDs before
        // measuring, so `sha256:abc` is ten characters here and three there —
        // it passes this check and is correctly rejected downstream. Measuring
        // the raw string is only sound in the direction it is used: everything
        // this rejects, the resolver would also reject.
        if spec.len() < COMMIT_PREFIX_MIN_LEN {
            return Err(LedgerIdParseError::new(format!(
                "Commit prefix must be at least {COMMIT_PREFIX_MIN_LEN} characters, got {}. \
                 {ACCEPTED_TIME_SPEC_SPELLINGS}",
                spec.len()
            )));
        }
        Ok(TimeSpec::AtCommit(spec.to_string()))
    }
}

/// The spellings [`TimeSpec::parse_at`] accepts, quoted back when a user reaches
/// for a canonical tag and mis-spells it.
pub const ACCEPTED_TIME_SPEC_SPELLINGS: &str =
    "Accepted: t:<N>, t:latest, latest, time:<ISO-8601> (or its alias iso:), \
     recorded:<ISO-8601>, commit:<hex-prefix>, snapshot:<id> (graph sources only), \
     a bare transaction number, a bare ISO-8601 timestamp, or a bare commit \
     hex-digest prefix";

impl From<LedgerIdTimeSpec> for TimeSpec {
    fn from(spec: LedgerIdTimeSpec) -> Self {
        match spec {
            LedgerIdTimeSpec::AtT(t) => TimeSpec::AtT(t),
            LedgerIdTimeSpec::AtIso(value) => TimeSpec::AtTime(value),
            LedgerIdTimeSpec::AtCommit(value) => TimeSpec::AtCommit(value),
            LedgerIdTimeSpec::AtRecorded(value) => TimeSpec::AtRecorded(value),
            LedgerIdTimeSpec::AtSnapshot(id) => TimeSpec::AtSnapshot(id),
        }
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

use serde::{Deserialize, Serialize};
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
    ) -> Result<(Self, GovernanceOptions), DatasetParseError> {
        let obj = match json.as_object() {
            Some(o) => o,
            None => return Ok((Self::new(), GovernanceOptions::default())),
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

        let qc_opts = GovernanceOptions::from_json(json)?;
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
///
/// Tracking-related opts keys (`meta`, `max-fuel`) live on a separate
/// [`TrackingOptions`] path; they are parsed and propagated by the
/// transaction route, not by this type.
///
/// `Serialize` / `Deserialize` exist so the struct can ride the consensus
/// request envelope (`QueuedTransact`, `QueuedPush`) from the accepting node
/// to the commit worker. They are **not** a client-facing decoder: a server
/// route must build this struct through [`GovernanceOptions::from_json`],
/// never by deserializing a request body directly, because serde would
/// populate [`GovernanceOptions::server_identity`] from client bytes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GovernanceOptions {
    pub identity: Option<String>,
    pub policy_class: Option<Vec<String>>,
    pub policy: Option<JsonValue>,
    pub policy_values: Option<HashMap<String, JsonValue>>,
    /// Auth-layer-verified identity of the caller, used only for
    /// `f:overrideControl` (`f:IdentityRestricted`) checks.
    ///
    /// This is the DID the server established from a verified JWS credential
    /// or bearer token. It is never parsed from the query/transaction JSON or
    /// from headers ([`GovernanceOptions::from_json`] always leaves it `None`),
    /// so a caller cannot satisfy an allow-list by writing it into `opts`.
    /// When a credential lets its holder select the policy identity (a trusted
    /// gateway acting for an end user), this stays the DID the credential was
    /// issued to while `identity` carries the selected one.
    ///
    /// It is distinct from `identity`, which is the policy-evaluation context
    /// and may legitimately come from the request. Server routes populate it;
    /// the CLI and embedded callers without their own auth layer leave it
    /// `None`, and identity-restricted overrides are then denied. An embedding
    /// application that verifies identities itself is the auth layer for that
    /// deployment and may set it.
    pub server_identity: Option<VerifiedIdentity>,
    /// Tri-state default-allow: `None` means the caller did not say, so the
    /// ledger's configured `f:defaultAllow` may fill it in
    /// ([`crate::config_resolver::merge_policy_opts`]); `Some(v)` is an explicit
    /// request-level override that wins over config. Resolve to a concrete bool
    /// with [`GovernanceOptions::effective_default_allow`] — absent on both
    /// sides is fail-closed (`false`).
    ///
    /// A bare `bool` here silently discarded a ledger's `f:defaultAllow true`
    /// for every identity-carrying request: `merge_policy_opts` treats any
    /// request with policy inputs as an override, and `false` was
    /// indistinguishable from "unset".
    pub default_allow: Option<bool>,
}

impl GovernanceOptions {
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

        let identity = match opts.get("identity") {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::String(value)) => Some(value.clone()),
            Some(_) => {
                return Err(DatasetParseError::InvalidOptions(
                    "'identity' must be a string".into(),
                ))
            }
        };

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

        let default_allow = match opts
            .get("default-allow")
            .or_else(|| opts.get("default_allow"))
            .or_else(|| opts.get("defaultAllow"))
        {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::Bool(value)) => Some(*value),
            Some(_) => {
                return Err(DatasetParseError::InvalidOptions(
                    "'default-allow' must be a boolean".into(),
                ))
            }
        };

        Ok(Self {
            identity,
            policy_class,
            policy,
            policy_values,
            // Deliberately not read from `opts`: only an auth layer may set it.
            server_identity: None,
            default_allow,
        })
    }

    /// Resolve the tri-state flag to the concrete bool the policy wrapper needs.
    /// Call this only after [`crate::config_resolver::merge_policy_opts`] has had
    /// a chance to fill `None` from the ledger's `f:defaultAllow`; still-unset
    /// means nobody configured it, which is fail-closed.
    pub fn effective_default_allow(&self) -> bool {
        self.default_allow.unwrap_or(false)
    }

    /// An explicitly empty rule selection with a deny default cannot grant
    /// access. Preserve this narrowing even when config prohibits replacements.
    /// Unlike a deny default alone, this also excludes configured class rules.
    pub(crate) fn denies_all(&self) -> bool {
        self.default_allow == Some(false)
            && self.policy_class.as_ref().is_some_and(Vec::is_empty)
            && self
                .policy
                .as_ref()
                .is_none_or(|p| p.is_null() || p.as_array().is_some_and(Vec::is_empty))
    }

    /// Whether the request selects or changes the policy set, subject to
    /// configured override controls. An empty class list selects no stored rules;
    /// an allow default can widen access. A deny default alone only narrows the
    /// configured set and does not count as a replacement selection.
    ///
    /// `server_identity` deliberately does not count: it authorizes config
    /// overrides, it does not select a policy set. On the server a verified
    /// identity always arrives alongside a forced `identity`, which is what
    /// engages enforcement.
    pub fn selects_policy_set(&self) -> bool {
        self.identity.is_some()
            || self.policy_class.is_some()
            || self.policy.as_ref().is_some_and(|p| !p.is_null())
            || self.policy_values.as_ref().is_some_and(|m| !m.is_empty())
            || self.default_allow == Some(true)
    }

    /// Whether the request engages policy enforcement. Unlike
    /// [`Self::selects_policy_set`], a deny default alone counts: it must not take
    /// the unrestricted shortcut, even though it retains configured policies.
    pub fn has_any_policy_inputs(&self) -> bool {
        self.selects_policy_set() || self.default_allow == Some(false)
    }
}

/// Parse time-travel specification from ledger ID string.
///
/// Supports compatible formats:
/// - `ledger:main@t:42` → identifier="ledger:main", TimeSpec::AtT(42)
/// - `ledger:main@t:latest` → identifier="ledger:main", TimeSpec::Latest
/// - `ledger:main@time:2025-01-01T00:00:00Z` → identifier="ledger:main", TimeSpec::AtTime(...)
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

    // The suffix grammar itself lives in `TimeSpec::parse`, shared with the
    // CLI's `--at` and the server's `at=` (#1805). All this layer does is find
    // the `@` and re-attach the fragment.
    let (identifier, time_spec) = match before_fragment.split_once('@') {
        Some((base, spec)) => {
            if base.is_empty() {
                return Err(DatasetParseError::InvalidGraphSource(
                    "Ledger ID cannot be empty before '@'".to_string(),
                ));
            }
            let spec = TimeSpec::parse_address_suffix(spec)
                .map_err(|e| DatasetParseError::InvalidGraphSource(e.to_string()))?;
            (base, Some(spec))
        }
        None => (before_fragment, None),
    };

    Ok((format!("{identifier}{fragment_suffix}"), time_spec))
}

/// Parse graph sources from a JSON value
///
/// Accepts:
/// - String: single graph source (may include @t:/@time:/@commit: time-travel syntax)
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
/// The graph selector is optional ("default", "txn-meta", or a graph IRI) and may
/// be spelled `@graph` or `graph` — the `from` single-source form reads the same
/// two spellings, so neither form silently ignores the other's.
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
                source.time_spec = Some(parse_object_at(at_str)?);
            }
        }

        // Parse graph selector (`@graph` or `graph`)
        source.graph_selector = parse_graph_selector_field(entry, &identifier, raw_identifier)?;

        // Parse policy override
        if let Some(policy_val) = entry.get("policy") {
            source.policy_override = Some(parse_source_policy_override(policy_val)?);
        }

        sources.push(source);
    }
    Ok(sources)
}

/// The object form's `"at"` key takes the same grammar as the CLI's `--at` and
/// the server's `at=`: every tagged spelling plus a bare timestamp, transaction
/// number, or commit prefix. It used to have its own three-way split that read
/// anything untagged as a timestamp, so `"at": "time:..."` or `"at": "t:5"`
/// became an invalid timestamp downstream.
fn parse_object_at(at_str: &str) -> Result<TimeSpec, DatasetParseError> {
    TimeSpec::parse_at(at_str).map_err(|e| DatasetParseError::InvalidGraphSource(e.to_string()))
}

/// Read a source object's graph selector, accepting either spelling.
///
/// The `fromNamed` object form historically read only `@graph` while the
/// `from` single-source form read only `graph`. Writing the other form's
/// spelling was not an error — the key was silently ignored and the source
/// resolved to the whole ledger, so the query returned a plausible wrong
/// answer with a 200. Both forms now accept both spellings.
fn parse_graph_selector_field(
    obj: &serde_json::Map<String, JsonValue>,
    identifier: &str,
    raw_identifier: &str,
) -> Result<Option<GraphSelector>, DatasetParseError> {
    let (key, graph_val) = match obj.get("@graph") {
        Some(v) => ("@graph", v),
        None => match obj.get("graph") {
            Some(v) => ("graph", v),
            None => return Ok(None),
        },
    };

    // Ambiguity: the identifier already selected a reserved graph by fragment.
    // Both reserved fragments are checked, not just `#txn-meta`: `#config` is
    // addressable the same way, so `{"@id": "L#config", "graph": …}` is the
    // same contradiction and must be refused the same way.
    if identifier.contains("#txn-meta") || identifier.contains("#config") {
        return Err(DatasetParseError::AmbiguousGraphSelector(
            raw_identifier.to_string(),
        ));
    }

    let graph_str = graph_val.as_str().ok_or_else(|| {
        DatasetParseError::InvalidGraphSource(format!(
            "'{key}' must be a string ('default', 'txn-meta', 'config', or a graph IRI)"
        ))
    })?;
    Ok(Some(GraphSelector::from_str(graph_str)))
}

/// Parse a single graph source from a JSON value
///
/// Accepts:
/// - String: identifier (may include @t:/@time:/@commit: time-travel syntax and #txn-meta fragment)
/// - Object: Extended graph source object with optional fields:
///   - `@id` / `id`: ledger reference (required)
///   - `t` / `at`: time specification
///   - `alias`: dataset-local alias (optional)
///   - `graph` / `@graph`: graph selector - "default", "txn-meta", or IRI string (optional)
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
                    source.time_spec = Some(parse_object_at(at_str)?);
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

            // Parse graph selector (`graph` or `@graph`)
            source.graph_selector = parse_graph_selector_field(obj, &identifier, raw_identifier)?;

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
mod time_spec_grammar_tests {
    //! The `--at` / `at=` / `@`-suffix grammar (#1805).
    //!
    //! Before this, three surfaces hand-rolled a heuristic that recognised a
    //! bare integer and a bare ISO-8601 timestamp and swept *everything else*
    //! into `AtCommit` — so `t:2` reached the commit-prefix resolver as the
    //! literal string `"t:2"` and failed with "Commit prefix must be at least
    //! 6 characters, got 3". Every tagged spelling below is a case that used
    //! to do that.

    use super::*;

    /// Every canonical tag, on the strict grammar an address uses.
    #[test]
    fn parse_accepts_every_canonical_tag() {
        assert_eq!(TimeSpec::parse("t:2").unwrap(), TimeSpec::AtT(2));
        assert_eq!(TimeSpec::parse("t:0").unwrap(), TimeSpec::AtT(0));
        assert_eq!(TimeSpec::parse("t:latest").unwrap(), TimeSpec::Latest);
        assert_eq!(
            TimeSpec::parse("iso:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse("recorded:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtRecorded("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse("commit:abc123def").unwrap(),
            TimeSpec::AtCommit("abc123def".to_string())
        );
    }

    /// The address grammar must stay strict: it is what `ledger@<spec>` accepts
    /// on every query surface, so the CLI's bare compatibility spellings must
    /// not leak into it.
    #[test]
    fn parse_rejects_the_bare_cli_spellings() {
        for bare in ["2", "latest", "2024-01-15T10:30:00Z", "abc123def"] {
            assert!(
                TimeSpec::parse(bare).is_err(),
                "address grammar must reject the bare spelling {bare:?}"
            );
        }
    }

    #[test]
    fn parse_rejects_malformed_tagged_specs() {
        for bad in [
            "t:",
            "t:abc",
            "time:",
            "iso:",
            "commit:",
            "commit:abc",
            "recorded:",
            "snapshot:",
            "snapshot:abc",
        ] {
            assert!(
                TimeSpec::parse(bad).is_err(),
                "expected {bad:?} to be an error"
            );
        }
    }

    /// Error text quotes the tag as the calling surface spells it: a bare spec
    /// reports `'t:'`, an address suffix reports `'@t:'`. Routing the address
    /// path through the bare entry point silently rewrote three published
    /// messages — caught by
    /// `it_query_time_travel::time_travel_missing_value_errors`, and pinned
    /// here at the unit level so the next such slip fails faster.
    #[test]
    fn error_text_quotes_the_tag_as_the_calling_surface_spells_it() {
        for tag in ["t:", "time:", "iso:", "commit:", "recorded:", "snapshot:"] {
            assert_eq!(
                TimeSpec::parse(tag).unwrap_err().to_string(),
                format!("Missing value after '{tag}'")
            );
            assert_eq!(
                TimeSpec::parse_address_suffix(tag).unwrap_err().to_string(),
                format!("Missing value after '@{tag}'")
            );
        }

        let bare = TimeSpec::parse("nope").unwrap_err().to_string();
        assert!(
            bare.contains("Expected t:, time:, recorded:, commit:, or snapshot:"),
            "got: {bare}"
        );
        assert!(
            !bare.contains('@'),
            "bare-spec error must not mention '@': {bare}"
        );

        let addressed = TimeSpec::parse_address_suffix("nope")
            .unwrap_err()
            .to_string();
        assert!(
            addressed.contains("Expected @t:, @time:, @recorded:, @commit:, or @snapshot:"),
            "got: {addressed}"
        );
    }

    /// The `--at` grammar is the canonical one plus three bare spellings.
    #[test]
    fn parse_at_accepts_canonical_and_legacy_spellings() {
        // Canonical — every one of these was broken before #1805.
        assert_eq!(TimeSpec::parse_at("t:2").unwrap(), TimeSpec::AtT(2));
        assert_eq!(TimeSpec::parse_at("t:latest").unwrap(), TimeSpec::Latest);
        assert_eq!(
            TimeSpec::parse_at("time:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("iso:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("recorded:2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtRecorded("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("commit:abc123def").unwrap(),
            TimeSpec::AtCommit("abc123def".to_string())
        );

        // Legacy bare spellings, which must keep working.
        assert_eq!(TimeSpec::parse_at("2").unwrap(), TimeSpec::AtT(2));
        assert_eq!(TimeSpec::parse_at("latest").unwrap(), TimeSpec::Latest);
        assert_eq!(
            TimeSpec::parse_at("2024-01-15T10:30:00Z").unwrap(),
            TimeSpec::AtTime("2024-01-15T10:30:00Z".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("abc123def").unwrap(),
            TimeSpec::AtCommit("abc123def".to_string())
        );
    }

    /// `t:2` and `2` must denote the same instant — the equivalence the CLI's
    /// integration tests assert row-for-row.
    #[test]
    fn parse_at_tagged_and_bare_integers_agree() {
        for t in [0_i64, 1, 42, 123_456] {
            assert_eq!(
                TimeSpec::parse_at(&t.to_string()).unwrap(),
                TimeSpec::parse_at(&format!("t:{t}")).unwrap(),
                "bare and tagged spellings of t={t} must agree"
            );
        }
    }

    /// A string that *starts with* a canonical tag is never reinterpreted as a
    /// bare form. This is the actual bug: the old heuristic's catch-all turned
    /// `t:abc` into `AtCommit("t:abc")` and let it fail downstream, three
    /// layers from the typo.
    #[test]
    fn parse_at_never_falls_back_to_a_prefix_for_a_tagged_spec() {
        for bad in ["t:abc", "iso:", "commit:abc", "recorded:", "t:"] {
            let err = TimeSpec::parse_at(bad).unwrap_err().to_string();
            assert!(
                err.contains("Accepted: t:<N>"),
                "{bad:?} must report the accepted spellings, got: {err}"
            );
        }
        assert!(TimeSpec::parse_at("").is_err());
    }

    /// One constant, not three. The floor was a bare literal in the core
    /// grammar (with the `6` written into its message too), a `pub const` on
    /// the resolver, and — briefly, while closing the asymmetry below — a
    /// third private copy here. This pins that every surface now moves
    /// together: change `COMMIT_PREFIX_MIN_LEN` and all of them follow,
    /// including the error text.
    #[test]
    fn every_surface_shares_one_commit_prefix_floor() {
        let at_floor = "a".repeat(COMMIT_PREFIX_MIN_LEN);
        let below = "a".repeat(COMMIT_PREFIX_MIN_LEN - 1);

        // Exactly at the floor: accepted in both spellings, on both grammars.
        assert!(TimeSpec::parse_at(&at_floor).is_ok());
        assert!(TimeSpec::parse_at(&format!("commit:{at_floor}")).is_ok());
        assert!(TimeSpec::parse_address_suffix(&format!("commit:{at_floor}")).is_ok());

        // One below: rejected by all three, and each message quotes the
        // constant rather than a literal that could drift away from it.
        let expected = format!("at least {COMMIT_PREFIX_MIN_LEN} characters");
        for err in [
            TimeSpec::parse_at(&below).unwrap_err().to_string(),
            TimeSpec::parse_at(&format!("commit:{below}"))
                .unwrap_err()
                .to_string(),
            TimeSpec::parse_address_suffix(&format!("commit:{below}"))
                .unwrap_err()
                .to_string(),
        ] {
            assert!(
                err.contains(&expected),
                "message must quote the constant: {err}"
            );
        }
    }

    /// The tagged and bare spellings must agree about *failure* too, not just
    /// success: a too-short prefix is rejected at the CLI boundary either way.
    /// Before this, `commit:abc` was rejected here while a bare `abc` was
    /// accepted and died at the resolver with the same complaint several
    /// layers down.
    #[test]
    fn parse_at_rejects_a_short_prefix_in_both_spellings() {
        for spec in ["abc", "a", "12ab", "abcde"] {
            let bare = TimeSpec::parse_at(spec).unwrap_err().to_string();
            let tagged = TimeSpec::parse_at(&format!("commit:{spec}"))
                .unwrap_err()
                .to_string();
            let expected = format!("at least {COMMIT_PREFIX_MIN_LEN} characters");
            assert!(
                bare.contains(&expected),
                "bare {spec:?} must be rejected at the boundary, got: {bare}"
            );
            assert!(tagged.contains(&expected), "got: {tagged}");
        }
        // Exactly at the floor, both spellings still succeed and agree.
        assert_eq!(
            TimeSpec::parse_at("abc123").unwrap(),
            TimeSpec::AtCommit("abc123".to_string())
        );
        assert_eq!(
            TimeSpec::parse_at("abc123").unwrap(),
            TimeSpec::parse_at("commit:abc123").unwrap()
        );
    }

    /// An all-digit string of 6+ characters is both a valid `t` and a valid hex
    /// prefix. `--at` has always resolved that in favour of `t`; `commit:`
    /// is the documented escape hatch, and this pins both halves.
    #[test]
    fn parse_at_resolves_the_digits_ambiguity_toward_t() {
        assert_eq!(
            TimeSpec::parse_at("123456").unwrap(),
            TimeSpec::AtT(123_456)
        );
        assert_eq!(
            TimeSpec::parse_at("commit:123456").unwrap(),
            TimeSpec::AtCommit("123456".to_string())
        );
    }

    /// The address path and the `--at` path are the same grammar modulo the
    /// `@`, which is the property that made "call the shared parser" the fix.
    #[test]
    fn address_suffix_and_bare_spec_agree_on_the_canonical_grammar() {
        for spec in [
            "t:7",
            "t:latest",
            "time:2024-01-15T10:30:00Z",
            "iso:2024-01-15T10:30:00Z",
            "recorded:2024-01-15T10:30:00Z",
            "commit:abc123def",
            "snapshot:42",
        ] {
            let (identifier, from_address) =
                parse_ledger_id_time_travel(&format!("mydb:main@{spec}")).unwrap();
            assert_eq!(identifier, "mydb:main");
            assert_eq!(
                from_address.unwrap(),
                TimeSpec::parse(spec).unwrap(),
                "address suffix @{spec} must mean the same as the bare spec {spec}"
            );
            assert_eq!(
                TimeSpec::parse_address_suffix(spec).unwrap(),
                TimeSpec::parse(spec).unwrap(),
                "the two entry points differ only in error text"
            );
        }
    }

    /// `@t:latest` used to be special-cased above the parser in this file; it
    /// now lives in `TimeSpec::parse`, including with a fragment selector.
    #[test]
    fn address_latest_still_parses_with_and_without_a_fragment() {
        let (id, spec) = parse_ledger_id_time_travel("mydb:main@t:latest").unwrap();
        assert_eq!((id.as_str(), spec), ("mydb:main", Some(TimeSpec::Latest)));

        let (id, spec) = parse_ledger_id_time_travel("mydb:main@t:latest#txn-meta").unwrap();
        assert_eq!(
            (id.as_str(), spec),
            ("mydb:main#txn-meta", Some(TimeSpec::Latest))
        );

        assert!(parse_ledger_id_time_travel("@t:latest").is_err());
    }

    #[test]
    fn from_ledger_id_time_spec_maps_every_variant() {
        use fluree_db_core::ledger_id::LedgerIdTimeSpec;
        assert_eq!(TimeSpec::from(LedgerIdTimeSpec::AtT(3)), TimeSpec::AtT(3));
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtIso("x".into())),
            TimeSpec::AtTime("x".into())
        );
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtRecorded("x".into())),
            TimeSpec::AtRecorded("x".into())
        );
        assert_eq!(
            TimeSpec::from(LedgerIdTimeSpec::AtCommit("abc123".into())),
            TimeSpec::AtCommit("abc123".into())
        );
    }
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

    /// Naming a reserved graph twice — once by fragment, once by selector — is
    /// a contradiction, for `#config` exactly as for `#txn-meta`.
    ///
    /// The ambiguity check read only `#txn-meta`, so
    /// `{"@id": "L#config", "graph": …}` silently took one of the two and ran.
    /// Both fragments address a reserved graph, so both make an explicit
    /// selector redundant-or-contradictory in the same way.
    #[test]
    fn both_reserved_fragments_conflict_with_an_explicit_graph_selector() {
        for frag in ["txn-meta", "config"] {
            let query = json!({
                "from": {"@id": format!("ledger:main#{frag}"), "graph": "default"},
                "select": ["?s"],
                "where": {"@id": "?s"}
            });
            assert!(
                DatasetSpec::from_json(&query).is_err(),
                "`#{frag}` plus an explicit graph selector must be refused as ambiguous"
            );
        }
    }

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

    /// The object form's `at` takes the shared `--at` grammar: tagged spellings
    /// (including the `time:` / `iso:` pair) and the bare forms, in both the
    /// `from` object and the `fromNamed` entry shapes.
    #[test]
    fn object_form_at_takes_the_shared_grammar() {
        let cases = [
            (
                "time:2024-01-15T10:30:00Z",
                TimeSpec::AtTime("2024-01-15T10:30:00Z".into()),
            ),
            (
                "iso:2024-01-15T10:30:00Z",
                TimeSpec::AtTime("2024-01-15T10:30:00Z".into()),
            ),
            (
                "2024-01-15T10:30:00Z",
                TimeSpec::AtTime("2024-01-15T10:30:00Z".into()),
            ),
            (
                "recorded:2024-01-15T10:30:00Z",
                TimeSpec::AtRecorded("2024-01-15T10:30:00Z".into()),
            ),
            ("t:7", TimeSpec::AtT(7)),
            ("7", TimeSpec::AtT(7)),
            ("latest", TimeSpec::Latest),
            ("commit:abc123", TimeSpec::AtCommit("abc123".into())),
            ("snapshot:42", TimeSpec::AtSnapshot(42)),
            // A date-only value is a (later-rejected) timestamp, not a commit
            // prefix: the error then talks about the timestamp the user wrote.
            ("2024-01-15", TimeSpec::AtTime("2024-01-15".into())),
        ];
        for (at, expected) in &cases {
            let query = json!({
                "from": {"@id": "ledger:main", "at": at},
                "fromNamed": [{"@id": "other:main", "at": at, "alias": "o"}],
                "select": ["?s"],
                "where": {"@id": "?s"}
            });
            let spec = DatasetSpec::from_json(&query).unwrap();
            assert_eq!(
                spec.default_graphs[0].time_spec.as_ref(),
                Some(expected),
                "from at={at}"
            );
            assert_eq!(
                spec.named_graphs[0].time_spec.as_ref(),
                Some(expected),
                "fromNamed at={at}"
            );
        }
        // A malformed tag is a parse error, not a timestamp that fails later.
        let query = json!({
            "from": {"@id": "ledger:main", "at": "t:abc"},
            "select": ["?s"],
            "where": {"@id": "?s"}
        });
        let err = DatasetSpec::from_json(&query).unwrap_err().to_string();
        assert!(err.contains("Invalid integer for t:"), "{err}");
    }

    // Ledger ID time-travel syntax tests (@t:, @time:, @commit:)

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

    // --- GovernanceOptions::default_allow tri-state parsing ---

    fn parse_default_allow(query: &JsonValue) -> Option<bool> {
        GovernanceOptions::from_json(query).unwrap().default_allow
    }

    /// An absent key must stay `None` so the ledger's `f:defaultAllow` can fill
    /// it — collapsing it to `false` here is what silently discarded config.
    #[test]
    fn default_allow_absent_parses_as_unset() {
        assert_eq!(parse_default_allow(&serde_json::json!({})), None);
        assert_eq!(
            parse_default_allow(&serde_json::json!({"opts": {}})),
            None,
            "an opts object with no default-allow key"
        );
        assert_eq!(
            parse_default_allow(&serde_json::json!({"opts": {"identity": "did:key:alice"}})),
            None,
            "carrying an identity is not a statement about default-allow"
        );
    }

    #[test]
    fn default_allow_explicit_values_parse_as_set() {
        for key in ["default-allow", "default_allow", "defaultAllow"] {
            assert_eq!(
                parse_default_allow(&serde_json::json!({"opts": {key: false}})),
                Some(false),
                "{key} = false"
            );
            assert_eq!(
                parse_default_allow(&serde_json::json!({"opts": {key: true}})),
                Some(true),
                "{key} = true"
            );
        }
    }

    /// Malformed values fail rather than silently falling back to config.
    #[test]
    fn malformed_default_allow_is_rejected_and_null_stays_unset() {
        assert!(GovernanceOptions::from_json(
            &serde_json::json!({"opts": {"default-allow": "true"}})
        )
        .is_err());
        assert_eq!(
            parse_default_allow(&serde_json::json!({"opts": {"default-allow": null}})),
            None
        );
    }

    #[test]
    fn effective_default_allow_is_fail_closed_when_unset() {
        assert!(!GovernanceOptions::default().effective_default_allow());
        assert!(!GovernanceOptions {
            default_allow: Some(false),
            ..Default::default()
        }
        .effective_default_allow());
        assert!(GovernanceOptions {
            default_allow: Some(true),
            ..Default::default()
        }
        .effective_default_allow());
    }

    /// `server_identity` is the value `f:overrideControl` gates on. It must
    /// only ever come from an auth layer, so no spelling of it in the request
    /// body may populate it — otherwise a caller could satisfy an
    /// `f:IdentityRestricted` allow-list by writing the DID into `opts`.
    #[test]
    fn from_json_never_populates_server_identity() {
        for key in ["server_identity", "serverIdentity", "server-identity"] {
            let query = json!({
                "select": ["?s"],
                "opts": {"identity": "did:key:caller", key: "did:key:admin"}
            });
            let opts = GovernanceOptions::from_json(&query).expect("parses");
            assert_eq!(opts.identity.as_deref(), Some("did:key:caller"));
            assert_eq!(
                opts.server_identity, None,
                "opts.{key} must not populate server_identity"
            );
        }
    }

    /// A verified identity authorizes config overrides; it is not itself a
    /// request for policy enforcement, so it must not force a policy wrap.
    #[test]
    fn server_identity_alone_is_not_a_policy_input() {
        assert!(!GovernanceOptions {
            server_identity: Some(VerifiedIdentity::new("did:key:admin")),
            ..Default::default()
        }
        .has_any_policy_inputs());
    }

    // ---------------------------------------------------------------------
    // Graph-selector spelling. `fromNamed` entries once read only `@graph`
    // and `from` source objects only `graph`; the other spelling was silently
    // ignored and the source resolved to the whole ledger, so the query
    // returned a wrong answer with no error. Both forms take both spellings.
    // ---------------------------------------------------------------------

    fn named_selector(query: &JsonValue, alias: &str) -> Option<GraphSelector> {
        let (spec, _) = DatasetSpec::from_query_json(query).expect("parses");
        spec.named_graphs
            .iter()
            .find(|s| s.source_alias.as_deref() == Some(alias))
            .expect("named source present")
            .graph_selector
            .clone()
    }

    fn default_selector(query: &JsonValue) -> Option<GraphSelector> {
        let (spec, _) = DatasetSpec::from_query_json(query).expect("parses");
        spec.default_graphs[0].graph_selector.clone()
    }

    #[test]
    fn from_named_entry_accepts_either_graph_spelling() {
        let with_at = json!({
            "fromNamed": {"g": {"@id": "db:main", "@graph": "http://ex.org/g1"}},
            "select": ["?s"]
        });
        let without_at = json!({
            "fromNamed": {"g": {"@id": "db:main", "graph": "http://ex.org/g1"}},
            "select": ["?s"]
        });

        assert!(matches!(
            named_selector(&with_at, "g"),
            Some(GraphSelector::Iri(ref s)) if s == "http://ex.org/g1"
        ));
        // Previously `None` — silently the whole ledger.
        assert!(matches!(
            named_selector(&without_at, "g"),
            Some(GraphSelector::Iri(ref s)) if s == "http://ex.org/g1"
        ));
        assert_eq!(
            named_selector(&with_at, "g"),
            named_selector(&without_at, "g")
        );
    }

    #[test]
    fn from_source_object_accepts_either_graph_spelling() {
        let with_bare = json!({
            "from": {"@id": "db:main", "graph": "http://ex.org/g1"},
            "select": ["?s"]
        });
        let with_at = json!({
            "from": {"@id": "db:main", "@graph": "http://ex.org/g1"},
            "select": ["?s"]
        });

        assert!(matches!(
            default_selector(&with_bare),
            Some(GraphSelector::Iri(ref s)) if s == "http://ex.org/g1"
        ));
        // Previously `None` — silently the whole ledger.
        assert!(matches!(
            default_selector(&with_at),
            Some(GraphSelector::Iri(ref s)) if s == "http://ex.org/g1"
        ));
        assert_eq!(default_selector(&with_bare), default_selector(&with_at));
    }

    #[test]
    fn graph_selector_keeps_txn_meta_ambiguity_check_for_both_spellings() {
        for key in ["graph", "@graph"] {
            let query = json!({
                "from": {"@id": "db:main#txn-meta", key: "http://ex.org/g1"},
                "select": ["?s"]
            });
            assert!(
                matches!(
                    DatasetSpec::from_query_json(&query),
                    Err(DatasetParseError::AmbiguousGraphSelector(_))
                ),
                "#txn-meta + '{key}' must stay ambiguous"
            );
        }
    }

    #[test]
    fn graph_selector_rejects_non_string_under_both_spellings() {
        for key in ["graph", "@graph"] {
            let query = json!({
                "from": {"@id": "db:main", key: 7},
                "select": ["?s"]
            });
            let err = DatasetSpec::from_query_json(&query).expect_err("must reject");
            let msg = err.to_string();
            assert!(msg.contains(key), "error should name the key used: {msg}");
        }
    }
}
