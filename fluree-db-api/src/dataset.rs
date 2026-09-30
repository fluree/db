//! Dataset types for multi-graph query execution
//!
//! This module provides the API-layer types for declaring and resolving datasets:
//!
//! - [`DatasetSpec`]: Declarative specification from query parsing (unresolved)
//! - [`GraphSource`]: One dataset member, parsed once into a typed reference
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

use fluree_db_core::{LedgerId, VerifiedIdentity};
use fluree_db_sparql::ResolvedDatasetClause;
use std::sync::Arc;

pub use fluree_db_core::{DatasetRef, GraphIri, GraphSel, LedgerRef, MemberRef};

/// Declarative dataset specification from query parsing
///
/// What the user asked for, parsed once: every member is a typed
/// [`GraphSource`], and nothing downstream re-reads its text to decide what it
/// names. Resolution against the nameservice (a connection surface) or a
/// target ledger's graph registry (a ledger-scoped surface) happens when the
/// dataset is built.
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
/// FROM NAMED <graph1:main>
/// FROM NAMED <graph2:main>
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

    /// Every member, default graphs first.
    pub fn sources(&self) -> impl Iterator<Item = &GraphSource> {
        self.default_graphs.iter().chain(&self.named_graphs)
    }

    /// The ledgers (or graph sources) this dataset names by address, each
    /// once, in first-mention order: what a connection surface authorizes and
    /// refreshes. A bare graph IRI or a graph keyword names no ledger.
    pub fn ledgers(&self) -> Vec<LedgerId> {
        let mut out: Vec<LedgerId> = Vec::new();
        let ids = self
            .sources()
            .filter_map(|s| s.address().map(LedgerRef::id))
            .chain(self.history_range.iter().map(|r| &r.ledger));
        for id in ids {
            if !out.contains(id) {
                out.push(id.clone());
            }
        }
        out
    }

    /// Create a DatasetSpec from a SPARQL dataset clause whose IRIs the
    /// prologue has already expanded (prefixes applied, BASE resolved), as
    /// [`fluree_db_sparql::resolve_dataset_clause`] returns it.
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
    pub fn from_sparql(clause: &ResolvedDatasetClause) -> Result<Self, DatasetParseError> {
        let parse = |iri: &Arc<str>| GraphSource::parse(iri);
        let default_graphs = clause
            .default_graphs
            .iter()
            .map(parse)
            .collect::<Result<Vec<_>, _>>()?;
        let named_graphs = clause
            .named_graphs
            .iter()
            .map(parse)
            .collect::<Result<Vec<_>, _>>()?;
        let history_range = match &clause.to_graph {
            Some(to) => {
                let [from] = default_graphs.as_slice() else {
                    return Err(DatasetParseError::InvalidFrom(
                        "FROM...TO requires exactly one FROM graph".to_string(),
                    ));
                };
                Some(HistoryTimeRange::between(
                    from,
                    &GraphSource::parse(to)?,
                    ("FROM", "TO"),
                )?)
            }
            // No TO clause = not a history query. Multiple FROM clauses are a
            // union, not a range.
            None => None,
        };
        Ok(Self {
            default_graphs,
            named_graphs,
            history_range,
        })
    }

    /// [`DatasetSpec::from_sparql`] for a parsed query: resolves the dataset
    /// clause against the query's prologue first. A query with no dataset
    /// clause has an empty spec.
    pub fn from_sparql_ast(ast: &fluree_db_sparql::SparqlAst) -> Result<Self, DatasetParseError> {
        match fluree_db_sparql::resolve_dataset_clause(ast)
            .map_err(|e| DatasetParseError::InvalidFrom(e.to_string()))?
        {
            Some(clause) => Self::from_sparql(&clause),
            None => Ok(Self::new()),
        }
    }
}

/// One member of a dataset, parsed once.
///
/// A member records the text as written (a SPARQL `FROM` / `FROM NAMED` IRI
/// after prefix and BASE expansion, a JSON-LD string, or a JSON-LD object's
/// `@id`), what that text names ([`MemberRef`]), the time it is read at, and
/// its per-source options. The fields are private: a `GraphSource` is built by
/// parsing ([`GraphSource::parse`]) or from an already-typed address
/// ([`GraphSource::ledger`]), never from an unclassified string.
///
/// A named member is known by its [`GraphSource::name`]: its alias when it has
/// one, else the text as written, pin included, so `FROM NAMED <L@t:2>` is
/// `GRAPH <L@t:2>` and two pins of one ledger are two members.
#[derive(Debug, Clone)]
pub struct GraphSource {
    written: Arc<str>,
    /// What `written` names. An address here carries no pin: the pin lives in
    /// `at`, where a JSON-LD `t` / `at` key or a path pin also lands.
    reference: MemberRef,
    at: Option<TimeSpec>,
    alias: Option<String>,
    policy_override: Option<SourcePolicyOverride>,
}

impl GraphSource {
    /// Parse a dataset-position string: a SPARQL `FROM` / `FROM NAMED` / `TO`
    /// IRI (after prologue expansion) or a JSON-LD `from` / `fromNamed`
    /// string. A graph keyword (`default`, `txn-meta`, `config`) names a graph
    /// of the target ledger; anything else is classified by
    /// [`DatasetRef::parse`].
    pub fn parse(s: &str) -> Result<Self, DatasetParseError> {
        let reference = MemberRef::parse(s)
            .map_err(|e| DatasetParseError::InvalidGraphSource(format!("'{s}': {e}")))?;
        Ok(Self::from_member(s.into(), reference))
    }

    /// A member for an already-parsed ledger address: its graph, and its pin
    /// as the member's time.
    pub fn ledger(address: LedgerRef) -> Self {
        let written: Arc<str> = if address.graph().is_default() {
            address.id().as_str().into()
        } else {
            format!("{}#{}", address.id(), address.graph()).into()
        };
        Self::from_member(written, MemberRef::Dataset(DatasetRef::Address(address)))
    }

    fn from_member(written: Arc<str>, reference: MemberRef) -> Self {
        let (reference, at) = match reference {
            MemberRef::Dataset(DatasetRef::Address(address)) => {
                let at = address.at().cloned();
                (
                    MemberRef::Dataset(DatasetRef::Address(address.without_at())),
                    at,
                )
            }
            other => (other, None),
        };
        Self {
            written,
            reference,
            at,
            alias: None,
            policy_override: None,
        }
    }

    /// The text as written.
    pub fn written(&self) -> &str {
        &self.written
    }

    /// What the text names (an address here carries no pin; see
    /// [`GraphSource::time_spec`]).
    pub fn reference(&self) -> &MemberRef {
        &self.reference
    }

    /// The address reading, when there is one: a ledger or graph source and
    /// a graph of it.
    pub fn address(&self) -> Option<&LedgerRef> {
        self.reference.address()
    }

    /// When the member is read.
    pub fn time_spec(&self) -> Option<&TimeSpec> {
        self.at.as_ref()
    }

    /// Dataset-local alias (unique within the request).
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// The name `GRAPH <name>` matches and `GRAPH ?g` binds for a named member:
    /// the alias, else the text as written.
    pub fn name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.written)
    }

    /// Per-source policy override.
    pub fn policy_override(&self) -> Option<&SourcePolicyOverride> {
        self.policy_override.as_ref()
    }

    /// Read the member at `time_spec`, replacing any time it named itself.
    pub fn with_time(mut self, time_spec: TimeSpec) -> Self {
        self.at = Some(time_spec);
        self
    }

    /// Set dataset-local alias
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Set per-source policy override
    pub fn with_policy(mut self, policy: SourcePolicyOverride) -> Self {
        self.policy_override = Some(policy);
        self
    }

    /// The written text minus its `@` pin (`L@t:2#g` → `L#g`): the name a
    /// pinned member was known by before members were named as written, kept
    /// as a non-enumerated alias so a `GRAPH <L>` over `FROM NAMED <L@t:2>`
    /// still matches. `None` for a member with no pin in its text.
    pub(crate) fn unpinned_name(&self) -> Option<String> {
        self.address()?;
        let (before_graph, graph) = match self.written.split_once('#') {
            Some((before, graph)) => (before, Some(graph)),
            None => (&*self.written, None),
        };
        let (base, _pin) = before_graph.split_once('@')?;
        Some(match graph {
            Some(graph) => format!("{base}#{graph}"),
            None => base.to_string(),
        })
    }
}

impl TryFrom<&str> for GraphSource {
    type Error = DatasetParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::parse(s)
    }
}

impl std::str::FromStr for GraphSource {
    type Err = DatasetParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
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

/// Graph selector for specifying which graph within a ledger to query.
///
/// The one graph-name keyword table lives in `fluree-db-core`
/// ([`GraphSel`]); this name is kept so existing paths compile. A ledger can
/// contain multiple named graphs:
/// - Default graph (g_id=0): the main data graph
/// - txn-meta graph (g_id=1): transaction metadata
/// - config graph (g_id=2): ledger governance/config
/// - User-defined named graphs: absolute IRIs mapped to g_id via the registry
///
/// `TxnMeta` and `Config` name RESERVED graphs. Selecting one is an explicit,
/// ledger-qualified act — the selector only exists because a caller wrote it —
/// so it is permitted here; what stays closed is implicit reachability
/// (`GRAPH ?g` enumeration, an unnamed `GRAPH <iri>`). See the reserved-graph
/// contract table on `Fluree::resolve_within_ledger_graph` in `view/query.rs`.
pub type GraphSelector = GraphSel;

/// Time specification for graph sources: one point in a ledger's history.
///
/// Defined in `fluree-db-core` (the address grammar lives there) and
/// re-exported here unchanged.
pub use fluree_db_core::{TimeSpec, ACCEPTED_TIME_SPEC_SPELLINGS};

/// Time range for history queries
///
/// Represents a range of time for querying changes/history on one ledger,
/// from `"from": "ledger@t:1", "to": "ledger@t:latest"` (JSON-LD) or
/// `FROM <ledger@t:1> TO <ledger@t:latest>` (SPARQL).
#[derive(Debug, Clone)]
pub struct HistoryTimeRange {
    /// The ledger whose history is read.
    pub ledger: LedgerId,
    /// Start of the time range
    pub from: TimeSpec,
    /// End of the time range
    pub to: TimeSpec,
}

impl HistoryTimeRange {
    /// Create a new history time range
    pub fn new(ledger: LedgerId, from: TimeSpec, to: TimeSpec) -> Self {
        Self { ledger, from, to }
    }

    /// The range between two parsed endpoints: both must name the same whole
    /// ledger, each with a time. `keys` names the two endpoints the way the
    /// query spelled them, for error text.
    fn between(
        from: &GraphSource,
        to: &GraphSource,
        (from_key, to_key): (&str, &str),
    ) -> Result<Self, DatasetParseError> {
        let endpoint = |source: &GraphSource, key: &str| -> Result<LedgerId, DatasetParseError> {
            match source.address() {
                Some(address) if address.graph().is_default() => Ok(address.id().clone()),
                Some(_) => Err(DatasetParseError::InvalidFrom(format!(
                    "a history range reads a whole ledger; drop the graph from '{}'",
                    source.written()
                ))),
                None => Err(DatasetParseError::InvalidFrom(format!(
                    "{key} in a history range must name a ledger, got '{}'",
                    source.written()
                ))),
            }
        };
        let ledger = endpoint(from, from_key)?;
        if endpoint(to, to_key)? != ledger {
            return Err(DatasetParseError::InvalidFrom(format!(
                "{from_key} and {to_key} must reference the same ledger: '{}' vs '{}'",
                from.written(),
                to.written()
            )));
        }
        let from_time = from.time_spec().cloned().ok_or_else(|| {
            DatasetParseError::InvalidFrom(format!(
                "{from_key} graph in a history range must have a time specification \
                 (e.g., ledger@t:1)"
            ))
        })?;
        let to_time = to.time_spec().cloned().ok_or_else(|| {
            DatasetParseError::InvalidFrom(format!(
                "{to_key} graph must have a time specification (e.g., ledger@t:latest)"
            ))
        })?;
        Ok(Self::new(ledger, from_time, to_time))
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
    /// Parse a DatasetSpec from a JSON-LD query body.
    ///
    /// One parser for every JSON-LD surface. The dataset may be given at the
    /// top level or inside `opts`, which takes precedence:
    ///
    /// - default graphs: `opts.from` || `opts.ledger` || `from` || `ledger`
    /// - named graphs: `opts.fromNamed` || `opts.from-named` || `fromNamed` ||
    ///   `from-named`
    /// - history end: `opts.to` || `to`
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
    /// A string is a dataset-position reference ([`GraphSource::parse`]): a
    /// ledger address, a graph IRI, or a graph keyword. An object's `@id` is a
    /// ledger address ([`LedgerRef::parse`]) and its `graph` / `@graph` a graph
    /// of that ledger ([`GraphSel::parse`]).
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
        let Some(obj) = json.as_object() else {
            return Ok(Self::new()); // Not an object, return empty spec
        };
        let opts_obj = obj.get("opts").and_then(|v| v.as_object());
        let in_opts = |key: &str| opts_obj.and_then(|o| o.get(key));

        let from_val = in_opts("from")
            .or_else(|| in_opts("ledger"))
            .or_else(|| obj.get("from"))
            .or_else(|| obj.get("ledger"));
        // "fromNamed" (new) takes precedence over "from-named" (legacy).
        let from_named_val = in_opts("fromNamed")
            .or_else(|| in_opts("from-named"))
            .or_else(|| obj.get("fromNamed"))
            .or_else(|| obj.get("from-named"));
        let to_val = in_opts("to").or_else(|| obj.get("to"));

        let mut spec = Self::new();
        if let Some(v) = from_val {
            spec.default_graphs = parse_graph_sources(v, "from")?;
        }

        // Explicit "to" key: a history query, mirroring SPARQL's FROM ... TO.
        if let Some(to_v) = to_val {
            let [from] = spec.default_graphs.as_slice() else {
                return Err(DatasetParseError::InvalidFrom(
                    "'to' requires exactly one 'from' graph".to_string(),
                ));
            };
            let to = parse_single_graph_source(to_v, "to")?;
            spec.history_range = Some(HistoryTimeRange::between(from, &to, ("'from'", "'to'"))?);
        }

        if let Some(v) = from_named_val {
            spec.named_graphs = match v.as_object() {
                Some(named_obj) => parse_named_graph_object(named_obj)?,
                None => parse_graph_sources(v, "fromNamed")?,
            };
        }

        validate_alias_uniqueness(&spec)?;
        Ok(spec)
    }

    /// [`DatasetSpec::from_json`] plus the query's connection options
    /// (identity and policy inputs from `opts`).
    pub fn from_query_json(
        json: &JsonValue,
    ) -> Result<(Self, GovernanceOptions), DatasetParseError> {
        let spec = Self::from_json(json)?;
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

/// Parse graph sources from a JSON value
///
/// Accepts:
/// - String: single graph source ([`GraphSource::parse`])
/// - Array: multiple graph sources
/// - Object: single graph source (see [`parse_single_graph_source`])
fn parse_graph_sources(
    val: &JsonValue,
    field_name: &str,
) -> Result<Vec<GraphSource>, DatasetParseError> {
    match val {
        JsonValue::String(s) => Ok(vec![GraphSource::parse(s)?]),
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
/// objects with `@id` (ledger ref) and `@graph` (graph selector):
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
/// Keys become the source alias. The `@id` field is required (ledger reference).
/// The graph selector is optional ("default", "txn-meta", "config", or an
/// absolute graph IRI) and may be spelled `@graph` or `graph` — the `from`
/// single-source form reads the same two spellings, so neither form silently
/// ignores the other's.
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
        let source = parse_object_source(entry, raw_identifier)?.with_alias(alias.clone());
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

/// A source object's `@id`, time keys, graph selector and policy (`alias` is
/// read by the caller, since `fromNamed` object keys supply it too).
///
/// `@id` is a ledger address. The graph selector may be spelled `@graph` or
/// `graph`: the `fromNamed` object form historically read only `@graph` while
/// the `from` single-source form read only `graph`, and writing the other
/// form's spelling was silently ignored — the source resolved to the whole
/// ledger and the query returned a plausible wrong answer with a 200. Naming
/// a graph both in the `@id` fragment and in the selector is refused as
/// ambiguous. An explicit `t` / `at` key overrides a pin in the `@id`.
fn parse_object_source(
    obj: &serde_json::Map<String, JsonValue>,
    raw_identifier: &str,
) -> Result<GraphSource, DatasetParseError> {
    let mut address = LedgerRef::parse(raw_identifier)
        .map_err(|e| DatasetParseError::InvalidGraphSource(format!("'@id' {e}")))?;

    let graph = match obj.get("@graph") {
        Some(v) => Some(("@graph", v)),
        None => obj.get("graph").map(|v| ("graph", v)),
    };
    if let Some((key, graph_val)) = graph {
        if !address.graph().is_default() {
            return Err(DatasetParseError::AmbiguousGraphSelector(
                raw_identifier.to_string(),
            ));
        }
        let graph_str = graph_val.as_str().ok_or_else(|| {
            DatasetParseError::InvalidGraphSource(format!(
                "'{key}' must be a string ('default', 'txn-meta', 'config', or a graph IRI)"
            ))
        })?;
        let sel = GraphSel::parse(graph_str)
            .map_err(|e| DatasetParseError::InvalidGraphSource(format!("'{key}': {e}")))?;
        address = address.with_graph(sel);
    }

    if let Some(t_val) = obj.get("t") {
        if let Some(t) = t_val.as_i64() {
            address = address.with_at(TimeSpec::AtT(t));
        }
    } else if let Some(at_val) = obj.get("at") {
        if let Some(at_str) = at_val.as_str() {
            address = address.with_at(parse_object_at(at_str)?);
        }
    }

    let mut source = GraphSource::ledger(address);
    source.written = raw_identifier.into();
    if let Some(policy_val) = obj.get("policy") {
        source = source.with_policy(parse_source_policy_override(policy_val)?);
    }
    Ok(source)
}

/// Parse a single graph source from a JSON value
///
/// Accepts:
/// - String: a dataset-position reference ([`GraphSource::parse`])
/// - Object: Extended graph source object with optional fields:
///   - `@id` / `id`: ledger reference (required)
///   - `t` / `at`: time specification
///   - `alias`: dataset-local alias (optional)
///   - `graph` / `@graph`: graph selector - "default", "txn-meta", "config", or an IRI (optional)
///   - `policy`: per-source policy override (optional)
fn parse_single_graph_source(
    val: &JsonValue,
    field_name: &str,
) -> Result<GraphSource, DatasetParseError> {
    match val {
        JsonValue::String(s) => GraphSource::parse(s),
        JsonValue::Object(obj) => {
            let raw_identifier = obj
                .get("@id")
                .or_else(|| obj.get("id"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    DatasetParseError::InvalidGraphSource(format!(
                        "'{field_name}' object must have '@id' or 'id' string field"
                    ))
                })?;
            let mut source = parse_object_source(obj, raw_identifier)?;
            if let Some(alias_val) = obj.get("alias") {
                let alias = alias_val.as_str().ok_or_else(|| {
                    DatasetParseError::InvalidGraphSource("'alias' must be a string".to_string())
                })?;
                source = source.with_alias(alias);
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
/// Also validates that an alias does not collide with another member's text as
/// written, since a member without an alias is known by that text.
fn validate_alias_uniqueness(spec: &DatasetSpec) -> Result<(), DatasetParseError> {
    use std::collections::HashSet;

    let mut all_keys: HashSet<&str> = spec.sources().map(GraphSource::written).collect();
    for source in spec.sources() {
        if let Some(alias) = source.alias() {
            if !all_keys.insert(alias) {
                return Err(DatasetParseError::DuplicateAlias(alias.to_string()));
            }
        }
    }
    Ok(())
}

/// The ledgers a SPARQL query's `FROM` / `FROM NAMED` / `TO` clauses name by
/// address, each once, as canonical `name:branch` ids (see
/// [`DatasetSpec::ledgers`]). A bare graph IRI names no ledger and is not
/// returned; prefixed and BASE-relative IRIs are expanded first.
///
/// Returns `Ok(vec![])` if the query has no dataset clause, and `Err` for a
/// query that does not parse or a clause IRI that is neither an address nor an
/// absolute IRI.
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
    Ok(DatasetSpec::from_sparql_ast(&ast)?
        .ledgers()
        .into_iter()
        .map(String::from)
        .collect())
}

/// Whether a SPARQL query carries a dataset clause (`FROM`, `FROM NAMED` or
/// `TO`). A query that does not parse has none.
pub fn sparql_has_dataset_clause(sparql: &str) -> bool {
    fluree_db_sparql::parse_sparql(sparql)
        .ast
        .is_some_and(|ast| crate::query::helpers::sparql_ast_has_dataset(&ast))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ledger a member's address names (a pin in the text is split off
    /// into the member's time; the text itself is kept as written).
    fn ledger_of(source: &GraphSource) -> String {
        source.address().expect("an address").id().to_string()
    }

    impl GraphSource {
        /// The graph a member's address selects, when it is not the default.
        fn graph_sel(&self) -> Option<GraphSel> {
            self.address()
                .map(|a| a.graph().clone())
                .filter(|g| !g.is_default())
        }
    }

    #[test]
    fn test_dataset_spec_empty() {
        let spec = DatasetSpec::new();
        assert!(spec.is_empty());
        assert_eq!(spec.num_graphs(), 0);
    }

    #[test]
    fn test_dataset_spec_with_graphs() {
        let spec = DatasetSpec::new()
            .with_default(GraphSource::parse("ledger1:main").unwrap())
            .with_default(GraphSource::parse("ledger2:main").unwrap())
            .with_named(GraphSource::parse("graph1").unwrap());

        assert!(!spec.is_empty());
        assert_eq!(spec.num_graphs(), 3);
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.named_graphs.len(), 1);
    }

    #[test]
    fn test_graph_source_with_time() {
        let source = GraphSource::parse("mydb:main")
            .unwrap()
            .with_time(TimeSpec::at_t(42));

        assert_eq!(source.written(), "mydb:main");
        assert!(matches!(source.time_spec(), Some(TimeSpec::AtT(42))));
    }

    #[test]
    fn test_graph_source_from_str() {
        let source: GraphSource = "test:ledger".parse().unwrap();
        assert_eq!(source.written(), "test:ledger");
        assert!(source.time_spec().is_none());
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main");
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
        assert_eq!(spec.default_graphs[0].written(), "ledger1:main");
        assert_eq!(spec.default_graphs[1].written(), "ledger2:main");
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
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
            &spec.default_graphs[0].time_spec(),
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
                spec.default_graphs[0].time_spec(),
                Some(expected),
                "from at={at}"
            );
            assert_eq!(
                spec.named_graphs[0].time_spec(),
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main@t:42");
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
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
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert_eq!(spec.default_graphs[0].graph_sel(), Some(GraphSel::TxnMeta));
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
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
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert!(matches!(
            &spec.default_graphs[0].time_spec(),
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
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert!(matches!(
            &spec.default_graphs[0].time_spec(),
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

        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger1:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
            Some(TimeSpec::AtT(10))
        ));

        assert_eq!(spec.default_graphs[1].written(), "ledger2:main");
        assert!(spec.default_graphs[1].time_spec().is_none());

        assert_eq!(ledger_of(&spec.default_graphs[2]), "ledger3:main");
        assert!(matches!(
            &spec.default_graphs[2].time_spec(),
            Some(TimeSpec::AtTime(s)) if s == "2025-01-01T00:00:00Z"
        ));
    }

    #[test]
    fn test_parse_ledger_id_invalid_time_format() {
        // `@` starts a pin only before a known tag, so in a dataset
        // position this text is not a ledger address at all: it parses as a
        // graph IRI (scheme `ledger`), which names no ledger to load.
        let query = json!({
            "from": "ledger:main@invalid:123",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });
        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(spec.default_graphs[0].address().is_none());
        assert!(spec.ledgers().is_empty());

        // A known tag with a bad value is a bad pin.
        let query = json!({"from": "ledger:main@t:abc", "select": ["?s"], "where": {"@id": "?s"}});
        assert!(DatasetSpec::from_json(&query).is_err());
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
        assert_eq!(spec.named_graphs[0].written(), "graph1");
        assert_eq!(spec.named_graphs[1].written(), "graph2");
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
            .find(|g| g.alias() == Some("products"))
            .expect("should have products alias");
        assert_eq!(products.written(), "mydb:main");
        assert!(matches!(
            &products.graph_sel(),
            Some(GraphSel::Named(ref iri)) if iri.as_str() == "http://example.org/graphs/products"
        ));

        let services = spec
            .named_graphs
            .iter()
            .find(|g| g.alias() == Some("services"))
            .expect("should have services alias");
        assert_eq!(services.written(), "mydb:main");
        assert!(matches!(
            &services.graph_sel(),
            Some(GraphSel::Named(ref iri)) if iri.as_str() == "http://example.org/graphs/services"
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
        assert_eq!(spec.named_graphs[0].alias(), Some("products"));
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
        assert_eq!(spec.named_graphs[0].written(), "graph1");
        assert_eq!(spec.named_graphs[1].written(), "graph2");
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
        assert_eq!(spec.default_graphs[0].written(), "ledger1:main");
        assert!(spec.default_graphs[0].time_spec().is_none());
        assert_eq!(spec.default_graphs[1].written(), "ledger2:main");
        assert!(matches!(
            spec.default_graphs[1].time_spec(),
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

    // SPARQL dataset clause tests: the clause as the parser and prologue give it

    /// The spec for a query with `dataset` between SELECT and WHERE, its IRIs
    /// expanded against a prologue declaring `ex:` and the empty prefix.
    fn sparql_spec(dataset: &str) -> Result<DatasetSpec, DatasetParseError> {
        let q = format!(
            "PREFIX ex: <http://ex.org/> PREFIX : <http://ex.org/local/> \
             SELECT * {dataset} WHERE {{ ?s ?p ?o }}"
        );
        let ast = fluree_db_sparql::parse_sparql(&q)
            .ast
            .expect("test query parses");
        DatasetSpec::from_sparql_ast(&ast)
    }

    #[test]
    fn test_from_sparql_no_clause() {
        assert!(sparql_spec("").unwrap().is_empty());
    }

    #[test]
    fn test_from_sparql_single_default() {
        let spec = sparql_spec("FROM <http://example.org/graph1>").unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(
            spec.default_graphs[0].written(),
            "http://example.org/graph1"
        );
        assert!(spec.named_graphs.is_empty());
    }

    #[test]
    fn test_from_sparql_multiple_default() {
        let spec = sparql_spec("FROM <http://example.org/graph1> FROM <http://example.org/graph2>")
            .unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(
            spec.default_graphs[0].written(),
            "http://example.org/graph1"
        );
        assert_eq!(
            spec.default_graphs[1].written(),
            "http://example.org/graph2"
        );
    }

    #[test]
    fn test_from_sparql_named_graphs() {
        let spec = sparql_spec(
            "FROM NAMED <http://example.org/named1> FROM NAMED <http://example.org/named2>",
        )
        .unwrap();
        assert!(spec.default_graphs.is_empty());
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(spec.named_graphs[0].written(), "http://example.org/named1");
        assert_eq!(spec.named_graphs[1].written(), "http://example.org/named2");
    }

    #[test]
    fn test_from_sparql_mixed() {
        let spec = sparql_spec(
            "FROM <http://example.org/default1> \
             FROM NAMED <http://example.org/named1> FROM NAMED <http://example.org/named2>",
        )
        .unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(
            spec.default_graphs[0].written(),
            "http://example.org/default1"
        );
    }

    /// Prefixed names expand against the prologue before they are read:
    /// they used to reach the resolver as the literal `ex:graph1`.
    #[test]
    fn test_from_sparql_prefixed_iri_is_expanded() {
        let spec = sparql_spec("FROM ex:graph1 FROM NAMED :localname").unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].written(), "http://ex.org/graph1");
        assert_eq!(spec.named_graphs.len(), 1);
        assert_eq!(
            spec.named_graphs[0].written(),
            "http://ex.org/local/localname"
        );
        // An expanded hierarchical IRI is a graph IRI, never a ledger.
        assert!(spec.default_graphs[0].address().is_none());
    }

    /// A relative FROM resolves against BASE.
    #[test]
    fn test_from_sparql_base_relative_iri_is_resolved() {
        let q = "BASE <http://ex.org/graphs/> SELECT * FROM NAMED <products> WHERE { ?s ?p ?o }";
        let ast = fluree_db_sparql::parse_sparql(q).ast.expect("parses");
        let spec = DatasetSpec::from_sparql_ast(&ast).unwrap();
        assert_eq!(
            spec.named_graphs[0].written(),
            "http://ex.org/graphs/products"
        );
    }

    #[test]
    fn test_from_sparql_time_travel_suffix() {
        let spec = sparql_spec(
            "FROM <ledger:main@t:42> FROM <ledger:main@iso:2025-01-01T00:00:00Z> \
             FROM NAMED <ledger:main@commit:abc123def456>",
        )
        .unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        let ledger = |s: &GraphSource| s.address().expect("an address").id().to_string();
        assert_eq!(ledger(&spec.default_graphs[0]), "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
            Some(TimeSpec::AtT(42))
        ));
        assert_eq!(ledger(&spec.default_graphs[1]), "ledger:main");
        assert!(matches!(
            spec.default_graphs[1].time_spec(),
            Some(TimeSpec::AtTime(s)) if s == "2025-01-01T00:00:00Z"
        ));

        assert_eq!(spec.named_graphs.len(), 1);
        // The member is named as written, pin included.
        assert_eq!(
            spec.named_graphs[0].name(),
            "ledger:main@commit:abc123def456"
        );
        assert_eq!(ledger(&spec.named_graphs[0]), "ledger:main");
        assert!(matches!(
            spec.named_graphs[0].time_spec(),
            Some(TimeSpec::AtCommit(s)) if s == "abc123def456"
        ));
    }

    /// FROM and TO spelling one ledger two ways (`ledger` vs `ledger:main`)
    /// are the same ledger; two different ledgers still aren't. JSON-LD and
    /// SPARQL both check this.
    #[test]
    fn history_range_accepts_two_spellings_of_one_ledger() {
        let json_spec = |from: &str, to: &str| {
            DatasetSpec::from_json(&json!({
                "from": from,
                "to": to,
                "select": ["?s"],
                "where": {"@id": "?s"}
            }))
        };
        let sparql_range = |from: &str, to: &str| sparql_spec(&format!("FROM <{from}> TO <{to}>"));

        for spec in [
            json_spec("ledger@t:1", "ledger:main@t:latest"),
            sparql_range("ledger@t:1", "ledger:main@t:latest"),
        ] {
            assert!(spec.expect("same ledger").is_history_mode());
        }
        assert!(json_spec("ledger@t:1", "other:main@t:latest").is_err());
        assert!(sparql_range("ledger@t:1", "ledger:dev@t:latest").is_err());
    }

    #[test]
    fn test_from_sparql_to_graph_history_range() {
        let spec = sparql_spec("FROM <ledger:main@t:1> TO <ledger:main@t:latest>").unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode from TO clause"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.ledger, "ledger:main");
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
        assert_eq!(range.ledger.as_str(), "ledger:main");
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
        assert_eq!(range.ledger.as_str(), "ledger:main");
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
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
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
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert_eq!(spec.default_graphs[0].graph_sel(), Some(GraphSel::TxnMeta));
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
            Some(TimeSpec::Latest)
        ));
    }

    // =============================================================================
    // Named Graph / Graph Selector Tests (query-connection handoff spec)
    // =============================================================================

    #[test]
    fn test_graph_selector_from_str() {
        assert!(matches!(
            GraphSel::parse("default").unwrap(),
            GraphSel::Default
        ));
        assert!(matches!(
            GraphSel::parse("txn-meta").unwrap(),
            GraphSel::TxnMeta
        ));
        assert!(matches!(
            GraphSel::parse("http://example.org/graph").unwrap(),
            GraphSel::Named(ref s) if s.as_str() == "http://example.org/graph"
        ));
        // IRI with hash (should not be confused with "default" or "txn-meta")
        assert!(matches!(
            GraphSel::parse("http://example.org/vocab#products").unwrap(),
            GraphSel::Named(ref s) if s.as_str() == "http://example.org/vocab#products"
        ));
    }

    #[test]
    fn test_graph_source_with_alias() {
        let source = GraphSource::parse("ledger:main")
            .unwrap()
            .with_alias("myAlias")
            .with_time(TimeSpec::at_t(42));

        assert_eq!(source.written(), "ledger:main");
        assert_eq!(source.alias(), Some("myAlias"));
        assert!(matches!(source.time_spec(), Some(TimeSpec::AtT(42))));
    }

    #[test]
    fn test_graph_source_with_graph_selector() {
        let source = GraphSource::ledger(
            LedgerRef::parse("ledger:main")
                .unwrap()
                .with_graph(GraphSel::TxnMeta),
        );

        assert_eq!(source.written(), "ledger:main#txn-meta");
        assert!(matches!(source.graph_sel(), Some(GraphSel::TxnMeta)));
    }

    #[test]
    fn test_parse_from_object_with_alias() {
        let query = json!({
            "from": {"@id": "ledger:main", "alias": "mydb"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].written(), "ledger:main");
        assert_eq!(spec.default_graphs[0].alias(), Some("mydb"));
    }

    #[test]
    fn test_parse_from_object_with_graph_default() {
        let query = json!({
            "from": {"@id": "ledger:main", "graph": "default"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        let address = spec.default_graphs[0].address().expect("an address");
        assert_eq!(address.graph(), &GraphSel::Default);
    }

    #[test]
    fn test_parse_from_object_with_graph_txn_meta() {
        let query = json!({
            "from": {"@id": "ledger:main", "alias": "meta", "graph": "txn-meta"},
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].written(), "ledger:main");
        assert_eq!(spec.default_graphs[0].alias(), Some("meta"));
        assert!(matches!(
            spec.default_graphs[0].graph_sel(),
            Some(GraphSel::TxnMeta)
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main");
        assert_eq!(spec.default_graphs[0].alias(), Some("products"));
        assert!(matches!(
            &spec.default_graphs[0].graph_sel(),
            Some(GraphSel::Named(ref iri)) if iri.as_str() == "http://example.org/vocab#products"
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
            .find(|g| g.alias() == Some("salesProducts"))
            .expect("should have salesProducts alias");
        assert_eq!(sales.written(), "sales:main");
        assert!(matches!(
            &sales.graph_sel(),
            Some(GraphSel::Named(ref iri)) if iri.as_str() == "http://example.org/vocab#products"
        ));

        let inventory = spec
            .named_graphs
            .iter()
            .find(|g| g.alias() == Some("inventoryProducts"))
            .expect("should have inventoryProducts alias");
        assert_eq!(inventory.written(), "inventory:main");
        assert!(matches!(
            &inventory.graph_sel(),
            Some(GraphSel::Named(ref iri)) if iri.as_str() == "http://example.org/vocab#products"
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main@t:5");
        assert_eq!(ledger_of(&spec.default_graphs[0]), "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
            Some(TimeSpec::AtT(5))
        ));
        assert_eq!(spec.default_graphs[0].alias(), Some("oldData"));
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

        let policy = spec.default_graphs[0].policy_override().unwrap();
        assert_eq!(policy.identity.as_deref(), Some("did:example:user1"));
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main#txn-meta");
        // The fragment is the member's graph.
        assert_eq!(spec.default_graphs[0].graph_sel(), Some(GraphSel::TxnMeta));
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
        assert_eq!(spec.default_graphs[0].written(), "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec(),
            Some(TimeSpec::AtT(42))
        ));
        // New fields are None
        assert!(spec.default_graphs[0].alias().is_none());
        assert!(spec.default_graphs[0].graph_sel().is_none());
        assert!(spec.default_graphs[0].policy_override().is_none());
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
            .find(|s| s.alias() == Some(alias))
            .expect("named source present")
            .graph_sel()
            .clone()
    }

    fn default_selector(query: &JsonValue) -> Option<GraphSelector> {
        let (spec, _) = DatasetSpec::from_query_json(query).expect("parses");
        spec.default_graphs[0].graph_sel().clone()
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
            Some(GraphSel::Named(ref s)) if s.as_str() == "http://ex.org/g1"
        ));
        // Previously `None` — silently the whole ledger.
        assert!(matches!(
            named_selector(&without_at, "g"),
            Some(GraphSel::Named(ref s)) if s.as_str() == "http://ex.org/g1"
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
            Some(GraphSel::Named(ref s)) if s.as_str() == "http://ex.org/g1"
        ));
        // Previously `None` — silently the whole ledger.
        assert!(matches!(
            default_selector(&with_at),
            Some(GraphSel::Named(ref s)) if s.as_str() == "http://ex.org/g1"
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
