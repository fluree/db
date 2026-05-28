//! Multi-query envelope: bundle multiple independent queries against a single
//! atomic snapshot.
//!
//! ## Design
//!
//! A multi-query envelope wraps N sub-queries with shared `@context`, shared
//! `opts`, and a single `asOf` snapshot moment. Each sub-query carries its own
//! `from` (dataset spec) and its own language (`jsonld` or `sparql`); they run
//! independently and in parallel, but all see the same per-ledger `t`.
//!
//! ### Inheritance rules
//!
//! Two rules cover the entire "what shadows what" surface:
//!
//! 1. **Mergeable fields** (`@context`, `opts`): shallow merge with envelope as
//!    default and sub-query winning on key conflict. `@context` follows JSON-LD
//!    inheritance semantics; `@context: null` in a sub-query resets the context
//!    for that entry only.
//! 2. **Temporal pin** (`asOf` vs any inner `@t:` / `t` field / SPARQL
//!    `FROM <ledger@t:...>`): collision is an **error**, not a merge. If the
//!    envelope sets `asOf`, no sub-query may carry an explicit temporal pin.
//!
//! `opts.t` is rejected at any nesting level in the multi-query envelope — the
//! single canonical place to pin sub-query time is the `from` field.
//!
//! ### Atomicity
//!
//! `asOf` resolves once at envelope-entry to a per-ledger `t` map. ISO timestamps
//! resolve to "the latest commit on each ledger at or before this moment." This
//! is **shared time resolution, not distributed atomicity** — multi-ledger
//! envelopes do not have a single global commit clock.

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;

// =============================================================================
// Bounds configuration
// =============================================================================

/// Server-configured limits applied to every multi-query envelope.
///
/// Constructed once at server startup from configuration; the dispatcher and
/// validator both consult this struct rather than reading globals.
#[derive(Debug, Clone, Copy)]
pub struct MultiQueryBounds {
    /// Maximum number of sub-queries permitted in a single envelope.
    ///
    /// Rejected at parse time before snapshot resolution begins.
    pub max_queries: usize,
    /// Maximum number of distinct ledgers referenced across the envelope's
    /// sub-queries.
    ///
    /// Each distinct ledger triggers a snapshot resolution; this caps the
    /// fan-out cost a single envelope can impose.
    pub max_distinct_ledgers: usize,
    /// Server-wide ceiling on concurrent sub-query execution.
    ///
    /// `opts.maxConcurrency` is clamped to this value.
    pub max_concurrency: usize,
    /// Server-wide ceiling on envelope wall-clock duration in milliseconds.
    ///
    /// `opts.timeoutMs` is clamped to this value.
    pub max_envelope_timeout_ms: u64,
    /// Server-wide ceiling on total assembled response size in bytes.
    ///
    /// Enforced during streaming serialization; the envelope errors with
    /// `ResponseSizeExceeded` once total bytes written crosses this threshold.
    pub max_response_size_bytes: usize,
}

impl MultiQueryBounds {
    /// Default bounds suitable for a single-tenant development server.
    ///
    /// Production deployments should tune via server configuration.
    pub const DEFAULT: Self = Self {
        max_queries: 64,
        max_distinct_ledgers: 8,
        max_concurrency: 16,
        max_envelope_timeout_ms: 60_000,
        max_response_size_bytes: 64 * 1024 * 1024,
    };
}

impl Default for MultiQueryBounds {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// =============================================================================
// Request types
// =============================================================================

/// Multi-query envelope request body.
///
/// Wire format (JSON):
/// ```json
/// {
///   "@context": { "schema": "http://schema.org/" },
///   "asOf":     "2024-01-01T12:00:00Z",
///   "opts":     { "meta": true },
///   "queries":  {
///     "alice": { "language": "jsonld", "query": { ... } },
///     "brian": { "language": "sparql", "query": "SELECT ..." }
///   }
/// }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MultiQueryRequest {
    /// Envelope-default JSON-LD context. Inherited by each JSON-LD sub-query and
    /// (for prefix-shaped entries) optionally injected as SPARQL `PREFIX`
    /// declarations.
    #[serde(rename = "@context", skip_serializing_if = "Option::is_none", default)]
    pub context: Option<JsonValue>,
    /// Shared snapshot pin. Integer (single-ledger only), ISO 8601 timestamp
    /// (multi-ledger), or omitted (server resolves "now" once at envelope
    /// entry).
    #[serde(rename = "asOf", skip_serializing_if = "Option::is_none", default)]
    pub as_of: Option<AsOf>,
    /// Envelope-default opts merged into each sub-query (inner wins on key
    /// conflict).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub opts: Option<JsonValue>,
    /// Map of alias → sub-query. Aliases become keys in `results` / `errors`.
    pub queries: indexmap::IndexMap<String, MultiQuerySubquery>,
}

/// Envelope-level snapshot pin.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AsOf {
    /// Numeric transaction `t` — only valid when every sub-query targets the
    /// same ledger (validation enforces this).
    T(i64),
    /// ISO 8601 timestamp. Each ledger resolves to its latest commit at or
    /// before this moment.
    Iso(String),
}

/// A single sub-query inside a multi-query envelope.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MultiQuerySubquery {
    /// Sub-query language. Determines which parser the `query` field gets
    /// routed through.
    pub language: SubqueryLanguage,
    /// Sub-query body. For `jsonld`, an object with the usual JSON-LD query
    /// keys (`from`, `select`/`construct`/`ask`, `where`, etc.). For `sparql`,
    /// the SPARQL query string with its own `FROM <...>` dataset clause.
    pub query: JsonValue,
    /// Per-sub-query opts overrides. Shallow-merged onto envelope opts, with
    /// these values winning on key conflict. `opts.t` is rejected here.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub opts: Option<JsonValue>,
}

/// Sub-query language discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubqueryLanguage {
    /// Fluree JSON-LD query syntax.
    #[serde(alias = "json-ld")]
    JsonLd,
    /// SPARQL 1.1 query syntax.
    Sparql,
}

// =============================================================================
// Response types
// =============================================================================

/// Multi-query envelope response body.
#[derive(Debug, Clone, Serialize)]
pub struct MultiQueryResponse {
    /// Aggregate outcome over all sub-queries.
    pub status: MultiQueryStatus,
    /// Snapshot resolution actually applied to the envelope.
    pub snapshot: SnapshotInfo,
    /// Per-alias successful results. Aliases that errored are absent here and
    /// present in `errors`.
    pub results: JsonMap<String, JsonValue>,
    /// Per-alias error entries for sub-queries that failed or timed out.
    #[serde(skip_serializing_if = "JsonMap::is_empty", default)]
    pub errors: JsonMap<String, JsonValue>,
    /// Optional fuel/timing aggregates when `opts.meta` is enabled.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub meta: Option<MultiQueryMeta>,
}

/// Top-level outcome over the envelope's sub-queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiQueryStatus {
    /// Every sub-query succeeded.
    Ok,
    /// At least one sub-query succeeded and at least one failed / timed out.
    Partial,
    /// Every sub-query failed or timed out.
    AllFailed,
}

/// Snapshot moment applied to the envelope, echoed back to the client for
/// debugging and replay.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotInfo {
    /// ISO 8601 wall-clock moment used as the resolution target. Either echoes
    /// the request's `asOf` ISO value, or — when the request omitted `asOf` —
    /// the server-resolved "now" moment.
    #[serde(rename = "asOf", skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
    /// Per-ledger numeric `t` that each sub-query observed. Each value is the
    /// latest commit at or before `asOf` for that ledger.
    ///
    /// **Note**: shared time resolution, not distributed atomicity.
    pub ledgers: JsonMap<String, JsonValue>,
}

/// Aggregate fuel/timing metadata for the whole envelope, included when
/// `opts.meta` is enabled.
#[derive(Debug, Clone, Default, Serialize)]
pub struct MultiQueryMeta {
    /// Best-effort sum of per-sub-query fuel consumption. Not a hard budget in
    /// v1 — sub-queries share fuel only via their own per-query caps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel_total: Option<f64>,
    /// Wall-clock duration from envelope entry to response assembly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
}

// =============================================================================
// Validation
// =============================================================================

/// Structured validation failure. Maps 1:1 to a 4xx response when the envelope
/// itself is rejected before any sub-query dispatches.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MultiQueryValidationError {
    #[error("envelope must contain at least one sub-query")]
    EmptyEnvelope,
    #[error(
        "envelope has {actual} sub-queries; server limit is {limit}"
    )]
    TooManyQueries { actual: usize, limit: usize },
    #[error(
        "envelope references {actual} distinct ledgers; server limit is {limit}"
    )]
    TooManyDistinctLedgers { actual: usize, limit: usize },
    #[error("sub-query alias must not be empty")]
    EmptyAlias,
    #[error(
        "sub-query '{alias}': asOf collision — envelope asOf is set and this sub-query carries an explicit temporal pin ({location})"
    )]
    AsOfCollision { alias: String, location: String },
    #[error(
        "sub-query '{alias}': opts.t is reserved in multi-query envelopes; pin time via 'from' or the envelope 'asOf' field"
    )]
    OptsTNotAllowed { alias: String },
    #[error(
        "envelope asOf is an integer t; this only applies to single-ledger envelopes, but {distinct} distinct ledgers were referenced"
    )]
    AsOfIntegerMultiLedger { distinct: usize },
    #[error("sub-query '{alias}': JSON-LD query body must be a JSON object")]
    JsonLdBodyNotObject { alias: String },
    #[error("sub-query '{alias}': SPARQL query body must be a string")]
    SparqlBodyNotString { alias: String },
    #[error(
        "sub-query '{alias}': missing or empty 'from' — each sub-query must specify its own dataset"
    )]
    MissingFrom { alias: String },
    #[error(
        "opts.maxConcurrency = {value} exceeds server limit {limit}"
    )]
    MaxConcurrencyExceeded { value: u64, limit: usize },
    #[error("opts.maxConcurrency must be at least 1")]
    MaxConcurrencyZero,
    #[error(
        "opts.timeoutMs = {value} exceeds server limit {limit}"
    )]
    TimeoutExceeded { value: u64, limit: u64 },
    #[error(
        "envelope-level fuel budget (max-fuel) is not supported in v1; \
         set per-sub-query opts.max-fuel instead"
    )]
    EnvelopeFuelBudgetUnsupported,
    #[error(
        "sub-query '{alias}': history queries (with 'to' / FROM <...> TO <...>) \
         are not yet supported inside multi-query envelopes — run them as \
         single queries"
    )]
    HistoryQueryInEnvelope { alias: String },
}

/// Validate an envelope against server bounds and the merge/collision rules.
///
/// Returns the set of distinct ledger identifiers referenced by the sub-queries
/// (with any per-query temporal suffix stripped) on success. The dispatcher
/// reuses this set to drive snapshot resolution without re-walking the
/// envelope.
pub fn validate_envelope(
    req: &MultiQueryRequest,
    bounds: &MultiQueryBounds,
) -> Result<BTreeSet<String>, MultiQueryValidationError> {
    if req.queries.is_empty() {
        return Err(MultiQueryValidationError::EmptyEnvelope);
    }
    if req.queries.len() > bounds.max_queries {
        return Err(MultiQueryValidationError::TooManyQueries {
            actual: req.queries.len(),
            limit: bounds.max_queries,
        });
    }

    // Envelope-level opts bounds (maxConcurrency, timeoutMs)
    if let Some(opts) = req.opts.as_ref().and_then(JsonValue::as_object) {
        validate_envelope_opts(opts, bounds)?;
    }

    let envelope_pinned = req.as_of.is_some();
    let mut distinct_ledgers: BTreeSet<String> = BTreeSet::new();

    for (alias, sq) in &req.queries {
        if alias.is_empty() {
            return Err(MultiQueryValidationError::EmptyAlias);
        }

        // Per-sub-query opts checks (opts.t reject).
        if let Some(sub_opts) = sq.opts.as_ref().and_then(JsonValue::as_object) {
            if sub_opts.contains_key("t") {
                return Err(MultiQueryValidationError::OptsTNotAllowed {
                    alias: alias.clone(),
                });
            }
        }

        match sq.language {
            SubqueryLanguage::JsonLd => {
                validate_jsonld_subquery(alias, sq, envelope_pinned, &mut distinct_ledgers)?;
            }
            SubqueryLanguage::Sparql => {
                validate_sparql_subquery(alias, sq, envelope_pinned, &mut distinct_ledgers)?;
            }
        }
    }

    if distinct_ledgers.len() > bounds.max_distinct_ledgers {
        return Err(MultiQueryValidationError::TooManyDistinctLedgers {
            actual: distinct_ledgers.len(),
            limit: bounds.max_distinct_ledgers,
        });
    }

    // An integer envelope asOf only applies to single-ledger envelopes.
    if matches!(req.as_of, Some(AsOf::T(_))) && distinct_ledgers.len() > 1 {
        return Err(MultiQueryValidationError::AsOfIntegerMultiLedger {
            distinct: distinct_ledgers.len(),
        });
    }

    Ok(distinct_ledgers)
}

fn validate_envelope_opts(
    opts: &JsonMap<String, JsonValue>,
    bounds: &MultiQueryBounds,
) -> Result<(), MultiQueryValidationError> {
    if let Some(mc) = opts.get("maxConcurrency").and_then(JsonValue::as_u64) {
        if mc == 0 {
            return Err(MultiQueryValidationError::MaxConcurrencyZero);
        }
        if (mc as usize) > bounds.max_concurrency {
            return Err(MultiQueryValidationError::MaxConcurrencyExceeded {
                value: mc,
                limit: bounds.max_concurrency,
            });
        }
    }
    if let Some(t) = opts.get("timeoutMs").and_then(JsonValue::as_u64) {
        if t > bounds.max_envelope_timeout_ms {
            return Err(MultiQueryValidationError::TimeoutExceeded {
                value: t,
                limit: bounds.max_envelope_timeout_ms,
            });
        }
    }
    // Envelope-level fuel budget would need shared-atomic accounting across
    // parallel sub-queries to be enforceable; v1 supports per-sub-query
    // max-fuel only. Rejecting outright keeps "envelope total" from being
    // silently multiplied across N aliases.
    if opts.contains_key("max-fuel")
        || opts.contains_key("max_fuel")
        || opts.contains_key("maxFuel")
    {
        return Err(MultiQueryValidationError::EnvelopeFuelBudgetUnsupported);
    }
    Ok(())
}

fn validate_jsonld_subquery(
    alias: &str,
    sq: &MultiQuerySubquery,
    envelope_pinned: bool,
    distinct: &mut BTreeSet<String>,
) -> Result<(), MultiQueryValidationError> {
    let body = sq
        .query
        .as_object()
        .ok_or_else(|| MultiQueryValidationError::JsonLdBodyNotObject {
            alias: alias.to_string(),
        })?;

    // History queries (explicit `to` endpoint) span a range across two `t`
    // values rather than a single snapshot. The envelope's shared-snapshot
    // contract doesn't compose meaningfully with that; reject in v1.
    if body.contains_key("to") {
        return Err(MultiQueryValidationError::HistoryQueryInEnvelope {
            alias: alias.to_string(),
        });
    }

    // Inner `t` field is a temporal pin.
    if envelope_pinned && body.contains_key("t") {
        return Err(MultiQueryValidationError::AsOfCollision {
            alias: alias.to_string(),
            location: "inner 't' field".to_string(),
        });
    }

    let from = body
        .get("from")
        .ok_or_else(|| MultiQueryValidationError::MissingFrom {
            alias: alias.to_string(),
        })?;

    let extracted = jsonld_extract_from(from);
    if extracted.is_empty() {
        // `from` was present but yielded no ledger identifiers (empty array,
        // object missing `@id`/`id`, or an unsupported shape) — surface as
        // missing so downstream parsing produces a clearer error if needed.
        return Err(MultiQueryValidationError::MissingFrom {
            alias: alias.to_string(),
        });
    }

    for entry in extracted {
        if envelope_pinned {
            if let Some(loc) = entry.pin_location {
                return Err(MultiQueryValidationError::AsOfCollision {
                    alias: alias.to_string(),
                    location: loc,
                });
            }
        }
        distinct.insert(entry.ledger);
    }

    Ok(())
}

fn validate_sparql_subquery(
    alias: &str,
    sq: &MultiQuerySubquery,
    envelope_pinned: bool,
    distinct: &mut BTreeSet<String>,
) -> Result<(), MultiQueryValidationError> {
    let body = sq
        .query
        .as_str()
        .ok_or_else(|| MultiQueryValidationError::SparqlBodyNotString {
            alias: alias.to_string(),
        })?;

    if sparql_is_history_query(body) {
        return Err(MultiQueryValidationError::HistoryQueryInEnvelope {
            alias: alias.to_string(),
        });
    }

    // If the SPARQL fails to parse, defer to the downstream parser for a
    // clearer error at execution time — validation only enforces the
    // multi-query invariants, not SPARQL grammar.
    let Some(extracted) = sparql_extract_from(body) else {
        return Ok(());
    };

    if extracted.is_empty() {
        return Err(MultiQueryValidationError::MissingFrom {
            alias: alias.to_string(),
        });
    }

    for entry in extracted {
        if envelope_pinned {
            if let Some(loc) = entry.pin_location {
                return Err(MultiQueryValidationError::AsOfCollision {
                    alias: alias.to_string(),
                    location: loc,
                });
            }
        }
        distinct.insert(entry.ledger);
    }

    Ok(())
}

/// Detect Fluree's SPARQL history-range extension: `FROM <a> TO <b>`. The
/// parser surfaces this via `DatasetClause::to_graph`.
fn sparql_is_history_query(sparql: &str) -> bool {
    use fluree_db_sparql::ast::QueryBody;
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    let Some(ast) = parsed.ast.as_ref() else {
        return false;
    };
    let dataset = match &ast.body {
        QueryBody::Select(q) => q.dataset.as_ref(),
        QueryBody::Construct(q) => q.dataset.as_ref(),
        QueryBody::Ask(q) => q.dataset.as_ref(),
        QueryBody::Describe(q) => q.dataset.as_ref(),
        QueryBody::Update(_) => None,
    };
    dataset.map(|d| d.to_graph.is_some()).unwrap_or(false)
}

// =============================================================================
// Ledger / temporal-pin extraction helpers
// =============================================================================

/// Per-entry result of `from`-extraction: the bare ledger identifier and, when
/// the entry carries an explicit temporal pin, a human-readable location string
/// used in the collision error message.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtractedFrom {
    /// Bare ledger identifier with any `@t:`/`@iso:`/`@commit:` suffix and
    /// `#named-graph` fragment stripped.
    ledger: String,
    /// `Some(location)` if the source entry expressed a temporal pin
    /// (suffix marker in the identifier string, or an explicit `t`/`at` field
    /// on an object-form entry).
    pin_location: Option<String>,
}

/// Extract per-entry ledger identifiers and temporal-pin status from a JSON-LD
/// `from` value.
///
/// Mirrors the dataset parser surface (`parse_single_graph_source` /
/// `parse_graph_sources` in `crate::dataset`):
/// - String → single graph source. The identifier may carry a
///   `@t:`/`@iso:`/`@commit:` suffix (temporal pin) and an optional
///   `#named-graph` fragment.
/// - Object → single graph source with `@id`/`id` for the identifier and
///   optional `t` (integer) or `at` (string: `commit:HASH` or ISO timestamp)
///   for an explicit temporal pin.
/// - Array → any mix of the two element shapes above.
///
/// Entries that lack a usable identifier (empty string, object without
/// `@id`/`id`) are skipped silently — the language parsers produce a clearer
/// error at execution time.
fn jsonld_extract_from(from: &JsonValue) -> Vec<ExtractedFrom> {
    let mut out = Vec::new();
    extract_jsonld_from_value(from, &mut out);
    out
}

fn extract_jsonld_from_value(val: &JsonValue, out: &mut Vec<ExtractedFrom>) {
    match val {
        JsonValue::String(s) if !s.is_empty() => {
            out.push(extract_jsonld_from_string(s));
        }
        JsonValue::Object(obj) => {
            if let Some(entry) = extract_jsonld_from_object(obj) {
                out.push(entry);
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                extract_jsonld_from_value(item, out);
            }
        }
        _ => {}
    }
}

fn extract_jsonld_from_string(s: &str) -> ExtractedFrom {
    let pin_location = TEMPORAL_MARKERS.iter().find_map(|m| {
        if s.contains(m) {
            Some(format!("'from' contains temporal pin: {s}"))
        } else {
            None
        }
    });
    ExtractedFrom {
        ledger: strip_temporal_suffix(s).to_string(),
        pin_location,
    }
}

fn extract_jsonld_from_object(obj: &JsonMap<String, JsonValue>) -> Option<ExtractedFrom> {
    let id = obj
        .get("@id")
        .or_else(|| obj.get("id"))
        .and_then(JsonValue::as_str)
        .filter(|s| !s.is_empty())?;

    let mut entry = extract_jsonld_from_string(id);

    // Explicit `t`/`at` on the object are temporal pins regardless of any
    // suffix on the identifier; their presence is enough to flag a collision.
    if entry.pin_location.is_none() {
        if obj.contains_key("t") {
            entry.pin_location = Some(format!(
                "'from' object for '{}' carries explicit 't' field",
                entry.ledger
            ));
        } else if obj.contains_key("at") {
            entry.pin_location = Some(format!(
                "'from' object for '{}' carries explicit 'at' field",
                entry.ledger
            ));
        }
    }

    Some(entry)
}

/// Strip a `@t:`/`@iso:`/`@commit:` suffix from a ledger identifier so distinct
/// ledger counting is independent of temporal pins.
fn strip_temporal_suffix(ledger: &str) -> &str {
    // Fragment (`#named-graph`) may follow the temporal marker; we strip it
    // alongside the marker to keep counting on the bare ledger name.
    let bare = ledger.split('#').next().unwrap_or(ledger);
    for marker in TEMPORAL_MARKERS {
        if let Some(idx) = bare.find(marker) {
            return &bare[..idx];
        }
    }
    bare
}

const TEMPORAL_MARKERS: &[&str] = &["@t:", "@iso:", "@commit:"];

/// Extract per-IRI ledger identifiers and temporal-pin status from a SPARQL
/// query's `FROM` / `FROM NAMED` dataset clauses.
///
/// Uses the real SPARQL parser ([`fluree_db_sparql::parse_sparql`]) so
/// commented-out `FROM` clauses and string literals containing the word
/// don't trip detection. Prefixed-name IRIs are ignored (they can't be ledger
/// references without context resolution); only full `<iri>` literals
/// participate.
///
/// Returns `None` when the SPARQL fails to parse — validation defers to the
/// downstream parser so the user sees a clearer SPARQL-parse error rather than
/// a misleading "missing from" or false-negative collision check.
fn sparql_extract_from(sparql: &str) -> Option<Vec<ExtractedFrom>> {
    use fluree_db_sparql::ast::QueryBody;

    let parsed = fluree_db_sparql::parse_sparql(sparql);
    let ast = parsed.ast.as_ref()?;
    let dataset = match &ast.body {
        QueryBody::Select(q) => q.dataset.as_ref(),
        QueryBody::Construct(q) => q.dataset.as_ref(),
        QueryBody::Ask(q) => q.dataset.as_ref(),
        QueryBody::Describe(q) => q.dataset.as_ref(),
        QueryBody::Update(_) => None,
    };

    let Some(ds) = dataset else { return Some(Vec::new()); };

    let mut out = Vec::new();
    for iri in ds.default_graphs.iter().chain(ds.named_graphs.iter()) {
        if let Some(entry) = extract_sparql_iri(iri) {
            out.push(entry);
        }
    }
    if let Some(to) = ds.to_graph.as_ref() {
        if let Some(entry) = extract_sparql_iri(to) {
            out.push(entry);
        }
    }
    Some(out)
}

fn extract_sparql_iri(iri: &fluree_db_sparql::ast::Iri) -> Option<ExtractedFrom> {
    use fluree_db_sparql::ast::IriValue;
    // Prefixed names need context resolution to be ledger references; skip
    // them here. Full IRIs are taken at face value as ledger identifiers
    // (matching how the server interprets `FROM <ledger>` today).
    let value = match &iri.value {
        IriValue::Full(s) => s.as_ref(),
        IriValue::Prefixed { .. } => return None,
    };
    if value.is_empty() {
        return None;
    }
    Some(extract_jsonld_from_string(value))
}

// =============================================================================
// Merge helpers (used by dispatcher)
// =============================================================================

/// Compute the effective `@context` for a sub-query: shallow merge of envelope
/// context (default) with the sub-query's `@context` (wins on key conflict).
///
/// Sub-query `@context: null` is treated as an explicit reset — the result is
/// `None`. Absent sub-query context inherits the envelope context unchanged.
pub fn merged_context(
    envelope: Option<&JsonValue>,
    inner: Option<&JsonValue>,
) -> Option<JsonValue> {
    match (envelope, inner) {
        (None, None) => None,
        (Some(env), None) => Some(env.clone()),
        // Explicit reset: sub-query @context: null clears inheritance.
        (_, Some(JsonValue::Null)) => None,
        (None, Some(inner)) => Some(inner.clone()),
        (Some(env), Some(inner)) => Some(shallow_merge_objects(env, inner)),
    }
}

/// Compute the effective `opts` for a sub-query: shallow merge of envelope
/// opts with sub-query opts (sub-query wins on key conflict).
pub fn merged_opts(envelope: Option<&JsonValue>, inner: Option<&JsonValue>) -> Option<JsonValue> {
    match (envelope, inner) {
        (None, None) => None,
        (Some(v), None) | (None, Some(v)) => Some(v.clone()),
        (Some(env), Some(inner)) => Some(shallow_merge_objects(env, inner)),
    }
}

/// Shallow merge two JSON values, returning a new value. If both are objects,
/// `inner`'s keys override `outer`'s on conflict. If either is not an object,
/// `inner` wins entirely (matches "sub-query overrides envelope" semantics).
fn shallow_merge_objects(outer: &JsonValue, inner: &JsonValue) -> JsonValue {
    match (outer, inner) {
        (JsonValue::Object(o), JsonValue::Object(i)) => {
            let mut merged = o.clone();
            for (k, v) in i {
                merged.insert(k.clone(), v.clone());
            }
            JsonValue::Object(merged)
        }
        // Non-object inner replaces outer entirely.
        _ => inner.clone(),
    }
}

/// SPARQL directives derived from an envelope-merged JSON-LD context for
/// prepending to a SPARQL sub-query that lacks its own corresponding directive.
///
/// Per the agreed design rule, injection is **per-directive-class** and
/// **all-or-nothing**: envelope `PREFIX` declarations are injected only if the
/// SPARQL query has no `PREFIX` declarations; envelope `BASE` is injected only
/// if the SPARQL query has no `BASE`. The two directive classes are decided
/// independently.
#[derive(Debug, Default, Clone)]
pub struct SparqlContextDirectives {
    /// `PREFIX foo: <iri>` lines (no trailing newline; caller joins).
    pub prefixes: Vec<String>,
    /// Optional `BASE <iri>` value (just the IRI, caller wraps).
    pub base: Option<String>,
}

impl SparqlContextDirectives {
    /// Extract prefix-shaped term mappings and the optional `@base` from a
    /// merged JSON-LD context.
    ///
    /// To qualify for SPARQL `PREFIX` injection, a context entry must:
    /// - Have a key that's a valid SPARQL `PN_PREFIX` (ASCII letters/digits/
    ///   underscore/hyphen, starting with a letter or underscore).
    /// - Have a string value that's a **namespace IRI**: either contains a
    ///   URI-scheme `://` separator or ends with `#` / `/` (the conventional
    ///   namespace terminators).
    ///
    /// JSON-LD term *aliases* like `"name": "schema:name"` are CURIE mappings,
    /// not absolute namespaces, and don't qualify — emitting them as
    /// `PREFIX name: <schema:name>` would produce invalid SPARQL.
    /// JSON-LD term *definitions* (object values with `@id`/`@type`/
    /// `@container`) are likewise JSON-LD-only and dropped here, as is
    /// `@vocab` (no SPARQL equivalent).
    pub fn from_context(ctx: &JsonValue) -> Self {
        let mut out = Self::default();
        let Some(obj) = ctx.as_object() else {
            return out;
        };
        for (k, v) in obj {
            match k.as_str() {
                "@base" => {
                    if let Some(s) = v.as_str().filter(|s| is_absolute_iri(s)) {
                        out.base = Some(s.to_string());
                    }
                }
                // JSON-LD-only directives — silently dropped for SPARQL.
                "@vocab" | "@version" | "@language" | "@protected" | "@import" => {}
                _ if k.starts_with('@') => {}
                _ => {
                    let Some(iri) = v.as_str() else { continue };
                    if is_valid_pn_prefix(k) && is_namespace_iri(iri) {
                        out.prefixes.push(format!("PREFIX {k}: <{iri}>"));
                    }
                }
            }
        }
        out
    }
}

/// Conservative SPARQL `PN_PREFIX` check: ASCII letter or underscore, then
/// letters/digits/underscore/hyphen. The W3C grammar permits more (Unicode
/// letters, period, etc.) but the safe ASCII subset covers every common
/// vocabulary prefix in the wild without risking malformed output.
fn is_valid_pn_prefix(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Absolute-IRI check: starts with a URI scheme followed by `:`, with at
/// least one further character after the colon. Distinct from
/// [`is_namespace_iri`] in that it does not require `#`/`/` termination —
/// used for `@base` (which is typically a document URI without a trailing
/// separator).
fn is_absolute_iri(s: &str) -> bool {
    let Some((scheme, rest)) = s.split_once(':') else {
        return false;
    };
    if rest.is_empty() {
        return false;
    }
    let mut chars = scheme.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
}

/// Namespace-IRI check: an absolute IRI that either uses a hierarchical
/// scheme (`://`) or ends with the conventional namespace terminators (`#`
/// or `/`). This excludes JSON-LD CURIE aliases like `schema:name` while
/// accepting both standard HTTP namespaces and URN-style ones like
/// `urn:example:vocab#`.
fn is_namespace_iri(s: &str) -> bool {
    if !is_absolute_iri(s) {
        return false;
    }
    s.contains("://") || s.ends_with('#') || s.ends_with('/')
}

/// Prepend envelope-derived `PREFIX` / `BASE` directives to a SPARQL query
/// string, **per-directive-class and all-or-nothing**:
///
/// - Envelope prefixes are prepended only if `sparql` has no `PREFIX` declarations.
/// - Envelope `BASE` is prepended only if `sparql` has no `BASE` declaration.
pub fn apply_sparql_context(
    sparql: &str,
    directives: &SparqlContextDirectives,
) -> String {
    let has_prefix = sparql_has_directive(sparql, "PREFIX");
    let has_base = sparql_has_directive(sparql, "BASE");

    let mut prelude = String::new();
    if !has_base {
        if let Some(iri) = directives.base.as_deref() {
            prelude.push_str(&format!("BASE <{iri}>\n"));
        }
    }
    if !has_prefix {
        for p in &directives.prefixes {
            prelude.push_str(p);
            prelude.push('\n');
        }
    }

    if prelude.is_empty() {
        sparql.to_string()
    } else {
        prelude.push_str(sparql);
        prelude
    }
}

/// Identifier-character predicate for SPARQL keyword boundary detection.
fn is_sparql_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Case-insensitive detection of a SPARQL keyword directive at any
/// token-boundary position.
fn sparql_has_directive(sparql: &str, keyword: &str) -> bool {
    let lower_sparql = sparql.to_ascii_lowercase();
    let lower_keyword = keyword.to_ascii_lowercase();
    let bytes = lower_sparql.as_bytes();
    let kw = lower_keyword.as_bytes();
    let kn = kw.len();
    let mut i = 0;
    while i + kn <= bytes.len() {
        if &bytes[i..i + kn] == kw
            && (i == 0 || !is_sparql_ident_char(bytes[i - 1] as char))
            && (i + kn == bytes.len() || !is_sparql_ident_char(bytes[i + kn] as char))
        {
            return true;
        }
        i += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -------------------------------------------------------------------------
    // Deserialization smoke tests
    // -------------------------------------------------------------------------

    #[test]
    fn deserializes_minimal_envelope() {
        let body = json!({
            "queries": {
                "a": {
                    "language": "jsonld",
                    "query": { "from": "myledger", "select": {"?s": ["*"]}, "where": [] }
                }
            }
        });
        let req: MultiQueryRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.queries.len(), 1);
        assert!(req.as_of.is_none());
    }

    #[test]
    fn deserializes_full_envelope_with_iso_asof_and_mixed_languages() {
        let body = json!({
            "@context": { "schema": "http://schema.org/" },
            "asOf": "2024-01-01T12:00:00Z",
            "opts": { "meta": true },
            "queries": {
                "alice": {
                    "language": "jsonld",
                    "query": { "from": "ledgerA", "select": {"?s": ["*"]}, "where": [] }
                },
                "brian": {
                    "language": "sparql",
                    "query": "SELECT ?x FROM <ledgerB> WHERE { ?x ?p ?o }"
                }
            }
        });
        let req: MultiQueryRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.queries.len(), 2);
        assert!(matches!(req.as_of, Some(AsOf::Iso(_))));
        assert_eq!(req.queries["alice"].language, SubqueryLanguage::JsonLd);
        assert_eq!(req.queries["brian"].language, SubqueryLanguage::Sparql);
    }

    #[test]
    fn deserializes_json_ld_alias_for_language() {
        let body = json!({
            "queries": {
                "a": {
                    "language": "json-ld",
                    "query": { "from": "myledger", "select": {"?s": ["*"]}, "where": [] }
                }
            }
        });
        let req: MultiQueryRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.queries["a"].language, SubqueryLanguage::JsonLd);
    }

    #[test]
    fn deserializes_integer_asof() {
        let body = json!({
            "asOf": 42,
            "queries": {
                "a": {
                    "language": "jsonld",
                    "query": { "from": "myledger", "select": {"?s": ["*"]}, "where": [] }
                }
            }
        });
        let req: MultiQueryRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.as_of, Some(AsOf::T(42)));
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    fn jsonld(from: &str) -> MultiQuerySubquery {
        MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "from": from, "select": {"?s": ["*"]}, "where": [] }),
            opts: None,
        }
    }

    fn sparql(body: &str) -> MultiQuerySubquery {
        MultiQuerySubquery {
            language: SubqueryLanguage::Sparql,
            query: json!(body),
            opts: None,
        }
    }

    fn envelope_with(
        queries: &[(&str, MultiQuerySubquery)],
        as_of: Option<AsOf>,
    ) -> MultiQueryRequest {
        let mut map = indexmap::IndexMap::new();
        for (alias, sq) in queries {
            map.insert((*alias).to_string(), sq.clone());
        }
        MultiQueryRequest {
            context: None,
            as_of,
            opts: None,
            queries: map,
        }
    }

    #[test]
    fn rejects_empty_envelope() {
        let req = MultiQueryRequest {
            context: None,
            as_of: None,
            opts: None,
            queries: indexmap::IndexMap::new(),
        };
        assert!(matches!(
            validate_envelope(&req, &MultiQueryBounds::DEFAULT),
            Err(MultiQueryValidationError::EmptyEnvelope)
        ));
    }

    #[test]
    fn rejects_too_many_queries() {
        let mut map = indexmap::IndexMap::new();
        for i in 0..10 {
            map.insert(format!("q{i}"), jsonld("myledger"));
        }
        let req = MultiQueryRequest {
            context: None,
            as_of: None,
            opts: None,
            queries: map,
        };
        let bounds = MultiQueryBounds {
            max_queries: 5,
            ..MultiQueryBounds::DEFAULT
        };
        assert!(matches!(
            validate_envelope(&req, &bounds),
            Err(MultiQueryValidationError::TooManyQueries { actual: 10, limit: 5 })
        ));
    }

    #[test]
    fn rejects_too_many_distinct_ledgers() {
        let req = envelope_with(
            &[
                ("a", jsonld("ledgerA")),
                ("b", jsonld("ledgerB")),
                ("c", jsonld("ledgerC")),
            ],
            None,
        );
        let bounds = MultiQueryBounds {
            max_distinct_ledgers: 2,
            ..MultiQueryBounds::DEFAULT
        };
        assert!(matches!(
            validate_envelope(&req, &bounds),
            Err(MultiQueryValidationError::TooManyDistinctLedgers { actual: 3, limit: 2 })
        ));
    }

    #[test]
    fn rejects_integer_asof_with_multi_ledger() {
        let req = envelope_with(
            &[("a", jsonld("ledgerA")), ("b", jsonld("ledgerB"))],
            Some(AsOf::T(42)),
        );
        assert!(matches!(
            validate_envelope(&req, &MultiQueryBounds::DEFAULT),
            Err(MultiQueryValidationError::AsOfIntegerMultiLedger { distinct: 2 })
        ));
    }

    #[test]
    fn accepts_integer_asof_with_single_ledger() {
        let req = envelope_with(&[("a", jsonld("ledgerA"))], Some(AsOf::T(42)));
        let distinct = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap();
        assert_eq!(distinct.len(), 1);
        assert!(distinct.contains("ledgerA"));
    }

    #[test]
    fn rejects_jsonld_temporal_pin_under_envelope_asof() {
        let req = envelope_with(
            &[("a", jsonld("ledgerA@t:42"))],
            Some(AsOf::Iso("2024-01-01T00:00:00Z".into())),
        );
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::AsOfCollision { .. }));
    }

    #[test]
    fn rejects_sparql_temporal_pin_under_envelope_asof() {
        let req = envelope_with(
            &[(
                "b",
                sparql("SELECT ?x FROM <ledgerA@t:42> WHERE { ?x ?p ?o }"),
            )],
            Some(AsOf::Iso("2024-01-01T00:00:00Z".into())),
        );
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::AsOfCollision { .. }));
    }

    #[test]
    fn rejects_inner_t_field_under_envelope_asof() {
        let mut sq = jsonld("ledgerA");
        sq.query
            .as_object_mut()
            .unwrap()
            .insert("t".into(), json!(99));
        let req = envelope_with(&[("a", sq)], Some(AsOf::Iso("2024-01-01T00:00:00Z".into())));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::AsOfCollision { .. }));
    }

    #[test]
    fn allows_temporal_pin_when_envelope_has_no_asof() {
        let req = envelope_with(&[("a", jsonld("ledgerA@t:42"))], None);
        validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap();
    }

    #[test]
    fn rejects_opts_t_in_subquery() {
        let mut sq = jsonld("ledgerA");
        sq.opts = Some(json!({ "t": 42 }));
        let req = envelope_with(&[("a", sq)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::OptsTNotAllowed { .. }));
    }

    #[test]
    fn rejects_missing_from_jsonld() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({ "select": {"?s": ["*"]}, "where": [] }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::MissingFrom { .. }));
    }

    #[test]
    fn rejects_missing_from_sparql() {
        let req = envelope_with(
            &[("a", sparql("SELECT ?x WHERE { ?x ?p ?o }"))],
            None,
        );
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::MissingFrom { .. }));
    }

    #[test]
    fn rejects_envelope_max_concurrency_over_limit() {
        let mut req = envelope_with(&[("a", jsonld("ledgerA"))], None);
        req.opts = Some(json!({ "maxConcurrency": 999 }));
        let bounds = MultiQueryBounds {
            max_concurrency: 16,
            ..MultiQueryBounds::DEFAULT
        };
        let err = validate_envelope(&req, &bounds).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::MaxConcurrencyExceeded { .. }));
    }

    #[test]
    fn rejects_envelope_timeout_over_limit() {
        let mut req = envelope_with(&[("a", jsonld("ledgerA"))], None);
        req.opts = Some(json!({ "timeoutMs": 999_999 }));
        let bounds = MultiQueryBounds {
            max_envelope_timeout_ms: 60_000,
            ..MultiQueryBounds::DEFAULT
        };
        let err = validate_envelope(&req, &bounds).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::TimeoutExceeded { .. }));
    }

    #[test]
    fn rejects_jsonld_body_not_object() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!("not an object"),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::JsonLdBodyNotObject { .. }));
    }

    #[test]
    fn rejects_sparql_body_not_string() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::Sparql,
            query: json!({ "not": "a string" }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::SparqlBodyNotString { .. }));
    }

    #[test]
    fn distinct_ledger_set_strips_temporal_suffix() {
        let req = envelope_with(
            &[
                ("a", jsonld("ledgerA@t:42")),
                ("b", jsonld("ledgerA@t:99")),
                ("c", jsonld("ledgerA")),
            ],
            None,
        );
        let distinct = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap();
        assert_eq!(distinct.len(), 1);
        assert!(distinct.contains("ledgerA"));
    }

    #[test]
    fn distinct_ledger_set_counts_sparql_from_clauses() {
        let req = envelope_with(
            &[
                ("a", sparql("SELECT ?x FROM <ledgerA> WHERE { ?x ?p ?o }")),
                (
                    "b",
                    sparql("SELECT ?x FROM <ledgerB> FROM NAMED <ledgerC> WHERE { ?x ?p ?o }"),
                ),
            ],
            None,
        );
        let distinct = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap();
        assert_eq!(distinct.len(), 3);
        assert!(distinct.contains("ledgerA"));
        assert!(distinct.contains("ledgerB"));
        assert!(distinct.contains("ledgerC"));
    }

    // -------------------------------------------------------------------------
    // Merge helpers
    // -------------------------------------------------------------------------

    #[test]
    fn merged_context_returns_envelope_when_inner_absent() {
        let env = json!({ "schema": "http://schema.org/" });
        let merged = merged_context(Some(&env), None).unwrap();
        assert_eq!(merged, env);
    }

    #[test]
    fn merged_context_inner_extends_outer_with_inner_winning() {
        let env = json!({ "schema": "http://schema.org/", "ex": "http://example.org/" });
        let inner = json!({ "ex": "http://override.example.org/", "name": "schema:name" });
        let merged = merged_context(Some(&env), Some(&inner)).unwrap();
        let obj = merged.as_object().unwrap();
        assert_eq!(obj["schema"], "http://schema.org/");
        assert_eq!(obj["ex"], "http://override.example.org/");
        assert_eq!(obj["name"], "schema:name");
    }

    #[test]
    fn merged_context_null_inner_resets_to_none() {
        let env = json!({ "schema": "http://schema.org/" });
        let merged = merged_context(Some(&env), Some(&JsonValue::Null));
        assert!(merged.is_none());
    }

    #[test]
    fn merged_opts_shallow_merges_with_inner_winning() {
        let env = json!({ "meta": true, "fuel": 10000 });
        let inner = json!({ "fuel": 1000, "policy": "alice" });
        let merged = merged_opts(Some(&env), Some(&inner)).unwrap();
        let obj = merged.as_object().unwrap();
        assert_eq!(obj["meta"], true);
        assert_eq!(obj["fuel"], 1000);
        assert_eq!(obj["policy"], "alice");
    }

    // -------------------------------------------------------------------------
    // SPARQL context injection
    // -------------------------------------------------------------------------

    #[test]
    fn sparql_directives_extract_prefixes_and_base() {
        let ctx = json!({
            "schema": "http://schema.org/",
            "ex":     "http://example.org/",
            "@base":  "http://example.org/data/",
            "@vocab": "http://example.org/",
            "name":   { "@id": "schema:name", "@type": "xsd:string" }
        });
        let directives = SparqlContextDirectives::from_context(&ctx);
        assert_eq!(directives.base.as_deref(), Some("http://example.org/data/"));
        assert_eq!(directives.prefixes.len(), 2);
        // Term definitions and @vocab dropped.
        assert!(!directives
            .prefixes
            .iter()
            .any(|p| p.contains("name") || p.contains("@vocab")));
    }

    #[test]
    fn apply_sparql_context_injects_when_no_directives_present() {
        let directives = SparqlContextDirectives {
            prefixes: vec!["PREFIX schema: <http://schema.org/>".into()],
            base: Some("http://example.org/".into()),
        };
        let sparql = "SELECT ?x WHERE { ?x schema:name 'Alice' }";
        let out = apply_sparql_context(sparql, &directives);
        assert!(out.contains("PREFIX schema:"));
        assert!(out.contains("BASE <http://example.org/>"));
        assert!(out.ends_with(sparql));
    }

    #[test]
    fn apply_sparql_context_skips_prefixes_when_query_has_any() {
        let directives = SparqlContextDirectives {
            prefixes: vec!["PREFIX schema: <http://schema.org/>".into()],
            base: Some("http://example.org/".into()),
        };
        let sparql = "PREFIX ex: <http://example.org/>\nSELECT ?x WHERE { ?x ?p ?o }";
        let out = apply_sparql_context(sparql, &directives);
        // BASE still injected (different directive class).
        assert!(out.contains("BASE <http://example.org/>"));
        // Envelope prefix is NOT injected — sub-query has its own PREFIX directives.
        assert_eq!(out.matches("PREFIX schema:").count(), 0);
    }

    #[test]
    fn apply_sparql_context_skips_base_when_query_has_base() {
        let directives = SparqlContextDirectives {
            prefixes: vec!["PREFIX schema: <http://schema.org/>".into()],
            base: Some("http://example.org/".into()),
        };
        let sparql = "BASE <http://other.example.org/>\nSELECT ?x WHERE { ?x ?p ?o }";
        let out = apply_sparql_context(sparql, &directives);
        // Envelope base is NOT injected — sub-query has its own BASE.
        assert_eq!(out.matches("BASE <http://example.org/>").count(), 0);
        // PREFIX still injected.
        assert!(out.contains("PREFIX schema:"));
    }

    #[test]
    fn apply_sparql_context_noop_when_query_has_both() {
        let directives = SparqlContextDirectives {
            prefixes: vec!["PREFIX schema: <http://schema.org/>".into()],
            base: Some("http://example.org/".into()),
        };
        let sparql = "PREFIX ex: <http://example.org/>\nBASE <http://other.example.org/>\nSELECT ?x WHERE { ?x ?p ?o }";
        let out = apply_sparql_context(sparql, &directives);
        assert_eq!(out, sparql);
    }

    // -------------------------------------------------------------------------
    // Temporal-pin scanner sanity (AST-based)
    // -------------------------------------------------------------------------

    fn pin_count(extracted: &[ExtractedFrom]) -> usize {
        extracted.iter().filter(|e| e.pin_location.is_some()).count()
    }

    #[test]
    fn sparql_temporal_pin_detected_in_from() {
        let cases = [
            "SELECT * FROM <ledger@t:42> WHERE { ?x ?p ?o }",
            "SELECT * FROM NAMED <ledger@iso:2024-01-01T00:00:00Z> WHERE { ?x ?p ?o }",
            "SELECT * FROM <ledger@commit:abc123> WHERE { ?x ?p ?o }",
        ];
        for sparql in cases {
            let extracted =
                sparql_extract_from(sparql).expect("SPARQL parses");
            assert!(
                pin_count(&extracted) >= 1,
                "expected temporal pin in: {sparql} (got {extracted:?})"
            );
        }
    }

    #[test]
    fn sparql_temporal_pin_not_detected_in_plain_query() {
        let extracted = sparql_extract_from("SELECT * FROM <ledger> WHERE { ?x ?p ?o }")
            .expect("SPARQL parses");
        assert_eq!(pin_count(&extracted), 0);

        let extracted = sparql_extract_from(
            "PREFIX ex: <http://example.org/> SELECT * FROM <ledger> WHERE { ?x ex:name ?n }",
        )
        .expect("SPARQL parses");
        assert_eq!(pin_count(&extracted), 0);
    }

    #[test]
    fn sparql_temporal_marker_inside_string_literal_not_flagged() {
        // The marker only appears inside a string literal (not a FROM IRI),
        // so the AST-based detector should not flag it — substring scans
        // would false-positive here.
        let sparql = r#"SELECT ?x FROM <ledgerA> WHERE { ?x <http://example.org/note> "see @t:42 in the docs" }"#;
        let extracted = sparql_extract_from(sparql).expect("SPARQL parses");
        assert_eq!(pin_count(&extracted), 0);
    }

    #[test]
    fn sparql_extract_returns_none_on_parse_failure() {
        // Syntactically invalid SPARQL — extractor returns None so the
        // downstream parser produces the user-facing error rather than
        // validation guessing.
        let extracted = sparql_extract_from("not a SPARQL query");
        assert!(extracted.is_none());
    }

    // -------------------------------------------------------------------------
    // JSON-LD object-form `from` (review fix High #2)
    // -------------------------------------------------------------------------

    #[test]
    fn jsonld_object_from_with_explicit_t_field_flagged_under_envelope_asof() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": { "@id": "ledgerA", "t": 42 },
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], Some(AsOf::Iso("2024-01-01T00:00:00Z".into())));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(
            matches!(err, MultiQueryValidationError::AsOfCollision { ref location, .. } if location.contains("'t' field")),
            "got: {err:?}"
        );
    }

    #[test]
    fn jsonld_object_from_with_explicit_at_field_flagged_under_envelope_asof() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": { "id": "ledgerA", "at": "commit:abc123" },
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], Some(AsOf::Iso("2024-01-01T00:00:00Z".into())));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(
            matches!(err, MultiQueryValidationError::AsOfCollision { ref location, .. } if location.contains("'at' field")),
            "got: {err:?}"
        );
    }

    #[test]
    fn jsonld_object_from_in_array_with_t_field_flagged() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": [
                    "ledgerA",
                    { "@id": "ledgerB", "t": 99 }
                ],
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], Some(AsOf::Iso("2024-01-01T00:00:00Z".into())));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::AsOfCollision { .. }));
    }

    #[test]
    fn jsonld_object_from_without_explicit_pin_accepted_under_envelope_asof() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": { "@id": "ledgerA", "alias": "primary" },
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], Some(AsOf::Iso("2024-01-01T00:00:00Z".into())));
        let distinct = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap();
        assert!(distinct.contains("ledgerA"));
    }

    #[test]
    fn jsonld_object_from_with_id_containing_suffix_still_flagged() {
        // Suffix in the identifier string — same collision as string form.
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": { "@id": "ledgerA@t:42" },
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], Some(AsOf::Iso("2024-01-01T00:00:00Z".into())));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(err, MultiQueryValidationError::AsOfCollision { .. }));
    }

    // -------------------------------------------------------------------------
    // Per-subquery emptiness (review fix High #1)
    // -------------------------------------------------------------------------

    #[test]
    fn rejects_second_subquery_with_empty_from_array_after_valid_first() {
        let valid = jsonld("ledgerA");
        let empty = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": [],
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", valid), ("b", empty)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        match err {
            MultiQueryValidationError::MissingFrom { ref alias } => assert_eq!(alias, "b"),
            other => panic!("expected MissingFrom on alias 'b', got {other:?}"),
        }
    }

    #[test]
    fn rejects_subquery_with_object_from_missing_id() {
        let valid = jsonld("ledgerA");
        let no_id = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": { "alias": "primary" },
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", valid), ("b", no_id)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        match err {
            MultiQueryValidationError::MissingFrom { ref alias } => assert_eq!(alias, "b"),
            other => panic!("expected MissingFrom on alias 'b', got {other:?}"),
        }
    }

    // -------------------------------------------------------------------------
    // SPARQL prefix-shape filtering (review fix Medium #2)
    // -------------------------------------------------------------------------

    #[test]
    fn sparql_directives_skip_curie_alias_term_mappings() {
        // "name": "schema:name" is a JSON-LD term alias, not a namespace IRI.
        // Emitting `PREFIX name: <schema:name>` would produce invalid SPARQL.
        let ctx = json!({
            "schema": "http://schema.org/",
            "name":   "schema:name"
        });
        let directives = SparqlContextDirectives::from_context(&ctx);
        assert_eq!(directives.prefixes.len(), 1);
        assert!(directives.prefixes[0].contains("PREFIX schema:"));
        assert!(!directives.prefixes.iter().any(|p| p.contains("name:")));
    }

    #[test]
    fn sparql_directives_skip_invalid_prefix_names() {
        // Key with characters not in the conservative PN_PREFIX subset.
        let ctx = json!({
            "1abc":      "http://example.org/",  // starts with digit
            "with space": "http://example.org/", // contains space
            "ok":        "http://example.org/"
        });
        let directives = SparqlContextDirectives::from_context(&ctx);
        assert_eq!(directives.prefixes.len(), 1);
        assert!(directives.prefixes[0].contains("PREFIX ok:"));
    }

    #[test]
    fn sparql_directives_accept_urn_namespace_with_hash_terminator() {
        let ctx = json!({
            "ex": "urn:example:vocab#"
        });
        let directives = SparqlContextDirectives::from_context(&ctx);
        assert_eq!(directives.prefixes.len(), 1);
        assert!(directives.prefixes[0].contains("urn:example:vocab#"));
    }

    #[test]
    fn sparql_directives_skip_non_absolute_base() {
        let ctx = json!({
            "@base": "/relative/path"
        });
        let directives = SparqlContextDirectives::from_context(&ctx);
        assert!(directives.base.is_none());
    }

    // -------------------------------------------------------------------------
    // History queries rejected (review fix High #2)
    // -------------------------------------------------------------------------

    #[test]
    fn jsonld_history_query_with_to_field_rejected() {
        let sq = MultiQuerySubquery {
            language: SubqueryLanguage::JsonLd,
            query: json!({
                "from": "ledgerA@t:1",
                "to":   "ledgerA@t:latest",
                "select": {"?s": ["*"]},
                "where": []
            }),
            opts: None,
        };
        let req = envelope_with(&[("a", sq)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        match err {
            MultiQueryValidationError::HistoryQueryInEnvelope { ref alias } => {
                assert_eq!(alias, "a")
            }
            other => panic!("expected HistoryQueryInEnvelope, got {other:?}"),
        }
    }

    #[test]
    fn sparql_history_range_query_rejected() {
        let sq = sparql(
            "SELECT ?x FROM <ledgerA@t:1> TO <ledgerA@t:latest> WHERE { ?x ?p ?o }",
        );
        let req = envelope_with(&[("a", sq)], None);
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(
            err,
            MultiQueryValidationError::HistoryQueryInEnvelope { .. }
        ));
    }

    // -------------------------------------------------------------------------
    // Envelope-level max-fuel rejected (review fix Medium #1)
    // -------------------------------------------------------------------------

    #[test]
    fn rejects_envelope_max_fuel() {
        let mut req = envelope_with(&[("a", jsonld("ledgerA"))], None);
        req.opts = Some(json!({ "max-fuel": 1000 }));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(
            err,
            MultiQueryValidationError::EnvelopeFuelBudgetUnsupported
        ));
    }

    #[test]
    fn rejects_envelope_max_fuel_snake_case() {
        let mut req = envelope_with(&[("a", jsonld("ledgerA"))], None);
        req.opts = Some(json!({ "max_fuel": 1000 }));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(
            err,
            MultiQueryValidationError::EnvelopeFuelBudgetUnsupported
        ));
    }

    #[test]
    fn rejects_envelope_max_fuel_camel_case() {
        let mut req = envelope_with(&[("a", jsonld("ledgerA"))], None);
        req.opts = Some(json!({ "maxFuel": 1000 }));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(
            err,
            MultiQueryValidationError::EnvelopeFuelBudgetUnsupported
        ));
    }

    #[test]
    fn per_subquery_max_fuel_still_allowed() {
        let mut sq = jsonld("ledgerA");
        sq.opts = Some(json!({ "max-fuel": 1000 }));
        let req = envelope_with(&[("a", sq)], None);
        validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap();
    }

    // -------------------------------------------------------------------------
    // maxConcurrency: 0 rejected (review fix Medium #2)
    // -------------------------------------------------------------------------

    #[test]
    fn rejects_max_concurrency_zero() {
        let mut req = envelope_with(&[("a", jsonld("ledgerA"))], None);
        req.opts = Some(json!({ "maxConcurrency": 0 }));
        let err = validate_envelope(&req, &MultiQueryBounds::DEFAULT).unwrap_err();
        assert!(matches!(
            err,
            MultiQueryValidationError::MaxConcurrencyZero
        ));
    }
}
