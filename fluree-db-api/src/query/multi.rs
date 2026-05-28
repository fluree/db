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
    #[error(
        "opts.timeoutMs = {value} exceeds server limit {limit}"
    )]
    TimeoutExceeded { value: u64, limit: u64 },
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

    for ledger_id in jsonld_from_ledger_strings(from) {
        if envelope_pinned && string_has_temporal_marker(&ledger_id) {
            return Err(MultiQueryValidationError::AsOfCollision {
                alias: alias.to_string(),
                location: format!("'from' contains temporal pin: {ledger_id}"),
            });
        }
        distinct.insert(strip_temporal_suffix(&ledger_id).to_string());
    }

    if distinct.is_empty() {
        // `from` was present but yielded no ledger identifiers (e.g., empty
        // array or non-string entries) — treat as missing.
        return Err(MultiQueryValidationError::MissingFrom {
            alias: alias.to_string(),
        });
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

    if envelope_pinned && sparql_has_temporal_pin(body) {
        return Err(MultiQueryValidationError::AsOfCollision {
            alias: alias.to_string(),
            location: "SPARQL FROM <...@t:...> clause".to_string(),
        });
    }

    let from_ledgers = sparql_from_ledgers(body);
    if from_ledgers.is_empty() {
        return Err(MultiQueryValidationError::MissingFrom {
            alias: alias.to_string(),
        });
    }

    for ledger in from_ledgers {
        distinct.insert(strip_temporal_suffix(&ledger).to_string());
    }

    Ok(())
}

// =============================================================================
// Ledger / temporal-pin extraction helpers
// =============================================================================

/// Pull ledger identifier strings out of a JSON-LD `from` value.
///
/// `from` may be a string, an array of strings, or an array of objects with
/// `@id`. Non-string entries are skipped silently — the language parsers
/// produce a clearer downstream error.
fn jsonld_from_ledger_strings(from: &JsonValue) -> Vec<String> {
    let mut out = Vec::new();
    match from {
        JsonValue::String(s) if !s.is_empty() => out.push(s.clone()),
        JsonValue::Array(items) => {
            for item in items {
                match item {
                    JsonValue::String(s) if !s.is_empty() => out.push(s.clone()),
                    JsonValue::Object(obj) => {
                        if let Some(id) = obj.get("@id").and_then(JsonValue::as_str) {
                            if !id.is_empty() {
                                out.push(id.to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    out
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

/// Lightweight detector: does this string carry a Fluree temporal pin?
fn string_has_temporal_marker(s: &str) -> bool {
    TEMPORAL_MARKERS.iter().any(|m| s.contains(m))
}

const TEMPORAL_MARKERS: &[&str] = &["@t:", "@iso:", "@commit:"];

/// Detect a temporal pin anywhere inside angle-bracketed IRIs in a SPARQL
/// query string.
///
/// The Fluree temporal markers (`@t:`, `@iso:`, `@commit:`) are extensions that
/// only appear in IRIs intended as time-travel pins; any occurrence inside
/// `<...>` is treated as a pin for collision purposes.
fn sparql_has_temporal_pin(sparql: &str) -> bool {
    let mut chars = sparql.char_indices();
    while let Some((_, c)) = chars.next() {
        if c == '<' {
            // Scan until '>' for any temporal marker.
            let mut iri = String::new();
            for (_, ic) in chars.by_ref() {
                if ic == '>' {
                    break;
                }
                iri.push(ic);
            }
            if string_has_temporal_marker(&iri) {
                return true;
            }
        }
    }
    false
}

/// Extract ledger identifiers from `FROM <iri>` and `FROM NAMED <iri>` clauses
/// in a SPARQL query string.
///
/// This is a coarse scanner — sufficient for distinct-ledger counting, not a
/// substitute for the SPARQL parser. Anything inside `<...>` immediately
/// following a `FROM` keyword (with optional `NAMED`) is treated as a ledger
/// reference.
fn sparql_from_ledgers(sparql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = sparql.as_bytes();
    let lower = sparql.to_ascii_lowercase();
    let lower_bytes = lower.as_bytes();
    let mut i = 0;

    while i + 4 <= lower_bytes.len() {
        if &lower_bytes[i..i + 4] == b"from"
            && (i == 0 || !is_sparql_ident_char(lower_bytes[i - 1] as char))
            && (i + 4 == lower_bytes.len() || !is_sparql_ident_char(lower_bytes[i + 4] as char))
        {
            // Found a FROM keyword. Skip optional "NAMED" and whitespace.
            let mut j = i + 4;
            j = skip_ws(lower_bytes, j);
            if j + 5 <= lower_bytes.len() && &lower_bytes[j..j + 5] == b"named" {
                let after = j + 5;
                if after == lower_bytes.len() || !is_sparql_ident_char(lower_bytes[after] as char) {
                    j = after;
                    j = skip_ws(lower_bytes, j);
                }
            }
            if j < bytes.len() && bytes[j] == b'<' {
                // Capture IRI up to matching '>'.
                let start = j + 1;
                if let Some(end_off) = bytes[start..].iter().position(|&b| b == b'>') {
                    let iri = &sparql[start..start + end_off];
                    if !iri.is_empty() {
                        out.push(iri.to_string());
                    }
                    i = start + end_off + 1;
                    continue;
                }
            }
        }
        i += 1;
    }

    out
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn is_sparql_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
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
    /// "Prefix-shaped" means a top-level key whose value is a string IRI; term
    /// definitions with `@id`/`@type`/`@container` are JSON-LD-only and dropped
    /// for SPARQL purposes, as is `@vocab` (no SPARQL equivalent).
    pub fn from_context(ctx: &JsonValue) -> Self {
        let mut out = Self::default();
        let Some(obj) = ctx.as_object() else {
            return out;
        };
        for (k, v) in obj {
            match k.as_str() {
                "@base" => {
                    if let Some(s) = v.as_str() {
                        out.base = Some(s.to_string());
                    }
                }
                // JSON-LD-only directives — silently dropped for SPARQL.
                "@vocab" | "@version" | "@language" | "@protected" | "@import" => {}
                _ if k.starts_with('@') => {}
                _ => {
                    if let Some(iri) = v.as_str() {
                        out.prefixes.push(format!("PREFIX {k}: <{iri}>"));
                    }
                }
            }
        }
        out
    }
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
    // Temporal-pin scanner sanity
    // -------------------------------------------------------------------------

    #[test]
    fn sparql_temporal_pin_detected_in_from() {
        assert!(sparql_has_temporal_pin("SELECT * FROM <ledger@t:42> WHERE { }"));
        assert!(sparql_has_temporal_pin(
            "SELECT * FROM NAMED <ledger@iso:2024-01-01T00:00:00Z> WHERE { }"
        ));
        assert!(sparql_has_temporal_pin(
            "SELECT * FROM <ledger@commit:abc123> WHERE { }"
        ));
    }

    #[test]
    fn sparql_temporal_pin_not_detected_in_plain_query() {
        assert!(!sparql_has_temporal_pin(
            "SELECT * FROM <ledger> WHERE { ?x ?p ?o }"
        ));
        assert!(!sparql_has_temporal_pin(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { ?x ex:name ?n }"
        ));
    }
}
