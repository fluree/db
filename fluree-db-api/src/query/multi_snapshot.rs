//! Multi-query envelope snapshot resolution and per-sub-query application.
//!
//! Two pieces:
//!
//! - [`resolve_envelope_snapshot`] runs once per envelope, before sub-query
//!   dispatch. It walks the distinct ledger set (provided by validation) and
//!   produces an [`EnvelopeSnapshot`] mapping each ledger to a concrete `t`,
//!   honoring the envelope `asOf` (integer, ISO timestamp, or omitted).
//! - [`apply_snapshot_to_jsonld`] / [`apply_snapshot_to_sparql`] rewrite each
//!   sub-query in place so its `from` clauses carry the resolved `t`. The
//!   collision-check in validation already ensures no sub-query carries its
//!   own temporal pin when the envelope sets `asOf`, so application is
//!   non-conflicting.
//!
//! The snapshot moment is recorded once at envelope-entry — the "shared time
//! resolution" the envelope contract promises. It is **not** distributed
//! atomicity: per-ledger `t` values represent each ledger's latest commit
//! at-or-before the moment, and ledger commit clocks are not synchronized.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::query::multi::AsOf;
use crate::{time_resolve, ApiError, Fluree, Result};

/// Resolved per-ledger `t` map for an envelope, plus the wall-clock moment it
/// represents (echoed back to the client).
///
/// Built by [`resolve_envelope_snapshot`] before dispatch and consumed by the
/// per-sub-query appliers to inject the right pin into each sub-query body.
#[derive(Debug, Clone)]
pub struct EnvelopeSnapshot {
    /// ISO 8601 wall-clock moment that produced the per-ledger map. Echoed in
    /// the response so clients can replay against the same moment. `None` only
    /// when the envelope used an explicit integer `asOf` (which is necessarily
    /// single-ledger and has no shared wall-clock interpretation).
    pub as_of: Option<String>,
    /// Per-ledger numeric `t`. For each ledger referenced by the envelope,
    /// this is the value every unpinned sub-query gets pinned to during
    /// application.
    pub ledgers: HashMap<String, i64>,
}

/// Failure modes specific to snapshot resolution. These map to a 5xx envelope
/// response — they indicate an infrastructure-side failure to honor a
/// syntactically-valid `asOf`.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeSnapshotError {
    #[error("invalid ISO 8601 timestamp for asOf: {iso} ({source})")]
    InvalidIso {
        iso: String,
        source: chrono::ParseError,
    },
    #[error("failed to load ledger '{ledger}' for snapshot resolution: {source}")]
    LedgerLoad { ledger: String, source: ApiError },
    #[error("failed to resolve asOf for ledger '{ledger}': {source}")]
    PerLedgerResolve { ledger: String, source: ApiError },
}

impl From<EnvelopeSnapshotError> for ApiError {
    fn from(err: EnvelopeSnapshotError) -> Self {
        ApiError::internal(err.to_string())
    }
}

/// Resolve the envelope's `asOf` to a per-ledger `t` map.
///
/// Behaviour by `asOf` variant:
/// - `Some(AsOf::T(t))`: the (single) ledger pins to `t` directly. Validation
///   already rejected multi-ledger envelopes paired with an integer `asOf`.
/// - `Some(AsOf::Iso(iso))`: each distinct ledger resolves the timestamp via
///   [`time_resolve::datetime_to_t`] against its own current snapshot.
/// - `None`: server-now is captured once and each distinct ledger pins to its
///   current `t`. The `as_of` field of the returned snapshot is populated
///   with that captured moment so the response is reproducible.
///
/// Per-ledger work runs sequentially in v1; the connection-level ledger cache
/// ([`Fluree::ledger_cached`]) coalesces repeated loads of the same ledger so
/// the bounded fan-out is acceptable. Parallel resolution can come later if
/// large envelopes show pressure here.
pub async fn resolve_envelope_snapshot(
    fluree: &Fluree,
    distinct_ledgers: &BTreeSet<String>,
    as_of: Option<&AsOf>,
) -> Result<EnvelopeSnapshot> {
    let (echoed_as_of, target_iso) = match as_of {
        Some(AsOf::Iso(iso)) => (Some(iso.clone()), Some(iso.clone())),
        Some(AsOf::T(_)) => (None, None),
        None => {
            // Server-resolved "now" — captured once, applied uniformly. We
            // echo this so the client knows exactly when the envelope ran.
            let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            (Some(now), None)
        }
    };

    let mut ledgers: HashMap<String, i64> = HashMap::with_capacity(distinct_ledgers.len());

    for ledger_id in distinct_ledgers {
        let t = match (&as_of, target_iso.as_deref()) {
            // Integer t: pin directly. The single-ledger invariant is
            // enforced at validation; if a caller bypasses validation we'd
            // pin every ledger to the same integer, which is the documented
            // worst-case but still well-defined.
            (Some(AsOf::T(t)), _) => *t,
            (_, Some(iso)) => resolve_iso_to_t(fluree, ledger_id, iso).await?,
            (None, None) => resolve_current_t(fluree, ledger_id).await?,
            // AsOf::T paired with an ISO target is unreachable.
            (Some(AsOf::Iso(_)), None) => unreachable!("ISO branch sets target_iso"),
        };
        ledgers.insert(ledger_id.clone(), t);
    }

    Ok(EnvelopeSnapshot {
        as_of: echoed_as_of,
        ledgers,
    })
}

async fn resolve_current_t(fluree: &Fluree, ledger_id: &str) -> Result<i64> {
    let handle =
        fluree
            .ledger_cached(ledger_id)
            .await
            .map_err(|e| EnvelopeSnapshotError::LedgerLoad {
                ledger: ledger_id.to_string(),
                source: e,
            })?;
    Ok(handle.t().await)
}

async fn resolve_iso_to_t(fluree: &Fluree, ledger_id: &str, iso: &str) -> Result<i64> {
    let handle =
        fluree
            .ledger_cached(ledger_id)
            .await
            .map_err(|e| EnvelopeSnapshotError::LedgerLoad {
                ledger: ledger_id.to_string(),
                source: e,
            })?;
    let view = handle.snapshot().await;
    let ledger = view.to_ledger_state();
    let current_t = ledger.t();

    let dt =
        DateTime::parse_from_rfc3339(iso).map_err(|source| EnvelopeSnapshotError::InvalidIso {
            iso: iso.to_string(),
            source,
        })?;
    // ledger#time flakes store epoch milliseconds; mirror the rounding rule
    // from load_graph_db_at so sub-millisecond ISO precision doesn't push us
    // off-by-one before the intended commit (especially around genesis).
    let mut target_epoch_ms = dt.timestamp_millis();
    if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
        target_epoch_ms += 1;
    }

    let resolved = time_resolve::datetime_to_t(
        &ledger.snapshot,
        Some(ledger.novelty.as_ref()),
        target_epoch_ms,
        current_t,
    )
    .await
    .map_err(|e| EnvelopeSnapshotError::PerLedgerResolve {
        ledger: ledger_id.to_string(),
        source: e,
    })?;

    Ok(resolved)
}

// =============================================================================
// Per-sub-query application
// =============================================================================

/// Apply the envelope snapshot to a JSON-LD sub-query body in place.
///
/// Rewrites the sub-query's `from` so every ledger reference is tagged with
/// the resolved `t`. Validation has already established that no sub-query
/// carries an explicit temporal pin while the envelope sets `asOf`, so this
/// is a non-conflicting in-place edit:
///
/// - String entry `"ledgerA"` → object `{"@id": "ledgerA", "t": N}` if
///   ledgerA appears in the snapshot map.
/// - Object entry with `@id`/`id` matching a snapshot ledger: an explicit
///   `"t": N` field is inserted. Any pre-existing `t` or `at` would have
///   tripped the collision check at validation time, so encountering one
///   here is silently overwritten only when validation was bypassed (e.g.,
///   in tests building the request directly).
/// - Ledger references not in the snapshot map are left untouched —
///   typically the case when the envelope omitted `asOf` and validation
///   produced no per-ledger pin (the snapshot map then represents current
///   `t` of each ledger at envelope-entry, and the application path below
///   still pins them so the bundle is atomic).
pub fn apply_snapshot_to_jsonld(query: &mut JsonValue, snapshot: &EnvelopeSnapshot) {
    let Some(body) = query.as_object_mut() else {
        return;
    };
    let Some(from) = body.get_mut("from") else {
        return;
    };
    apply_jsonld_from(from, &snapshot.ledgers);
}

fn apply_jsonld_from(from: &mut JsonValue, ledgers: &HashMap<String, i64>) {
    match from {
        JsonValue::String(_) => {
            // Take ownership of the string and rebuild as an object node.
            if let JsonValue::String(s) = std::mem::replace(from, JsonValue::Null) {
                *from = pin_string_entry(s, ledgers);
            }
        }
        JsonValue::Object(obj) => {
            pin_object_entry(obj, ledgers);
        }
        JsonValue::Array(items) => {
            for item in items.iter_mut() {
                apply_jsonld_from(item, ledgers);
            }
        }
        _ => {}
    }
}

fn pin_string_entry(s: String, ledgers: &HashMap<String, i64>) -> JsonValue {
    // If the identifier already carries an explicit temporal pin (e.g.
    // `ledgerA@t:42`), leave it alone — the user-specified pin wins. Without
    // this check we'd rewrite to `{ "@id": "ledgerA@t:42", "t": current }`
    // which the dataset parser interprets as `t` overriding the suffix.
    if string_has_explicit_pin(&s) {
        return JsonValue::String(s);
    }
    let bare = bare_ledger_id(&s);
    if let Some(t) = ledgers.get(bare) {
        let mut obj = JsonMap::new();
        obj.insert("@id".to_string(), JsonValue::String(s));
        obj.insert("t".to_string(), JsonValue::Number((*t).into()));
        JsonValue::Object(obj)
    } else {
        JsonValue::String(s)
    }
}

fn pin_object_entry(obj: &mut JsonMap<String, JsonValue>, ledgers: &HashMap<String, i64>) {
    // Explicit object-level pin wins over envelope snapshot — same rule as
    // string-form, applied to `t` / `at` fields and to a marker-bearing
    // `@id`/`id`.
    if obj.contains_key("t") || obj.contains_key("at") {
        return;
    }
    let id = obj
        .get("@id")
        .or_else(|| obj.get("id"))
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    let Some(id) = id else { return };
    if string_has_explicit_pin(&id) {
        return;
    }
    let bare = bare_ledger_id(&id);
    let Some(t) = ledgers.get(bare).copied() else {
        return;
    };
    obj.insert("t".to_string(), JsonValue::Number(t.into()));
}

/// Does this identifier string carry a `@t:`/`@iso:`/`@commit:` suffix?
fn string_has_explicit_pin(s: &str) -> bool {
    ["@t:", "@iso:", "@commit:"].iter().any(|m| s.contains(m))
}

/// Bare ledger id (no temporal suffix, no named-graph fragment). Mirrors the
/// suffix-stripping done by validation.
fn bare_ledger_id(s: &str) -> &str {
    let bare = s.split('#').next().unwrap_or(s);
    for marker in ["@t:", "@iso:", "@commit:"] {
        if let Some(idx) = bare.find(marker) {
            return &bare[..idx];
        }
    }
    bare
}

/// Does this identifier string carry a Fluree temporal marker (`@t:`,
/// `@iso:`, `@commit:`)? Used to skip rewrite on already-pinned IRIs.
/// Fragment suffixes (`#named-graph`) are not temporal and do not match.
fn has_temporal_marker(s: &str) -> bool {
    ["@t:", "@iso:", "@commit:"].iter().any(|m| s.contains(m))
}

/// Apply the envelope snapshot to a SPARQL sub-query, returning a new query
/// string with `FROM <iri>` / `FROM NAMED <iri>` rewritten as
/// `FROM <iri@t:N>` for each ledger present in the snapshot map.
///
/// Uses the SPARQL parser's IRI spans to do precise byte-level splicing; this
/// avoids ever touching IRIs that appear inside comments or string literals.
/// Replacements are applied right-to-left so earlier spans aren't shifted.
///
/// When the SPARQL fails to parse, the original is returned unchanged — the
/// downstream parser produces the user-facing parse error rather than this
/// layer attempting an opaque rewrite.
pub fn apply_snapshot_to_sparql(sparql: &str, snapshot: &EnvelopeSnapshot) -> String {
    use fluree_db_sparql::ast::{IriValue, QueryBody};

    let parsed = fluree_db_sparql::parse_sparql(sparql);
    let Some(ast) = parsed.ast.as_ref() else {
        return sparql.to_string();
    };
    let dataset = match &ast.body {
        QueryBody::Select(q) => q.dataset.as_ref(),
        QueryBody::Construct(q) => q.dataset.as_ref(),
        QueryBody::Ask(q) => q.dataset.as_ref(),
        QueryBody::Describe(q) => q.dataset.as_ref(),
        QueryBody::Update(_) => return sparql.to_string(),
    };
    let Some(ds) = dataset else {
        return sparql.to_string();
    };

    // Collect (span, replacement) pairs. We work with full IRIs only; prefixed
    // names need context expansion and are skipped (they also can't be ledger
    // references in current Fluree SPARQL).
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    for iri in ds.default_graphs.iter().chain(ds.named_graphs.iter()) {
        if let IriValue::Full(value) = &iri.value {
            let value_str: &str = value.as_ref();
            let bare = bare_ledger_id(value_str);
            if let Some(t) = snapshot.ledgers.get(bare) {
                // Defensive: skip IRIs that already carry an explicit
                // temporal pin. Validation rejects collisions when
                // envelope asOf is set, so this only fires in the
                // no-asOf path where an inner pin should win.
                if has_temporal_marker(value_str) {
                    continue;
                }
                // Splice `@t:N` BEFORE any `#fragment` so named-graph
                // selectors (e.g. `<ledger#txn-meta>`) are pinned
                // correctly: `<ledger@t:42#txn-meta>`. The dataset parser
                // reattaches the fragment after temporal parsing.
                let replacement = match value_str.split_once('#') {
                    Some((base, fragment)) => format!("<{base}@t:{t}#{fragment}>"),
                    None => format!("<{value_str}@t:{t}>"),
                };
                edits.push((iri.span.start, iri.span.end, replacement));
            }
        }
    }

    if edits.is_empty() {
        return sparql.to_string();
    }

    edits.sort_by(|a, b| a.0.cmp(&b.0));
    // Apply right-to-left.
    let mut out = sparql.to_string();
    for (start, end, replacement) in edits.into_iter().rev() {
        if start <= out.len() && end <= out.len() && start <= end {
            out.replace_range(start..end, &replacement);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot(pairs: &[(&str, i64)]) -> EnvelopeSnapshot {
        let mut ledgers = HashMap::new();
        for (k, v) in pairs {
            ledgers.insert((*k).to_string(), *v);
        }
        EnvelopeSnapshot {
            as_of: Some("2024-01-01T00:00:00.000Z".into()),
            ledgers,
        }
    }

    // -------------------------------------------------------------------------
    // JSON-LD application
    // -------------------------------------------------------------------------

    #[test]
    fn jsonld_string_from_becomes_object_with_t() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let mut query = json!({
            "from": "ledgerA",
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        let from = &query["from"];
        assert_eq!(from["@id"], "ledgerA");
        assert_eq!(from["t"], 42);
    }

    #[test]
    fn jsonld_object_from_gets_t_inserted() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let mut query = json!({
            "from": { "@id": "ledgerA", "alias": "primary" },
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"]["t"], 42);
        assert_eq!(query["from"]["alias"], "primary");
    }

    #[test]
    fn jsonld_object_from_with_id_alias_works() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let mut query = json!({
            "from": { "id": "ledgerA" },
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"]["t"], 42);
    }

    #[test]
    fn jsonld_array_from_each_entry_gets_pinned() {
        let snap = snapshot(&[("ledgerA", 42), ("ledgerB", 99)]);
        let mut query = json!({
            "from": ["ledgerA", { "@id": "ledgerB", "alias": "b" }],
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"][0]["@id"], "ledgerA");
        assert_eq!(query["from"][0]["t"], 42);
        assert_eq!(query["from"][1]["t"], 99);
    }

    #[test]
    fn jsonld_ledger_not_in_snapshot_is_left_alone() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let mut query = json!({
            "from": "ledgerB",
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        // ledgerB stays a bare string when not in the snapshot.
        assert_eq!(query["from"], "ledgerB");
    }

    #[test]
    fn jsonld_named_graph_fragment_preserved() {
        // Fragment suffix is part of the identifier; the bare ledger id is
        // matched against the snapshot map, but the full identifier
        // (including fragment) is preserved in the rewritten @id.
        let snap = snapshot(&[("ledgerA", 42)]);
        let mut query = json!({
            "from": "ledgerA#txn-meta",
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"]["@id"], "ledgerA#txn-meta");
        assert_eq!(query["from"]["t"], 42);
    }

    // -------------------------------------------------------------------------
    // SPARQL application
    // -------------------------------------------------------------------------

    #[test]
    fn sparql_from_clause_gets_t_suffix_appended() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let sparql = "SELECT ?x FROM <ledgerA> WHERE { ?x ?p ?o }";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert!(out.contains("FROM <ledgerA@t:42>"), "got: {out}");
    }

    #[test]
    fn sparql_from_named_clause_gets_t_suffix_appended() {
        let snap = snapshot(&[("ledgerA", 42), ("ledgerB", 99)]);
        let sparql = "SELECT ?x FROM <ledgerA> FROM NAMED <ledgerB> WHERE { ?x ?p ?o }";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert!(out.contains("FROM <ledgerA@t:42>"));
        assert!(out.contains("FROM NAMED <ledgerB@t:99>"));
    }

    #[test]
    fn sparql_iri_in_string_literal_is_not_rewritten() {
        // Span-based rewrite only touches the parsed FROM <iri> — markers
        // inside string literals or other IRI positions are untouched.
        let snap = snapshot(&[("ledgerA", 42)]);
        let sparql =
            r#"SELECT ?x FROM <ledgerA> WHERE { ?x <http://example.org/p> "note: <ledgerA>" }"#;
        let out = apply_snapshot_to_sparql(sparql, &snap);
        // The literal occurrence inside the string literal stays bare.
        assert!(out.contains(r#""note: <ledgerA>""#), "got: {out}");
        // The FROM clause is rewritten.
        assert!(out.contains("FROM <ledgerA@t:42>"), "got: {out}");
    }

    #[test]
    fn sparql_unparseable_returns_original_unchanged() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let sparql = "not a SPARQL query at all";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert_eq!(out, sparql);
    }

    #[test]
    fn sparql_ledger_not_in_snapshot_is_left_alone() {
        let snap = snapshot(&[("ledgerA", 42)]);
        let sparql = "SELECT ?x FROM <ledgerB> WHERE { ?x ?p ?o }";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert_eq!(out, sparql);
    }

    #[test]
    fn sparql_existing_temporal_suffix_left_untouched() {
        // Validation would have rejected this if envelope asOf is set, but
        // the applier is defensive: it skips IRIs that already carry a pin
        // rather than double-pinning.
        let snap = snapshot(&[("ledgerA", 42)]);
        let sparql = "SELECT ?x FROM <ledgerA@t:99> WHERE { ?x ?p ?o }";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert_eq!(out, sparql);
    }

    #[test]
    fn sparql_fragment_iri_gets_t_spliced_before_fragment() {
        // FROM <ledger#txn-meta> must be rewritten to
        // FROM <ledger@t:42#txn-meta> — @t goes BEFORE the fragment
        // because the dataset parser separates fragment from temporal
        // suffix and reattaches the fragment after the time-spec.
        let snap = snapshot(&[("ledgerA", 42)]);
        let sparql = "SELECT ?x FROM <ledgerA#txn-meta> WHERE { ?x ?p ?o }";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert!(
            out.contains("FROM <ledgerA@t:42#txn-meta>"),
            "expected fragment preserved with t spliced before, got: {out}"
        );
    }

    #[test]
    fn sparql_fragment_iri_from_named_clause_gets_t_spliced_before_fragment() {
        let snap = snapshot(&[("ledgerA", 42), ("ledgerB", 99)]);
        let sparql =
            "SELECT ?x FROM <ledgerA#txn-meta> FROM NAMED <ledgerB#extras> WHERE { ?x ?p ?o }";
        let out = apply_snapshot_to_sparql(sparql, &snap);
        assert!(out.contains("FROM <ledgerA@t:42#txn-meta>"), "got: {out}");
        assert!(
            out.contains("FROM NAMED <ledgerB@t:99#extras>"),
            "got: {out}"
        );
    }

    // -------------------------------------------------------------------------
    // Inner pin preserved when envelope omits asOf (review fix High #1)
    // -------------------------------------------------------------------------

    #[test]
    fn jsonld_string_from_with_suffix_left_untouched() {
        // Inner explicit pin (`@t:42`) wins — applier must leave the string
        // alone. Otherwise we'd rewrite to {"@id":"ledgerA@t:42","t":99}
        // and the object `t` would silently override the suffix.
        let snap = snapshot(&[("ledgerA", 99)]);
        let mut query = json!({
            "from": "ledgerA@t:42",
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"], "ledgerA@t:42");
    }

    #[test]
    fn jsonld_string_from_with_iso_suffix_left_untouched() {
        let snap = snapshot(&[("ledgerA", 99)]);
        let mut query = json!({
            "from": "ledgerA@iso:2024-01-01T00:00:00Z",
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"], "ledgerA@iso:2024-01-01T00:00:00Z");
    }

    #[test]
    fn jsonld_object_from_with_explicit_t_field_left_untouched() {
        let snap = snapshot(&[("ledgerA", 99)]);
        let mut query = json!({
            "from": { "@id": "ledgerA", "t": 42 },
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        // Original t survives — applier doesn't overwrite when a pin exists.
        assert_eq!(query["from"]["t"], 42);
    }

    #[test]
    fn jsonld_object_from_with_explicit_at_field_left_untouched() {
        let snap = snapshot(&[("ledgerA", 99)]);
        let mut query = json!({
            "from": { "@id": "ledgerA", "at": "commit:abc123" },
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"]["at"], "commit:abc123");
        // No `t` added — `at` is the active pin.
        assert!(query["from"].as_object().unwrap().get("t").is_none());
    }

    #[test]
    fn jsonld_object_from_with_id_suffix_left_untouched() {
        let snap = snapshot(&[("ledgerA", 99)]);
        let mut query = json!({
            "from": { "@id": "ledgerA@t:42" },
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        // No `t` field added — the suffix in @id is the pin.
        assert_eq!(query["from"]["@id"], "ledgerA@t:42");
        assert!(query["from"].as_object().unwrap().get("t").is_none());
    }

    #[test]
    fn jsonld_mixed_array_preserves_pinned_entries_only() {
        // Two entries for the same ledger: one with an explicit pin, one bare.
        // The pinned entry stays; the bare entry gets the envelope's t.
        let snap = snapshot(&[("ledgerA", 99), ("ledgerB", 42)]);
        let mut query = json!({
            "from": ["ledgerA@t:1", "ledgerB"],
            "select": {"?s": ["*"]},
            "where": []
        });
        apply_snapshot_to_jsonld(&mut query, &snap);
        assert_eq!(query["from"][0], "ledgerA@t:1");
        assert_eq!(query["from"][1]["@id"], "ledgerB");
        assert_eq!(query["from"][1]["t"], 42);
    }
}
