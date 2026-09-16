//! Per-predicate policy gating of the raw-row lanes.
//!
//! The binary scan's cursor lane and the batched leaflet probe lanes emit
//! index rows without per-leaf policy filtering. Under a non-root policy they
//! used to decline unconditionally. Now a lane that reads exactly one
//! statically known predicate stays on when the view set provably cannot
//! touch that predicate (`covers_predicate` is false and the default allows),
//! short-circuits to empty when it cannot touch it and the default denies,
//! and falls back to the filtered path otherwise.
//!
//! Every case pins routing with the `fast-path outcome` stamps, not just the
//! answer: the filtered fallback computes the same rows, so a lane that
//! silently stopped firing is invisible to a value assertion.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use crate::support::span_capture::{init_test_tracing, SpanStore};
use fluree_db_api::Fluree;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, GovernanceOptions, GraphDb, IndexConfig, QueryInput, ReindexOptions,
    TxnOpts,
};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "policy/predicate-lanes:main";
const EX: &str = "http://example.org/ns/";
const SCAN_SITE: &str = "policy_predicate_scan";
const PROBE_SITE: &str = "policy_predicate_probe";

/// Three people over a persisted index: two plain users and one admin.
async fn indexed_people() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, LEDGER);
    // Hold off background indexing so the reindex below is the only build.
    let no_background = IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };
    fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": EX},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice", "ex:age": 30, "ex:ssn": "111"},
                    {"@id": "ex:bob", "@type": "ex:User", "ex:name": "Bob", "ex:age": 25, "ex:ssn": "222"},
                    {"@id": "ex:carol", "@type": "ex:Admin", "ex:name": "Carol", "ex:age": 41, "ex:ssn": "333"}
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_background,
        )
        .await
        .expect("insert people");
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");
    fluree
}

async fn policed_view(fluree: &Fluree, policy: JsonValue, default_allow: bool) -> GraphDb {
    let view = fluree
        .db_with_policy(
            LEDGER,
            &GovernanceOptions {
                policy: Some(policy),
                default_allow: Some(default_allow),
                ..Default::default()
            },
        )
        .await
        .expect("db_with_policy");
    assert!(
        !view.is_root(),
        "inline rules must build an enforcing context"
    );
    assert!(
        view.snapshot.stats.is_some(),
        "indexed view carries statistics"
    );
    view
}

fn deny_ssn() -> JsonValue {
    json!([{
        "@id": format!("{EX}denySsn"),
        "f:action": "f:view",
        "f:required": true,
        "f:onProperty": [{"@id": format!("{EX}ssn")}],
        "f:allow": false
    }])
}

/// Routing outcomes stamped on `site` since `before`.
fn outcomes(store: &SpanStore, before: usize, site: &str) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

enum Lane {
    /// The lane must have proceeded at least once and never declined.
    MustFire,
    /// The lane must have declined at least once and never proceeded.
    MustNotFire,
}

async fn run(
    fluree: &Fluree,
    view: &GraphDb,
    store: &SpanStore,
    query: impl Into<QueryInput<'_>>,
    site: &str,
    lane: Lane,
    label: &str,
) -> JsonValue {
    let before = store.find_events("fast-path outcome").len();
    let result = fluree
        .query(view, query)
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
    let rows = result.to_jsonld(&view.snapshot).expect("jsonld");
    let seen = outcomes(store, before, site);
    let proceeded = seen.iter().any(|o| o == "proceed");
    let declined = seen.iter().any(|o| o == "fallback:gate_declined");
    match lane {
        Lane::MustFire => assert!(
            proceeded && !declined,
            "{label}: lane `{site}` must fire and never decline; stamps: {seen:?}"
        ),
        Lane::MustNotFire => assert!(
            declined && !proceeded,
            "{label}: lane `{site}` must decline and never fire; stamps: {seen:?}"
        ),
    }
    rows
}

fn row_count(rows: &JsonValue) -> usize {
    rows.as_array().map(Vec::len).unwrap_or(0)
}

/// A property rule on `ex:ssn` under an allow default: scans and probes of
/// other predicates keep their raw-row lanes; `ex:ssn` and a wildcard
/// predicate fall back and are filtered.
#[tokio::test(flavor = "current_thread")]
async fn property_rule_keeps_lanes_for_untouched_predicates() {
    let fluree = indexed_people().await;
    let view = policed_view(&fluree, deny_ssn(), true).await;
    let (store, guard) = init_test_tracing();

    let names = run(
        &fluree,
        &view,
        &store,
        &json!({"@context": {"ex": EX}, "select": "?n", "where": {"@id": "?s", "ex:name": "?n"}}),
        SCAN_SITE,
        Lane::MustFire,
        "scan ex:name",
    )
    .await;
    assert_eq!(row_count(&names), 3, "{names}");

    let ssn = run(
        &fluree,
        &view,
        &store,
        &json!({"@context": {"ex": EX}, "select": "?v", "where": {"@id": "?s", "ex:ssn": "?v"}}),
        SCAN_SITE,
        Lane::MustNotFire,
        "scan ex:ssn",
    )
    .await;
    assert_eq!(row_count(&ssn), 0, "{ssn}");

    let preds = run(
        &fluree,
        &view,
        &store,
        &json!({"@context": {"ex": EX}, "selectDistinct": "?p", "where": {"@id": "?s", "?p": "?o"}}),
        SCAN_SITE,
        Lane::MustNotFire,
        "wildcard predicate scan",
    )
    .await;
    let preds = preds.to_string();
    assert!(
        preds.contains("ex:name") && !preds.contains("ex:ssn"),
        "wildcard scan must be filtered: {preds}"
    );

    let joined = run(
        &fluree,
        &view,
        &store,
        QueryInput::Sparql(
            "SELECT ?n ?a WHERE { ?s <http://example.org/ns/name> ?n . ?s <http://example.org/ns/age> ?a }",
        ),
        PROBE_SITE,
        Lane::MustFire,
        "probe ex:age",
    )
    .await;
    assert_eq!(row_count(&joined), 3, "{joined}");

    let joined_ssn = run(
        &fluree,
        &view,
        &store,
        QueryInput::Sparql(
            "SELECT ?n ?v WHERE { ?s <http://example.org/ns/name> ?n . ?s <http://example.org/ns/ssn> ?v }",
        ),
        PROBE_SITE,
        Lane::MustNotFire,
        "probe ex:ssn",
    )
    .await;
    assert_eq!(row_count(&joined_ssn), 0, "{joined_ssn}");

    drop(guard);
}

/// SPARQL twin of the scan cases: the gate sits in the operator, so the same
/// verdicts must come out of the other surface.
#[tokio::test(flavor = "current_thread")]
async fn property_rule_sparql_twin() {
    let fluree = indexed_people().await;
    let view = policed_view(&fluree, deny_ssn(), true).await;
    let (store, guard) = init_test_tracing();

    let names = run(
        &fluree,
        &view,
        &store,
        QueryInput::Sparql("SELECT ?n WHERE { ?s <http://example.org/ns/name> ?n }"),
        SCAN_SITE,
        Lane::MustFire,
        "sparql scan ex:name",
    )
    .await;
    assert_eq!(row_count(&names), 3, "{names}");

    let ssn = run(
        &fluree,
        &view,
        &store,
        QueryInput::Sparql("SELECT ?v WHERE { ?s <http://example.org/ns/ssn> ?v }"),
        SCAN_SITE,
        Lane::MustNotFire,
        "sparql scan ex:ssn",
    )
    .await;
    assert_eq!(row_count(&ssn), 0, "{ssn}");

    drop(guard);
}

/// A class rule restricts subjects, not predicates. The view set expands it
/// into every property the class carries, so a scan of `ex:name` is covered:
/// the lane must decline and the filtered path must hide the admin.
#[tokio::test(flavor = "current_thread")]
async fn class_rule_declines_covered_predicate() {
    let fluree = indexed_people().await;
    let policy = json!([{
        "@id": format!("{EX}denyAdmins"),
        "f:action": "f:view",
        "f:required": true,
        "f:onClass": [{"@id": format!("{EX}Admin")}],
        "f:allow": false
    }]);
    let view = policed_view(&fluree, policy, true).await;
    let (store, guard) = init_test_tracing();

    let names = run(
        &fluree,
        &view,
        &store,
        &json!({"@context": {"ex": EX}, "select": "?n", "where": {"@id": "?s", "ex:name": "?n"}}),
        SCAN_SITE,
        Lane::MustNotFire,
        "scan ex:name under class rule",
    )
    .await;
    let names = names.to_string();
    assert!(
        names.contains("Alice") && names.contains("Bob") && !names.contains("Carol"),
        "class rule must hide the admin: {names}"
    );

    drop(guard);
}

/// A deny default with one allow rule: the allowed predicate is covered and
/// takes the filtered path; an untouched predicate is hidden outright, so
/// the scan short-circuits to empty without reading a leaf.
#[tokio::test(flavor = "current_thread")]
async fn deny_default_short_circuits_untouched_predicate() {
    let fluree = indexed_people().await;
    let policy = json!([{
        "@id": format!("{EX}allowName"),
        "f:action": "f:view",
        "f:onProperty": [{"@id": format!("{EX}name")}],
        "f:allow": true
    }]);
    let view = policed_view(&fluree, policy, false).await;
    let (store, guard) = init_test_tracing();

    let names = run(
        &fluree,
        &view,
        &store,
        &json!({"@context": {"ex": EX}, "select": "?n", "where": {"@id": "?s", "ex:name": "?n"}}),
        SCAN_SITE,
        Lane::MustNotFire,
        "scan ex:name under deny default",
    )
    .await;
    assert_eq!(row_count(&names), 3, "{names}");

    let ages = run(
        &fluree,
        &view,
        &store,
        &json!({"@context": {"ex": EX}, "select": "?a", "where": {"@id": "?s", "ex:age": "?a"}}),
        SCAN_SITE,
        Lane::MustFire,
        "scan ex:age under deny default",
    )
    .await;
    assert_eq!(row_count(&ages), 0, "{ages}");

    drop(guard);
}
