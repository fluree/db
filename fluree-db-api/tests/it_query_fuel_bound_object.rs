//! Regression tests for bound-object queries whose object is **absent** from
//! the persisted dictionary, against the binary (FIR6) index.
//!
//! Bug class: a constant object that no flake references (a class nothing is
//! typed as, or a ref IRI never interned) used to be mishandled as "can't
//! narrow" instead of "provably empty", causing a full predicate scan:
//!   - `BinaryScanOperator::open` dropped the object filter into its
//!     un-narrowed catch-all and post-filtered the whole predicate;
//!   - `count_bound_object_v6` errored and fell back to a generic
//!     scan + aggregate.
//!
//! Either way cost scaled with predicate cardinality (~1 fuel per `rdf:type`
//! flake), so a query returning nothing could burn hundreds of thousands of
//! fuel.
//!
//! An object provably absent from the base dictionary cannot be referenced by
//! any base flake, so these paths must short-circuit (scan → overlay-only;
//! count → 0). These tests pin that invariant: fuel for an absent-object query
//! stays bounded by a small constant **independent of dataset size**, while
//! present-value lookups still return correct rows/counts.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{assert_index_defaults, genesis_ledger, rebuild_and_publish_index};

const LEDGER_ID: &str = "fuel-bound-object:main";

/// Number of typed entities. Large enough that a full predicate scan (the
/// pre-fix behavior) would consume fuel far above `ABSENT_FUEL_CEILING`.
const N: usize = 500;

/// Upper bound on fuel for any query whose constant object is absent from the
/// dictionary. With no novelty, the overlay-only fallback does zero index work,
/// so this is generous headroom. Pre-fix, these queries cost ~N fuel.
const ABSENT_FUEL_CEILING: f64 = 25.0;

fn ctx() -> JsonValue {
    json!({ "ns": "http://example.org/ns#" })
}

/// Seed N widgets (`ns:Widget`), each owned by one of 5 owner entities, then
/// rebuild the binary index so the data lives in persisted base leaflets.
async fn seed(fluree: &fluree_db_api::Fluree) {
    let ledger0 = genesis_ledger(fluree, LEDGER_ID);

    let mut graph = Vec::with_capacity(N + 5);
    for i in 0..5 {
        graph.push(json!({
            "@id": format!("ns:owner-{i}"),
            "@type": "ns:Owner",
            "ns:label": format!("Owner {i}"),
        }));
    }
    for i in 0..N {
        // `ns:tag` is deliberately mixed-datatype (string on even, integer on
        // odd). xsd:string is its only string-compatible datatype, so a
        // plain-string lookup still narrows to a tight xsd:string seek — the
        // integer values can't match a string and don't block the narrowing.
        let tag = if i % 2 == 0 {
            json!(format!("tag-{i}"))
        } else {
            json!(i)
        };
        graph.push(json!({
            "@id": format!("ns:widget-{i}"),
            "@type": "ns:Widget",
            "ns:name": format!("widget name {i}"),
            "ns:owner": { "@id": format!("ns:owner-{}", i % 5) },
            "ns:tag": tag,
        }));
    }

    fluree
        .insert(ledger0, &json!({ "@context": ctx(), "@graph": graph }))
        .await
        .expect("insert seed data");

    rebuild_and_publish_index(fluree, LEDGER_ID).await;
}

/// Run a tracked JSON-LD `select ["?s"]` query against `LEDGER_ID`.
async fn tracked(fluree: &fluree_db_api::Fluree, where_obj: JsonValue) -> (usize, f64) {
    tracked_on(fluree, LEDGER_ID, where_obj).await
}

/// Run a tracked JSON-LD `select ["?s"]` query against an explicit ledger,
/// returning `(row_count, fuel)`.
async fn tracked_on(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    where_obj: JsonValue,
) -> (usize, f64) {
    let query = json!({
        "@context": ctx(),
        "from": ledger_id,
        "select": ["?s"],
        "where": where_obj,
    });

    let result = fluree
        .query_from()
        .jsonld(&query)
        .track_all()
        .execute_tracked()
        .await
        .expect("tracked query should succeed");

    assert_eq!(result.status, 200, "query status");
    let rows = result
        .result
        .as_array()
        .map(std::vec::Vec::len)
        .expect("result should be a JSON array");
    let fuel = result.fuel.expect("fuel should be present under track_all");
    (rows, fuel)
}

/// Run a tracked SPARQL scalar COUNT, returning `(count, fuel)`. Uses SPARQL so
/// the planner selects the `count_bound_object_v6` fast path.
async fn tracked_count(fluree: &fluree_db_api::Fluree, class_iri: &str) -> (Option<i64>, f64) {
    let sparql =
        format!("SELECT (COUNT(?s) AS ?n) FROM <{LEDGER_ID}> WHERE {{ ?s a <{class_iri}> }}");
    let result = fluree
        .query_from()
        .sparql(&sparql)
        .track_all()
        .execute_tracked()
        .await
        .expect("tracked count query should succeed");

    assert_eq!(result.status, 200, "count query status");
    let fuel = result.fuel.expect("fuel should be present under track_all");
    // W3C SPARQL JSON: results.bindings[0].<var>.value
    let count = result
        .result
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(serde_json::Value::as_array)
        .and_then(|b| b.first())
        .and_then(|row| row.as_object())
        .and_then(|row| row.values().next())
        .and_then(|cell| cell.get("value"))
        .and_then(serde_json::Value::as_str)
        .and_then(|s| s.parse::<i64>().ok());
    (count, fuel)
}

#[tokio::test]
async fn bound_object_absent_string_does_not_scan_predicate() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    // `ns:tag` mixes xsd:string + int; xsd:string is its only string-compatible
    // datatype, so a plain-string lookup narrows to a tight xsd:string seek. An
    // *absent* string resolves to no str_id → NotFound → overlay-only, returning
    // nothing without a full predicate scan. (The `(None,None)` find_string_id
    // probe for non-narrowable predicates is covered by the langString test.)
    let (ghost_rows, ghost_fuel) = tracked(
        &fluree,
        json!({ "@id": "?s", "ns:tag": "tag-nonexistent-value" }),
    )
    .await;
    assert_eq!(ghost_rows, 0, "absent string matches nothing");
    assert!(
        ghost_fuel <= ABSENT_FUEL_CEILING,
        "absent string burned {ghost_fuel} fuel (ceiling {ABSENT_FUEL_CEILING}); \
         a full ns:tag predicate scan regressed"
    );

    // Present string now narrows via per-predicate datatype stats: ns:tag is
    // mixed xsd:string + int, but xsd:string is the only string-compatible tag,
    // so a plain-string lookup seeks (XSD_STRING, str_id) — bounded fuel + correct.
    let (present_rows, present_fuel) =
        tracked(&fluree, json!({ "@id": "?s", "ns:tag": "tag-0" })).await;
    assert_eq!(present_rows, 1, "present string matches exactly widget-0");
    assert!(
        present_fuel <= ABSENT_FUEL_CEILING,
        "present string on mixed string+int predicate burned {present_fuel} fuel; \
         expected a tight xsd:string seek, not a full scan"
    );

    // Pure xsd:string predicate (ns:name) also narrows to a tight seek.
    let (name_rows, name_fuel) =
        tracked(&fluree, json!({ "@id": "?s", "ns:name": "widget name 0" })).await;
    assert_eq!(
        name_rows, 1,
        "present string on pure-string predicate matches widget-0"
    );
    assert!(
        name_fuel <= ABSENT_FUEL_CEILING,
        "present string on pure xsd:string predicate burned {name_fuel} fuel; expected a tight seek"
    );
}

#[tokio::test]
async fn untyped_string_on_langstring_predicate_stays_lenient_and_short_circuits_absent() {
    // `ns:label` has BOTH xsd:string and rdf:langString values, so the gate must
    // DECLINE to narrow (langString present). Two things to verify:
    //   1. Negative guard: a present untyped string stays lenient — it matches
    //      across both datatypes (xsd:string "shared" and "shared"@en).
    //   2. The non-narrowable path's absent-string short-circuit still works: an
    //      absent string hits the `(None,None)` find_string_id probe → NotFound
    //      → overlay-only, so it must NOT full-scan the (large) ns:label predicate.
    // The bulk of distinct labels makes a full scan visibly exceed the ceiling.
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "fuel-langstring:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let mut graph = vec![
        json!({ "@id": "ns:plain", "ns:label": "shared" }),
        json!({ "@id": "ns:tagged", "ns:label": { "@value": "shared", "@language": "en" } }),
    ];
    for i in 0..N {
        graph.push(json!({ "@id": format!("ns:row-{i}"), "ns:label": format!("label-{i}") }));
    }
    fluree
        .insert(ledger0, &json!({ "@context": ctx(), "@graph": graph }))
        .await
        .expect("insert");
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // (1) Present "shared" → lenient match across xsd:string + langString. This
    // predicate has langString, so it intentionally does NOT narrow (Phase 2),
    // hence we assert correctness only, not fuel.
    let (present_rows, _present_fuel) = tracked_on(
        &fluree,
        ledger_id,
        json!({ "@id": "?s", "ns:label": "shared" }),
    )
    .await;
    assert_eq!(
        present_rows, 2,
        "untyped 'shared' must match both xsd:string and langString (lenient); \
         optimizer must not narrow when langString is present"
    );

    // (2) Absent string → short-circuit via the find_string_id probe, no scan.
    let (absent_rows, absent_fuel) = tracked_on(
        &fluree,
        ledger_id,
        json!({ "@id": "?s", "ns:label": "no-such-label" }),
    )
    .await;
    assert_eq!(absent_rows, 0, "absent string matches nothing");
    assert!(
        absent_fuel <= ABSENT_FUEL_CEILING,
        "absent string on a langString predicate burned {absent_fuel} fuel (ceiling \
         {ABSENT_FUEL_CEILING}); the find_string_id short-circuit regressed"
    );
}

#[tokio::test]
async fn bound_object_absent_count_does_not_scan_predicate() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    // Present class: the fast COUNT path returns the correct total. (Note: the
    // V6 count fast path reads leaflets directly and is currently unmetered, so
    // we don't assert on its fuel — only on correctness.)
    let (present_count, _present_fuel) =
        tracked_count(&fluree, "http://example.org/ns#Widget").await;
    assert_eq!(present_count, Some(N as i64), "present class count");

    // Absent class: COUNT must short-circuit to 0 instead of falling back to a
    // full predicate scan + aggregate. The fallback IS metered, so pre-fix this
    // burned ~N fuel; post-fix it stays bounded regardless of N.
    let (ghost_count, ghost_fuel) = tracked_count(&fluree, "http://example.org/ns#Ghost").await;
    assert_eq!(ghost_count, Some(0), "absent class counts 0");
    assert!(
        ghost_fuel <= ABSENT_FUEL_CEILING,
        "absent-class COUNT burned {ghost_fuel} fuel (ceiling {ABSENT_FUEL_CEILING}); \
         it fell back to a full rdf:type scan (~{N} flakes) instead of returning 0"
    );
}

#[tokio::test]
async fn bound_object_absent_from_dict_does_not_scan_predicate() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree).await;

    // Baseline: present class returns all widgets via a tight (o_type,o_key)
    // seek. This is the legitimate, result-proportional cost we contrast against.
    let (present_rows, present_fuel) =
        tracked(&fluree, json!({ "@id": "?s", "@type": "ns:Widget" })).await;
    assert_eq!(present_rows, N, "present class should match every widget");
    assert!(
        present_fuel > 0.0,
        "present-class query should consume fuel"
    );

    // Absent class: no entity is typed `ns:Ghost`. Must NOT scan the rdf:type
    // predicate — fuel stays bounded regardless of N.
    let (ghost_rows, ghost_fuel) =
        tracked(&fluree, json!({ "@id": "?s", "@type": "ns:Ghost" })).await;
    assert_eq!(ghost_rows, 0, "absent class matches nothing");
    assert!(
        ghost_fuel <= ABSENT_FUEL_CEILING,
        "absent class burned {ghost_fuel} fuel (ceiling {ABSENT_FUEL_CEILING}); \
         a full rdf:type predicate scan regressed (~{N} flakes)"
    );

    // Absent ref object: `ns:owner-999` is never referenced. Same invariant.
    let (absent_ref_rows, absent_ref_fuel) = tracked(
        &fluree,
        json!({ "@id": "?s", "ns:owner": { "@id": "ns:owner-999" } }),
    )
    .await;
    assert_eq!(absent_ref_rows, 0, "absent ref matches nothing");
    assert!(
        absent_ref_fuel <= ABSENT_FUEL_CEILING,
        "absent ref burned {absent_ref_fuel} fuel (ceiling {ABSENT_FUEL_CEILING}); \
         a full ns:owner predicate scan regressed"
    );

    // Correctness guard: a present ref object still resolves and narrows.
    // owner-0 owns widgets 0,5,10,... = ceil(N/5) of them.
    let expected_owned = N.div_ceil(5);
    let (owned_rows, owned_fuel) = tracked(
        &fluree,
        json!({ "@id": "?s", "ns:owner": { "@id": "ns:owner-0" } }),
    )
    .await;
    assert_eq!(
        owned_rows, expected_owned,
        "present ref should return owner-0's widgets"
    );
    assert!(owned_fuel > 0.0, "present-ref query should consume fuel");
}
