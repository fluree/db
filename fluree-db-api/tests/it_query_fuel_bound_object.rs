//! Regression test for bound-object scans against the binary (FIR6) index.
//!
//! Bug (fix #1): a constant object that is **absent** from the persisted
//! dictionary (a class that no entity is typed as, or a ref IRI that no flake
//! references) used to drop the object filter and fall into the un-narrowed
//! catch-all in `BinaryScanOperator::open`, scanning the *entire* predicate and
//! post-filtering to discover 0 matches. Cost scaled with predicate cardinality
//! (e.g. ~1 fuel per `rdf:type` flake), so a query returning nothing could cost
//! hundreds of thousands of fuel.
//!
//! An object value provably absent from the base dictionary cannot be
//! referenced by any base flake, so the scan must short-circuit to the
//! overlay-only (novelty) path instead. This test pins that invariant: the
//! fuel for an absent-object query is bounded by a small constant **independent
//! of dataset size**, while present-value lookups still return correct rows.

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
        graph.push(json!({
            "@id": format!("ns:widget-{i}"),
            "@type": "ns:Widget",
            "ns:name": format!("widget name {i}"),
            "ns:owner": { "@id": format!("ns:owner-{}", i % 5) },
        }));
    }

    fluree
        .insert(ledger0, &json!({ "@context": ctx(), "@graph": graph }))
        .await
        .expect("insert seed data");

    rebuild_and_publish_index(fluree, LEDGER_ID).await;
}

/// Run a tracked JSON-LD query, returning `(row_count, fuel)`.
async fn tracked(fluree: &fluree_db_api::Fluree, where_obj: JsonValue) -> (usize, f64) {
    let query = json!({
        "@context": ctx(),
        "from": LEDGER_ID,
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
