//! Regression tests: generic `xsd:duration` values must round-trip through
//! the binary index (issue #1326).
//!
//! A mixed duration (year-month AND day-time components, e.g.
//! `P1Y2M3DT4H5M6S`) cannot use the inline `yearMonthDuration` /
//! `dayTimeDuration` encodings, so it is stored via the string dictionary.
//! The binary decode path previously returned `FlakeValue::Null` for these,
//! so SELECTing the value after a reindex produced `null` instead of the
//! stored duration, and a bound generic-duration object matched nothing.

#![cfg(feature = "native")]

use crate::support::{self, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn rows_for(
    fluree: &MemoryFluree,
    ledger: &MemoryLedger,
    predicate: &str,
) -> serde_json::Value {
    let q = json!({
        "@context": ctx(),
        "select": ["?v"],
        "where": {"@id": "ex:task", predicate: "?v"}
    });
    support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query should succeed")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
}

/// Reindex the ledger and return the freshly loaded state, asserting the
/// binary range provider is active so reads exercise the indexed path.
async fn reindex_and_load(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert!(
        indexed.snapshot.range_provider.is_some(),
        "expected binary range provider after reindex"
    );
    indexed
}

/// Insert a mixed generic duration (plus subtype controls), reindex, and read
/// the values back through the binary index.
#[tokio::test]
async fn generic_duration_round_trips_through_binary_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:roundtrip";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:task",
                "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"},
                "ex:ym": {"@value": "P14M", "@type": "xsd:yearMonthDuration"},
                "ex:dt": {"@value": "PT36H", "@type": "xsd:dayTimeDuration"}
            }
        ]
    });
    let novelty = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    // Novelty (pre-index) read: the serialization the indexed path must
    // reproduce for a canonical-form generic duration.
    let took_novelty = rows_for(&fluree, &novelty, "ex:took").await;
    assert_eq!(
        normalize_rows(&took_novelty),
        normalize_rows(&json!([[
            {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}
        ]])),
        "novelty read should return the typed duration, got {took_novelty}"
    );

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    let took = rows_for(&fluree, &indexed, "ex:took").await;
    assert_eq!(
        normalize_rows(&took),
        normalize_rows(&took_novelty),
        "generic xsd:duration must round-trip through the binary index, got {took}"
    );

    // Controls: the inline-encoded subtypes were unaffected by the bug and
    // must keep decoding (to their canonical forms).
    let ym = rows_for(&fluree, &indexed, "ex:ym").await;
    assert_eq!(
        normalize_rows(&ym),
        normalize_rows(&json!([[{"@value": "P1Y2M", "@type": "xsd:yearMonthDuration"}]]))
    );
    let dt = rows_for(&fluree, &indexed, "ex:dt").await;
    assert_eq!(
        normalize_rows(&dt),
        normalize_rows(&json!([[{"@value": "P1DT12H", "@type": "xsd:dayTimeDuration"}]]))
    );
}

/// A generic duration bound as a query constraint object must also match
/// after reindex (equality against the decoded value, not Null).
#[tokio::test]
async fn generic_duration_matches_bound_object_after_reindex() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:bound";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a", "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}},
            {"@id": "ex:b", "ex:took": {"@value": "P2YT1S", "@type": "xsd:duration"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    let q = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": {"@id": "?s", "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}}
    });
    let rows = support::query_jsonld(&fluree, &indexed, &q)
        .await
        .expect("query should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:a"]])),
        "bound generic duration must match exactly one subject, got {rows}"
    );
}

/// A SPARQL COUNT with a bound generic-duration object — the shape served by
/// the V6 count fast path — must count exactly the matching subjects.
#[tokio::test]
async fn generic_duration_bound_object_count_after_reindex() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:count";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a", "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}},
            {"@id": "ex:b", "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}},
            {"@id": "ex:c", "ex:took": {"@value": "P2YT1S", "@type": "xsd:duration"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    let q = r#"
        PREFIX ex: <http://example.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT (COUNT(?s) AS ?n)
        WHERE { ?s ex:took "P1Y2M3DT4H5M6S"^^xsd:duration }
    "#;
    let rows = support::query_sparql(&fluree, &indexed, q)
        .await
        .expect("sparql count should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([[2]])),
        "bound-duration COUNT must count the two matching subjects, got {rows}"
    );
}

/// The V6 count fast path (`value_to_otype_okey_simple`) also encodes the
/// inline duration subtypes. A bound `xsd:yearMonthDuration` COUNT keys on
/// `encode_year_month_dur(months)`, so it must count exactly the subjects
/// whose (canonicalized) value matches — regardless of the bound lexical form.
/// (`P14M` and `P1Y2M` both parse to 14 months and key identically.)
#[tokio::test]
async fn year_month_duration_bound_object_count_after_reindex() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:ym-count";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a", "ex:ym": {"@value": "P14M", "@type": "xsd:yearMonthDuration"}},
            {"@id": "ex:b", "ex:ym": {"@value": "P14M", "@type": "xsd:yearMonthDuration"}},
            {"@id": "ex:c", "ex:ym": {"@value": "P25M", "@type": "xsd:yearMonthDuration"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    // Bind the canonical form (P1Y2M) to prove the key is value-based, not
    // lexical: it must still match the P14M-inserted rows.
    let q = r#"
        PREFIX ex: <http://example.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT (COUNT(?s) AS ?n)
        WHERE { ?s ex:ym "P1Y2M"^^xsd:yearMonthDuration }
    "#;
    let rows = support::query_sparql(&fluree, &indexed, q)
        .await
        .expect("sparql count should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([[2]])),
        "bound yearMonthDuration COUNT must count the two matching subjects, got {rows}"
    );
}

/// V6 count fast path for the `xsd:dayTimeDuration` subtype arm: keys on
/// `encode_day_time_dur(micros)`, counting exactly the matching subjects.
#[tokio::test]
async fn day_time_duration_bound_object_count_after_reindex() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:dt-count";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a", "ex:dt": {"@value": "PT36H", "@type": "xsd:dayTimeDuration"}},
            {"@id": "ex:b", "ex:dt": {"@value": "PT36H", "@type": "xsd:dayTimeDuration"}},
            {"@id": "ex:c", "ex:dt": {"@value": "PT10H", "@type": "xsd:dayTimeDuration"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    // Bind the canonical form (P1DT12H) of the inserted PT36H.
    let q = r#"
        PREFIX ex: <http://example.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT (COUNT(?s) AS ?n)
        WHERE { ?s ex:dt "P1DT12H"^^xsd:dayTimeDuration }
    "#;
    let rows = support::query_sparql(&fluree, &indexed, q)
        .await
        .expect("sparql count should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([[2]])),
        "bound dayTimeDuration COUNT must count the two matching subjects, got {rows}"
    );
}

/// Non-canonical lexical input: the novelty read preserves the original
/// form, while the indexed read returns the canonical form the resolver
/// interned — the value-based-storage design shared by all temporal types,
/// pinned here for generic durations.
#[tokio::test]
async fn generic_duration_non_canonical_input_canonicalizes_on_reindex() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:non-canonical";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:task", "ex:took": {"@value": "P0Y14MT0S", "@type": "xsd:duration"}}
        ]
    });
    let novelty = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let took_novelty = rows_for(&fluree, &novelty, "ex:took").await;
    assert_eq!(
        normalize_rows(&took_novelty),
        normalize_rows(&json!([[{"@value": "P0Y14MT0S", "@type": "xsd:duration"}]])),
        "novelty read preserves the original lexical, got {took_novelty}"
    );

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    let took = rows_for(&fluree, &indexed, "ex:took").await;
    assert_eq!(
        normalize_rows(&took),
        normalize_rows(&json!([[{"@value": "P1Y2M", "@type": "xsd:duration"}]])),
        "indexed read returns the canonical lexical, got {took}"
    );
}

/// A negative mixed duration is already canonical and must round-trip
/// identically on both paths.
#[tokio::test]
async fn negative_generic_duration_round_trips_through_binary_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:negative";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:task", "ex:took": {"@value": "-P1Y2M3DT4H5M6S", "@type": "xsd:duration"}}
        ]
    });
    let novelty = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let took_novelty = rows_for(&fluree, &novelty, "ex:took").await;
    assert_eq!(
        normalize_rows(&took_novelty),
        normalize_rows(&json!([[{"@value": "-P1Y2M3DT4H5M6S", "@type": "xsd:duration"}]])),
        "novelty read should return the typed negative duration, got {took_novelty}"
    );

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    let took = rows_for(&fluree, &indexed, "ex:took").await;
    assert_eq!(
        normalize_rows(&took),
        normalize_rows(&took_novelty),
        "negative generic duration must round-trip through the binary index, got {took}"
    );
}

/// A generic duration asserted AFTER indexing (novelty on top of a binary
/// base) must read back correctly through the overlay-translation path.
#[tokio::test]
async fn generic_duration_novelty_over_binary_base_round_trips() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:novelty-overlay";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Base: an unrelated triple so the index has content, then reindex.
    let base = json!({
        "@context": ctx(),
        "@graph": [{"@id": "ex:task", "ex:label": "base"}]
    });
    fluree.insert(ledger0, &base).await.expect("insert base");
    let indexed = reindex_and_load(&fluree, ledger_id).await;

    // Novelty on top of the binary base: a generic duration.
    let novelty = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:task", "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}}
        ]
    });
    let ledger = fluree
        .insert(indexed, &novelty)
        .await
        .expect("insert novelty")
        .ledger;

    let took = rows_for(&fluree, &ledger, "ex:took").await;
    assert_eq!(
        normalize_rows(&took),
        normalize_rows(&json!([[
            {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}
        ]])),
        "novelty duration over a binary base must round-trip, got {took}"
    );
}

/// Retracting an indexed generic duration from novelty must cancel the base
/// row — the retraction has to pair with the indexed value, whether through
/// the translated overlay lane or the raw-flake merge.
#[tokio::test]
async fn generic_duration_retraction_cancels_indexed_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "duration-index:retract";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:task", "ex:took": {"@value": "P1Y2M3DT4H5M6S", "@type": "xsd:duration"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");

    let indexed = reindex_and_load(&fluree, ledger_id).await;

    let retract = json!({
        "@context": ctx(),
        "where":  {"@id": "ex:task", "ex:took": "?d"},
        "delete": {"@id": "ex:task", "ex:took": "?d"}
    });
    let ledger = fluree
        .update(indexed, &retract)
        .await
        .expect("retract duration")
        .ledger;

    let took = rows_for(&fluree, &ledger, "ex:took").await;
    assert_eq!(
        normalize_rows(&took),
        normalize_rows(&json!([])),
        "retracted duration must not survive the base/overlay merge, got {took}"
    );
}
