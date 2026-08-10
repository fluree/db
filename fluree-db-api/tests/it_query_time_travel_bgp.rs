//! Regression tests for time-travel BGP queries that combine a type-class
//! triple with a same-subject property triple.
//!
//! The bug: when a SPARQL BGP combines `?s a <Class>` with a same-subject
//! triple `?s <p> <literal>` (or `?s <p> ?o` with `?o` projected through a
//! GROUP BY key), the join path bypasses the time-travel filter and returns
//! the latest state at every `t`. The same query expressed with a FILTER or
//! a BIND alias returns the correct historical state.
//!
//! Root cause hypothesis: `NestedLoopJoinOperator`'s batched probe paths
//! (`flush_batched_accumulator_binary` →
//! `scan_leaves_into_scatter`, `flush_batched_exists_accumulator_binary` →
//! `batched_subject_probe_binary`) read base leaflet rows directly without
//! applying the `to_t` filter or replaying the history sidecar — so they
//! silently return latest-state results for historical snapshots once the
//! data has been reindexed.

#![cfg(feature = "native")]

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const LEDGER_ID: &str = "tt-bgp:main";

fn ctx() -> JsonValue {
    json!({
        "ns": "http://example.org/ns#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Seed 20 invoices: 18 with status "paid", 2 with status "approved".
/// Then change the 2 "approved" invoices to "paid" at t=2.
/// Reindex after each commit so the persisted base index sees t=2 as max.
async fn seed_invoice_ledger(fluree: &fluree_db_api::Fluree) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, LEDGER_ID);

    // t=1: 20 invoices.
    let mut invoices = Vec::with_capacity(20);
    for i in 0..20 {
        let status = if i < 18 { "paid" } else { "approved" };
        invoices.push(json!({
            "@id": format!("ns:Invoice/inv-{:02}", i),
            "@type": "ns:Invoice",
            "ns:status": status,
            "ns:totalAmount": 100 + i,
        }));
    }
    let tx1 = json!({"@context": ctx(), "@graph": invoices});
    let _ledger1 = fluree.insert(ledger0, &tx1).await.expect("tx1").ledger;

    // Rebuild index so the t=1 state is persisted in base leaflets.
    support::rebuild_and_publish_index(fluree, LEDGER_ID).await;

    // Reload the ledger so the new index is picked up.
    let ledger1 = fluree.ledger(LEDGER_ID).await.expect("reload after t=1");

    // t=2: change inv-18 and inv-19 from "approved" to "paid".
    let tx2 = json!({
        "@context": ctx(),
        "where": {
            "@id": "?inv",
            "ns:status": "approved"
        },
        "delete": {
            "@id": "?inv",
            "ns:status": "approved"
        },
        "insert": {
            "@id": "?inv",
            "ns:status": "paid"
        }
    });
    let ledger2 = fluree.update(ledger1, &tx2).await.expect("tx2").ledger;

    // Rebuild again so the post-t=2 base index has retracts in the sidecar
    // and "paid" as the live value for inv-18 / inv-19.
    support::rebuild_and_publish_index(fluree, LEDGER_ID).await;
    fluree.ledger(LEDGER_ID).await.expect("reload after t=2");
    ledger2
}

/// Which index the fully-retracted fixture should be served from.
///
/// The three strategies are separately reachable in production and were
/// separately wrong/right before the full-rebuild fix, so every shape below
/// is measured against all three rather than against whichever one the
/// helper happened to pick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexStrategy {
    /// `rebuild_index_from_commits` — the path `Fluree::reindex()` takes.
    FullRebuild,
    /// `build_index_for_record` — incremental once a base index exists.
    Incremental,
    /// Never indexed; everything served from novelty.
    NoveltyOnly,
}

impl IndexStrategy {
    async fn publish(self, fluree: &fluree_db_api::Fluree, ledger_id: &str) {
        match self {
            Self::FullRebuild => support::rebuild_and_publish_index(fluree, ledger_id).await,
            Self::Incremental => support::build_and_publish_index(fluree, ledger_id).await,
            Self::NoveltyOnly => {}
        }
    }
}

/// Seed a ledger that exercises the empty-after-retract leaflet case.
///
/// At t=1 every invoice has a `ns:legacyFlag "true"` triple. At t=2 the
/// legacy flag is fully retracted from every invoice (no replacement). The
/// predicate then has zero live rows, so nothing but its history sidecar
/// can answer a query at t=1 — the builder must still emit its (zero-row)
/// partition, or every predicate-keyed lane silently reports "no rows".
/// Historical queries at t=1 must see the legacy flag; queries at t=2 must
/// not.
///
/// `ns:status` is a deliberate live-predicate control: it shares the
/// subjects and survives t=2, so a shape that returns nothing for
/// `ns:legacyFlag` but 5 for `ns:status` isolates the defect to the
/// fully-retracted predicate rather than to time travel in general.
async fn seed_fully_retracted_ledger_with(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    strategy: IndexStrategy,
    invoice_count: usize,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // t=1: N invoices, all carrying ns:legacyFlag "true" and a status.
    let mut invoices = Vec::with_capacity(invoice_count);
    for i in 0..invoice_count {
        invoices.push(json!({
            "@id": format!("ns:Invoice/inv-{:04}", i),
            "@type": "ns:Invoice",
            "ns:status": "paid",
            "ns:legacyFlag": "true",
        }));
    }
    let tx1 = json!({"@context": ctx(), "@graph": invoices});
    let _ = fluree.insert(ledger0, &tx1).await.expect("tx1");
    strategy.publish(fluree, ledger_id).await;
    let l1 = fluree.ledger(ledger_id).await.expect("reload after t=1");

    // t=2: retract every ns:legacyFlag triple — no replacement value.
    let tx2 = json!({
        "@context": ctx(),
        "where": {"@id": "?inv", "ns:legacyFlag": "?flag"},
        "delete": {"@id": "?inv", "ns:legacyFlag": "?flag"}
    });
    let l2 = fluree.update(l1, &tx2).await.expect("tx2").ledger;
    strategy.publish(fluree, ledger_id).await;
    fluree.ledger(ledger_id).await.expect("reload after t=2");
    l2
}

async fn seed_fully_retracted_ledger(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    seed_fully_retracted_ledger_with(fluree, ledger_id, IndexStrategy::FullRebuild, 5).await
}

/// Run a SPARQL SELECT and return the raw JSON-LD row array.
async fn run_rows(fluree: &fluree_db_api::Fluree, sparql: &str) -> Vec<JsonValue> {
    fluree
        .query_from()
        .sparql(sparql)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("sparql should succeed: {e}\n{sparql}"))
        .as_array()
        .expect("array result")
        .clone()
}

async fn run_row_count(fluree: &fluree_db_api::Fluree, sparql: &str) -> usize {
    run_rows(fluree, sparql).await.len()
}

async fn run_count_sparql(fluree: &fluree_db_api::Fluree, sparql: &str) -> i64 {
    let jsonld = fluree
        .query_from()
        .sparql(sparql)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("count sparql should succeed");

    let arr = jsonld.as_array().expect("array result");
    assert_eq!(arr.len(), 1, "expected exactly one row, got {jsonld}");
    let row = arr[0].as_array().expect("row is array");
    assert_eq!(row.len(), 1, "expected exactly one column, got {jsonld}");
    row[0].as_i64().expect("count is integer")
}

/// Pattern E (broken): `?inv a ns:Invoice ; ns:status "paid"` at t=1
/// must return 18 (the historical count of paid invoices), not 20 (the
/// latest count).
#[tokio::test]
async fn time_travel_type_plus_literal_object_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_invoice_ledger(&fluree).await;

    let sparql_t1 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{LEDGER_ID}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:status "paid" }}"#
    );
    let count_t1 = run_count_sparql(&fluree, &sparql_t1).await;
    assert_eq!(
        count_t1, 18,
        "at t=1 only 18 invoices were paid, but query returned {count_t1} \
         (likely the latest count of 20 — time-travel filter ignored)"
    );

    let sparql_t2 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{LEDGER_ID}@t:2>
          WHERE {{ ?inv a ns:Invoice ; ns:status "paid" }}"#
    );
    let count_t2 = run_count_sparql(&fluree, &sparql_t2).await;
    assert_eq!(count_t2, 20, "at t=2 all 20 invoices are paid");
}

/// Pattern D (control — already works): `?inv a ns:Invoice ; ns:status ?s
/// FILTER(?s = "paid")` must return the same 18 / 20 counts as pattern E
/// at the corresponding t. This locks in the existing correct behavior so
/// a fix to E does not regress D.
#[tokio::test]
async fn time_travel_type_plus_filter_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_invoice_ledger(&fluree).await;

    let sparql_t1 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{LEDGER_ID}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:status ?s . FILTER(?s = "paid") }}"#
    );
    let count_t1 = run_count_sparql(&fluree, &sparql_t1).await;
    assert_eq!(
        count_t1, 18,
        "FILTER variant must match literal-object variant at t=1"
    );

    let sparql_t2 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{LEDGER_ID}@t:2>
          WHERE {{ ?inv a ns:Invoice ; ns:status ?s . FILTER(?s = "paid") }}"#
    );
    let count_t2 = run_count_sparql(&fluree, &sparql_t2).await;
    assert_eq!(
        count_t2, 20,
        "FILTER variant must match literal-object variant at t=2"
    );
}

/// Pattern A (broken): `?inv a ns:Invoice ; ns:status ?status` GROUP BY
/// ?status. At t=1 the "paid" group must have 18 rows, not 20. The bug:
/// the batched-subject join path for the second triple ignores `to_t`
/// and reads base leaflet rows directly, returning latest-state status
/// values regardless of the snapshot time.
#[tokio::test]
async fn time_travel_type_plus_group_by_property_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_invoice_ledger(&fluree).await;

    let sparql_t1 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?status (COUNT(?inv) AS ?n)
          FROM <{LEDGER_ID}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:status ?status }}
          GROUP BY ?status"
    );
    let jsonld = fluree
        .query_from()
        .sparql(&sparql_t1)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("group-by sparql should succeed");

    let rows = jsonld.as_array().expect("array").clone();
    let mut paid: Option<i64> = None;
    let mut approved: Option<i64> = None;
    for row in &rows {
        let arr = row.as_array().expect("row");
        let status = arr[0].as_str().unwrap_or_default();
        let count = arr[1].as_i64().expect("count");
        match status {
            "paid" => paid = Some(count),
            "approved" => approved = Some(count),
            _ => {}
        }
    }
    assert_eq!(
        paid,
        Some(18),
        "at t=1, paid count must be 18; full result: {jsonld}"
    );
    assert_eq!(
        approved,
        Some(2),
        "at t=1, approved count must be 2; full result: {jsonld}"
    );
}

/// SPARQL surface regression for time-travel after a full retract.
///
/// At t=1 every invoice carries `ns:legacyFlag "true"`; at t=2 the flag
/// is fully retracted with no replacement. Historical queries at t=1
/// must still see the flag, queries at t=2 must not.
///
/// The three variants after the original two-triple star are the meaning-
/// preserving edits that used to flip this test from 5 to 0: a null
/// `FILTER`, putting the retracted predicate first, and dropping the
/// `rdf:type` anchor. Each one moves the query off `PropertyJoinOperator`,
/// which is the only lane that reached the retracted rows while the
/// full-rebuild builder was dropping their partition. They are pinned here
/// so this test gates the storage invariant rather than one plan shape.
#[tokio::test]
async fn time_travel_fully_retracted_leaflet_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tt-bgp-empty:main";
    let _ledger = seed_fully_retracted_ledger(&fluree, ledger_id).await;

    // Pattern E shape: type-class triple + same-subject literal-object
    // triple. Goes through `flush_batched_exists_accumulator_binary` →
    // `batched_subject_probe_binary`.
    let q_t1 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{ledger_id}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag "true" }}"#
    );
    assert_eq!(
        run_count_sparql(&fluree, &q_t1).await,
        5,
        "at t=1 all 5 invoices carry ns:legacyFlag; the leaflet that was \
         emptied at t=2 must replay from its sidecar"
    );

    let q_t2 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{ledger_id}@t:2>
          WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag "true" }}"#
    );
    assert_eq!(
        run_count_sparql(&fluree, &q_t2).await,
        0,
        "at t=2 the legacy flag was fully retracted; count must be 0"
    );

    // Pattern A shape: type + same-subject ?o triple, GROUP BY ?o. Goes
    // through `flush_batched_accumulator_binary` → `scan_leaves_into_scatter`.
    let q_grp_t1 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?flag (COUNT(?inv) AS ?n)
          FROM <{ledger_id}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag ?flag }}
          GROUP BY ?flag"
    );
    let jsonld = fluree
        .query_from()
        .sparql(&q_grp_t1)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("group-by sparql should succeed");
    let rows = jsonld.as_array().expect("array");
    let true_count = rows.iter().find_map(|row| {
        let arr = row.as_array()?;
        if arr[0].as_str()? == "true" {
            arr[1].as_i64()
        } else {
            None
        }
    });
    assert_eq!(
        true_count,
        Some(5),
        "at t=1 group-by must see all 5 retracted-since flags; got {jsonld}"
    );

    // Edit 1: add a semantically-null FILTER. `isIRI(?inv)` cannot change the
    // answer — but it stops the block being triples-only, so the star no
    // longer fuses into PropertyJoinOperator.
    let q_filter = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{ledger_id}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag "true" . FILTER(isIRI(?inv)) }}"#
    );
    assert_eq!(
        run_count_sparql(&fluree, &q_filter).await,
        5,
        "a null FILTER must not change the answer"
    );

    // Edit 2: put the retracted predicate first, so it becomes the driver.
    let q_flag_first = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{ledger_id}@t:1>
          WHERE {{ ?inv ns:legacyFlag "true" ; ns:status "paid" }}"#
    );
    assert_eq!(
        run_count_sparql(&fluree, &q_flag_first).await,
        5,
        "triple order must not change the answer"
    );

    // Edit 3: drop the rdf:type anchor entirely — a plain single-triple scan.
    let q_plain = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT (COUNT(?inv) AS ?n)
          FROM <{ledger_id}@t:1>
          WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(
        run_count_sparql(&fluree, &q_plain).await,
        5,
        "a plain scan of a fully-retracted predicate must replay its history"
    );
}

/// Every query shape the fully-retracted fixture can be read through, under
/// every index strategy.
///
/// Before the full-rebuild fix, eight of these ten cells were wrong under
/// `FullRebuild` and every cell was correct under `Incremental` and
/// `NoveltyOnly` — the builder dropped the predicate's partition because it
/// had no live rows, and only `PropertyJoinOperator`'s batched subject probe
/// reached the surviving SPOT sidecars. Running the whole grid keeps the
/// two correct writers pinned as controls while the third is changed.
async fn assert_fully_retracted_shapes(strategy: IndexStrategy, ledger_id: &str) {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_fully_retracted_ledger_with(&fluree, ledger_id, strategy, 5).await;
    let ctx = format!("strategy {strategy:?}");

    let prefix = "PREFIX ns: <http://example.org/ns#>";

    // 1. Single triple, bound object.
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}@t:1>
           WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "{ctx}: single triple, bound object");

    // 2. Single triple, var object — and the value must be the real one.
    let q = format!(
        r"{prefix} SELECT ?inv ?f FROM <{ledger_id}@t:1>
           WHERE {{ ?inv ns:legacyFlag ?f }}"
    );
    let rows = run_rows(&fluree, &q).await;
    assert_eq!(rows.len(), 5, "{ctx}: single triple, var object");
    assert!(
        rows.iter()
            .all(|r| r.as_array().and_then(|a| a[1].as_str()) == Some("true")),
        "{ctx}: var object must bind the historical value; got {rows:?}"
    );

    // 3. COUNT over the single triple.
    let q = format!(
        r#"{prefix} SELECT (COUNT(?inv) AS ?n) FROM <{ledger_id}@t:1>
           WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_count_sparql(&fluree, &q).await, 5, "{ctx}: COUNT over single triple");

    // 4. Type-anchored 2-star (the property-join lane).
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}@t:1>
           WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "{ctx}: type-anchored 2-star");

    // 5. Same 2-star plus a null FILTER.
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}@t:1>
           WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag "true" . FILTER(isIRI(?inv)) }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "{ctx}: 2-star + null FILTER");

    // 6. Flag-first 2-star.
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}@t:1>
           WHERE {{ ?inv ns:legacyFlag "true" ; ns:status "paid" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "{ctx}: flag-first 2-star");

    // 7. OPTIONAL — the worst presentation: pre-fix this returned 5 rows with
    //    ?f unbound, which reads as a truthful "the flag did not exist at t=1".
    let q = format!(
        r"{prefix} SELECT ?inv ?f FROM <{ledger_id}@t:1>
           WHERE {{ ?inv a ns:Invoice . OPTIONAL {{ ?inv ns:legacyFlag ?f }} }}"
    );
    let rows = run_rows(&fluree, &q).await;
    assert_eq!(rows.len(), 5, "{ctx}: OPTIONAL row count");
    let bound = rows
        .iter()
        .filter(|r| r.as_array().and_then(|a| a[1].as_str()) == Some("true"))
        .count();
    assert_eq!(
        bound, 5,
        "{ctx}: OPTIONAL must bind ?f for all 5 invoices at t=1, not report null; got {rows:?}"
    );

    // 8. FILTER EXISTS.
    let q = format!(
        r"{prefix} SELECT ?inv FROM <{ledger_id}@t:1>
           WHERE {{ ?inv a ns:Invoice . FILTER EXISTS {{ ?inv ns:legacyFlag ?f }} }}"
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "{ctx}: FILTER EXISTS");

    // 9. Live-predicate control — must be unaffected in every cell.
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}@t:1>
           WHERE {{ ?inv ns:status "paid" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "{ctx}: live-predicate control");

    // 10. Current state: the predicate really is gone at t=2.
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}@t:2>
           WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 0, "{ctx}: current state must be empty");

    // 11. Latest (no @t) must also stay empty — the preserved zero-row
    //     partition must never resurrect rows outside a replay.
    let q = format!(
        r#"{prefix} SELECT ?inv FROM <{ledger_id}>
           WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 0, "{ctx}: latest must be empty");
}

#[tokio::test]
async fn fully_retracted_shapes_full_rebuild() {
    assert_fully_retracted_shapes(IndexStrategy::FullRebuild, "tt-matrix-full:main").await;
}

#[tokio::test]
async fn fully_retracted_shapes_incremental() {
    assert_fully_retracted_shapes(IndexStrategy::Incremental, "tt-matrix-incr:main").await;
}

#[tokio::test]
async fn fully_retracted_shapes_novelty_only() {
    assert_fully_retracted_shapes(IndexStrategy::NoveltyOnly, "tt-matrix-novelty:main").await;
}

/// Membership-join sized fixture: 300 subjects clears
/// `MEMBERSHIP_JOIN_MIN_DRIVING` (256), so the driving side takes the
/// membership-join lane rather than the nested-loop one. The defect is a
/// build-side absence, so the mechanism predicts this lane fails too — pin
/// that it does not.
#[tokio::test]
async fn time_travel_fully_retracted_membership_join_sized() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tt-membership:main";
    let _ledger =
        seed_fully_retracted_ledger_with(&fluree, ledger_id, IndexStrategy::FullRebuild, 300).await;

    let q = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv FROM <{ledger_id}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 300, "membership-join lane at t=1");

    let q_plain = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv FROM <{ledger_id}@t:1>
          WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q_plain).await, 300, "plain scan at t=1");
}

/// Partial retraction bounds the blast radius: when only some of a
/// predicate's rows are retracted the partition survives on its remaining
/// live rows, so this shape was already correct. Pinned as a control so a
/// future change to the emission rule cannot quietly break it.
#[tokio::test]
async fn time_travel_partially_retracted_predicate_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tt-partial:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let mut invoices = Vec::with_capacity(5);
    for i in 0..5 {
        invoices.push(json!({
            "@id": format!("ns:Invoice/inv-{i:02}"),
            "@type": "ns:Invoice",
            "ns:status": "paid",
            "ns:legacyFlag": "true",
        }));
    }
    let tx1 = json!({"@context": ctx(), "@graph": invoices});
    let _ = fluree.insert(ledger0, &tx1).await.expect("tx1");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let l1 = fluree.ledger(ledger_id).await.expect("reload after t=1");

    // Retract the flag from two of the five.
    let tx2 = json!({
        "@context": ctx(),
        "where": {"@id": "?inv", "ns:legacyFlag": "?flag"},
        "values": ["?inv", [{"@value": "ns:Invoice/inv-00", "@type": "@id"},
                            {"@value": "ns:Invoice/inv-01", "@type": "@id"}]],
        "delete": {"@id": "?inv", "ns:legacyFlag": "?flag"}
    });
    let _ = fluree.update(l1, &tx2).await.expect("tx2");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.ledger(ledger_id).await.expect("reload after t=2");

    let q_t1 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv FROM <{ledger_id}@t:1>
          WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q_t1).await, 5, "all 5 carried the flag at t=1");

    let q_t2 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv FROM <{ledger_id}@t:2>
          WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q_t2).await, 3, "3 still carry the flag at t=2");
}

/// Remediation story for indexes already damaged by an older binary: a
/// rebuild reads the commit chain from genesis and never the previous
/// index's leaves, so re-running `reindex` with a fixed binary reconstructs
/// the missing partitions. Pinned by rebuilding a second time over an
/// already-published index and re-asserting the historical read.
#[tokio::test]
async fn repeat_full_rebuild_restores_retracted_history() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tt-reindex-again:main";
    let _ledger = seed_fully_retracted_ledger(&fluree, ledger_id).await;

    let q = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv FROM <{ledger_id}@t:1>
          WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q).await, 5, "first rebuild");

    // A second reindex over the already-published index — the user-facing
    // remediation for a ledger indexed by an older binary.
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.ledger(ledger_id).await.expect("reload after reindex");
    assert_eq!(run_row_count(&fluree, &q).await, 5, "reindex again");

    let q_t2 = format!(
        r#"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv FROM <{ledger_id}@t:2>
          WHERE {{ ?inv ns:legacyFlag "true" }}"#
    );
    assert_eq!(run_row_count(&fluree, &q_t2).await, 0, "t=2 stays empty after reindex");
}

/// Microbench: compare latest vs historical batched-probe timing.
///
/// Run with: `cargo test -p fluree-db-api --features native --test
/// it_query_time_travel_bgp -- --ignored --nocapture
/// time_travel_bench_replay_overhead`.
///
/// Builds a 10k-invoice ledger with ~10% status mutations between t=1 and
/// t=2. Each query path goes through `flush_batched_exists_accumulator_binary`
/// (pattern E) and `flush_batched_accumulator_binary` (pattern A). The
/// historical path additionally runs `replay_leaflet_at_t` per leaflet.
#[tokio::test]
#[ignore]
async fn time_travel_bench_replay_overhead() {
    use std::time::Instant;

    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tt-bgp-bench:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    const N: usize = 10_000;
    const MUTATED: usize = 1_000; // ~10%

    // t=1: N invoices, last MUTATED status="approved", rest "paid".
    let mut invoices = Vec::with_capacity(N);
    for i in 0..N {
        let status = if i < N - MUTATED { "paid" } else { "approved" };
        invoices.push(json!({
            "@id": format!("ns:Invoice/inv-{:06}", i),
            "@type": "ns:Invoice",
            "ns:status": status,
            "ns:totalAmount": 100 + i,
        }));
    }
    let tx1 = json!({"@context": ctx(), "@graph": invoices});
    let _ = fluree.insert(ledger0, &tx1).await.expect("tx1");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let l1 = fluree.ledger(ledger_id).await.unwrap();

    // t=2: flip MUTATED rows from "approved" to "paid".
    let tx2 = json!({
        "@context": ctx(),
        "where": {"@id": "?inv", "ns:status": "approved"},
        "delete": {"@id": "?inv", "ns:status": "approved"},
        "insert": {"@id": "?inv", "ns:status": "paid"}
    });
    fluree.update(l1, &tx2).await.expect("tx2");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    // Pattern E (literal-object exists). At latest expect N paid; at t=1
    // expect N-MUTATED paid.
    let q_lit = |t: i64| {
        format!(
            r#"PREFIX ns: <http://example.org/ns#>
              SELECT (COUNT(?inv) AS ?n)
              FROM <{ledger_id}@t:{t}>
              WHERE {{ ?inv a ns:Invoice ; ns:status "paid" }}"#
        )
    };
    // Pattern A (group by status). Same shape, different join helper.
    let q_grp = |t: i64| {
        format!(
            r"PREFIX ns: <http://example.org/ns#>
              SELECT ?status (COUNT(?inv) AS ?n)
              FROM <{ledger_id}@t:{t}>
              WHERE {{ ?inv a ns:Invoice ; ns:status ?status }}
              GROUP BY ?status"
        )
    };

    // Warm up caches/dicts.
    for _ in 0..2 {
        let _ = run_count_sparql(&fluree, &q_lit(2)).await;
        let _ = run_count_sparql(&fluree, &q_lit(1)).await;
    }

    const ITERS: u32 = 30;
    let mut t_lit_latest = std::time::Duration::ZERO;
    let mut t_lit_hist = std::time::Duration::ZERO;
    let mut t_grp_latest = std::time::Duration::ZERO;
    let mut t_grp_hist = std::time::Duration::ZERO;
    for _ in 0..ITERS {
        let q = q_lit(2);
        let s = Instant::now();
        let _ = run_count_sparql(&fluree, &q).await;
        t_lit_latest += s.elapsed();

        let q = q_lit(1);
        let s = Instant::now();
        let _ = run_count_sparql(&fluree, &q).await;
        t_lit_hist += s.elapsed();

        let q = q_grp(2);
        let s = Instant::now();
        let _ = fluree
            .query_from()
            .sparql(&q)
            .format(fluree_db_api::FormatterConfig::jsonld())
            .execute_formatted()
            .await
            .unwrap();
        t_grp_latest += s.elapsed();

        let q = q_grp(1);
        let s = Instant::now();
        let _ = fluree
            .query_from()
            .sparql(&q)
            .format(fluree_db_api::FormatterConfig::jsonld())
            .execute_formatted()
            .await
            .unwrap();
        t_grp_hist += s.elapsed();
    }
    let to_avg = |d: std::time::Duration| (d.as_secs_f64() * 1000.0) / f64::from(ITERS);
    println!(
        "\n--- batched join probe: latest vs historical ({N} invoices, ~{MUTATED} mutated, {ITERS} iters) ---"
    );
    println!(
        "pattern E (literal-object exists): latest = {:.2} ms/iter, t=1 = {:.2} ms/iter, ratio = {:.2}x",
        to_avg(t_lit_latest),
        to_avg(t_lit_hist),
        to_avg(t_lit_hist) / to_avg(t_lit_latest)
    );
    println!(
        "pattern A (group-by status):       latest = {:.2} ms/iter, t=1 = {:.2} ms/iter, ratio = {:.2}x",
        to_avg(t_grp_latest),
        to_avg(t_grp_hist),
        to_avg(t_grp_hist) / to_avg(t_grp_latest)
    );
}

/// Pattern B (control — already worked pre-fix): `?inv a ns:Invoice ;
/// ns:status ?s . BIND(?s AS ?aliased)` GROUP BY `?aliased`. The BIND
/// indirection forces this through a different operator shape than
/// pattern A; the bug report showed it returned the correct historical
/// counts even before the fix. Lock that in so the fix to A/E doesn't
/// regress B.
#[tokio::test]
async fn time_travel_type_plus_bind_alias_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_invoice_ledger(&fluree).await;

    let sparql_t1 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?aliased (COUNT(?inv) AS ?n)
          FROM <{LEDGER_ID}@t:1>
          WHERE {{ ?inv a ns:Invoice ; ns:status ?s . BIND(?s AS ?aliased) }}
          GROUP BY ?aliased"
    );
    let jsonld = fluree
        .query_from()
        .sparql(&sparql_t1)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("group-by sparql should succeed");

    let rows = jsonld.as_array().expect("array").clone();
    let mut paid: Option<i64> = None;
    let mut approved: Option<i64> = None;
    for row in &rows {
        let arr = row.as_array().expect("row");
        let status = arr[0].as_str().unwrap_or_default();
        let count = arr[1].as_i64().expect("count");
        match status {
            "paid" => paid = Some(count),
            "approved" => approved = Some(count),
            _ => {}
        }
    }
    assert_eq!(
        paid,
        Some(18),
        "BIND-alias variant must match pattern A at t=1; full result: {jsonld}"
    );
    assert_eq!(
        approved,
        Some(2),
        "BIND-alias variant must match pattern A at t=1; full result: {jsonld}"
    );
}

/// `PropertyJoinOperator` SPOT-walk path at a historical `t`.
///
/// A 3+ predicate same-subject star with unbound objects and no datatype
/// constraints meets `can_spot_walk_remaining`, routing the trailing
/// predicates through `batched_subject_star_spot` rather than the
/// scatter-side `scan_leaves_into_scatter`. The fix gates the SPOT walk
/// on `at_latest_t(ctx)`; this test pins that historical queries through
/// that path return the t=1 state, not the latest state.
#[tokio::test]
async fn time_travel_property_join_spot_walk_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ledger = seed_invoice_ledger(&fluree).await;

    // Three same-subject predicates after the type-class triple. With ?status
    // and ?amount unbound and no FILTER/datatype constraint, this is the
    // shape `can_spot_walk_remaining` accepts.
    let sparql_t1 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv ?status ?amount
          FROM <{LEDGER_ID}@t:1>
          WHERE {{
            ?inv a ns:Invoice .
            ?inv ns:status ?status .
            ?inv ns:totalAmount ?amount .
          }}"
    );
    let jsonld = fluree
        .query_from()
        .sparql(&sparql_t1)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("star sparql should succeed");
    let rows = jsonld.as_array().expect("array");
    assert_eq!(
        rows.len(),
        20,
        "expected 20 invoice rows at t=1; got {jsonld}"
    );
    let approved_at_t1 = rows
        .iter()
        .filter(|row| {
            row.as_array()
                .and_then(|a| a.get(1))
                .and_then(|s| s.as_str())
                == Some("approved")
        })
        .count();
    assert_eq!(
        approved_at_t1, 2,
        "at t=1 the star walk must see the 2 'approved' invoices; got {jsonld}"
    );

    let sparql_t2 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?inv ?status ?amount
          FROM <{LEDGER_ID}@t:2>
          WHERE {{
            ?inv a ns:Invoice .
            ?inv ns:status ?status .
            ?inv ns:totalAmount ?amount .
          }}"
    );
    let jsonld_t2 = fluree
        .query_from()
        .sparql(&sparql_t2)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("star sparql at t=2 should succeed");
    let rows_t2 = jsonld_t2.as_array().expect("array");
    let approved_at_t2 = rows_t2
        .iter()
        .filter(|row| {
            row.as_array()
                .and_then(|a| a.get(1))
                .and_then(|s| s.as_str())
                == Some("approved")
        })
        .count();
    assert_eq!(
        approved_at_t2, 0,
        "at t=2 no invoice is 'approved' anymore; got {jsonld_t2}"
    );
}

/// A fully-retracted *subject* is the SPOT-order analogue of the
/// fully-retracted predicate: at t=2 the entity is gone entirely, so no
/// leaflet in the primary order materializes a row for it. Its transitions
/// still ride along in a neighbouring leaflet's sidecar, but the leaf's
/// routing keys stop at the last live record — so branch-level leaf
/// selection can miss it.
#[tokio::test]
async fn time_travel_fully_retracted_subject_respects_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tt-dead-subject:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let mut invoices = Vec::with_capacity(5);
    for i in 0..5 {
        invoices.push(json!({
            "@id": format!("ns:Invoice/inv-{i:02}"),
            "@type": "ns:Invoice",
            "ns:status": "paid",
            "ns:legacyFlag": "true",
        }));
    }
    let tx1 = json!({"@context": ctx(), "@graph": invoices});
    let _ = fluree.insert(ledger0, &tx1).await.expect("tx1");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let l1 = fluree.ledger(ledger_id).await.expect("reload after t=1");

    // Delete every triple of the highest-sorting invoice.
    let tx2 = json!({
        "@context": ctx(),
        "where": {"@id": "ns:Invoice/inv-04", "?p": "?o"},
        "delete": {"@id": "ns:Invoice/inv-04", "?p": "?o"}
    });
    let _ = fluree.update(l1, &tx2).await.expect("tx2");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.ledger(ledger_id).await.expect("reload after t=2");

    let q_t1 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?p ?o FROM <{ledger_id}@t:1>
          WHERE {{ <http://example.org/ns#Invoice/inv-04> ?p ?o }}"
    );
    assert_eq!(
        run_row_count(&fluree, &q_t1).await,
        3,
        "at t=1 the deleted invoice still had @type, status and legacyFlag"
    );

    let q_t2 = format!(
        r"PREFIX ns: <http://example.org/ns#>
          SELECT ?p ?o FROM <{ledger_id}@t:2>
          WHERE {{ <http://example.org/ns#Invoice/inv-04> ?p ?o }}"
    );
    assert_eq!(run_row_count(&fluree, &q_t2).await, 0, "gone at t=2");
}
