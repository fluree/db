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

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{assert_index_defaults, genesis_ledger};

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

/// Seed a ledger that exercises the empty-after-retract leaflet case.
///
/// At t=1 every invoice has a `ns:legacyFlag "true"` triple. At t=2 the
/// legacy flag is fully retracted from every invoice (no replacement). The
/// indexer preserves the now-empty leaflet's metadata (`row_count == 0`
/// but `history_*` populated) so time-travel can still recover the
/// retracted state. Historical queries at t=1 must see the legacy flag;
/// queries at t=2 must not.
async fn seed_fully_retracted_ledger(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // t=1: 5 invoices, all carrying ns:legacyFlag "true" and a status.
    let mut invoices = Vec::with_capacity(5);
    for i in 0..5 {
        invoices.push(json!({
            "@id": format!("ns:Invoice/inv-{:02}", i),
            "@type": "ns:Invoice",
            "ns:status": "paid",
            "ns:legacyFlag": "true",
        }));
    }
    let tx1 = json!({"@context": ctx(), "@graph": invoices});
    let _ = fluree.insert(ledger0, &tx1).await.expect("tx1");
    support::rebuild_and_publish_index(fluree, ledger_id).await;
    let l1 = fluree.ledger(ledger_id).await.expect("reload after t=1");

    // t=2: retract every ns:legacyFlag triple — no replacement value.
    let tx2 = json!({
        "@context": ctx(),
        "where": {"@id": "?inv", "ns:legacyFlag": "?flag"},
        "delete": {"@id": "?inv", "ns:legacyFlag": "?flag"}
    });
    let l2 = fluree.update(l1, &tx2).await.expect("tx2").ledger;
    support::rebuild_and_publish_index(fluree, ledger_id).await;
    fluree.ledger(ledger_id).await.expect("reload after t=2");
    l2
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
/// Note: a *full* `rebuild_index_from_commits` of this small ledger does
/// not actually produce a `row_count == 0` (empty-after-retract) leaflet
/// — the rebuild path just omits the predicate from the t=2 index. The
/// specific empty-leaflet+sidecar shape is covered by
/// `replay_at_t_reconstructs_empty_after_retract_leaflet` in
/// `fluree-db-binary-index/src/read/replay.rs`, and the four batched
/// probe sites + the cursor were updated to not pre-skip those leaflets.
/// Even so, this end-to-end test locks in the post-retract time-travel
/// behavior at the SPARQL surface.
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
