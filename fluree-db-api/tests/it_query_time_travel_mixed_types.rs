//! Batched join lanes read a time-travel view through history replay, which
//! restores rows into a leaflet whose current constants describe only its
//! current rows. A reference retracted at t=2 from a predicate whose
//! remaining values are strings sits, at t=1, in a leaflet whose
//! `o_type_const` says string. Reading that constant instead of the replayed
//! batch's column decoded the reference as a string (another value of the
//! dictionary) and missed it as a bound object.

#![cfg(feature = "native")]

use crate::support;
use crate::support::span_capture::init_test_tracing;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, ReindexOptions, TxnOpts};
use serde_json::json;

const EX: &str = "http://example.org/ns/";
const NO_BACKGROUND: IndexConfig = IndexConfig {
    reindex_min_bytes: 1_000_000_000,
    reindex_max_bytes: 1_000_000_000,
};

async fn reindex(fluree: &Fluree, id: &str, leaflet_rows: Option<usize>) {
    let opts = match leaflet_rows {
        Some(rows) => ReindexOptions::default().with_indexer_config(
            fluree_db_indexer::IndexerConfig::default()
                .with_leaflet_rows(rows)
                .with_leaf_rows(rows * 4),
        ),
        None => ReindexOptions::default(),
    };
    fluree.reindex(id, opts).await.expect("reindex");
}

/// `ex:s ex:p ex:o` retracted at t=2, its other values strings, indexed at
/// t=2 with `leaflet_rows` per leaflet.
async fn retracted_reference(leaflet_rows: Option<usize>) -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "tt-mixed:main";
    let ledger = support::genesis_ledger(&fluree, id).await;
    fluree
        .insert_with_opts(
            ledger,
            &json!({"@context": {"ex": EX}, "@graph": [
                {"@id": "ex:s", "ex:p": [{"@id": "ex:o"}, "still present"], "ex:tag": "t"},
                {"@id": "ex:s2", "ex:label": "second subject", "ex:p": "s2 value"}
            ]}),
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("insert");
    reindex(&fluree, id, leaflet_rows).await;
    let ledger = fluree.ledger(id).await.expect("ledger");
    fluree
        .update_with_opts(
            ledger,
            &json!({"@context": {"ex": EX}, "delete": {"@id": "ex:s", "ex:p": {"@id": "ex:o"}}}),
            TxnOpts::default(),
            CommitOpts::default(),
            &NO_BACKGROUND,
        )
        .await
        .expect("retract");
    reindex(&fluree, id, leaflet_rows).await;
    fluree
}

/// The query's rows at t=1 as sorted `var=value` strings, and whether the
/// nested-loop join ran batched on the lane `lane_field` names.
async fn rows_at_t1(fluree: &Fluree, query: &str, lane_field: &str) -> (Vec<String>, bool) {
    let db = fluree.db_at_t("tt-mixed:main", 1).await.expect("db at t=1");
    let (store, guard) = init_test_tracing();
    let result = fluree
        .query(&db, format!("PREFIX ex: <{EX}>\n{query}").as_str())
        .await
        .expect("query");
    let batched = store
        .find_events("nested loop join runtime path")
        .iter()
        .any(|e| {
            e.fields.get("use_batched").map(String::as_str) == Some("true")
                && e.fields.get(lane_field).map(String::as_str) == Some("true")
        });
    drop(guard);
    let json = result.to_sparql_json(&db.snapshot).expect("sparql json");
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .flat_map(|b| {
            let mut cells: Vec<String> = b
                .as_object()
                .unwrap()
                .iter()
                .map(|(var, term)| format!("{var}={}", term["value"].as_str().unwrap()))
                .collect();
            cells.sort();
            [cells.join(" ")]
        })
        .collect();
    rows.sort();
    (rows, batched)
}

#[tokio::test(flavor = "current_thread")]
async fn batched_lanes_read_a_replayed_reference_as_a_reference() {
    for leaflet_rows in [None, Some(1), Some(4)] {
        let fluree = retracted_reference(leaflet_rows).await;

        let (rows, batched) = rows_at_t1(
            &fluree,
            "SELECT ?o WHERE { VALUES ?s { ex:s } ?s ex:p ?o }",
            "batched_eligible",
        )
        .await;
        assert!(batched, "{leaflet_rows:?}: subject lane must run");
        assert_eq!(
            rows,
            vec![format!("o={EX}o"), "o=still present".to_string()],
            "{leaflet_rows:?}: subject lane"
        );

        let (rows, batched) = rows_at_t1(
            &fluree,
            "SELECT ?s WHERE { VALUES ?s { ex:s ex:s2 } ?s ex:p ex:o }",
            "batched_exists_eligible",
        )
        .await;
        assert!(batched, "{leaflet_rows:?}: existence lane must run");
        assert_eq!(
            rows,
            vec![format!("s={EX}s")],
            "{leaflet_rows:?}: existence lane"
        );
    }
}
