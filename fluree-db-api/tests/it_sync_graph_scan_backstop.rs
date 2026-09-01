//! The whole-graph memory backstop, in a binary of its own.
//!
//! `whole_graph_scan_limit()` (fluree-db-transact/src/stage.rs) reads
//! `FLUREE_MAX_GRAPH_SCAN_FLAKES` from the process environment on every scan
//! and has no programmatic override, so setting that variable is the only way
//! to exercise the cap.
//!
//! Under bare `cargo test` a binary runs its tests as threads in one process,
//! so the cap leaks into every sibling test between the `set_var` here and the
//! `remove_var` at the end. Grouped alongside the rest of the graph-sync
//! suite, that surfaced as unrelated tests failing with
//! `WholeGraphScanTooLarge { limit: 2 }`. Being alone in this binary is what
//! makes the mutation safe — nextest's process-per-test isolation is not
//! enough on its own, because it does not apply to `cargo test`.
//!
//! See docs/contributing/tests.md, "Kept standalone".

#![cfg(feature = "native")]

mod support;

use crate::support::genesis_ledger;
use fluree_db_api::{FlureeBuilder, SyncGraphOpts};
use serde_json::{json, Value as JsonValue};

const ONT_IRI: &str = "http://example.org/graphs/ontology";
const OTHER_IRI: &str = "http://example.org/graphs/other";

fn payload_v1() -> JsonValue {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            { "@id": "ex:alice", "ex:name": "Alice", "ex:role": "engineer" },
            { "@id": "ex:bob", "ex:name": "Bob" }
        ]
    })
}

/// Seed a ledger with one default-graph triple and one triple in OTHER_IRI.
async fn seed(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    let ledger = genesis_ledger(fluree, ledger_id);
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/> .
        ex:default-subject ex:p "default-graph-value" .
        GRAPH <{OTHER_IRI}> {{
            ex:zed ex:name "Zed" .
        }}
        "#,
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed insert");
    result.receipt.t
}

/// The whole-graph memory backstop: staging materializes the target
/// graph's current flakes, so a graph past the cap must fail loud (an
/// OOM kill otherwise, with no guard in between — `NoveltyWouldExceed`
/// only ever sees the netted delta). The limit is read per scan, so the
/// env changes below take effect immediately.
#[tokio::test]
async fn whole_graph_scan_backstop_fails_loud_before_materializing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sync-graph/backstop:main";
    seed(&fluree, ledger_id).await;

    std::env::set_var("FLUREE_MAX_GRAPH_SCAN_FLAKES", "2");
    // First sync scans an EMPTY graph (0 <= 2): the cap bounds the scan,
    // not the payload, so populating past the cap succeeds.
    let report = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("first sync scans an empty graph");
    assert_eq!(report.asserted, 3);

    // Now the graph holds 3 > 2: every whole-graph verb refuses.
    let err = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect_err("resync over the cap must fail loud");
    assert!(
        err.to_string().contains("FLUREE_MAX_GRAPH_SCAN_FLAKES"),
        "error names the knob: {err}"
    );

    // Dry run takes the same scan and must fail the same way.
    let dry = fluree
        .sync_named_graph(
            ledger_id,
            ONT_IRI,
            &payload_v1(),
            SyncGraphOpts {
                dry_run: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("dry run must fail the way the real run would");
    assert!(dry.to_string().contains("FLUREE_MAX_GRAPH_SCAN_FLAKES"));

    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let clear = fluree
        .stage_owned(ledger)
        .txn(fluree_db_transact::Txn::clear_graph(ONT_IRI))
        .execute()
        .await
        .expect_err("CLEAR shares the scan and the backstop");
    assert!(clear.to_string().contains("FLUREE_MAX_GRAPH_SCAN_FLAKES"));

    // 0 disables; the identical resync is a no-op again.
    std::env::set_var("FLUREE_MAX_GRAPH_SCAN_FLAKES", "0");
    let report = fluree
        .sync_named_graph(ledger_id, ONT_IRI, &payload_v1(), SyncGraphOpts::default())
        .await
        .expect("disabled cap syncs normally");
    assert!(!report.committed);
    std::env::remove_var("FLUREE_MAX_GRAPH_SCAN_FLAKES");
}
