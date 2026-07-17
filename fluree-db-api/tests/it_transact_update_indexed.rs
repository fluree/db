//! Multi-operation SPARQL UPDATE against **real storage**: binary-index
//! merges and commit-envelope reloads (PR-1454 review).
//!
//! The memory-backed multi-op tests in `it_transact_update.rs` prove the
//! sequential-staging semantics; these prove the two envelope-level
//! properties the fold relies on survive real persistence:
//!
//! 1. a dangling retract (`INSERT x ; DELETE x` netting to a retract of a
//!    never-existing fact) crossing a genuine novelty→index merge, and the
//!    cumulative namespace delta round-tripping a real commit envelope;
//! 2. the persisted `graph_delta` id↔IRI mapping agreeing with the ids the
//!    staged flakes actually carry, proven by reloading from disk (the live
//!    view self-heals from the in-memory registry; a reload trusts the
//!    envelope).

#![cfg(feature = "native")]

use crate::support::{
    query_sparql, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, IndexConfig};

/// The `INSERT x ; DELETE x` fold nets to a retract of a fact the base
/// never contained. The commit's novelty carries that dangling retract and
/// the index merge must cross it without resurrecting or corrupting
/// anything; the third operation's namespace (introduced only mid-request)
/// must round-trip the commit envelope and still resolve after a cold
/// reload from disk.
#[tokio::test]
async fn multi_op_dangling_retract_and_ns_delta_survive_index_merge_and_reload() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path.clone())
        .build()
        .expect("build file fluree");
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };
    let ledger_id = "it/multiop-indexed:main";

    local
        .run_until(async {
            fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");

            // Establish a binary-index base.
            let seeded = fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(
                    r#"PREFIX ex: <http://example.org/ns/>
                       INSERT DATA { ex:base ex:p "seed" }"#,
                )
                .index_config(index_cfg.clone())
                .commit()
                .await
                .expect("seed commit");
            trigger_index_and_wait_outcome(&handle, ledger_id, seeded.receipt.t).await;

            // The multi-op request: op 1+2 net to a dangling retract, op 3
            // introduces a namespace no earlier operation (or the base)
            // knows.
            let request = fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(
                    r#"PREFIX ex: <http://example.org/ns/>
                       PREFIX fresh: <http://fresh.example/only-in-op-3/>
                       INSERT DATA { ex:gone ex:p "dangling" } ;
                       DELETE DATA { ex:gone ex:p "dangling" } ;
                       INSERT DATA { fresh:kept fresh:q "kept" }"#,
                )
                .index_config(index_cfg.clone())
                .commit()
                .await
                .expect("multi-op request commits");

            // Merge the request's novelty — dangling retract included —
            // into the binary index.
            trigger_index_and_wait_outcome(&handle, ledger_id, request.receipt.t).await;
            request.receipt.t
        })
        .await;

    // Cold reload from disk: the commit envelope (namespace map included)
    // is all the new instance knows.
    drop(fluree);
    let fluree2 = FlureeBuilder::file(path)
        .build()
        .expect("rebuild file fluree");
    let ledger = fluree2.ledger(ledger_id).await.expect("reload ledger");

    let json = query_sparql(
        &fluree2,
        &ledger,
        r"SELECT ?o WHERE { <http://example.org/ns/gone> <http://example.org/ns/p> ?o }",
    )
    .await
    .expect("query dangling")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    assert!(
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .is_empty(),
        "the dangling retract must net to absence across the index merge: {json}"
    );

    let json = query_sparql(
        &fluree2,
        &ledger,
        r"SELECT ?o WHERE { <http://fresh.example/only-in-op-3/kept> <http://fresh.example/only-in-op-3/q> ?o }",
    )
    .await
    .expect("query fresh ns")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    assert_eq!(
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .len(),
        1,
        "op 3's namespace must round-trip the commit envelope and reload: {json}"
    );

    let json = query_sparql(
        &fluree2,
        &ledger,
        r"SELECT ?o WHERE { <http://example.org/ns/base> <http://example.org/ns/p> ?o }",
    )
    .await
    .expect("query base")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    assert_eq!(
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .len(),
        1,
        "pre-request data must survive untouched: {json}"
    );
}

/// PR-1454 review (bplatz): per-op staging assigns new GraphIds in arrival
/// order while `build_commit` may assign over a sorted union — if the
/// persisted `commit_record.graph_delta` mapping disagrees with the ids the
/// staged flakes carry, a reload routes named-graph data to the WRONG
/// graph. Arrival order (zzz before aaa) deliberately inverts sorted order
/// so any re-assignment is visible.
#[tokio::test]
async fn multi_op_graph_delta_mapping_survives_reload() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path.clone())
        .build()
        .expect("build file fluree");
    let ledger_id = "it/multiop-graph-delta:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(
            r#"PREFIX ex: <http://example.org/ns/>
               INSERT DATA { GRAPH <urn:g:zzz> { ex:a ex:p "in-zzz" } } ;
               INSERT DATA { GRAPH <urn:g:aaa> { ex:b ex:p "in-aaa" } }"#,
        )
        .commit()
        .await
        .expect("multi-op named-graph request commits");

    let assert_routing = |json_zzz: serde_json::Value, json_aaa: serde_json::Value, when: &str| {
        let zzz = json_zzz["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["o"]["value"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            zzz,
            vec!["in-zzz".to_string()],
            "{when}: <urn:g:zzz> must contain exactly its own data"
        );
        let aaa = json_aaa["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["o"]["value"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            aaa,
            vec!["in-aaa".to_string()],
            "{when}: <urn:g:aaa> must contain exactly its own data"
        );
    };

    let q_zzz = r"SELECT ?o WHERE { GRAPH <urn:g:zzz> { ?s ?p ?o } }";
    let q_aaa = r"SELECT ?o WHERE { GRAPH <urn:g:aaa> { ?s ?p ?o } }";

    // Live view (self-heals from the in-memory registry) — sanity.
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let live_zzz = query_sparql(&fluree, &ledger, q_zzz)
        .await
        .expect("live zzz")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    let live_aaa = query_sparql(&fluree, &ledger, q_aaa)
        .await
        .expect("live aaa")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    assert_routing(live_zzz, live_aaa, "live view");

    // Cold reload: the persisted graph_delta is all the new instance has.
    drop(fluree);
    let fluree2 = FlureeBuilder::file(path)
        .build()
        .expect("rebuild file fluree");
    let ledger = fluree2.ledger(ledger_id).await.expect("reload ledger");
    let cold_zzz = query_sparql(&fluree2, &ledger, q_zzz)
        .await
        .expect("cold zzz")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    let cold_aaa = query_sparql(&fluree2, &ledger, q_aaa)
        .await
        .expect("cold aaa")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    assert_routing(cold_zzz, cold_aaa, "cold reload");
}
