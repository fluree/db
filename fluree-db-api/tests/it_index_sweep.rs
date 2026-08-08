//! Storage-sweep entry points on `Fluree`.

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// A ledger whose index chain is intact has nothing to reclaim, and planning
/// says so without touching storage.
#[tokio::test]
async fn planning_a_healthy_ledger_finds_no_orphans() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("sweeptest").await.expect("create");
    let cached = fluree.ledger_cached("sweeptest").await.expect("cache");
    fluree
        .stage(&cached)
        .insert(&json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:v": 1}))
        .execute()
        .await
        .expect("insert");

    let plan = fluree
        .plan_index_sweep("sweeptest")
        .await
        .expect("planning succeeds");

    assert!(
        plan.orphans.is_empty(),
        "an intact ledger has nothing orphaned: {:?}",
        plan.orphans
    );
}

/// Sweeping names a ledger, not a branch. A `name:branch` argument would
/// silently sweep nothing, so it must not be mistaken for a valid target.
#[tokio::test]
async fn sweeping_an_unknown_ledger_reports_not_found() {
    let fluree = FlureeBuilder::memory().build_memory();

    let err = fluree
        .plan_index_sweep("nosuchledger")
        .await
        .expect_err("an unknown ledger has no branches to hold");

    assert!(
        err.to_string().contains("Not found"),
        "expected a not-found error, got: {err}"
    );
}

/// Reclaiming a healthy ledger is a no-op rather than an error, so operators
/// can run it on a schedule without special-casing the nothing-to-do case.
#[tokio::test]
async fn sweeping_a_healthy_ledger_reclaims_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("sweeptest").await.expect("create");
    let cached = fluree.ledger_cached("sweeptest").await.expect("cache");
    fluree
        .stage(&cached)
        .insert(&json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:v": 1}))
        .execute()
        .await
        .expect("insert");

    let result = fluree
        .sweep_index_storage("sweeptest")
        .await
        .expect("sweeping succeeds");

    assert_eq!(result.reclaimed, 0);
    assert!(result.failures.is_empty(), "{:?}", result.failures);
}

/// End-to-end over a real index: build a ledger, index it, reindex it, inject a
/// stray artifact, sweep, and query.
///
/// Every other sweep test uses synthetic roots holding a single dictionary CID.
/// This one exercises the live set against artifacts a real build produces —
/// leaves, dictionaries, and whatever sits behind branch manifests — because
/// the failure that matters is `collect_root_cas_ids_expanded` missing a CID
/// some root genuinely references. That would classify a live artifact as
/// orphaned, and the only way to observe it is to delete and then read.
#[tokio::test]
async fn sweeping_a_real_ledger_reclaims_strays_and_leaves_queries_intact() {
    use crate::support::{genesis_ledger_for_fluree, query_jsonld_formatted};
    use fluree_db_api::ReindexOptions;
    use fluree_db_core::{ContentKind, StorageBackend, StorageRead, StorageWrite};
    use fluree_db_transact::{CommitOpts, TxnOpts};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sweep-e2e:main";
    let ledger_name = "it/sweep-e2e";

    // Hold off background indexing so the reindex below is the only build.
    let no_background = fluree_db_api::IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    let ledger0 = genesis_ledger_for_fluree(&fluree, ledger_id);
    let ledger1 = fluree
        .insert_with_opts(
            ledger0,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30},
                    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob", "ex:age": 25},
                    {"@id": "ex:acme", "@type": "ex:Organization", "ex:name": "Acme"}
                ]
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_background,
        )
        .await
        .expect("first insert")
        .ledger;

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("first reindex");

    // A second generation, so the chain has a superseded root to reason about.
    fluree
        .insert_with_opts(
            ledger1,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:carol",
                "@type": "ex:Person",
                "ex:name": "Carol",
                "ex:age": 41
            }),
            TxnOpts::default(),
            CommitOpts::default(),
            &no_background,
        )
        .await
        .expect("second insert");

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("second reindex");

    // Inject an artifact no root references — the shape a severed chain leaves
    // behind. Written through the same addressing the indexer uses.
    let StorageBackend::Managed(storage) = fluree.backend().clone() else {
        panic!("memory builder must yield a managed backend");
    };
    let stray = fluree_db_core::ContentId::new(ContentKind::IndexLeaf, b"orphaned-leaf");
    let stray_addr = fluree_db_core::content_address(
        storage.storage_method(),
        ContentKind::IndexLeaf,
        ledger_id,
        &stray.digest_hex(),
    );
    storage
        .write_bytes(&stray_addr, b"orphaned leaf bytes")
        .await
        .expect("write stray");

    let plan = fluree
        .plan_index_sweep(ledger_name)
        .await
        .expect("plan succeeds against a real index");
    assert!(
        plan.live > 0,
        "a real index must contribute reachable artifacts; got live={}",
        plan.live
    );
    assert!(
        plan.orphans.contains(&stray_addr),
        "the injected stray must be reclaimable"
    );

    let result = fluree
        .sweep_index_storage(ledger_name)
        .await
        .expect("sweep succeeds");
    assert!(result.reclaimed >= 1);
    assert!(result.failures.is_empty(), "{:?}", result.failures);
    assert!(
        !storage.exists(&stray_addr).await.expect("exists"),
        "the stray artifact is gone"
    );

    // The real assertion: a query served from the swept index still returns
    // every row. If the live set missed a CID, the read fails or comes up short.
    let loaded = fluree.ledger(ledger_id).await.expect("load after sweep");
    let results = query_jsonld_formatted(
        &fluree,
        &loaded,
        &json!({
            "@context": { "ex": "http://example.org/" },
            "select": { "?s": ["*"] },
            "where": { "@id": "?s", "@type": "ex:Person" }
        }),
    )
    .await
    .expect("query after sweep");

    let rows = results.as_array().expect("select returns an array");
    assert_eq!(
        rows.len(),
        3,
        "all three people survive the sweep: {results}"
    );
}
