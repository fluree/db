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

/// Each incremental build writes a fresh HLL sketch. The collector must retire
/// the superseded one with its index version, or every build leaves a sketch
/// only a sweep can find.
#[tokio::test(flavor = "multi_thread")]
async fn collected_index_versions_leave_no_stats_sketches_behind() {
    use fluree_db_core::{ContentStore, StorageBackend, StorageMethod, StorageRead};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use std::time::Duration;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let (worker, handle) = fluree_db_api::BackgroundIndexerWorker::new(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        crate::support::collecting_indexer_config(),
    );
    tokio::spawn(worker.run());
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    let ledger_name = "sketchgc";
    let ledger_id = fluree_db_api::LedgerId::parse("sketchgc:main").unwrap();
    let index_cfg = fluree_db_api::IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000_000,
    };
    fluree.create_ledger(ledger_name).await.expect("create");

    let mut first_root = None;
    for round in 0..5 {
        let ledger = fluree.ledger(ledger_id.as_str()).await.expect("load");
        let result = fluree
            .insert_with_opts(
                ledger,
                &json!({
                    "@context": {"ex": "http://example.org/"},
                    "@id": format!("ex:s{round}"),
                    "ex:v": round
                }),
                TxnOpts::default(),
                CommitOpts::default(),
                &index_cfg,
            )
            .await
            .expect("insert");
        let completion = handle.trigger(&ledger_id, result.receipt.t).await;
        match tokio::time::timeout(Duration::from_secs(60), completion.wait())
            .await
            .expect("build timed out")
        {
            fluree_db_api::IndexOutcome::Completed { .. } => {}
            other => panic!("build {round} did not complete: {other:?}"),
        }
        if first_root.is_none() {
            first_root = fluree
                .nameservice()
                .lookup(ledger_id.as_str())
                .await
                .expect("lookup")
                .expect("record")
                .index_head_id;
        }
    }
    let first_root = first_root.expect("first build published");

    // The collector really ran: the first version is past retention.
    let store = fluree.content_store(ledger_id.as_str());
    let mut collected = false;
    for _ in 0..100 {
        if !store.has(&first_root).await.expect("has") {
            collected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        collected,
        "the collector never released the first index version"
    );
    drop(
        handle
            .hold_gc(&fluree_db_api::LedgerName::parse(ledger_name).unwrap())
            .await,
    );

    let StorageBackend::Managed(storage) = fluree.backend().clone() else {
        panic!("file builder must yield a managed backend");
    };
    let sketches = storage
        .list_prefix(&format!(
            "fluree:{}://{}/index/stats/",
            storage.storage_method(),
            ledger_id.path_prefix()
        ))
        .await
        .expect("list sketches");
    assert!(!sketches.is_empty(), "builds must write stats sketches");

    let plan = fluree.plan_index_sweep(ledger_name).await.expect("plan");
    let leaked: Vec<_> = plan
        .orphans
        .iter()
        .filter(|addr| addr.contains("/index/stats/"))
        .collect();
    assert!(
        leaked.is_empty(),
        "sketches of collected versions were left behind: {leaked:?}"
    );
}

/// Ledgers indexed before the collector retired superseded sketches still hold
/// one per collected version. The sweep must reclaim those and keep the one
/// the head root references — no query reads it, so only the next incremental
/// build would notice it gone.
#[tokio::test]
async fn sweep_reclaims_leftover_stats_sketches_and_keeps_the_heads() {
    use fluree_db_api::ReindexOptions;
    use fluree_db_core::{ContentKind, StorageBackend, StorageRead, StorageWrite};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "sketchsweep:main";
    fluree.create_ledger("sketchsweep").await.expect("create");
    let cached = fluree.ledger_cached("sketchsweep").await.expect("cache");
    fluree
        .stage(&cached)
        .insert(&json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:v": 1}))
        .execute()
        .await
        .expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    let StorageBackend::Managed(storage) = fluree.backend().clone() else {
        panic!("memory builder must yield a managed backend");
    };
    let stats_prefix = format!(
        "fluree:{}://{}/index/stats/",
        storage.storage_method(),
        fluree_db_api::LedgerId::parse(ledger_id)
            .unwrap()
            .path_prefix()
    );
    let heads = storage.list_prefix(&stats_prefix).await.expect("list");
    assert_eq!(heads.len(), 1, "the build writes one sketch: {heads:?}");

    let leftover = fluree_db_core::ContentId::new(ContentKind::StatsSketch, b"collected version");
    let leftover_addr = fluree_db_core::content_address(
        storage.storage_method(),
        ContentKind::StatsSketch,
        ledger_id,
        &leftover.digest_hex(),
    );
    storage
        .write_bytes(&leftover_addr, b"{}")
        .await
        .expect("write leftover");

    let plan = fluree.plan_index_sweep("sketchsweep").await.expect("plan");
    let stats_orphans: Vec<_> = plan
        .orphans
        .iter()
        .filter(|addr| addr.starts_with(&stats_prefix))
        .collect();
    assert_eq!(stats_orphans, vec![&leftover_addr]);

    let result = fluree
        .sweep_index_storage("sketchsweep")
        .await
        .expect("sweep");
    assert!(result.failures.is_empty(), "{:?}", result.failures);
    assert_eq!(
        storage.list_prefix(&stats_prefix).await.expect("list"),
        heads,
        "only the head's sketch survives"
    );
}

/// A reindex writes index artifacts before publishing the root that references
/// them, so a sweep running alongside it would see them as unreferenced and
/// delete them. Reindex must therefore take the same exclusive hold the sweep
/// does, and report a conflict when it cannot (#1548).
#[tokio::test]
async fn reindex_refuses_while_the_ledger_is_held_for_maintenance() {
    use fluree_db_api::ReindexOptions;

    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, handle) = crate::support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async {
            fluree.create_ledger("holdtest").await.expect("create");
            let cached = fluree.ledger_cached("holdtest").await.expect("cache");
            fluree
                .stage(&cached)
                .insert(&json!({
                    "@context": {"ex": "http://example.org/"},
                    "@id": "ex:a",
                    "ex:v": 1
                }))
                .execute()
                .await
                .expect("insert");

            // Stand in for a sweep already holding the ledger.
            let _held = handle
                .acquire_maintenance(&fluree_db_api::LedgerId::parse("holdtest:main").unwrap())
                .expect("acquire succeeds");

            let err = fluree
                .reindex("holdtest:main", ReindexOptions::default())
                .await
                .expect_err("reindex must not proceed while the ledger is held");

            assert!(
                err.to_string().contains("another maintenance operation"),
                "expected a maintenance conflict, got: {err}"
            );
        })
        .await;
}
