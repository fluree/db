//! Annotation hydration: M2a scan vs M2b arena.
//!
//! Compares the wall-clock cost of `HydrationFormatter::inject_annotations`
//! across two read paths. The bench uses a **subject-hydration** query
//! (`select: {"?person": ["*", {"ex:worksFor": ["*"]}]}`) so the
//! `@annotation` body is materialized via the formatter's
//! `inject_annotations` call site — a flat select with `@annotation`
//! in the where clause goes through the sync JSON-LD formatter and
//! never reaches the arena reader.
//!
//! Both ledgers are reindexed before the bench runs:
//!
//! - **scan**: reindex without an `AttachmentEventsProvider` →
//!   indexed root carries `has_annotations=true,
//!   annotation_index=None`. `inject_annotations` falls through to a
//!   POST range query for `f:reifiesSubject` against the indexed
//!   base. This is the M2a indexed-scan-fallback path, not the
//!   novelty-only path.
//! - **arena**: reindex with the api `AttachmentEventsProvider` →
//!   `annotation_index` is sealed. `inject_annotations` constructs
//!   an `AnnotationArenaReader` once per response and resolves each
//!   edge via the merged forward arena.
//!
//! ## Workload
//!
//! One edge with N attachments. The query hydrates `ex:alice` →
//! the worksFor ref expansion triggers `inject_annotations`, which
//! must surface every live annotation.
//!
//! Counts: 1, 100, 10_000.
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench annotation_hydration
//!
//! Quick validation (1 iteration each, no stats):
//!
//!   cargo bench -p fluree-db-api --bench annotation_hydration -- --test

#![cfg(feature = "native")]

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_api::FlureeBuilder;
use fluree_db_indexer::IndexerConfig;
use serde_json::json;
use tokio::runtime::Runtime;

mod support {
    // Local copy of the test-support helpers needed by the bench.
    // We can't reach into the integration tests' `support` module
    // from a bench target, so the relevant helpers are duplicated.
    use async_trait::async_trait;
    use fluree_db_api::{Fluree, LedgerManager, NsNotify};
    use fluree_db_indexer::{AttachmentEventCoverage, AttachmentEventsProvider};
    use std::sync::Arc;
    use tokio::task::LocalSet;

    pub fn start_background_indexer_with_attachments(
        fluree: &Fluree,
        config: fluree_db_indexer::IndexerConfig,
    ) -> (LocalSet, fluree_db_indexer::IndexerHandle) {
        struct TestProvider {
            manager: Arc<LedgerManager>,
        }

        impl std::fmt::Debug for TestProvider {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestProvider").finish()
            }
        }

        #[async_trait]
        impl AttachmentEventsProvider for TestProvider {
            async fn attachment_events(&self, ledger_id: &str) -> Option<AttachmentEventCoverage> {
                use fluree_db_api::ledger_manager::RunningCoverage;
                let result = self
                    .manager
                    .try_running_attachment_events(ledger_id)
                    .await?;
                Some(match result.coverage {
                    RunningCoverage::Authoritative => {
                        AttachmentEventCoverage::Authoritative(result.events)
                    }
                    RunningCoverage::Augment => AttachmentEventCoverage::Augment(result.events),
                })
            }
        }

        let manager = fluree
            .ledger_manager()
            .expect("ledger caching must be enabled")
            .clone();
        let provider: Arc<dyn AttachmentEventsProvider> = Arc::new(TestProvider { manager });
        let config = config.with_attachment_events_provider(provider);

        let (worker, handle) = fluree_db_api::BackgroundIndexerWorker::new(
            fluree.backend().clone(),
            Arc::new(fluree.nameservice_mode().clone()),
            config,
        );
        let local = LocalSet::new();
        local.spawn_local(worker.run());
        (local, handle)
    }

    pub async fn wait_for_index_application(fluree: &Fluree, ledger_id: &str, target_index_t: i64) {
        use std::time::{Duration, Instant};

        let ns_record = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .expect("ns lookup")
            .expect("ns record");
        let canonical = ns_record.ledger_id.clone();
        let mgr = fluree.ledger_manager().expect("caching enabled");
        let _ = mgr
            .notify(NsNotify {
                ledger_id: canonical.clone(),
                record: Some(ns_record),
            })
            .await
            .expect("notify");

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let handle = fluree
                .ledger_cached(&canonical)
                .await
                .expect("ledger_cached");
            let view = handle.snapshot().await;
            if view.snapshot.t >= target_index_t {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for index application; current snapshot.t={}",
                view.snapshot.t
            );
            tokio::task::yield_now().await;
        }
    }
}

fn bench_annotation_hydration(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("annotation_hydration");
    group.sample_size(20);

    for n in &[1usize, 100, 10_000] {
        group.throughput(Throughput::Elements(*n as u64));

        // ---- Build two ledgers seeded with N attachments ----
        // One stays in scan-fallback (no provider on its worker).
        // The other gets an arena via the full provider chain.

        let (fluree_scan, ledger_id_scan) =
            runtime.block_on(seed_ledger_and_optionally_seal(*n, false));
        let (fluree_arena, ledger_id_arena) =
            runtime.block_on(seed_ledger_and_optionally_seal(*n, true));

        // Sanity: the arena ledger must have annotation_index set;
        // the scan ledger must not.
        let arena_state = runtime
            .block_on(fluree_arena.ledger(&ledger_id_arena))
            .unwrap();
        assert!(
            arena_state.snapshot.has_arena_reader(),
            "arena ledger must have arena reader"
        );
        let scan_state = runtime
            .block_on(fluree_scan.ledger(&ledger_id_scan))
            .unwrap();
        assert!(
            !scan_state.snapshot.has_arena_reader(),
            "scan ledger must not have arena reader"
        );

        // Subject-hydration query — `inject_annotations` fires
        // when the worksFor ref value is materialized during
        // expansion of `ex:alice`. A flat `select` with
        // `@annotation` in the `where` clause would route through
        // the sync JSON-LD formatter and never hit the arena
        // reader.
        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
            "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
        });

        group.bench_with_input(BenchmarkId::new("scan", n), n, |b, _| {
            b.to_async(&runtime).iter(|| async {
                let state = fluree_scan.ledger(&ledger_id_scan).await.unwrap();
                let db = fluree_db_api::GraphDb::from_ledger_state(&state);
                let result = fluree_scan.query(&db, &query).await.unwrap();
                let _ = result.to_jsonld_async(db.as_graph_db_ref()).await.unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("arena", n), n, |b, _| {
            b.to_async(&runtime).iter(|| async {
                let state = fluree_arena.ledger(&ledger_id_arena).await.unwrap();
                let db = fluree_db_api::GraphDb::from_ledger_state(&state);
                let result = fluree_arena.query(&db, &query).await.unwrap();
                let _ = result.to_jsonld_async(db.as_graph_db_ref()).await.unwrap();
            });
        });
    }

    group.finish();
}

/// Build a ledger with N annotations on one edge, then reindex.
///
/// - `seal=true`: reindex with the api `AttachmentEventsProvider`
///   attached → `annotation_index` is populated, hydration takes
///   the arena path.
/// - `seal=false`: reindex with a bare worker (no provider) →
///   indexed root has `has_annotations=true, annotation_index=None`,
///   hydration takes the M2a indexed-scan-fallback path. The data
///   lives in the indexed POST, not in novelty.
async fn seed_ledger_and_optionally_seal(n: usize, seal: bool) -> (fluree_db_api::Fluree, String) {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = format!(
        "bench/annotation-hydration:{}-{}",
        n,
        if seal { "arena" } else { "scan" }
    );

    // Bulk insert: one base edge plus N annotations on it. We do
    // this in chunks of 1000 to keep transaction sizes reasonable.
    let mut state = make_genesis(&fluree, &ledger_id);
    let chunk_size = 1000usize;
    let mut emitted = 0usize;
    let base = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:alice",
        "ex:worksFor": { "@id": "ex:acme" }
    });
    state = fluree.insert(state, &base).await.unwrap().ledger;
    while emitted < n {
        let count = (n - emitted).min(chunk_size);
        let mut graph = Vec::with_capacity(count);
        for i in 0..count {
            graph.push(json!({
                "@id": format!("ex:emp/alice-acme-{}", emitted + i),
                "ex:worksFor": "@reifiesEdge",
                "ex:role": format!("Role-{}", emitted + i)
            }));
            // Note: above is not a valid annotation shape — we
            // construct annotations via the proper inline form below.
            // Drop the placeholder.
        }
        graph.clear();

        // Inline-annotation form. Because the M1 lowering only
        // attaches one annotation per insert (the @annotation key),
        // we issue one transaction per annotation. For large N this
        // is slow but accurate — bulk-insert APIs for annotations
        // aren't part of v1.
        for i in 0..count {
            let txn = json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": format!("ex:emp/alice-acme-{}", emitted + i),
                        "ex:role": format!("Role-{}", emitted + i)
                    }
                }
            });
            state = fluree.insert(state, &txn).await.unwrap().ledger;
        }
        emitted += count;
    }

    let receipt_t = state.t();
    let (local, handle) = if seal {
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small())
    } else {
        let (worker, handle) = fluree_db_api::BackgroundIndexerWorker::new(
            fluree.backend().clone(),
            Arc::new(fluree.nameservice_mode().clone()),
            IndexerConfig::small(),
        );
        let local = tokio::task::LocalSet::new();
        local.spawn_local(worker.run());
        (local, handle)
    };
    local
        .run_until(async {
            // Cache the ledger pre-trigger so the provider (when
            // attached) sees its overlay events.
            let _ = fluree.ledger_cached(&ledger_id).await.unwrap();
            let completion = handle.trigger(&ledger_id, receipt_t).await;
            let _ = completion.wait().await;
            support::wait_for_index_application(&fluree, &ledger_id, receipt_t).await;
        })
        .await;

    (fluree, ledger_id)
}

fn make_genesis(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> fluree_db_api::LedgerState {
    let canonical = fluree_db_core::ledger_id::normalize_ledger_id(ledger_id)
        .unwrap_or_else(|_| ledger_id.to_string());
    let snapshot = fluree_db_core::LedgerSnapshot::genesis(&canonical);
    let _ = fluree;
    fluree_db_api::LedgerState::new(snapshot, fluree_db_api::Novelty::new(0))
}

criterion_group!(benches, bench_annotation_hydration);
criterion_main!(benches);
