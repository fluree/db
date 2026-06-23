//! Planner direction benchmark for edge-annotation queries (M3.3).
//!
//! Compares query throughput across two scan paths:
//!
//! - **arena**: ledger reindexed with the api `AttachmentEventsProvider`
//!   so `annotation_index` is sealed. The stats-cache merges arena
//!   counters into `StatsView` (M3.1), giving the join planner a real
//!   row-count for each `f:reifies*` predicate.
//! - **scan**: ledger reindexed without the provider so
//!   `annotation_index = None`. Property stats for `f:reifies*` come
//!   only from `IndexStats.properties` (the regular HLL).
//!
//! Two query shapes per ledger:
//!
//! - **edge-rooted-selective**: `?ann` is found via a bound-subject
//!   edge probe (ex:alice ex:worksFor ?org { @annotation { ?ann } }).
//!   Touches one annotation. Sensitive to whether the planner orders
//!   the bound-subject edge probe before the `f:reifies*` lookups.
//! - **annotation-rooted-selective**: `?ann ex:role "Director" ;
//!   @reifies { ?person ex:worksFor ?org }`. Filters annotations by
//!   metadata, returns the edges they reify.
//!
//! ## Workload
//!
//! - 1 base edge per person, N people, N annotations total (one per
//!   edge).
//! - All edges share the same predicate (ex:worksFor) and object
//!   (ex:acme). Subjects are unique (ex:person-{i}).
//! - Annotations carry `ex:role` from a small set so the
//!   annotation-rooted query has a non-trivial filter to drive.
//!
//! Counts: 100, 1000.
//!
//! ## Running
//!
//!   cargo bench -p fluree-db-api --bench annotation_planner
//!
//! Quick validation (1 iteration each, no stats):
//!
//!   cargo bench -p fluree-db-api --bench annotation_planner -- --test

#![cfg(feature = "native")]

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_api::FlureeBuilder;
use fluree_db_indexer::IndexerConfig;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

mod support {
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

        let deadline = Instant::now() + Duration::from_secs(30);
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

const ROLES: &[&str] = &[
    "Engineer",
    "Manager",
    "Director",
    "VicePresident",
    "Architect",
];

fn edge_rooted_query() -> JsonValue {
    json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?ann", "?org"],
        "where": {
            "@id": "ex:person-0",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "@id": "?ann" }
            }
        }
    })
}

fn annotation_rooted_query() -> JsonValue {
    json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Director",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    })
}

fn bench_annotation_planner(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("annotation_planner");
    group.sample_size(20);

    for n in &[100usize, 1000] {
        group.throughput(Throughput::Elements(*n as u64));

        let (fluree_arena, ledger_arena) =
            runtime.block_on(seed_ledger_and_optionally_seal(*n, true));
        let (fluree_scan, ledger_scan) =
            runtime.block_on(seed_ledger_and_optionally_seal(*n, false));

        // Sanity: the arena ledger must have an annotation_index;
        // the scan ledger must not.
        let arena_state = runtime
            .block_on(fluree_arena.ledger(&ledger_arena))
            .unwrap();
        assert!(
            arena_state.snapshot.annotation_index.is_some(),
            "arena ledger must have annotation_index after seal"
        );
        let scan_state = runtime.block_on(fluree_scan.ledger(&ledger_scan)).unwrap();
        assert!(
            scan_state.snapshot.annotation_index.is_none(),
            "scan ledger must not have annotation_index"
        );

        let edge_q = edge_rooted_query();
        let ann_q = annotation_rooted_query();

        group.bench_with_input(BenchmarkId::new("edge-rooted-arena", n), n, |b, _| {
            b.to_async(&runtime).iter(|| async {
                let state = fluree_arena.ledger(&ledger_arena).await.unwrap();
                let db = fluree_db_api::GraphDb::from_ledger_state(&state);
                let _ = fluree_arena.query(&db, &edge_q).await.unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("edge-rooted-scan", n), n, |b, _| {
            b.to_async(&runtime).iter(|| async {
                let state = fluree_scan.ledger(&ledger_scan).await.unwrap();
                let db = fluree_db_api::GraphDb::from_ledger_state(&state);
                let _ = fluree_scan.query(&db, &edge_q).await.unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("annotation-rooted-arena", n), n, |b, _| {
            b.to_async(&runtime).iter(|| async {
                let state = fluree_arena.ledger(&ledger_arena).await.unwrap();
                let db = fluree_db_api::GraphDb::from_ledger_state(&state);
                let _ = fluree_arena.query(&db, &ann_q).await.unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("annotation-rooted-scan", n), n, |b, _| {
            b.to_async(&runtime).iter(|| async {
                let state = fluree_scan.ledger(&ledger_scan).await.unwrap();
                let db = fluree_db_api::GraphDb::from_ledger_state(&state);
                let _ = fluree_scan.query(&db, &ann_q).await.unwrap();
            });
        });
    }

    group.finish();
}

/// Seed a ledger with N `(person-i, worksFor, acme)` edges, each
/// carrying one annotation cycling through `ROLES`. Optionally seal
/// the annotation arena at the end.
async fn seed_ledger_and_optionally_seal(n: usize, seal: bool) -> (fluree_db_api::Fluree, String) {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = format!(
        "bench/annotation-planner:{}-{}",
        n,
        if seal { "arena" } else { "scan" }
    );

    let mut state = make_genesis(&fluree, &ledger_id);
    for i in 0..n {
        let role = ROLES[i % ROLES.len()];
        let txn = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:person-{i}"),
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": {
                    "@id": format!("ex:emp/{i}"),
                    "ex:role": role
                }
            }
        });
        state = fluree.insert(state, &txn).await.unwrap().ledger;
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

criterion_group!(benches, bench_annotation_planner);
criterion_main!(benches);
