//! Pre-rebuild performance regression guard for the reindex hot path.
//!
//! Two regressions converged to push small-ledger reindexes past the 900s
//! Lambda ceiling on a 787-commit ledger:
//!
//! 1. `Novelty::apply_commit` was called per-commit during
//!    `LedgerState::load_novelty`'s catch-up, accumulating `O(M·N̄)` work
//!    via repeated two-way `merge_batch_into_index` calls. Replaced with
//!    `Novelty::bulk_apply_commits` (`O(N log N)`).
//!
//! 2. `ApiFulltextConfigProvider::resolve` always called
//!    `LedgerState::load`, so even when the config graph had never been
//!    written the resolver paid for a full chain walk plus full novelty
//!    build just to discover it was empty. Replaced with an
//!    envelope-only probe via
//!    [`first_t_where_graph_registered`] that short-circuits when the
//!    config graph IRI never appears in any commit's `graph_delta`.
//!
//! Tests in this file lock both fixes in:
//!
//! - [`first_t_where_graph_registered_no_full_reads_when_iri_absent`] —
//!   deterministic counting test that asserts the envelope walk uses
//!   only `get_range`, never `get`, when the target IRI is missing.
//! - [`first_t_where_graph_registered_returns_lowest_t_when_iri_present`]
//!   — confirms the helper actually finds the registration `t`.
//! - [`orchestrator_first_reindex_no_config_completes_quickly`] —
//!   end-to-end orchestrator-path reindex of a multi-commit ledger
//!   with no config graph, asserting wall-clock stays well under the
//!   pre-fix regression scale. Smoke-test for both fixes wired
//!   together.

#![cfg(feature = "native")]

use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::error::Result as StorageResult;
use fluree_db_core::{
    config_graph_iri, first_t_where_graph_registered, ContentId, ContentKind, ContentStore,
    LedgerSnapshot,
};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod support;

/// Atomic counters tracking every read through a [`CountingContentStore`].
#[derive(Default)]
struct FetchCounters {
    get_calls: AtomicU64,
    get_range_calls: AtomicU64,
}

impl std::fmt::Debug for FetchCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchCounters")
            .field("get_calls", &self.get_calls.load(Ordering::Relaxed))
            .field(
                "get_range_calls",
                &self.get_range_calls.load(Ordering::Relaxed),
            )
            .finish()
    }
}

/// `ContentStore` wrapper that counts read traffic. Mirrors the helper in
/// `it_rebuild_fetch_counts.rs` but slimmed to just the counters we need
/// here (envelope-walk verification doesn't care about byte totals).
#[derive(Clone, Debug)]
struct CountingContentStore<C> {
    inner: C,
    counters: Arc<FetchCounters>,
}

impl<C: ContentStore> CountingContentStore<C> {
    fn new(inner: C) -> Self {
        Self {
            inner,
            counters: Arc::new(FetchCounters::default()),
        }
    }

    fn counters(&self) -> Arc<FetchCounters> {
        self.counters.clone()
    }
}

#[async_trait]
impl<C: ContentStore + Send + Sync> ContentStore for CountingContentStore<C> {
    async fn has(&self, id: &ContentId) -> StorageResult<bool> {
        self.inner.has(id).await
    }

    async fn get(&self, id: &ContentId) -> StorageResult<Vec<u8>> {
        self.counters.get_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get(id).await
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> StorageResult<ContentId> {
        self.inner.put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> StorageResult<()> {
        self.inner.put_with_id(id, bytes).await
    }

    async fn release(&self, id: &ContentId) -> StorageResult<()> {
        self.inner.release(id).await
    }

    async fn get_range(
        &self,
        id: &ContentId,
        range: std::ops::Range<u64>,
    ) -> StorageResult<Vec<u8>> {
        self.counters
            .get_range_calls
            .fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(id, range).await
    }
}

/// Seed `n` commits into a fresh in-memory ledger. None of these commits
/// touch the config graph — they're plain entity inserts, mirroring a
/// production scenario where no `f:LedgerConfig` ever exists.
async fn seed_no_config_commits(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    n: usize,
) -> LedgerState {
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));
    let no_auto = IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };
    for i in 0..n {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:entity{i}"),
            "@type": "ex:Entity",
            "ex:label": format!("entity {i}"),
            "ex:value": i as i64,
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                Default::default(),
                Default::default(),
                &no_auto,
            )
            .await
            .expect("insert_with_opts")
            .ledger;
    }
    ledger
}

#[tokio::test]
async fn first_t_where_graph_registered_no_full_reads_when_iri_absent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/envelope-shortcircuit-no-config:main";

    const N: usize = 8;
    let _ledger = seed_no_config_commits(&fluree, ledger_id, N).await;

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record");
    assert_eq!(record.commit_t, N as i64);

    let counted = CountingContentStore::new(fluree.content_store(ledger_id));
    let counters = counted.counters();
    let head = record.commit_head_id.expect("head");

    let cfg_iri = config_graph_iri(ledger_id);
    let registered = first_t_where_graph_registered(&counted, &head, &cfg_iri)
        .await
        .expect("envelope walk");

    assert!(
        registered.is_none(),
        "config graph never registered, expected None, got {registered:?}"
    );

    let get_calls = counters.get_calls.load(Ordering::Relaxed);
    let get_range_calls = counters.get_range_calls.load(Ordering::Relaxed);

    // Envelope-only probe must use byte-range exclusively. Any future
    // refactor that re-introduces a full-blob fetch (e.g., reverting to
    // `load_commit_by_id`) flips `get_calls` non-zero and fails here.
    assert_eq!(
        get_calls, 0,
        "envelope walk must NOT issue full-blob `get` calls; got {get_calls}"
    );
    // One envelope range read per commit.
    assert_eq!(
        get_range_calls, N as u64,
        "envelope walk must issue exactly {N} byte-range reads; got {get_range_calls}"
    );
}

#[tokio::test]
async fn first_t_where_graph_registered_returns_lowest_t_when_iri_present() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/envelope-shortcircuit-with-config:main";

    let no_auto = IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // Genesis: write a single triple to the config graph in the very first
    // commit. The config graph IRI is `urn:fluree:<ledger_id>#config` per
    // `config_graph_iri`, so a TriG `GRAPH <…#config>` block lands flakes
    // there.
    let cfg_iri = config_graph_iri(ledger_id);
    let trig = format!(
        "@prefix f: <https://ns.flur.ee/db#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         GRAPH <{cfg_iri}> {{\n\
            <urn:config:main> rdf:type f:LedgerConfig .\n\
         }}\n"
    );

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));
    ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("write config commit (t=1)")
        .ledger;

    // Add a few non-config commits afterwards (t=2..5).
    for i in 0..4 {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:doc{i}"),
            "@type": "ex:Doc",
            "ex:title": format!("Doc {i}"),
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                Default::default(),
                Default::default(),
                &no_auto,
            )
            .await
            .expect("insert_with_opts")
            .ledger;
    }

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record");
    let head = record.commit_head_id.expect("head");

    let registered =
        first_t_where_graph_registered(&fluree.content_store(ledger_id), &head, &cfg_iri)
            .await
            .expect("envelope walk");

    assert_eq!(
        registered,
        Some(1),
        "config graph was registered at the genesis commit (t=1)"
    );
}

/// End-to-end orchestrator-path reindex on a no-config ledger. Mirrors the
/// AWS Lambda configuration: `BackgroundIndexerWorker` with the API's
/// `ApiFulltextConfigProvider` attached, `IndexerHandle::trigger`,
/// `wait().await`. Asserts wall-clock stays well under the pre-fix
/// regression scale (production reindexes sat past the 13-min waiter
/// timeout repeatedly before the fix).
///
/// Wall-clock budgets are necessarily soft on CI hardware, but we set the
/// budget at a level that comfortably passes locally yet would fail under
/// any reintroduction of the full-chain `LedgerState::load` path on a
/// no-config ledger.
#[tokio::test]
async fn orchestrator_first_reindex_no_config_completes_quickly() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/orchestrator-perf-no-config:main";

    const N: usize = 200;
    let _ledger = seed_no_config_commits(&fluree, ledger_id, N).await;

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record");
    assert_eq!(record.commit_t, N as i64);
    assert!(
        record.index_head_id.is_none(),
        "expected no prior index — this test exercises the first-build path"
    );

    // Wire the orchestrator the same way `start_background_indexing_dyn`
    // does for AWS: provider attached so `build_index_for_record` invokes
    // it before dispatching.
    let cfg = fluree_db_indexer::IndexerConfig::small()
        .with_fulltext_config_provider(fluree.fulltext_config_provider());
    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        cfg,
    );

    let started = Instant::now();
    let outcome = local
        .run_until(async move {
            let completion = handle.trigger(ledger_id, N as i64).await;
            completion.wait().await
        })
        .await;
    let elapsed = started.elapsed();

    match outcome {
        fluree_db_api::IndexOutcome::Completed { index_t, .. } => {
            assert_eq!(index_t, N as i64);
        }
        fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
        fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
    }

    // 60s is generous on slow CI but well below the 13-min waiter timeout
    // that motivated this fix. A regression in either fix (per-commit
    // novelty O(N²) replay or full LedgerState::load on no-config
    // ledgers) would push this past the budget on Lambda; on CI the
    // multi-core machine masks per-commit O(N²) costs somewhat, but a
    // FULL `LedgerState::load` walk on 200 commits with the provider
    // attached would still register clearly above the noise floor here.
    const BUDGET: Duration = Duration::from_secs(60);
    assert!(
        elapsed < BUDGET,
        "orchestrator-path reindex of {N} commits took {elapsed:?}, expected < {BUDGET:?}"
    );
}
