//! Committed data that the index cannot store halts background indexing
//! (`IndexStatusResult::halted`), on the incremental path and on the full
//! rebuild path, and an incremental build does not fall back to a rebuild
//! for it.
//!
//! The data is `tests/fixtures/vector-lengths-4.2.3`: a ledger the v4.2.3 CLI
//! wrote, indexed at t=1, holding at t=2 a vector of another length on the
//! same property. The index keeps one vector length per property. The first
//! build over that v4.2.3 index is a full rebuild, so the incremental case
//! first rebuilds the index at t=1 in this build's format.
//!
//! Own test binary: asserts which build path ran through span capture, which
//! is thread-local and process-shared (see `it_upsert_skip_fires`).

#![cfg(feature = "native")]

mod support;

use crate::support::{span_capture, start_background_indexer_local};
use fluree_db_api::{Fluree, FlureeBuilder, IndexOutcome, LedgerId};
use fluree_db_indexer::IndexerConfig;

const LEDGER: &str = "ex:main";
const INCREMENTAL: &str = "attempting incremental index";
const REBUILD: &str = "starting full rebuild path";

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// The index the fixture's build to t=2 starts from.
#[derive(Clone, Copy)]
enum Base {
    /// The v4.2.3 index at t=1. A build over an earlier format is a full
    /// rebuild.
    AsWritten,
    /// That index rebuilt at t=1 in this build's format, so the build to t=2
    /// can go incremental.
    Current,
}

/// Replace the fixture's index at t=1 with one rebuilt in this build's format.
async fn rebuild_the_base(fluree: &Fluree) {
    let record = fluree
        .nameservice()
        .lookup(LEDGER)
        .await
        .unwrap()
        .expect("ledger record");
    let store = fluree.content_store(&fluree.storage_namespace(LEDGER).await.unwrap());
    let head =
        fluree_db_core::load_commit_by_id(store.as_ref(), record.commit_head_id.as_ref().unwrap())
            .await
            .expect("head commit");
    let mut at_1 = record.clone();
    at_1.commit_head_id = head.parent_ids().next().cloned();
    at_1.commit_t = 1;
    let built =
        fluree_db_indexer::rebuild_index_from_commits(store, LEDGER, &at_1, IndexerConfig::small())
            .await
            .expect("rebuild at t=1");
    fluree
        .nameservice_mode()
        .publisher()
        .expect("read-write nameservice")
        .publish_index_allow_equal_fenced(LEDGER, record.fence, 1, &built.root_id)
        .await
        .expect("publish the rebuilt index");
}

/// Index the fixture from `base` with the background indexer under `config`,
/// then write with no room left in novelty. Returns the build's outcome, the
/// ledger's index status after it, and the write's refusal.
async fn index_the_fixture(
    base: Base,
    config: IndexerConfig,
) -> (
    IndexOutcome,
    fluree_db_api::IndexStatusResult,
    fluree_db_api::ApiError,
) {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/vector-lengths-4.2.3/storage");
    let tmp = tempfile::TempDir::new().expect("tempdir");
    copy_dir(&fixture, tmp.path());
    // No default indexer: the worker below is the only one, and it starts
    // after the base is rebuilt (its startup sweep builds a ledger behind its
    // commits).
    let mut fluree: Fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build");
    if let Base::Current = base {
        rebuild_the_base(&fluree).await;
    }
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        config,
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    local
        .run_until(async move {
            let ledger = fluree.ledger(LEDGER).await.expect("load the fixture");
            assert_eq!((ledger.index_t(), ledger.t()), (1, 2));
            let completion = handle.trigger(&LedgerId::parse(LEDGER).unwrap(), 2).await;
            // A build retried as if the failure were transient never reports.
            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(60), completion.wait())
                    .await
                    .expect("the build reports an outcome instead of retrying");
            let status = fluree.index_status(LEDGER).await.expect("status");
            let full = fluree_db_api::IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1,
            };
            let data =
                serde_json::json!({"@id": "http://example.org/c", "http://example.org/name": "c"});
            let ledger = fluree.ledger_cached(LEDGER).await.expect("handle");
            let refused = fluree
                .stage(&ledger)
                .insert(&data)
                .index_config(full)
                .execute()
                .await
                .map(|_| ())
                .expect_err("novelty is past its maximum");
            (outcome, status, refused)
        })
        .await
}

fn assert_halted(outcome: &IndexOutcome, status: &fluree_db_api::IndexStatusResult) {
    match outcome {
        IndexOutcome::Failed(e) => assert!(e.contains("vector dims mismatch"), "{e}"),
        other => panic!("expected the build to fail, got {other:?}"),
    }
    assert!(status.halted, "{status:?}");
    assert!(
        status
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("vector dims mismatch")),
        "{status:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_incremental_build_halts_without_falling_back() {
    let (spans, _guard) = span_capture::init_test_tracing();
    let (outcome, status, refused) = index_the_fixture(Base::Current, IndexerConfig::small()).await;
    assert_halted(&outcome, &status);
    assert!(spans.has_event(INCREMENTAL), "the incremental path ran");
    // A write refused for want of room in novelty is logged with the halt.
    assert!(
        matches!(
            refused,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::NoveltyAtMax)
        ),
        "{refused:?}"
    );
    assert!(
        spans.has_event(
            "novelty at max: writes are refused because indexing of this ledger is halted on \
             committed data it cannot index"
        ),
        "the refusal names the halt"
    );
    assert!(
        !spans.has_event(REBUILD),
        "a build that fails on the data does not fall back to a full rebuild"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_rebuild_halts() {
    let (spans, _guard) = span_capture::init_test_tracing();
    let config = IndexerConfig {
        incremental_enabled: false,
        ..IndexerConfig::small()
    };
    let (outcome, status, _) = index_the_fixture(Base::Current, config).await;
    assert_halted(&outcome, &status);
    assert!(spans.has_event(REBUILD), "the full rebuild path ran");
    assert!(!spans.has_event(INCREMENTAL));
}

/// The first build over the v4.2.3 index is a full rebuild, which halts the
/// same way.
#[tokio::test(flavor = "current_thread")]
async fn a_build_over_an_earlier_format_rebuilds_and_halts() {
    let (spans, _guard) = span_capture::init_test_tracing();
    let (outcome, status, _) = index_the_fixture(Base::AsWritten, IndexerConfig::small()).await;
    assert_halted(&outcome, &status);
    assert!(spans.has_event(REBUILD), "the full rebuild path ran");
    assert!(!spans.has_event(INCREMENTAL));
}
