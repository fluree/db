//! A cached-handle commit must uniquely own the ledger dictionaries when it
//! extends them.
//!
//! Every commit calls `Arc::make_mut` on `dict_novelty` and
//! `runtime_small_dicts`. That mutates in place only while the commit is their
//! sole owner; any other live holder turns each call into a deep clone costing
//! O(dictionary entries accumulated since the last index) — a per-commit tax
//! that compounds quadratically across a window where indexing lags.
//!
//! The commit path publishes its ownership count on the `fluree::cow_probe`
//! target, and this test asserts on it. Two holders used to be structural on
//! this path: the ledger cache kept a full `Arc` clone of the state the commit
//! staged against, and once an index was installed the `BinaryRangeProvider`
//! attached to the cached snapshot held a second pair of dictionary clones.
//!
//! Own test binary: it installs a process-global tracing subscriber and reads
//! every `fluree::cow_probe` event the process emits, so it must not share a
//! process with other tests that commit.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_cached_handle_cow --features native

#![cfg(feature = "native")]

use std::sync::Mutex;

use fluree_db_api::{Fluree, FlureeBuilder, LedgerHandle, RefreshOpts, ReindexOptions};
use serde_json::json;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// One `fluree::cow_probe` observation from the commit path.
#[derive(Debug, Clone, Copy)]
struct Ownership {
    dict: u64,
    runtime_small_dicts: u64,
    /// Whether a `BinaryRangeProvider` was attached to the snapshot this
    /// commit started from — i.e. whether the post-index regime was in play.
    provider_attached: bool,
}

static OBSERVED: Mutex<Vec<Ownership>> = Mutex::new(Vec::new());

#[derive(Default)]
struct ProbeVisitor {
    dict: Option<u64>,
    runtime_small_dicts: Option<u64>,
    provider_attached: bool,
    from_commit_path: bool,
}

impl Visit for ProbeVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "dict_novelty_strong" => self.dict = Some(value),
            "runtime_small_dicts_strong" => self.runtime_small_dicts = Some(value),
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_u64(field, value as u64);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        // `had_binary_provider` on the commit path, `provider_attached` on the
        // cache catch-up path.
        if matches!(field.name(), "had_binary_provider" | "provider_attached") {
            self.provider_attached = value;
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Only the transact commit path is under test here; the cache
        // catch-up path emits the same target from `apply_single_commit`.
        if field.name() == "message" {
            self.from_commit_path = format!("{value:?}").contains("commit_txn");
        }
    }
}

struct ProbeLayer;

impl<S: tracing::Subscriber> Layer<S> for ProbeLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "fluree::cow_probe" {
            return;
        }
        let mut visitor = ProbeVisitor::default();
        event.record(&mut visitor);
        if !visitor.from_commit_path {
            return;
        }
        if let (Some(dict), Some(runtime_small_dicts)) = (visitor.dict, visitor.runtime_small_dicts)
        {
            OBSERVED
                .lock()
                .expect("probe capture lock never poisoned")
                .push(Ownership {
                    dict,
                    runtime_small_dicts,
                    provider_attached: visitor.provider_attached,
                });
        }
    }
}

fn drain_observations() -> Vec<Ownership> {
    std::mem::take(&mut *OBSERVED.lock().expect("probe capture lock never poisoned"))
}

/// Every commit in `observed` must have owned the dictionaries outright.
fn assert_uniquely_owned(observed: &[Ownership], phase: &str) {
    for (i, o) in observed.iter().enumerate() {
        assert_eq!(
            o.dict,
            1,
            "{phase}: commit {i} shared dict_novelty with {} other holder(s), so Arc::make_mut \
             deep-cloned the dictionary instead of extending it in place",
            o.dict - 1
        );
        assert_eq!(
            o.runtime_small_dicts,
            1,
            "{phase}: commit {i} shared runtime_small_dicts with {} other holder(s)",
            o.runtime_small_dicts - 1
        );
    }
}

async fn commit_one(fluree: &Fluree, handle: &LedgerHandle, n: usize) {
    let data = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": format!("ex:person-{n}"),
        "ex:name": format!("Person {n}"),
        "ex:city": format!("City {}", n % 7),
    });
    fluree
        .stage(handle)
        .insert(&data)
        .execute()
        .await
        .expect("commit through the cached handle");
}

#[tokio::test(flavor = "multi_thread")]
async fn cached_handle_commits_uniquely_own_the_dictionaries() {
    let _ = tracing_subscriber::registry().with(ProbeLayer).try_init();

    let tmp = tempfile::TempDir::new().expect("tempdir");
    // Indexing is driven explicitly below so the index install lands at a
    // known point rather than whenever a background worker gets there.
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build");

    let ledger_id = "it/cached-cow:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache the ledger");

    // --- Phase 1: commits before any index exists -------------------------
    for n in 0..5 {
        commit_one(&fluree, &handle, n).await;
    }
    assert!(
        handle.snapshot().await.snapshot.range_provider.is_none(),
        "no index has been published yet, so no range provider should be attached"
    );
    let pre_index = drain_observations();
    assert_eq!(pre_index.len(), 5, "expected one probe per commit");
    assert_uniquely_owned(&pre_index, "before any index");

    // --- Phase 2: publish an index and install it on the cached handle ----
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    fluree
        .refresh(ledger_id, RefreshOpts::default())
        .await
        .expect("refresh the cached handle onto the new index");
    // Hard assert, not a skip: if the index never reached the cached handle,
    // the phases below would silently re-test the pre-index case.
    assert!(
        handle.snapshot().await.snapshot.range_provider.is_some(),
        "the published index must be installed on the cached handle"
    );
    drain_observations();

    // --- Phase 3: the first commit after an index install ------------------
    commit_one(&fluree, &handle, 100).await;
    let after_install = drain_observations();
    assert_eq!(after_install.len(), 1, "expected one probe for one commit");
    assert!(
        after_install[0].provider_attached,
        "this commit must have started from a snapshot with a range provider \
         attached, otherwise it does not exercise the post-index regime"
    );
    assert_uniquely_owned(&after_install, "first commit after an index install");

    // --- Phase 4: steady state with an index attached ----------------------
    for n in 101..106 {
        commit_one(&fluree, &handle, n).await;
    }
    let steady = drain_observations();
    assert_eq!(steady.len(), 5, "expected one probe per commit");
    assert!(
        steady.iter().all(|o| o.provider_attached),
        "every steady-state commit should still have the index attached"
    );
    assert_uniquely_owned(&steady, "post-index steady state");

    // --- Phase 5: the lock-held path -------------------------------------
    // Everything above goes through the optimistic path, which stages against
    // a snapshot taken before the lock. A SPARQL UPDATE instead holds the
    // write lock across stage and commit and clones the state through
    // `clone_state()`; it shares the commit tail, so it must own its
    // dictionaries too.
    fluree
        .stage(&handle)
        .sparql_update(
            "PREFIX ex: <http://example.org/> \
             INSERT DATA { ex:sparql-subject ex:name \"via SPARQL\" }",
        )
        .execute()
        .await
        .expect("SPARQL UPDATE through the cached handle");
    let lock_held = drain_observations();
    assert_eq!(lock_held.len(), 1, "expected one probe for one commit");
    assert_uniquely_owned(&lock_held, "lock-held (SPARQL UPDATE) path");
}
