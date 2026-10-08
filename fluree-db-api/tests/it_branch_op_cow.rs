//! Branch-op commits (merge, revert) must uniquely own the ledger
//! dictionaries when they extend them.
//!
//! Both flows stage against a `clone_state()` of the locked cache and then run
//! the same `finalize_state_with_base` dictionary `make_mut`s as a transact
//! commit — so without `apply_staged_detached`'s cache detach, the cache's
//! co-held `Arc`s turn every merge/revert commit into a deep clone of the
//! dictionaries, O(entries accumulated since the last index).
//!
//! The commit path publishes its ownership count on the `fluree::cow_probe`
//! target, and this test asserts on it, mirroring `it_cached_handle_cow`.
//!
//! Own test binary: it installs a process-global tracing subscriber and reads
//! every `fluree::cow_probe` event the process emits, so it must not share a
//! process with other tests that commit.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_branch_op_cow

use std::sync::Mutex;

use fluree_db_api::{CommitRef, ConflictStrategy, FlureeBuilder};
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
}

static OBSERVED: Mutex<Vec<Ownership>> = Mutex::new(Vec::new());

#[derive(Default)]
struct ProbeVisitor {
    dict: Option<u64>,
    runtime_small_dicts: Option<u64>,
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

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Only the shared commit path is under test; the cache catch-up path
        // emits the same target from `apply_single_commit`.
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

fn person(id: &str, name: &str) -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@id": format!("ex:{id}"),
        "ex:name": name,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_and_revert_commits_uniquely_own_the_dictionaries() {
    let _ = tracing_subscriber::registry().with(ProbeLayer).try_init();

    let fluree = FlureeBuilder::memory().build_memory();
    // Vacuity guard: without a ledger manager both flows take their
    // no-guard fallback and never exercise the detach at all.
    assert!(
        fluree.ledger_manager().is_some(),
        "this test requires the cached-handle (ledger manager) path"
    );

    let ledger = fluree.create_ledger("branch-cow").await.expect("create");
    fluree
        .insert(ledger, &person("alice", "Alice"))
        .await
        .expect("seed main"); // main t=1

    fluree
        .create_branch("branch-cow", "dev", None, None)
        .await
        .expect("create branch");

    let dev = fluree.ledger("branch-cow:dev").await.expect("load dev");
    fluree
        .insert(dev, &person("bob", "Bob"))
        .await
        .expect("commit on dev"); // dev t=2

    // Diverge main so the merge below is a general merge — the shape that
    // builds a merge commit and runs the apply under the write guard. A
    // fast-forward publishes a ref and commits nothing.
    let main = fluree.ledger("branch-cow:main").await.expect("load main");
    let carol_cid = fluree
        .insert(main, &person("carol", "Carol"))
        .await
        .expect("diverge main") // main t=2
        .receipt
        .commit_id;

    // --- Merge: dev → main ------------------------------------------------
    drain_observations();
    let report = fluree
        .merge_branch("branch-cow", "dev", None, ConflictStrategy::default())
        .await
        .expect("merge dev into main");
    assert!(
        !report.fast_forward,
        "the merge must build a commit (general merge) to exercise the apply path"
    );
    assert_eq!(report.new_head_t, 3, "merge commit lands at t=3");
    let merge_probes = drain_observations();
    assert_eq!(
        merge_probes.len(),
        1,
        "expected exactly one commit probe from the merge apply"
    );
    assert_uniquely_owned(&merge_probes, "merge commit");

    // --- Revert: undo Carol's commit on main ------------------------------
    drain_observations();
    let report = fluree
        .revert_commit(
            "branch-cow",
            "main",
            CommitRef::Exact(carol_cid),
            ConflictStrategy::Abort,
        )
        .await
        .expect("revert the divergence commit");
    assert_eq!(report.new_head_t, 4, "revert commit lands at t=4");
    let revert_probes = drain_observations();
    assert_eq!(
        revert_probes.len(),
        1,
        "expected exactly one commit probe from the revert apply"
    );
    assert_uniquely_owned(&revert_probes, "revert commit");

    // The detach must also have been repaired: the cached handle serves the
    // post-revert head, not the placeholder a failed refill would leave.
    let handle = fluree
        .ledger_cached("branch-cow:main")
        .await
        .expect("cached handle after branch ops");
    assert_eq!(
        handle.t().await,
        4,
        "cache refilled with the committed head"
    );
}
