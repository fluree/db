use fluree_db_core::{ContentKind, ContentStore as _};
use serde_json::json;

use super::{Blob, GarbageReason};
use crate::{Fluree, FlureeBuilder};

async fn file_fluree(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build_async()
        .await
        .unwrap()
}

/// A reconciliation checks one slice of the digest space per step, keeping
/// the time it started, until every file is checked once; one that must start
/// later starts over.
#[tokio::test]
async fn reconciliation_resumes_where_its_last_step_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("steps").await.unwrap();
    for name in ["alice", "bob", "carol"] {
        let ledger = fluree.ledger("steps:main").await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@id": format!("http://example.org/{name}"), "http://example.org/name": name}),
            )
            .await
            .unwrap();
    }
    let namespace = fluree.storage_namespace("steps:main").await.unwrap();
    for n in 0..3 {
        fluree
            .content_store(&namespace)
            .put(ContentKind::Txn, format!("{{\"stray\":{n}}}").as_bytes())
            .await
            .unwrap();
    }

    let mut steps = Vec::new();
    for _ in 0..10 {
        let step = fluree
            .reconcile_step("steps", 2, None, &mut None)
            .await
            .unwrap()
            .unwrap();
        steps.push(step.clone());
        if step.complete {
            break;
        }
    }
    assert!(steps.last().is_some_and(|s| s.complete), "{steps:?}");
    // Six files in slices of about two: four slices.
    assert_eq!(steps.len(), 4, "{steps:?}");
    assert!(steps.iter().all(|s| s.listed == 6), "{steps:?}");
    assert_eq!(
        steps.iter().map(|s| s.checked).sum::<usize>(),
        6,
        "{steps:?}"
    );
    assert_eq!(
        steps.iter().map(|s| s.recorded).sum::<usize>(),
        3,
        "{steps:?}"
    );

    // A step that may not resume progress from before `not_before` checks
    // everything again.
    let first = fluree
        .reconcile_step("steps", 2, None, &mut None)
        .await
        .unwrap()
        .unwrap();
    assert!(!first.complete);
    let restarted = fluree
        .reconcile_step("steps", 100, Some(i64::MAX), &mut None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restarted.checked, 6, "{restarted:?}");
    assert!(restarted.complete);

    // Steps sharing a listing list once, and again once the branches change.
    let mut listing = None;
    let first = fluree
        .reconcile_step("steps", 2, None, &mut listing)
        .await
        .unwrap()
        .unwrap();
    let second = fluree
        .reconcile_step("steps", 2, None, &mut listing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (first.listed, second.listed),
        (6, 0),
        "{first:?} {second:?}"
    );
    fluree
        .create_branch("steps", "dev", None, None)
        .await
        .unwrap();
    let third = fluree
        .reconcile_step("steps", 2, None, &mut listing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.listed, 6, "{third:?}");
}

/// A blob a reconciliation recorded stays unresolved while a writer pins
/// it, even once a failed attempt records it again.
#[tokio::test]
async fn recording_a_blob_again_keeps_it_unresolved() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("again").await.unwrap();
    let ledger = fluree.ledger("again:main").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@id": "http://example.org/alice", "http://example.org/name": "alice"}),
        )
        .await
        .unwrap();
    let namespace = fluree.storage_namespace("again:main").await.unwrap();
    let stray = fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, b"{\"stray\":true}")
        .await
        .unwrap();
    let report = fluree.reconcile_garbage("again").await.unwrap().unwrap();
    assert_eq!(report.recorded, 1);

    // A retry stores the same body, holds it, and fails again later.
    let pin = fluree.blob_pinner(&namespace).unwrap().pin(&stray).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    fluree
        .record_garbage(
            &[Blob::new(namespace.clone(), stray.clone())],
            GarbageReason::Abandoned,
        )
        .await
        .unwrap();
    assert!(!fluree.resolve_garbage("again", 0).await.unwrap());
    assert!(fluree.content_store(&namespace).has(&stray).await.unwrap());

    drop(pin);
    assert!(fluree.resolve_garbage("again", 0).await.unwrap());
    assert!(!fluree.content_store(&namespace).has(&stray).await.unwrap());
}

/// A ledger that cannot be reconciled yet, such as one with no commit, does
/// not keep the next ledger from its turn.
#[tokio::test]
async fn an_unsettled_ledger_does_not_hold_up_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("a-empty").await.unwrap();
    fluree.create_ledger("b-full").await.unwrap();
    let ledger = fluree.ledger("b-full:main").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@id": "http://example.org/alice", "http://example.org/name": "alice"}),
        )
        .await
        .unwrap();

    let pass = fluree
        .reconcile_due(
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert!(
        pass.ledgers
            .iter()
            .any(|(name, report)| name == "b-full" && report.complete),
        "{pass:?}"
    );
}

/// A pass out of time leaves the ledgers due, and says how long the
/// longest-waiting has gone unreconciled.
#[tokio::test]
async fn a_pass_out_of_time_leaves_its_ledgers_due() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    for name in ["one", "two"] {
        fluree.create_ledger(name).await.unwrap();
        commit_raw(&fluree, &format!("{name}:main"), name).await;
    }
    let none = std::time::Duration::ZERO;
    let pass = fluree.reconcile_due(none, none).await.unwrap();
    assert!(pass.ledgers.is_empty(), "{pass:?}");
    assert_eq!((pass.due, pass.oldest_ms), (2, None), "{pass:?}");

    let before = super::now_ms();
    fluree.reconcile_garbage("one").await.unwrap().unwrap();
    let pass = fluree.reconcile_due(none, none).await.unwrap();
    assert_eq!(pass.due, 2, "{pass:?}");
    assert!(pass.oldest_ms.is_some_and(|ms| ms >= before), "{pass:?}");

    let pass = fluree
        .reconcile_due(
            std::time::Duration::from_secs(3600),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(pass.ledgers.len(), 1, "{pass:?}");
    assert_eq!(pass.due, 0, "{pass:?}");
}

async fn commit_raw(fluree: &Fluree, ledger_id: &str, name: &str) {
    fluree
        .graph(ledger_id)
        .transact()
        .txn_opts(fluree_db_transact::TxnOpts::default().store_raw_txn(true))
        .update(&json!({
            "@context": {"ex": "http://example.org/ns/"},
            "insert": {"@id": format!("ex:{name}"), "ex:name": name}
        }))
        .commit()
        .await
        .unwrap();
}

/// Over every file listed, the slice-at-a-time check a reconciliation makes
/// agrees with the one-blob check collection makes: indexed history and
/// novelty, a branch reading its parent's folder, the same body stored in
/// two folders, commits a merge copied in, and strays.
#[tokio::test]
async fn reconciliation_and_collection_agree_on_every_file() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("agree").await.unwrap();
    for name in ["a", "b", "c"] {
        commit_raw(&fluree, "agree:main", name).await;
    }
    fluree
        .reindex("agree:main", crate::ReindexOptions::default())
        .await
        .unwrap();
    commit_raw(&fluree, "agree:main", "d").await;
    fluree
        .create_branch("agree", "dev", None, None)
        .await
        .unwrap();
    for name in ["e", "f"] {
        commit_raw(&fluree, "agree:dev", name).await;
    }
    commit_raw(&fluree, "agree:main", "g").await;
    // Each branch reads its own copy of the same body.
    commit_raw(&fluree, "agree:dev", "same").await;
    commit_raw(&fluree, "agree:main", "same").await;
    fluree
        .merge_branch("agree", "dev", None, crate::ConflictStrategy::default())
        .await
        .unwrap();
    commit_raw(&fluree, "agree:dev", "h").await;

    let main = fluree.storage_namespace("agree:main").await.unwrap();
    let dev = fluree.storage_namespace("agree:dev").await.unwrap();
    // The body of `a`, which dev inherited, stored again in dev's folder:
    // dev reads that copy, main its own.
    let a_body = serde_json::to_vec(&json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:a", "ex:name": "a"}
    }))
    .unwrap();
    fluree
        .content_store(&dev)
        .put(ContentKind::Txn, &a_body)
        .await
        .unwrap();
    // The body of `h`, only dev's, stored again in main's folder: dev reads
    // its own copy, so nothing reads that one.
    let h_body = serde_json::to_vec(&json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": "ex:h", "ex:name": "h"}
    }))
    .unwrap();
    fluree
        .content_store(&main)
        .put(ContentKind::Txn, &h_body)
        .await
        .unwrap();
    for (namespace, stray) in [(&main, "main"), (&dev, "dev")] {
        fluree
            .content_store(namespace)
            .put(
                ContentKind::Txn,
                format!("{{\"stray\":\"{stray}\"}}").as_bytes(),
            )
            .await
            .unwrap();
    }

    let binding = fluree
        .publisher()
        .unwrap()
        .get_binding("agree")
        .await
        .unwrap()
        .unwrap()
        .value;
    let listing = fluree.list_ledger(&binding).await.unwrap();
    let holders = fluree
        .load_holders("agree", &binding)
        .await
        .unwrap()
        .expect("settled");
    let listed = super::check::Listed::new(listing.slice(1, 0));
    let read = holders.read(&listed).await.unwrap();
    let mut unread = Vec::new();
    for (blob, read) in listed.blobs.iter().zip(read) {
        let referenced = holders.references(&fluree, blob).await.unwrap();
        assert_eq!(read, referenced, "{blob:?}");
        if !read {
            unread.push(blob.clone());
        }
    }
    assert_eq!(unread.len(), 3, "{unread:?}");
    assert!(listed.blobs.len() > 20, "{}", listed.blobs.len());
}

/// A run another restarted lists again before going on: an orphan that
/// appeared after the first run's listing is recorded, and collected, before
/// the newer run counts as complete.
#[tokio::test]
async fn a_run_restarted_by_another_lists_again() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("race").await.unwrap();
    for name in ["a", "b", "c"] {
        commit_raw(&fluree, "race:main", name).await;
    }
    // Housekeeping lists, and checks the first of four slices.
    let mut housekeeping = None;
    let first = fluree
        .reconcile_step("race", 2, None, &mut housekeeping)
        .await
        .unwrap()
        .unwrap();
    assert!(!first.complete);
    // An orphan lands in a slice past the first.
    let namespace = fluree.storage_namespace("race:main").await.unwrap();
    let orphan = (0..)
        .map(|n| format!("{{\"orphan\":{n}}}").into_bytes())
        .find(|bytes| {
            let id = fluree_db_core::ContentId::new(ContentKind::Txn, bytes);
            id.as_cid().hash().digest()[0] >= 64
        })
        .unwrap();
    let orphan = fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, &orphan)
        .await
        .unwrap();
    // A purge restarts the reconciliation and checks its first slice.
    let since = super::now_ms();
    let purge = fluree
        .reconcile_step("race", 2, Some(since), &mut None)
        .await
        .unwrap()
        .unwrap();
    assert!(!purge.complete);
    // Housekeeping goes on with the listing it holds.
    for _ in 0..10 {
        let step = fluree
            .reconcile_step("race", 2, None, &mut housekeeping)
            .await
            .unwrap()
            .unwrap();
        if step.complete {
            break;
        }
    }

    assert!(fluree.resolve_garbage("race", since).await.unwrap());
    assert!(!fluree.content_store(&namespace).has(&orphan).await.unwrap());
}

/// A collection with no time left loads nothing, leaves every ledger, and
/// the next starts with the first it did not reach.
#[tokio::test]
async fn a_collection_out_of_time_loads_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    for name in ["a", "b", "c"] {
        fluree.create_ledger(name).await.unwrap();
        commit_raw(&fluree, &format!("{name}:main"), name).await;
        let namespace = fluree
            .storage_namespace(&format!("{name}:main"))
            .await
            .unwrap();
        let stray = fluree
            .content_store(&namespace)
            .put(
                ContentKind::Txn,
                format!("{{\"stray\":\"{name}\"}}").as_bytes(),
            )
            .await
            .unwrap();
        fluree
            .record_garbage(&[Blob::new(namespace, stray)], GarbageReason::Abandoned)
            .await
            .unwrap();
    }
    let none = std::time::Duration::ZERO;

    let report = fluree
        .collect_garbage_within(none, Some(none))
        .await
        .unwrap();
    assert_eq!(report.deleted, 0, "{report:?}");
    assert_eq!(report.ledgers_left, 3, "{report:?}");
    assert_eq!(
        report.timings,
        super::CollectTimings::default(),
        "{report:?}"
    );
    assert_eq!(fluree.root.collect_from.lock().as_deref(), Some("a"));

    let report = fluree.collect_garbage_within(none, None).await.unwrap();
    assert_eq!(report.deleted, 3, "{report:?}");
    assert_eq!(*fluree.root.collect_from.lock(), None);
}

pub(crate) mod cluster {
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use fluree_db_core::{content_address, ContentId, ContentKind, ContentStore as _};
    use parking_lot::Mutex;
    use serde_json::json;
    use tokio::sync::Notify;

    use super::file_fluree;
    use crate::collect::Collector;
    use crate::{Fluree, Result};

    /// An open intent: its object's hash, whether deleting, what is spared.
    type Open = (String, bool, Vec<String>);

    /// The replicated collection state, kept here.
    #[derive(Debug, Default)]
    pub(crate) struct Cluster {
        intents: Mutex<BTreeMap<u64, Open>>,
        next: Mutex<u64>,
        queued: Mutex<HashSet<ContentId>>,
        /// How many more leases this member is granted; unlimited if `None`.
        leases: Mutex<Option<usize>>,
        /// The next `delete` answers once this is notified.
        hold_delete: Mutex<Option<Arc<Notify>>>,
        /// The term its leases are granted in.
        pub(crate) term: std::sync::atomic::AtomicU64,
        /// How long a lease lasts: a second if unset.
        pub(crate) lease_ms: Mutex<Option<u64>>,
    }

    impl Cluster {
        /// One that grants `leases` leases, then no more: deposed.
        pub(crate) fn with_leases(leases: usize) -> Self {
            Self {
                leases: Mutex::new(Some(leases)),
                ..Default::default()
            }
        }

        fn spare(&self, number: u64, address: &str) {
            let mut intents = self.intents.lock();
            let (_, deleting, spared) = intents.get_mut(&number).expect("open");
            assert!(!*deleting);
            spared.push(address.to_string());
        }
    }

    #[async_trait]
    impl Collector for Cluster {
        async fn condemn(&self, intent: &str) -> Result<u64> {
            let mut next = self.next.lock();
            self.intents
                .lock()
                .insert(*next, (intent.to_string(), false, Vec::new()));
            *next += 1;
            Ok(*next - 1)
        }

        async fn delete(&self, number: u64) -> Result<Option<Vec<String>>> {
            let deleting = self.intents.lock().get_mut(&number).map(|intent| {
                intent.1 = true;
                intent.2.clone()
            });
            let hold = self.hold_delete.lock().take();
            if let Some(hold) = hold {
                hold.notified().await;
            }
            Ok(deleting)
        }

        async fn complete(&self, number: u64) -> Result<()> {
            self.intents.lock().remove(&number);
            Ok(())
        }

        async fn open(&self) -> Vec<(u64, String, bool)> {
            self.intents
                .lock()
                .iter()
                .map(|(n, (hash, deleting, _))| (*n, hash.clone(), *deleting))
                .collect()
        }

        async fn queued(&self, _fluree: &Fluree, _ledger: &str) -> Result<HashSet<ContentId>> {
            Ok(self.queued.lock().clone())
        }

        async fn enable(&self) -> Result<bool> {
            Ok(true)
        }

        async fn requests_recorded_by(&self, _ledger: &str, _ms: i64) -> bool {
            false
        }

        async fn lease(&self) -> Option<crate::collect::Lease> {
            let mut leases = self.leases.lock();
            match leases.as_mut() {
                Some(0) => return None,
                Some(left) => *left -= 1,
                None => {}
            }
            Some(crate::collect::Lease {
                until: Instant::now() + Duration::from_millis(self.lease_ms.lock().unwrap_or(1000)),
                term: self.term.load(std::sync::atomic::Ordering::SeqCst),
            })
        }
    }

    /// A cluster member leaves a blob nothing names to the collection, as a
    /// record, instead of deleting it; elsewhere it is the caller's.
    #[tokio::test]
    async fn a_member_leaves_what_nothing_names_to_the_collection() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        fluree.create_ledger("left").await.unwrap();
        let namespace = fluree.storage_namespace("left:main").await.unwrap();
        let id = fluree
            .content_store(&namespace)
            .put(ContentKind::Txn, b"{\"orphan\":true}")
            .await
            .unwrap();
        assert!(!fluree
            .leave_for_collection(&namespace, std::slice::from_ref(&id))
            .await
            .unwrap());
        fluree.set_collector(Arc::new(Cluster::default()));
        assert!(fluree
            .leave_for_collection(&namespace, std::slice::from_ref(&id))
            .await
            .unwrap());
        let binding = fluree
            .publisher()
            .unwrap()
            .get_binding("left")
            .await
            .unwrap()
            .unwrap();
        let recorded: Vec<_> = fluree
            .garbage_records(&binding.value.root)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.blob.id)
            .collect();
        assert_eq!(recorded, [id]);
    }

    pub(crate) async fn pass(
        fluree: &Fluree,
        cluster: &Arc<Cluster>,
    ) -> crate::garbage::GarbageReport {
        let collector: Arc<dyn Collector> = cluster.clone();
        fluree
            .collect_in_cluster(&collector, Duration::ZERO, Duration::ZERO, None)
            .await
            .unwrap()
    }

    /// A pass condemns the recorded blobs nothing references, and the next
    /// deletes those still unreferenced and not spared, then closes the
    /// intent. A member deposed mid-deletion stops; the intent stays open
    /// and the next leader carries it on.
    #[tokio::test]
    async fn a_cluster_condemns_what_is_unreferenced_and_deletes_it_on_the_next_pass() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        fluree.create_ledger("swept").await.unwrap();
        let ledger = fluree.ledger("swept:main").await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@id": "http://example.org/a", "http://example.org/name": "a"}),
            )
            .await
            .unwrap();
        let namespace = fluree.storage_namespace("swept:main").await.unwrap();
        let mut strays = Vec::new();
        for n in 0..4 {
            let id = fluree
                .content_store(&namespace)
                .put(ContentKind::Txn, format!("{{\"stray\":{n}}}").as_bytes())
                .await
                .unwrap();
            let address =
                content_address("file", ContentKind::Txn, &namespace, &id.address_digest());
            strays.push((id, address));
        }
        let exists = |address: &str| {
            let storage = fluree.admin_storage().unwrap();
            let address = address.to_string();
            async move { storage.exists(&address).await.unwrap() }
        };
        fluree.reconcile_garbage("swept").await.unwrap().unwrap();
        let cluster = Arc::new(Cluster::default());
        // A queued request names the last.
        cluster.queued.lock().insert(strays[3].0.clone());

        let first = pass(&fluree, &cluster).await;
        assert_eq!(first.condemned, 3, "{first:?}");
        assert_eq!(first.resolved, 1, "{first:?}");
        for (_, address) in &strays {
            assert!(exists(address).await, "condemned, not deleted");
        }

        // A writer stores the second again; a request names the third.
        cluster.spare(0, &strays[1].1);
        cluster.queued.lock().insert(strays[2].0.clone());
        let second = pass(&fluree, &cluster).await;
        assert_eq!(
            (second.deleted, second.spared, second.resolved),
            (1, 1, 1),
            "{second:?}"
        );
        assert!(cluster.intents.lock().is_empty(), "closed");
        assert!(!exists(&strays[0].1).await);
        for (_, address) in &strays[1..] {
            assert!(exists(address).await);
        }

        // The spared one's writer abandoned it: reconciliation records it
        // again and it is condemned, but the member is deposed once its
        // deletion has begun, before it deletes anything; the next pass
        // carries the intent on.
        fluree.reconcile_garbage("swept").await.unwrap().unwrap();
        let third = pass(&fluree, &cluster).await;
        assert_eq!((third.condemned, third.resolved), (1, 2), "{third:?}");
        *cluster.leases.lock() = Some(1);
        let deposed = pass(&fluree, &cluster).await;
        assert_eq!(deposed.deleted, 0, "{deposed:?}");
        assert!(exists(&strays[1].1).await);
        assert!(cluster.intents.lock()[&1].1, "left deleting");
        *cluster.leases.lock() = None;
        let resumed = pass(&fluree, &cluster).await;
        assert_eq!(resumed.deleted, 1, "{resumed:?}");
        assert!(!exists(&strays[1].1).await);
        assert!(cluster.intents.lock().is_empty());
    }

    /// A ledger with one recorded stray nothing references, and its
    /// address.
    async fn one_stray(fluree: &Fluree) -> String {
        fluree.create_ledger("stray").await.unwrap();
        let ledger = fluree.ledger("stray:main").await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@id": "http://example.org/a", "http://example.org/name": "a"}),
            )
            .await
            .unwrap();
        let namespace = fluree.storage_namespace("stray:main").await.unwrap();
        let id = fluree
            .content_store(&namespace)
            .put(ContentKind::Txn, b"{\"stray\":true}")
            .await
            .unwrap();
        fluree.reconcile_garbage("stray").await.unwrap().unwrap();
        content_address("file", ContentKind::Txn, &namespace, &id.address_digest())
    }

    /// A pass carrying on an intent another stopped short of finishing finds
    /// commits it deleted already: nothing is left of those to check, and it
    /// deletes the rest and closes the intent.
    #[tokio::test]
    async fn a_pass_carries_on_past_commits_an_earlier_one_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        fluree.create_ledger("donor").await.unwrap();
        for name in ["a", "b"] {
            let ledger = fluree.ledger("donor:main").await.unwrap();
            fluree
                .insert(
                    ledger,
                    &json!({"@id": format!("http://example.org/{name}"), "http://example.org/name": name}),
                )
                .await
                .unwrap();
        }
        let donor = fluree.storage_namespace("donor:main").await.unwrap();
        let record = fluree
            .nameservice()
            .lookup("donor:main")
            .await
            .unwrap()
            .unwrap();
        let commits = fluree_db_core::collect_first_parent_cids(
            fluree.content_store(&donor).as_ref(),
            record.commit_head_id.as_ref().unwrap(),
            0,
        )
        .await
        .unwrap();
        // The donor's commits copied where nothing references them.
        fluree.create_ledger("stray").await.unwrap();
        let ledger = fluree.ledger("stray:main").await.unwrap();
        fluree
            .insert(
                ledger,
                &json!({"@id": "http://example.org/s", "http://example.org/name": "s"}),
            )
            .await
            .unwrap();
        let namespace = fluree.storage_namespace("stray:main").await.unwrap();
        let mut strays = Vec::new();
        for (_, id) in &commits {
            let bytes = fluree.content_store(&donor).get(id).await.unwrap();
            fluree
                .content_store(&namespace)
                .put(ContentKind::Commit, &bytes)
                .await
                .unwrap();
            strays.push(content_address(
                "file",
                ContentKind::Commit,
                &namespace,
                &id.address_digest(),
            ));
        }
        fluree.reconcile_garbage("stray").await.unwrap().unwrap();
        let cluster = Arc::new(Cluster::default());
        assert_eq!(pass(&fluree, &cluster).await.condemned, strays.len());

        let storage = fluree.admin_storage().unwrap();
        storage.delete(&strays[0]).await.unwrap();
        pass(&fluree, &cluster).await;
        for stray in &strays {
            assert!(!storage.exists(stray).await.unwrap(), "{stray}");
        }
        let after = pass(&fluree, &cluster).await;
        assert_eq!((after.condemned, after.deleted), (0, 0), "{after:?}");
    }

    /// A pass waits for one under way: neither closes an intent the other
    /// still deletes.
    #[tokio::test]
    async fn a_pass_waits_for_the_one_under_way() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        one_stray(&fluree).await;
        let cluster = Arc::new(Cluster::default());
        assert_eq!(pass(&fluree, &cluster).await.condemned, 1);

        let release = Arc::new(Notify::new());
        *cluster.hold_delete.lock() = Some(release.clone());
        let second = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            pass(&fluree, &cluster).await
        };
        let open_meanwhile = async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let open = !cluster.intents.lock().is_empty();
            release.notify_one();
            open
        };
        let (_, _, open_meanwhile) = tokio::join!(pass(&fluree, &cluster), second, open_meanwhile);
        assert!(
            open_meanwhile,
            "closed while the first pass was deleting it"
        );
        assert!(cluster.intents.lock().is_empty());
    }

    /// A member that does not lead writes no intent.
    #[tokio::test]
    async fn a_member_that_does_not_lead_writes_no_intent() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        let stray = one_stray(&fluree).await;
        let cluster = Arc::new(Cluster::default());
        *cluster.leases.lock() = Some(0);
        assert_eq!(pass(&fluree, &cluster).await.condemned, 0);
        let intents = dir.path().join("@retention/collect/intent");
        assert!(
            std::fs::read_dir(&intents).map_or(true, |mut d| d.next().is_none()),
            "an intent object was written"
        );
        assert!(fluree
            .admin_storage()
            .unwrap()
            .exists(&stray)
            .await
            .unwrap());
    }

    /// Intents condemning the same entries are named apart, so a late
    /// delete of one's object never removes the other's.
    #[tokio::test]
    async fn intents_condemning_the_same_entries_are_named_apart() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        let entries = vec![crate::collect::IntentEntry {
            address: "fluree:file://x/main/txn/a.json".into(),
            reason: "test".into(),
            ledger: None,
            record: None,
        }];
        let first = fluree.put_intent(entries.clone()).await.unwrap();
        let second = fluree.put_intent(entries).await.unwrap();
        assert_ne!(first, second);
        assert_eq!(fluree.intent(&first).await.unwrap().entries.len(), 1);
    }

    /// A pass's deletes are the term's it began in: led again in a later
    /// term, it deletes nothing more, and a pass begun in that term carries
    /// the intent on.
    #[tokio::test]
    async fn a_pass_led_again_in_a_later_term_deletes_nothing_more() {
        let dir = tempfile::tempdir().unwrap();
        let fluree = file_fluree(&dir).await;
        let stray = one_stray(&fluree).await;
        let cluster = Arc::new(Cluster::default());
        assert_eq!(pass(&fluree, &cluster).await.condemned, 1);

        let release = Arc::new(Notify::new());
        *cluster.hold_delete.lock() = Some(release.clone());
        let later_term = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cluster
                .term
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            release.notify_one();
        };
        let (stopped, ()) = tokio::join!(pass(&fluree, &cluster), later_term);
        assert_eq!(stopped.deleted, 0, "{stopped:?}");
        assert!(fluree
            .admin_storage()
            .unwrap()
            .exists(&stray)
            .await
            .unwrap());

        assert_eq!(pass(&fluree, &cluster).await.deleted, 1);
        assert!(!fluree
            .admin_storage()
            .unwrap()
            .exists(&stray)
            .await
            .unwrap());
    }

    /// A leader's lease taken again in a later term is not its: lost.
    #[tokio::test]
    async fn a_leader_lease_is_lost_once_its_term_ends() {
        use fluree_db_core::DeleteLease;
        let cluster = Arc::new(Cluster::default());
        let first = cluster.lease().await.unwrap();
        let expired = crate::collect::Lease {
            until: Instant::now(),
            ..first
        };
        let lease = crate::collect::LeaderLease::new(cluster.clone(), expired);
        assert!(lease.until().await.is_some(), "taken again in its term");
        cluster
            .term
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let lease = crate::collect::LeaderLease::new(cluster.clone(), expired);
        assert!(lease.until().await.is_none());
    }
}
