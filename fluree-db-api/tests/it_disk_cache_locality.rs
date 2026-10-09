//! The disk artifact cache serves remote storage only: a copy of a local file,
//! or of bytes already in memory, can never be cheaper than the read it would
//! replace.

#![cfg(feature = "native")]

use crate::support;
use crate::support::hooked_storage::{HookedStorage, StorageHooks};
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{
    BackgroundIndexerWorker, EncryptedStorage, EncryptionKey, Fluree, FlureeBuilder, IndexerConfig,
    LedgerManagerConfig, NameServiceMode, StaticKeyProvider, TriggerIndexOptions, VerifyProblem,
};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_core::{
    BranchedContentStore, ContentId, ContentKind, ContentStore, MemoryStorage, Storage,
    StorageBackend, StorageMethod, StorageRead, StorageWrite,
};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                files_under(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}

fn person(i: usize) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@id": format!("ex:p{i}"),
        "@type": "ex:Person",
        "ex:name": format!("Person {i}")
    })
}

async fn assert_people(fluree: &Fluree, ledger_id: &str, expected: usize) {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let rows = support::query_jsonld_formatted(
        fluree,
        &ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?name",
            "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"}
        }),
    )
    .await
    .expect("query");
    assert_eq!(rows.as_array().map(Vec::len), Some(expected), "{rows}");
}

/// Builds and queries over local file storage read the store's own files, so
/// nothing lands in the disk cache.
#[tokio::test(flavor = "multi_thread")]
async fn local_file_storage_writes_nothing_to_the_disk_cache() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let indexer_config = IndexerConfig::small().with_data_dir(tmp.path().join("data"));
    let cache_dir = tmp.path().join("cache");
    let mut fluree = FlureeBuilder::file(tmp.path().join("storage").to_string_lossy().to_string())
        .with_ledger_cache_config(LedgerManagerConfig {
            cache_dir: cache_dir.clone(),
            ..LedgerManagerConfig::default()
        })
        .build()
        .expect("build");
    let (worker, handle) = BackgroundIndexerWorker::new(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        indexer_config,
    );
    tokio::spawn(worker.run());
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    let ledger_id = "it/disk-cache-local:main";
    fluree.create_ledger(ledger_id).await.expect("create");
    // The first build is a full rebuild; the rest are incremental, which is
    // where the indexer copies what it writes into the cache.
    for round in 0..3 {
        let ledger = fluree.ledger(ledger_id).await.expect("load");
        fluree.insert(ledger, &person(round)).await.expect("insert");
        fluree
            .trigger_index(ledger_id, TriggerIndexOptions::default())
            .await
            .expect("trigger_index");
    }
    assert_people(&fluree, ledger_id, 3).await;

    let cached = files_under(&cache_dir);
    assert!(
        cached.is_empty(),
        "local storage was copied into the disk cache: {cached:?}"
    );
    assert!(
        !cache_dir.exists(),
        "nothing to cache, yet the cache was set up"
    );
}

/// A ledger on `storage` after a full rebuild, an incremental build and a
/// query, with the disk cache of both its indexer and its readers in one
/// private directory.
struct Built {
    fluree: Fluree,
    ledger_id: &'static str,
    root_id: ContentId,
    cache_dir: PathBuf,
    _tmp: tempfile::TempDir,
}

async fn build_twice_and_query<S: Storage + Clone + 'static>(storage: S) -> Built {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let indexer_config = IndexerConfig::small().with_data_dir(tmp.path().join("data"));
    let cache_dir = tmp.path().join("cache");
    let nameservice = MemoryNameService::new();
    let mut fluree: Fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig {
            cache_dir: cache_dir.clone(),
            ..LedgerManagerConfig::default()
        })
        .build_with(
            storage.clone(),
            NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
        );
    let (worker, handle) = BackgroundIndexerWorker::new(
        fluree.backend().clone(),
        Arc::new(nameservice),
        indexer_config,
    );
    tokio::spawn(worker.run());
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    let ledger_id = "it/disk-cache:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let mut root_id = None;
    for round in 0..2 {
        ledger = fluree
            .insert(ledger, &person(round))
            .await
            .expect("insert")
            .ledger;
        root_id = fluree
            .trigger_index(ledger_id, TriggerIndexOptions::default())
            .await
            .expect("trigger_index")
            .root_id;
    }
    assert_people(&fluree, ledger_id, 2).await;
    Built {
        fluree,
        ledger_id,
        root_id: root_id.expect("the build published a root"),
        cache_dir,
        _tmp: tmp,
    }
}

/// Memory storage has no local files to skip, so only its own answer keeps
/// it out of the cache.
#[tokio::test(flavor = "multi_thread")]
async fn memory_storage_writes_nothing_to_the_disk_cache() {
    let built = build_twice_and_query(MemoryStorage::new()).await;
    let cached = files_under(&built.cache_dir);
    assert!(
        cached.is_empty(),
        "memory storage was copied into the disk cache: {cached:?}"
    );
}

/// Remote storage keeps the cache: an incremental build copies what it wrote
/// there, so the first read does not fetch it back. Checked on the new stats
/// sketch because nothing reads one until the next build — the build's own
/// read-through of the previous version fills the cache regardless.
#[tokio::test(flavor = "multi_thread")]
async fn remote_storage_builds_seed_the_disk_cache() {
    let built = build_twice_and_query(MemoryStorage::new().simulating_remote()).await;

    let root_bytes = built
        .fluree
        .content_store(built.ledger_id)
        .get(&built.root_id)
        .await
        .expect("root bytes");
    let sketch = IndexRoot::decode(&root_bytes)
        .expect("decode root")
        .sketch_ref
        .expect("the build wrote a stats sketch");
    assert!(
        built.cache_dir.join(sketch.to_string()).exists(),
        "an incremental build over remote storage did not seed what it wrote"
    );
}

/// Encrypted remote storage reads back plaintext, so nothing of it may land in
/// the cache — not what the indexer wrote, nor what readers fetched. The
/// remote test above is the same flow without the key.
#[tokio::test(flavor = "multi_thread")]
async fn encrypted_remote_storage_writes_nothing_to_the_disk_cache() {
    let storage = EncryptedStorage::new(
        MemoryStorage::new().simulating_remote(),
        StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 0)),
    );
    let built = build_twice_and_query(storage).await;
    let cached = files_under(&built.cache_dir);
    assert!(
        cached.is_empty(),
        "encrypted storage spilled plaintext to the disk cache: {cached:?}"
    );
}

/// Once armed, counts reads and holds each one until released.
#[derive(Debug, Default)]
struct HeldReads {
    armed: AtomicBool,
    released: AtomicBool,
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl StorageHooks for HeldReads {
    async fn before_read(&self, _address: &str) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        while !self.released.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// An encrypted remote backend coalesces its own readers' fetches, and no one
/// else's: a waiter takes the leader's plaintext without decrypting, so a
/// reader holding the wrong key must run, and fail, its own read even while
/// the right key's fetch of the same object is in flight.
#[tokio::test(flavor = "multi_thread")]
async fn encrypted_fetches_are_shared_only_within_one_backend() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let remote = HookedStorage::over(
        MemoryStorage::new().simulating_remote(),
        HeldReads::default(),
    );
    let backend = |key: u8| {
        StorageBackend::Managed(Arc::new(EncryptedStorage::new(
            remote.clone(),
            StaticKeyProvider::new(EncryptionKey::new([key; 32], 0)),
        )))
        .with_disk_cache(tmp.path())
    };
    let (right, wrong) = (backend(0x42), backend(0x24));
    let id = right
        .content_store("db:main")
        .put(ContentKind::IndexLeaf, b"secret leaf")
        .await
        .expect("put");

    let hooks = remote.hooks();
    let reads = || hooks.reads.load(Ordering::SeqCst);
    let get = |backend: &StorageBackend| {
        let store = backend.content_store("db:main");
        let id = id.clone();
        tokio::spawn(async move { store.get(&id).await })
    };
    hooks.armed.store(true, Ordering::SeqCst);
    let leader = get(&right);
    while reads() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let follower = get(&right);
    let intruder = get(&wrong);
    for _ in 0..100 {
        if reads() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Let a read that should not happen show up before counting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    hooks.released.store(true, Ordering::SeqCst);

    assert_eq!(leader.await.unwrap().expect("leader"), b"secret leaf");
    assert_eq!(follower.await.unwrap().expect("follower"), b"secret leaf");
    assert!(
        intruder.await.unwrap().is_err(),
        "a wrong-key reader received another backend's plaintext"
    );
    assert_eq!(
        reads(),
        2,
        "one backend's readers share a fetch; another backend runs its own"
    );
    assert!(files_under(tmp.path()).is_empty());
}

/// A fetch is shared only within one namespace: a child branch's store
/// misses an inherited object and falls back to its parent, and that miss,
/// overlapping a read straight from the parent, must not fail the parent's
/// read. Checked with the disk cache and with fetch-only coalescing.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_branch_miss_does_not_fail_a_concurrent_parent_read() {
    for encrypt in [false, true] {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let remote = HookedStorage::over(
            MemoryStorage::new().simulating_remote(),
            HeldReads::default(),
        );
        let storage: Arc<dyn Storage> = if encrypt {
            Arc::new(EncryptedStorage::new(
                remote.clone(),
                StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 0)),
            ))
        } else {
            Arc::new(remote.clone())
        };
        let backend = StorageBackend::Managed(storage).with_disk_cache(tmp.path());
        let parent = backend.content_store("db:main");
        let child = BranchedContentStore::with_parents(
            backend.content_store("db:dev"),
            vec![BranchedContentStore::leaf(Arc::clone(&parent))],
        );
        let id = parent
            .put(ContentKind::IndexLeaf, b"inherited leaf")
            .await
            .expect("put");

        let hooks = remote.hooks();
        let reads = || hooks.reads.load(Ordering::SeqCst);
        hooks.armed.store(true, Ordering::SeqCst);
        let child_read = {
            let id = id.clone();
            tokio::spawn(async move { child.get(&id).await })
        };
        while reads() < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let parent_read = {
            let (parent, id) = (Arc::clone(&parent), id.clone());
            tokio::spawn(async move { parent.get(&id).await })
        };
        for _ in 0..100 {
            if reads() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        hooks.released.store(true, Ordering::SeqCst);

        let parent_bytes = parent_read.await.unwrap();
        assert!(
            matches!(&parent_bytes, Ok(bytes) if bytes == b"inherited leaf"),
            "encrypt={encrypt}: the parent's read took the child's outcome: {parent_bytes:?}"
        );
        assert_eq!(
            child_read.await.unwrap().expect("child falls back"),
            b"inherited leaf"
        );
    }
}

/// Verification reads the storage itself: a commit the storage lost is
/// reported missing even while this process's disk cache holds a copy.
#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_a_commit_the_storage_lost_despite_a_cached_copy() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let cache_dir = tmp.path().join("cache");
    let storage = MemoryStorage::new().simulating_remote();
    let fluree: Fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig {
            cache_dir: cache_dir.clone(),
            ..LedgerManagerConfig::default()
        })
        .build_with(
            storage.clone(),
            NameServiceMode::ReadWrite(Arc::new(MemoryNameService::new())),
        );
    let ledger_id = "it/verify-cache:main";
    let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let head = fluree
        .insert(ledger, &person(0))
        .await
        .expect("insert")
        .receipt
        .commit_id;

    fluree
        .content_store(ledger_id)
        .get(&head)
        .await
        .expect("read the head");
    assert!(cache_dir.join(head.to_string()).exists(), "head not cached");
    let address = fluree_db_core::storage::content_address(
        storage.storage_method(),
        ContentKind::Commit,
        ledger_id,
        &head.digest_hex(),
    );
    assert!(storage.exists(&address).await.unwrap());
    storage.delete(&address).await.expect("delete");

    let report = fluree.verify_ledger(ledger_id, None).await.expect("verify");
    assert!(
        matches!(
            report.problems.as_slice(),
            [VerifyProblem::MissingHead { commit_id }] if *commit_id == head
        ),
        "{:?}",
        report.problems
    );
}
