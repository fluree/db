//! A ledger whose binary index store fails to load.
//!
//! The ledger manager does not cache such a ledger without its store: the
//! request gets the load's own error, requests within
//! `LedgerManagerConfig::store_load_retry` get it without loading again, and
//! the first request after that loads again. A store fails to load here
//! because a dictionary branch its root names is missing from storage.

use crate::support;
use fluree_db_api::{ApiError, FlureeBuilder, LedgerManagerConfig, TransactError};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;

const EX: &str = "http://example.org/";

/// A file-backed instance over `db`.
fn build(db: &Path, store_load_retry: Duration) -> fluree_db_api::Fluree {
    FlureeBuilder::file(db.to_string_lossy().to_string())
        .without_indexing()
        .with_ledger_cache_config(LedgerManagerConfig {
            store_load_retry,
            ..LedgerManagerConfig::default()
        })
        .build()
        .unwrap()
}

/// A store that cannot load: the branch of the subject reverse dictionary
/// of `ledger_id`'s published root, which a store load reads first, moved
/// aside. [`Self::repair`] puts it back.
struct BrokenStore {
    file: PathBuf,
    aside: PathBuf,
}

impl BrokenStore {
    async fn hide(fluree: &fluree_db_api::Fluree, db: &Path, ledger_id: &str) -> Self {
        let record = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .unwrap()
            .unwrap();
        let store = fluree.content_store(&record.storage_namespace());
        let bytes = store.get(&record.index_head_id.unwrap()).await.unwrap();
        let root = fluree_db_binary_index::format::index_root::IndexRoot::decode(&bytes).unwrap();
        let digest = root.dict_refs.subject_reverse.branch.digest_hex();
        let file = file_named(db, &digest).expect("the branch's file");
        let aside = file.with_extension("aside");
        std::fs::rename(&file, &aside).unwrap();
        Self { file, aside }
    }

    fn repair(&self) {
        std::fs::rename(&self.aside, &self.file).unwrap();
    }
}

/// The file under `dir` whose name holds `part`.
fn file_named(dir: &Path, part: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if let Some(found) = file_named(&path, part) {
                return Some(found);
            }
        } else if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().contains(part))
        {
            return Some(path);
        }
    }
    None
}

/// `ledger_id` with an index (at t=1) and a commit in novelty (t=2).
async fn seed(db: &Path, ledger_id: &str) {
    let fluree = build(db, Duration::ZERO);
    fluree.create_ledger(ledger_id).await.unwrap();
    let nodes: Vec<_> = (0..100)
        .map(|i| json!({"@id": format!("ex:s{i}"), "ex:name": format!("name {i}")}))
        .collect();
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&json!({"@context": {"ex": EX}, "@graph": nodes}))
        .commit()
        .await
        .unwrap();
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    fluree.disconnect_ledger(ledger_id).await;
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&json!({"@context": {"ex": EX}, "@id": "ex:late", "ex:name": "late"}))
        .commit()
        .await
        .unwrap();
    fluree.disconnect_ledger(ledger_id).await;
}

/// A store load's failure: a 500, not a refusal of the request.
fn load_failure<T>(result: fluree_db_api::Result<T>, what: &str) -> ApiError {
    match result {
        Ok(_) => panic!("{what}: succeeded on a ledger whose store cannot load"),
        Err(e) => {
            assert_eq!(e.status_code(), 500, "{what}: {e}");
            assert!(
                !matches!(e, ApiError::Transact(TransactError::IndexNotLoaded(_))),
                "{what}: {e}"
            );
            e
        }
    }
}

async fn count(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> usize {
    fluree
        .graph(ledger_id)
        .query()
        .sparql("SELECT ?s WHERE { ?s <http://example.org/name> ?n }")
        .execute()
        .await
        .expect("query")
        .row_count()
}

/// The request after a failed load loads again, so the ledger heals once its
/// store can load, with no restart; until then it is not served without it.
#[tokio::test]
async fn a_store_that_fails_to_load_loads_again_on_the_next_request() {
    let db = tempfile::tempdir().unwrap();
    let id = "store-load/heal:main";
    seed(db.path(), id).await;
    let fluree = build(db.path(), Duration::ZERO);
    let broken = BrokenStore::hide(&fluree, db.path(), id).await;

    load_failure(fluree.ledger_cached(id).await, "load");
    load_failure(
        fluree
            .graph(id)
            .transact()
            .insert(&json!({"@context": {"ex": EX}, "@id": "ex:x", "ex:name": "x"}))
            .commit()
            .await,
        "insert",
    );

    broken.repair();
    let handle = fluree.ledger_cached(id).await.expect("the store loads");
    assert!(handle.snapshot().await.binary_store.is_some());
    fluree
        .graph(id)
        .transact()
        .insert(&json!({"@context": {"ex": EX}, "@id": "ex:x", "ex:name": "x"}))
        .commit()
        .await
        .expect("an insert once the store has loaded");
    assert_eq!(count(&fluree, id).await, 102);
}

/// A lasting failure is loaded at most once per `store_load_retry`: within
/// the window every request gets the remembered failure, even once the store
/// could load, and the first request after it loads again.
#[tokio::test]
async fn a_lasting_store_load_failure_is_loaded_once_per_window() {
    let db = tempfile::tempdir().unwrap();
    let id = "store-load/window:main";
    seed(db.path(), id).await;
    let window = Duration::from_secs(1);
    let fluree = build(db.path(), window);
    let broken = BrokenStore::hide(&fluree, db.path(), id).await;

    let first = load_failure(fluree.ledger_cached(id).await, "first load");
    broken.repair();
    // The store could load now, but the window has not passed: no load runs.
    let again = load_failure(fluree.ledger_cached(id).await, "within the window");
    assert_eq!(again.to_string(), first.to_string());
    let together = futures::future::join_all((0..20).map(|_| fluree.ledger_cached(id))).await;
    assert!(together.iter().all(Result::is_err), "within the window");

    tokio::time::sleep(window + Duration::from_millis(200)).await;
    let handle = fluree
        .ledger_cached(id)
        .await
        .expect("the first request after the window loads again");
    assert!(handle.snapshot().await.binary_store.is_some());
}

/// A reload that cannot load the store keeps the state and store the ledger
/// has, rather than swapping in a state without a store. The reload meets a
/// newer root: one it shares every artifact with loads nothing.
#[tokio::test]
async fn a_reload_that_cannot_load_the_store_keeps_the_ledger_it_has() {
    let db = tempfile::tempdir().unwrap();
    let id = "store-load/reload:main";
    seed(db.path(), id).await;
    let fluree = build(db.path(), Duration::ZERO);
    let handle = fluree.ledger_cached(id).await.unwrap();
    assert!(handle.snapshot().await.binary_store.is_some());

    support::rebuild_and_publish_index(&fluree, id).await;
    let broken = BrokenStore::hide(&fluree, db.path(), id).await;
    let manager = fluree.ledger_manager().expect("caching enabled");
    load_failure(
        manager
            .reload(&fluree_db_api::LedgerId::parse(id).unwrap())
            .await,
        "reload",
    );
    let handle = fluree.ledger_cached(id).await.expect("the ledger it had");
    assert!(handle.snapshot().await.binary_store.is_some());

    broken.repair();
    assert_eq!(count(&fluree, id).await, 101);
    fluree
        .graph(id)
        .transact()
        .insert(&json!({"@context": {"ex": EX}, "@id": "ex:x", "ex:name": "x"}))
        .commit()
        .await
        .expect("an insert on the ledger the reload kept");
}

/// Reads that a ledger cached without its store answered (each query loaded
/// the store on its own) now get the load's error, as writes do; a ledger
/// that exists still says so.
#[tokio::test]
async fn reads_of_a_ledger_whose_store_fails_to_load_get_the_load_error() {
    let db = tempfile::tempdir().unwrap();
    let id = "store-load/reads:main";
    seed(db.path(), id).await;
    let fluree = build(db.path(), Duration::ZERO);
    let _broken = BrokenStore::hide(&fluree, db.path(), id).await;
    let head = fluree
        .nameservice()
        .lookup(id)
        .await
        .unwrap()
        .unwrap()
        .commit_head_id
        .unwrap();

    load_failure(
        fluree
            .graph(id)
            .query()
            .sparql("SELECT ?s WHERE { ?s ?p ?o }")
            .execute()
            .await,
        "query",
    );
    load_failure(
        fluree.graph(id).commit(&head).execute().await,
        "commit detail",
    );
    load_failure(
        fluree
            .graph_at(id, fluree_db_api::TimeSpec::AtT(1))
            .query()
            .sparql("SELECT ?s WHERE { ?s ?p ?o }")
            .execute()
            .await,
        "query at t=1",
    );
    assert!(fluree.ledger_exists(id).await.unwrap());
}

/// Admitting a datatype past the limit leaves a ledger that can never be
/// indexed, and without its store this process cannot count the datatypes
/// the index holds. The write is refused in every state, never certified:
/// with the store (422), through the ledger manager when the store fails to
/// load (the load's error), and on a state built without the store (503).
#[tokio::test]
async fn a_datatype_past_the_limit_is_refused_in_every_state() {
    use fluree_db_transact::datatype_limit::MAX_NON_RESERVED_DATATYPES as MAX;
    let datatypes = |range: std::ops::Range<usize>| {
        let nodes: Vec<_> = range
            .map(|i| {
                json!({
                    "@id": format!("ex:s{i}"),
                    "ex:p": {"@value": format!("v{i}"), "@type": format!("ex:U{i}")}
                })
            })
            .collect();
        json!({"@context": {"ex": EX}, "@graph": nodes})
    };
    let db = tempfile::tempdir().unwrap();
    let id = "store-load/datatypes:main";
    let fluree = build(db.path(), Duration::ZERO);
    fluree.create_ledger(id).await.unwrap();
    fluree
        .graph(id)
        .transact()
        .insert(&datatypes(0..MAX))
        .commit()
        .await
        .expect("every datatype the ledger can hold");
    support::rebuild_and_publish_index(&fluree, id).await;
    fluree.disconnect_ledger(id).await;
    let one_more = datatypes(MAX..MAX + 1);

    match fluree
        .graph(id)
        .transact()
        .insert(&one_more)
        .commit()
        .await
        .map(drop)
    {
        Err(ApiError::Transact(TransactError::DatatypeLimitExceeded { .. })) => {}
        other => panic!("with the store: expected the datatype limit, got {other:?}"),
    }

    fluree.disconnect_ledger(id).await;
    let broken = BrokenStore::hide(&fluree, db.path(), id).await;
    load_failure(
        fluree.graph(id).transact().insert(&one_more).commit().await,
        "through the ledger manager, the store failing to load",
    );
    broken.repair();

    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    let without_store = fluree_db_api::LedgerState::load_with_store(
        fluree.content_store(&record.storage_namespace()),
        record,
    )
    .await
    .unwrap();
    match fluree.insert(without_store, &one_more).await.map(drop) {
        Err(e @ ApiError::Transact(TransactError::IndexNotLoaded(_))) => {
            assert_eq!(e.status_code(), 503);
        }
        other => panic!("a state without the store: expected IndexNotLoaded, got {other:?}"),
    }

    assert_eq!(fluree.ledger(id).await.unwrap().t(), 1, "nothing committed");
    support::rebuild_and_publish_index(&fluree, id).await;
}
