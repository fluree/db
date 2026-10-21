//! Content hashes other than SHA2-256: commits minted with SHA2-512 chain
//! onto SHA2-256 history and back, and keep their identity through restart,
//! reindex, time travel, transfer, archive and drop.

use std::collections::HashMap;
use std::path::Path;

use fluree_db_api::{
    Base64Bytes, ExportCommitsRequest, Fluree, FlureeBuilder, GovernanceOptions, IndexConfig,
    PushCommitsRequest,
};
use fluree_db_core::{
    collect_dag_cids, load_commit_by_id, plan_commit_transfer, ContentId, ContentStore,
    HashAlgorithm,
};
use serde_json::json;

const LEDGER: &str = "it/content-hash:main";

fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    }
}

fn file_fluree(dir: &Path, alg: HashAlgorithm) -> Fluree {
    FlureeBuilder::file(dir.to_string_lossy().to_string())
        .with_content_hash(alg)
        .build()
        .expect("fluree")
}

async fn insert_name(fluree: &Fluree, ledger: &str, subject: &str, name: &str) -> ContentId {
    fluree
        .graph(ledger)
        .transact()
        .insert(&json!({
            "@context": {"ex": "http://example.org/"},
            "@id": format!("ex:{subject}"),
            "ex:name": name
        }))
        .commit()
        .await
        .expect("insert")
        .receipt
        .commit_id
}

async fn names(fluree: &Fluree, from: &str) -> Vec<String> {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": from,
        "select": "?n",
        "where": {"@id": "?s", "ex:name": "?n"}
    });
    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_id = from.split('@').next().unwrap();
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let mut out: Vec<String> = result
        .to_jsonld(&ledger.snapshot)
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    out.sort();
    out
}

fn files_named(dir: &Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().unwrap().to_string_lossy().contains(needle) {
                out.push(path);
            }
        }
    }
    out
}

#[tokio::test]
async fn sha2_512_commits_chain_onto_sha2_256_history_and_back() {
    let dir = tempfile::tempdir().unwrap();

    let alice = {
        let fluree = file_fluree(dir.path(), HashAlgorithm::Sha2_256);
        fluree.create_ledger(LEDGER).await.unwrap();
        insert_name(&fluree, LEDGER, "alice", "Alice").await
    };
    assert_eq!(alice.hash_algorithm(), Some(HashAlgorithm::Sha2_256));

    let bob = {
        let fluree = file_fluree(dir.path(), HashAlgorithm::Sha2_512);
        assert_eq!(fluree.content_hash(), HashAlgorithm::Sha2_512);
        let bob = insert_name(&fluree, LEDGER, "bob", "Bob").await;
        assert_eq!(bob.hash_algorithm(), Some(HashAlgorithm::Sha2_512));
        // Its parent link names the SHA2-256 commit, unchanged.
        let store = fluree.branched_content_store(LEDGER).await.unwrap();
        let commit = load_commit_by_id(store.as_ref(), &bob).await.unwrap();
        assert_eq!(commit.id.as_ref(), Some(&bob));
        assert_eq!(commit.parents, vec![alice.clone()]);
        bob
    };
    // The object is stored under its tagged address.
    let bob_files = files_named(dir.path(), &bob.address_digest());
    assert_eq!(bob_files.len(), 1, "{bob_files:?}");
    assert!(bob.address_digest().starts_with("sha2-512-"));

    // A writer back on SHA2-256 reads the SHA2-512 commit and chains onto it.
    let fluree = file_fluree(dir.path(), HashAlgorithm::Sha2_256);
    let carol = insert_name(&fluree, LEDGER, "carol", "Carol").await;
    assert_eq!(carol.hash_algorithm(), Some(HashAlgorithm::Sha2_256));
    let store = fluree.branched_content_store(LEDGER).await.unwrap();
    let dag = collect_dag_cids(store.as_ref(), &carol, 0).await.unwrap();
    let algs: Vec<_> = dag.iter().map(|(_, id)| id.hash_algorithm()).collect();
    assert_eq!(dag.len(), 3, "{dag:?}");
    assert!(algs.contains(&Some(HashAlgorithm::Sha2_512)), "{algs:?}");

    // Time travel to each era, by t and by commit.
    assert_eq!(names(&fluree, LEDGER).await, ["Alice", "Bob", "Carol"]);
    assert_eq!(names(&fluree, &format!("{LEDGER}@t:1")).await, ["Alice"]);
    assert_eq!(
        names(&fluree, &format!("{LEDGER}@t:2")).await,
        ["Alice", "Bob"]
    );
    assert_eq!(
        names(&fluree, &format!("{LEDGER}@commit:{bob}")).await,
        ["Alice", "Bob"]
    );
    assert_eq!(
        names(&fluree, &format!("{LEDGER}@commit:{alice}")).await,
        ["Alice"]
    );
    let short = fluree_db_core::commit_subject::commit_ref(&bob);
    assert!(short.starts_with("sha2-512:"), "{short}");
    assert_eq!(
        names(&fluree, &format!("{LEDGER}@commit:{}", &short[..24])).await,
        ["Alice", "Bob"]
    );
    // The commit's txn-meta subject names its hash.
    let meta = fluree
        .query_connection(&json!({
            "from": format!("{LEDGER}#txn-meta"),
            "select": "?c",
            "where": {"@id": "?c", "https://ns.flur.ee/db#t": 2}
        }))
        .await
        .expect("txn-meta query");
    let ledger = fluree.ledger(LEDGER).await.unwrap();
    let subjects = meta.to_jsonld(&ledger.snapshot).unwrap();
    assert!(
        subjects.to_string().contains("urn:fluree:commit-sha2-512-"),
        "{subjects}"
    );

    // A full rebuild over the mixed chain answers the same.
    fluree
        .reindex(LEDGER, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    assert_eq!(
        names(&fluree, &format!("{LEDGER}@t:2")).await,
        ["Alice", "Bob"]
    );
    assert_eq!(
        names(&fluree, &format!("{LEDGER}@commit:{bob}")).await,
        ["Alice", "Bob"]
    );

    // A hard drop finds and deletes the SHA2-512 object too.
    fluree
        .drop_ledger(
            LEDGER.trim_end_matches(":main"),
            fluree_db_api::DropMode::Hard,
        )
        .await
        .expect("drop");
    assert!(
        files_named(dir.path(), &bob.address_digest()).is_empty(),
        "the SHA2-512 commit survived the drop"
    );
}

async fn bundle(fluree: &Fluree, ledger: &str, name_ids: bool) -> PushCommitsRequest {
    let store = fluree.branched_content_store(ledger).await.unwrap();
    let head = fluree
        .ledger(ledger)
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .unwrap();
    let plan = plan_commit_transfer(store.as_ref(), &head, None)
        .await
        .unwrap()
        .unwrap();
    let mut commits = Vec::new();
    for cid in &plan.lineage {
        commits.push(Base64Bytes(store.get(cid).await.unwrap().into_vec()));
    }
    PushCommitsRequest {
        commits,
        blobs: HashMap::new(),
        missing_blobs: Vec::new(),
        merged_commits: Vec::new(),
        commit_ids: if name_ids {
            plan.lineage.clone()
        } else {
            Vec::new()
        },
        merged_commit_ids: Vec::new(),
    }
}

#[tokio::test]
async fn sha2_512_commits_transfer_by_push_pull_and_archive() {
    let source = FlureeBuilder::memory()
        .with_content_hash(HashAlgorithm::Sha2_512)
        .build_memory();
    source.create_ledger(LEDGER).await.unwrap();
    insert_name(&source, LEDGER, "alice", "Alice").await;
    let head = insert_name(&source, LEDGER, "bob", "Bob").await;
    assert_eq!(head.hash_algorithm(), Some(HashAlgorithm::Sha2_512));

    // Push to a receiver that writes SHA2-256: it verifies each commit
    // against the CID the sender names.
    let receiver = FlureeBuilder::memory().build_memory();
    let pushed = "it/content-hash-pushed:main";
    receiver.create_ledger(pushed).await.unwrap();
    receiver
        .push_commits(
            pushed,
            bundle(&source, LEDGER, true).await,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect("push");
    assert_eq!(names(&receiver, pushed).await, ["Alice", "Bob"]);
    let received = receiver.ledger(pushed).await.unwrap();
    assert_eq!(received.head_commit_id.as_ref(), Some(&head));

    // Without the names, the bytes alone say SHA2-256, and the chain does not
    // link.
    let unnamed = "it/content-hash-unnamed:main";
    receiver.create_ledger(unnamed).await.unwrap();
    let refused = receiver
        .push_commits(
            unnamed,
            bundle(&source, LEDGER, false).await,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await;
    assert!(refused.is_err(), "an unnamed SHA2-512 chain was accepted");

    // A named CID the bytes do not match is refused.
    let mut forged = bundle(&source, LEDGER, true).await;
    forged.commit_ids[1] = ContentId::new_with(
        HashAlgorithm::Sha2_512,
        fluree_db_core::ContentKind::Commit,
        b"other",
    );
    let forged_target = "it/content-hash-forged:main";
    receiver.create_ledger(forged_target).await.unwrap();
    assert!(receiver
        .push_commits(
            forged_target,
            forged,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .is_err());

    // Pull: an export page names its commits, and the importer keeps them.
    let handle = source.ledger_cached(LEDGER).await.unwrap();
    let page = source
        .export_commit_range(
            &handle,
            &ExportCommitsRequest {
                limit: Some(100),
                lineage: true,
                ..Default::default()
            },
        )
        .await
        .expect("export");
    assert_eq!(page.commit_ids.len(), page.commits.len());
    let pulled = "it/content-hash-pulled:main";
    receiver.create_ledger(pulled).await.unwrap();
    let (mut commit_ids, _) = page.ids().unwrap();
    let mut commits = page.commits.clone();
    commit_ids.reverse();
    commits.reverse();
    receiver
        .import_commits_incremental(
            pulled,
            PushCommitsRequest {
                commits,
                blobs: page.blobs.clone(),
                missing_blobs: Vec::new(),
                merged_commits: Vec::new(),
                commit_ids,
                merged_commit_ids: Vec::new(),
            },
        )
        .await
        .expect("pull");
    assert_eq!(names(&receiver, pulled).await, ["Alice", "Bob"]);
    assert_eq!(
        receiver
            .ledger(pulled)
            .await
            .unwrap()
            .head_commit_id
            .as_ref(),
        Some(&head)
    );
}

#[tokio::test]
async fn sha2_512_commits_survive_archive_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(dir.path(), HashAlgorithm::Sha2_512);
    fluree.create_ledger(LEDGER).await.unwrap();
    insert_name(&fluree, LEDGER, "alice", "Alice").await;
    let head = insert_name(&fluree, LEDGER, "bob", "Bob").await;

    let mut archive = Vec::new();
    fluree
        .archive_ledger(LEDGER, false, &mut archive)
        .await
        .expect("archive");
    // Restored by a separate SHA2-256 instance with its own storage.
    let target_dir = tempfile::tempdir().unwrap();
    let target = file_fluree(target_dir.path(), HashAlgorithm::Sha2_256);
    let restored = target
        .restore_ledger("it/content-hash-restored", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;
    assert_eq!(names(&target, &restored).await, ["Alice", "Bob"]);
    assert_eq!(
        target
            .ledger(&restored)
            .await
            .unwrap()
            .head_commit_id
            .as_ref(),
        Some(&head)
    );
    assert_eq!(names(&target, &format!("{restored}@t:1")).await, ["Alice"]);
}

#[tokio::test]
async fn unsupported_hashes_are_refused_by_the_store() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.unwrap();
    let ns = fluree.ledger(LEDGER).await.unwrap().storage_namespace();
    let store = fluree.content_store(&ns);
    // An identity-multihash CID: nothing this build hashes with.
    let bogus: ContentId = "bafkqaayaaeba".parse().unwrap();
    assert_eq!(bogus.hash_algorithm(), None);
    assert!(store.put_with_id(&bogus, &[0, 1, 2]).await.is_err());
}

/// An instance whose readers are not known to support SHA2-512 (a cluster
/// member) admits none of it, by push, pull, clone or restore.
#[tokio::test]
async fn unconfirmed_readers_refuse_other_hashes_on_every_transfer() {
    let dir = tempfile::tempdir().unwrap();
    let source = file_fluree(dir.path(), HashAlgorithm::Sha2_512);
    source.create_ledger(LEDGER).await.unwrap();
    insert_name(&source, LEDGER, "alice", "Alice").await;
    insert_name(&source, LEDGER, "bob", "Bob").await;

    let receiver = FlureeBuilder::memory().build_memory();
    receiver.set_content_hash_readers_confirmed(false);
    let refused = |e: fluree_db_api::ApiError| e.to_string().contains("SHA2-256");

    let pushed = "it/content-hash-member-push:main";
    receiver.create_ledger(pushed).await.unwrap();
    let push = receiver
        .push_commits(
            pushed,
            bundle(&source, LEDGER, true).await,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await;
    assert!(push.is_err_and(refused), "push admitted SHA2-512");

    let pulled = "it/content-hash-member-pull:main";
    receiver.create_ledger(pulled).await.unwrap();
    let pull = receiver
        .import_commits_incremental(pulled, bundle(&source, LEDGER, true).await)
        .await;
    assert!(pull.is_err_and(refused), "pull admitted SHA2-512");

    // Clone: pages written in bulk, then the head set.
    let cloned = "it/content-hash-member-clone:main";
    receiver.create_ledger(cloned).await.unwrap();
    let handle = receiver.ledger_cached(cloned).await.unwrap();
    let page = source
        .export_commit_range(
            &source.ledger_cached(LEDGER).await.unwrap(),
            &ExportCommitsRequest::default(),
        )
        .await
        .expect("export");
    let bulk = receiver.import_commits_bulk(&handle, &page).await;
    assert!(bulk.is_err_and(refused), "clone admitted SHA2-512");
    let head = receiver
        .set_commit_head(&handle, &page.head_commit_id, page.head_t)
        .await;
    assert!(head.is_err_and(refused), "clone finalized a SHA2-512 head");

    let mut archive = Vec::new();
    source
        .archive_ledger(LEDGER, false, &mut archive)
        .await
        .expect("archive");
    let target_dir = tempfile::tempdir().unwrap();
    let target = file_fluree(target_dir.path(), HashAlgorithm::Sha2_256);
    target.set_content_hash_readers_confirmed(false);
    let restored = "it/content-hash-member-restore";
    let restore = target
        .restore_ledger(restored, &mut archive.as_slice())
        .await;
    assert!(restore.is_err_and(refused), "restore admitted SHA2-512");
    // The name was released: once confirmed, the same restore succeeds.
    target.set_content_hash_readers_confirmed(true);
    target
        .restore_ledger(restored, &mut archive.as_slice())
        .await
        .expect("restore once confirmed");
}
