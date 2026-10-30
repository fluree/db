//! A restore of a soft-dropped ledger interrupted part way, then resumed.

use fluree_db_nameservice::{lifecycle, NameServiceError};
use serde_json::json;

use crate::retention::{registry, FactSelector, RemovalRequest};
use crate::{DropMode, FlureeBuilder};

fn email(subject: &str, value: &str) -> RemovalRequest {
    RemovalRequest {
        occurrences: vec![],
        matching: vec![FactSelector {
            subject: format!("http://example.org/{subject}"),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!(value)),
        }],
    }
}

/// A retry of a restore keeps what its first attempt merged into the
/// reinstated configs, rather than writing the drop's copies over it.
#[tokio::test]
async fn a_resumed_restore_keeps_what_its_first_attempt_merged() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    fluree.create_ledger("t/resumed").await.unwrap();
    let source = "t/resumed:main";
    let doc = json!({"@graph": [
        {"@id": "http://example.org/alice", "http://example.org/email": "a@old"},
        {"@id": "http://example.org/bob", "http://example.org/email": "b@old"}
    ]});
    fluree
        .graph(source)
        .transact()
        .insert(&doc)
        .commit()
        .await
        .unwrap();
    let mut archive = Vec::new();
    fluree
        .archive_ledger(source, false, &mut archive)
        .await
        .unwrap();
    let copy = fluree
        .restore_ledger("t/resumed-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    // The source's own removal: a registration its drop keeps.
    fluree
        .remove_occurrences(source, email("bob", "b@old"))
        .await
        .unwrap();
    let instance = fluree
        .drop_ledger("t/resumed", DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    let removal = fluree
        .remove_occurrences(&copy, email("alice", "a@old"))
        .await
        .unwrap();

    // Merged, retracted, then stopped; the checkpoint then lost, so only
    // the reinstated configs hold what the merge added.
    let store = fluree.publisher().unwrap();
    let stopped = lifecycle::restore_dropped_with(store, &instance, |entry, records| async {
        fluree.prepare_restore(entry, records).await?;
        Err(NameServiceError::storage("stopped"))
    })
    .await;
    assert!(stopped.is_err());
    std::fs::remove_dir_all(dir.path().join("@retention/journal")).unwrap();
    lifecycle::resume_restore_with(store, &instance, |entry, records| {
        fluree.prepare_restore(entry, records)
    })
    .await
    .unwrap()
    .expect("resumed");
    assert!(store.get_dropped(&instance).await.unwrap().is_none());

    let status = fluree.retention_status(source).await.unwrap();
    let journal = status.registration.expect("registration").journal;
    for occurrence in &removal.manifest {
        assert!(
            journal
                .removals
                .iter()
                .any(|r| r.commit == occurrence.commit && r.slot == occurrence.slot),
            "{occurrence:?} not in {journal:?}"
        );
    }
}

/// A restore interrupted between its retraction and the journal entry for
/// it journals the retraction when resumed.
#[tokio::test]
async fn a_restore_interrupted_after_its_retraction_journals_it_when_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    fluree.create_ledger("t/interrupted").await.unwrap();
    let source = "t/interrupted:main";
    let insert = |doc: serde_json::Value| {
        let fluree = &fluree;
        async move {
            fluree
                .graph(source)
                .transact()
                .insert(&doc)
                .commit()
                .await
                .unwrap();
        }
    };
    insert(json!({"@id": "http://example.org/alice", "http://example.org/email": "a@old"})).await;
    let mut archive = Vec::new();
    fluree
        .archive_ledger(source, false, &mut archive)
        .await
        .unwrap();
    let copy = fluree
        .restore_ledger("t/interrupted-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    insert(json!({"@id": "http://example.org/bob", "http://example.org/email": "b@old"})).await;
    let instance = fluree
        .drop_ledger("t/interrupted", DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    fluree
        .remove_occurrences(&copy, email("alice", "a@old"))
        .await
        .unwrap();

    // Retracted, then stopped before the journal held the retraction; the
    // checkpoint, which the stopped attempt did reach, then lost.
    let store = fluree.publisher().unwrap();
    let stopped = lifecycle::restore_dropped_with(store, &instance, |entry, records| async {
        fluree.prepare_restore(entry, records).await?;
        let record = store.raw_record(source).await?.unwrap();
        let head = record.commit_head_id.unwrap().to_string();
        registry::update_fenced(
            store,
            fluree.registry_objects(),
            source,
            record.fence,
            |current| {
                let mut next = current?.clone();
                next.journal.removals.retain(|r| r.commit != head);
                Some(next)
            },
        )
        .await
        .unwrap();
        Err(NameServiceError::storage("stopped"))
    })
    .await;
    assert!(stopped.is_err());
    std::fs::remove_dir_all(dir.path().join("@retention/journal")).unwrap();
    lifecycle::resume_restore_with(store, &instance, |entry, records| {
        fluree.prepare_restore(entry, records)
    })
    .await
    .unwrap()
    .expect("resumed");

    let journal = fluree
        .retention_status(source)
        .await
        .unwrap()
        .registration
        .expect("registration")
        .journal;
    let head = fluree
        .nameservice()
        .lookup(source)
        .await
        .unwrap()
        .unwrap()
        .commit_head_id
        .unwrap()
        .to_string();
    assert!(
        journal.removals.iter().any(|r| r.commit == head),
        "the restore's retraction is not journaled: {journal:?}"
    );
}

/// A restore stopped after it wrote its registration keeps the chunks that
/// registration names through sweeps: its records are not visible yet, but
/// a resumed restore reads them.
#[tokio::test]
async fn a_stopped_restore_keeps_its_registration_chunks_through_sweeps() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    fluree.create_ledger("t/stopped").await.unwrap();
    let source = "t/stopped:main";
    for (who, value) in [("alice", "a@old"), ("bob", "b@old")] {
        fluree
            .graph(source)
            .transact()
            .insert(&json!({
                "@id": format!("http://example.org/{who}"),
                "http://example.org/email": value
            }))
            .commit()
            .await
            .unwrap();
        fluree
            .remove_occurrences(source, email(who, value))
            .await
            .unwrap();
    }
    let instance = fluree
        .drop_ledger("t/stopped", DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();

    // Stopped once its registration holds a journal the drop's copy does not.
    let store = fluree.publisher().unwrap();
    let stopped = lifecycle::restore_dropped_with(store, &instance, |entry, records| async {
        fluree.prepare_restore(entry, records).await?;
        let record = store.raw_record(source).await?.unwrap();
        registry::update_fenced(
            store,
            fluree.registry_objects(),
            source,
            record.fence,
            |current| {
                let mut next = current?.clone();
                next.journal.removals.truncate(1);
                Some(next)
            },
        )
        .await
        .unwrap();
        Err(NameServiceError::storage("stopped"))
    })
    .await;
    assert!(stopped.is_err());
    let stored = registry::read_stored(store, source)
        .await
        .unwrap()
        .1
        .unwrap();
    let named: Vec<String> =
        serde_json::from_value(stored["journal"]["removalChunks"].clone()).unwrap();

    let grace = std::time::Duration::from_millis(20);
    fluree.sweep_registration_chunks(grace).await.unwrap();
    tokio::time::sleep(grace).await;
    fluree.sweep_registration_chunks(grace).await.unwrap();
    let storage = fluree.admin_storage().unwrap();
    for hash in &named {
        let address = format!(
            "fluree:{}://@retention/registration/{hash}.json",
            storage.storage_method()
        );
        assert!(storage.exists(&address).await.unwrap(), "{hash} was swept");
    }
    lifecycle::resume_restore_with(store, &instance, |entry, records| {
        fluree.prepare_restore(entry, records)
    })
    .await
    .unwrap()
    .expect("resumed");
}
