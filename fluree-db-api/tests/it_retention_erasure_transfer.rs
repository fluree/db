//! Erased history in archives: an erased commit travels as its erasure
//! chain, with its latest replacement, its receipts and what authorized
//! each step, never as its original; a restore elsewhere checks all of it
//! before anything reads the history.

use fluree_db_api::retention::erasure::ErasedCommit;
use fluree_db_api::retention::RemovalRequest;
use fluree_db_core::pack::{
    decode_frame, encode_authorization_frame, encode_data_frame, encode_end_frame,
    encode_erasure_artifact_frame, encode_erasure_frame, encode_error_frame, encode_header_frame,
    encode_manifest_frame, read_stream_preamble, write_stream_preamble, PackFrame,
    DEFAULT_MAX_PAYLOAD,
};
use fluree_db_transact::TxnOpts;

use super::*;

/// An instance that proves nothing, checking receipts as the test prover
/// makes them.
fn receiving(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .with_erasure_verifier(Arc::new(Verifier::accepting_dev_receipts()))
        .build()
        .unwrap()
}

/// `archive` with each frame passed through `edit`, which drops a frame by
/// returning `None`.
fn rewritten(archive: &[u8], mut edit: impl FnMut(PackFrame) -> Option<PackFrame>) -> Vec<u8> {
    let mut pos = read_stream_preamble(archive).unwrap();
    let mut out = Vec::new();
    write_stream_preamble(&mut out);
    loop {
        let (frame, used) = decode_frame(&archive[pos..], DEFAULT_MAX_PAYLOAD).unwrap();
        pos += used;
        let end = matches!(frame, PackFrame::End);
        if let Some(frame) = edit(frame) {
            encode(&frame, &mut out);
        }
        if end {
            return out;
        }
    }
}

fn encode(frame: &PackFrame, out: &mut Vec<u8>) {
    match frame {
        PackFrame::Header(h) => encode_header_frame(h, out),
        PackFrame::Data { cid, payload } => encode_data_frame(cid, payload, out),
        PackFrame::Error(m) => encode_error_frame(m, out),
        PackFrame::Manifest(v) => encode_manifest_frame(v, out),
        PackFrame::Erasure(v) => encode_erasure_frame(v, out),
        PackFrame::Authorization(v) => encode_authorization_frame(v, out),
        PackFrame::ErasureArtifact { kind, payload } => {
            encode_erasure_artifact_frame(*kind, payload, out);
        }
        PackFrame::End => encode_end_frame(out),
    }
}

/// The frames of `archive` that carry its erasures.
fn erasure_frames(archive: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    rewritten(archive, |frame| {
        if matches!(
            frame,
            PackFrame::Erasure(_) | PackFrame::Authorization(_) | PackFrame::ErasureArtifact { .. }
        ) {
            let mut buf = Vec::new();
            encode(&frame, &mut buf);
            frames.push(buf);
        }
        Some(frame)
    });
    frames
}

/// An erasure frame's chain, its digest made again after `edit`.
fn rechained(mut chain: JsonValue, edit: impl FnOnce(&mut JsonValue)) -> JsonValue {
    edit(&mut chain);
    let erased: ErasedCommit = serde_json::from_value(chain.clone()).unwrap();
    chain["digest"] = json!(erased.digest());
    chain
}

async fn erase_published(fluree: &Fluree, id: &str, request: ErasureRequest) -> String {
    let receipt = fluree
        .erase_occurrences(id, request, None)
        .await
        .expect("erase");
    reindex(fluree, id).await;
    assert_eq!(fluree.run_erasure_pass(id).await.unwrap().published, 1);
    receipt.request_id
}

async fn archive(fluree: &Fluree, id: &str) -> Vec<u8> {
    let mut archive = Vec::new();
    fluree.archive_ledger(id, true, &mut archive).await.unwrap();
    archive
}

/// The value gone at `copy`, its history refused, the rest served, and the
/// erased commit read as its replacement.
async fn received_without(fluree: &Fluree, copy: &str, erased: &ContentId, t: i64) {
    assert!(objects(fluree, copy, "ex:alice", "ex:email")
        .await
        .is_empty());
    assert_eq!(
        objects(fluree, copy, "ex:bob", "ex:email").await,
        vec![json!("b@kept")]
    );
    assert_eq!(
        objects(fluree, copy, "ex:carol", "ex:email").await,
        vec![json!("c@later")]
    );
    let read = store_of(fluree, copy).await.get(erased).await.unwrap();
    assert!(is_replacement(&read));
    let past = fluree
        .query_connection(&json!({
            "@context": ctx(),
            "from": format!("{copy}@t:{t}"),
            "select": "?o",
            "where": {"@id": "ex:alice", "ex:email": "?o"}
        }))
        .await;
    assert!(
        matches!(past, Err(ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
}

/// Insert, keeping the transaction document.
async fn insert_kept(fluree: &Fluree, ledger_id: &str, doc: JsonValue) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .txn_opts(TxnOpts::default().store_raw_txn(true))
        .insert(&doc)
        .commit()
        .await
        .unwrap()
        .receipt
        .t
}

/// A ledger whose first commit holds alice's email, erased and published.
async fn source(name: &str) -> (tempfile::TempDir, Fluree, String, ContentId, i64) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, name).await;
    let t1 = insert_kept(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:bob", "ex:email": "b@kept"}
        ]}),
    )
    .await;
    insert_kept(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:carol", "ex:email": "c@later"}),
    )
    .await;
    let erased = commit_at(&fluree, &id, t1).await;
    erase_published(&fluree, &id, email("alice", "a@secret")).await;
    (dir, fluree, id, erased, t1)
}

/// Another storage, with no prover, restores an archive of erased history:
/// the erased commits read as their replacements, neither the value nor its
/// history is served, no original or transaction document holding the
/// value arrives, and the copy archives its erased history in turn.
#[tokio::test]
async fn an_archive_carries_erased_history_to_another_storage() {
    let (dir, fluree, id, erased, t1) = source("it/transfer-source").await;
    assert_eq!(t1, 1, "the genesis commit is the erased one");
    assert!(!files_holding(dir.path(), "a@secret").is_empty());

    let mut archive = Vec::new();
    let sent = fluree
        .archive_ledger(&id, true, &mut archive)
        .await
        .unwrap();
    // The assertion, and the removal's retraction of it.
    assert_eq!(sent.erased_commits_sent, 2, "{sent:?}");
    assert_eq!(sent.index_artifacts_sent, 0, "{sent:?}");
    // Carol's; the erased commit's holds the value.
    assert_eq!(sent.txn_blobs_sent, 1, "{sent:?}");
    assert!(!contains(&archive, "a@secret"));

    let received = tempfile::tempdir().unwrap();
    let receiver = receiving(&received);
    let restored = receiver
        .restore_ledger("it/transfer-copy", &mut archive.as_slice())
        .await
        .unwrap();
    assert_eq!(restored.erased_commits, 2, "{restored:?}");
    assert_eq!(restored.index_t, None);
    let copy = restored.ledger_id;
    received_without(&receiver, &copy, &erased, t1).await;
    let registration = receiver
        .retention_status(&copy)
        .await
        .unwrap()
        .registration
        .expect("the archive's journal");
    assert_eq!(
        registration.lineage,
        Some(erased.to_string()),
        "the erased genesis keys the journal checkpoint"
    );
    assert_eq!(
        files_holding(received.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );

    drop(receiver);
    let receiver = receiving(&received);
    reindex(&receiver, &copy).await;
    received_without(&receiver, &copy, &erased, t1).await;

    let again = archive_of(&receiver, &copy, 2).await;
    let third = tempfile::tempdir().unwrap();
    let onward = receiving(&third);
    let copy = onward
        .restore_ledger("it/transfer-onward", &mut again.as_slice())
        .await
        .unwrap()
        .ledger_id;
    received_without(&onward, &copy, &erased, t1).await;
}

fn file_named(root: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name().is_some_and(|n| n == name) {
                return Some(path);
            }
        }
    }
    None
}

async fn archive_of(fluree: &Fluree, id: &str, erased: usize) -> Vec<u8> {
    let mut archive = Vec::new();
    let sent = fluree.archive_ledger(id, true, &mut archive).await.unwrap();
    assert_eq!(sent.erased_commits_sent, erased, "{sent:?}");
    archive
}

/// Each step is checked against what authorized it: the control commit
/// recording its request and manifest, and the occurrences that manifest
/// names. A refused restore leaves nothing behind.
#[tokio::test]
async fn a_restore_checks_each_step_against_its_authorization() {
    let (_dir, fluree, id, _, _) = source("it/transfer-authorized").await;
    let other = erase_published(&fluree, &id, email("carol", "c@later")).await;
    let archive = archive(&fluree, &id).await;
    let received = tempfile::tempdir().unwrap();
    let receiver = receiving(&received);
    let refused = |what: &'static str, archive: Vec<u8>| {
        let receiver = &receiver;
        async move {
            match receiver
                .restore_ledger("it/transfer-checked", &mut archive.as_slice())
                .await
            {
                Ok(restored) => panic!("{what} accepted: {restored:?}"),
                Err(e) => e.to_string(),
            }
        }
    };

    // A genuine step under another genuine request, which named other slots.
    let mut relabeled = false;
    let swapped = rewritten(&archive, |frame| match frame {
        PackFrame::Erasure(chain) if !relabeled && chain["steps"][0]["requestId"] != other => {
            relabeled = true;
            Some(PackFrame::Erasure(rechained(chain, |c| {
                c["steps"][0]["requestId"] = json!(other);
            })))
        }
        frame => Some(frame),
    });
    assert!(relabeled);
    let why = refused("relabeled", swapped).await;
    assert!(why.contains("did not name"), "{why}");

    let unauthorized = rewritten(&archive, |frame| match frame {
        PackFrame::Authorization(_) => None,
        frame => Some(frame),
    });
    let why = refused("unauthorized", unauthorized).await;
    assert!(why.contains("does not carry"), "{why}");

    // A manifest widened to cover more than the control commit records.
    let widened = rewritten(&archive, |frame| match frame {
        PackFrame::Authorization(mut authorization) => {
            let mut more = authorization["manifest"][0].clone();
            more["slot"] = json!(999);
            authorization["manifest"].as_array_mut().unwrap().push(more);
            Some(PackFrame::Authorization(authorization))
        }
        frame => Some(frame),
    });
    let why = refused("widened", widened).await;
    assert!(why.contains("another manifest"), "{why}");

    let forged = rewritten(&archive, |frame| match frame {
        PackFrame::Erasure(mut chain) => {
            chain["steps"][0]["slots"] = json!([99]);
            Some(PackFrame::Erasure(chain))
        }
        frame => Some(frame),
    });
    let why = refused("forged", forged).await;
    assert!(why.contains("does not match its digest"), "{why}");

    let retargeted = rewritten(&archive, |frame| match frame {
        PackFrame::Erasure(mut chain) => {
            chain["txn"] = json!("0".repeat(64));
            Some(PackFrame::Erasure(chain))
        }
        frame => Some(frame),
    });
    let why = refused("retargeted", retargeted).await;
    assert!(why.contains("another transaction document"), "{why}");

    // Another ledger's genuine erasure, which this history does not hold.
    let foreign = {
        let other = ledger(&fluree, "it/transfer-foreign").await;
        insert(
            &fluree,
            &other,
            json!({"@context": ctx(), "@id": "ex:dave", "ex:email": "d@secret"}),
        )
        .await;
        erase_published(&fluree, &other, email("dave", "d@secret")).await;
        erasure_frames(&archive_of(&fluree, &other, 2).await)
    };
    let mut spliced = rewritten(&archive, |frame| match frame {
        PackFrame::End => None,
        frame => Some(frame),
    });
    spliced.extend(foreign.concat());
    encode_end_frame(&mut spliced);
    let why = refused("foreign", spliced).await;
    assert!(why.contains("its history does not hold"), "{why}");

    // A step's slots changed with its digest: its proof does not verify.
    let reslotted = rewritten(&archive, |frame| match frame {
        PackFrame::Erasure(chain) => Some(PackFrame::Erasure(rechained(chain, |c| {
            c["steps"][0]["slots"] = json!([]);
        }))),
        frame => Some(frame),
    });
    let why = refused("reslotted", reslotted).await;
    assert!(why.contains("does not verify"), "{why}");

    let copy = receiver
        .restore_ledger("it/transfer-checked", &mut archive.as_slice())
        .await
        .expect("the genuine archive, under the name the refusals left free")
        .ledger_id;
    assert!(objects(&receiver, &copy, "ex:carol", "ex:email")
        .await
        .is_empty());
}

/// The control commit of a request made on a branch is not in the history
/// of the branch archived; the archive carries it all the same.
#[tokio::test]
async fn a_restore_takes_an_authorization_made_on_another_branch() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/transfer-branched").await;
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:bob", "ex:email": "b@kept"}
        ]}),
    )
    .await;
    fluree
        .create_branch("it/transfer-branched", "dev", None, None)
        .await
        .expect("branch");
    let dev = "it/transfer-branched:dev";
    let receipt = fluree
        .erase_occurrences(dev, email("alice", "a@secret"), None)
        .await
        .expect("erase");
    reindex(&fluree, dev).await;
    reindex(&fluree, &id).await;
    assert_eq!(fluree.run_erasure_pass(dev).await.unwrap().published, 1);
    let dev_record = fluree.nameservice().lookup(dev).await.unwrap().unwrap();
    assert_eq!(dev_record.commit_t, receipt.authorized_t);
    let control = dev_record.commit_head_id.unwrap();
    assert!(chain(&fluree, &id).await.iter().all(|(_, c)| *c != control));

    let archive = archive(&fluree, &id).await;
    let received = tempfile::tempdir().unwrap();
    let receiver = receiving(&received);
    let copy = receiver
        .restore_ledger("it/transfer-branched-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    assert!(objects(&receiver, &copy, "ex:alice", "ex:email")
        .await
        .is_empty());
    assert_eq!(
        objects(&receiver, &copy, "ex:bob", "ex:email").await,
        vec![json!("b@kept")]
    );
}

/// A restore never erases history this storage holds as its original, nor
/// takes an erasure that differs from the one it holds of a commit: both
/// are refused, leaving the copy here reading as before.
#[tokio::test]
async fn a_restore_refuses_erasures_of_history_held_here() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, "it/transfer-held").await;
    let t1 = insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret", "ex:name": "Alice"}),
    )
    .await;
    let shared = commit_at(&fluree, &id, t1).await;
    let mut before = Vec::new();
    fluree
        .archive_ledger(&id, false, &mut before)
        .await
        .unwrap();
    erase_published(&fluree, &id, email("alice", "a@secret")).await;
    let after = archive(&fluree, &id).await;

    let here = tempfile::tempdir().unwrap();
    let receiver = erasing(&here, Arc::new(NativeProver::default()));
    let held = receiver
        .restore_ledger("it/transfer-held-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    let refused = receiver
        .restore_ledger("it/transfer-held-again", &mut after.as_slice())
        .await
        .expect_err("held here");
    assert!(
        refused.to_string().contains(&format!("{shared}"))
            && refused.to_string().contains("holds on this storage"),
        "{refused}"
    );
    assert_eq!(
        objects(&receiver, &held, "ex:alice", "ex:email").await,
        vec![json!("a@secret")],
        "nothing was published"
    );

    // Here, the same commit loses another value: one step each, apart.
    let name = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/name".into(),
            graph: None,
            object: None,
        }],
        ..Default::default()
    };
    erase_published(&receiver, &held, name).await;
    let refused = receiver
        .restore_ledger("it/transfer-held-again", &mut after.as_slice())
        .await
        .expect_err("divergent");
    assert!(
        refused.to_string().contains(&format!("{shared}"))
            && refused.to_string().contains("differs"),
        "{refused}"
    );
    assert_eq!(
        objects(&receiver, &held, "ex:alice", "ex:email").await,
        vec![json!("a@secret")]
    );
}

/// A commit erased in two steps, its intermediate replacement purged at the
/// source, restores without a prover: the chain verifies against the
/// latest replacement.
#[tokio::test]
async fn a_two_step_erasure_restores_after_its_source_purged() {
    use std::time::Duration;

    let (dir, fluree, id, erased, t1) = source("it/transfer-purged").await;
    let bob = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/bob".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: None,
        }],
        ..Default::default()
    };
    erase_published(&fluree, &id, bob).await;
    let mut steps = Vec::new();
    rewritten(&archive(&fluree, &id).await, |frame| {
        if let PackFrame::Erasure(chain) = &frame {
            if chain["commit"] == erased.to_string() {
                steps = chain["steps"].as_array().unwrap().clone();
            }
        }
        Some(frame)
    });
    assert_eq!(steps.len(), 2);
    let first: ContentId = steps[0]["replacement"].as_str().unwrap().parse().unwrap();
    let intermediate = format!("{}.fcr", first.address_digest());
    assert!(file_named(dir.path(), &intermediate).is_some());
    for _ in 0..2 {
        fluree.run_purge_pass(&id, Duration::ZERO).await.unwrap();
    }
    assert!(
        file_named(dir.path(), &intermediate).is_none(),
        "the source purged its intermediate replacement"
    );
    let archive = archive(&fluree, &id).await;

    let received = tempfile::tempdir().unwrap();
    let receiver = receiving(&received);
    let copy = receiver
        .restore_ledger("it/transfer-purged-copy", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    for who in ["ex:alice", "ex:bob"] {
        assert!(objects(&receiver, &copy, who, "ex:email").await.is_empty());
    }
    assert_eq!(
        objects(&receiver, &copy, "ex:carol", "ex:email").await,
        vec![json!("c@later")]
    );
    let past = receiver
        .query_connection(&json!({
            "@context": ctx(),
            "from": format!("{copy}@t:{t1}"),
            "select": "?o",
            "where": {"@id": "ex:bob", "ex:email": "?o"}
        }))
        .await;
    assert!(
        matches!(past, Err(ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
}

/// An instance that cannot hold erased history refuses an archive of it,
/// and a pack, which does not carry erasures yet, refuses the ledger.
#[tokio::test]
async fn erased_history_is_refused_where_it_cannot_go() {
    let (_dir, fluree, id, _, _) = source("it/transfer-refused").await;
    let archive = archive(&fluree, &id).await;
    let memory = FlureeBuilder::memory().build_memory();
    let refused = memory
        .restore_ledger("it/transfer-memory", &mut archive.as_slice())
        .await
        .expect_err("memory storage");
    assert!(refused.to_string().contains("cannot store"), "{refused}");

    let handle = fluree.ledger_cached(&id).await.unwrap();
    let request = fluree_db_api::pack::full_ledger_pack_request(&handle, false)
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let stats = fluree_db_api::pack::stream_pack(&fluree, &handle, &request, tx).await;
    assert_eq!(stats.commits_sent, 0);
    let mut stream = Vec::new();
    while let Some(chunk) = rx.recv().await {
        stream.extend(chunk.unwrap());
    }
    assert!(
        contains(&stream, "permanently erased"),
        "{}",
        String::from_utf8_lossy(&stream)
    );
}

/// Each restore of erased history records its folder as reading the erased
/// commits: a later erasure of the same commit on one copy finds the other,
/// which holds no file of it, and removes the value there too.
#[tokio::test]
async fn a_later_erasure_here_reaches_every_copy_restored() {
    let (_dir, fluree, id, _, _) = source("it/transfer-copies").await;
    let archive = archive(&fluree, &id).await;
    let here = tempfile::tempdir().unwrap();
    let receiver = erasing(&here, Arc::new(NativeProver::default()));
    let mut copies = Vec::new();
    for name in ["it/transfer-copy-a", "it/transfer-copy-b"] {
        let restored = receiver
            .restore_ledger(name, &mut archive.as_slice())
            .await
            .unwrap();
        copies.push(restored.ledger_id);
    }
    let receipt = receiver
        .erase_occurrences(&copies[0], email("bob", "b@kept"), None)
        .await
        .expect("erase");
    let reached: Vec<&str> = receipt
        .removals
        .iter()
        .map(|r| r.ledger_id.as_str())
        .collect();
    assert_eq!(reached, [copies[0].as_str(), copies[1].as_str()]);
    assert!(objects(&receiver, &copies[1], "ex:bob", "ex:email")
        .await
        .is_empty());
}

/// A removal made on one restored copy reaches a copy restored later from
/// the same archive, through the journal checkpoint of their lineage, which
/// the erased genesis commit names.
#[tokio::test]
async fn a_restore_takes_what_its_lineage_removed_since() {
    let (_dir, fluree, id, _, _) = source("it/transfer-lineage").await;
    let archive = archive(&fluree, &id).await;
    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let first = receiver
        .restore_ledger("it/transfer-lineage-a", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    receiver
        .remove_occurrences(
            &first,
            RemovalRequest {
                matching: vec![FactSelector {
                    subject: "http://example.org/bob".into(),
                    predicate: "http://example.org/email".into(),
                    graph: None,
                    object: None,
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second = receiver
        .restore_ledger("it/transfer-lineage-b", &mut archive.as_slice())
        .await
        .unwrap()
        .ledger_id;
    assert!(objects(&receiver, &second, "ex:bob", "ex:email")
        .await
        .is_empty());
}
