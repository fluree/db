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

/// A ledger erasing alice's email at t1, with archives from before and
/// after: `(dir, source, id, t1, commit at t1, before, after)`.
async fn erased_between_archives(
    name: &str,
) -> (
    tempfile::TempDir,
    Fluree,
    String,
    i64,
    ContentId,
    Vec<u8>,
    Vec<u8>,
) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&fluree, name).await;
    let t1 = insert_kept(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "@type": "ex:Doc",
               "ex:email": "a@secret", "ex:name": "Alice"}),
    )
    .await;
    insert_kept(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@kept"}),
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
    (dir, fluree, id, t1, shared, before, after)
}

/// A ledger here holding the original of a commit a restore erases, the
/// value still current in its index, reads as the erasure does once the
/// restore returns: no current value, its history refused, the commit read
/// as its replacement. No retraction is committed there, and the restored
/// ledger's purge deletes the original, without a prover.
#[tokio::test]
async fn a_restore_erases_what_a_ledger_here_holds() {
    use std::time::Duration;

    let (_dir, _source, _, t1, shared, before, after) =
        erased_between_archives("it/transfer-held").await;
    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-held-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    receiver
        .create_branch("it/transfer-held-copy", "dev", None, None)
        .await
        .unwrap();
    let dev = "it/transfer-held-copy:dev".to_string();
    reindex(&receiver, &held).await;
    reindex(&receiver, &dev).await;
    let head_before = chain(&receiver, &held).await;
    assert_eq!(
        objects(&receiver, &held, "ex:alice", "ex:email").await,
        vec![json!("a@secret")]
    );

    let restored = receiver
        .restore_ledger("it/transfer-held-again", &mut after.as_slice())
        .await
        .unwrap();
    assert_eq!(
        restored.reconciled,
        [dev.clone(), held.clone()],
        "{restored:?}"
    );
    assert_eq!(
        chain(&receiver, &held).await,
        head_before,
        "nothing committed"
    );
    for branch in [&held, &dev] {
        assert!(objects(&receiver, branch, "ex:alice", "ex:email")
            .await
            .is_empty());
    }
    assert_eq!(
        objects(&receiver, &held, "ex:alice", "ex:name").await,
        vec![json!("Alice")]
    );
    let read = store_of(&receiver, &held).await.get(&shared).await.unwrap();
    assert!(is_replacement(&read));
    let past = receiver
        .query_connection(&json!({
            "@context": ctx(),
            "from": format!("{held}@t:{t1}"),
            "select": "?o",
            "where": {"@id": "ex:alice", "ex:email": "?o"}
        }))
        .await;
    assert!(
        matches!(past, Err(ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
    assert!(!files_holding(here.path(), "a@secret").is_empty());

    // The purge is the holder's: a restore that failed after this point
    // would leave it all the same.
    let job = receiver
        .retention_status(&held)
        .await
        .unwrap()
        .registration
        .expect("registration")
        .erasures;
    assert_eq!(job.len(), 1, "{job:?}");
    assert_eq!(job[0].stage, ErasureStage::Published);
    let report = receiver
        .run_purge_pass(&held, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    assert_eq!(
        files_holding(here.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );
    assert!(objects(&receiver, &held, "ex:alice", "ex:email")
        .await
        .is_empty());
}

/// An erasure that differs from the one this storage holds of a commit is
/// refused, leaving the copy here as it was.
#[tokio::test]
async fn a_restore_refuses_a_divergent_erasure() {
    let (_dir, _source, _, _, shared, before, after) =
        erased_between_archives("it/transfer-divergent").await;
    let here = tempfile::tempdir().unwrap();
    let receiver = erasing(&here, Arc::new(NativeProver::default()));
    let held = receiver
        .restore_ledger("it/transfer-divergent-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
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
        .restore_ledger("it/transfer-divergent-again", &mut after.as_slice())
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

/// A value only a ledger's history holds here, retracted in a commit the
/// transfer erases too, leaves its index leaf by leaf: its history entries
/// go, and reads of that history are refused.
#[tokio::test]
async fn a_restore_erases_history_a_ledger_here_holds() {
    let dir = tempfile::tempdir().unwrap();
    let source = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&source, "it/transfer-history").await;
    let t1 = insert_kept(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    source
        .graph(&id)
        .transact()
        .update(&json!({
            "@context": ctx(),
            "delete": {"@id": "ex:alice", "ex:email": "a@secret"}
        }))
        .commit()
        .await
        .unwrap();
    insert(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:bob", "ex:email": "b@kept"}),
    )
    .await;
    let before = archive(&source, &id).await;
    erase_published(&source, &id, email("alice", "a@secret")).await;
    let after = archive(&source, &id).await;

    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-history-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    reindex(&receiver, &held).await;
    let entries = indexed_history_entries(&receiver, &held).await;
    let past = json!({
        "@context": ctx(),
        "from": format!("{held}@t:{t1}"),
        "select": "?o",
        "where": {"@id": "ex:alice", "ex:email": "?o"}
    });
    assert!(receiver.query_connection(&past).await.is_ok());

    let restored = receiver
        .restore_ledger("it/transfer-history-again", &mut after.as_slice())
        .await
        .unwrap();
    assert_eq!(restored.reconciled, [held.as_str()], "{restored:?}");
    assert!(indexed_history_entries(&receiver, &held).await < entries);
    let refused = receiver.query_connection(&past).await;
    assert!(
        matches!(refused, Err(ApiError::HistoryUnavailable(_))),
        "{refused:?}"
    );
    assert_eq!(
        objects(&receiver, &held, "ex:bob", "ex:email").await,
        vec![json!("b@kept")]
    );
}

/// A ledger here that has not indexed the erased commit reads as the erasure
/// does all the same: its unindexed commits are read as their replacements.
#[tokio::test]
async fn a_restore_reaches_a_ledger_that_has_not_indexed_the_commit() {
    use std::time::Duration;

    let (_dir, _source, _, t1, _, before, after) =
        erased_between_archives("it/transfer-unindexed").await;
    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-unindexed-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    let record = receiver.nameservice().lookup(&held).await.unwrap().unwrap();
    assert_eq!(record.index_head_id, None, "nothing indexed");
    let restored = receiver
        .restore_ledger("it/transfer-unindexed-again", &mut after.as_slice())
        .await
        .unwrap();
    assert_eq!(restored.reconciled, [held.as_str()], "{restored:?}");
    assert!(objects(&receiver, &held, "ex:alice", "ex:email")
        .await
        .is_empty());
    let past = receiver
        .query_connection(&json!({
            "@context": ctx(),
            "from": format!("{held}@t:{t1}"),
            "select": "?o",
            "where": {"@id": "ex:alice", "ex:email": "?o"}
        }))
        .await;
    assert!(
        matches!(past, Err(ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
    reindex(&receiver, &held).await;
    assert!(objects(&receiver, &held, "ex:alice", "ex:email")
        .await
        .is_empty());
    let report = receiver
        .run_purge_pass(&held, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    assert_eq!(
        files_holding(here.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );
}

/// A ledger here whose own commit retracts the erased value cannot be made to
/// read as the erasure does: only a prover could erase that commit. The
/// restore is refused before anything is written there.
#[tokio::test]
async fn a_restore_is_refused_where_a_ledger_here_retracted_the_value() {
    let (_dir, _source, _, _, _, before, after) =
        erased_between_archives("it/transfer-cannot").await;
    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-cannot-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    receiver
        .graph(&held)
        .transact()
        .update(&json!({
            "@context": ctx(),
            "delete": {"@id": "ex:alice", "ex:email": "a@secret"}
        }))
        .commit()
        .await
        .unwrap();
    reindex(&receiver, &held).await;
    let refused = receiver
        .restore_ledger("it/transfer-cannot-again", &mut after.as_slice())
        .await
        .expect_err("a retraction here");
    assert!(
        refused.status_code() == 409 && refused.to_string().contains("without a prover"),
        "{refused}"
    );
    let journal = receiver
        .retention_status(&held)
        .await
        .unwrap()
        .registration
        .map(|r| r.journal.removals.len());
    assert_eq!(journal.unwrap_or(0), 0, "nothing journaled");
}

/// A ledger's full-text index here that reads the erased property is
/// rebuilt before the restore returns, and serves without the value; one
/// that never read it keeps its index.
#[tokio::test]
async fn a_restore_rebuilds_a_search_index_that_read_the_value() {
    let (_dir, _source, _, _, _, before, after) =
        erased_between_archives("it/transfer-search").await;
    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-search-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    reindex(&receiver, &held).await;
    let index = |name: &str, property: &str| {
        receiver.create_full_text_index(fluree_db_api::Bm25CreateConfig::new(
            name,
            &held,
            json!({
                "@context": ctx(),
                "where": [{"@id": "?x", "@type": "ex:Doc"}],
                "select": {"?x": ["@id", property]}
            }),
        ))
    };
    let emails = index("transfer-emails", "ex:email")
        .await
        .unwrap()
        .graph_source_id;
    let names = index("transfer-names", "ex:name")
        .await
        .unwrap()
        .graph_source_id;
    let search = |source: &str, text: &str| {
        json!({
            "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
            "from": held,
            "where": [{
                "f:graphSource": source,
                "f:searchText": text,
                "f:searchLimit": 10,
                "f:searchResult": {"f:resultId": "?doc"}
            }],
            "select": ["?doc"]
        })
    };
    let hits = |result: fluree_db_api::QueryResult| -> usize {
        result.batches.iter().map(fluree_db_api::Batch::len).sum()
    };
    let head = |id: &str| {
        let receiver = &receiver;
        let id = id.to_string();
        async move {
            receiver
                .nameservice()
                .lookup_graph_source(&id)
                .await
                .unwrap()
                .unwrap()
                .index_id
        }
    };
    let found = receiver
        .query_connection(&search(&emails, "secret"))
        .await
        .unwrap();
    assert_eq!(hits(found), 1);
    // Not yet synced into either: a rebuild would take it in.
    insert(
        &receiver,
        &held,
        json!({"@context": ctx(), "@id": "ex:carol", "@type": "ex:Doc", "ex:name": "Carol"}),
    )
    .await;
    let (emails_before, names_before) = (head(&emails).await, head(&names).await);

    receiver
        .restore_ledger("it/transfer-search-again", &mut after.as_slice())
        .await
        .unwrap();
    assert_ne!(head(&emails).await, emails_before);
    assert_eq!(head(&names).await, names_before);
    let found = receiver
        .query_connection(&search(&emails, "secret"))
        .await
        .expect("served");
    assert_eq!(hits(found), 0);
    let found = receiver
        .query_connection(&search(&names, "Alice"))
        .await
        .expect("served");
    assert_eq!(hits(found), 1);
}

/// A chain that extends the one this storage holds erases the further
/// slots from the copies reading the shorter one, though they hold no file
/// of the commit.
#[tokio::test]
async fn a_restore_extends_an_erasure_held_here() {
    let (_dir, source, id, _, shared, _, first) =
        erased_between_archives("it/transfer-extended").await;
    let name = ErasureRequest {
        matching: vec![FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/name".into(),
            graph: None,
            object: None,
        }],
        ..Default::default()
    };
    erase_published(&source, &id, name).await;
    let second = archive(&source, &id).await;

    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let earlier = receiver
        .restore_ledger("it/transfer-extended-copy", &mut first.as_slice())
        .await
        .unwrap()
        .ledger_id;
    reindex(&receiver, &earlier).await;
    assert!(commit_file(here.path(), &shared).is_none());
    assert_eq!(
        objects(&receiver, &earlier, "ex:alice", "ex:name").await,
        vec![json!("Alice")]
    );

    let restored = receiver
        .restore_ledger("it/transfer-extended-again", &mut second.as_slice())
        .await
        .unwrap();
    assert_eq!(restored.reconciled, [earlier.as_str()], "{restored:?}");
    for copy in [&earlier, &restored.ledger_id] {
        assert!(objects(&receiver, copy, "ex:alice", "ex:name")
            .await
            .is_empty());
    }
}

/// A pack of only new history, over an erased commit the receiver already
/// has, carries no index either: one built before a purge holds the value.
#[tokio::test]
async fn a_pack_over_erased_history_carries_no_index() {
    let (_dir, fluree, id, _, _) = source("it/transfer-pack").await;
    let have = chain(&fluree, &id).await[0].1.clone();
    insert(
        &fluree,
        &id,
        json!({"@context": ctx(), "@id": "ex:dave", "ex:email": "d@later"}),
    )
    .await;
    let record = fluree.nameservice().lookup(&id).await.unwrap().unwrap();
    // From a receiver holding the erasures already: none is sent.
    let request = fluree_db_api::pack::PackRequest::with_indexes(
        vec![record.commit_head_id.clone().unwrap()],
        vec![have],
        record
            .index_head_id
            .clone()
            .expect("indexed before publication"),
        None,
    )
    .with_erasures(fluree.held_erasure_digests().await.unwrap());
    let handle = fluree.ledger_cached(&id).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let stats = fluree_db_api::pack::stream_pack(&fluree, &handle, &request, tx).await;
    while rx.recv().await.is_some() {}
    assert_eq!(stats.commits_sent, 1, "{stats:?}");
    assert_eq!(stats.erased_commits_sent, 0, "{stats:?}");
    assert_eq!(stats.index_artifacts_sent, 0, "{stats:?}");
}

/// An archive's erasures come with the journal entries of every occurrence
/// they erase: without them, the restored ledger would serve its history
/// as complete when it is not.
#[tokio::test]
async fn a_restore_needs_the_journal_of_what_it_erases() {
    let (_dir, fluree, id, _, _) = source("it/transfer-journaled").await;
    let archive = archive(&fluree, &id).await;
    let received = tempfile::tempdir().unwrap();
    let receiver = receiving(&received);
    let refused = |what: &'static str, archive: Vec<u8>| {
        let receiver = &receiver;
        async move {
            match receiver
                .restore_ledger("it/transfer-journaled-copy", &mut archive.as_slice())
                .await
            {
                Ok(restored) => panic!("{what} accepted: {restored:?}"),
                Err(e) => e.to_string(),
            }
        }
    };
    let unjournaled = rewritten(&archive, |frame| match frame {
        PackFrame::Manifest(mut manifest) if manifest["phase"] == "nameservice" => {
            manifest.as_object_mut().unwrap().remove("retention");
            Some(PackFrame::Manifest(manifest))
        }
        frame => Some(frame),
    });
    let why = refused("unjournaled", unjournaled).await;
    assert!(why.contains("does not record"), "{why}");
    let partly = rewritten(&archive, |frame| match frame {
        PackFrame::Manifest(mut manifest) if manifest["phase"] == "nameservice" => {
            let removals = manifest["retention"]["journal"]["removals"]
                .as_array_mut()
                .unwrap();
            assert!(removals.len() > 1);
            removals.pop();
            Some(PackFrame::Manifest(manifest))
        }
        frame => Some(frame),
    });
    let why = refused("partly journaled", partly).await;
    assert!(why.contains("does not record"), "{why}");
}

/// How many `ex:email` values `ledger` serves: counted by the query engine,
/// and listed.
async fn emails(fluree: &Fluree, ledger: &str) -> (JsonValue, usize) {
    let counted = fluree
        .query_connection(&json!({
            "@context": ctx(),
            "from": ledger,
            "select": ["(as (count ?o) ?n)"],
            "where": {"@id": "?s", "ex:email": "?o"}
        }))
        .await
        .unwrap();
    let ledger_state = fluree.ledger(ledger).await.unwrap();
    let counted = counted.to_jsonld(&ledger_state.snapshot).unwrap();
    let listed = fluree
        .query_connection(&json!({
            "@context": ctx(),
            "from": ledger,
            "select": ["?s", "?o"],
            "where": {"@id": "?s", "ex:email": "?o"}
        }))
        .await
        .unwrap();
    let listed = listed.to_jsonld(&ledger_state.snapshot).unwrap();
    (counted, listed.as_array().map_or(0, Vec::len))
}

/// The index statistics' count of `ex:email` values in `ledger`.
async fn email_stat(fluree: &Fluree, ledger: &str) -> i64 {
    let info = fluree.ledger_info(ledger).execute().await.unwrap();
    let info = serde_json::to_value(&info).unwrap();
    let properties = info["stats"]["properties"].as_object().expect("stats");
    properties
        .iter()
        .find(|(iri, _)| iri.ends_with("email"))
        .and_then(|(_, stat)| stat["count"].as_i64())
        .expect("an email count")
}

/// A value still current in a ledger here leaves it without a rebuild, as
/// a retraction would have taken it: counts drop by one, another subject's
/// occurrence of the same string stays, and history the journal does not
/// cover reads as before, all as a full rebuild then finds them.
#[tokio::test]
async fn an_erased_current_value_leaves_counts_and_shared_values_right() {
    let dir = tempfile::tempdir().unwrap();
    let source = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&source, "it/transfer-counts").await;
    let t1 = insert_kept(
        &source,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:bob", "ex:email": "a@secret"},
            {"@id": "ex:carol", "ex:email": "c@kept"}
        ]}),
    )
    .await;
    insert_kept(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:dave", "ex:email": "d@kept"}),
    )
    .await;
    let mut before = Vec::new();
    source
        .archive_ledger(&id, false, &mut before)
        .await
        .unwrap();
    erase_published(&source, &id, email("alice", "a@secret")).await;
    let after = archive(&source, &id).await;

    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-counts-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    reindex(&receiver, &held).await;
    assert_eq!(emails(&receiver, &held).await, (json!([[4]]), 4));
    assert_eq!(email_stat(&receiver, &held).await, 4);
    let root_before = receiver
        .nameservice()
        .lookup(&held)
        .await
        .unwrap()
        .unwrap()
        .index_head_id;

    let restored = receiver
        .restore_ledger("it/transfer-counts-again", &mut after.as_slice())
        .await
        .unwrap();
    assert_eq!(restored.reconciled, [held.as_str()], "{restored:?}");
    let record = receiver.nameservice().lookup(&held).await.unwrap().unwrap();
    assert_ne!(record.index_head_id, root_before);
    assert_eq!(emails(&receiver, &held).await, (json!([[3]]), 3));
    assert_eq!(email_stat(&receiver, &held).await, 3);
    assert!(objects(&receiver, &held, "ex:alice", "ex:email")
        .await
        .is_empty());
    assert_eq!(
        objects(&receiver, &held, "ex:bob", "ex:email").await,
        vec![json!("a@secret")]
    );
    let past = |subject: &str| {
        json!({
            "@context": ctx(),
            "from": format!("{held}@t:{t1}"),
            "select": "?o",
            "where": {"@id": subject, "ex:email": "?o"}
        })
    };
    let bob = receiver.query_connection(&past("ex:bob")).await.unwrap();
    let state = receiver.ledger(&held).await.unwrap();
    assert_eq!(bob.to_jsonld(&state.snapshot).unwrap(), json!(["a@secret"]));
    assert!(matches!(
        receiver.query_connection(&past("ex:alice")).await,
        Err(ApiError::HistoryUnavailable(_))
    ));

    reindex(&receiver, &held).await;
    assert_eq!(emails(&receiver, &held).await, (json!([[3]]), 3));
    assert_eq!(email_stat(&receiver, &held).await, 3);
    let bob = receiver.query_connection(&past("ex:bob")).await.unwrap();
    let state = receiver.ledger(&held).await.unwrap();
    assert_eq!(bob.to_jsonld(&state.snapshot).unwrap(), json!(["a@secret"]));
}

/// A restore refused for its manifest is refused before any ledger here
/// changes for its erasures.
#[tokio::test]
async fn a_restore_refused_for_its_manifest_changes_nothing_here() {
    let (_dir, _source, _, _, shared, before, after) =
        erased_between_archives("it/transfer-manifest").await;
    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-manifest-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    reindex(&receiver, &held).await;
    let broken = rewritten(&after, |frame| match frame {
        PackFrame::Manifest(mut manifest) if manifest["phase"] == "nameservice" => {
            manifest["default_context_id"] = json!(shared.to_string());
            Some(PackFrame::Manifest(manifest))
        }
        frame => Some(frame),
    });
    let refused = receiver
        .restore_ledger("it/transfer-manifest-again", &mut broken.as_slice())
        .await
        .expect_err("a default context the archive does not hold");
    assert_eq!(refused.status_code(), 400, "{refused}");
    assert_eq!(
        objects(&receiver, &held, "ex:alice", "ex:email").await,
        vec![json!("a@secret")]
    );
    assert!(!is_replacement(
        &store_of(&receiver, &held).await.get(&shared).await.unwrap()
    ));
    let registration = receiver.retention_status(&held).await.unwrap().registration;
    assert!(registration.is_none(), "{registration:?}");
}

/// A ledger here whose index holds one erased value while another is in its
/// unindexed commits loses both: the build that erases the indexed one reads
/// the unindexed commits as their replacements.
#[tokio::test]
async fn a_restore_erases_indexed_and_unindexed_values_alike() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let source = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&source, "it/transfer-mixed").await;
    insert_kept(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:alice", "ex:email": "a@secret"}),
    )
    .await;
    reindex(&source, &id).await;
    insert_kept(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:carol", "ex:email": "c@secret"}),
    )
    .await;
    let mut before = Vec::new();
    source.archive_ledger(&id, true, &mut before).await.unwrap();
    let both = ErasureRequest {
        matching: ["alice", "carol"]
            .into_iter()
            .map(|who| FactSelector {
                subject: format!("http://example.org/{who}"),
                predicate: "http://example.org/email".into(),
                graph: None,
                object: None,
            })
            .collect(),
        ..Default::default()
    };
    erase_published(&source, &id, both).await;
    let after = archive(&source, &id).await;

    let here = tempfile::tempdir().unwrap();
    let receiver = receiving(&here);
    let held = receiver
        .restore_ledger("it/transfer-mixed-copy", &mut before.as_slice())
        .await
        .unwrap()
        .ledger_id;
    let record = receiver.nameservice().lookup(&held).await.unwrap().unwrap();
    assert_eq!((record.index_t, record.commit_t), (1, 2));
    receiver
        .restore_ledger("it/transfer-mixed-again", &mut after.as_slice())
        .await
        .unwrap();
    for who in ["ex:alice", "ex:carol"] {
        assert!(
            objects(&receiver, &held, who, "ex:email").await.is_empty(),
            "{who}"
        );
    }
    let report = receiver
        .run_purge_pass(&held, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    for secret in ["a@secret", "c@secret"] {
        assert_eq!(
            files_holding(here.path(), secret),
            Vec::<std::path::PathBuf>::new(),
            "{secret}"
        );
    }
}

/// `source`'s pack of `request` for `id`, as its pack endpoint streams it.
async fn packed(source: &Fluree, id: &str, request: &fluree_db_api::pack::PackRequest) -> Vec<u8> {
    let handle = source.ledger_cached(id).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    fluree_db_api::pack::stream_pack(source, &handle, request, tx).await;
    let mut bytes = Vec::new();
    while let Some(chunk) = rx.recv().await {
        bytes.extend(chunk.unwrap());
    }
    bytes
}

/// What a clone or pull does with a pack: store its objects in `ledger`'s
/// folder, take its erasures and journal, then move the head to `head`.
async fn take_pack(
    receiver: &Fluree,
    ledger: &str,
    pack: &[u8],
    head: (&ContentId, i64),
    erasure_authority: bool,
) -> fluree_db_api::Result<fluree_db_api::retention::erasure::AcceptedPack> {
    let record = receiver.nameservice().lookup(ledger).await?.unwrap();
    let storage = receiver.backend().admin_storage_cloned().unwrap();
    let mut reception = fluree_db_api::retention::erasure::PackReception::default();
    let mut pos = read_stream_preamble(pack).unwrap();
    loop {
        let (frame, used) = decode_frame(&pack[pos..], DEFAULT_MAX_PAYLOAD).unwrap();
        pos += used;
        match frame {
            PackFrame::End => break,
            PackFrame::Error(e) => panic!("pack error: {e}"),
            PackFrame::Data { cid, payload } => {
                fluree_db_nameservice_sync::ingest_pack_frame(
                    &cid,
                    &payload,
                    &storage,
                    &record.storage_namespace(),
                )
                .await
                .unwrap();
            }
            frame => receiver.receive_pack_frame(&mut reception, frame).await?,
        }
    }
    let accepted = receiver
        .accept_pack(ledger, reception, head.0, erasure_authority)
        .await?;
    let handle = receiver.ledger_cached(ledger).await?;
    receiver.set_commit_head(&handle, head.0, head.1).await?;
    Ok(accepted)
}

/// A receiver as the CLI runs one: no prover, and no ledger cache.
fn pulling(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .without_ledger_caching()
        .with_erasure_verifier(Arc::new(Verifier::accepting_dev_receipts()))
        .build()
        .unwrap()
}

/// `id`'s head in `fluree`.
async fn head_of(fluree: &Fluree, id: &str) -> (ContentId, i64) {
    let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
    (record.commit_head_id.unwrap(), record.commit_t)
}

/// A clone takes erased history from a remote designated an erasure
/// authority, as a restore does, with the journal that refuses its history;
/// from any other remote, it takes none of it.
#[tokio::test]
async fn a_clone_takes_erased_history_from_an_erasure_authority() {
    let (_dir, source, id, erased, t1) = source("it/clone-source").await;
    let (head, head_t) = head_of(&source, &id).await;
    let here = tempfile::tempdir().unwrap();
    let receiver = pulling(&here);

    receiver.create_ledger("it/clone-refused").await.unwrap();
    let request = fluree_db_api::pack::PackRequest::commits(vec![head.clone()], vec![])
        .with_erasures(receiver.held_erasure_digests().await.unwrap());
    let pack = packed(&source, &id, &request).await;
    let refused = take_pack(
        &receiver,
        "it/clone-refused:main",
        &pack,
        (&head, head_t),
        false,
    )
    .await
    .expect_err("not an erasure authority");
    assert_eq!(refused.status_code(), 403, "{refused}");
    assert!(receiver.held_erasure_digests().await.unwrap().is_empty());

    receiver.create_ledger("it/clone-copy").await.unwrap();
    let accepted = take_pack(
        &receiver,
        "it/clone-copy:main",
        &pack,
        (&head, head_t),
        true,
    )
    .await
    .unwrap();
    assert_eq!(accepted.erased_commits, 2, "{accepted:?}");
    received_without(&receiver, "it/clone-copy:main", &erased, t1).await;
    assert_eq!(
        files_holding(here.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );
}

/// The case pulling only new history would get wrong: a copy holding a
/// current, indexed value its source then retracts and erases. The pull
/// brings the erasure of the commit the copy already has before the new
/// commits, and the copy loses the value, its counts and its history,
/// with nothing committed to it and no rebuild; its purge then deletes the
/// original. A later pull sends no chain the copy holds.
#[tokio::test]
async fn a_pull_erases_what_the_copy_already_holds() {
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let source = erasing(&dir, Arc::new(NativeProver::default()));
    let id = ledger(&source, "it/pull-source").await;
    let t1 = insert_kept(
        &source,
        &id,
        json!({"@context": ctx(), "@graph": [
            {"@id": "ex:alice", "ex:email": "a@secret"},
            {"@id": "ex:bob", "ex:email": "b@kept"}
        ]}),
    )
    .await;
    let here = tempfile::tempdir().unwrap();
    let receiver = pulling(&here);
    receiver.create_ledger("it/pull-copy").await.unwrap();
    let copy = "it/pull-copy:main";
    let (head, head_t) = head_of(&source, &id).await;
    let request = fluree_db_api::pack::PackRequest::commits(vec![head.clone()], vec![])
        .with_erasures(Vec::new());
    take_pack(
        &receiver,
        copy,
        &packed(&source, &id, &request).await,
        (&head, head_t),
        true,
    )
    .await
    .unwrap();
    reindex(&receiver, copy).await;
    assert_eq!(email_stat(&receiver, copy).await, 2);
    let copied = head;

    erase_published(&source, &id, email("alice", "a@secret")).await;
    insert(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:carol", "ex:name": "Carol"}),
    )
    .await;
    let (head, head_t) = head_of(&source, &id).await;
    let request = fluree_db_api::pack::PackRequest::commits(vec![head.clone()], vec![copied])
        .with_erasures(receiver.held_erasure_digests().await.unwrap());
    let pack = packed(&source, &id, &request).await;
    let accepted = take_pack(&receiver, copy, &pack, (&head, head_t), true)
        .await
        .unwrap();
    // The commit the copy had, and the removal's retraction it did not.
    assert_eq!(accepted.erased_commits, 2, "{accepted:?}");
    assert_eq!(accepted.reconciled, [copy], "{accepted:?}");

    assert!(objects(&receiver, copy, "ex:alice", "ex:email")
        .await
        .is_empty());
    assert_eq!(
        objects(&receiver, copy, "ex:carol", "ex:name").await,
        vec![json!("Carol")]
    );
    assert_eq!(email_stat(&receiver, copy).await, 1);
    let past = receiver
        .query_connection(&json!({
            "@context": ctx(),
            "from": format!("{copy}@t:{t1}"),
            "select": "?o",
            "where": {"@id": "ex:alice", "ex:email": "?o"}
        }))
        .await;
    assert!(
        matches!(past, Err(ApiError::HistoryUnavailable(_))),
        "{past:?}"
    );
    let report = receiver.run_purge_pass(copy, Duration::ZERO).await.unwrap();
    assert_eq!(report.purged, 1, "{report:?}");
    assert_eq!(
        files_holding(here.path(), "a@secret"),
        Vec::<std::path::PathBuf>::new()
    );

    let again = fluree_db_api::pack::PackRequest::commits(vec![head.clone()], vec![head])
        .with_erasures(receiver.held_erasure_digests().await.unwrap());
    let handle = source.ledger_cached(&id).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let sent = fluree_db_api::pack::stream_pack(&source, &handle, &again, tx).await;
    while rx.recv().await.is_some() {}
    assert_eq!(sent.erased_commits_sent, 0, "{sent:?}");
}

/// A pack that does not take erasures is refused once the history holds
/// one, even one the receiver already has, which it would go on serving;
/// so is a paginated export of that history.
#[tokio::test]
async fn erased_history_needs_a_pack_that_takes_it() {
    let (_dir, source, id, _, _) = source("it/pull-old-client").await;
    insert(
        &source,
        &id,
        json!({"@context": ctx(), "@id": "ex:dave", "ex:email": "d@later"}),
    )
    .await;
    let chain = chain(&source, &id).await;
    let (head, have) = (chain[0].1.clone(), chain[1].1.clone());
    let request = fluree_db_api::pack::PackRequest::commits(vec![head], vec![have]);
    let pack = packed(&source, &id, &request).await;
    assert!(
        contains(&pack, "permanently erased"),
        "{}",
        String::from_utf8_lossy(&pack)
    );

    // A first page short of the erased commit, as a pull's that stops at
    // its own head would be.
    let handle = source.ledger_cached(&id).await.unwrap();
    let first_page = fluree_db_api::ExportCommitsRequest {
        limit: Some(1),
        ..Default::default()
    };
    let refused = source
        .export_commit_range(&handle, &first_page)
        .await
        .expect_err("a paginated export of erased history");
    assert_eq!(refused.status_code(), 501, "{refused}");
}
